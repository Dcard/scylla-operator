package orphanedpv

import (
	"context"
	"fmt"
	"strings"
	"time"

	scyllav1 "github.com/scylladb/scylla-operator/pkg/api/scylla/v1"
	"github.com/scylladb/scylla-operator/pkg/controller/helpers"
	"github.com/scylladb/scylla-operator/pkg/naming"
	"github.com/scylladb/scylla-operator/pkg/resourceapply"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type PVItem struct {
	PV          *corev1.PersistentVolume
	ServiceName string
}

func (opc *Controller) getPVsForScyllaCluster(ctx context.Context, sc *scyllav1.ScyllaCluster) ([]*PVItem, []string, error) {
	var errs []error
	var requeueReasons []string
	var pis []*PVItem
	for _, rack := range sc.Spec.Datacenter.Racks {
		stsName := naming.StatefulSetNameForRack(rack, sc)
		for i := int32(0); i < rack.Members; i++ {
			svcName := fmt.Sprintf("%s-%d", stsName, i)
			pvcName := fmt.Sprintf("%s-%s", naming.PVCTemplateName, svcName)
			pvc, err := opc.pvcLister.PersistentVolumeClaims(sc.Namespace).Get(pvcName)
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.V(2).InfoS("PVC not found", "PVC", fmt.Sprintf("%s/%s", sc.Namespace, pvcName))
					// We aren't watching PVCs so we need to requeue manually
					requeueReasons = append(requeueReasons, "PVC not found")
					continue
				}
				errs = append(errs, err)
				continue
			}

			if len(pvc.Spec.VolumeName) == 0 {
				klog.V(2).InfoS("PVC not bound yet", "PVC", klog.KObj(pvc))
				requeueReasons = append(requeueReasons, "PVC not bound yet")
				continue
			}

			pv, err := opc.pvLister.Get(pvc.Spec.VolumeName)
			if err != nil {
				errs = append(errs, err)
				continue
			}

			pis = append(pis, &PVItem{
				PV:          pv,
				ServiceName: svcName,
			})
		}
	}

	return pis, requeueReasons, utilerrors.NewAggregate(errs)
}

func (opc *Controller) sync(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.ErrorS(err, "Failed to split meta namespace cache key", "cacheKey", key)
		return err
	}

	startTime := time.Now()
	klog.V(4).InfoS("Started syncing ScyllaCluster", "ScyllaCluster", klog.KRef(namespace, name), "startTime", startTime)
	defer func() {
		klog.V(4).InfoS("Finished syncing ScyllaCluster", "ScyllaCluster", klog.KRef(namespace, name), "duration", time.Since(startTime))
	}()

	sc, err := opc.scyllaLister.ScyllaClusters(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		klog.V(2).InfoS("ScyllaCluster has been deleted", "ScyllaCluster", klog.KObj(sc))
		return nil
	}
	if err != nil {
		return err
	}

	if sc.DeletionTimestamp != nil {
		return nil
	}

	if !sc.Spec.AutomaticOrphanedNodeCleanup {
		klog.V(4).InfoS("ScyllaCluster doesn't have AutomaticOrphanedNodeCleanup enabled", "ScyllaCluster", klog.KRef(namespace, name))
		return nil
	}

	nodes, err := opc.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}

	var errs []error

	pis, requeueReasons, err := opc.getPVsForScyllaCluster(ctx, sc)
	// Process at least some PVs even if there were errors retrieving the rest
	if err != nil {
		errs = append(errs, err)
	}

	for _, pi := range pis {
		orphaned, err := helpers.IsOrphanedPV(pi.PV, nodes)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if !orphaned {
			continue
		}

		klog.V(2).InfoS("PV is orphaned", "ScyllaCluster", sc, "PV", klog.KObj(pi.PV))

		// Verify that the node doesn't exist with a live call.
		freshNodes, err := opc.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}

		freshOrphaned, err := helpers.IsOrphanedPV(pi.PV, helpers.GetNodePointerArrayFromArray(freshNodes.Items))
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if !freshOrphaned {
			continue
		}

		klog.V(2).InfoS("PV is verified as orphaned.", "ScyllaCluster", sc, "PV", klog.KObj(pi.PV))

		backgroundPropagationPolicy := metav1.DeletePropagationBackground

		if pi.PV.Spec.ClaimRef == nil {
			continue
		}

		pvc := pi.PV.Spec.ClaimRef

		// First, Delete the PVC if it exists.
		// The PVC has finalizer protection so it will wait for the pod to be deleted.
		// We can't do it the other way around or the pod could be recreated before we delete the PVC.
		pvcMeta := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
			},
		}
		klog.V(2).InfoS("Deleting the PVC to replace member",
			"ScyllaCluster", klog.KObj(sc),
			"PVC", klog.KObj(pvcMeta),
		)
		err = opc.kubeClient.CoreV1().PersistentVolumeClaims(pvcMeta.Namespace).Delete(ctx, pvcMeta.Name, metav1.DeleteOptions{
			PropagationPolicy: &backgroundPropagationPolicy,
		})
		if err != nil {
			if apierrors.IsNotFound(err) {
				klog.V(4).InfoS("PVC not found", "PVC", klog.KObj(pvcMeta))
			} else {
				resourceapply.ReportDeleteEvent(opc.eventRecorder, pvcMeta, err)
				return err
			}
		}
		resourceapply.ReportDeleteEvent(opc.eventRecorder, pvcMeta, nil)

		// Hack: Sleeps are terrible thing to in sync logic but this compensates for a broken
		// StatefulSet controller in Kubernetes. StatefulSet doesn't reconcile PVCs and decides
		// it's presence only from its cache, and never recreates it if it's missing, only when it
		// creates the pod. This gives StatefulSet controller a chance to see the PVC was deleted
		// before deleting the pod.
		time.Sleep(10 * time.Second)

		// Evict the Pod if it exists.
		podMeta := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pvc.Namespace,
				Name:      strings.TrimPrefix(pvc.Name, "data-"),
			},
		}

		pod, err := opc.kubeClient.CoreV1().Pods(podMeta.Namespace).Get(ctx, podMeta.Name, metav1.GetOptions{})
		if err == nil && pod.Status.Phase == corev1.PodPending {
			klog.V(2).InfoS("Deleting the pending Pod to replace member",
				"ScyllaCluster", klog.KObj(sc),
				"Pod", klog.KObj(podMeta),
			)
			// TODO: Revert back to eviction when it's fixed in kubernetes 1.19.z (#732)
			//       (https://github.com/kubernetes/kubernetes/issues/103970)
			err = opc.kubeClient.CoreV1().Pods(podMeta.Namespace).Delete(ctx, podMeta.Name, metav1.DeleteOptions{
				PropagationPolicy: &backgroundPropagationPolicy,
			})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.V(4).InfoS("Pod not found", "Pod", klog.ObjectRef{Namespace: namespace, Name: strings.TrimPrefix(pvc.Name, "data-")})
				} else {
					resourceapply.ReportDeleteEvent(opc.eventRecorder, podMeta, err)
					return err
				}
			}
			resourceapply.ReportDeleteEvent(opc.eventRecorder, podMeta, nil)
		}
	}

	err = utilerrors.NewAggregate(errs)
	if err != nil {
		return err
	}

	if len(requeueReasons) > 0 {
		return helpers.NewRequeueError(requeueReasons...)
	}

	return nil
}
