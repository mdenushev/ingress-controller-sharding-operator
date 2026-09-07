package controller

import (
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
)

func (e *Engine[C]) reconcileTerminating(s *scope) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)

	if err := e.setLifecycle(s, controllerv1.PhaseTerminating,
		condition(controllerv1.ConditionReady, false, "Terminating", "Parent is being deleted, children are draining"),
		condition(controllerv1.ConditionResharding, false, "Terminating", "Parent is being deleted")); err != nil {
		logger.Error(err, "[finalizer] unable to publish terminating status")
	}

	childrenList, err := e.listChildren(s)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cannot get children list: %w", err)
	}

	// step 1: get object children
	// step 2: mark all children for deletion, set unregister mark instantly
	// step 3: check which children should be deleted now, delete them
	// step 4: if no children left waiting for deletion - remove finalizer,
	//         otherwise requeue after FinalizerDeletionTerminationPeriod
	waitingForDeletion := 0
	for _, child := range childrenList.Items {
		waiting, drainErr := e.drainChild(s, &child)
		if drainErr != nil {
			return ctrl.Result{}, drainErr
		}
		if waiting {
			waitingForDeletion++
		}
	}

	if waitingForDeletion > 0 {
		return ctrl.Result{RequeueAfter: e.Settings.FinalizerDeletionTerminationPeriod}, nil
	}
	return e.removeFinalizer(s)
}

// drainChild walks one child of a deleted parent through the drain steps:
// mark it for unregistering first, delete it once its window passed. waiting
// is true while the child still exists and its window has not passed.
func (e *Engine[C]) drainChild(s *scope, child *unstructured.Unstructured) (bool, error) {
	logger := log.FromContext(s.ctx)

	var shardName string
	for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if objStatus[statusKeyName] == child.GetName() {
				shardName = shard
			}
		}
	}

	deleteAfter, deleteAfterExists, err := parseDeleteAfterAnnotation(child)
	if err != nil {
		logger.Error(
			err,
			"[finalizer] unable to parse auto-delete-after annotation",
			"objectKind",
			child.GetKind(),
			"objectName",
			child.GetName(),
		)
		return false, fmt.Errorf("[finalizer] cannot parse delete after annotation: %w", err)
	}

	if !deleteAfterExists || !e.clock.isMarkedForUnregistering(child) {
		logger.Info(
			"[finalizer] mark child for deletion",
			"objectKind",
			child.GetKind(),
			"objectName",
			child.GetName(),
		)
		e.clock.markForUnregistering(child)
		setDeleteAfterAnnotation(child, e.Settings.FinalizerDeletionTerminationPeriod)

		if updateErr := e.Update(s.ctx, child); updateErr != nil {
			logger.Error(
				updateErr,
				"[finalizer] unable to set auto-delete-after and unregister annotation on child",
				"objectKind",
				child.GetKind(),
				"objectName",
				child.GetName(),
			)
			return false, fmt.Errorf(
				"[finalizer] unable to set auto-delete-after and unregister annotation on child: %w",
				updateErr,
			)
		}
		e.eventf(
			s,
			EventFinalizerDraining,
			"Draining child %s %s before deletion",
			child.GetKind(),
			child.GetName(),
		)
		return true, nil
	}

	if !time.Now().After(deleteAfter) {
		return true, nil
	}

	logger.Info("[finalizer] deleting child", "objectKind", child.GetKind(), "objectName", child.GetName())
	if deleteErr := e.Delete(s.ctx, child); deleteErr != nil {
		logger.Error(
			deleteErr,
			"[finalizer] unable to delete child",
			"objectKind",
			child.GetKind(),
			"objectName",
			child.GetName(),
		)
		return false, deleteErr
	}
	logger.Info(
		"[finalizer] successfully deleted child from cluster",
		"objectKind",
		child.GetKind(),
		"objectName",
		child.GetName(),
	)
	e.eventf(s, EventChildDeleted, "Deleted child %s %s", child.GetKind(), child.GetName())
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
	return false, nil
}

// removeFinalizer refreshes the parent and removes the controller finalizer
// once every child is drained and deleted.
func (e *Engine[C]) removeFinalizer(s *scope) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)

	if err := e.Get(s.ctx, s.req.NamespacedName, s.obj); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("[finalizer] object not found, finalizer removal skipped")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("[finalizer] failed to refresh object: %w", err)
	}

	controllerutil.RemoveFinalizer(s.obj, e.Settings.FinalizerKey)
	if err := e.Update(s.ctx, s.obj); err != nil {
		if apierrors.IsConflict(err) {
			logger.Info("[finalizer] version conflict during finalizer removal, requeueing")
			return ctrl.Result{Requeue: true}, nil
		}
		if apierrors.IsNotFound(err) {
			logger.Info("[finalizer] object not found after finalizer removal, finalizer loop skipped")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("cannot remove finalizer: %w", err)
	}
	logger.Info("successfully removed finalizer from object")
	e.eventf(s, EventFinalizerRemoved, "All children drained, finalizer removed")
	return ctrl.Result{}, nil
}

// publishLifecycle mirrors the outcome of the pass into the parent status.
