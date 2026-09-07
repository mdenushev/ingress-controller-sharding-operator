package controller

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
)

func (e *Engine[C]) pruneChildren(s *scope, currentList map[string][]map[string]string) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)

	childObjs, err := e.listChildren(s)
	if err != nil {
		logger.Error(err, "unable to list child objects")
		return ctrl.Result{}, err
	}

	for _, obj := range childObjs.Items {
		result, done, pruneErr := e.pruneChild(s, &obj, currentList)
		if pruneErr != nil {
			return ctrl.Result{}, pruneErr
		}
		if done {
			// A mutation or a pending deletion window ended the pass.
			return result, nil
		}
	}

	if staleErr := e.dropStaleStatusRecords(s, currentList); staleErr != nil {
		return ctrl.Result{}, staleErr
	}

	e.tracker.markReady(s.key)
	return ctrl.Result{}, nil
}

// pruneChild keeps a desired child recorded in the status, and walks a child
// that is no longer desired (or is a tmp child, which always runs its
// timeline) through the graceful deletion steps. done is true when the pass
// must end with the returned result.
func (e *Engine[C]) pruneChild(
	s *scope,
	obj *unstructured.Unstructured,
	currentList map[string][]map[string]string,
) (ctrl.Result, bool, error) {
	logger := log.FromContext(s.ctx)

	keep := false
	var shardName string
	for _, shard := range s.shards {
		if findInStatus(shard.Name, obj.GetKind(), obj.GetName(), &currentList) {
			keep = true
			shardName = shard.Name
			break
		}
	}

	// tmp children always run their deletion timeline, kept or not.
	if keep && !isTmpChildName(s.obj.GetName(), obj.GetName()) {
		return ctrl.Result{}, false, e.addChildToStatus(s, obj.GetKind(), obj.GetName(), shardName)
	}

	for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if objStatus[statusKeyName] == obj.GetName() {
				shardName = shard
			}
		}
	}
	shouldDelete, err := e.evaluateDeletionTiming(s, obj, shardName)
	if err != nil {
		logger.Error(err, "error handling deletion timing", "objectKind", obj.GetKind(), "objectName", obj.GetName())
		return ctrl.Result{}, false, err
	}
	if shouldDelete {
		if deleteErr := e.Delete(s.ctx, obj); deleteErr != nil {
			logger.Error(deleteErr, "unable to delete", "objectKind", obj.GetKind(), "objectName", obj.GetName())
			return ctrl.Result{}, false, deleteErr
		}
		logger.Info("successfully deleted from cluster", "objectKind", obj.GetKind(), "objectName", obj.GetName())
		e.eventf(s, EventChildDeleted, "Deleted %s %s", obj.GetKind(), obj.GetName())
		metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
		s.mutated = true
		return ctrl.Result{}, true, nil
	}
	// The child waits for its deletion window; keep it recorded and come
	// back when the window may have passed.
	if statusErr := e.addChildToStatus(s, obj.GetKind(), obj.GetName(), shardName); statusErr != nil {
		return ctrl.Result{}, false, statusErr
	}
	return ctrl.Result{RequeueAfter: e.Settings.TerminationPeriod}, true, nil
}

// dropStaleStatusRecords removes status records whose objects no longer exist
// in the cluster.
func (e *Engine[C]) dropStaleStatusRecords(s *scope, currentList map[string][]map[string]string) error {
	logger := log.FromContext(s.ctx)

	for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if findInStatus(shard, objStatus[statusKeyKind], objStatus[statusKeyName], &currentList) {
				continue
			}
			obj := &unstructured.Unstructured{}
			obj.SetKind(objStatus[statusKeyKind])
			obj.SetAPIVersion(s.obj.GetObject().GetObjectKind().GroupVersionKind().Version)
			obj.SetNamespace(s.obj.GetNamespace())
			obj.SetName(objStatus[statusKeyName])
			if getErr := e.Get(
				s.ctx,
				client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()},
				obj,
			); getErr != nil {
				// If it does not exist, delete the object from the status.
				if removeErr := e.removeChildFromStatus(s, objStatus[statusKeyName]); removeErr != nil {
					logger.Error(
						removeErr,
						"unable to update status",
						"objectKind",
						s.obj.GetKind(),
						"objectName",
						objStatus[statusKeyName],
					)
					return removeErr
				}
			}
			e.tracker.markReady(s.key)
		}
	}
	return nil
}

// evaluateDeletionTiming drives the graceful deletion timeline of one child:
// first stamp auto-delete-after, then mark for service discovery
// unregistering one termination period before the deadline, and only report
// shouldDelete once the deadline passed.
func (e *Engine[C]) evaluateDeletionTiming(
	s *scope,
	obj *unstructured.Unstructured,
	shardName string,
) (bool, error) {
	logger := log.FromContext(s.ctx)

	deleteAfterTime, deleteAfterExists, err := parseDeleteAfterAnnotation(obj)
	if err != nil {
		logger.Error(
			err,
			"unable to parse auto-delete-after annotation",
			"objectKind",
			obj.GetKind(),
			"objectName",
			obj.GetName(),
		)
		return false, err
	}

	delTime := e.Settings.TerminationPeriod * regularChildDeleteWindows
	if isTmpChildName(s.obj.GetName(), obj.GetName()) {
		delTime = e.Settings.TerminationPeriod * tmpChildDeleteWindows
	}
	markedForDeletion := e.clock.isMarkedForUnregistering(obj)

	if deleteAfterExists {
		if time.Now().After(deleteAfterTime) && markedForDeletion {
			// Time to delete
			return true, nil
		}

		timeBeforeUnregister := deleteAfterTime.Add(-e.Settings.TerminationPeriod)
		if time.Now().After(timeBeforeUnregister) && !markedForDeletion {
			e.clock.markForUnregistering(obj)
			setDeleteAfterAnnotation(obj, delTime)

			if updateErr := e.Update(s.ctx, obj); updateErr != nil {
				logger.Error(
					updateErr,
					"unable to update object with marked-for-deletion annotation",
					"objectKind",
					obj.GetKind(),
					"objectName",
					obj.GetName(),
				)
				return false, updateErr
			}
			logger.Info("marked-for-deletion annotation set", "objectKind", obj.GetKind(), "objectName", obj.GetName())
			e.eventf(
				s,
				EventMarkedForDeletion,
				"Marked %s %s for service discovery unregistering",
				obj.GetKind(),
				obj.GetName(),
			)
			metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
			s.mutated = true
		}

		return false, nil
	}

	setDeleteAfterAnnotation(obj, delTime)
	if updateErr := e.Update(s.ctx, obj); updateErr != nil {
		logger.Error(
			updateErr,
			"unable to update auto-delete-after annotation",
			"objectKind",
			obj.GetKind(),
			"objectName",
			obj.GetName(),
		)
		return false, updateErr
	}
	logger.Info(
		"auto-delete-after annotation set",
		"objectKind",
		obj.GetKind(),
		"objectName",
		obj.GetName(),
		"auto-delete-after",
		obj.GetAnnotations()[AutoDeleteAfterAnnotation],
	)
	e.eventf(
		s,
		EventDeletionScheduled,
		"Scheduled %s %s for deletion at %s",
		obj.GetKind(),
		obj.GetName(),
		obj.GetAnnotations()[AutoDeleteAfterAnnotation],
	)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
	e.tracker.markWaiting(s.key)
	s.mutated = true
	return false, nil
}

// reconcileTerminating drains the children of a deleted parent: every child
// is marked for service discovery unregistering, deleted after its window,
// and only then the finalizer is removed.
