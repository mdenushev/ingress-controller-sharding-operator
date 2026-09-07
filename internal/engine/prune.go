package engine

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
	"k8s.tochka.com/sharded-ingress-controller/internal/status"
)

// pruneChildren walks the live children and schedules the ones that are no
// longer desired for graceful deletion (unregister from service discovery
// first, delete after the termination window). It also drops status records
// whose objects are gone.
func (e *Engine[C]) pruneChildren(s *scope, currentList map[string][]map[string]string) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)

	childObjs, err := e.listChildren(s)
	if err != nil {
		logger.Error(err, "unable to list child objects")
		return ctrl.Result{}, err
	}

	for _, obj := range childObjs.Items {
		// A child that is still desired just stays recorded in the status —
		// except tmp children, which always run their deletion timeline.
		desired, shardName := desiredIn(s.shards, &obj, currentList)
		if desired && !isTmpChildName(s.obj.GetName(), obj.GetName()) {
			if statusErr := e.addChildToStatus(s, obj.GetKind(), obj.GetName(), shardName); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			continue
		}

		result, done, pruneErr := e.pruneChild(s, &obj, shardName)
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

// desiredIn reports whether the child is in the pass's desired list, and on
// which shard.
func desiredIn(
	shards []Shard,
	obj *unstructured.Unstructured,
	currentList map[string][]map[string]string,
) (bool, string) {
	for _, shard := range shards {
		if status.FindIn(shard.Name, obj.GetKind(), obj.GetName(), currentList) {
			return true, shard.Name
		}
	}
	return false, ""
}

// pruneChild walks one no-longer-desired (or tmp) child through the graceful
// deletion steps. shardName is the metrics/bookkeeping label, overridden by
// the shard recorded in the status when there is one. done is true when the
// pass must end with the returned result.
func (e *Engine[C]) pruneChild(
	s *scope,
	obj *unstructured.Unstructured,
	shardName string,
) (ctrl.Result, bool, error) {
	logger := log.FromContext(s.ctx)

	if recorded := e.recordedShard(s, obj.GetName()); recorded != "" {
		shardName = recorded
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
		e.eventf(s, status.EventChildDeleted, "Deleted %s %s", obj.GetKind(), obj.GetName())
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

// recordedShard returns the shard the named child is recorded under in the
// parent status, "" when it is not recorded.
func (e *Engine[C]) recordedShard(s *scope, name string) string {
	recorded := ""
	for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if objStatus[status.KeyName] == name {
				recorded = shard
			}
		}
	}
	return recorded
}

// dropStaleStatusRecords removes status records whose objects no longer exist
// in the cluster.
func (e *Engine[C]) dropStaleStatusRecords(s *scope, currentList map[string][]map[string]string) error {
	logger := log.FromContext(s.ctx)

	for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if status.FindIn(shard, objStatus[status.KeyKind], objStatus[status.KeyName], currentList) {
				continue
			}
			obj := &unstructured.Unstructured{}
			obj.SetKind(objStatus[status.KeyKind])
			obj.SetAPIVersion(s.obj.GetObject().GetObjectKind().GroupVersionKind().Version)
			obj.SetNamespace(s.obj.GetNamespace())
			obj.SetName(objStatus[status.KeyName])
			if getErr := e.Get(
				s.ctx,
				client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()},
				obj,
			); getErr != nil {
				// If it does not exist, delete the object from the status.
				if removeErr := e.removeChildFromStatus(s, objStatus[status.KeyName]); removeErr != nil {
					logger.Error(
						removeErr,
						"unable to update status",
						"objectKind",
						s.obj.GetKind(),
						"objectName",
						objStatus[status.KeyName],
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
	deleteAfterTime, deleteAfterExists, err := parseDeleteAfterAnnotation(obj)
	if err != nil {
		log.FromContext(s.ctx).Error(
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
	marked := e.clock.isMarkedForUnregistering(obj)

	if !deleteAfterExists {
		return false, e.scheduleChildDeletion(s, obj, shardName, delTime)
	}
	if time.Now().After(deleteAfterTime) && marked {
		// The deadline passed and service discovery dropped the child.
		return true, nil
	}
	if time.Now().After(deleteAfterTime.Add(-e.Settings.TerminationPeriod)) && !marked {
		return false, e.markChildForUnregistering(s, obj, shardName, delTime)
	}
	return false, nil
}

// scheduleChildDeletion opens the child's deletion timeline: it stamps
// auto-delete-after delTime out and announces the schedule.
func (e *Engine[C]) scheduleChildDeletion(
	s *scope,
	obj *unstructured.Unstructured,
	shardName string,
	delTime time.Duration,
) error {
	logger := log.FromContext(s.ctx)

	setDeleteAfterAnnotation(obj, delTime)
	if err := e.Update(s.ctx, obj); err != nil {
		logger.Error(
			err,
			"unable to update auto-delete-after annotation",
			"objectKind",
			obj.GetKind(),
			"objectName",
			obj.GetName(),
		)
		return err
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
		status.EventDeletionScheduled,
		"Scheduled %s %s for deletion at %s",
		obj.GetKind(),
		obj.GetName(),
		obj.GetAnnotations()[AutoDeleteAfterAnnotation],
	)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
	e.tracker.MarkWaiting(s.key)
	s.mutated = true
	return nil
}

// markChildForUnregistering flags the child for service discovery removal one
// termination period before its deadline and restarts the deadline delTime
// out, giving service discovery a full window to converge before the delete.
func (e *Engine[C]) markChildForUnregistering(
	s *scope,
	obj *unstructured.Unstructured,
	shardName string,
	delTime time.Duration,
) error {
	logger := log.FromContext(s.ctx)

	e.clock.markForUnregistering(obj)
	setDeleteAfterAnnotation(obj, delTime)
	if err := e.Update(s.ctx, obj); err != nil {
		logger.Error(
			err,
			"unable to update object with marked-for-deletion annotation",
			"objectKind",
			obj.GetKind(),
			"objectName",
			obj.GetName(),
		)
		return err
	}
	logger.Info("marked-for-deletion annotation set", "objectKind", obj.GetKind(), "objectName", obj.GetName())
	e.eventf(
		s,
		status.EventMarkedForDeletion,
		"Marked %s %s for service discovery unregistering",
		obj.GetKind(),
		obj.GetName(),
	)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
	s.mutated = true
	return nil
}
