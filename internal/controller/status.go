package controller

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// Event reasons emitted on the parent objects.
const (
	EventChildCreated      = "ChildCreated"
	EventChildUpdated      = "ChildUpdated"
	EventChildDeleted      = "ChildDeleted"
	EventTmpChildCreated   = "TmpChildCreated"
	EventMarkedForDeletion = "MarkedForDeletion"
	EventDeletionScheduled = "DeletionScheduled"
	EventApplyScheduled    = "ApplyScheduled"
	EventReshardingStarted = "ReshardingStarted"
	EventFinalizerDraining = "FinalizerDraining"
	EventFinalizerRemoved  = "FinalizerRemoved"
	EventChildBuildFailed  = "ChildBuildFailed"
	EventChildApplyFailed  = "ChildApplyFailed"
)

// eventf records a Normal kube event on the parent when a recorder is wired.
func (e *Engine[C]) eventf(s *scope, reason, format string, args ...interface{}) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.Eventf(s.obj, corev1.EventTypeNormal, reason, format, args...)
}

// warnf records a Warning kube event on the parent when a recorder is wired.
func (e *Engine[C]) warnf(s *scope, reason, format string, args ...interface{}) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.Eventf(s.obj, corev1.EventTypeWarning, reason, format, args...)
}

func findInStatus(shard, kind, name string, createdObjects *map[string][]map[string]string) bool {
	if objList, ok := (*createdObjects)[shard]; ok {
		for _, obj := range objList {
			if obj["kind"] == kind && obj["name"] == name {
				return true
			}
		}
	}
	return false
}

// updateStatusWithRetry runs mutate (which must modify s.obj and push the
// status) and retries on version conflicts, refreshing the object in between
// so mutate re-applies on top of the latest version.
func (e *Engine[C]) updateStatusWithRetry(s *scope, mutate func() error) error {
	maxRetries := 5
	var err error
	for i := 0; i < maxRetries; i++ {
		err = mutate()
		if err == nil {
			return nil
		}
		if !errors.IsConflict(err) {
			return err
		}
		// The object has been modified by someone else, fetch the latest
		// version and try again.
		if getErr := e.Get(s.ctx, types.NamespacedName{Name: s.obj.GetName(), Namespace: s.obj.GetNamespace()}, s.obj); getErr != nil {
			return getErr
		}
	}
	return err
}

// addChildToStatus records the child under the shard in the parent status and
// keeps the per-class child metric in sync.
func (e *Engine[C]) addChildToStatus(s *scope, kind, name, shardName string) error {
	if shardName == "" || kind == "" || name == "" {
		return nil
	}
	status := s.obj.GetShardedStatus()
	if !findInStatus(shardName, kind, name, &status.CreatedObjects) {
		err := e.updateStatusWithRetry(s, func() error {
			createdObjects := s.obj.GetShardedStatus().CreatedObjects
			if findInStatus(shardName, kind, name, &createdObjects) {
				return errors.NewAlreadyExists(schema.GroupResource{}, shardName)
			}
			if createdObjects == nil {
				createdObjects = make(map[string][]map[string]string)
			}
			createdObjects[shardName] = append(createdObjects[shardName], map[string]string{"kind": kind, "name": name})

			// Clean up empty entries
			for key, value := range createdObjects {
				if len(value) == 0 {
					delete(createdObjects, key)
				}
			}

			// Keep the list stable for readers and comparisons.
			sort.Slice(createdObjects[shardName], func(i, j int) bool {
				return createdObjects[shardName][i]["name"] < createdObjects[shardName][j]["name"]
			})

			s.obj.GetShardedStatus().CreatedObjects = createdObjects
			return e.Status().Update(s.ctx, s.obj)
		})
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
	}
	e.tracker.noteChildClass(s.key, shardName)
	return nil
}

// removeChildFromStatus drops every record of the named child from the parent
// status.
func (e *Engine[C]) removeChildFromStatus(s *scope, name string) error {
	return e.updateStatusWithRetry(s, func() error {
		status := s.obj.GetShardedStatus()
		for key, valSlice := range status.CreatedObjects {
			for i, valMap := range valSlice {
				if valMap["name"] == name {
					status.CreatedObjects[key] = append(valSlice[:i], valSlice[i+1:]...)
					break
				}
			}
			if len(status.CreatedObjects[key]) == 0 {
				delete(status.CreatedObjects, key)
			}
		}
		return e.Status().Update(s.ctx, s.obj)
	})
}

// setLifecycle publishes the phase, conditions and observedGeneration on the
// parent status. It writes only when something actually changed to avoid
// update storms.
func (e *Engine[C]) setLifecycle(s *scope, phase controllerv1.ShardedPhase, ready, resharding metav1.Condition) error {
	return e.updateStatusWithRetry(s, func() error {
		status := s.obj.GetShardedStatus()
		changed := false

		if status.Phase != phase {
			status.Phase = phase
			changed = true
		}
		if status.ObservedGeneration != s.obj.GetGeneration() {
			status.ObservedGeneration = s.obj.GetGeneration()
			changed = true
		}
		ready.ObservedGeneration = s.obj.GetGeneration()
		resharding.ObservedGeneration = s.obj.GetGeneration()
		if apimeta.SetStatusCondition(&status.Conditions, ready) {
			changed = true
		}
		if apimeta.SetStatusCondition(&status.Conditions, resharding) {
			changed = true
		}

		if !changed {
			return nil
		}
		return e.Status().Update(s.ctx, s.obj)
	})
}

func condition(condType string, isTrue bool, reason, message string) metav1.Condition {
	status := metav1.ConditionFalse
	if isTrue {
		status = metav1.ConditionTrue
	}
	return metav1.Condition{
		Type:    condType,
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}
