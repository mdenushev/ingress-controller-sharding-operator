// Package status holds the pure bookkeeping over the parent status
// (createdObjects records, phase, conditions) and the event reason constants
// mirrored to it. Everything here is side-effect free: the engine owns the
// cluster I/O around these helpers.
package status

import (
	"sort"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// Keys of the per-child records in status.createdObjects.
const (
	KeyKind = "kind"
	KeyName = "name"
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

	EventShardSelectionFailed = "ShardSelectionFailed"
)

// FindIn reports whether the child is recorded under the shard in
// createdObjects.
func FindIn(shard, kind, name string, createdObjects map[string][]map[string]string) bool {
	for _, obj := range createdObjects[shard] {
		if obj[KeyKind] == kind && obj[KeyName] == name {
			return true
		}
	}
	return false
}

// AppendChild records the child under the shard, dropping empty shard entries
// and keeping the shard's list sorted so readers and comparisons see a stable
// order. It returns the (possibly newly allocated) map and whether the record
// was added; added is false when the child was already recorded.
func AppendChild(
	createdObjects map[string][]map[string]string,
	shard, kind, name string,
) (map[string][]map[string]string, bool) {
	if FindIn(shard, kind, name, createdObjects) {
		return createdObjects, false
	}
	if createdObjects == nil {
		createdObjects = make(map[string][]map[string]string)
	}
	createdObjects[shard] = append(createdObjects[shard], map[string]string{KeyKind: kind, KeyName: name})

	for key, value := range createdObjects {
		if len(value) == 0 {
			delete(createdObjects, key)
		}
	}

	sort.Slice(createdObjects[shard], func(i, j int) bool {
		return createdObjects[shard][i][KeyName] < createdObjects[shard][j][KeyName]
	})
	return createdObjects, true
}

// RemoveChild drops every record of the named child from the status, cleaning
// up shard entries that become empty.
func RemoveChild(st *controllerv1.ShardedStatus, name string) {
	for key, valSlice := range st.CreatedObjects {
		for i, valMap := range valSlice {
			if valMap[KeyName] == name {
				st.CreatedObjects[key] = append(valSlice[:i], valSlice[i+1:]...)
				break
			}
		}
		if len(st.CreatedObjects[key]) == 0 {
			delete(st.CreatedObjects, key)
		}
	}
}

// Condition builds a condition of the given type in the True or False state.
func Condition(condType string, isTrue bool, reason, message string) metav1.Condition {
	condStatus := metav1.ConditionFalse
	if isTrue {
		condStatus = metav1.ConditionTrue
	}
	return metav1.Condition{
		Type:    condType,
		Status:  condStatus,
		Reason:  reason,
		Message: message,
	}
}

// ApplyLifecycle sets the phase, observedGeneration and both conditions on the
// status and reports whether anything actually changed, so callers can skip
// no-op status updates.
func ApplyLifecycle(
	st *controllerv1.ShardedStatus,
	generation int64,
	phase controllerv1.ShardedPhase,
	ready, resharding metav1.Condition,
) bool {
	changed := false

	if st.Phase != phase {
		st.Phase = phase
		changed = true
	}
	if st.ObservedGeneration != generation {
		st.ObservedGeneration = generation
		changed = true
	}
	ready.ObservedGeneration = generation
	resharding.ObservedGeneration = generation
	if apimeta.SetStatusCondition(&st.Conditions, ready) {
		changed = true
	}
	if apimeta.SetStatusCondition(&st.Conditions, resharding) {
		changed = true
	}
	return changed
}
