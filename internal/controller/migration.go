package controller

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// Resharding moves the children of a parent from one shard to another without
// dropping traffic. The timeline is driven by two child annotations and the
// base window T (Settings.TerminationPeriod):
//
//	t0     a tmp child is created on the OLD shard and scheduled to be
//	       deleted at t0+3T (auto-delete-after); the main child keeps the
//	       old class.
//	t0+T   the main child switches to the NEW shard; the tmp child keeps
//	       serving the old shard while DNS/service discovery converges.
//	t0+2T  the tmp child is marked for service discovery unregistering.
//	t0+3T  the tmp child is deleted.
const (
	// AutoDeleteAfterAnnotation schedules a child for deletion at the
	// recorded RFC3339 time.
	AutoDeleteAfterAnnotation = "auto-delete-after"
	// OldShardAnnotation records on a tmp child which shard the children
	// are migrating from.
	OldShardAnnotation = "old-shard"

	tmpNameSuffix = "tmp"
)

// tmpChildName is the name of the tmp child that keeps the old shard alive
// during a migration of child number shardNumber.
func tmpChildName(parentName string, shardNumber int) string {
	return fmt.Sprintf("%s-%d-%s", parentName, shardNumber, tmpNameSuffix)
}

// isTmpChildName reports whether the object name belongs to a tmp child of
// the given parent.
func isTmpChildName(parentName, objName string) bool {
	return strings.HasPrefix(objName, parentName) && strings.HasSuffix(objName, tmpNameSuffix)
}

// reshardingConflict returns the shard the child is recorded on when it
// differs from newShard, i.e. when a migration is needed. It returns "" when
// the child is already recorded on newShard or not recorded at all.
func reshardingConflict(status *controllerv1.ShardedStatus, newShard, childName string) string {
	oldShard := ""
	for shard, objs := range status.CreatedObjects {
		for _, obj := range objs {
			if obj["name"] != childName {
				continue
			}
			if shard == newShard {
				// Already recorded on the target shard: no migration,
				// whatever other shards still list the child.
				return ""
			}
			if oldShard == "" {
				oldShard = shard
			}
		}
	}
	return oldShard
}

// migrationClock evaluates the migration timeline annotations.
type migrationClock struct {
	terminationPeriod    time.Duration
	unregisterAnnotation string
}

// migrationHold is the verdict of holdOldShard over a live tmp child.
type migrationHold struct {
	// Active is true while the main child must keep the old shard: the
	// migration window has not passed yet (or has not even started).
	Active bool
	// OldShard is the shard recorded on the tmp child; set only when
	// Active is true.
	OldShard string
}

// holdOldShard inspects a live tmp child and decides whether the main child
// must still keep the old shard. The hold ends (Active=false) once the
// migration window has passed or the annotations are absent, letting the main
// child switch to the new shard.
func (m migrationClock) holdOldShard(annotations map[string]string) migrationHold {
	if deleteAfterStr, exists := annotations[AutoDeleteAfterAnnotation]; exists {
		deleteAfterTime, err := time.Parse(time.RFC3339, deleteAfterStr)
		if err != nil {
			return migrationHold{}
		}
		// The tmp child dies at t0+3T; the main child holds the old
		// shard until t0+T, i.e. while now < deleteAfter-2T.
		timeBeforeChange := deleteAfterTime.Add(-m.terminationPeriod * 2)
		if !time.Now().After(timeBeforeChange) {
			if oldShard, exists := annotations[OldShardAnnotation]; exists {
				return migrationHold{Active: true, OldShard: oldShard}
			}
		}
	} else if oldShard, exists := annotations[OldShardAnnotation]; exists {
		// The deletion pass has not stamped the tmp child yet: the
		// migration clock has not started, keep the old shard.
		return migrationHold{Active: true, OldShard: oldShard}
	}
	return migrationHold{}
}

func parseDeleteAfterAnnotation(obj *unstructured.Unstructured) (deleteAfter time.Time, exists bool, err error) {
	if obj.GetAnnotations() == nil {
		return time.Time{}, false, nil
	}

	deleteAfterStr, exists := obj.GetAnnotations()[AutoDeleteAfterAnnotation]
	if !exists {
		return time.Time{}, false, nil
	}
	deleteAfter, err = time.Parse(time.RFC3339, deleteAfterStr)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("unable to parse auto-delete-after annotation as RFC3339: %w", err)
	}
	return deleteAfter, true, nil
}

func setDeleteAfterAnnotation(obj *unstructured.Unstructured, period time.Duration) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AutoDeleteAfterAnnotation] = time.Now().Add(period).UTC().Format(time.RFC3339)
	obj.SetAnnotations(annotations)
}

// markForUnregistering flags the child so service discovery drops it before
// the object is deleted.
func (m migrationClock) markForUnregistering(obj *unstructured.Unstructured) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[m.unregisterAnnotation] = "true"
	obj.SetAnnotations(annotations)
}

func (m migrationClock) isMarkedForUnregistering(obj *unstructured.Unstructured) bool {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false
	}
	return annotations[m.unregisterAnnotation] == "true"
}
