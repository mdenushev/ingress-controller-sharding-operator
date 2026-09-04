package controller

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/go-logr/logr"
	networkingv1 "k8s.io/api/networking/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// Scheduler decides when a parent may apply changes to its shards, protecting
// the ingress controllers from config-reload storms.
type Scheduler interface {
	// NoteShard registers a shard so its rate-limit windows are tracked.
	NoteShard(shard string)
	// Schedule books the next apply slot for the parent. When the slot is
	// in the future it returns the ctrl.Result to requeue with and
	// handled=true; when the parent may proceed right now it returns
	// handled=false.
	Schedule(objKey string, status *controllerv1.ShardedStatus, shards []Shard, logger logr.Logger) (ctrl.Result, bool)
}

// applyPlan tracks the rate-limit windows of one shard.
type applyPlan struct {
	lastCreating               time.Time
	lastDeleting               time.Time
	currentDeletingWindowStart time.Time
	nextDeletingWindowStart    time.Time
}

// cooldownScheduler spaces creations and deletions on a shard by
// ShardUpdateCooldown and groups deletions into TerminationPeriod-sized
// windows. It is the pre-refactoring behavior kept behind the Scheduler
// interface; a fair per-shard queue can replace it without touching the
// engine.
type cooldownScheduler struct {
	terminationPeriod   time.Duration
	shardUpdateCooldown time.Duration
	nextApplyTime       map[string]*applyPlan
	tracker             *stateTracker
}

func newCooldownScheduler(terminationPeriod, shardUpdateCooldown time.Duration, tracker *stateTracker) *cooldownScheduler {
	return &cooldownScheduler{
		terminationPeriod:   terminationPeriod,
		shardUpdateCooldown: shardUpdateCooldown,
		nextApplyTime:       make(map[string]*applyPlan),
		tracker:             tracker,
	}
}

func (s *cooldownScheduler) NoteShard(shard string) {
	s.plan(shard).lastCreating = time.Now()
}

// plan returns the shard's window state, creating it lazily for shards that
// were not discovered at start-up (e.g. classes removed from the config while
// their children still exist).
func (s *cooldownScheduler) plan(shard string) *applyPlan {
	ap, exists := s.nextApplyTime[shard]
	if !exists {
		ap = &applyPlan{}
		s.nextApplyTime[shard] = ap
	}
	return ap
}

// applyAction is the kind of pending change classify detected.
type applyAction int

const (
	// actionNone: nothing to book, requeue immediately.
	actionNone applyAction = iota
	// actionCreate: children have to be created/updated on a shard.
	actionCreate
	// actionDelete: children have to be removed from an old shard.
	actionDelete
)

// Schedule classifies the pending change (creating on a new shard vs deleting
// from an old one) from the difference between the shards recorded in the
// status and the shards the parent should live on, then books a slot in the
// target shard's windows.
func (s *cooldownScheduler) Schedule(objKey string, status *controllerv1.ShardedStatus, shards []Shard, logger logr.Logger) (ctrl.Result, bool) {
	action, shard := s.classify(objKey, status, shards, logger)

	var result ctrl.Result
	switch action {
	case actionCreate:
		result = ctrl.Result{RequeueAfter: time.Until(s.bookCreateSlot(shard))}
	case actionDelete:
		result = ctrl.Result{RequeueAfter: time.Until(s.bookDeleteSlot(objKey, shard, logger))}
	default:
		result = ctrl.Result{Requeue: true}
	}

	s.tracker.markWaiting(objKey)
	return result, true
}

// classify compares the shards recorded in the status with the shards the
// parent must live on and decides which action the parent needs next, and on
// which shard.
func (s *cooldownScheduler) classify(objKey string, status *controllerv1.ShardedStatus, shards []Shard, logger logr.Logger) (applyAction, string) {
	if len(status.CreatedObjects) == 0 {
		// Nothing recorded yet: first creation goes through unthrottled.
		return actionNone, ""
	}

	var statusShards, applyShards []string
	for shard, v := range status.CreatedObjects {
		if len(v) > 0 {
			statusShards = append(statusShards, shard)
		}
	}
	for _, shard := range shards {
		applyShards = append(applyShards, shard.Name)
	}
	diffShard := difference(statusShards, applyShards)

	switch {
	case len(diffShard) == 0 && len(applyShards) == 1 && len(statusShards) == 0,
		len(statusShards) == 1 && len(applyShards) == 1 && applyShards[0] != statusShards[0],
		reflect.DeepEqual(diffShard, statusShards):
		// Everything recorded lives elsewhere: (re)create on the target shard.
		return actionCreate, applyShards[0]
	case len(diffShard) >= 1 && len(statusShards) > 1 && s.tracker.isManaged(objKey):
		// A managed parent has leftovers on shards it no longer targets.
		return actionDelete, diffShard[0]
	case !s.tracker.isManaged(objKey) && len(diffShard) != 0:
		// Not seen by this process yet: adopt by creating, never by deleting.
		return actionCreate, diffShard[0]
	case len(diffShard) == 0:
		return actionNone, ""
	default:
		logger.Info("Rescheduling", "use-all-class-shards object", objKey)
		return actionNone, ""
	}
}

// bookCreateSlot books the next creation slot on the shard, spacing bookings
// by the shard update cooldown, and resets the shard's deletion windows.
func (s *cooldownScheduler) bookCreateSlot(shard string) time.Time {
	ap := s.plan(shard)

	slot := time.Now().Add(1 * time.Second)
	if time.Now().Add(-s.shardUpdateCooldown).Before(ap.lastCreating) {
		// The shard was booked recently: create after its cooldown.
		slot = ap.lastCreating.Add(s.shardUpdateCooldown)
	}

	ap.lastCreating = slot
	ap.lastDeleting = slot
	ap.currentDeletingWindowStart = slot.Add(s.terminationPeriod)
	ap.nextDeletingWindowStart = slot.Add(s.terminationPeriod * 3)
	return slot
}

// bookDeleteSlot books the next deletion slot on the shard. Deletions are
// grouped into termination windows so an ingress controller reloads once per
// window instead of once per object.
func (s *cooldownScheduler) bookDeleteSlot(objKey, shard string, logger logr.Logger) time.Time {
	ap := s.plan(shard)
	slot := time.Now()

	// First deletion right after a creation booking: reuse its slot and
	// restart the windows from it.
	if slot.Add(-s.shardUpdateCooldown).Before(ap.lastCreating) && slot.After(ap.nextDeletingWindowStart) {
		slot = ap.lastCreating
		ap.lastCreating = slot.Add(s.shardUpdateCooldown)
		ap.lastDeleting = slot.Add(s.shardUpdateCooldown)
		ap.currentDeletingWindowStart = slot.Add(s.terminationPeriod)
		ap.nextDeletingWindowStart = slot.Add(s.terminationPeriod * 3)
		logger.Info("Print timings", "object", objKey, "last-creating", time.Until(ap.lastCreating), "last-deleting", time.Until(ap.lastDeleting), "current-deleting", time.Until(ap.currentDeletingWindowStart), "next-deleting", time.Until(ap.nextDeletingWindowStart))
	}

	if slot.Add(s.terminationPeriod * 2).Before(ap.nextDeletingWindowStart) {
		// Well inside the current window: chain after the last deletion.
		slot = ap.lastDeleting.Add(s.shardUpdateCooldown)
	} else if slot.Add(s.terminationPeriod).Before(ap.nextDeletingWindowStart) {
		// Too close to the window edge: open a new window and go there.
		slot = ap.nextDeletingWindowStart.Add(s.shardUpdateCooldown)
		ap.currentDeletingWindowStart = slot.Add(s.terminationPeriod)
		ap.nextDeletingWindowStart = slot.Add(s.terminationPeriod * 3)
		logger.Info("New termination window created and deleting later in new termination window", "object", objKey, "delay", time.Until(slot))
	} else {
		slot = time.Now().Add(1 * time.Second)
		logger.Info("Deleting now", "object", objKey, "delay", time.Until(slot))
	}

	ap.lastDeleting = slot
	ap.lastCreating = slot
	return slot
}

// discoverClusterShards counts the shard ingress classes present in the
// cluster and lowers maxShards where the cluster has fewer shards than the
// configuration asks for. It also registers every discovered shard with the
// scheduler.
func discoverClusterShards(ctx context.Context, c client.Client, maxShards map[string]int, scheduler Scheduler, logger logr.Logger) error {
	shardCounts := make(map[string]int)

	ingressClassList := &networkingv1.IngressClassList{}
	if err := c.List(ctx, ingressClassList); err != nil {
		return err
	}

	shardSuffixRegex := regexp.MustCompile(`-(\d+)$`)

	for _, ingressClass := range ingressClassList.Items {
		if shardSuffixRegex.MatchString(ingressClass.Name) {
			baseName := shardSuffixRegex.ReplaceAllString(ingressClass.Name, "")
			shardCounts[baseName]++
		}
		scheduler.NoteShard(ingressClass.Name)
	}

	for className, configShard := range maxShards {
		if count, exists := shardCounts[className]; exists {
			if count < configShard {
				logger.Info("Reducing shard count to match Cluster value", "IngressClass", className, "ConfiguredShards", configShard, "CurrentShards", count)
				maxShards[className] = count
			}
			for i := 0; i < configShard; i++ {
				scheduler.NoteShard(fmt.Sprintf("%s-%d", className, i))
			}
		} else {
			logger.Info("ClassName from maxShards not found in Cluster, setting to 0", "IngressClass", className)
			maxShards[className] = 0
			scheduler.NoteShard(className)
		}
	}
	return nil
}

func difference(a, b []string) []string {
	mb := make(map[string]bool)
	for _, x := range b {
		mb[x] = true
	}
	var diff []string
	for _, x := range a {
		if !mb[x] {
			diff = append(diff, x)
		}
	}
	return diff
}
