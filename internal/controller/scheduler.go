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

// Schedule classifies the pending change (creating on a new shard vs deleting
// from an old one) from the difference between the shards recorded in the
// status and the shards the parent should live on, then books a slot in the
// target shard's windows.
func (s *cooldownScheduler) Schedule(objKey string, status *controllerv1.ShardedStatus, shards []Shard, logger logr.Logger) (ctrl.Result, bool) {
	lastUpdatingTime := time.Now()
	lastDeletingTime := time.Now()
	var diffShard, statusShards, applyShards []string
	var currentShard string
	var creating, deleting bool

	createdObjects := status.CreatedObjects
	if len(createdObjects) > 0 {
		for shard, v := range createdObjects {
			if len(v) > 0 {
				statusShards = append(statusShards, shard)
			}
		}
		for _, shard := range shards {
			applyShards = append(applyShards, shard.Name)
		}
		diffShard = difference(statusShards, applyShards)
		if len(diffShard) == 0 && len(applyShards) == 1 && len(statusShards) == 0 ||
			(len(statusShards) == 1 && len(applyShards) == 1 && applyShards[0] != statusShards[0]) ||
			reflect.DeepEqual(diffShard, statusShards) {
			currentShard = applyShards[0]
			creating = true
		} else if len(diffShard) >= 1 && len(statusShards) > 1 && s.tracker.isManaged(objKey) {
			currentShard = diffShard[0]
			deleting = true
		} else if !s.tracker.isManaged(objKey) && len(diffShard) != 0 {
			currentShard = diffShard[0]
			creating = true
		} else if len(diffShard) == 0 {
			s.tracker.markWaiting(objKey)
			return ctrl.Result{Requeue: true}, true
		} else {
			logger.Info("Rescheduling", "use-all-class-shards object", objKey)
			s.tracker.markWaiting(objKey)
			return ctrl.Result{Requeue: true}, true
		}
	} else {
		s.tracker.markWaiting(objKey)
	}

	if creating {
		ap := s.plan(currentShard)
		// check for future updates
		if lastUpdatingTime.Add(-s.shardUpdateCooldown).Before(ap.lastCreating) {
			// creating later
			lastUpdatingTime = ap.lastCreating.Add(s.shardUpdateCooldown)
		} else {
			// last creating time is in the past, creating now
			lastUpdatingTime = time.Now().Add(1 * time.Second)
		}
		ap.lastCreating = lastUpdatingTime
		ap.lastDeleting = lastUpdatingTime
		ap.currentDeletingWindowStart = lastUpdatingTime.Add(s.terminationPeriod)
		ap.nextDeletingWindowStart = lastUpdatingTime.Add(s.terminationPeriod * 3)
		s.tracker.markWaiting(objKey)
		return ctrl.Result{RequeueAfter: time.Until(lastUpdatingTime)}, true
	}

	if deleting {
		ap := s.plan(currentShard)
		// check for first deletion on shard
		if lastDeletingTime.Add(-s.shardUpdateCooldown).Before(ap.lastCreating) && lastDeletingTime.After(ap.nextDeletingWindowStart) {
			lastDeletingTime = ap.lastCreating
			ap.lastCreating = lastDeletingTime.Add(s.shardUpdateCooldown)
			ap.lastDeleting = lastDeletingTime.Add(s.shardUpdateCooldown)
			ap.currentDeletingWindowStart = lastDeletingTime.Add(s.terminationPeriod)
			ap.nextDeletingWindowStart = lastDeletingTime.Add(s.terminationPeriod * 3)
			logger.Info("Print timings", "object", objKey, "last-creating", time.Until(ap.lastCreating), "last-deleting", time.Until(ap.lastDeleting), "current-deleting", time.Until(ap.currentDeletingWindowStart), "next-deleting", time.Until(ap.nextDeletingWindowStart))
		}

		if lastDeletingTime.Add(s.terminationPeriod * 2).Before(ap.nextDeletingWindowStart) {
			lastDeletingTime = ap.lastDeleting.Add(s.shardUpdateCooldown)
		} else if lastDeletingTime.Add(s.terminationPeriod).Before(ap.nextDeletingWindowStart) {
			lastDeletingTime = ap.nextDeletingWindowStart.Add(s.shardUpdateCooldown)
			ap.currentDeletingWindowStart = lastDeletingTime.Add(s.terminationPeriod)
			ap.nextDeletingWindowStart = lastDeletingTime.Add(s.terminationPeriod * 3)
			logger.Info("New termination window created and deleting later in new termination window", "object", objKey, "delay", time.Until(lastDeletingTime))
		} else {
			lastDeletingTime = time.Now().Add(1 * time.Second)
			logger.Info("Deleting now", "object", objKey, "delay", time.Until(lastDeletingTime))
		}
		ap.lastDeleting = lastDeletingTime
		ap.lastCreating = lastDeletingTime
		s.tracker.markWaiting(objKey)
		return ctrl.Result{RequeueAfter: time.Until(lastDeletingTime)}, true
	}

	s.tracker.markWaiting(objKey)
	return ctrl.Result{Requeue: true}, true
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
