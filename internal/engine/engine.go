package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/scheduler"
	"k8s.tochka.com/sharded-ingress-controller/internal/status"
)

//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedingresses/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedingresses/finalizers,verbs=update
//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedhttpproxies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedhttpproxies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=networking.k8s.tochka.com,resources=shardedhttpproxies/finalizers,verbs=update
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingressclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=projectcontour.io,resources=httpproxies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

const (
	ExponentialBackoffBaseDelay = 5 * time.Millisecond
	ExponentialBackoffMaxDelay  = 1000 * time.Second
)

// Engine is the reconciliation core shared by both controllers. It walks the
// lifecycle from the design diagram:
//
//	Created -> compute desired -> compare with current and fix -> Ready
//	                                   |-- shard changed  -> Resharding
//	                                   |-- spec changed   -> Provisioning
//	                                   `-- no changes     -> Ready
//	deletionTimestamp set              -> Terminating (graceful child drain)
//
// Every mutating action ends the pass and requeues, so the loop applies one
// change at a time and reports progress via the parent status and events.
// C is the concrete child type (*networkingv1.Ingress, *contourv1.HTTPProxy).
type Engine[C client.Object] struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Settings Settings

	Adapter   ChildAdapter[C]
	Renderer  DesiredRenderer[C]
	Selector  ShardSelector
	Scheduler Scheduler
	// NewSharded returns an empty parent object to fetch into.
	NewSharded func() ShardedObject
	CtrlName   string

	tracker *stateTracker
	clock   migrationClock

	initMu      sync.Mutex
	initialized bool
}

// NewEngine wires an Engine for one parent/child type pair.
func NewEngine[C client.Object](
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	settings Settings,
	adapter ChildAdapter[C],
	renderer DesiredRenderer[C],
	newSharded func() ShardedObject,
	ctrlName string,
) *Engine[C] {
	tracker := newStateTracker(ctrlName)
	return &Engine[C]{
		Client:     c,
		Scheme:     scheme,
		Recorder:   recorder,
		Settings:   settings,
		Adapter:    adapter,
		Renderer:   renderer,
		Selector:   &hashShardSelector{maxShards: settings.MaxShards},
		Scheduler:  scheduler.NewCooldown(settings.TerminationPeriod, settings.ShardUpdateCooldown, tracker),
		NewSharded: newSharded,
		CtrlName:   ctrlName,
		tracker:    tracker,
		clock: migrationClock{
			terminationPeriod:    settings.TerminationPeriod,
			unregisterAnnotation: settings.UnregisterAnnotation,
		},
	}
}

// scope carries the per-request state of one reconcile pass.
type scope struct {
	ctx context.Context
	req ctrl.Request
	key string
	obj ShardedObject

	shards       []Shard
	regular      bool
	useAllShards bool

	// resharding is set when any shard is mid-migration.
	resharding bool
	// mutated is set when the pass changed anything in the cluster.
	mutated bool
}

func (e *Engine[C]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if err := e.ensureInitialized(ctx); err != nil {
		return ctrl.Result{}, err
	}

	s := &scope{ctx: ctx, req: req, key: req.String(), obj: e.NewSharded()}

	if err := e.Get(ctx, req.NamespacedName, s.obj); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Resource not found. Ignoring since object must be deleted", "objectKey", s.key)
			e.tracker.forget(s.key)
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch sharded object")
		return ctrl.Result{}, err
	}

	if err := e.ensureFinalizer(s); err != nil {
		logger.Error(err, "unable to set controller finalizer")
		return ctrl.Result{}, err
	}

	if !s.obj.GetDeletionTimestamp().IsZero() {
		return e.reconcileTerminating(s)
	}

	s.useAllShards = s.obj.GetAnnotations()[e.Settings.AllShardsPlacementAnnotation] == trueValue

	var err error
	s.shards, s.regular, err = e.Selector.ShardsFor(s.obj, s.useAllShards)
	if err != nil {
		logger.Error(err, "Unable to use shard")
		e.warnf(s, status.EventShardSelectionFailed, "Unable to pick a shard: %v", err)
		return ctrl.Result{}, err
	}

	if result, waiting := e.scheduleApply(s, logger); waiting {
		return result, nil
	}

	// Compute desired: spec from the parent, shard from the selector,
	// rendered into the set of child objects.
	desired, err := e.computeDesired(s)
	if err != nil {
		logger.Error(err, "children object can't be generated")
		e.warnf(s, status.EventChildBuildFailed, "Unable to render children: %v", err)
		return ctrl.Result{}, err
	}

	e.tracker.updateMetrics()

	// Compare with current and fix: create/update children, then prune the
	// ones no longer desired.
	result := e.applyChildren(s, desired)

	e.publishLifecycle(s, result)
	return result, nil
}

// ensureFinalizer sets the controller finalizer on a live parent that does
// not carry it yet.
func (e *Engine[C]) ensureFinalizer(s *scope) error {
	if !s.obj.GetDeletionTimestamp().IsZero() || controllerutil.ContainsFinalizer(s.obj, e.Settings.FinalizerKey) {
		return nil
	}
	controllerutil.AddFinalizer(s.obj, e.Settings.FinalizerKey)
	if err := e.Update(s.ctx, s.obj); err != nil {
		return fmt.Errorf("cannot set controller finalizer: %w", err)
	}
	return nil
}

// scheduleApply implements the rate limiting: every pass that is not already
// booked asks the scheduler for a slot first. waiting is true when the pass
// must requeue with the returned result until the slot arrives.
func (e *Engine[C]) scheduleApply(s *scope, logger logr.Logger) (ctrl.Result, bool) {
	if e.tracker.isWaiting(s.key) {
		return ctrl.Result{}, false
	}
	shardNames := make([]string, 0, len(s.shards))
	for _, shard := range s.shards {
		shardNames = append(shardNames, shard.Name)
	}
	result, handled := e.Scheduler.Schedule(s.key, s.obj.GetShardedStatus(), shardNames, logger)
	if !handled {
		return ctrl.Result{}, false
	}
	if result.RequeueAfter > time.Second {
		e.eventf(
			s,
			status.EventApplyScheduled,
			"Apply on shard scheduled in %s",
			result.RequeueAfter.Round(time.Second),
		)
	}
	if s.obj.GetShardedStatus().Phase == "" {
		_ = e.setLifecycle(s, controllerv1.PhasePending,
			status.Condition(controllerv1.ConditionReady, false, "Pending", "Waiting for the first apply slot"),
			status.Condition(controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress"))
	}
	return result, true
}

// ensureInitialized discovers the cluster shards once before the first pass.
func (e *Engine[C]) ensureInitialized(ctx context.Context) error {
	e.initMu.Lock()
	defer e.initMu.Unlock()
	if e.initialized {
		return nil
	}
	logger := log.Log.WithName(e.CtrlName)
	if err := scheduler.DiscoverClusterShards(ctx, e.Client, e.Settings.MaxShards, e.Scheduler, logger); err != nil {
		return err
	}
	e.initialized = true
	return nil
}
