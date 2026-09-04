package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
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
func NewEngine[C client.Object](c client.Client, scheme *runtime.Scheme, recorder record.EventRecorder, settings Settings, adapter ChildAdapter[C], renderer DesiredRenderer[C], newSharded func() ShardedObject, ctrlName string) *Engine[C] {
	tracker := newStateTracker(ctrlName)
	return &Engine[C]{
		Client:     c,
		Scheme:     scheme,
		Recorder:   recorder,
		Settings:   settings,
		Adapter:    adapter,
		Renderer:   renderer,
		Selector:   &hashShardSelector{maxShards: settings.MaxShards},
		Scheduler:  newCooldownScheduler(settings.TerminationPeriod, settings.ShardUpdateCooldown, tracker),
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

	s := &scope{ctx: ctx, req: req, key: req.NamespacedName.String(), obj: e.NewSharded()}

	if err := e.Get(ctx, req.NamespacedName, s.obj); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Resource not found. Ignoring since object must be deleted", "objectKey", s.key)
			e.tracker.forget(s.key)
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch sharded object")
		return ctrl.Result{}, err
	}

	// If object doesn't have finalizer — set finalizer
	if s.obj.GetDeletionTimestamp().IsZero() && !controllerutil.ContainsFinalizer(s.obj, e.Settings.FinalizerKey) {
		controllerutil.AddFinalizer(s.obj, e.Settings.FinalizerKey)
		if err := e.Update(ctx, s.obj); err != nil {
			logger.Error(err, "unable to set controller finalizer")
			return ctrl.Result{}, fmt.Errorf("cannot set controller finalizer: %w", err)
		}
	}

	if !s.obj.GetDeletionTimestamp().IsZero() {
		return e.reconcileTerminating(s)
	}

	if val := s.obj.GetAnnotations()[e.Settings.AllShardsPlacementAnnotation]; val == "true" {
		s.useAllShards = true
	}

	var err error
	s.shards, s.regular, err = e.Selector.ShardsFor(s.obj, s.useAllShards)
	if err != nil {
		logger.Error(err, "Unable to use shard")
		return ctrl.Result{}, nil
	}

	// Rate limiting: every pass that is not already booked asks the
	// scheduler for a slot first and requeues until the slot arrives.
	if !e.tracker.isWaiting(s.key) {
		result, handled := e.Scheduler.Schedule(s.key, s.obj.GetShardedStatus(), s.shards, logger)
		if handled {
			if result.RequeueAfter > time.Second {
				e.eventf(s, EventApplyScheduled, "Apply on shard scheduled in %s", result.RequeueAfter.Round(time.Second))
			}
			if s.obj.GetShardedStatus().Phase == "" {
				_ = e.setLifecycle(s, controllerv1.PhasePending,
					condition(controllerv1.ConditionReady, false, "Pending", "Waiting for the first apply slot"),
					condition(controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress"))
			}
			return result, nil
		}
	}

	// Compute desired: spec from the parent, shard from the selector,
	// rendered into the set of child objects.
	desired, err := e.computeDesired(s)
	if err != nil {
		logger.Error(err, "children object can't be generated")
		e.warnf(s, EventChildBuildFailed, "Unable to render children: %v", err)
		return ctrl.Result{}, nil
	}

	e.tracker.updateMetrics()

	// Compare with current and fix: create/update children, then prune the
	// ones no longer desired.
	result, err := e.applyChildren(s, desired)

	e.publishLifecycle(s, result)
	return result, err
}

// ensureInitialized discovers the cluster shards once before the first pass.
func (e *Engine[C]) ensureInitialized(ctx context.Context) error {
	e.initMu.Lock()
	defer e.initMu.Unlock()
	if e.initialized {
		return nil
	}
	logger := log.Log.WithName(e.CtrlName)
	if err := discoverClusterShards(ctx, e.Client, e.Settings.MaxShards, e.Scheduler, logger); err != nil {
		return err
	}
	e.initialized = true
	return nil
}

// computeDesired resolves the migration context of every shard and renders
// the desired children.
func (e *Engine[C]) computeDesired(s *scope) ([]DesiredChild[C], error) {
	var all []DesiredChild[C]
	for _, shard := range s.shards {
		plan, err := e.resolveShardPlan(s, shard)
		if err != nil {
			return nil, err
		}
		if plan.OldShard != "" && plan.OldShard != shard.Name {
			s.resharding = true
		}
		if plan.CreateTmp {
			e.eventf(s, EventReshardingStarted, "Resharding from %s to %s: creating tmp child to keep the old shard serving", plan.OldShard, shard.Name)
		}
		children, err := e.Renderer.RenderChildren(s.obj, plan)
		if err != nil {
			return nil, err
		}
		all = append(all, children...)
	}
	return all, nil
}

// resolveShardPlan detects whether the shard is mid-migration by combining
// the recorded status with the live tmp child, and decides which ingress
// class the children must carry right now.
func (e *Engine[C]) resolveShardPlan(s *scope, shard Shard) (ShardPlan, error) {
	plan := ShardPlan{
		Shard:          shard,
		EffectiveClass: shard.Name,
		Regular:        s.regular,
		UseAllShards:   s.useAllShards,
	}

	childBase := fmt.Sprintf("%s-%d", s.obj.GetName(), shard.Number)
	conflict := reshardingConflict(s.obj.GetShardedStatus(), shard.Name, childBase)
	plan.OldShard = conflict

	tmp := e.Adapter.NewObject()
	err := e.Get(s.ctx, types.NamespacedName{Name: tmpChildName(s.obj.GetName(), shard.Number), Namespace: s.obj.GetNamespace()}, tmp)
	if err != nil {
		if apierrors.IsNotFound(err) && conflict != "" {
			// Migration starts: the tmp child does not exist yet and
			// the status still records the child on another shard.
			plan.CreateTmp = true
			plan.EffectiveClass = conflict
		}
		// Any other error falls through: the pass continues on the
		// new class, exactly as if no tmp child existed.
		return plan, nil
	}

	// The tmp child exists: while its migration window has not passed the
	// main child keeps the old class so traffic stays on the old shard.
	if hold := e.clock.holdOldShard(tmp.GetAnnotations()); hold.Active {
		plan.OldShard = hold.OldShard
		plan.EffectiveClass = hold.OldShard
	}
	return plan, nil
}

// applyChildren brings the cluster to the desired set: it creates missing
// children, updates drifted ones and prunes children that are no longer
// desired. A create or delete ends the pass immediately so the loop applies
// one change at a time.
func (e *Engine[C]) applyChildren(s *scope, desired []DesiredChild[C]) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)
	statusList := make(map[string][]map[string]string)

	if !e.tracker.isManaged(s.key) {
		e.tracker.markManaged(s.key)
	}
	e.tracker.noteShardedClass(s.key, s.obj.GetIngressClassName())

	for _, current := range desired {
		found := e.Adapter.NewObject()
		if err := ctrl.SetControllerReference(s.obj, current.Obj, e.Scheme); err != nil {
			logger.Error(err, "unable to set controller reference", "objectKind", e.Adapter.Kind(), "objectName", current.Obj.GetName())
		}
		err := e.Get(s.ctx, types.NamespacedName{Name: current.Obj.GetName(), Namespace: current.Obj.GetNamespace()}, found)
		if err != nil {
			if apierrors.IsNotFound(err) {
				result, err := e.createChild(s, current)
				if err != nil {
					logger.Error(err, "unable to create", "objectKind", e.Adapter.Kind(), "objectName", current.Obj.GetName())
				}
				return result, nil
			}
			logger.Error(err, "unable to get", "objectKind", e.Adapter.Kind(), "objectName", current.Obj.GetName())
		} else {
			if err := e.updateChild(s, found, current); err != nil {
				logger.Error(err, "unable to update", "objectKind", e.Adapter.Kind(), "objectName", current.Obj.GetName())
			}
		}

		statusList[current.Shard.Name] = append(statusList[current.Shard.Name], map[string]string{"kind": e.Adapter.Kind(), "name": current.Obj.GetName()})
		for _, name := range current.AlsoBook {
			statusList[current.Shard.Name] = append(statusList[current.Shard.Name], map[string]string{"kind": e.Adapter.Kind(), "name": name})
		}
	}

	result, err := e.pruneChildren(s, statusList)
	if err != nil {
		logger.Error(err, "unable to delete unlisted objects")
	}
	return result, nil
}

func (e *Engine[C]) createChild(s *scope, child DesiredChild[C]) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)
	kind := e.Adapter.Kind()
	name := child.Obj.GetName()

	if err := e.Create(s.ctx, child.Obj); err != nil {
		logger.Error(err, "unable to create", "objectKind", kind, "objectName", name)
		e.tracker.markErrored(s.key)
		e.warnf(s, EventChildApplyFailed, "Unable to create %s %s: %v", kind, name, err)
		return ctrl.Result{}, err
	}

	logger.Info("successfully created", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	if isTmpChildName(s.obj.GetName(), name) {
		e.eventf(s, EventTmpChildCreated, "Created tmp %s %s to keep the old shard serving during migration", kind, name)
	} else {
		e.eventf(s, EventChildCreated, "Created %s %s on shard %s", kind, name, child.Shard.Name)
	}
	if err := e.addChildToStatus(s, kind, name, child.Shard.Name); err != nil {
		return ctrl.Result{}, err
	}
	e.tracker.doneWaiting(s.key)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, child.Shard.Name).Inc()
	s.mutated = true
	return ctrl.Result{}, nil
}

func (e *Engine[C]) updateChild(s *scope, existing C, child DesiredChild[C]) error {
	logger := log.FromContext(s.ctx)
	kind := e.Adapter.Kind()
	name := child.Obj.GetName()

	equal, err := e.Adapter.Equal(existing.DeepCopyObject().(C), child.Obj.DeepCopyObject().(C))
	if err != nil {
		logger.Error(err, "unable to compare", "objectKind", kind, "objectName", name)
		return err
	}
	if equal {
		return nil
	}

	merged := e.Adapter.Merge(existing, child.Obj)
	if err := e.Update(s.ctx, merged); err != nil {
		logger.Error(err, "unable to update", "objectKind", kind, "objectName", name)
		e.tracker.markErrored(s.key)
		e.warnf(s, EventChildApplyFailed, "Unable to update %s %s: %v", kind, name, err)
		return err
	}
	logger.Info("successfully updated", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	e.eventf(s, EventChildUpdated, "Updated %s %s on shard %s", kind, name, child.Shard.Name)
	if err := e.addChildToStatus(s, kind, name, child.Shard.Name); err != nil {
		return err
	}
	e.tracker.doneWaiting(s.key)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, child.Shard.Name).Inc()
	s.mutated = true
	return nil
}

// listChildren lists the live children owned by the parent.
func (e *Engine[C]) listChildren(s *scope) (unstructured.UnstructuredList, error) {
	childObjs := unstructured.UnstructuredList{}
	childObjs.SetGroupVersionKind(e.Adapter.ListGVK())

	if err := e.List(s.ctx, &childObjs, client.InNamespace(s.req.Namespace)); err != nil {
		return unstructured.UnstructuredList{}, err
	}

	parentKind := s.obj.GetKind()
	res := unstructured.UnstructuredList{}
	for _, childObj := range childObjs.Items {
		for _, owner := range childObj.GetOwnerReferences() {
			if owner.Name == s.obj.GetName() && owner.Kind == parentKind {
				res.Items = append(res.Items, childObj)
			}
		}
	}
	return res, nil
}

// pruneChildren walks the live children and schedules the ones that are no
// longer desired for graceful deletion (unregister from service discovery
// first, delete after the termination window). It also drops status records
// whose objects are gone.
func (e *Engine[C]) pruneChildren(s *scope, currentList map[string][]map[string]string) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)
	status := s.obj.GetShardedStatus()

	childObjs, err := e.listChildren(s)
	if err != nil {
		logger.Error(err, "unable to list child objects")
		return ctrl.Result{}, err
	}

	for _, obj := range childObjs.Items {
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
		if !keep || isTmpChildName(s.obj.GetName(), obj.GetName()) {
			for shard, objStatusSlice := range status.CreatedObjects {
				for _, objStatus := range objStatusSlice {
					if objStatus["name"] == obj.GetName() {
						shardName = shard
					}
				}
			}
			shouldDelete, err := e.evaluateDeletionTiming(s, &obj, shardName)
			if err != nil {
				logger.Error(err, "error handling deletion timing", "objectKind", obj.GetKind(), "objectName", obj.GetName())
				return ctrl.Result{}, err
			}
			if shouldDelete {
				if err := e.Delete(s.ctx, &obj); err != nil {
					logger.Error(err, "unable to delete", "objectKind", obj.GetKind(), "objectName", obj.GetName())
					return ctrl.Result{}, err
				}
				logger.Info("successfully deleted from cluster", "objectKind", obj.GetKind(), "objectName", obj.GetName())
				e.eventf(s, EventChildDeleted, "Deleted %s %s", obj.GetKind(), obj.GetName())
				metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
				s.mutated = true
				return ctrl.Result{}, nil
			}
			// The child waits for its deletion window; keep it recorded
			// and come back when the window may have passed.
			if err := e.addChildToStatus(s, obj.GetKind(), obj.GetName(), shardName); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: e.Settings.TerminationPeriod}, nil
		}

		if err := e.addChildToStatus(s, obj.GetKind(), obj.GetName(), shardName); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Drop status records whose objects no longer exist in the cluster.
	for shard, objStatusSlice := range status.CreatedObjects {
		for _, objStatus := range objStatusSlice {
			if !findInStatus(shard, objStatus["kind"], objStatus["name"], &currentList) {
				obj := &unstructured.Unstructured{}
				obj.SetKind(objStatus["kind"])
				obj.SetAPIVersion(s.obj.GetObject().GetObjectKind().GroupVersionKind().Version)
				obj.SetNamespace(s.obj.GetNamespace())
				obj.SetName(objStatus["name"])
				if err := e.Get(s.ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj); err != nil {
					// If it does not exist, delete the object from the status
					if err := e.removeChildFromStatus(s, objStatus["name"]); err != nil {
						logger.Error(err, "unable to update status", "objectKind", s.obj.GetKind(), "objectName", objStatus["name"])
						return ctrl.Result{}, err
					}
				}
				e.tracker.markReady(s.key)
			}
		}
	}

	e.tracker.markReady(s.key)
	return ctrl.Result{}, nil
}

// evaluateDeletionTiming drives the graceful deletion timeline of one child:
// first stamp auto-delete-after, then mark for service discovery
// unregistering one termination period before the deadline, and only report
// shouldDelete once the deadline passed.
func (e *Engine[C]) evaluateDeletionTiming(s *scope, obj *unstructured.Unstructured, shardName string) (shouldDelete bool, err error) {
	logger := log.FromContext(s.ctx)

	deleteAfterTime, deleteAfterExists, err := parseDeleteAfterAnnotation(obj)
	if err != nil {
		logger.Error(err, "unable to parse auto-delete-after annotation", "objectKind", obj.GetKind(), "objectName", obj.GetName())
		return false, err
	}

	delTime := e.Settings.TerminationPeriod * 2
	if isTmpChildName(s.obj.GetName(), obj.GetName()) {
		delTime = e.Settings.TerminationPeriod * 3
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

			if err := e.Update(s.ctx, obj); err != nil {
				logger.Error(err, "unable to update object with marked-for-deletion annotation", "objectKind", obj.GetKind(), "objectName", obj.GetName())
				return false, err
			}
			logger.Info("marked-for-deletion annotation set", "objectKind", obj.GetKind(), "objectName", obj.GetName())
			e.eventf(s, EventMarkedForDeletion, "Marked %s %s for service discovery unregistering", obj.GetKind(), obj.GetName())
			metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
			s.mutated = true
		}

		return false, nil
	}

	setDeleteAfterAnnotation(obj, delTime)
	if err := e.Update(s.ctx, obj); err != nil {
		logger.Error(err, "unable to update auto-delete-after annotation", "objectKind", obj.GetKind(), "objectName", obj.GetName())
		return false, err
	}
	logger.Info("auto-delete-after annotation set", "objectKind", obj.GetKind(), "objectName", obj.GetName(), "auto-delete-after", obj.GetAnnotations()[AutoDeleteAfterAnnotation])
	e.eventf(s, EventDeletionScheduled, "Scheduled %s %s for deletion at %s", obj.GetKind(), obj.GetName(), obj.GetAnnotations()[AutoDeleteAfterAnnotation])
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
	e.tracker.markWaiting(s.key)
	s.mutated = true
	return false, nil
}

// reconcileTerminating drains the children of a deleted parent: every child
// is marked for service discovery unregistering, deleted after its window,
// and only then the finalizer is removed.
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
		var shardName string
		for shard, objStatusSlice := range s.obj.GetShardedStatus().CreatedObjects {
			for _, objStatus := range objStatusSlice {
				if objStatus["name"] == child.GetName() {
					shardName = shard
				}
			}
		}

		deleteAfter, deleteAfterExists, err := parseDeleteAfterAnnotation(&child)
		if err != nil {
			logger.Error(err, "[finalizer] unable to parse auto-delete-after annotation", "objectKind", child.GetKind(), "objectName", child.GetName())
			return ctrl.Result{}, fmt.Errorf("[finalizer] cannot parse delete after annotation: %w", err)
		}

		if !deleteAfterExists || !e.clock.isMarkedForUnregistering(&child) {
			logger.Info("[finalizer] mark child for deletion", "objectKind", child.GetKind(), "objectName", child.GetName())
			e.clock.markForUnregistering(&child)
			setDeleteAfterAnnotation(&child, e.Settings.FinalizerDeletionTerminationPeriod)

			if err := e.Update(s.ctx, &child); err != nil {
				logger.Error(err, "[finalizer] unable to set auto-delete-after and unregister annotation on child", "objectKind", child.GetKind(), "objectName", child.GetName())
				return ctrl.Result{}, fmt.Errorf("[finalizer] unable to set auto-delete-after and unregister annotation on child: %w", err)
			}
			e.eventf(s, EventFinalizerDraining, "Draining child %s %s before deletion", child.GetKind(), child.GetName())
			waitingForDeletion++
			continue
		}

		if time.Now().After(deleteAfter) {
			logger.Info("[finalizer] deleting child", "objectKind", child.GetKind(), "objectName", child.GetName())
			if err := e.Delete(s.ctx, &child); err != nil {
				logger.Error(err, "[finalizer] unable to delete child", "objectKind", child.GetKind(), "objectName", child.GetName())
				return ctrl.Result{}, err
			}
			logger.Info("[finalizer] successfully deleted child from cluster", "objectKind", child.GetKind(), "objectName", child.GetName())
			e.eventf(s, EventChildDeleted, "Deleted child %s %s", child.GetKind(), child.GetName())
			metrics.ProcessingCounter.WithLabelValues(e.CtrlName, shardName).Inc()
		} else {
			waitingForDeletion++
		}
	}

	if waitingForDeletion == 0 {
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
	return ctrl.Result{RequeueAfter: e.Settings.FinalizerDeletionTerminationPeriod}, nil
}

// publishLifecycle mirrors the outcome of the pass into the parent status.
func (e *Engine[C]) publishLifecycle(s *scope, result ctrl.Result) {
	logger := log.FromContext(s.ctx)

	switch {
	case s.resharding:
		if err := e.setLifecycle(s, controllerv1.PhaseResharding,
			condition(controllerv1.ConditionReady, false, "Resharding", "Children are migrating between shards"),
			condition(controllerv1.ConditionResharding, true, "MigrationInProgress", "Children are migrating to their new shard")); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	case s.mutated || result.RequeueAfter > 0 || result.Requeue: //nolint:staticcheck // Requeue kept for parity with requeue-now results
		if err := e.setLifecycle(s, controllerv1.PhaseProvisioning,
			condition(controllerv1.ConditionReady, false, "Provisioning", "Children are being applied"),
			condition(controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress")); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	default:
		if err := e.setLifecycle(s, controllerv1.PhaseReady,
			condition(controllerv1.ConditionReady, true, "ChildrenReady", "All children match the desired state"),
			condition(controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress")); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	}
}
