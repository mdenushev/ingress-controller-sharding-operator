package engine

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/status"
)

// eventf records a Normal kube event on the parent when a recorder is wired.
func (e *Engine[C]) eventf(s *scope, reason, format string, args ...any) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.Eventf(s.obj, corev1.EventTypeNormal, reason, format, args...)
}

// warnf records a Warning kube event on the parent when a recorder is wired.
func (e *Engine[C]) warnf(s *scope, reason, format string, args ...any) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.Eventf(s.obj, corev1.EventTypeWarning, reason, format, args...)
}

// updateStatusWithRetry runs mutate (which must modify s.obj and push the
// status) and retries on version conflicts, refreshing the object in between
// so mutate re-applies on top of the latest version.
func (e *Engine[C]) updateStatusWithRetry(s *scope, mutate func() error) error {
	maxRetries := 5
	var err error
	for range maxRetries {
		err = mutate()
		if err == nil {
			return nil
		}
		if !errors.IsConflict(err) {
			return err
		}
		// The object has been modified by someone else, fetch the latest
		// version and try again.
		if getErr := e.Get(
			s.ctx,
			types.NamespacedName{Name: s.obj.GetName(), Namespace: s.obj.GetNamespace()},
			s.obj,
		); getErr != nil {
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
	if !status.FindIn(shardName, kind, name, s.obj.GetShardedStatus().CreatedObjects) {
		err := e.updateStatusWithRetry(s, func() error {
			createdObjects, added := status.AppendChild(
				s.obj.GetShardedStatus().CreatedObjects,
				shardName, kind, name,
			)
			if !added {
				return errors.NewAlreadyExists(schema.GroupResource{}, shardName)
			}
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
		status.RemoveChild(s.obj.GetShardedStatus(), name)
		return e.Status().Update(s.ctx, s.obj)
	})
}

// setLifecycle publishes the phase, conditions and observedGeneration on the
// parent status. It writes only when something actually changed to avoid
// update storms.
func (e *Engine[C]) setLifecycle(s *scope, phase controllerv1.ShardedPhase, ready, resharding metav1.Condition) error {
	return e.updateStatusWithRetry(s, func() error {
		if !status.ApplyLifecycle(s.obj.GetShardedStatus(), s.obj.GetGeneration(), phase, ready, resharding) {
			return nil
		}
		return e.Status().Update(s.ctx, s.obj)
	})
}

// publishLifecycle mirrors the outcome of the pass into the parent status.
func (e *Engine[C]) publishLifecycle(s *scope, result ctrl.Result) {
	logger := log.FromContext(s.ctx)

	switch {
	case s.resharding:
		if err := e.setLifecycle(
			s,
			controllerv1.PhaseResharding,
			status.Condition(controllerv1.ConditionReady, false, "Resharding", "Children are migrating between shards"),
			status.Condition(
				controllerv1.ConditionResharding,
				true,
				"MigrationInProgress",
				"Children are migrating to their new shard",
			),
		); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	case s.mutated || result.RequeueAfter > 0 || result.Requeue:
		if err := e.setLifecycle(
			s,
			controllerv1.PhaseProvisioning,
			status.Condition(controllerv1.ConditionReady, false, "Provisioning", "Children are being applied"),
			status.Condition(
				controllerv1.ConditionResharding,
				false,
				"NoMigration",
				"No shard migration in progress",
			),
		); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	default:
		if err := e.setLifecycle(
			s,
			controllerv1.PhaseReady,
			status.Condition(
				controllerv1.ConditionReady,
				true,
				"ChildrenReady",
				"All children match the desired state",
			),
			status.Condition(
				controllerv1.ConditionResharding,
				false,
				"NoMigration",
				"No shard migration in progress",
			),
		); err != nil {
			logger.Error(err, "unable to publish lifecycle status")
		}
	}
}
