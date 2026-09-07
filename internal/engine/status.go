package engine

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

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

// Status updates are not retried on version conflicts: a conflict fails the
// pass, controller-runtime requeues it and the next pass re-reads the parent
// and re-applies on top of the latest version.

// addChildToStatus records the child under the shard in the parent status and
// keeps the per-class child metric in sync.
func (e *Engine[C]) addChildToStatus(s *scope, kind, name, shardName string) error {
	if shardName == "" || kind == "" || name == "" {
		return nil
	}
	createdObjects, added := status.AppendChild(s.obj.GetShardedStatus().CreatedObjects, shardName, kind, name)
	if added {
		s.obj.GetShardedStatus().CreatedObjects = createdObjects
		if err := e.Status().Update(s.ctx, s.obj); err != nil {
			return err
		}
	}
	e.tracker.noteChildClass(s.key, shardName)
	return nil
}

// removeChildFromStatus drops every record of the named child from the parent
// status.
func (e *Engine[C]) removeChildFromStatus(s *scope, name string) error {
	status.RemoveChild(s.obj.GetShardedStatus(), name)
	return e.Status().Update(s.ctx, s.obj)
}

// setLifecycle publishes the phase, conditions and observedGeneration on the
// parent status. It writes only when something actually changed to avoid
// update storms.
func (e *Engine[C]) setLifecycle(s *scope, phase controllerv1.ShardedPhase, ready, resharding metav1.Condition) error {
	if !status.ApplyLifecycle(s.obj.GetShardedStatus(), s.obj.GetGeneration(), phase, ready, resharding) {
		return nil
	}
	return e.Status().Update(s.ctx, s.obj)
}

// publishLifecycle mirrors the outcome of the pass into the parent status.
func (e *Engine[C]) publishLifecycle(s *scope, result ctrl.Result) error {
	var (
		phase      controllerv1.ShardedPhase
		ready      metav1.Condition
		resharding metav1.Condition
	)

	switch {
	case s.resharding:
		phase = controllerv1.PhaseResharding
		ready = status.Condition(
			controllerv1.ConditionReady, false, "Resharding", "Children are migrating between shards")
		resharding = status.Condition(
			controllerv1.ConditionResharding, true, "MigrationInProgress", "Children are migrating to their new shard")
	case s.mutated || result.RequeueAfter > 0 || result.Requeue:
		phase = controllerv1.PhaseProvisioning
		ready = status.Condition(
			controllerv1.ConditionReady, false, "Provisioning", "Children are being applied")
		resharding = status.Condition(
			controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress")
	default:
		phase = controllerv1.PhaseReady
		ready = status.Condition(
			controllerv1.ConditionReady, true, "ChildrenReady", "All children match the desired state")
		resharding = status.Condition(
			controllerv1.ConditionResharding, false, "NoMigration", "No shard migration in progress")
	}

	return e.setLifecycle(s, phase, ready, resharding)
}
