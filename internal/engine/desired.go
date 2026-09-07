package engine

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.tochka.com/sharded-ingress-controller/internal/status"
)

// computeDesired resolves the migration context of every shard and renders
// the desired children.
func (e *Engine[C]) computeDesired(s *scope) ([]DesiredChild[C], error) {
	var all []DesiredChild[C]
	for _, shard := range s.shards {
		plan := e.resolveShardPlan(s, shard)

		if plan.OldShard != "" && plan.OldShard != shard.Name {
			s.resharding = true
		}
		if plan.CreateTmp {
			e.eventf(
				s,
				status.EventReshardingStarted,
				"Resharding from %s to %s: creating tmp child to keep the old shard serving",
				plan.OldShard,
				shard.Name,
			)
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
func (e *Engine[C]) resolveShardPlan(s *scope, shard Shard) ShardPlan {
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
	err := e.Get(
		s.ctx,
		types.NamespacedName{Name: TmpChildName(s.obj.GetName(), shard.Number), Namespace: s.obj.GetNamespace()},
		tmp,
	)
	switch {
	case err == nil:
		// The tmp child exists: while its migration window has not passed
		// the main child keeps the old class so traffic stays on the old
		// shard.
		if hold := e.clock.holdOldShard(tmp.GetAnnotations()); hold.Active {
			plan.OldShard = hold.OldShard
			plan.EffectiveClass = hold.OldShard
		}
	case apierrors.IsNotFound(err) && conflict != "":
		// Migration starts: the tmp child does not exist yet and the
		// status still records the child on another shard.
		plan.CreateTmp = true
		plan.EffectiveClass = conflict
	case !apierrors.IsNotFound(err):
		// Any other error falls through: the pass continues on the new
		// class, exactly as if no tmp child existed.
		log.FromContext(s.ctx).Error(
			err,
			"unable to fetch tmp child, continuing on the target shard",
			"objectKind",
			e.Adapter.Kind(),
			"objectName",
			TmpChildName(s.obj.GetName(), shard.Number),
		)
	}
	return plan
}

// applyChildren brings the cluster to the desired set: it creates missing
// children, updates drifted ones and prunes children that are no longer
// desired. A create or delete ends the pass immediately so the loop applies
