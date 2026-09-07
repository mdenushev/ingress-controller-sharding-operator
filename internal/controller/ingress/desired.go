package ingress

import (
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

// renderer renders the desired Ingress children of a ShardedIngress for one
// shard.
type renderer struct {
	settings engine.Settings
}

func newRenderer(settings engine.Settings) *renderer {
	return &renderer{settings: settings}
}

func (b *renderer) RenderChildren(
	sharded engine.ShardedObject,
	plan engine.ShardPlan,
) ([]engine.DesiredChild[*networkingv1.Ingress], error) {
	src, ok := sharded.(*controllerv1.ShardedIngress)
	if !ok {
		return nil, fmt.Errorf("unsupported sharded object type: %T", sharded)
	}

	var children []engine.DesiredChild[*networkingv1.Ingress]

	shardedIngress := src.DeepCopy()
	if shardedIngress.Spec.Template.Labels == nil {
		shardedIngress.Spec.Template.Labels = make(map[string]string)
	}
	if shardedIngress.Spec.Template.Annotations == nil {
		shardedIngress.Spec.Template.Annotations = make(map[string]string)
	}

	tmpName := engine.TmpChildName(shardedIngress.Name, plan.Shard.Number)

	// While migrating, a tmp child pinned to the old shard keeps serving
	// traffic until service discovery converges on the new shard.
	if plan.CreateTmp {
		tempShardedIngress := shardedIngress.DeepCopy()
		tempShardedIngress.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.OldShard
		tempShardedIngress.Spec.Template.Annotations[engine.OldShardAnnotation] = plan.OldShard
		tmpIngress := renderIngress(tempShardedIngress, tmpName, plan.OldShard)
		children = append(children, engine.DesiredChild[*networkingv1.Ingress]{Shard: plan.Shard, Obj: tmpIngress})
	}

	shardedIngress.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.EffectiveClass

	if plan.UseAllShards && shardedIngress.Spec.Template.Labels[b.settings.AppNameLabel] != "" {
		app := shardedIngress.Spec.Template.Labels[b.settings.AppNameLabel]
		existingTags := shardedIngress.Spec.Template.Annotations[b.settings.ServiceDiscoveryTagsAnnotation]
		if existingTags != "" {
			shardedIngress.Spec.Template.Annotations[b.settings.ServiceDiscoveryTagsAnnotation] = existingTags + "," + plan.EffectiveClass
		} else {
			shardedIngress.Spec.Template.Annotations[b.settings.ServiceDiscoveryTagsAnnotation] = plan.EffectiveClass
		}

		if len(shardedIngress.Spec.Template.Spec.Rules) > 0 {
			firstRule := shardedIngress.Spec.Template.Spec.Rules[0]
			for _, host := range b.settings.AllShardsBaseHosts {
				newRule := firstRule.DeepCopy()
				newRule.Host = fmt.Sprintf("%s.%s-%s.%s", plan.EffectiveClass, shardedIngress.Namespace, app, host)
				shardedIngress.Spec.Template.Spec.Rules = append(shardedIngress.Spec.Template.Spec.Rules, *newRule)
			}
		}
	}

	// On a sharded class every child carries its shard number in the name;
	// only a non-sharded (regular) class keeps the bare parent name.
	mainName := shardedIngress.Name
	if !plan.Regular {
		mainName = fmt.Sprintf("%s-%d", shardedIngress.Name, plan.Shard.Number)
	}

	main := engine.DesiredChild[*networkingv1.Ingress]{
		Shard: plan.Shard,
		Obj:   renderIngress(shardedIngress, mainName, plan.EffectiveClass),
	}
	// Mid-migration the tmp child must stay off the pruning list even in
	// passes where it is not rendered, so it is booked alongside the main
	// child.
	if plan.OldShard != "" {
		main.ExtraChildNames = []string{tmpName}
	}
	children = append(children, main)

	return children, nil
}

func renderIngress(shardedIngress *controllerv1.ShardedIngress, name, ingressClass string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		Name:        name,
		Namespace:   shardedIngress.Namespace,
		Annotations: shardedIngress.Spec.Template.Annotations,
		Labels:      shardedIngress.Spec.Template.Labels,
		Spec: networkingv1.IngressSpec{
			IngressClassName: &ingressClass,
			DefaultBackend:   shardedIngress.Spec.Template.Spec.DefaultBackend,
			TLS:              shardedIngress.Spec.Template.Spec.TLS,
			Rules:            shardedIngress.Spec.Template.Spec.Rules,
		},
	}
}
