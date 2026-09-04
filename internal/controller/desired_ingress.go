package controller

import (
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// ingressBuilder renders the desired Ingress children of a ShardedIngress for
// one shard.
type ingressBuilder struct {
	settings Settings
}

func newIngressBuilder(settings Settings) *ingressBuilder {
	return &ingressBuilder{settings: settings}
}

func (b *ingressBuilder) BuildChildren(sharded ShardedObject, plan ShardPlan) ([]DesiredChild, error) {
	src, ok := sharded.(*controllerv1.ShardedIngress)
	if !ok {
		return nil, fmt.Errorf("unsupported sharded object type: %T", sharded)
	}

	var children []DesiredChild

	shardedIngress := src.DeepCopy()
	if shardedIngress.Spec.Template.Labels == nil {
		shardedIngress.Spec.Template.Labels = make(map[string]string)
	}
	if shardedIngress.Spec.Template.Annotations == nil {
		shardedIngress.Spec.Template.Annotations = make(map[string]string)
	}

	tmpName := tmpChildName(shardedIngress.Name, plan.Shard.Number)

	// While migrating, a tmp child pinned to the old shard keeps serving
	// traffic until service discovery converges on the new shard.
	if plan.CreateTmp {
		tempShardedIngress := shardedIngress.DeepCopy()
		tempShardedIngress.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.OldShard
		tempShardedIngress.Spec.Template.Annotations[OldShardAnnotation] = plan.OldShard
		tmpIngress := renderIngress(tempShardedIngress, tmpName, plan.OldShard)
		children = append(children, DesiredChild{Shard: plan.Shard, Obj: tmpIngress})
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

	mainName := shardedIngress.Name
	if sharded.GetIngressClassName() != plan.Shard.Name {
		mainName = fmt.Sprintf("%s-%d", shardedIngress.Name, plan.Shard.Number)
	}

	main := DesiredChild{
		Shard: plan.Shard,
		Obj:   renderIngress(shardedIngress, mainName, plan.EffectiveClass),
	}
	// Mid-migration the tmp child must stay off the pruning list even in
	// passes where it is not rendered, so it is booked alongside the main
	// child.
	if plan.OldShard != "" {
		main.AlsoBook = []string{tmpName}
	}
	children = append(children, main)

	return children, nil
}

func renderIngress(shardedIngress *controllerv1.ShardedIngress, name, ingressClass string) *networkingv1.Ingress {
	return &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   shardedIngress.Namespace,
			Annotations: shardedIngress.Spec.Template.Annotations,
			Labels:      shardedIngress.Spec.Template.Labels,
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &ingressClass,
			DefaultBackend:   shardedIngress.Spec.Template.Spec.DefaultBackend,
			TLS:              shardedIngress.Spec.Template.Spec.TLS,
			Rules:            shardedIngress.Spec.Template.Spec.Rules,
		},
	}
}
