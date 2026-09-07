// Package ingress reconciles ShardedIngress parents into per-shard
// networking/v1 Ingress children: the type-specific adapter and renderer
// plugged into the shared lifecycle engine.
package ingress

import (
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

// Reconciler reconciles a ShardedIngress into per-shard Ingress children.
type Reconciler struct {
	*engine.Engine[*networkingv1.Ingress]
}

func NewReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	settings engine.Settings,
) *Reconciler {
	return &Reconciler{
		Engine: engine.NewEngine(
			c, scheme, recorder, settings,
			newAdapter(settings),
			newRenderer(settings),
			func() engine.ShardedObject {
				return &controllerv1.ShardedIngress{
					TypeMeta: metav1.TypeMeta{Kind: "ShardedIngress", APIVersion: controllerv1.GroupVersion.String()},
				}
			},
			"shardedingress",
		),
	}
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, parallel, qps, burst int) error {
	return engine.SetupWithManager(
		mgr,
		r.Engine,
		&controllerv1.ShardedIngress{},
		&networkingv1.Ingress{},
		parallel,
		qps,
		burst,
	)
}
