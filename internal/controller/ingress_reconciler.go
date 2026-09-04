package controller

import (
	"golang.org/x/time/rate"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// ShardedIngressReconciler reconciles a ShardedIngress into per-shard
// Ingress children.
type ShardedIngressReconciler struct {
	*Engine[*networkingv1.Ingress]
}

func NewShardedIngressReconciler(c client.Client, scheme *runtime.Scheme, recorder record.EventRecorder, settings Settings) *ShardedIngressReconciler {
	return &ShardedIngressReconciler{
		Engine: NewEngine(
			c, scheme, recorder, settings,
			newIngressAdapter(settings),
			newIngressRenderer(settings),
			func() ShardedObject {
				return &controllerv1.ShardedIngress{
					TypeMeta: metav1.TypeMeta{Kind: "ShardedIngress", APIVersion: controllerv1.GroupVersion.String()},
				}
			},
			"shardedingress",
		),
	}
}

func (r *ShardedIngressReconciler) SetupWithManager(mgr ctrl.Manager, parallel int, qps int, burst int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&controllerv1.ShardedIngress{}).Owns(&networkingv1.Ingress{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: parallel,
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter(
				workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](ExponentialBackoffBaseDelay, ExponentialBackoffMaxDelay),
				&workqueue.TypedBucketRateLimiter[ctrl.Request]{Limiter: rate.NewLimiter(rate.Limit(qps), burst)},
			)}).
		Complete(r)
}
