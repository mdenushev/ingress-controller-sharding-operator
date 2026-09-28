package engine

import (
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// SetupWithManager wires an engine into the manager with the shared
// rate-limiter options: exponential backoff per object plus a global API
// token bucket.
func SetupWithManager[C client.Object](
	mgr ctrl.Manager,
	e *Engine[C],
	parent client.Object,
	child client.Object,
	parallel, qps, burst int,
) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(parent).Owns(child).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: parallel,
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter(
				workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](
					ExponentialBackoffBaseDelay,
					ExponentialBackoffMaxDelay,
				),
				&workqueue.TypedBucketRateLimiter[ctrl.Request]{Limiter: rate.NewLimiter(rate.Limit(qps), burst)},
			),
		}).
		Complete(e)
}
