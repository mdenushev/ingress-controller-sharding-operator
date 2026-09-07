// Package httpproxy reconciles ShardedHTTPProxy parents into per-shard
// Contour HTTPProxy children: the type-specific adapter and renderer plugged
// into the shared lifecycle engine.
package httpproxy

import (
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

// Reconciler reconciles a ShardedHTTPProxy into per-shard Contour HTTPProxy
// children.
type Reconciler struct {
	*engine.Engine[*contourv1.HTTPProxy]
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
				return &controllerv1.ShardedHTTPProxy{
					TypeMeta: metav1.TypeMeta{Kind: "ShardedHTTPProxy", APIVersion: controllerv1.GroupVersion.String()},
				}
			},
			"shardedhttpproxy",
		),
	}
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, parallel, qps, burst int) error {
	return engine.SetupWithManager(
		mgr,
		r.Engine,
		&controllerv1.ShardedHTTPProxy{},
		&contourv1.HTTPProxy{},
		parallel,
		qps,
		burst,
	)
}
