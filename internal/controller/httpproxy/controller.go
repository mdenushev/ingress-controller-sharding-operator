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

// Controller bundles the lifecycle engine with the HTTPProxy-specific
// adapter and renderer; the reconciliation loop itself lives in the embedded
// engine.
type Controller struct {
	*engine.Engine[*contourv1.HTTPProxy]
}

func NewController(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	settings engine.Settings,
) *Controller {
	return &Controller{
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

func (r *Controller) SetupWithManager(mgr ctrl.Manager, parallel, qps, burst int) error {
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
