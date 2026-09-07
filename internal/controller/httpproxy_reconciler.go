package controller

import (
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// ShardedHTTPProxyReconciler reconciles a ShardedHTTPProxy into per-shard
// Contour HTTPProxy children.
type ShardedHTTPProxyReconciler struct {
	*Engine[*contourv1.HTTPProxy]
}

func NewShardedHTTPProxyReconciler(
	c client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
	settings Settings,
) *ShardedHTTPProxyReconciler {
	return &ShardedHTTPProxyReconciler{
		Engine: NewEngine(
			c, scheme, recorder, settings,
			newHTTPProxyAdapter(settings),
			newHTTPProxyRenderer(settings),
			func() ShardedObject {
				return &controllerv1.ShardedHTTPProxy{
					TypeMeta: metav1.TypeMeta{Kind: "ShardedHTTPProxy", APIVersion: controllerv1.GroupVersion.String()},
				}
			},
			"shardedhttpproxy",
		),
	}
}

func (r *ShardedHTTPProxyReconciler) SetupWithManager(mgr ctrl.Manager, parallel, qps, burst int) error {
	return setupWithManager(
		mgr,
		r.Engine,
		&controllerv1.ShardedHTTPProxy{},
		&contourv1.HTTPProxy{},
		parallel,
		qps,
		burst,
	)
}
