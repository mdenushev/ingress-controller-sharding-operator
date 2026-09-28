package httpproxy

import (
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

// adapter adapts Contour HTTPProxy children to the engine.
type adapter struct{}

func newAdapter(engine.Settings) *adapter { return &adapter{} }

func (a *adapter) Kind() string { return "HTTPProxy" }

func (a *adapter) ListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "projectcontour.io", Version: "v1", Kind: "HTTPProxyList"}
}

func (a *adapter) NewObject() *contourv1.HTTPProxy {
	return &contourv1.HTTPProxy{}
}

func (a *adapter) Equal(existing, desired *contourv1.HTTPProxy) (bool, error) {
	return apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) &&
		apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(existing.OwnerReferences, desired.OwnerReferences), nil
}

func (a *adapter) Merge(existing, desired *contourv1.HTTPProxy) *contourv1.HTTPProxy {
	existing.Spec = desired.Spec
	existing.Annotations = desired.Annotations
	existing.Labels = desired.Labels
	existing.OwnerReferences = desired.OwnerReferences
	return existing
}
