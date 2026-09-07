package controller

import (
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// httpProxyAdapter adapts Contour HTTPProxy children to the engine.
type httpProxyAdapter struct{}

func newHTTPProxyAdapter(Settings) *httpProxyAdapter { return &httpProxyAdapter{} }

func (a *httpProxyAdapter) Kind() string { return "HTTPProxy" }

func (a *httpProxyAdapter) ListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "projectcontour.io", Version: "v1", Kind: "HTTPProxyList"}
}

func (a *httpProxyAdapter) NewObject() *contourv1.HTTPProxy {
	return &contourv1.HTTPProxy{}
}

func (a *httpProxyAdapter) Equal(existing, desired *contourv1.HTTPProxy) (bool, error) {
	return apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) &&
		apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(existing.OwnerReferences, desired.OwnerReferences), nil
}

func (a *httpProxyAdapter) Merge(existing, desired *contourv1.HTTPProxy) *contourv1.HTTPProxy {
	existing.Spec = desired.Spec
	existing.Annotations = desired.Annotations
	existing.Labels = desired.Labels
	existing.OwnerReferences = desired.OwnerReferences
	return existing
}
