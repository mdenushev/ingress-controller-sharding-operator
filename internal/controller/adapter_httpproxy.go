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

func (a *httpProxyAdapter) Equal(old, new *contourv1.HTTPProxy) (bool, error) {
	return apiequality.Semantic.DeepEqual(old.Annotations, new.Annotations) &&
		apiequality.Semantic.DeepEqual(old.Spec, new.Spec) &&
		apiequality.Semantic.DeepEqual(old.Labels, new.Labels) &&
		apiequality.Semantic.DeepEqual(old.OwnerReferences, new.OwnerReferences), nil
}

func (a *httpProxyAdapter) Merge(old, new *contourv1.HTTPProxy) *contourv1.HTTPProxy {
	old.Spec = new.Spec
	old.Annotations = new.Annotations
	old.Labels = new.Labels
	old.OwnerReferences = new.OwnerReferences
	return old
}
