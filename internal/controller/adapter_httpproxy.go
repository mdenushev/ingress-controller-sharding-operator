package controller

import (
	"fmt"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// httpProxyAdapter adapts Contour HTTPProxy children to the engine.
type httpProxyAdapter struct{}

func newHTTPProxyAdapter(Settings) *httpProxyAdapter { return &httpProxyAdapter{} }

func (a *httpProxyAdapter) Kind() string { return "HTTPProxy" }

func (a *httpProxyAdapter) ListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "projectcontour.io", Version: "v1", Kind: "HTTPProxyList"}
}

func (a *httpProxyAdapter) NewObject() client.Object {
	return &contourv1.HTTPProxy{}
}

func (a *httpProxyAdapter) Equal(existing, desired client.Object) (bool, error) {
	old, ok := existing.(*contourv1.HTTPProxy)
	if !ok {
		return false, fmt.Errorf("do not know how to compare %T and %T", existing, desired)
	}
	new, ok := desired.(*contourv1.HTTPProxy)
	if !ok {
		return false, fmt.Errorf("do not know how to compare %T and %T", existing, desired)
	}
	return apiequality.Semantic.DeepEqual(old.Annotations, new.Annotations) &&
		apiequality.Semantic.DeepEqual(old.Spec, new.Spec) &&
		apiequality.Semantic.DeepEqual(old.Labels, new.Labels) &&
		apiequality.Semantic.DeepEqual(old.OwnerReferences, new.OwnerReferences), nil
}

func (a *httpProxyAdapter) Merge(existing, desired client.Object) (client.Object, error) {
	old, ok := existing.(*contourv1.HTTPProxy)
	if !ok {
		return nil, fmt.Errorf("unsupported object type: %T", existing)
	}
	new, ok := desired.(*contourv1.HTTPProxy)
	if !ok {
		return nil, fmt.Errorf("unsupported object type: %T", desired)
	}
	old.Spec = new.Spec
	old.Annotations = new.Annotations
	old.Labels = new.Labels
	old.OwnerReferences = new.OwnerReferences
	return old, nil
}
