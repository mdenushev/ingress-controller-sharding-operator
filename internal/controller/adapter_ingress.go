package controller

import (
	"fmt"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ingressAdapter adapts networking/v1 Ingress children to the engine.
type ingressAdapter struct {
	domainSubstring           string
	mutatingWebhookAnnotation string
}

func newIngressAdapter(settings Settings) *ingressAdapter {
	return &ingressAdapter{
		domainSubstring:           settings.DomainSubstring,
		mutatingWebhookAnnotation: settings.MutatingWebhookAnnotation,
	}
}

func (a *ingressAdapter) Kind() string { return "Ingress" }

func (a *ingressAdapter) ListGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "IngressList"}
}

func (a *ingressAdapter) NewObject() client.Object {
	return &networkingv1.Ingress{}
}

// Equal compares an existing Ingress with the desired one. When the cluster's
// mutating webhook rewrites hosts, the fields it manages (server-alias
// annotation and TLS hosts outside the main domain) are treated as equal as
// long as the desired values are a subset of the existing ones — otherwise
// every reconcile would fight the webhook.
func (a *ingressAdapter) Equal(existing, desired client.Object) (bool, error) {
	old, ok := existing.(*networkingv1.Ingress)
	if !ok {
		return false, fmt.Errorf("do not know how to compare %T and %T", existing, desired)
	}
	new, ok := desired.(*networkingv1.Ingress)
	if !ok {
		return false, fmt.Errorf("do not know how to compare %T and %T", existing, desired)
	}

	mutateHostsValue, exists := new.Annotations[a.mutatingWebhookAnnotation]
	if exists && mutateHostsValue != "" && mutateHostsValue != "false" {
		oldAnnotations := old.Annotations
		newAnnotations := new.Annotations

		newServerAlias := newAnnotations["nginx.ingress.kubernetes.io/server-alias"]
		oldServerAlias := oldAnnotations["nginx.ingress.kubernetes.io/server-alias"]
		if newServerAlias == "" && oldServerAlias != "" {
			newAnnotations["nginx.ingress.kubernetes.io/server-alias"] = oldAnnotations["nginx.ingress.kubernetes.io/server-alias"]
		} else if newServerAlias != "" && oldServerAlias != "" {
			allExist := true
			for _, alias := range strings.Split(newServerAlias, ",") {
				if !strings.Contains(oldServerAlias, alias) {
					allExist = false
					break
				}
			}
			if allExist {
				newAnnotations["nginx.ingress.kubernetes.io/server-alias"] = oldServerAlias
			}
		} else if newServerAlias == "" && oldServerAlias == "" {
			delete(newAnnotations, "nginx.ingress.kubernetes.io/server-alias")
			delete(oldAnnotations, "nginx.ingress.kubernetes.io/server-alias")
		}

		old.Annotations = oldAnnotations
		new.Annotations = newAnnotations

		newTLS := new.Spec.TLS
		oldTLS := old.Spec.TLS
		allTLSExist := true

		// Check if all hosts in newTLS exist in oldTLS
		for _, newTLSHost := range newTLS {
			for _, host := range newTLSHost.Hosts {
				hostExists := false
				for _, oldTLSHost := range oldTLS {
					if contains(oldTLSHost.Hosts, host) {
						hostExists = true
						break
					}
				}
				if !hostExists {
					allTLSExist = false
					break
				}
			}
			if !allTLSExist {
				break
			}
		}

		// Check if any host in oldTLS does not exist in newTLS and does not
		// contain the main domain substring, because that means that
		// additional hosts were removed.
		for _, oldTLSHost := range oldTLS {
			for _, host := range oldTLSHost.Hosts {
				if !containsAllHosts(newTLS, host) && !strings.Contains(host, a.domainSubstring) {
					allTLSExist = false
					break
				}
			}
			if !allTLSExist {
				break
			}
		}

		// No need to update the TLS block if all conditions are met
		if allTLSExist {
			new.Spec.TLS = old.Spec.TLS
		}
	}

	return apiequality.Semantic.DeepEqual(old.Annotations, new.Annotations) &&
		apiequality.Semantic.DeepEqual(old.Spec, new.Spec) &&
		apiequality.Semantic.DeepEqual(old.Labels, new.Labels) &&
		apiequality.Semantic.DeepEqual(old.OwnerReferences, new.OwnerReferences), nil
}

// Merge copies the desired spec and metadata onto the existing Ingress so the
// update keeps resourceVersion and server-populated fields.
func (a *ingressAdapter) Merge(existing, desired client.Object) (client.Object, error) {
	old, ok := existing.(*networkingv1.Ingress)
	if !ok {
		return nil, fmt.Errorf("unsupported object type: %T", existing)
	}
	new, ok := desired.(*networkingv1.Ingress)
	if !ok {
		return nil, fmt.Errorf("unsupported object type: %T", desired)
	}
	old.Spec = new.Spec
	old.Annotations = new.Annotations
	old.Labels = new.Labels
	old.OwnerReferences = new.OwnerReferences
	return old, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func containsAllHosts(tlsList []networkingv1.IngressTLS, host string) bool {
	for _, tls := range tlsList {
		if contains(tls.Hosts, host) {
			return true
		}
	}
	return false
}
