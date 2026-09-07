package controller

import (
	"slices"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// serverAliasAnnotation is the nginx annotation the cluster's mutating
// webhook manages on the children.
const serverAliasAnnotation = "nginx.ingress.kubernetes.io/server-alias"

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

func (a *ingressAdapter) NewObject() *networkingv1.Ingress {
	return &networkingv1.Ingress{}
}

// Equal compares an existing Ingress with the desired one. When the cluster's
// mutating webhook rewrites hosts, the fields it manages (server-alias
// annotation and TLS hosts outside the main domain) are treated as equal as
// long as the desired values are a subset of the existing ones — otherwise
// every reconcile would fight the webhook.
func (a *ingressAdapter) Equal(existing, desired *networkingv1.Ingress) (bool, error) {
	if a.webhookManagesHosts(desired) {
		alignServerAlias(existing, desired)
		// No need to update the TLS block if the desired hosts are
		// already covered by the existing one.
		if a.desiredTLSSubsetOfExisting(existing, desired) {
			desired.Spec.TLS = existing.Spec.TLS
		}
	}

	return apiequality.Semantic.DeepEqual(existing.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(existing.Spec, desired.Spec) &&
		apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(existing.OwnerReferences, desired.OwnerReferences), nil
}

// webhookManagesHosts reports whether the mutating webhook rewrites the hosts
// of the desired Ingress.
func (a *ingressAdapter) webhookManagesHosts(desired *networkingv1.Ingress) bool {
	mutateHostsValue, exists := desired.Annotations[a.mutatingWebhookAnnotation]
	return exists && mutateHostsValue != "" && mutateHostsValue != "false"
}

// alignServerAlias treats the webhook-managed server-alias annotation as
// unchanged when the desired aliases are a subset of the existing ones, by
// copying the existing value onto the desired object before the comparison.
func alignServerAlias(existing, desired *networkingv1.Ingress) {
	newServerAlias := desired.Annotations[serverAliasAnnotation]
	oldServerAlias := existing.Annotations[serverAliasAnnotation]
	switch {
	case newServerAlias == "" && oldServerAlias != "":
		desired.Annotations[serverAliasAnnotation] = oldServerAlias
	case newServerAlias != "" && oldServerAlias != "":
		allExist := true
		for _, alias := range strings.Split(newServerAlias, ",") {
			if !strings.Contains(oldServerAlias, alias) {
				allExist = false
				break
			}
		}
		if allExist {
			desired.Annotations[serverAliasAnnotation] = oldServerAlias
		}
	case newServerAlias == "" && oldServerAlias == "":
		delete(desired.Annotations, serverAliasAnnotation)
		delete(existing.Annotations, serverAliasAnnotation)
	}
}

// desiredTLSSubsetOfExisting reports whether every desired TLS host already
// exists in the existing TLS block and no webhook-added host (one outside the
// main domain) would be removed by applying the desired block.
func (a *ingressAdapter) desiredTLSSubsetOfExisting(existing, desired *networkingv1.Ingress) bool {
	newTLS := desired.Spec.TLS
	oldTLS := existing.Spec.TLS

	// Check if all hosts in newTLS exist in oldTLS.
	for _, newTLSHost := range newTLS {
		for _, host := range newTLSHost.Hosts {
			if !tlsContainsHost(oldTLS, host) {
				return false
			}
		}
	}

	// Check if any host in oldTLS does not exist in newTLS and does not
	// contain the main domain substring, because that means that
	// additional hosts were removed.
	for _, oldTLSHost := range oldTLS {
		for _, host := range oldTLSHost.Hosts {
			if !tlsContainsHost(newTLS, host) && !strings.Contains(host, a.domainSubstring) {
				return false
			}
		}
	}
	return true
}

// Merge copies the desired spec and metadata onto the existing Ingress so the
// update keeps resourceVersion and server-populated fields.
func (a *ingressAdapter) Merge(existing, desired *networkingv1.Ingress) *networkingv1.Ingress {
	existing.Spec = desired.Spec
	existing.Annotations = desired.Annotations
	existing.Labels = desired.Labels
	existing.OwnerReferences = desired.OwnerReferences
	return existing
}

func tlsContainsHost(tlsList []networkingv1.IngressTLS, host string) bool {
	for _, tls := range tlsList {
		if slices.Contains(tls.Hosts, host) {
			return true
		}
	}
	return false
}
