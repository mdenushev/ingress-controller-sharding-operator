package httpproxy

import (
	"testing"

	. "github.com/onsi/gomega"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

func newVirtualHostsParent(hosts string) *controllerv1.ShardedHTTPProxy {
	return &controllerv1.ShardedHTTPProxy{
		Kind: "ShardedHTTPProxy", APIVersion: controllerv1.GroupVersion.String(),
		Name: "app", Namespace: "default",
		Annotations: map[string]string{testVHAnnotation: hosts},
		Spec: controllerv1.ShardedHTTPProxySpec{
			Template: controllerv1.HTTPProxyTemplateSpec{
				Spec: contourv1.HTTPProxySpec{
					IngressClassName: "new-class",
					VirtualHost:      &contourv1.VirtualHost{Fqdn: "app.example.com"},
				},
			},
		},
	}
}

func renderNames(g Gomega, b *renderer, hosts string) []string {
	plan := engine.ShardPlan{
		Shard:          engine.Shard{Number: 0, Name: testNewShardClass},
		EffectiveClass: testNewShardClass,
	}
	children, err := b.RenderChildren(newVirtualHostsParent(hosts), plan)
	g.Expect(err).NotTo(HaveOccurred())

	names := make([]string, 0, len(children))
	for _, child := range children {
		names = append(names, child.Obj.GetName())
	}
	return names
}

// By default the extra virtual-host children are named by the annotation
// list index, so the order of the annotation defines the names.
func TestVirtualHostChildNamesByIndex(t *testing.T) {
	g := NewWithT(t)
	b := newRenderer(engine.Settings{
		ServiceDiscoveryClassLabel: testClassLabel,
		RootHTTPProxyLabel:         testRootLabel,
		VirtualHostsAnnotation:     testVHAnnotation,
	})

	g.Expect(renderNames(g, b, "a.example.com,b.example.com")).
		To(Equal([]string{"app-0", "app-0-0", "app-0-1"}))
}

// With hashed names enabled every virtual-host child is named by a hash of
// its host, so reordering the annotation keeps the same names and the engine
// has nothing to delete or recreate.
func TestVirtualHostChildNamesHashedAreOrderIndependent(t *testing.T) {
	g := NewWithT(t)
	b := newRenderer(engine.Settings{
		ServiceDiscoveryClassLabel: testClassLabel,
		RootHTTPProxyLabel:         testRootLabel,
		VirtualHostsAnnotation:     testVHAnnotation,
		HashedVirtualHostNames:     true,
	})

	names := renderNames(g, b, "a.example.com,b.example.com")
	g.Expect(names).To(HaveLen(3))
	g.Expect(names[0]).To(Equal("app-0"))
	g.Expect(names[1]).To(MatchRegexp(`^app-0-[0-9a-f]{16}$`))
	g.Expect(names[2]).To(MatchRegexp(`^app-0-[0-9a-f]{16}$`))
	g.Expect(names[1]).NotTo(Equal(names[2]))

	// Reordering the annotation must produce the same name set.
	reordered := renderNames(g, b, "b.example.com,a.example.com")
	g.Expect(reordered).To(ConsistOf(names[0], names[1], names[2]))
	g.Expect(reordered[1]).To(Equal(names[2]), "each host must keep its own name")
	g.Expect(reordered[2]).To(Equal(names[1]), "each host must keep its own name")
}
