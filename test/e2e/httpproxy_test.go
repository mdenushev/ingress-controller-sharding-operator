//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// virtualHostsAnnotation is the default annotation the operator reads the
// extra virtual hosts from (config default).
const virtualHostsAnnotation = "k8s.tochka.com/virtual-hosts"

const (
	proxyHost      = "proxy.e2e.cluster.local"
	proxyAliasHost = "proxy-alias.e2e.cluster.local"

	// The root proxy keeps the parent name + shard number; every extra
	// virtual host gets its own proxy including the root.
	proxyChildName = "app-0"
	proxyAliasName = "app-0-0"
	proxyTmpName   = "app-0-tmp"
)

func newProxyParent(namespace string) *controllerv1.ShardedHTTPProxy {
	return &controllerv1.ShardedHTTPProxy{
		Name:        parentName,
		Namespace:   namespace,
		Annotations: map[string]string{virtualHostsAnnotation: proxyAliasHost},
		Spec: controllerv1.ShardedHTTPProxySpec{
			Template: controllerv1.HTTPProxyTemplateSpec{
				Labels: map[string]string{"app": parentName},
				Spec: contourv1.HTTPProxySpec{
					IngressClassName: oldClass,
					VirtualHost:      &contourv1.VirtualHost{Fqdn: proxyHost},
					Routes: []contourv1.Route{{
						Services: []contourv1.Service{{Name: parentName, Port: 80}},
					}},
				},
			},
		},
	}
}

// proxyPhaseGetter polls the parent's status.phase; "" while unreadable.
func proxyPhaseGetter(
	ctx context.Context,
	cl client.Client,
	key types.NamespacedName,
) func() controllerv1.ShardedPhase {
	return func() controllerv1.ShardedPhase {
		got := &controllerv1.ShardedHTTPProxy{}
		if err := cl.Get(ctx, key, got); err != nil {
			return ""
		}
		return got.Status.Phase
	}
}

// proxyClassGetter polls the ingress class of a child; "" while unreadable.
func proxyClassGetter(ctx context.Context, cl client.Client) func(key types.NamespacedName) string {
	return func(key types.NamespacedName) string {
		child := &contourv1.HTTPProxy{}
		if err := cl.Get(ctx, key, child); err != nil {
			return ""
		}
		return child.Spec.IngressClassName
	}
}

func TestShardedHTTPProxyLifecycle(t *testing.T) {
	gt := NewWithT(t)
	ctx := context.Background()
	cl := newE2EClient(t)

	namespace := fmt.Sprintf("sharding-e2e-proxy-%d", time.Now().Unix())
	ns := &corev1.Namespace{Name: namespace}
	gt.Expect(cl.Create(ctx, ns)).To(Succeed())
	t.Cleanup(func() { _ = cl.Delete(context.Background(), ns) })

	parent := newProxyParent(namespace)
	gt.Expect(cl.Create(ctx, parent)).To(Succeed())

	parentKey := types.NamespacedName{Namespace: namespace, Name: parentName}
	childKey := types.NamespacedName{Namespace: namespace, Name: proxyChildName}
	aliasKey := types.NamespacedName{Namespace: namespace, Name: proxyAliasName}
	tmpKey := types.NamespacedName{Namespace: namespace, Name: proxyTmpName}

	getPhase := proxyPhaseGetter(ctx, cl, parentKey)
	getChildClass := proxyClassGetter(ctx, cl)
	eventReasons := eventReasonsGetter(ctx, cl, namespace, parentName)

	t.Run("root and virtual-host children are created and parent becomes Ready", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func() string { return getChildClass(childKey) }, 2*time.Minute, 2*time.Second).
			Should(Equal(oldShardClass), "root proxy must appear on the old shard")

		// The root proxy carries the routes and the root label; the template's
		// virtual host is only rendered into the per-alias children.
		root := &contourv1.HTTPProxy{}
		g.Expect(cl.Get(ctx, childKey, root)).To(Succeed())
		g.Expect(root.Spec.Routes).NotTo(BeEmpty())
		g.Expect(root.Labels).To(HaveKeyWithValue("k8s.tochka.com/base-proxy", "true"),
			"root proxy must carry the root label")
		g.Expect(root.Labels).To(HaveKeyWithValue("k8s.tochka.com/ingress-class", oldShardClass))

		// The extra virtual host gets its own proxy that includes the root.
		g.Eventually(func() string { return getChildClass(aliasKey) }, 2*time.Minute, 2*time.Second).
			Should(Equal(oldShardClass), "alias proxy must appear on the old shard")
		alias := &contourv1.HTTPProxy{}
		g.Expect(cl.Get(ctx, aliasKey, alias)).To(Succeed())
		g.Expect(alias.Spec.VirtualHost).NotTo(BeNil())
		g.Expect(alias.Spec.VirtualHost.Fqdn).To(Equal(proxyAliasHost))
		g.Expect(alias.Spec.Includes).To(ContainElement(contourv1.Include{
			Name:      proxyChildName,
			Namespace: namespace,
		}), "alias proxy must route through the root proxy")

		g.Eventually(getPhase, 2*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))

		got := &controllerv1.ShardedHTTPProxy{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		g.Expect(got.Status.CreatedObjects).To(HaveKey(oldShardClass))
		g.Expect(got.Finalizers).NotTo(BeEmpty())

		g.Eventually(eventReasons, time.Minute, 2*time.Second).
			Should(ContainElement("ChildCreated"), "parent must record a ChildCreated event")
	})

	t.Run("spec update propagates to the children", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedHTTPProxy{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		// The template routes are rendered into every child, so a port change
		// must reach the root proxy.
		got.Spec.Template.Spec.Routes[0].Services[0].Port = 8080
		g.Expect(cl.Update(ctx, got)).To(Succeed())

		g.Eventually(func() int64 {
			child := &contourv1.HTTPProxy{}
			if err := cl.Get(ctx, childKey, child); err != nil ||
				len(child.Spec.Routes) == 0 || len(child.Spec.Routes[0].Services) == 0 {
				return 0
			}
			return int64(child.Spec.Routes[0].Services[0].Port)
		}, 2*time.Minute, 2*time.Second).Should(Equal(int64(8080)))
		g.Eventually(getPhase, 2*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))
	})

	t.Run("resharding migrates the children through a tmp object", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedHTTPProxy{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		got.Spec.Template.Spec.IngressClassName = newClass
		g.Expect(cl.Update(ctx, got)).To(Succeed())

		// Step 1: a tmp root proxy pinned to the old shard appears.
		g.Eventually(func() string { return getChildClass(tmpKey) }, 2*time.Minute, time.Second).
			Should(Equal(oldShardClass), "tmp proxy must keep the old shard serving")

		// Step 2: the main children move to the new shard.
		g.Eventually(func() string { return getChildClass(childKey) }, 3*time.Minute, 2*time.Second).
			Should(Equal(newShardClass), "root proxy must move to the new shard")
		g.Eventually(func() string { return getChildClass(aliasKey) }, 3*time.Minute, 2*time.Second).
			Should(Equal(newShardClass), "alias proxy must move to the new shard")

		// Step 3: the tmp proxy is gracefully removed.
		g.Eventually(func() bool {
			err := cl.Get(ctx, tmpKey, &contourv1.HTTPProxy{})
			return apierrors.IsNotFound(err)
		}, 3*time.Minute, 2*time.Second).Should(BeTrue(), "tmp proxy must be deleted after the migration window")

		g.Eventually(getPhase, 3*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))
		g.Eventually(eventReasons, time.Minute, 2*time.Second).
			Should(ContainElement("ReshardingStarted"))

		g.Eventually(func() bool {
			latest := &controllerv1.ShardedHTTPProxy{}
			if err := cl.Get(ctx, parentKey, latest); err != nil {
				return false
			}
			_, oldRecorded := latest.Status.CreatedObjects[oldShardClass]
			_, newRecorded := latest.Status.CreatedObjects[newShardClass]
			return newRecorded && !oldRecorded
		}, 3*time.Minute, 2*time.Second).Should(BeTrue(), "status must record the children under the new shard only")
	})

	t.Run("deletion drains children before removing the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedHTTPProxy{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		g.Expect(cl.Delete(ctx, got)).To(Succeed())

		for name, key := range map[string]types.NamespacedName{"root": childKey, "alias": aliasKey} {
			g.Eventually(func() bool {
				err := cl.Get(ctx, key, &contourv1.HTTPProxy{})
				return apierrors.IsNotFound(err)
			}, 2*time.Minute, 2*time.Second).Should(BeTrue(), "%s proxy must be deleted", name)
		}

		g.Eventually(func() bool {
			err := cl.Get(ctx, parentKey, &controllerv1.ShardedHTTPProxy{})
			return apierrors.IsNotFound(err)
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(), "parent must go away once children are drained")
	})
}
