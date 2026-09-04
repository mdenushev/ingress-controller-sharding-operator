//go:build e2e

// Package e2e runs the operator lifecycle against a real cluster (kind in
// CI): child creation, spec updates, resharding between ingress classes and
// finalizer-driven deletion. The operator must already be deployed with the
// config from test/e2e/manifests (short migration windows), see the e2e
// GitHub workflow or `make test-e2e` prerequisites.
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

const (
	oldClass      = "e2e-a"
	oldShardClass = "e2e-a-0"
	newClass      = "e2e-b"
	newShardClass = "e2e-b-0"

	parentName = "app"
	childName  = "app-0"
	tmpName    = "app-0-tmp"
)

func newE2EClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := controllerv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		t.Fatalf("unable to load kubeconfig: %v", err)
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("unable to build client: %v", err)
	}
	return cl
}

func newParent(namespace string) *controllerv1.ShardedIngress {
	className := oldClass
	return &controllerv1.ShardedIngress{
		ObjectMeta: metav1.ObjectMeta{Name: parentName, Namespace: namespace},
		Spec: controllerv1.ShardedIngressSpec{
			Template: &controllerv1.IngressTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": parentName},
				},
				Spec: networkingv1.IngressSpec{
					IngressClassName: &className,
					Rules: []networkingv1.IngressRule{{
						Host: "app.e2e.cluster.local",
						IngressRuleValue: networkingv1.IngressRuleValue{
							HTTP: &networkingv1.HTTPIngressRuleValue{
								Paths: []networkingv1.HTTPIngressPath{{
									Path:     "/",
									PathType: ptr(networkingv1.PathTypePrefix),
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: parentName,
											Port: networkingv1.ServiceBackendPort{Number: 80},
										},
									},
								}},
							},
						},
					}},
				},
			},
		},
	}
}

func ptr[T any](v T) *T { return &v }

func TestShardedIngressLifecycle(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cl := newE2EClient(t)

	namespace := fmt.Sprintf("sharding-e2e-%d", time.Now().Unix())
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	g.Expect(cl.Create(ctx, ns)).To(Succeed())
	t.Cleanup(func() { _ = cl.Delete(context.Background(), ns) })

	parent := newParent(namespace)
	g.Expect(cl.Create(ctx, parent)).To(Succeed())

	parentKey := types.NamespacedName{Namespace: namespace, Name: parentName}
	childKey := types.NamespacedName{Namespace: namespace, Name: childName}
	tmpKey := types.NamespacedName{Namespace: namespace, Name: tmpName}

	getPhase := func() controllerv1.ShardedPhase {
		got := &controllerv1.ShardedIngress{}
		if err := cl.Get(ctx, parentKey, got); err != nil {
			return ""
		}
		return got.Status.Phase
	}
	getChildClass := func(key types.NamespacedName) string {
		child := &networkingv1.Ingress{}
		if err := cl.Get(ctx, key, child); err != nil {
			return ""
		}
		if child.Spec.IngressClassName == nil {
			return ""
		}
		return *child.Spec.IngressClassName
	}
	eventReasons := func() []string {
		events := &corev1.EventList{}
		if err := cl.List(ctx, events, client.InNamespace(namespace)); err != nil {
			return nil
		}
		var reasons []string
		for _, ev := range events.Items {
			if ev.InvolvedObject.Name == parentName {
				reasons = append(reasons, ev.Reason)
			}
		}
		return reasons
	}

	t.Run("child is created and parent becomes Ready", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func() string { return getChildClass(childKey) }, 2*time.Minute, 2*time.Second).
			Should(Equal(oldShardClass), "child ingress must appear on the old shard")
		g.Eventually(getPhase, 2*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))

		got := &controllerv1.ShardedIngress{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		g.Expect(got.Status.CreatedObjects).To(HaveKey(oldShardClass))
		g.Expect(got.Finalizers).NotTo(BeEmpty())

		g.Eventually(eventReasons, time.Minute, 2*time.Second).
			Should(ContainElement("ChildCreated"), "parent must record a ChildCreated event")
	})

	t.Run("spec update propagates to the child", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedIngress{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		got.Spec.Template.Spec.Rules[0].Host = "app-v2.e2e.cluster.local"
		g.Expect(cl.Update(ctx, got)).To(Succeed())

		g.Eventually(func() string {
			child := &networkingv1.Ingress{}
			if err := cl.Get(ctx, childKey, child); err != nil || len(child.Spec.Rules) == 0 {
				return ""
			}
			return child.Spec.Rules[0].Host
		}, 2*time.Minute, 2*time.Second).Should(Equal("app-v2.e2e.cluster.local"))
		g.Eventually(getPhase, 2*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))
	})

	t.Run("resharding migrates the child through a tmp object", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedIngress{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		got.Spec.Template.Spec.IngressClassName = ptr(newClass)
		g.Expect(cl.Update(ctx, got)).To(Succeed())

		// Step 1: a tmp child pinned to the old shard appears.
		g.Eventually(func() string { return getChildClass(tmpKey) }, 2*time.Minute, time.Second).
			Should(Equal(oldShardClass), "tmp child must keep the old shard serving")

		// Step 2: the main child moves to the new shard.
		g.Eventually(func() string { return getChildClass(childKey) }, 3*time.Minute, 2*time.Second).
			Should(Equal(newShardClass), "main child must move to the new shard")

		// Step 3: the tmp child is gracefully removed.
		g.Eventually(func() bool {
			err := cl.Get(ctx, tmpKey, &networkingv1.Ingress{})
			return apierrors.IsNotFound(err)
		}, 3*time.Minute, 2*time.Second).Should(BeTrue(), "tmp child must be deleted after the migration window")

		g.Eventually(getPhase, 3*time.Minute, 2*time.Second).
			Should(Equal(controllerv1.PhaseReady))
		g.Eventually(eventReasons, time.Minute, 2*time.Second).
			Should(ContainElement("ReshardingStarted"))

		g.Eventually(func() bool {
			got := &controllerv1.ShardedIngress{}
			if err := cl.Get(ctx, parentKey, got); err != nil {
				return false
			}
			_, oldRecorded := got.Status.CreatedObjects[oldShardClass]
			_, newRecorded := got.Status.CreatedObjects[newShardClass]
			return newRecorded && !oldRecorded
		}, 3*time.Minute, 2*time.Second).Should(BeTrue(), "status must record the child under the new shard only")
	})

	t.Run("deletion drains children before removing the finalizer", func(t *testing.T) {
		g := NewWithT(t)
		got := &controllerv1.ShardedIngress{}
		g.Expect(cl.Get(ctx, parentKey, got)).To(Succeed())
		g.Expect(cl.Delete(ctx, got)).To(Succeed())

		g.Eventually(func() bool {
			err := cl.Get(ctx, childKey, &networkingv1.Ingress{})
			return apierrors.IsNotFound(err)
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(), "children must be deleted")

		g.Eventually(func() bool {
			err := cl.Get(ctx, parentKey, &controllerv1.ShardedIngress{})
			return apierrors.IsNotFound(err)
		}, 2*time.Minute, 2*time.Second).Should(BeTrue(), "parent must go away once children are drained")
	})
}
