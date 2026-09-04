package controller

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// TestReconcileLifecycleToReady drives a fresh ShardedHTTPProxy through the
// whole loop: Pending (waiting for an apply slot) -> Provisioning (child
// created) -> Ready, checking the phase, the Ready condition, the created
// child and the recorded events along the way.
func TestReconcileLifecycleToReady(t *testing.T) {
	g := NewWithT(t)

	testScheme := runtime.NewScheme()
	g.Expect(controllerv1.AddToScheme(testScheme)).To(Succeed())
	g.Expect(contourv1.AddToScheme(testScheme)).To(Succeed())
	g.Expect(networkingv1.AddToScheme(testScheme)).To(Succeed())

	sharded := &controllerv1.ShardedHTTPProxy{
		TypeMeta:   metav1.TypeMeta{Kind: "ShardedHTTPProxy", APIVersion: controllerv1.GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
		Spec: controllerv1.ShardedHTTPProxySpec{
			Template: controllerv1.HTTPProxyTemplateSpec{
				Spec: contourv1.HTTPProxySpec{
					IngressClassName: "new-class",
					VirtualHost:      &contourv1.VirtualHost{Fqdn: "app.example.com"},
				},
			},
		},
	}
	shardClass := &networkingv1.IngressClass{ObjectMeta: metav1.ObjectMeta{Name: "new-class-0"}}

	settings := Settings{
		MaxShards:                  map[string]int{"new-class": 1},
		TerminationPeriod:          time.Minute,
		ShardUpdateCooldown:        time.Millisecond,
		ServiceDiscoveryClassLabel: testClassLabel,
		RootHTTPProxyLabel:         testRootLabel,
		VirtualHostsAnnotation:     testVHAnnotation,
		UnregisterAnnotation:       testUnregisterAnnotation,
		FinalizerKey:               "test/finalizer",
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&controllerv1.ShardedHTTPProxy{}).
		WithObjects(sharded, shardClass).
		Build()
	recorder := record.NewFakeRecorder(64)

	engine := NewEngine(
		fakeClient, testScheme, recorder, settings,
		newHTTPProxyAdapter(settings),
		newHTTPProxyBuilder(settings),
		func() ShardedObject {
			return &controllerv1.ShardedHTTPProxy{
				TypeMeta: metav1.TypeMeta{Kind: "ShardedHTTPProxy", APIVersion: controllerv1.GroupVersion.String()},
			}
		},
		"shardedhttpproxy",
	)

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "app"}}

	var phases []controllerv1.ShardedPhase
	for i := 0; i < 6; i++ {
		_, err := engine.Reconcile(ctx, req)
		g.Expect(err).NotTo(HaveOccurred(), "pass %d", i)

		got := &controllerv1.ShardedHTTPProxy{}
		g.Expect(fakeClient.Get(ctx, req.NamespacedName, got)).To(Succeed())
		if len(phases) == 0 || phases[len(phases)-1] != got.Status.Phase {
			phases = append(phases, got.Status.Phase)
		}
		if got.Status.Phase == controllerv1.PhaseReady {
			break
		}
	}

	g.Expect(phases).To(Equal([]controllerv1.ShardedPhase{
		controllerv1.PhasePending,
		controllerv1.PhaseProvisioning,
		controllerv1.PhaseReady,
	}))

	// The child must exist on the shard with the service discovery label.
	child := &contourv1.HTTPProxy{}
	g.Expect(fakeClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "app-0"}, child)).To(Succeed())
	g.Expect(child.Spec.IngressClassName).To(Equal("new-class-0"))
	g.Expect(child.Labels).To(HaveKeyWithValue(testClassLabel, "new-class-0"))
	g.Expect(child.Labels).To(HaveKeyWithValue(testRootLabel, "true"))

	// The parent status must record the child, carry a finalizer and a
	// True Ready condition.
	got := &controllerv1.ShardedHTTPProxy{}
	g.Expect(fakeClient.Get(ctx, req.NamespacedName, got)).To(Succeed())
	g.Expect(got.Finalizers).To(ContainElement("test/finalizer"))
	g.Expect(got.Status.CreatedObjects).To(HaveKeyWithValue("new-class-0",
		[]map[string]string{{"kind": "HTTPProxy", "name": "app-0"}}))
	g.Expect(got.Status.ObservedGeneration).To(Equal(got.Generation))

	var ready, resharding *metav1.Condition
	for i := range got.Status.Conditions {
		switch got.Status.Conditions[i].Type {
		case controllerv1.ConditionReady:
			ready = &got.Status.Conditions[i]
		case controllerv1.ConditionResharding:
			resharding = &got.Status.Conditions[i]
		}
	}
	g.Expect(ready).NotTo(BeNil())
	g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(resharding).NotTo(BeNil())
	g.Expect(resharding.Status).To(Equal(metav1.ConditionFalse))

	// A ChildCreated event must have been recorded.
	events := drainEvents(recorder)
	g.Expect(events).To(ContainElement(ContainSubstring(EventChildCreated)))
}

func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case ev := <-recorder.Events:
			events = append(events, ev)
		default:
			return events
		}
	}
}
