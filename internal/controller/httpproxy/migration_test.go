package httpproxy

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

const (
	testClassLabel           = "service-discovery/class"
	testRootLabel            = "httpproxy/root"
	testVHAnnotation         = "httpproxy/virtual-hosts"
	testUnregisterAnnotation = "service-discovery/unregister"
	testOldShardClass        = "old-class-0"
	testNewShardClass        = "new-class-0"
)

// noopScheduler lets every pass proceed immediately, so the migration tests
// exercise the desired-state logic without waiting for apply slots.
type noopScheduler struct{}

func (noopScheduler) NoteShard(string) {}

func (noopScheduler) Schedule(
	string, *controllerv1.ShardedStatus, []string, logr.Logger,
) (ctrl.Result, bool) {
	return ctrl.Result{}, false
}

// newMigratingShardedHTTPProxy returns a ShardedHTTPProxy that has been
// switched to ingress class "new-class" while its status still records the
// child object under the old shard, i.e. mid class migration.
func newMigratingShardedHTTPProxy() *controllerv1.ShardedHTTPProxy {
	return &controllerv1.ShardedHTTPProxy{
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
		Status: controllerv1.ShardedStatus{
			CreatedObjects: map[string][]map[string]string{
				testOldShardClass: {{"kind": "HTTPProxy", "name": "app-0"}},
			},
		},
	}
}

// newTestReconciler wires a Reconciler over a fake client that carries the
// parent, the target shard's IngressClass and any pre-existing children. The
// scheduler is stubbed out so passes never wait for a slot.
func newTestReconciler(
	t *testing.T,
	sharded *controllerv1.ShardedHTTPProxy,
	existing ...client.Object,
) (*Reconciler, ctrl.Request) {
	t.Helper()

	testScheme := runtime.NewScheme()
	if err := controllerv1.AddToScheme(testScheme); err != nil {
		t.Fatal(err)
	}
	if err := contourv1.AddToScheme(testScheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(testScheme); err != nil {
		t.Fatal(err)
	}

	settings := engine.Settings{
		MaxShards:                  map[string]int{"new-class": 1},
		TerminationPeriod:          time.Minute,
		ServiceDiscoveryClassLabel: testClassLabel,
		RootHTTPProxyLabel:         testRootLabel,
		VirtualHostsAnnotation:     testVHAnnotation,
		UnregisterAnnotation:       testUnregisterAnnotation,
		FinalizerKey:               "test/finalizer",
	}

	// The shard's IngressClass must exist, otherwise start-up discovery
	// lowers MaxShards to 0 and disables sharding for the class.
	shardClass := &networkingv1.IngressClass{ObjectMeta: metav1.ObjectMeta{Name: testNewShardClass}}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&controllerv1.ShardedHTTPProxy{}).
		WithObjects(append([]client.Object{sharded, shardClass}, existing...)...).
		Build()

	r := NewReconciler(fakeClient, testScheme, nil, settings)
	r.Scheduler = noopScheduler{}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: sharded.Namespace, Name: sharded.Name}}
	return r, req
}

func getHTTPProxy(g Gomega, r *Reconciler, name string) *contourv1.HTTPProxy {
	proxy := &contourv1.HTTPProxy{}
	g.Expect(r.Client.Get(
		context.Background(),
		types.NamespacedName{Namespace: "default", Name: name},
		proxy,
	)).To(Succeed())
	return proxy
}

// During class migration the tmp object must carry the OLD shard class in
// both spec.ingressClassName and the service discovery label. A regression
// here (empty class) poisons the tmp object's old-shard annotation and makes
// every subsequent reconcile wipe the class from the main child object,
// which then loops create/delete forever.
func TestReconcileMigrationCreatesTmpWithOldClass(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()

	r, req := newTestReconciler(t, newMigratingShardedHTTPProxy())

	// Pass 1 starts the migration: the tmp child appears on the old shard.
	_, err := r.Reconcile(ctx, req)
	g.Expect(err).NotTo(HaveOccurred())

	tmp := getHTTPProxy(g, r, "app-0-tmp")
	g.Expect(tmp.Spec.IngressClassName).To(Equal(testOldShardClass))
	g.Expect(tmp.Labels).To(HaveKeyWithValue(testClassLabel, testOldShardClass))
	g.Expect(tmp.Labels).To(HaveKeyWithValue(testRootLabel, "true"))
	g.Expect(tmp.Annotations).To(HaveKeyWithValue(engine.OldShardAnnotation, testOldShardClass))

	// The migration pass must surface as the Resharding phase.
	parent := &controllerv1.ShardedHTTPProxy{}
	g.Expect(r.Client.Get(ctx, req.NamespacedName, parent)).To(Succeed())
	g.Expect(parent.Status.Phase).To(Equal(controllerv1.PhaseResharding))

	// Pass 2 creates the main child, still on the old class while the tmp
	// child keeps the old shard serving.
	_, err = r.Reconcile(ctx, req)
	g.Expect(err).NotTo(HaveOccurred())

	main := getHTTPProxy(g, r, "app-0")
	g.Expect(main.Spec.IngressClassName).To(Equal(testOldShardClass))
	g.Expect(main.Labels).To(HaveKeyWithValue(testClassLabel, testOldShardClass))

	// The main child is accounted under the new shard so that pruning does
	// not schedule it for deletion mid-migration.
	g.Expect(r.Client.Get(ctx, req.NamespacedName, parent)).To(Succeed())
	g.Expect(parent.Status.CreatedObjects[testNewShardClass]).
		To(ContainElement(HaveKeyWithValue("name", "app-0")))
}

// While the tmp object exists and its deletion window has not started, the
// main child object must keep the old shard class taken from the tmp
// object's old-shard annotation.
func TestReconcileMigrationKeepsOldClassWhileTmpAlive(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()

	tmp := &contourv1.HTTPProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "app-0-tmp",
			Namespace:   "default",
			Annotations: map[string]string{engine.OldShardAnnotation: testOldShardClass},
		},
	}
	r, req := newTestReconciler(t, newMigratingShardedHTTPProxy(), tmp)

	_, err := r.Reconcile(ctx, req)
	g.Expect(err).NotTo(HaveOccurred())

	main := getHTTPProxy(g, r, "app-0")
	g.Expect(main.Spec.IngressClassName).To(Equal(testOldShardClass))
	g.Expect(main.Labels).To(HaveKeyWithValue(testClassLabel, testOldShardClass))

	// No second tmp child may appear: the pass renders the main child only.
	proxies := &contourv1.HTTPProxyList{}
	g.Expect(r.Client.List(ctx, proxies)).To(Succeed())
	g.Expect(proxies.Items).To(HaveLen(2))
}

// Once the tmp object's deletion window has started, the main child object
// must switch to the new shard class.
func TestReconcileMigrationSwitchesToNewClassAfterWindow(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()

	tmp := &contourv1.HTTPProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-0-tmp",
			Namespace: "default",
			Annotations: map[string]string{
				engine.OldShardAnnotation:        testOldShardClass,
				engine.AutoDeleteAfterAnnotation: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
			},
		},
	}
	r, req := newTestReconciler(t, newMigratingShardedHTTPProxy(), tmp)

	_, err := r.Reconcile(ctx, req)
	g.Expect(err).NotTo(HaveOccurred())

	main := getHTTPProxy(g, r, "app-0")
	g.Expect(main.Spec.IngressClassName).To(Equal(testNewShardClass))
	g.Expect(main.Labels).To(HaveKeyWithValue(testClassLabel, testNewShardClass))

	parent := &controllerv1.ShardedHTTPProxy{}
	g.Expect(r.Client.Get(ctx, req.NamespacedName, parent)).To(Succeed())
	g.Expect(parent.Status.CreatedObjects[testNewShardClass]).
		To(ContainElement(HaveKeyWithValue("name", "app-0")))
}

// Mid-migration the live main object must never be scheduled for deletion.
// Booked under the old shard it was missing from the current shard's status
// list, so every reconcile set auto-delete-after on it and the next one wiped
// the annotation while reconciling the spec — an endless churn in which the
// migration never completed.
func TestReconcileMigrationDoesNotChurnAutoDeleteOnMain(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()

	ownerRef := func() []metav1.OwnerReference {
		yes := true
		return []metav1.OwnerReference{{
			APIVersion:         controllerv1.GroupVersion.String(),
			Kind:               "ShardedHTTPProxy",
			Name:               "app",
			Controller:         &yes,
			BlockOwnerDeletion: &yes,
		}}
	}
	childTypeMeta := metav1.TypeMeta{Kind: "HTTPProxy", APIVersion: contourv1.GroupVersion.String()}
	tmp := &contourv1.HTTPProxy{
		TypeMeta: childTypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:            "app-0-tmp",
			Namespace:       "default",
			Annotations:     map[string]string{engine.OldShardAnnotation: testOldShardClass},
			OwnerReferences: ownerRef(),
		},
		Spec: contourv1.HTTPProxySpec{IngressClassName: testOldShardClass},
	}
	main := &contourv1.HTTPProxy{
		TypeMeta: childTypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name:            "app-0",
			Namespace:       "default",
			Labels:          map[string]string{testClassLabel: testOldShardClass, testRootLabel: "true"},
			OwnerReferences: ownerRef(),
		},
		Spec: contourv1.HTTPProxySpec{IngressClassName: testOldShardClass},
	}
	r, req := newTestReconciler(t, newMigratingShardedHTTPProxy(), tmp, main)

	var tmpDeleteAfter string
	for cycle := 1; cycle <= 3; cycle++ {
		_, err := r.Reconcile(ctx, req)
		g.Expect(err).NotTo(HaveOccurred())

		gotMain := getHTTPProxy(g, r, "app-0")
		g.Expect(gotMain.Annotations).NotTo(HaveKey(engine.AutoDeleteAfterAnnotation),
			"cycle %d: live main object must not be scheduled for deletion", cycle)

		// The tmp object still has to run its deletion timeline, and its
		// deadline must be set once rather than rescheduled every cycle. This
		// also proves the deletion pass really ran.
		gotTmp := getHTTPProxy(g, r, "app-0-tmp")
		g.Expect(gotTmp.Annotations).To(HaveKey(engine.AutoDeleteAfterAnnotation), "cycle %d", cycle)
		if tmpDeleteAfter == "" {
			tmpDeleteAfter = gotTmp.Annotations[engine.AutoDeleteAfterAnnotation]
		} else {
			g.Expect(gotTmp.Annotations[engine.AutoDeleteAfterAnnotation]).To(Equal(tmpDeleteAfter),
				"cycle %d: tmp auto-delete-after must not be rescheduled", cycle)
		}
	}
}
