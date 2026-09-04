package controller

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
)

// stateTracker keeps the in-memory bookkeeping the rate limiter and metrics
// are built on: which parents are waiting for an apply slot, which are ready,
// managed or in error, and which ingress classes the parents/children were
// last seen on.
type stateTracker struct {
	ctrlName string

	waiting map[string]bool
	ready   map[string]bool
	managed map[string]bool
	errored map[string]bool

	shardedClasses *sync.Map
	childClasses   *sync.Map
}

func newStateTracker(ctrlName string) *stateTracker {
	return &stateTracker{
		ctrlName:       ctrlName,
		waiting:        make(map[string]bool),
		ready:          make(map[string]bool),
		managed:        make(map[string]bool),
		errored:        make(map[string]bool),
		shardedClasses: &sync.Map{},
		childClasses:   &sync.Map{},
	}
}

// move removes the key from every src list and adds it to every dest list.
func (t *stateTracker) move(key string, srcs []map[string]bool, dests []map[string]bool) {
	for _, src := range srcs {
		delete(src, key)
	}
	for _, dest := range dests {
		dest[key] = true
	}
	t.updateMetrics()
}

func (t *stateTracker) markWaiting(key string) {
	t.move(key, []map[string]bool{t.ready, t.errored}, []map[string]bool{t.waiting})
}

func (t *stateTracker) markReady(key string) {
	t.move(key, []map[string]bool{t.waiting, t.errored}, []map[string]bool{t.ready})
}

func (t *stateTracker) markErrored(key string) {
	t.move(key, []map[string]bool{t.waiting, t.ready}, []map[string]bool{t.errored})
}

func (t *stateTracker) isWaiting(key string) bool { return t.waiting[key] }
func (t *stateTracker) isManaged(key string) bool { return t.managed[key] }

func (t *stateTracker) markManaged(key string) { t.managed[key] = true }

func (t *stateTracker) doneWaiting(key string) {
	delete(t.waiting, key)
}

// forget drops every trace of the key after the parent has been deleted.
func (t *stateTracker) forget(key string) {
	if _, exists := t.errored[key]; exists {
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			metrics.ErrorListGauge.Delete(prometheus.Labels{
				"controller": t.ctrlName,
				"name":       parts[1],
				"namespace":  parts[0],
			})
		}
	}
	delete(t.waiting, key)
	delete(t.ready, key)
	delete(t.managed, key)
	delete(t.errored, key)

	if ingressClass, exists := t.shardedClasses.Load(key); exists {
		metrics.DeletingCounter.WithLabelValues("shardedingress").Inc()
		metrics.ShardedIngressClassObjectCount.With(prometheus.Labels{"controller": t.ctrlName, "ingress_class": ingressClass.(string)}).Dec()
		t.shardedClasses.Delete(key)
	}
	if ingressClass, exists := t.childClasses.Load(key); exists {
		metrics.ChildIngressClassObjectCount.With(prometheus.Labels{"controller": t.ctrlName, "ingress_class": ingressClass.(string)}).Dec()
		t.childClasses.Delete(key)
	}
	t.updateMetrics()
}

// noteShardedClass records which ingress class the parent currently uses and
// keeps the per-class object count metric in sync.
func (t *stateTracker) noteShardedClass(key, class string) {
	t.noteClass(t.shardedClasses, metrics.ShardedIngressClassObjectCount, key, class)
}

// noteChildClass records which shard the parent's children currently use.
func (t *stateTracker) noteChildClass(key, class string) {
	t.noteClass(t.childClasses, metrics.ChildIngressClassObjectCount, key, class)
}

func (t *stateTracker) noteClass(cache *sync.Map, gauge *prometheus.GaugeVec, key, class string) {
	if prev, exists := cache.Load(key); exists {
		if prevStr, ok := prev.(string); ok && prevStr != class {
			gauge.With(prometheus.Labels{"controller": t.ctrlName, "ingress_class": prevStr}).Dec()
			cache.Delete(key)
			gauge.With(prometheus.Labels{"controller": t.ctrlName, "ingress_class": class}).Inc()
			cache.Store(key, class)
		}
		return
	}
	gauge.With(prometheus.Labels{"controller": t.ctrlName, "ingress_class": class}).Inc()
	cache.Store(key, class)
}

func (t *stateTracker) updateMetrics() {
	metrics.WaitingListGauge.WithLabelValues(t.ctrlName).Set(float64(len(t.waiting)))
	metrics.ReadyListGauge.WithLabelValues(t.ctrlName).Set(float64(len(t.ready)))
	metrics.ManagedListGauge.WithLabelValues(t.ctrlName).Set(float64(len(t.managed)))
	for key := range t.errored {
		parts := strings.Split(key, "/")
		if len(parts) == 2 {
			metrics.ErrorListGauge.WithLabelValues(t.ctrlName, parts[0], parts[1]).Set(float64(1))
		}
	}
}
