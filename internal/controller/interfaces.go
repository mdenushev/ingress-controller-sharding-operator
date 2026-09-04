package controller

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// Settings carries every tunable shared by both controllers. All values are
// plain (no pointers): they are read-only after start-up.
type Settings struct {
	// MaxShards maps an ingress class name to the number of shards it is
	// split into. 0 disables sharding for the class.
	MaxShards map[string]int

	// TerminationPeriod is the base window T of the migration timeline:
	// a child is switched to the new shard after T, marked for service
	// discovery unregistering after 2T and deleted after 3T.
	TerminationPeriod time.Duration
	// ShardUpdateCooldown throttles consecutive updates on one shard.
	ShardUpdateCooldown time.Duration

	DomainSubstring           string
	MutatingWebhookAnnotation string
	// UnregisterAnnotation marks a child for removal from service discovery.
	UnregisterAnnotation string

	ServiceDiscoveryClassLabel     string
	ServiceDiscoveryTagsAnnotation string
	AppNameLabel                   string

	// RootHTTPProxyLabel and VirtualHostsAnnotation are HTTPProxy-specific.
	RootHTTPProxyLabel     string
	VirtualHostsAnnotation string

	AllShardsPlacementAnnotation string
	AllShardsBaseHosts           []string

	FinalizerKey                       string
	FinalizerTerminationPeriod         time.Duration
	FinalizerDeletionTerminationPeriod time.Duration
}

// ShardedObject is a parent custom resource (ShardedIngress or
// ShardedHTTPProxy) reconciled into a set of child objects.
type ShardedObject interface {
	client.Object
	GetObject() client.Object
	GetIngressClassName() string
	GetKind() string
	GetChildKind() string
	GetShardedStatus() *controllerv1.ShardedStatus
}

// Shard is one shard of an ingress class.
type Shard struct {
	Number int
	Name   string
}

// ShardPlan is the per-shard input for building desired children. The engine
// resolves the resharding context (tmp object state, old shard) before the
// builder runs, so builders stay pure.
type ShardPlan struct {
	Shard Shard
	// OldShard is the shard children are migrating from; "" when no
	// migration is in progress.
	OldShard string
	// EffectiveClass is the ingress class rendered into the children: the
	// old class while traffic must stay on the old shard, the shard's own
	// class otherwise.
	EffectiveClass string
	// CreateTmp is true when the tmp child that keeps the old shard alive
	// has to be created in this pass.
	CreateTmp bool
	// Regular is true when sharding is disabled for the class, so the
	// single child keeps the parent's name.
	Regular bool
	// UseAllShards is true when the parent is placed on every shard of the
	// class.
	UseAllShards bool
}

// DesiredChild is one child object that should exist in the cluster.
type DesiredChild struct {
	// Shard is the bookkeeping shard (always the new one, even while the
	// object itself still carries the old class mid-migration).
	Shard Shard
	Obj   client.Object
	// AlsoBook lists extra child names recorded in the parent status for
	// this shard, keeping mid-migration objects off the pruning list.
	AlsoBook []string
}

// DesiredBuilder renders the desired children of a parent for one shard.
// Implementations must not touch the cluster.
type DesiredBuilder interface {
	BuildChildren(sharded ShardedObject, plan ShardPlan) ([]DesiredChild, error)
}

// ChildAdapter hides the concrete child type (Ingress or HTTPProxy) from the
// engine.
type ChildAdapter interface {
	// Kind of the child objects, e.g. "Ingress".
	Kind() string
	// ListGVK is the GroupVersionKind of the child list type.
	ListGVK() schema.GroupVersionKind
	// NewObject returns an empty child object.
	NewObject() client.Object
	// Equal reports whether the existing child already matches the desired
	// one, ignoring differences produced by cluster mutation webhooks.
	Equal(existing, desired client.Object) (bool, error)
	// Merge copies the desired spec and metadata onto the existing object,
	// keeping server-populated fields intact.
	Merge(existing, desired client.Object) (client.Object, error)
}

// ShardSelector decides which shards a parent lives on.
type ShardSelector interface {
	// ShardsFor returns the shards for the parent plus whether the parent
	// is "regular" (sharding disabled, single child under the parent name).
	ShardsFor(sharded ShardedObject, useAllShards bool) ([]Shard, bool, error)
}
