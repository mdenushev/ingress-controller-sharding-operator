package v1

import (
	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type HTTPProxyTemplateSpec struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the behavior of a HTTPProxy.
	// +optional
	Spec contourv1.HTTPProxySpec `json:"spec,omitempty"`
}

// ShardedHTTPProxySpec defines the desired state of ShardedHTTPProxy
type ShardedHTTPProxySpec struct {
	Template HTTPProxyTemplateSpec `json:"template,omitempty"`
}

// ShardedPhase describes the current lifecycle phase of a sharded object.
// +kubebuilder:validation:Enum=Pending;Provisioning;Resharding;Ready;Terminating
type ShardedPhase string

const (
	// PhasePending means the object has been observed but no children exist yet.
	PhasePending ShardedPhase = "Pending"
	// PhaseProvisioning means children are being created or updated on their current shard.
	PhaseProvisioning ShardedPhase = "Provisioning"
	// PhaseResharding means children are being migrated to a different shard:
	// a tmp child keeps serving the old shard while the main child moves.
	PhaseResharding ShardedPhase = "Resharding"
	// PhaseReady means all children match the desired state.
	PhaseReady ShardedPhase = "Ready"
	// PhaseTerminating means the object is being deleted and children are drained.
	PhaseTerminating ShardedPhase = "Terminating"
)

// Condition types reported on sharded objects.
const (
	// ConditionReady is True when all children match the desired state.
	ConditionReady = "Ready"
	// ConditionResharding is True while a shard migration is in progress.
	ConditionResharding = "Resharding"
)

// ShardedStatus defines the observed state of sharded objects
type ShardedStatus struct {
	// CreatedObjects maps a shard name to the child objects created on it.
	// +kubebuilder:default:={}
	CreatedObjects map[string][]map[string]string `json:"createdObjects"`

	// Phase is the current lifecycle phase of the object.
	// +optional
	Phase ShardedPhase `json:"phase,omitempty"`

	// ObservedGeneration is the generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the current reconciliation state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Class",type="string",JSONPath=".spec.template.spec.ingressClassName",description="Class of the Ingress resource"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Lifecycle phase of the sharded object"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status",description="Ready condition"

// ShardedHTTPProxy is the Schema for the shardedhttpproxies API
type ShardedHTTPProxy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShardedHTTPProxySpec `json:"spec,omitempty"`
	Status ShardedStatus        `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ShardedHTTPProxyList contains a list of ShardedHTTPProxy
type ShardedHTTPProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardedHTTPProxy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ShardedHTTPProxy{}, &ShardedHTTPProxyList{})
}

func (s *ShardedHTTPProxy) GetCreatedObjects() *map[string][]map[string]string {
	return &s.Status.CreatedObjects
}

func (s *ShardedHTTPProxy) SetCreatedObjects(new map[string][]map[string]string) {
	s.Status.CreatedObjects = new
}

func (s *ShardedHTTPProxy) GetObject() client.Object {
	return s
}

func (s *ShardedHTTPProxy) GetShardedStatus() *ShardedStatus {
	return &s.Status
}

func (s *ShardedHTTPProxy) GetIngressClassName() string {
	return s.Spec.Template.Spec.IngressClassName
}

func (s *ShardedHTTPProxy) GetChildKind() string {
	return contourv1.HTTPProxy{}.Kind
}

func (s *ShardedHTTPProxy) GetKind() string {
	return s.Kind
}
