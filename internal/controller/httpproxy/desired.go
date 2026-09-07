package httpproxy

import (
	"fmt"
	"maps"
	"strings"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
	"k8s.tochka.com/sharded-ingress-controller/internal/engine"
)

// trueValue is the value of the root-proxy marker label.
const trueValue = "true"

// renderer renders the desired HTTPProxy children of a ShardedHTTPProxy for
// one shard: a root proxy plus one proxy per extra virtual host, each
// including the root.
type renderer struct {
	settings engine.Settings
}

func newRenderer(settings engine.Settings) *renderer {
	return &renderer{settings: settings}
}

func (b *renderer) RenderChildren(
	sharded engine.ShardedObject,
	plan engine.ShardPlan,
) ([]engine.DesiredChild[*contourv1.HTTPProxy], error) {
	src, ok := sharded.(*controllerv1.ShardedHTTPProxy)
	if !ok {
		return nil, fmt.Errorf("unsupported sharded object type: %T", sharded)
	}

	var children []engine.DesiredChild[*contourv1.HTTPProxy]

	shardedHTTPProxy := src.DeepCopy()
	if shardedHTTPProxy.Spec.Template.Labels == nil {
		shardedHTTPProxy.Spec.Template.Labels = make(map[string]string)
	}
	if shardedHTTPProxy.Spec.Template.Annotations == nil {
		shardedHTTPProxy.Spec.Template.Annotations = make(map[string]string)
	}

	tmpName := engine.TmpChildName(shardedHTTPProxy.Name, plan.Shard.Number)

	// While migrating, tmp children pinned to the old shard keep serving
	// traffic until service discovery converges on the new shard.
	if plan.CreateTmp {
		tempShardedHTTPProxy := shardedHTTPProxy.DeepCopy()
		tempShardedHTTPProxy.SetName(tmpName)
		tempShardedHTTPProxy.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.OldShard
		tempShardedHTTPProxy.Spec.Template.Annotations[engine.OldShardAnnotation] = plan.OldShard

		children = append(children, b.renderFamily(tempShardedHTTPProxy, tmpName, plan.OldShard, plan.Shard)...)
	}

	shardedHTTPProxy.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.EffectiveClass

	// On a sharded class every child carries its shard number in the name;
	// only a non-sharded (regular) class keeps the bare parent name.
	mainName := shardedHTTPProxy.Name
	if !plan.Regular {
		mainName = fmt.Sprintf("%s-%d", shardedHTTPProxy.Name, plan.Shard.Number)
	}
	shardedHTTPProxy.SetName(mainName)

	children = append(children, b.renderFamily(shardedHTTPProxy, mainName, plan.EffectiveClass, plan.Shard)...)

	return children, nil
}

// renderFamily renders one proxy family on the given class: the root proxy
// (marked with the root label) plus one proxy per extra virtual host, each
// including the root.
func (b *renderer) renderFamily(
	src *controllerv1.ShardedHTTPProxy,
	baseName, class string,
	shard engine.Shard,
) []engine.DesiredChild[*contourv1.HTTPProxy] {
	root := b.renderHTTPProxy(src, baseName, class, nil)
	root.Labels[b.settings.RootHTTPProxyLabel] = trueValue
	children := []engine.DesiredChild[*contourv1.HTTPProxy]{{Shard: shard, Obj: root}}

	for i, host := range b.virtualHosts(src) {
		virtualHost := newVirtualHostFromTemplate(src.Spec.Template.Spec.VirtualHost, host)
		children = append(children, engine.DesiredChild[*contourv1.HTTPProxy]{
			Shard: shard,
			Obj:   b.renderHTTPProxy(src, fmt.Sprintf("%s-%d", baseName, i), class, virtualHost),
		})
	}
	return children
}

// virtualHosts lists the extra hosts requested via the virtual-hosts
// annotation on the parent.
func (b *renderer) virtualHosts(shardedHTTPProxy *controllerv1.ShardedHTTPProxy) []string {
	serverAlias, exists := shardedHTTPProxy.Annotations[b.settings.VirtualHostsAnnotation]
	if !exists || serverAlias == "" {
		return nil
	}
	return strings.Split(serverAlias, ",")
}

// newVirtualHostFromTemplate copies the template's VirtualHost (all fields,
// current and future) and replaces Fqdn with the given host.
func newVirtualHostFromTemplate(template *contourv1.VirtualHost, host string) *contourv1.VirtualHost {
	if template == nil {
		return &contourv1.VirtualHost{Fqdn: host}
	}
	virtualHost := template.DeepCopy()
	virtualHost.Fqdn = host
	return virtualHost
}

func (b *renderer) renderHTTPProxy(
	shardedHTTPProxy *controllerv1.ShardedHTTPProxy,
	name, ingressClass string,
	virtualHost *contourv1.VirtualHost,
) *contourv1.HTTPProxy {
	httpProxy := &contourv1.HTTPProxy{
		Name:        name,
		Namespace:   shardedHTTPProxy.Namespace,
		Annotations: shardedHTTPProxy.Spec.Template.Annotations,
		Labels:      copyLabels(shardedHTTPProxy.Spec.Template.Labels),
		Spec: contourv1.HTTPProxySpec{
			VirtualHost:      virtualHost,
			Routes:           shardedHTTPProxy.Spec.Template.Spec.Routes,
			TCPProxy:         shardedHTTPProxy.Spec.Template.Spec.TCPProxy,
			IngressClassName: ingressClass,
		},
	}

	// Extra virtual hosts route through the root proxy.
	if virtualHost != nil {
		httpProxy.Spec.Includes = []contourv1.Include{
			{
				Name:      shardedHTTPProxy.Name,
				Namespace: shardedHTTPProxy.Namespace,
			},
		}
	}

	return httpProxy
}

func copyLabels(source map[string]string) map[string]string {
	res := make(map[string]string, len(source))
	maps.Copy(res, source)
	return res
}
