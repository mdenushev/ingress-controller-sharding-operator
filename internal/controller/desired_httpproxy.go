package controller

import (
	"fmt"
	"strings"

	contourv1 "github.com/projectcontour/contour/apis/projectcontour/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	controllerv1 "k8s.tochka.com/sharded-ingress-controller/api/v1"
)

// httpProxyRenderer renders the desired HTTPProxy children of a
// ShardedHTTPProxy for one shard: a root proxy plus one proxy per extra
// virtual host, each including the root.
type httpProxyRenderer struct {
	settings Settings
}

func newHTTPProxyRenderer(settings Settings) *httpProxyRenderer {
	return &httpProxyRenderer{settings: settings}
}

func (b *httpProxyRenderer) RenderChildren(sharded ShardedObject, plan ShardPlan) ([]DesiredChild[*contourv1.HTTPProxy], error) {
	src, ok := sharded.(*controllerv1.ShardedHTTPProxy)
	if !ok {
		return nil, fmt.Errorf("unsupported sharded object type: %T", sharded)
	}

	var children []DesiredChild[*contourv1.HTTPProxy]

	shardedHTTPProxy := src.DeepCopy()
	if shardedHTTPProxy.Spec.Template.Labels == nil {
		shardedHTTPProxy.Spec.Template.Labels = make(map[string]string)
	}
	if shardedHTTPProxy.Spec.Template.Annotations == nil {
		shardedHTTPProxy.Spec.Template.Annotations = make(map[string]string)
	}

	tmpName := tmpChildName(shardedHTTPProxy.Name, plan.Shard.Number)

	// While migrating, tmp children pinned to the old shard keep serving
	// traffic until service discovery converges on the new shard.
	if plan.CreateTmp {
		tempShardedHTTPProxy := shardedHTTPProxy.DeepCopy()
		tempShardedHTTPProxy.SetName(tmpName)
		tempShardedHTTPProxy.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.OldShard
		tempShardedHTTPProxy.Spec.Template.Annotations[OldShardAnnotation] = plan.OldShard

		tmpProxy := b.renderHTTPProxy(tempShardedHTTPProxy, tmpName, plan.OldShard, nil)
		tmpProxy.ObjectMeta.Labels[b.settings.RootHTTPProxyLabel] = "true"
		children = append(children, DesiredChild[*contourv1.HTTPProxy]{Shard: plan.Shard, Obj: tmpProxy})

		for i, host := range b.virtualHosts(tempShardedHTTPProxy) {
			virtualHost := newVirtualHostFromTemplate(tempShardedHTTPProxy.Spec.Template.Spec.VirtualHost, host)
			httpProxy := b.renderHTTPProxy(tempShardedHTTPProxy, fmt.Sprintf("%s-%d", tmpName, i), plan.OldShard, virtualHost)
			children = append(children, DesiredChild[*contourv1.HTTPProxy]{Shard: plan.Shard, Obj: httpProxy})
		}
	}

	shardedHTTPProxy.Spec.Template.Labels[b.settings.ServiceDiscoveryClassLabel] = plan.EffectiveClass

	mainName := shardedHTTPProxy.Name
	if sharded.GetIngressClassName() != plan.Shard.Name {
		mainName = fmt.Sprintf("%s-%d", shardedHTTPProxy.Name, plan.Shard.Number)
	}
	shardedHTTPProxy.SetName(mainName)

	baseHTTPProxy := b.renderHTTPProxy(shardedHTTPProxy, mainName, plan.EffectiveClass, nil)
	baseHTTPProxy.ObjectMeta.Labels[b.settings.RootHTTPProxyLabel] = "true"
	children = append(children, DesiredChild[*contourv1.HTTPProxy]{Shard: plan.Shard, Obj: baseHTTPProxy})

	for i, host := range b.virtualHosts(shardedHTTPProxy) {
		virtualHost := newVirtualHostFromTemplate(shardedHTTPProxy.Spec.Template.Spec.VirtualHost, host)
		httpProxy := b.renderHTTPProxy(shardedHTTPProxy, fmt.Sprintf("%s-%d", mainName, i), plan.EffectiveClass, virtualHost)
		children = append(children, DesiredChild[*contourv1.HTTPProxy]{Shard: plan.Shard, Obj: httpProxy})
	}

	return children, nil
}

// virtualHosts lists the extra hosts requested via the virtual-hosts
// annotation on the parent.
func (b *httpProxyRenderer) virtualHosts(shardedHTTPProxy *controllerv1.ShardedHTTPProxy) []string {
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

func (b *httpProxyRenderer) renderHTTPProxy(shardedHTTPProxy *controllerv1.ShardedHTTPProxy, name, ingressClass string, virtualHost *contourv1.VirtualHost) *contourv1.HTTPProxy {
	httpProxy := &contourv1.HTTPProxy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   shardedHTTPProxy.Namespace,
			Annotations: shardedHTTPProxy.Spec.Template.Annotations,
			Labels:      copyLabels(shardedHTTPProxy.Spec.Template.Labels),
		},
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
	for k, v := range source {
		res[k] = v
	}
	return res
}
