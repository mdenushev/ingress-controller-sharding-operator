package engine

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
	"k8s.tochka.com/sharded-ingress-controller/internal/status"
)

// applyChildren brings the cluster to the desired set: it creates missing
// children, updates drifted ones and prunes children that are no longer
// desired. A create or delete ends the pass immediately so the loop applies
// one change at a time.
func (e *Engine[C]) applyChildren(s *scope, desired []DesiredChild[C]) (ctrl.Result, error) {
	statusList := make(map[string][]map[string]string)

	if !e.tracker.IsManaged(s.key) {
		e.tracker.markManaged(s.key)
	}
	e.tracker.noteShardedClass(s.key, s.obj.GetIngressClassName())

	for _, child := range desired {
		if err := ctrl.SetControllerReference(s.obj, child.Obj, e.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf(
				"cannot set controller reference on %s %s: %w",
				e.Adapter.Kind(), child.Obj.GetName(), err,
			)
		}

		existing := e.Adapter.NewObject()
		err := e.Get(
			s.ctx,
			types.NamespacedName{Name: child.Obj.GetName(), Namespace: child.Obj.GetNamespace()},
			existing,
		)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			// The creation is the pass's one mutation: stop here.
			return ctrl.Result{}, e.createChild(s, child)
		}
		if updateErr := e.updateChild(s, existing, child); updateErr != nil {
			return ctrl.Result{}, updateErr
		}

		statusList[child.Shard.Name] = append(
			statusList[child.Shard.Name],
			map[string]string{status.KeyKind: e.Adapter.Kind(), status.KeyName: child.Obj.GetName()},
		)
		for _, name := range child.ExtraChildNames {
			statusList[child.Shard.Name] = append(
				statusList[child.Shard.Name],
				map[string]string{status.KeyKind: e.Adapter.Kind(), status.KeyName: name},
			)
		}
	}

	return e.pruneChildren(s, statusList)
}

func (e *Engine[C]) createChild(s *scope, child DesiredChild[C]) error {
	logger := log.FromContext(s.ctx)
	kind := e.Adapter.Kind()
	name := child.Obj.GetName()

	if err := e.Create(s.ctx, child.Obj); err != nil {
		logger.Error(err, "unable to create", "objectKind", kind, "objectName", name)
		e.tracker.markErrored(s.key)
		e.warnf(s, status.EventChildApplyFailed, "Unable to create %s %s: %v", kind, name, err)
		return err
	}

	logger.Info("successfully created", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	if isTmpChildName(s.obj.GetName(), name) {
		e.eventf(
			s,
			status.EventTmpChildCreated,
			"Created tmp %s %s to keep the old shard serving during migration",
			kind,
			name,
		)
	} else {
		e.eventf(s, status.EventChildCreated, "Created %s %s on shard %s", kind, name, child.Shard.Name)
	}
	if err := e.addChildToStatus(s, kind, name, child.Shard.Name); err != nil {
		return err
	}
	e.tracker.doneWaiting(s.key)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, child.Shard.Name).Inc()
	s.mutated = true
	return nil
}

func (e *Engine[C]) updateChild(s *scope, existing C, child DesiredChild[C]) error {
	logger := log.FromContext(s.ctx)
	kind := e.Adapter.Kind()
	name := child.Obj.GetName()

	existingCopy, okExisting := existing.DeepCopyObject().(C)
	desiredCopy, okDesired := child.Obj.DeepCopyObject().(C)
	if !okExisting || !okDesired {
		return fmt.Errorf("unexpected child object type %T", existing)
	}
	equal, err := e.Adapter.Equal(existingCopy, desiredCopy)
	if err != nil {
		logger.Error(err, "unable to compare", "objectKind", kind, "objectName", name)
		return err
	}
	if equal {
		return nil
	}

	merged := e.Adapter.Merge(existing, child.Obj)
	if updateErr := e.Update(s.ctx, merged); updateErr != nil {
		logger.Error(updateErr, "unable to update", "objectKind", kind, "objectName", name)
		e.tracker.markErrored(s.key)
		e.warnf(s, status.EventChildApplyFailed, "Unable to update %s %s: %v", kind, name, updateErr)
		return updateErr
	}
	logger.Info("successfully updated", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	e.eventf(s, status.EventChildUpdated, "Updated %s %s on shard %s", kind, name, child.Shard.Name)
	if statusErr := e.addChildToStatus(s, kind, name, child.Shard.Name); statusErr != nil {
		return statusErr
	}
	e.tracker.doneWaiting(s.key)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, child.Shard.Name).Inc()
	s.mutated = true
	return nil
}

// listChildren lists the live children owned by the parent.
func (e *Engine[C]) listChildren(s *scope) (unstructured.UnstructuredList, error) {
	childObjs := unstructured.UnstructuredList{}
	childObjs.SetGroupVersionKind(e.Adapter.ListGVK())

	if err := e.List(s.ctx, &childObjs, client.InNamespace(s.req.Namespace)); err != nil {
		return unstructured.UnstructuredList{}, err
	}

	parentKind := s.obj.GetKind()
	res := unstructured.UnstructuredList{}
	for _, childObj := range childObjs.Items {
		for _, owner := range childObj.GetOwnerReferences() {
			if owner.Name == s.obj.GetName() && owner.Kind == parentKind {
				res.Items = append(res.Items, childObj)
			}
		}
	}
	return res, nil
}
