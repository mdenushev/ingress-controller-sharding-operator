package controller

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"k8s.tochka.com/sharded-ingress-controller/internal/metrics"
)

// one change at a time.
func (e *Engine[C]) applyChildren(s *scope, desired []DesiredChild[C]) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)
	statusList := make(map[string][]map[string]string)

	if !e.tracker.isManaged(s.key) {
		e.tracker.markManaged(s.key)
	}
	e.tracker.noteShardedClass(s.key, s.obj.GetIngressClassName())

	for _, current := range desired {
		found := e.Adapter.NewObject()
		if err := ctrl.SetControllerReference(s.obj, current.Obj, e.Scheme); err != nil {
			logger.Error(
				err,
				"unable to set controller reference",
				"objectKind",
				e.Adapter.Kind(),
				"objectName",
				current.Obj.GetName(),
			)
		}
		err := e.Get(
			s.ctx,
			types.NamespacedName{Name: current.Obj.GetName(), Namespace: current.Obj.GetNamespace()},
			found,
		)
		switch {
		case apierrors.IsNotFound(err):
			result, createErr := e.createChild(s, current)
			if createErr != nil {
				logger.Error(
					createErr,
					"unable to create",
					"objectKind",
					e.Adapter.Kind(),
					"objectName",
					current.Obj.GetName(),
				)
			}
			return result, nil
		case err != nil:
			logger.Error(err, "unable to get", "objectKind", e.Adapter.Kind(), "objectName", current.Obj.GetName())
		default:
			if updateErr := e.updateChild(s, found, current); updateErr != nil {
				logger.Error(
					updateErr,
					"unable to update",
					"objectKind",
					e.Adapter.Kind(),
					"objectName",
					current.Obj.GetName(),
				)
			}
		}

		statusList[current.Shard.Name] = append(
			statusList[current.Shard.Name],
			map[string]string{statusKeyKind: e.Adapter.Kind(), statusKeyName: current.Obj.GetName()},
		)
		for _, name := range current.AlsoBook {
			statusList[current.Shard.Name] = append(
				statusList[current.Shard.Name],
				map[string]string{statusKeyKind: e.Adapter.Kind(), statusKeyName: name},
			)
		}
	}

	result, err := e.pruneChildren(s, statusList)
	if err != nil {
		logger.Error(err, "unable to delete unlisted objects")
	}
	return result, nil
}

func (e *Engine[C]) createChild(s *scope, child DesiredChild[C]) (ctrl.Result, error) {
	logger := log.FromContext(s.ctx)
	kind := e.Adapter.Kind()
	name := child.Obj.GetName()

	if err := e.Create(s.ctx, child.Obj); err != nil {
		logger.Error(err, "unable to create", "objectKind", kind, "objectName", name)
		e.tracker.markErrored(s.key)
		e.warnf(s, EventChildApplyFailed, "Unable to create %s %s: %v", kind, name, err)
		return ctrl.Result{}, err
	}

	logger.Info("successfully created", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	if isTmpChildName(s.obj.GetName(), name) {
		e.eventf(
			s,
			EventTmpChildCreated,
			"Created tmp %s %s to keep the old shard serving during migration",
			kind,
			name,
		)
	} else {
		e.eventf(s, EventChildCreated, "Created %s %s on shard %s", kind, name, child.Shard.Name)
	}
	if err := e.addChildToStatus(s, kind, name, child.Shard.Name); err != nil {
		return ctrl.Result{}, err
	}
	e.tracker.doneWaiting(s.key)
	metrics.ProcessingCounter.WithLabelValues(e.CtrlName, child.Shard.Name).Inc()
	s.mutated = true
	return ctrl.Result{}, nil
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
		e.warnf(s, EventChildApplyFailed, "Unable to update %s %s: %v", kind, name, updateErr)
		return updateErr
	}
	logger.Info("successfully updated", "objectKind", kind, "objectName", name)
	e.tracker.markReady(s.key)
	e.eventf(s, EventChildUpdated, "Updated %s %s on shard %s", kind, name, child.Shard.Name)
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

// pruneChildren walks the live children and schedules the ones that are no
// longer desired for graceful deletion (unregister from service discovery
// first, delete after the termination window). It also drops status records
// whose objects are gone.
