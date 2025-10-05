/*
2025 NVIDIA CORPORATION & AFFILIATES

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sync"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	"github.com/Mellanox/network-operator/pkg/consts"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	drainer "github.com/Mellanox/network-operator/pkg/drain"
	sriovnetworkv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	constants "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/drain"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/platforms"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/utils"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/vars"
)

// DrainReconcile is a struct that contains drain controller configurations
type DrainReconcile struct {
	client.Client
	scheme      *runtime.Scheme
	recorder    record.EventRecorder
	drainer     drain.DrainInterface
	migrationCh chan struct{}

	drainCheckMutex sync.Mutex
	log             logr.Logger
}

// NewDrainReconcileController creates a new DrainReconcile controller
func NewDrainReconcileController(client client.Client, scheme *runtime.Scheme, recorder record.EventRecorder,
	platformHelper platforms.Interface, migrationCh chan struct{}, log logr.Logger) (*DrainReconcile, error) {
	drainer, err := drainer.NewDrainRequestor(client, log, platformHelper)
	if err != nil {
		return nil, err
	}

	return &DrainReconcile{
		client,
		scheme,
		recorder,
		drainer,
		migrationCh,
		sync.Mutex{},
		log}, nil
}

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=sriovnetwork.openshift.io,resources=sriovnodestates,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=maintenance.nvidia.com,resources=nodemaintenances,verbs=get;list;watch
//+kubebuilder:rbac:groups=maintenance.nvidia.com,resources=nodemaintenances/status,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.8.3/pkg/reconcile
func (r *DrainReconcile) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	select {
	case <-r.migrationCh:
	case <-ctx.Done():
		return ctrl.Result{}, fmt.Errorf("canceled")
	}
	r.log.WithName("Drain Reconcile")

	req.Namespace = vars.Namespace

	// get node object
	node := &corev1.Node{}
	found, err := r.getObject(ctx, req, node)
	if err != nil {
		r.log.Error(err, "failed to get node object")
		return ctrl.Result{}, err
	}
	if !found {
		r.log.Info("node not found don't, requeue the request", "node", req.Name)
		return ctrl.Result{}, nil
	}

	// get sriovNodeNodeState object
	nodeNetworkState := &sriovnetworkv1.SriovNetworkNodeState{}
	found, err = r.getObject(ctx, req, nodeNetworkState)
	if err != nil {
		r.log.Error(err, "failed to get sriovNetworkNodeState object")
		return ctrl.Result{}, err
	}
	if !found {
		r.log.Info("sriovNetworkNodeState not found, don't requeue the request")
		return ctrl.Result{}, nil
	}

	// create the drain state annotation if it doesn't exist in the sriovNetworkNodeState object
	nodeStateDrainAnnotationCurrent, currentNodeStateExist,
		err := r.ensureAnnotationExists(ctx, nodeNetworkState, constants.NodeStateDrainAnnotationCurrent)
	if err != nil {
		r.log.Error(err, "failed to ensure nodeStateDrainAnnotationCurrent")
		return ctrl.Result{}, err
	}
	_, desireNodeStateExist, err := r.ensureAnnotationExists(ctx, nodeNetworkState,
		constants.NodeStateDrainAnnotation)
	if err != nil {
		r.log.Error(err, "failed to ensure nodeStateDrainAnnotation")
		return ctrl.Result{}, err
	}

	// create the drain state annotation if it doesn't exist in the node object
	nodeDrainAnnotation, nodeExist, err := r.ensureAnnotationExists(ctx, node, constants.NodeDrainAnnotation)
	if err != nil {
		r.log.Error(err, "failed to ensure nodeStateDrainAnnotation")
		return ctrl.Result{}, err
	}

	// requeue the request if we needed to add any of the annotations
	if !nodeExist || !currentNodeStateExist || !desireNodeStateExist {
		return ctrl.Result{Requeue: true}, nil
	}
	r.log.V(consts.LogLevelInfo).Info("Drain annotations", "nodeAnnotation", nodeDrainAnnotation,
		"nodeStateAnnotation", nodeStateDrainAnnotationCurrent)

	// Check the node request
	if nodeDrainAnnotation == constants.DrainIdle {
		// this cover the case the node is on idle

		// node request to be on idle and the currect state is idle
		// we don't do anything
		if nodeStateDrainAnnotationCurrent == constants.DrainIdle {
			r.log.Info("node and nodeState are on idle nothing todo")
			return reconcile.Result{}, nil
		}

		// we have two options here:
		// 1. node request idle and the current status is drain complete
		// this means the daemon finish is work, so we need to clean the drain
		//
		// 2. the operator is still draining the node but maybe the sriov policy changed and the daemon
		//  doesn't need to drain anymore, so we can stop the drain
		if nodeStateDrainAnnotationCurrent == constants.DrainComplete ||
			nodeStateDrainAnnotationCurrent == constants.Draining {
			return r.handleNodeIdleNodeStateDrainingOrCompleted(ctx, node, nodeNetworkState)
		}
	}

	// this cover the case a node request to drain or reboot
	if nodeDrainAnnotation == constants.DrainRequired ||
		nodeDrainAnnotation == constants.RebootRequired {
		return r.handleNodeDrainOrReboot(ctx, node, nodeNetworkState,
			nodeDrainAnnotation, nodeStateDrainAnnotationCurrent)
	}

	r.log.Error(nil, "unexpected node drain annotation")
	return reconcile.Result{}, fmt.Errorf("unexpected node drain annotation")
}

func (r *DrainReconcile) getObject(ctx context.Context, req ctrl.Request, object client.Object) (bool, error) {
	err := r.Get(ctx, req.NamespacedName, object)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *DrainReconcile) ensureAnnotationExists(ctx context.Context,
	object client.Object, key string) (string, bool, error) {
	value, exist := object.GetAnnotations()[key]
	if !exist {
		err := utils.AnnotateObject(ctx, object, key, constants.DrainIdle, r.Client)
		if err != nil {
			return "", false, err
		}
		return constants.DrainIdle, false, nil
	}

	return value, true, nil
}

// DrainAnnotationPredicate contains the predicate for node drain annotation changes
type DrainAnnotationPredicate struct {
	predicate.Funcs
	log logr.Logger
}

//nolint:dupl,revive
func (DrainAnnotationPredicate) Create(e event.CreateEvent) bool {
	if e.Object == nil {
		return false
	}

	if _, hasAnno := e.Object.GetAnnotations()[constants.NodeDrainAnnotation]; hasAnno {
		return true
	}
	return false
}

//nolint:dupl,revive
func (d DrainAnnotationPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		return false
	}
	if e.ObjectNew == nil {
		return false
	}

	oldAnno, hasOldAnno := e.ObjectOld.GetAnnotations()[constants.NodeDrainAnnotation]
	newAnno, hasNewAnno := e.ObjectNew.GetAnnotations()[constants.NodeDrainAnnotation]

	d.log.V(consts.LogLevelDebug).Info("Update", "oldAnno", oldAnno, "newAnno", newAnno)
	if !hasOldAnno && hasNewAnno {
		return true
	}

	return oldAnno != newAnno
}

// DrainStateAnnotationPredicate contains the predicate for
// sriov node state drain annotation changes
type DrainStateAnnotationPredicate struct {
	predicate.Funcs
	log logr.Logger
}

//nolint:revive
func (DrainStateAnnotationPredicate) Create(e event.CreateEvent) bool {
	return e.Object != nil
}

//nolint:revive
func (d DrainStateAnnotationPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		return false
	}
	if e.ObjectNew == nil {
		return false
	}

	oldAnno, hasOldAnno := e.ObjectOld.GetAnnotations()[constants.NodeStateDrainAnnotationCurrent]
	newAnno, hasNewAnno := e.ObjectNew.GetAnnotations()[constants.NodeStateDrainAnnotationCurrent]

	d.log.V(consts.LogLevelDebug).Info("Update", "oldAnno", oldAnno, "newAnno", newAnno)
	if !hasOldAnno || !hasNewAnno {
		return true
	}

	return oldAnno != newAnno
}

// SetupWithManager sets up the controller with the Manager.
func (r *DrainReconcile) SetupWithManager(mgr ctrl.Manager) error {
	createUpdateEnqueue := handler.Funcs{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[client.Object],
			w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			w.Add(reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: drainer.GetDrainRequestorOpts(r.drainer).MaintenanceOPRequestorNS,
				Name:      e.Object.GetName(),
			}})
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[client.Object],
			w workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			w.Add(reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: drainer.GetDrainRequestorOpts(r.drainer).MaintenanceOPRequestorNS,
				Name:      e.ObjectNew.GetName(),
			}})
		},
	}

	logger := mgr.GetLogger().WithValues("Function", "Drain")
	requestorOpts := drainer.GetRequestorOptsFromEnvs()
	// Watch for spec and annotation changes
	nodePredicates := builder.WithPredicates(DrainAnnotationPredicate{})
	nodeStatePredicates := builder.WithPredicates(DrainStateAnnotationPredicate{log: logger})
	// TODO: Make sure there is once logger instance to be used for all the predicates
	nodeMaintenancePredicates := drainer.NewConditionChangedPredicate(logger,
		requestorOpts.MaintenanceOPRequestorID)
	requestorIDPredicate := drainer.NewRequestorIDPredicate(logger,
		requestorOpts.MaintenanceOPRequestorID)
	m := ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 50,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				//nolint:lll
				// Inspired by https://github.com/kubernetes-sigs/controller-runtime/blob/52b17917caa97ec546423867d9637f1787830f3e/pkg/builder/controller.go#L447
				if req, ok := any(request).(*reconcile.Request); ok && req != nil {
					logger = logger.WithValues("node", request.Name)
				}
				return logger
			},
		}).
		For(&corev1.Node{}, nodePredicates).
		Watches(&sriovnetworkv1.SriovNetworkNodeState{}, createUpdateEnqueue, nodeStatePredicates).
		Watches(&maintenancev1alpha1.NodeMaintenance{}, createUpdateEnqueue,
			builder.WithPredicates(nodeMaintenancePredicates, requestorIDPredicate))

	return m.Complete(r)
}
