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
	"cmp"
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"time"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	"github.com/Mellanox/network-operator/api/v1alpha1"
	"github.com/Mellanox/network-operator/pkg/consts"
	"github.com/go-logr/logr"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/drain"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/platforms"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/vars"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type DrainRequestorOptions struct {
	// UseMaintenanceOperator enables requestor upgrade mode
	UseMaintenanceOperator bool
	// MaintenanceOPRequestorID is the requestor ID for maintenance operator
	MaintenanceOPRequestorID string
	// MaintenanceOPRequestorNS is a user defined namespace which nodeMaintennace
	// objects will be created
	MaintenanceOPRequestorNS string
	// NodeMaintenanceNamePrefix is a prefix for nodeMaintenance object name
	// e.g. <prefix>-<node-name> to distinguish between different requestors if desired
	NodeMaintenanceNamePrefix string
	// MaintenanceOPPodEvictionFilter is a filter to be used for pods eviction
	// by maintenance operator
	MaintenanceOPPodEvictionFilter []maintenancev1alpha1.PodEvictionFiterEntry
}

type DrainInterface interface {
	DrainNode(context.Context, *corev1.Node, bool, bool) (bool, error)
	CompleteDrainNode(context.Context, *corev1.Node) (bool, error)
}

type DrainRequestor struct {
	opts            DrainRequestorOptions
	k8sClient       client.Client
	kubeClient      kubernetes.Interface
	platformHelpers platforms.Interface
	log             logr.Logger
}

const (
	// MaintenanceOPEvictionGPU is a default filter for GPU OP pods eviction
	MaintenanceOPEvictionGPU = "nvidia.com/gpu-*"
	// MaintenanceOPEvictionRDMA is a default filter for Network OP pods eviction
	MaintenanceOPEvictionRDMA = "nvidia.com/rdma*"
	// DefaultNodeMaintenanceNamePrefix is a default prefix for nodeMaintenance object name
	DefaultNodeMaintenanceNamePrefix = "" //sriov-operator-drainer"
	// trueString is the word true as string to avoid duplication and linting errors
	trueString = "true"
	// DrainTimeOut is the default timeout for the drain operation
	DrainTimeOut = 90 * time.Second
)

var (
	ErrNodeMaintenanceUpgradeDisabled = errors.New("node maintenance upgrade mode is disabled")
	defaultNodeMaintenance            *maintenancev1alpha1.NodeMaintenance
	Scheme                            = runtime.NewScheme()
)

type ConditionChangedPredicate struct {
	predicate.Funcs
	requestorID string

	log logr.Logger
}

// NewConditionChangedPredicate creates a new ConditionChangedPredicate
func NewConditionChangedPredicate(log logr.Logger, requestorID string) ConditionChangedPredicate {
	return ConditionChangedPredicate{
		Funcs:       predicate.Funcs{},
		log:         log,
		requestorID: requestorID,
	}
}

// Update implements Predicate.
func (p ConditionChangedPredicate) Update(e event.TypedUpdateEvent[client.Object]) bool {
	p.log.V(consts.LogLevelDebug).Info("ConditionChangedPredicate Update event")

	if e.ObjectOld == nil {
		p.log.Error(nil, "old object is nil in update event, ignoring event.")
		return false
	}
	if e.ObjectNew == nil {
		p.log.Error(nil, "new object is nil in update event, ignoring event.")
		return false
	}

	oldO, ok := e.ObjectOld.(*maintenancev1alpha1.NodeMaintenance)
	if !ok {
		p.log.Error(nil, "failed to cast old object to NodeMaintenance in update event, ignoring event.")
		return false
	}

	newO, ok := e.ObjectNew.(*maintenancev1alpha1.NodeMaintenance)
	if !ok {
		p.log.Error(nil, "failed to cast new object to NodeMaintenance in update event, ignoring event.")
		return false
	}

	cmpByType := func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	}

	// sort old and new obj.Status.Conditions so they can be compared using DeepEqual
	slices.SortFunc(oldO.Status.Conditions, cmpByType)
	slices.SortFunc(newO.Status.Conditions, cmpByType)

	condChanged := !reflect.DeepEqual(oldO.Status.Conditions, newO.Status.Conditions)
	// Check if the object is marked for deletion
	deleting := len(newO.Finalizers) == 0 && len(oldO.Finalizers) > 0
	deleting = deleting && !newO.DeletionTimestamp.IsZero()
	enqueue := condChanged || deleting

	p.log.V(consts.LogLevelDebug).Info("update event for NodeMaintenance",
		"name", newO.Name, "namespace", newO.Namespace,
		"condition-changed", condChanged,
		"deleting", deleting, "enqueue-request", enqueue)

	return enqueue
}

// NewRequestorIDPredicate creates a new predicate that checks if nodeMaintenance object is
// related to current requestorID, whether owned or shared with current requestorID
func NewRequestorIDPredicate(log logr.Logger, requestorID string) predicate.Funcs {
	return predicate.NewPredicateFuncs(func(object client.Object) bool {
		nm, ok := object.(*maintenancev1alpha1.NodeMaintenance)
		if !ok {
			log.Error(nil, "failed to cast object to NodeMaintenance in update event, ignoring event.")
			return false
		}
		// check if requestorID is the owner of the object or if is under AdditionalRequestors list
		return requestorID == nm.Spec.RequestorID || slices.Contains(nm.Spec.AdditionalRequestors, requestorID)
	})
}

func setDefaultNodeMaintenance(opts DrainRequestorOptions,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) {
	drainSpec := &maintenancev1alpha1.DrainSpec{
		Force: true,
		// TODO: Add pod selector
		PodSelector:    "",
		TimeoutSecond:  int32(DrainTimeOut.Seconds()),
		DeleteEmptyDir: true,
	}
	defaultNodeMaintenance = &maintenancev1alpha1.NodeMaintenance{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: opts.MaintenanceOPRequestorNS,
		},
		Spec: maintenancev1alpha1.NodeMaintenanceSpec{
			RequestorID: opts.MaintenanceOPRequestorID,
			// TODO: Add wait for pod completion
			WaitForPodCompletion: nil,
			DrainSpec:            drainSpec,
		},
	}
}

func NewDrainRequestor(k8sClient client.Client, log logr.Logger,
	platformHelpers platforms.Interface) (*DrainRequestor, error) {
	kclient, err := kubernetes.NewForConfig(vars.Config)
	if err != nil {
		return nil, err
	}
	opts := GetRequestorOptsFromEnvs()
	setDefaultNodeMaintenance(opts, nil)

	return &DrainRequestor{
		opts:            opts,
		k8sClient:       k8sClient,
		kubeClient:      kclient,
		platformHelpers: platformHelpers,
		log:             log,
	}, nil
}

// TODO: Align d.log() with reqLogger
func (d *DrainRequestor) DrainNode(ctx context.Context, node *corev1.Node, fullNodeDrain, singleNode bool) (bool, error) {
	reqLogger := ctx.Value("logger").(logr.Logger).WithName("drainNode")
	reqLogger.Info("Node drain requested")

	completed, err := d.platformHelpers.OpenshiftBeforeDrainNode(ctx, node)
	if err != nil {
		reqLogger.Error(err, "error running OpenshiftDrainNode")
		return false, err
	}

	if !completed {
		reqLogger.Info("OpenshiftDrainNode did not finish re queue the node request")
		return false, nil
	}

	// Check if we are on a single node, and we require a reboot/full-drain we just return
	if fullNodeDrain && singleNode {
		return true, nil
	}

	// create node maintenance object
	nm, err := d.newNodeMaintenance(ctx, node.Name)
	if err != nil {
		reqLogger.Error(err, "error creating node maintenance")
		return false, err
	}
	cond := meta.FindStatusCondition(nm.Status.Conditions, maintenancev1alpha1.ConditionReasonReady)
	if cond != nil {
		if cond.Reason == maintenancev1alpha1.ConditionReasonReady {
			d.log.V(consts.LogLevelDebug).Info("node maintenance operation completed", nm.Spec.NodeName, cond.Reason)
			reqLogger.Info("drainNode(): Drain completed")
			return true, nil
		}
	}

	return false, nil
}

// CompleteDrainNode run un-cordon for the requested node
// for openshift system we also remove the pause from the machine config pool this node is part of
// only if we are the last draining node on that pool
func (d *DrainRequestor) CompleteDrainNode(ctx context.Context, node *corev1.Node) (bool, error) {
	logger := ctx.Value("logger").(logr.Logger).WithName("CompleteDrainNode")

	nmName := d.getNodeMaintenanceName(node.Name)
	// run the un cordon function on the node, by deleting node maintenance object
	// once node maintenance object is actually deleted by maintenance operator,
	// the node will be uncordoned.
	err := d.deleteNodeMaintenance(ctx, nmName)
	if err != nil {
		logger.Error(
			err, "failed to delete NodeMaintenance, node uncordon failed", "nodeMaintenance",
			nmName)
		return false, err
	}

	// call the openshift complete drain to unpause the MCP
	// only if we are the last draining node in the pool
	completed, err := d.platformHelpers.OpenshiftAfterCompleteDrainNode(ctx, node)
	if err != nil {
		logger.Error(err, "failed to complete openshift draining")
		return false, err
	}
	//logger.V(2).Info("CompleteDrainNode:()", "drainCompleted", completed)
	logger.Info("CompleteDrainNode:()", "drainCompleted", completed)
	return completed, nil
}

func (d *DrainRequestor) newNodeMaintenance(ctx context.Context, nodeName string) (*maintenancev1alpha1.NodeMaintenance, error) {
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := d.k8sClient.Get(ctx, types.NamespacedName{Name: nodeName,
		Namespace: d.opts.MaintenanceOPRequestorNS},
		nm, &client.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return nm, err
		}
	}
	// check if node maintenance object is already created
	if nm.GetUID() != "" {
		return nm, nil
	}

	d.log.V(consts.LogLevelInfo).Info("Creating", "node maintenance", nm, "namespace", nm.Namespace)
	nm = defaultNodeMaintenance.DeepCopy()
	nm.Name = d.getNodeMaintenanceName(nodeName)
	nm.Spec.NodeName = nodeName
	err = d.k8sClient.Create(ctx, nm, &client.CreateOptions{})
	if err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return nm, err
		}
	}
	return nm, nil
}

// deleteNodeMaintenance requests to delete nodeMaintenance obj
func (d *DrainRequestor) deleteNodeMaintenance(ctx context.Context,
	nodeName string) error {

	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := d.k8sClient.Get(ctx, types.NamespacedName{Name: d.getNodeMaintenanceName(nodeName),
		Namespace: d.opts.MaintenanceOPRequestorNS},
		nm, &client.GetOptions{})
	if err != nil {
		// we expect returned error to be "NotFound", indicating that node has been already uncordoned
		// by maintenance operator
		if !k8serrors.IsNotFound(err) {
			return err
		}
	}
	if nm.Spec.RequestorID == d.opts.MaintenanceOPRequestorID {
		d.log.V(consts.LogLevelInfo).Info("Deleting",
			"nodeMaintenance", client.ObjectKeyFromObject(nm))

		// send deletion request assuming maintenance OP will handle actual obj deletion
		// avoid deletion if deletion timestamp is already set
		if nm.DeletionTimestamp == nil {
			err = d.k8sClient.Delete(ctx, nm)
			if err != nil {
				return err
			}
		}
	}
	return err
}

// getNodeMaintenanceName returns expected name of the nodeMaintenance object
func (d *DrainRequestor) getNodeMaintenanceName(nodeName string) string {
	//return fmt.Sprintf("%s-%s", d.opts.NodeMaintenanceNamePrefix, nodeName)
	return nodeName
}

func GetDrainRequestorOpts(drainer drain.DrainInterface) DrainRequestorOptions {
	drainRequestor, ok := drainer.(*DrainRequestor)
	if !ok {
		return DrainRequestorOptions{}
	}

	return drainRequestor.opts
}

// GetRequestorEnvs returns requstor upgrade related options according to provided environment variables
func GetRequestorOptsFromEnvs() DrainRequestorOptions {
	opts := DrainRequestorOptions{}
	if os.Getenv("MAINTENANCE_OPERATOR_ENABLED") == trueString {
		opts.UseMaintenanceOperator = true
	}
	if os.Getenv("DRAIN_CONTROLLER_REQUESTOR_NAMESPACE") != "" {
		opts.MaintenanceOPRequestorNS = os.Getenv("DRAIN_CONTROLLER_REQUESTOR_NAMESPACE")
	} else {
		opts.MaintenanceOPRequestorNS = "default"
	}
	if os.Getenv("DRAIN_CONTROLLER_REQUESTOR_ID") != "" {
		opts.MaintenanceOPRequestorID = os.Getenv("DRAIN_CONTROLLER_REQUESTOR_ID")
	}
	if os.Getenv("DRAIN_CONTROLLER_NODE_MAINTENANCE_PREFIX") != "" {
		opts.NodeMaintenanceNamePrefix = os.Getenv("DRAIN_CONTROLLER_NODE_MAINTENANCE_PREFIX")
	} else {
		opts.NodeMaintenanceNamePrefix = DefaultNodeMaintenanceNamePrefix
	}
	return opts
}
