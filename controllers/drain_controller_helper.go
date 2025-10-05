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

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sriovnetworkv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	constants "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/utils"
)

func (dr *DrainReconcile) handleNodeIdleNodeStateDrainingOrCompleted(ctx context.Context,
	node *corev1.Node,
	nodeNetworkState *sriovnetworkv1.SriovNetworkNodeState) (ctrl.Result, error) {
	dr.log.WithName("handleNodeIdleNodeStateDrainingOrCompleted")
	completed, err := dr.drainer.CompleteDrainNode(ctx, node)
	if err != nil {
		dr.log.Error(err, "failed to complete drain on node")
		dr.recorder.Event(nodeNetworkState,
			corev1.EventTypeWarning,
			"DrainController",
			"failed to drain node")
		return ctrl.Result{}, err
	}

	// if we didn't manage to complete the un drain of the node we retry
	if !completed {
		dr.log.Info("complete drain was not completed re queueing the request")
		dr.recorder.Event(nodeNetworkState,
			corev1.EventTypeWarning,
			"DrainController",
			"node complete drain was not completed")
		// TODO: make this time configurable
		return reconcile.Result{RequeueAfter: constants.DrainControllerRequeueTime}, nil
	}

	// check if node annotation is already set to drain idle
	if utils.ObjectHasAnnotation(nodeNetworkState, constants.NodeStateDrainAnnotationCurrent, constants.DrainIdle) {
		dr.log.Info("node annotation is already set to drain idle, nothing to do")
		return ctrl.Result{}, nil
	}

	// move the node state back to idle
	err = utils.AnnotateObject(ctx, nodeNetworkState, constants.NodeStateDrainAnnotationCurrent, constants.DrainIdle, dr.Client)
	if err != nil {
		dr.log.Error(err, "failed to annotate node with annotation", "annotation", constants.DrainIdle)
		return ctrl.Result{}, err
	}

	dr.log.Info("completed the un drain for node")
	dr.recorder.Event(nodeNetworkState,
		corev1.EventTypeWarning,
		"DrainController",
		"node un drain completed")
	return ctrl.Result{}, nil
}

func (dr *DrainReconcile) handleNodeDrainOrReboot(ctx context.Context,
	node *corev1.Node,
	nodeNetworkState *sriovnetworkv1.SriovNetworkNodeState,
	nodeDrainAnnotation,
	nodeStateDrainAnnotationCurrent string) (ctrl.Result, error) {
	dr.log.WithName("handleNodeDrainOrReboot")
	// nothing to do here we need to wait for the node to move back to idle
	if nodeStateDrainAnnotationCurrent == constants.DrainComplete {
		dr.log.Info("node requested a drain and nodeState is on drain completed nothing todo")
		return ctrl.Result{}, nil
	}

	// Check if we are on a single node, and we require a reboot/full-drain we just return
	fullNodeDrain := nodeDrainAnnotation == constants.RebootRequired
	singleNode := false
	if fullNodeDrain {
		nodeList := &corev1.NodeList{}
		err := dr.Client.List(ctx, nodeList)
		if err != nil {
			dr.log.Error(err, "failed to list nodes")
			return ctrl.Result{}, err
		}
		if len(nodeList.Items) == 1 {
			dr.log.Info("drainNode(): FullNodeDrain requested and we are on Single node")
			singleNode = true
		}
	}

	// call the drain function that will also call drain to other platform providers like openshift
	drained, err := dr.drainer.DrainNode(ctx, node, fullNodeDrain, singleNode)
	if err != nil {
		dr.log.Error(err, "error trying to drain the node")
		dr.recorder.Event(nodeNetworkState,
			corev1.EventTypeWarning,
			"DrainController",
			"failed to drain node")
		return reconcile.Result{}, err
	}

	// if we didn't manage to complete the drain of the node we retry
	if !drained {
		dr.log.Info("the nodes was not drained re queueing the request")
		dr.recorder.Event(nodeNetworkState,
			corev1.EventTypeWarning,
			"DrainController",
			"node drain operation was not completed")
		return reconcile.Result{RequeueAfter: constants.DrainControllerRequeueTime}, nil
	}

	// if we manage to drain we label the node state with drain completed and finish
	err = utils.AnnotateObject(ctx, nodeNetworkState, constants.NodeStateDrainAnnotationCurrent, constants.DrainComplete, dr.Client)
	if err != nil {
		dr.log.Error(err, "failed to annotate node with annotation", "annotation", constants.DrainComplete)
		return ctrl.Result{}, err
	}

	dr.log.Info("node drained successfully")
	dr.recorder.Event(nodeNetworkState,
		corev1.EventTypeWarning,
		"DrainController",
		"node drain completed")
	return ctrl.Result{}, nil
}
