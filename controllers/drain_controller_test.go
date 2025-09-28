package controllers //nolint:dupl

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	sriovnetworkv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	constants "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/utils"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/vars"
)

var (
	drainRequestorID = "drain.requestor.com"
	drainRequestorNS = "default"
)

var _ = Describe("Drain Controller", Ordered, func() {

	BeforeAll(func() {
		By("Setup maintennace controller mock")
		mockNodeMaintenanceController(ctx, k8sClient, metav1.Condition{
			Type:               maintenancev1alpha1.ConditionTypeReady,
			Status:             v1.ConditionTrue,
			Reason:             maintenancev1alpha1.ConditionReasonReady,
			Message:            "Maintenance completed successfully",
			LastTransitionTime: v1.NewTime(time.Now()),
		})

		err := k8sClient.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"}})
		Expect(err).ToNot(HaveOccurred())
	})

	BeforeEach(func() {
		Expect(k8sClient.DeleteAllOf(context.Background(), &corev1.Node{}, &client.DeleteAllOfOptions{DeleteOptions: client.DeleteOptions{GracePeriodSeconds: pointer.Int64(0)}})).ToNot(HaveOccurred())
		Expect(k8sClient.DeleteAllOf(context.Background(), &sriovnetworkv1.SriovNetworkNodeState{}, client.InNamespace(vars.Namespace), &client.DeleteAllOfOptions{DeleteOptions: client.DeleteOptions{GracePeriodSeconds: pointer.Int64(0)}})).ToNot(HaveOccurred())
		Expect(k8sClient.DeleteAllOf(context.Background(), &corev1.Pod{}, client.InNamespace(namespaceName), &client.DeleteAllOfOptions{DeleteOptions: client.DeleteOptions{GracePeriodSeconds: pointer.Int64(0)}})).ToNot(HaveOccurred())

		poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
		poolConfig.SetNamespace(namespaceName)
		poolConfig.SetName("test-workers")
		err := k8sClient.Delete(context.Background(), poolConfig)
		if err != nil {
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
		}

		podList := &corev1.PodList{}
		err = k8sClient.List(context.Background(), podList, &client.ListOptions{Namespace: "default"})
		Expect(err).ToNot(HaveOccurred())
		for _, podObj := range podList.Items {
			err = k8sClient.Delete(context.Background(), &podObj, &client.DeleteOptions{GracePeriodSeconds: pointer.Int64(0)})
			Expect(err).ToNot(HaveOccurred())
		}

	})

	Context("when there is only one node", func() {

		It("should drain single node on drain require", func(ctx context.Context) {
			node, nodeState := createNode(ctx, "node1")

			simulateDaemonSetAnnotation(node, constants.DrainRequired)

			expectNodeStateAnnotation(nodeState, constants.DrainComplete)
			expectNodeIsNotSchedulable(node)

			simulateDaemonSetAnnotation(node, constants.DrainIdle)

			expectNodeStateAnnotation(nodeState, constants.DrainIdle)
			expectNodeIsSchedulable(node)

		})

		It("should not drain on reboot for single node", func(ctx context.Context) {
			node, nodeState := createNode(ctx, "node1")

			simulateDaemonSetAnnotation(node, constants.RebootRequired)

			expectNodeStateAnnotation(nodeState, constants.DrainComplete)
			expectNodeIsSchedulable(node)

			simulateDaemonSetAnnotation(node, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState, constants.DrainIdle)
			expectNodeIsSchedulable(node)
		})

		It("should drain on reboot for multiple node", func(ctx context.Context) {
			node, nodeState := createNode(ctx, "node1")
			createNode(ctx, "node2")

			simulateDaemonSetAnnotation(node, constants.RebootRequired)

			expectNodeStateAnnotation(nodeState, constants.DrainComplete)
			expectNodeIsNotSchedulable(node)

			simulateDaemonSetAnnotation(node, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState, constants.DrainIdle)
			expectNodeIsSchedulable(node)
		})
	})

	Context("when there are multiple nodes", func() {

		It("should drain nodes serially with default pool selector", func(ctx context.Context) {
			node1, nodeState1 := createNode(ctx, "node1")
			node2, nodeState2 := createNode(ctx, "node2")
			node3, nodeState3 := createNode(ctx, "node3")

			// Two nodes require to drain at the same time
			simulateDaemonSetAnnotation(node1, constants.DrainRequired)
			simulateDaemonSetAnnotation(node2, constants.DrainRequired)

			// Only the first node drains
			expectNodeStateAnnotation(nodeState1, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState2, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsNotSchedulable(node1)
			expectNodeIsSchedulable(node2)
			expectNodeIsSchedulable(node3)

			simulateDaemonSetAnnotation(node1, constants.DrainIdle)

			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeIsSchedulable(node1)

			// Second node starts draining
			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState2, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsSchedulable(node1)
			expectNodeIsNotSchedulable(node2)
			expectNodeIsSchedulable(node3)

			simulateDaemonSetAnnotation(node2, constants.DrainIdle)

			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState2, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsSchedulable(node1)
			expectNodeIsSchedulable(node2)
			expectNodeIsSchedulable(node3)
		})

		It("should drain nodes in parallel with a custom pool selector", func(ctx context.Context) {
			node1, nodeState1 := createNode(ctx, "node1")
			node2, nodeState2 := createNode(ctx, "node2")
			node3, nodeState3 := createNode(ctx, "node3")

			maxun := intstr.Parse("2")
			poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
			poolConfig.SetNamespace(namespaceName)
			poolConfig.SetName("test-workers")
			poolConfig.Spec = sriovnetworkv1.SriovNetworkPoolConfigSpec{MaxUnavailable: &maxun, NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"test": "",
				},
			}}
			Expect(k8sClient.Create(context.TODO(), poolConfig)).Should(Succeed())

			// Two nodes require to drain at the same time
			simulateDaemonSetAnnotation(node1, constants.DrainRequired)
			simulateDaemonSetAnnotation(node2, constants.DrainRequired)

			// Both nodes drain
			expectNodeStateAnnotation(nodeState1, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState2, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsNotSchedulable(node1)
			expectNodeIsNotSchedulable(node2)
			expectNodeIsSchedulable(node3)

			simulateDaemonSetAnnotation(node1, constants.DrainIdle)

			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeIsSchedulable(node1)

			// Second node starts draining
			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState2, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsSchedulable(node1)
			expectNodeIsNotSchedulable(node2)
			expectNodeIsSchedulable(node3)

			simulateDaemonSetAnnotation(node2, constants.DrainIdle)

			expectNodeStateAnnotation(nodeState1, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState2, constants.DrainIdle)
			expectNodeStateAnnotation(nodeState3, constants.DrainIdle)
			expectNodeIsSchedulable(node1)
			expectNodeIsSchedulable(node2)
			expectNodeIsSchedulable(node3)
		})

		It("should drain nodes in parallel with a custom pool selector and honor MaxUnavailable", func(ctx context.Context) {
			node1, nodeState1 := createNode(ctx, "node1")
			node2, nodeState2 := createNode(ctx, "node2")
			node3, nodeState3 := createNode(ctx, "node3")

			maxun := intstr.Parse("2")
			poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
			poolConfig.SetNamespace(namespaceName)
			poolConfig.SetName("test-workers")
			poolConfig.Spec = sriovnetworkv1.SriovNetworkPoolConfigSpec{MaxUnavailable: &maxun, NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"test": "",
				},
			}}
			Expect(k8sClient.Create(context.TODO(), poolConfig)).Should(Succeed())

			// Two nodes require to drain at the same time
			simulateDaemonSetAnnotation(node1, constants.DrainRequired)
			simulateDaemonSetAnnotation(node2, constants.DrainRequired)
			simulateDaemonSetAnnotation(node3, constants.DrainRequired)

			expectNumberOfDrainingNodes(2, nodeState1, nodeState2, nodeState3)
			ExpectDrainCompleteNodesHaveIsNotSchedule(nodeState1, nodeState2, nodeState3)
		})

		It("should drain all nodes in parallel with a custom pool using nil in max unavailable", func(ctx context.Context) {
			node1, nodeState1 := createNode(ctx, "node1")
			node2, nodeState2 := createNode(ctx, "node2")
			node3, nodeState3 := createNode(ctx, "node3")

			poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
			poolConfig.SetNamespace(namespaceName)
			poolConfig.SetName("test-workers")
			poolConfig.Spec = sriovnetworkv1.SriovNetworkPoolConfigSpec{MaxUnavailable: nil, NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"test": "",
				},
			}}
			Expect(k8sClient.Create(context.TODO(), poolConfig)).Should(Succeed())

			// Two nodes require to drain at the same time
			simulateDaemonSetAnnotation(node1, constants.DrainRequired)
			simulateDaemonSetAnnotation(node2, constants.DrainRequired)
			simulateDaemonSetAnnotation(node3, constants.DrainRequired)

			expectNodeStateAnnotation(nodeState1, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState2, constants.DrainComplete)
			expectNodeStateAnnotation(nodeState3, constants.DrainComplete)
			expectNodeIsNotSchedulable(node1)
			expectNodeIsNotSchedulable(node2)
			expectNodeIsNotSchedulable(node3)
		})

		It("should drain in parallel nodes from two different pools, one custom and one default", func() {
			node1, nodeState1 := createNode(ctx, "node1")
			node2, nodeState2 := createNodeWithLabel(ctx, "node2", "pool")
			createPodOnNode(ctx, "test-node-2", "node2")

			maxun := intstr.Parse("1")
			poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
			poolConfig.SetNamespace(namespaceName)
			poolConfig.SetName("test-workers")
			poolConfig.Spec = sriovnetworkv1.SriovNetworkPoolConfigSpec{MaxUnavailable: &maxun, NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"pool": "",
				},
			}}
			Expect(k8sClient.Create(context.TODO(), poolConfig)).Should(Succeed())

			simulateDaemonSetAnnotation(node2, constants.RebootRequired)
			expectNodeStateAnnotation(nodeState2, constants.Draining)

			simulateDaemonSetAnnotation(node1, constants.DrainRequired)
			expectNodeStateAnnotation(nodeState1, constants.DrainComplete)
		})

		It("should select all the nodes to drain in parallel when the selector is empty", func() {
			node1, nodeState1 := createNode(ctx, "node3")
			node2, nodeState2 := createNodeWithLabel(ctx, "node4", "pool")
			createPodOnNode(ctx, "test-empty-1", "node3")
			createPodOnNode(ctx, "test-empty-2", "node4")

			maxun := intstr.Parse("10")
			poolConfig := &sriovnetworkv1.SriovNetworkPoolConfig{}
			poolConfig.SetNamespace(namespaceName)
			poolConfig.SetName("test-workers")
			poolConfig.Spec = sriovnetworkv1.SriovNetworkPoolConfigSpec{MaxUnavailable: &maxun}
			Expect(k8sClient.Create(context.TODO(), poolConfig)).Should(Succeed())

			simulateDaemonSetAnnotation(node2, constants.RebootRequired)
			simulateDaemonSetAnnotation(node1, constants.RebootRequired)
			expectNodeStateAnnotation(nodeState2, constants.Draining)
			expectNodeStateAnnotation(nodeState1, constants.Draining)
		})
	})
})

func expectNodeStateAnnotation(nodeState *sriovnetworkv1.SriovNetworkNodeState, expectedAnnotationValue string) {
	EventuallyWithOffset(1, func(g Gomega) {
		g.Expect(k8sClient.Get(context.Background(), types.NamespacedName{Namespace: nodeState.Namespace, Name: nodeState.Name}, nodeState)).
			ToNot(HaveOccurred())

		g.Expect(utils.ObjectHasAnnotation(nodeState, constants.NodeStateDrainAnnotationCurrent, expectedAnnotationValue)).
			To(BeTrue(),
				"Node[%s] annotation[%s] == '%s'. Expected '%s'", nodeState.Name, constants.NodeDrainAnnotation, nodeState.GetLabels()[constants.NodeStateDrainAnnotationCurrent], expectedAnnotationValue)
	}, "20s", "1s").Should(Succeed())
}

func expectNumberOfDrainingNodes(numbOfDrain int, nodesState ...*sriovnetworkv1.SriovNetworkNodeState) {
	EventuallyWithOffset(1, func(g Gomega) {
		drainingNodes := 0
		for _, nodeState := range nodesState {
			g.Expect(k8sClient.Get(context.Background(), types.NamespacedName{Namespace: nodeState.Namespace, Name: nodeState.Name}, nodeState)).
				ToNot(HaveOccurred())

			if utils.ObjectHasAnnotation(nodeState, constants.NodeStateDrainAnnotationCurrent, constants.DrainComplete) {
				drainingNodes++
			}
		}

		g.Expect(drainingNodes).To(Equal(numbOfDrain))
	}, "20s", "1s").Should(Succeed())
}

func ExpectDrainCompleteNodesHaveIsNotSchedule(nodesState ...*sriovnetworkv1.SriovNetworkNodeState) {
	for _, nodeState := range nodesState {
		if utils.ObjectHasAnnotation(nodeState, constants.NodeStateDrainAnnotationCurrent, constants.DrainComplete) {
			node := &corev1.Node{}
			Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: nodeState.Name}, node)).
				ToNot(HaveOccurred())
			expectNodeIsNotSchedulable(node)
		}
	}
}

func expectNodeIsNotSchedulable(node *corev1.Node) {
	EventuallyWithOffset(1, func(g Gomega) {
		g.Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: node.Name}, node)).
			ToNot(HaveOccurred())

		g.Expect(node.Spec.Unschedulable).To(BeTrue())
	}, "20s", "1s").Should(Succeed())
}

func expectNodeIsSchedulable(node *corev1.Node) {
	EventuallyWithOffset(1, func(g Gomega) {
		g.Expect(k8sClient.Get(context.Background(), types.NamespacedName{Name: node.Name}, node)).
			ToNot(HaveOccurred())

		g.Expect(node.Spec.Unschedulable).To(BeFalse())
	}, "20s", "1s").Should(Succeed())
}

func simulateDaemonSetAnnotation(node *corev1.Node, drainAnnotationValue string) {
	ExpectWithOffset(1,
		utils.AnnotateObject(context.Background(), node, constants.NodeDrainAnnotation, drainAnnotationValue, k8sClient)).
		ToNot(HaveOccurred())
}

func createNode(ctx context.Context, nodeName string) (*corev1.Node, *sriovnetworkv1.SriovNetworkNodeState) {
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				constants.NodeDrainAnnotation:                     constants.DrainIdle,
				"machineconfiguration.openshift.io/desiredConfig": "worker-1",
			},
			Labels: map[string]string{
				"test": "",
			},
		},
	}

	nodeState := sriovnetworkv1.SriovNetworkNodeState{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: vars.Namespace,
			Labels: map[string]string{
				constants.NodeStateDrainAnnotationCurrent: constants.DrainIdle,
			},
		},
	}

	Expect(k8sClient.Create(ctx, &node)).ToNot(HaveOccurred())
	Expect(k8sClient.Create(ctx, &nodeState)).ToNot(HaveOccurred())

	return &node, &nodeState
}

func createNodeWithLabel(ctx context.Context, nodeName string, label string) (*corev1.Node, *sriovnetworkv1.SriovNetworkNodeState) {
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				constants.NodeDrainAnnotation:                     constants.DrainIdle,
				"machineconfiguration.openshift.io/desiredConfig": "worker-1",
			},
			Labels: map[string]string{
				label: "",
			},
		},
	}

	nodeState := sriovnetworkv1.SriovNetworkNodeState{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: vars.Namespace,
			Annotations: map[string]string{
				constants.NodeStateDrainAnnotationCurrent: constants.DrainIdle,
			},
		},
	}

	Expect(k8sClient.Create(ctx, &node)).ToNot(HaveOccurred())
	Expect(k8sClient.Create(ctx, &nodeState)).ToNot(HaveOccurred())

	return &node, &nodeState
}

func createPodOnNode(ctx context.Context, podName, nodeName string) {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "test", Image: "test", Command: []string{"test"}}},
			NodeName: nodeName, TerminationGracePeriodSeconds: pointer.Int64(60)}}
	Expect(k8sClient.Create(ctx, &pod)).ToNot(HaveOccurred())
}

func mockNodeMaintenanceController(
	ctx context.Context,
	c client.Client,
	desiredCondition metav1.Condition,
) {
	go func() {
		defer GinkgoRecover()

		// Create a rate-limited work queue
		queue := workqueue.NewNamedRateLimitingQueue(
			workqueue.NewItemExponentialFailureRateLimiter(100*time.Millisecond, 5*time.Second),
			"nodeMaintenance",
		)
		defer queue.ShutDown()

		// Add initial work item to start polling
		queue.Add("poll")

		for {
			select {
			case <-ctx.Done():
				return
			default:
				item, shutdown := queue.Get()
				if shutdown {
					return
				}

				// Process work item directly (no second goroutine)
				func() {
					defer queue.Done(item)

					// Process the polling work
					nms := &maintenancev1alpha1.NodeMaintenanceList{}
					err := c.List(ctx, nms, client.InNamespace(drainRequestorNS))
					if err != nil {
						return
					}

					processItems(ctx, c, nms, desiredCondition)
					// Re-queue for next poll (success case)
					queue.AddAfter("poll", 100*time.Millisecond)
				}()
			}
		}
	}()
}

func processItems(ctx context.Context, c client.Client,
	nms *maintenancev1alpha1.NodeMaintenanceList, desiredCondition metav1.Condition) {
	// Process each NodeMaintenance object
	for _, nm := range nms.Items {
		if nm.Spec.RequestorID != drainRequestorID {
			continue
		}

		// Handle deletion case
		if !nm.DeletionTimestamp.IsZero() {
			By("maintenance operator: remove finalizer")
			if controllerutil.ContainsFinalizer(&nm, maintenancev1alpha1.MaintenanceFinalizerName) {
				//Expect(controllerutil.RemoveFinalizer(&nm, maintenancev1alpha1.MaintenanceFinalizerName)).To(Succeed())
				original := nm.DeepCopy()
				nm.SetFinalizers([]string{})
				patch := client.MergeFrom(original)
				Expect(c.Patch(ctx, &nm, patch)).To(Succeed())
			}

			// uncordon node
			By("maintenance operator: uncordon node")
			node := &corev1.Node{}
			Expect(c.Get(ctx, types.NamespacedName{Name: nm.Name}, node)).ToNot(HaveOccurred())
			node.Spec.Unschedulable = false
			Expect(c.Update(ctx, node)).NotTo(Succeed())
			continue
		}

		// Handle creation/update case
		if !controllerutil.ContainsFinalizer(&nm, maintenancev1alpha1.MaintenanceFinalizerName) {
			By("maintenance operator: add finalizer")
			nm.Finalizers = append(nm.Finalizers, maintenancev1alpha1.MaintenanceFinalizerName)
			Expect(c.Update(ctx, &nm)).To(Succeed())
			continue
		}

		// Update status conditions
		if nm.Status.Conditions == nil {
			Expect(nm.Finalizers).To(ContainElement(maintenancev1alpha1.MaintenanceFinalizerName))
			// Update status conditions
			meta.SetStatusCondition(&nm.Status.Conditions, desiredCondition)
			Expect(c.Status().Update(ctx, &nm)).To(Succeed())
			Expect(nm.Status.Conditions).To(HaveLen(1))

			// cordon node
			By("maintenance operator: cordon node")
			node := &corev1.Node{}
			Expect(c.Get(ctx, types.NamespacedName{Name: nm.Name}, node)).ToNot(HaveOccurred())
			node.Spec.Unschedulable = true
			Expect(c.Update(ctx, node)).To(Succeed())
		}
	}
}
