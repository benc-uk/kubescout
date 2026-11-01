/*
Purpose: Project supported Kubernetes resources into internal evidence objects.
Flow: Unstructured API responses are identity-checked and reduced to selected
status, ownership and relationship fields for collection and investigation.
Cache wrappers retain projected data, not raw objects; model-facing filtering
and redaction occur downstream.
*/
package kube

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type cachedObject struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Value health.Object
}

type cachedList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []cachedObject
}

// DeepCopyObject copies cache metadata and clones projected maps and slices for runtime consumers.
func (o *cachedObject) DeepCopyObject() runtime.Object {
	copy := *o
	copy.ObjectMeta = *o.ObjectMeta.DeepCopy()
	copy.Value.Conditions = slices.Clone(o.Value.Conditions)
	copy.Value.Containers = slices.Clone(o.Value.Containers)
	copy.Value.Labels = maps.Clone(o.Value.Labels)
	copy.Value.Selector = maps.Clone(o.Value.Selector)
	copy.Value.Ports = slices.Clone(o.Value.Ports)
	copy.Value.Endpoints = slices.Clone(o.Value.Endpoints)

	return &copy
}

// ProjectResource validates identity and scope and returns the approved evidence projection without cache metadata.
func ProjectResource(u *unstructured.Unstructured, kind, namespace string) (health.Object, error) {
	projected, err := project(u, kind, namespace)
	if err != nil {
		return health.Object{}, err
	}

	return projected.Value, nil
}

// DeepCopyObject copies the cached list and each item's metadata and projected collections.
func (o *cachedList) DeepCopyObject() runtime.Object {
	copy := *o

	copy.Items = make([]cachedObject, len(o.Items))
	for i := range o.Items {
		copy.Items[i] = *o.Items[i].DeepCopyObject().(*cachedObject)
	}

	return &copy
}

// project validates identity and namespace before retaining approved fields for a supported resource kind.
// It enforces projection bounds and Warning-event constraints without retaining raw objects or messages.
func project(u *unstructured.Unstructured, kind, namespace string) (*cachedObject, error) {
	if u.GetName() == "" || u.GetUID() == "" || u.GetResourceVersion() == "" || u.GetNamespace() != namespace {
		return nil, errors.New("missing resource identity or unexpected namespace")
	}

	o := health.Object{
		Resource: health.Resource{Kind: kind, Namespace: namespace, Name: u.GetName(), UID: string(u.GetUID())},
		Version:  u.GetResourceVersion(),
		Created:  u.GetCreationTimestamp().Time, Deleting: u.GetDeletionTimestamp() != nil,
	}
	for _, owner := range u.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller && slices.Contains([]string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet", "Job", "Service"}, owner.Kind) {
			o.Owner = health.Resource{Kind: owner.Kind, Namespace: namespace, Name: owner.Name, UID: string(owner.UID)}
			break
		}
	}

	decode := func(out any) error {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, out); err != nil {
			return errors.New("invalid Kubernetes resource schema")
		}

		return nil
	}

	switch kind {
	case "Node":
		var node corev1.Node
		if err := decode(&node); err != nil {
			return nil, err
		}

		for _, c := range node.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time})
		}
	case "Pod":
		var pod corev1.Pod
		if err := decode(&pod); err != nil {
			return nil, err
		}

		o.Phase = string(pod.Status.Phase)
		if len(pod.Labels) > 128 {
			return nil, errors.New("Pod label projection exceeds limit")
		}

		o.Labels = maps.Clone(pod.Labels)
		for _, c := range pod.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time})
		}

		o.Containers = append(containers(pod.Spec.InitContainers, pod.Status.InitContainerStatuses, true), containers(pod.Spec.Containers, pod.Status.ContainerStatuses, false)...)
	case "Deployment":
		var d appsv1.Deployment
		if err := decode(&d); err != nil {
			return nil, err
		}

		o.Desired = 1
		if d.Spec.Replicas != nil {
			o.Desired = *d.Spec.Replicas
		}

		o.Generation, o.Observed = d.Generation, d.Status.ObservedGeneration

		o.Available, o.Updated, o.Paused = d.Status.AvailableReplicas, d.Status.UpdatedReplicas, d.Spec.Paused
		for _, c := range d.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time, Updated: c.LastUpdateTime.Time})
		}
	case "StatefulSet":
		var controller appsv1.StatefulSet
		if err := decode(&controller); err != nil {
			return nil, err
		}

		o.Desired = 1
		if controller.Spec.Replicas != nil {
			o.Desired = *controller.Spec.Replicas
		}

		o.Generation, o.Observed = controller.Generation, controller.Status.ObservedGeneration
		o.Ready, o.Available, o.Current, o.Updated = controller.Status.ReadyReplicas, controller.Status.AvailableReplicas, controller.Status.CurrentReplicas, controller.Status.UpdatedReplicas

		o.UpdateStrategy = string(controller.Spec.UpdateStrategy.Type)
		if controller.Spec.UpdateStrategy.RollingUpdate != nil && controller.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
			o.Partition = *controller.Spec.UpdateStrategy.RollingUpdate.Partition
		}
	case "DaemonSet":
		var controller appsv1.DaemonSet
		if err := decode(&controller); err != nil {
			return nil, err
		}

		o.Generation, o.Observed = controller.Generation, controller.Status.ObservedGeneration
		o.Desired, o.Ready, o.Available, o.Current, o.Updated, o.Misscheduled = controller.Status.DesiredNumberScheduled, controller.Status.NumberReady,
			controller.Status.NumberAvailable, controller.Status.CurrentNumberScheduled, controller.Status.UpdatedNumberScheduled, controller.Status.NumberMisscheduled
		o.UpdateStrategy = string(controller.Spec.UpdateStrategy.Type)
	case "Service":
		var service corev1.Service
		if err := decode(&service); err != nil {
			return nil, err
		}

		if len(service.Spec.Selector) > 128 || len(service.Spec.Ports) > 128 {
			return nil, errors.New("Service projection exceeds limit")
		}

		o.Selector = maps.Clone(service.Spec.Selector)

		o.ServiceType, o.PublishNotReady = string(service.Spec.Type), service.Spec.PublishNotReadyAddresses
		if o.ServiceType == "" {
			o.ServiceType = "ClusterIP"
		}

		for _, port := range service.Spec.Ports {
			protocol, target := string(port.Protocol), port.TargetPort.String()
			if protocol == "" {
				protocol = "TCP"
			}

			if port.TargetPort.IntVal == 0 && port.TargetPort.StrVal == "" {
				target = fmt.Sprint(port.Port)
			}

			o.Ports = append(o.Ports, health.Port{Name: port.Name, Protocol: protocol, Number: port.Port, Target: target})
		}
	case "EndpointSlice":
		var slice discoveryv1.EndpointSlice
		if err := decode(&slice); err != nil {
			return nil, err
		}

		if len(slice.Endpoints) > 1000 || len(slice.Ports) > 128 {
			return nil, errors.New("EndpointSlice projection exceeds limit")
		}

		o.ServiceName, o.AddressType = slice.Labels[discoveryv1.LabelServiceName], string(slice.AddressType)
		for _, port := range slice.Ports {
			value := health.Port{Protocol: "TCP"}
			if port.Name != nil {
				value.Name = *port.Name
			}

			if port.Protocol != nil {
				value.Protocol = string(*port.Protocol)
			}

			if port.Port != nil {
				value.Number = *port.Port
			}

			o.Ports = append(o.Ports, value)
		}

		for _, endpoint := range slice.Endpoints {
			if len(endpoint.Addresses) == 0 || len(endpoint.Addresses) > 100 {
				return nil, errors.New("invalid EndpointSlice address count")
			}

			value := health.Endpoint{Ready: endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready,
				Serving:     endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving,
				Terminating: endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating}
			if endpoint.TargetRef != nil && endpoint.TargetRef.Kind == "Pod" && (endpoint.TargetRef.Namespace == namespace || endpoint.TargetRef.Namespace == "") {
				value.Target = health.Resource{Kind: "Pod", Namespace: namespace, Name: endpoint.TargetRef.Name, UID: string(endpoint.TargetRef.UID)}
			}

			o.Endpoints = append(o.Endpoints, value)
		}
	case "Job":
		var job batchv1.Job
		if err := decode(&job); err != nil {
			return nil, err
		}

		for _, c := range job.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time})
		}
	case "ReplicaSet":
		// Identity and controller ownership connect replica Pods to their Deployment.
	case "Event":
		var event corev1.Event
		if err := decode(&event); err != nil {
			return nil, err
		}

		if event.Type != "Warning" {
			return nil, errors.New("unexpected non-warning event")
		}

		o.Target = health.Resource{Kind: event.InvolvedObject.Kind, Name: event.InvolvedObject.Name, Namespace: event.InvolvedObject.Namespace, UID: string(event.InvolvedObject.UID)}
		if o.Target.Name == "" || o.Target.Kind == "" {
			return nil, errors.New("event has no target identity")
		}

		o.EventTime = event.LastTimestamp.Time
		for _, at := range []time.Time{event.EventTime.Time, event.FirstTimestamp.Time} {
			if at.After(o.EventTime) {
				o.EventTime = at
			}
		}

		o.EventCount = event.Count
		if event.Series != nil {
			if event.Series.LastObservedTime.After(o.EventTime) {
				o.EventTime = event.Series.LastObservedTime.Time
			}

			o.EventCount = event.Series.Count
		}

		o.EventReason = event.Reason
	default:
		return nil, errors.New("unsupported resource kind")
	}

	return &cachedObject{
		TypeMeta:   metav1.TypeMeta{Kind: kind},
		ObjectMeta: metav1.ObjectMeta{Name: u.GetName(), Namespace: namespace, UID: u.GetUID(), ResourceVersion: u.GetResourceVersion()},
		Value:      o,
	}, nil
}

// containers projects status entries with configured images preferred over reported images,
// retaining restart and termination evidence but excluding raw messages and environment values.
func containers(spec []corev1.Container, status []corev1.ContainerStatus, init bool) []health.Container {
	images := map[string]string{}
	for _, c := range spec {
		images[c.Name] = c.Image
	}

	out := make([]health.Container, 0, len(status))
	for _, c := range status {
		image := images[c.Name]
		if image == "" {
			image = c.Image
		}

		value := health.Container{Name: c.Name, Image: image, Init: init, Restarts: c.RestartCount}
		if c.State.Waiting != nil {
			value.Waiting = c.State.Waiting.Reason
		}

		if c.State.Terminated != nil {
			value.ExitCode = new(c.State.Terminated.ExitCode)
			value.Termination = c.State.Terminated.Reason
		}

		if c.LastTerminationState.Terminated != nil {
			value.PreviousExit = new(c.LastTerminationState.Terminated.ExitCode)
			value.PreviousReason = c.LastTerminationState.Terminated.Reason
		}

		out = append(out, value)
	}

	return out
}
