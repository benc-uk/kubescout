package kube

import (
	"errors"
	"slices"
	"time"

	"k8s-ops-agent/internal/health"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type cachedObject struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Value health.Object
}

func (o *cachedObject) DeepCopyObject() runtime.Object {
	copy := *o
	copy.ObjectMeta = *o.ObjectMeta.DeepCopy()
	copy.Value.Conditions = slices.Clone(o.Value.Conditions)
	copy.Value.Containers = slices.Clone(o.Value.Containers)
	return &copy
}

type cachedList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []cachedObject
}

func (o *cachedList) DeepCopyObject() runtime.Object {
	copy := *o
	copy.Items = make([]cachedObject, len(o.Items))
	for i := range o.Items {
		copy.Items[i] = *o.Items[i].DeepCopyObject().(*cachedObject)
	}
	return &copy
}

func project(u *unstructured.Unstructured, kind, namespace string) (*cachedObject, error) {
	if u.GetName() == "" || u.GetUID() == "" || u.GetResourceVersion() == "" || u.GetNamespace() != namespace {
		return nil, errors.New("missing resource identity or unexpected namespace")
	}
	o := health.Object{
		Resource: health.Resource{Kind: kind, Namespace: namespace, Name: u.GetName(), UID: string(u.GetUID())},
		Created:  u.GetCreationTimestamp().Time, Deleting: u.GetDeletionTimestamp() != nil,
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
	case "Job":
		var job batchv1.Job
		if err := decode(&job); err != nil {
			return nil, err
		}
		for _, c := range job.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time})
		}
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
