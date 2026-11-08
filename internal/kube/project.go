/*
Purpose: Project supported Kubernetes resources into internal evidence objects.
Flow: Unstructured API responses are identity-checked and reduced to selected
status, ownership and relationship fields for collection and investigation.
Storage retains capacities and private claim links, never volume sources or credentials.
CronJobs retain scheduling facts and active counts, not raw active Job references or templates.
HPAs retain numeric metric/scaling facts without selectors or metrics API access.
Ingresses retain bounded routes and counts, never annotations, Secret names or raw addresses.
VolumeAttachments retain operation/error metadata but exclude inline sources, attachment metadata and error messages.
Cache wrappers retain projected data, not raw objects; model-facing filtering
and redaction occur downstream.
Extra-resource projections validate the configured API identity and retain only selected status facts.
*/
package kube

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/resourcepolicy"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	copy.Value.Claims = slices.Clone(o.Value.Claims)
	copy.Value.Storage.AccessModes = slices.Clone(o.Value.Storage.AccessModes)

	copy.Value.HPA = o.Value.HPA.Clone()
	if o.Value.Extra != nil {
		copy.Value.Extra = runtime.DeepCopyJSON(o.Value.Extra)
	}

	copy.Value.Ingress.Routes = slices.Clone(o.Value.Ingress.Routes)

	if o.Value.CronJob.StartingDeadline != nil {
		copy.Value.CronJob.StartingDeadline = new(*o.Value.CronJob.StartingDeadline)
	}

	if o.Value.CronJob.LastSchedule != nil {
		copy.Value.CronJob.LastSchedule = new(*o.Value.CronJob.LastSchedule)
	}

	if o.Value.CronJob.LastSuccess != nil {
		copy.Value.CronJob.LastSuccess = new(*o.Value.CronJob.LastSuccess)
	}

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

// ProjectExtraResource validates explicit API identity and projects bounded status facts without native describe.
func ProjectExtraResource(u *unstructured.Unstructured, definition resourcepolicy.Resource, namespace string, secrets ...string) (health.Object, error) {
	version := schema.GroupVersion{Group: definition.Group, Version: definition.Version}.String()
	if u.GetKind() != definition.Kind || u.GetAPIVersion() != version || u.GetName() == "" ||
		u.GetUID() == "" || u.GetResourceVersion() == "" || u.GetNamespace() != namespace {
		return health.Object{}, errors.New("extra resource has invalid API identity or namespace")
	}

	facts, err := definition.Project(u.Object, secrets...)
	if err != nil {
		return health.Object{}, err
	}

	return health.Object{Resource: health.Resource{Kind: definition.Kind, APIGroup: definition.Group,
		Namespace: namespace, Name: u.GetName(), UID: string(u.GetUID())},
		Version: u.GetResourceVersion(), Created: u.GetCreationTimestamp().Time,
		Deleting: u.GetDeletionTimestamp() != nil, Extra: facts}, nil
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
		if owner.Controller != nil && *owner.Controller && slices.Contains([]string{"ReplicaSet", "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "Service"}, owner.Kind) {
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
		if len(pod.Spec.Volumes) > 128 {
			return nil, errors.New("Pod volume projection exceeds limit")
		}

		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				o.Claims = append(o.Claims, volume.PersistentVolumeClaim.ClaimName)
			}
		}
	case "PersistentVolumeClaim":
		var claim corev1.PersistentVolumeClaim
		if err := decode(&claim); err != nil {
			return nil, err
		}

		if len(claim.Spec.AccessModes) > 8 || len(claim.Status.Conditions) > 32 {
			return nil, errors.New("PVC projection exceeds limit")
		}

		o.Phase = string(claim.Status.Phase)
		if requested, ok := claim.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			o.Storage.Requested = requested.String()
		}

		if capacity, ok := claim.Status.Capacity[corev1.ResourceStorage]; ok {
			o.Storage.Capacity = capacity.String()
		}

		o.Storage.VolumeName, o.Storage.Mode = claim.Spec.VolumeName, string(corev1.PersistentVolumeFilesystem)

		if claim.Spec.StorageClassName != nil {
			o.Storage.Class = *claim.Spec.StorageClassName
		}

		if claim.Spec.VolumeMode != nil {
			o.Storage.Mode = string(*claim.Spec.VolumeMode)
		}

		for _, mode := range claim.Spec.AccessModes {
			o.Storage.AccessModes = append(o.Storage.AccessModes, string(mode))
		}

		for _, c := range claim.Status.Conditions {
			o.Conditions = append(o.Conditions, health.Condition{Type: string(c.Type), Status: string(c.Status), Reason: c.Reason, Transition: c.LastTransitionTime.Time, Updated: c.LastProbeTime.Time})
		}
	case "VolumeAttachment":
		var attachment storagev1.VolumeAttachment
		if err := decode(&attachment); err != nil {
			return nil, err
		}

		if attachment.Spec.Attacher == "" || attachment.Spec.NodeName == "" ||
			len(attachment.Spec.Attacher) > 253 || len(attachment.Spec.NodeName) > 253 ||
			(attachment.Spec.Source.PersistentVolumeName == nil) == (attachment.Spec.Source.InlineVolumeSpec == nil) {
			return nil, errors.New("invalid or oversized VolumeAttachment specification")
		}

		o.Attachment = health.AttachmentState{Attacher: attachment.Spec.Attacher, NodeName: attachment.Spec.NodeName,
			Inline: attachment.Spec.Source.InlineVolumeSpec != nil, Attached: attachment.Status.Attached,
			AttachError: attachmentError(attachment.Status.AttachError), DetachError: attachmentError(attachment.Status.DetachError)}

		if attachment.Spec.Source.PersistentVolumeName != nil {
			name := *attachment.Spec.Source.PersistentVolumeName
			if name == "" || len(name) > 253 {
				return nil, errors.New("invalid or oversized VolumeAttachment PV name")
			}

			o.Attachment.VolumeName = name
		}

		if attachment.DeletionTimestamp != nil {
			o.Attachment.DeletingSince = attachment.DeletionTimestamp.Time
		}
	case "PersistentVolume":
		var volume corev1.PersistentVolume
		if err := decode(&volume); err != nil {
			return nil, err
		}

		if len(volume.Spec.AccessModes) > 8 {
			return nil, errors.New("PV projection exceeds limit")
		}

		o.Phase = string(volume.Status.Phase)
		if capacity, ok := volume.Spec.Capacity[corev1.ResourceStorage]; ok {
			o.Storage.Capacity = capacity.String()
		}

		o.Storage.Class = volume.Spec.StorageClassName
		o.Storage.ReclaimPolicy, o.Storage.Mode = string(volume.Spec.PersistentVolumeReclaimPolicy), string(corev1.PersistentVolumeFilesystem)

		if volume.Spec.VolumeMode != nil {
			o.Storage.Mode = string(*volume.Spec.VolumeMode)
		}

		for _, mode := range volume.Spec.AccessModes {
			o.Storage.AccessModes = append(o.Storage.AccessModes, string(mode))
		}

		if claim := volume.Spec.ClaimRef; claim != nil && (claim.Kind == "" || claim.Kind == "PersistentVolumeClaim") {
			o.Storage.Claim = health.Resource{Kind: "PersistentVolumeClaim", Namespace: claim.Namespace, Name: claim.Name, UID: string(claim.UID)}
		}
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
	case "Ingress":
		var ingress networkingv1.Ingress
		if err := decode(&ingress); err != nil {
			return nil, err
		}

		if err := projectIngress(ingress, &o); err != nil {
			return nil, err
		}
	case "HorizontalPodAutoscaler":
		var hpa autoscalingv2.HorizontalPodAutoscaler
		if err := decode(&hpa); err != nil {
			return nil, err
		}

		if err := projectHPA(hpa, &o); err != nil {
			return nil, err
		}
	case "CronJob":
		var cron batchv1.CronJob
		if err := decode(&cron); err != nil {
			return nil, err
		}

		if len(cron.Status.Active) > 128 || len(cron.Spec.Schedule) > 256 || cron.Spec.TimeZone != nil && len(*cron.Spec.TimeZone) > 128 {
			return nil, errors.New("CronJob projection exceeds limit")
		}

		o.Generation = cron.Generation
		o.CronJob = health.CronJobState{Schedule: cron.Spec.Schedule, ConcurrencyPolicy: string(cron.Spec.ConcurrencyPolicy),
			StartingDeadline: cron.Spec.StartingDeadlineSeconds, SuccessfulHistory: 3, FailedHistory: 1, Active: len(cron.Status.Active)}

		if o.CronJob.ConcurrencyPolicy == "" {
			o.CronJob.ConcurrencyPolicy = string(batchv1.AllowConcurrent)
		}

		if cron.Spec.TimeZone != nil {
			o.CronJob.TimeZone = *cron.Spec.TimeZone
		}

		if cron.Spec.Suspend != nil {
			o.CronJob.Suspended = *cron.Spec.Suspend
		}

		if cron.Spec.SuccessfulJobsHistoryLimit != nil {
			o.CronJob.SuccessfulHistory = *cron.Spec.SuccessfulJobsHistoryLimit
		}

		if cron.Spec.FailedJobsHistoryLimit != nil {
			o.CronJob.FailedHistory = *cron.Spec.FailedJobsHistoryLimit
		}

		if cron.Status.LastScheduleTime != nil {
			o.CronJob.LastSchedule = new(cron.Status.LastScheduleTime.Time)
		}

		if cron.Status.LastSuccessfulTime != nil {
			o.CronJob.LastSuccess = new(cron.Status.LastSuccessfulTime.Time)
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
		if !resourcepolicy.BuiltinKind(o.Target.Kind) {
			version, err := schema.ParseGroupVersion(event.InvolvedObject.APIVersion)
			if err != nil {
				return nil, errors.New("invalid event target API version")
			}

			o.Target.APIGroup = version.Group
		}

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

// attachmentError projects error presence, time and optional numeric code, excluding raw driver messages.
func attachmentError(value *storagev1.VolumeError) health.AttachmentError {
	if value == nil {
		return health.AttachmentError{}
	}

	out := health.AttachmentError{Present: true, At: value.Time.Time}
	if value.ErrorCode != nil {
		out.HasCode, out.Code = true, *value.ErrorCode
	}

	return out
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
