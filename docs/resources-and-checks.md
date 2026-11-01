# Kubernetes resources and health checks

[Overview](../README.md) · [Architecture](../ARCHITECTURE.md) · [Operations](operations.md)

This is the reference for the Kubernetes resources KubeScout gathers, the facts
it retains, and the health checks it runs on those facts. It describes the
current implementation, including StatefulSet, DaemonSet, PVC/PV, CronJob, HPA,
Ingress and VolumeAttachment support.

## How to interpret a check

Checks produce **private evidence for AI investigation**, not incidents.
Their `info`, `warning` and `critical` severities are investigation hints, not
the final incident severity. AI may choose **no action**, even for critical
evidence. Only a validated AI decision can raise, update, reopen or resolve an
incident.

Likewise, a disappearing finding does not automatically resolve an incident.
Recovery requires continuous fresh observations and an AI review. Collection
outages, missing permissions, tool failures and model failures are diagnostics,
not evidence of health or synthetic incidents. No findings or incidents is not
proof that the cluster is healthy.

For scheduling, correlation and lifecycle details, see the
[architecture guide](../ARCHITECTURE.md).

## Collection inventory

The [collector](../internal/kube/source.go) gathers the following sixteen resource
kinds. Watch mode uses initial paginated lists followed by watches; once mode
uses bounded lists. A watch notification is an API change notification, not a
Kubernetes Event object.

All projections retain resource identity, resource version, creation time and
deletion state for internal use. Supported controller owner references are
retained for correlation. The table lists additional facts from the
[projection implementation](../internal/kube/project.go) and
[internal object types](../internal/health/types.go), not whole API objects.

| Kind | API version | Scope | Additional facts retained | Check or relationship purpose |
|---|---|---|---|---|
| Node | `v1` | Cluster | Condition types, statuses, reasons and transition times | Readiness and resource-pressure evidence |
| Pod | `v1` | Namespace | Phase; conditions; regular/init container names, images, waiting reasons, current/previous termination reasons and exit codes, cumulative restarts; private labels and explicit PVC names | Scheduling, readiness and container evidence; controller, Service and PVC relationships |
| Deployment | `apps/v1` | Namespace | Desired, available and updated replicas; generation/observed generation; paused state; conditions and their transition/update times | Rollout progress and replica failures |
| ReplicaSet | `apps/v1` | Namespace | Identity and controller ownership only | Connects replica Pods to Deployments; **no standalone health check** |
| StatefulSet | `apps/v1` | Namespace | Desired, ready, available, current and updated replicas; generation/observed generation; update strategy and rolling-update partition | Replica/update shortfalls and direct Pod ownership |
| DaemonSet | `apps/v1` | Namespace | Desired/current/updated scheduled counts; ready, available and misscheduled counts; generation/observed generation; update strategy | Placement, availability and update shortfalls; direct Pod ownership |
| Service | `v1` | Namespace | Type; private selector; port names, numbers, protocols and targets; `publishNotReadyAddresses` | Backend availability and port-definition evidence |
| EndpointSlice | `discovery.k8s.io/v1` | Namespace | Service-name linkage, address type, ports, endpoint ready/serving/terminating conditions and same-namespace Pod target references | Supports Service checks and related-resource context; **no standalone health check** |
| PersistentVolumeClaim | `v1` | Namespace | Phase; storage class, requested/reported capacity, access modes and volume mode; condition types/statuses/reasons/times; private bound PV name | Pending/lost binding and resize-error evidence; scoped consumer and verified PV context |
| PersistentVolume | `v1` | Cluster | Phase; storage class, capacity, access modes, volume mode and reclaim policy; private claimRef identity | Explicit failed-volume evidence; verified PVC binding context |
| VolumeAttachment | `storage.k8s.io/v1` | Cluster | Attacher, Node/PV names or inline-source flag, attached/deleting state and deletion time; attach/detach error presence, optional code and timestamp, not raw messages | Stage-aware attachment error evidence and current PV/Node plus scoped claim/consumer context |
| Job | `batch/v1` | Namespace | Condition types, statuses, reasons and transition times | Terminal Job failures and Pod ownership |
| CronJob | `batch/v1` | Namespace | Schedule, optional time zone, suspension, concurrency policy, optional starting deadline, history limits, active Job count and last schedule/success times | Stable owner for scheduled Jobs and their Pods; **no standalone health check** |
| HorizontalPodAutoscaler | `autoscaling/v2` | Namespace | Scale-target API/kind/name; min/max and current/desired replicas; generation/observed generation; last-scale time; condition types/statuses/reasons/times; approved metric targets/current numeric values and scaling behavior | Inactive/unable-to-scale evidence and same-namespace workload context, not controller ownership |
| Ingress | `networking.k8s.io/v1` | Namespace | Generation, explicit class name; default/HTTP routes with host/path/path type and Service name/port; custom-backend presence; TLS-entry and load-balancer-address counts only | Missing backend Service/Service-port evidence and related Service/workload context, not reachability |
| Event, Warning only | `v1` | Namespace | Involved resource identity, reason, count and selected occurrence time | Recent supporting evidence, not proof of an active failure |

### Scope, bounds and freshness

- Nodes, PVs and VolumeAttachments are always observed cluster-wide, including
  unclaimed PVs and attachments related to out-of-scope workloads. Other kinds follow the same namespace watch/ignore
  selection. Explicit watch lists use namespace-scoped requests; all/ignore mode
  uses all-namespace requests without discovering Namespace objects.
- Ignored objects are filtered before projection and cache admission. Ignore
  filtering is **not an RBAC boundary**. Node, PV and VolumeAttachment Warning-event tool reads also
  respect the selected event namespaces.
- All kinds and namespaces share the retained-object budget (`--max-objects`,
  default 5,000), pagination safeguards and collection freshness requirements.
  Missing access to a required kind makes collection incomplete; it is not
  silently treated as an empty, healthy resource list.
- Initial informer synchronization does not prove continuing freshness.
  Broken or expired watches invalidate/rebuild the cache; incomplete
  observations cannot establish recovery.
- Pod label maps, Service selectors and projected port lists are bounded to
  128 entries each. Each EndpointSlice retains at most 1,000 endpoint records,
  **without endpoint addresses**.
- Pod volume lists are bounded to 128 entries; only explicit PVC claim names are
  retained. PVC/PV access-mode lists are bounded to eight entries and PVC
  conditions to 32. Storage sources, CSI volume handles/attributes, mount
  options/paths, server addresses and Secret references are not retained.
- CronJobs retain at most 128 active Job references as a count only, with schedule
  strings bounded to 256 bytes and time zones to 128 bytes. Raw active Job
  identities and Job templates are not retained in the cache.
- HPAs retain at most 32 configured metrics, 32 current metric records and 32
  conditions, plus 16 policies per scaling direction. Metric identifiers and
  target names are bounded to 256 bytes; scale-target kinds/API versions to 128.
  Metric selectors and arbitrary raw configuration maps are excluded.
- Ingresses retain at most 128 rules and 128 total routes including the default
  backend, plus counts for at most 128 TLS entries and load-balancer addresses.
  Host, class and Service names are bounded to 253 bytes, port names to 63 and
  paths to 2,048. Annotations, TLS hosts/Secret names, raw addresses and custom
  backend object identities are not retained.
- VolumeAttachment attacher, Node and PV names are bounded to 253 bytes each.
  Exactly one PV-name or inline source is required; inline sources retain only
  a presence flag. Attachment metadata, raw error messages, inline volume specs,
  Secret references, device paths and volume handles are not retained in the cache.

See [Kubernetes access](operations.md#kubernetes-access),
[namespace selection](operations.md#namespace-scope-and-observer-identity),
[operational limits](operations.md#limits-and-scheduling) and the
[agent RBAC](../deploy/rbac.yaml). The dashboard reads cluster-scoped incident
records separately; it does not gather these workload resources. Incident
storage scope does not change workload namespace selection or the namespace
fields on anchors and affected resources.

### What AI can inspect

The collection snapshot is private host-side data. UIDs and resource versions
support identity/freshness checks, while labels and selectors support local
relationships; these are not passed to the model as raw objects. Investigations
receive bounded related-resource handles and approved evidence rather than the
entire cache.

The [inspection tool](../internal/investigation/inspect_resource.go) can GET a
supplied, authorized handle and return additional approved facts:

| Resources | Examples of additional inspection facts |
|---|---|
| Nodes | Capacity, allocatable resources, taints and redacted condition messages |
| Pods | Node assignment, scheduling constraints, condition messages, regular-container status, and regular/init container specifications limited to names, images, resource requests/limits and ports |
| Deployments, StatefulSets, DaemonSets | Current status counts and conditions; configured replica counts where applicable; update strategy and minimum-ready time where present; approved template container specifications |
| StatefulSets | Current and update revision strings, in addition to replica counts |
| ReplicaSets | Current replica status and conditions, configured replica count |
| Jobs | Failed/succeeded counts, backoff limit, conditions and approved template container specifications |
| CronJobs | Approved schedule/status facts above and regular/init container names, images, resource requests/limits and ports from the nested Job template; no commands, environment values or raw active Job references |
| HPAs | Approved scale-target, replica, metric and behavior facts; bounded redacted conditions/messages; an authorized target handle only when a supported current workload matches |
| Ingresses | Bounded redacted class/host/path and backend Service port descriptions, default/custom-backend flags, TLS/address counts and already-authorized same-namespace Service handles |
| Services, EndpointSlices | Approved port and Service settings or aggregate endpoint-condition counts; no raw selectors or endpoint addresses |
| PVCs, PVs | Phase, storage class, capacity, access/volume modes; requested PVC storage or PV reclaim policy; bounded redacted conditions and PV status reason/message; verified binding handles only when the peer is authorized |
| VolumeAttachments | Attacher, Node/PV names or inline flag; attached/deleting state; optional deletion/error times and error codes; bounded redacted attach/detach messages; supplied current PV/Node handles |

The [related Warning-event tool](../internal/investigation/related_warning_events.go)
reads events for an authorized resource, returning bounded, redacted reasons and
messages with counts/timestamps. It accepts at most 20 events and rejects
incomplete or over-limit reads rather than returning a success-shaped partial
list. The health rule's `--event-lookback` is **not** a time filter on this tool;
AI must interpret the returned timestamps.

Both tools share [scope, call, byte and timeout limits](../internal/investigation/session.go).
An Event itself is not an `inspect_resource` handle. Tools cannot run commands,
fetch arbitrary URLs, read Secrets or Pod logs, or write workloads. Operational
text is bounded and redacted, but arbitrary names/messages can still be sensitive;
redaction is not a guarantee that all sensitive text has been removed.

## Policy defaults

Defaults come from [the health policy](../internal/health/types.go); flags are
defined in [configuration parsing](../internal/config/config.go).

| Flag | Default | Actual use |
|---|---|---|
| `--startup-grace` | `120s` | Pod startup/readiness, Service backend/port, Ingress backend configuration, PVC pending, attachment errors and HPA condition evidence |
| `--rollout-grace` | `300s` | Deployment, StatefulSet and DaemonSet evidence |
| `--event-lookback` | `1h` | Recent Warning-event health evidence |
| `--restart-threshold` | `3` | Informational cumulative container restart evidence |

Grace expires **at** the boundary (`elapsed >= grace`). Where a grace comparison
uses a missing timestamp, it treats the grace as expired. Zero startup/rollout
grace is allowed; event lookback and restart threshold must be positive.
Warning-event age filtering instead excludes missing timestamps and includes
events exactly at the lookback boundary.

These settings control evidence, not guaranteed incident or resolution times.
The rollout-grace flag's current CLI help says "Deployment rollout grace", but
the same policy value is also used for StatefulSets and DaemonSets.

## Check catalogue

The [registry](../internal/health/checks.go) runs seven pure check groups against
the snapshot. Each section below names the exact rule IDs and their conditions.
Exclusions are resource-specific: there is no blanket grace period or universal
deletion exclusion across all checks.

### Nodes

Source: [nodes.go](../internal/health/nodes.go).

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `node.ready` | `Ready` is absent or its status is not `True` | `critical` |
| `node.MemoryPressure` | `MemoryPressure` exists and its status is not `False` | `warning` |
| `node.DiskPressure` | `DiskPressure` exists and its status is not `False` | `warning` |
| `node.PIDPressure` | `PIDPressure` exists and its status is not `False` | `warning` |

There is no grace period. `Unknown` readiness is not healthy readiness;
an absent pressure condition does not trigger a pressure rule, but a present
`Unknown` pressure condition does.

### Pods

Source: [pods.go](../internal/health/pods.go).

Deleting Pods and Pods in `Succeeded` phase are excluded from **all** Pod checks.
For the table, "old enough" means creation age has reached startup grace.

| Rule ID | Trigger | Evidence severity / timing |
|---|---|---|
| `pod.failed` | Phase is `Failed` | `critical`, no grace |
| `pod.unknown` | Phase is `Unknown` | `warning`, no grace |
| `pod.unschedulable` | Old enough; `PodScheduled=False`; that condition's transition has also reached startup grace | `warning` |
| `pod.pending` | Old enough; phase is `Pending`; `PodScheduled` is not `False` (including absent) | `warning` |
| `pod.ready` | Old enough; phase is not `Pending`, `Failed` or `Unknown`; `Ready` is absent/not `True`; readiness transition has also reached startup grace | `warning` |
| `pod.container.waiting` | A regular/init container has one of the waiting reasons below | `warning`, no grace |
| `pod.container.terminated` | A regular/init container is currently terminated with a nonzero exit code | `warning`, no grace |
| `pod.container.restarts` | A regular/init container's cumulative restart count is at least the threshold | `info`, no grace |

Waiting reasons checked: `CrashLoopBackOff`, `ImagePullBackOff`, `ErrImagePull`,
`InvalidImageName`, `CreateContainerConfigError`, `CreateContainerError` and
`RunContainerError`.

The scheduling and readiness rules use both Pod age and condition-transition
age so a newly changed condition is not judged solely by an old Pod's creation
time. A Pending Pod explicitly marked unscheduled does not also receive
`pod.pending`.

Container findings identify the container and its image when available.
Cumulative restarts do not establish recency or a restart rate; previous
termination reason/exit code is supporting context, not a separate current
failure. Multiple Pod/container findings can describe one underlying problem.

### Deployments

Source: [deployments.go](../internal/health/deployments.go).

Deployments with desired replicas equal to zero are skipped. Omitted
`spec.replicas` projects as one.

Grace-based severity is `info` before rollout grace and `warning` afterward.
The grace clock uses the latest of creation time, the `Progressing` condition's
transition and update times, and the `Available` condition's transition time
when `Available=False`.

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `deployment.unobserved` | Observed generation is less than desired generation | Grace-based; skips the remaining Deployment checks for this object |
| `deployment.progress_deadline` | Not paused; `Progressing=False` with reason `ProgressDeadlineExceeded` | `critical`, independent of local grace |
| `deployment.replica_failure` | `ReplicaFailure=True` | `critical`, independent of local grace |
| `deployment.replicas` | Available replicas are below desired, or the Deployment is not paused and updated replicas are below desired | Grace-based |

Pausing suppresses the progress-deadline rule and the updated-replica comparison,
not observation lag, replica failure or insufficient available replicas.
There is no separate deletion exclusion in the Deployment check.

### StatefulSets and DaemonSets

Source: [controllers.go](../internal/health/controllers.go).

Both kinds are already collected and checked. Deleting controllers are skipped.
All findings in this section are `info` before rollout grace and `warning` at
or after it, measured from **controller creation time**.

| Rule ID | Trigger |
|---|---|
| `statefulset.unobserved` | Desired replicas are positive and observed generation is below desired generation |
| `statefulset.replicas` | Desired replicas are positive and ready/available replicas are below desired, or updated replicas are below the strategy's update target |
| `daemonset.unobserved` | Desired scheduled count is positive and observed generation is below desired generation |
| `daemonset.replicas` | Desired scheduled count is positive and ready/available/current scheduled counts are below desired, or updated scheduled count is below the strategy's update target |
| `daemonset.misscheduled` | Misscheduled count is positive |

For each object, an unobserved-generation finding skips all remaining controller
checks, including DaemonSet misscheduling. With zero desired count, observation
lag and replica/update shortfalls are not reported; a DaemonSet can still
produce misscheduling evidence.

**Strategy and count differences:**

- StatefulSet desired replicas come from `spec.replicas` (one if omitted).
  DaemonSet desired count comes from `status.desiredNumberScheduled`, not the
  cluster's total Node count.
- For `OnDelete`, neither kind requires updated replicas to reach an update
  target. Readiness/availability checks still apply.
- For other strategies, StatefulSet's update target is
  `max(0, desired - partition)`; a missing partition is zero. DaemonSet's update
  target is the desired scheduled count.
- StatefulSet `currentReplicas` is retained context, not a separate shortfall
  test. DaemonSet `currentNumberScheduled` does participate in its shortfall
  test. Misscheduling is separate from a count shortfall.
- The StatefulSet check compares counts, not per-Pod ordinals or revision
  strings. Custom start ordinals are not projected into this calculation;
  revision strings are available through inspection rather than a revision
  mismatch rule.

**Grace is not a stall detector.** Unlike Deployments, these checks do not use
progress-condition timestamps or a Kubernetes progress-deadline failure.
A later update to an old controller does not reset its creation-based grace.
A shortfall may therefore be warning evidence immediately during a legitimate
later rollout; it does not, by itself, prove that rollout has stalled.

For example:

- An observed StatefulSet with desired/ready/available counts of three, partition
  two and one updated replica meets the current update target. Old replicas are
  intentional in that count-based check.
- An observed `OnDelete` controller can have zero updated replicas without update
  shortfall evidence if readiness/availability (and DaemonSet placement) counts
  are satisfied.
- `OnDelete` and partitions do not excuse too few ready or available replicas.
  A DaemonSet reporting Pods on nodes that should not run them can also produce
  `daemonset.misscheduled`, even when its desired placement counts are met.

### Services and EndpointSlices

Source: [services.go](../internal/health/services.go).

Deleting Services, `ExternalName` Services and Services with no selector are
excluded. Eligible Services have `info` evidence during startup grace and
`warning` evidence at/after it, based on Service creation time.

| Rule ID | Trigger |
|---|---|
| `service.backends` | No ready endpoints across matching EndpointSlices |
| `service.ports` | Ready endpoints exist, but a Service port has no matching name/protocol in any matching slice containing ready endpoints |

When there are no ready endpoints, only the backend rule runs; there is no
additional missing-port finding. Port matching defaults an omitted protocol to
TCP. It does not verify numeric target-port correctness or that the application
actually listens on the port.

Relationship and readiness semantics:

- Slices must share the Service namespace and carry its name in the
  `kubernetes.io/service-name` label. Deleting slices are excluded. If a projected
  controller owner is present, it must identify the current Service incarnation.
- Non-deleting Pods in the same namespace whose labels match every Service
  selector entry provide counts and related-resource context. Selector matches
  are **not ownership** and are not a substitute for ready endpoints.
- An omitted endpoint `ready` or `serving` condition counts as true; omitted
  `terminating` counts as false. Counts are independent: a ready, terminating
  endpoint still contributes to the ready count.
- `serving`, `terminating` and `publishNotReadyAddresses` provide AI context;
  they do not independently trigger rules or override the ready-count test.

EndpointSlices have no standalone health rule. Ready endpoints are Kubernetes
configuration/readiness evidence, not a DNS, network or application traffic
probe. Neither rule establishes that a Service is reachable or unreachable.

### Ingresses

Sources: [Ingress projection](../internal/kube/ingress.go),
[pure checks](../internal/health/ingresses.go) and
[inspection](../internal/investigation/inspect_ingress.go).

Deleting Ingresses are skipped. Both rules produce `info` before startup grace
and `warning` at/after it, measured from **Ingress creation time**:

| Rule ID | Trigger |
|---|---|
| `ingress.service_missing` | A Service backend is absent from the complete same-namespace snapshot |
| `ingress.port_missing` | That Service exists, but its `spec.ports` lacks the named or numeric port referenced by the Ingress |

Default backends and HTTP paths use the same rules. Missing-Service evidence
is deduplicated per Service name; missing-port evidence per Service/port. There
is no additional missing-port finding for an absent Service. Port numbers match
the Service's `port`, **not** `targetPort`. Named ports match the Service port name.
Selectorless and ExternalName Services can be valid backends; their presence and
port definitions are checked without assuming managed endpoints.

These are configuration findings, not proof that requests fail. Ingress has no
portable Ready/Accepted condition. Empty load-balancer status, omitted class,
route overlaps and TLS configuration do not independently trigger findings.
No IngressClass, Secret, certificate, DNS or HTTP endpoint is read or probed;
TLS validity, DNS, reachability and application responses remain unknown.
Legacy class annotations are not inspected or treated as missing configuration.
Custom resource backends retain only a presence flag and are not checked or read.

Already-collected backend Services, their EndpointSlices, selected/target Pods
and verified controllers provide bounded context. Services can include referring
Ingresses in their own investigation. These relationships are not ownership and
do not change either anchor. Existing Service readiness checks are not duplicated
as Ingress rules. Controller Warning Events can support an Ingress investigation,
but their presence, reason and age need AI interpretation.

AI may use the existing `service-availability` family or choose no-action.
Collection errors never become an empty Service inventory: incomplete reads
skip AI and preserve unresolved incidents. Backend repair requires fresh
continuous observations and an AI decision before an incident can resolve.

### PersistentVolumeClaims and PersistentVolumes

Source: [volumes.go](../internal/health/volumes.go).

Deleting PVCs/PVs are skipped. These rules produce private hints, not storage
probes, data-loss conclusions or automatic incidents.

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `pvc.pending` | PVC phase is `Pending` | `info` before startup grace; `warning` at/after it, from claim creation time |
| `pvc.lost` | PVC phase is `Lost` | `critical`, no grace |
| `pvc.resize_error` | `ControllerResizeError=True` or `NodeResizeError=True` | `warning`, one finding per condition type |
| `pv.failed` | PV phase is `Failed` | `critical`, no grace |

A Pending claim can intentionally wait for its first consumer. The agent does
not read StorageClasses and cannot establish `WaitForFirstConsumer` mode from
these facts. Normal `Resizing` or `FileSystemResizePending` conditions, and
Available/Released PVs, do not independently trigger findings. A resize error
does not prove existing capacity is unusable; a failed PV does not establish
application impact or data loss. Missing reported capacity remains absent,
not a fabricated zero.

PVs and PVCs keep their own stable anchors. Related binding context requires
the PVC's `spec.volumeName` to match the PV name and the PV's `claimRef` to match
the current PVC namespace, name and UID. Stale, UID-less or one-sided references
do not establish a binding. Only already-collected scoped claims and explicit
same-namespace Pod consumers are included. Pod/controller ownership is unchanged;
storage relationships are context, not ownership or automatic coalescing.
Generic ephemeral volumes are not linked through their generated claim names.

Tools never expose raw claim references or volume source configuration.
Validated AI may use the `storage-availability` family, or return no-action.

### VolumeAttachments

Sources: [projection](../internal/kube/project.go),
[pure checks](../internal/health/attachments.go) and
[inspection](../internal/investigation/inspect_attachment.go).

VolumeAttachments are controller-managed requests to attach storage to a Node,
not evidence that kubelet mounted the filesystem or an application can use it.
Not every CSI driver needs attachments; an empty attachment list is valid.

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `volumeattachment.attach_error` | Not deleting; `attached=false`; an attach error is present | `info` before startup grace, `warning` at/after it, from attachment creation |
| `volumeattachment.detach_error` | Deleting; `attached=true`; a detach error is present | `info` before startup grace, `warning` at/after it, from deletion time |

A missing grace timestamp counts as expired. Retry error timestamps do not reset
grace. An old attachment encountering a new attach error can therefore produce
warning evidence immediately; these are error hints, not a stall detector.
Normal pending requests, long elapsed time alone and normal deletion have no
standalone rule. Attach errors after `attached=true`, detach errors outside
deletion and detach errors after `attached=false` are retained inspection context
but do not independently trigger these rules.

Related PVs and Nodes must already be collected and match the attachment's names.
These references contain no target UID: they identify current named objects,
not UID-verified ownership or proof that an old attachment targets a replacement
incarnation. Verified PV/PVC binding can add only collected namespace-scoped
claims and their explicit consumers/controllers. Attachment, PV and workload
anchors remain separate, using the existing `storage-availability` family.

Inspection excludes `attachmentMetadata`, raw inline specifications and backend
configuration. Operational error messages are bounded/redacted, but arbitrary
driver text is not guaranteed secret-free. Optional numeric error codes and
timestamps remain absent when not reported; an explicit code zero stays zero.
Error-time churn alone does not invalidate semantic fingerprints, while attached
state, deletion, target identity, error presence and codes do.

No CSI or storage API writes occur. Reported attachment success or disappearance
permits recovery review, not automatic incident resolution or a claim that storage
is mounted, data is intact or an application recovered. AI may choose no-action.

### Jobs

Source: [jobs.go](../internal/health/jobs.go).

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `job.failed` | `Failed=True` or `FailureTarget=True` | `critical`, no grace |

Only one finding is emitted per Job, with `Failed` checked before `FailureTarget`.
Failure counts alone do not trigger this rule. There is no separate deletion
exclusion. Owned Pods can independently supply Pod evidence for the same Job.

### CronJobs

Sources: [CronJob projection](../internal/kube/project.go),
[context building](../internal/incident/evidence.go) and
[inspection](../internal/investigation/inspect_resource.go).

CronJobs follow the same namespace filters, shared object budget and continuing
freshness checks as Jobs. Missing `suspend` means false; omitted concurrency policy
means `Allow`; omitted successful/failed history limits mean three/one. Omitted
starting deadline and last-schedule/last-success timestamps remain absent, while
an explicit zero deadline remains zero. An omitted time zone is not assumed UTC.

There is no standalone missed-schedule or suspension rule. Scheduling semantics,
time zones, concurrency policies and retained history need interpretation, not
an elapsed-time heuristic. Existing `job.failed` and Pod rules provide private
evidence grouped under the verified CronJob through `Pod -> Job -> CronJob`.
Recent Warning Events involving the CronJob can also trigger an investigation.
AI can inspect scheduling facts and related Jobs, use `job-failure` for a meaningful
scheduled execution problem, or choose no-action. A historical failed Job may
remain after later successes and does not by itself prove a current failure.

New scheduled Job/Pod incarnations retain the CronJob anchor. Unresolved records
already anchored to a Job retain that Job's identity and active evidence; enabling
CronJob support must not silently move those signals into a new group and mistake
their disappearance for recovery. The verified CronJob remains inspection context
for those legacy Job investigations. No existing incident records are migrated.

### HorizontalPodAutoscalers

Sources: [HPA projection](../internal/kube/hpa.go),
[pure checks](../internal/health/hpa.go) and
[inspection](../internal/investigation/inspect_hpa.go).

Deleting HPAs are skipped. Both rules use `info` before startup grace and
`warning` at/after it, measured from **HPA creation time**:

| Rule ID | Trigger |
|---|---|
| `hpa.unable_to_scale` | `AbleToScale=False` |
| `hpa.inactive` | `ScalingActive=False`, except reason `ScalingDisabled` |

`ScalingLimited=True`, replica-count differences, min/max limits, stabilization,
rate limits and disabled scaling are context, not independent failures.
Missing/Unknown conditions do not trigger these rules and do not establish
health. The conditions report autoscaling capability, not application reachability
or impact; validated AI may use `autoscaling`, or choose no-action.

Omitted `minReplicas` projects as one; an explicit zero remains zero. Optional
observed generation, last-scale time and scaling behavior remain absent when
omitted. Resource, ContainerResource, Pods, Object and External metrics retain
approved names and numeric target/current values. Object-metric references are
descriptive only; metric label selectors are never retained or passed to AI.
Configured stabilization windows, selection policy, numeric rate policies and
tolerance are available when present. Tools do not query any metrics API or
scale subresource.

Scale-target context matches already-collected Deployments, StatefulSets or
ReplicaSets using exact `apps/v1`, kind, name and the HPA namespace. Other API
versions/kinds are not guessed or fetched; their bounded target description can
still be inspected. Relationships use the current observed target incarnation,
not a fabricated owner UID. HPA and workload anchors remain unchanged, with
bounded target/controller/Pod context available for investigation.

Metric samples, last-scale timestamps and condition timestamp churn alone do
not change semantic investigation fingerprints. Configuration, replica state,
condition status/reason and related-resource identity changes do. Tool-inspected
resource versions must still match before any AI decision is applied.

### Warning Events

Sources: [events.go](../internal/health/events.go) and
[Event projection](../internal/kube/project.go).

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `event.warning` | A projected Warning Event has a nonzero occurrence time, is not in the future and is no older than event lookback | `info` |

Collection requests only core `v1` Warning Events, not Normal Events.
Projection selects the latest of `lastTimestamp`, `eventTime`, `firstTimestamp`
and, when present, `series.lastObservedTime`. Count uses `series.count` when a
series exists, otherwise the Event's count.

The finding points to the involved resource, with the Event name, reason and
count as supporting evidence. It is not a separate Event incident. Context
building requires the involved resource to match a collected object; an Event
for an unknown/uncollected target does not create an investigation on its own.
Recent events may describe past failures, and a larger count does not prove a
new or ongoing failure.

## How related checks fit together

[Context building](../internal/incident/evidence.go) follows controller ownership:
Pod -> ReplicaSet -> Deployment, Pod -> Job -> CronJob, or Pod -> StatefulSet/DaemonSet/Job. ReplicaSet
identity is therefore useful even without a ReplicaSet health check. Controller
and replica findings are brought together rather than automatically becoming
one incident per Pod or rule.

For example, a StatefulSet availability shortfall and its Pod's
`CrashLoopBackOff` can contribute to the same controller investigation.
Replacing that Pod does not by itself replace the controller anchor or prove
recovery. StatefulSet/DaemonSet checks complement, rather than replace, the
existing Pod checks.

Services anchor their own backend evidence across EndpointSlice replacement and
can include related Pods/controllers as context. Storage resources retain their
standalone identities while sharing verified binding and scoped consumer context.
HPAs and supported targets share bounded context without merging incident anchors.
VolumeAttachments share current named PV/Node and scoped claim/consumer context
without changing attachment or workload ownership.
Ingresses and backend Services likewise share route/workload context without
duplicating endpoint-readiness evidence or treating it as a traffic probe.
AI still decides which
meaningful problem families, if any, justify incidents. Healthy-looking counts,
the absence of a Warning Event or a temporarily running container do not
automatically resolve an existing incident.

## Keeping this reference current

Update this document when resource registrations, approved projections,
inspection fields, health rules or policy defaults change. Keep exact rule IDs,
thresholds and exclusions aligned with the linked source files; keep operational
setup in the [operations guide](operations.md) and orchestration/lifecycle design
in the [architecture guide](../ARCHITECTURE.md).
