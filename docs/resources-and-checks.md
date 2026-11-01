# Kubernetes resources and health checks

[Overview](../README.md) · [Architecture](../ARCHITECTURE.md) · [Operations](operations.md)

This is the reference for the Kubernetes resources KubeScout gathers, the facts
it retains, and the health checks it runs on those facts. It describes the
current implementation, including existing StatefulSet and DaemonSet support.

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

The [collector](../internal/kube/source.go) gathers the following ten resource
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
| Pod | `v1` | Namespace | Phase; conditions; regular/init container names, images, waiting reasons, current/previous termination reasons and exit codes, cumulative restarts; private labels | Scheduling, readiness and container evidence; controller and Service relationships |
| Deployment | `apps/v1` | Namespace | Desired, available and updated replicas; generation/observed generation; paused state; conditions and their transition/update times | Rollout progress and replica failures |
| ReplicaSet | `apps/v1` | Namespace | Identity and controller ownership only | Connects replica Pods to Deployments; **no standalone health check** |
| StatefulSet | `apps/v1` | Namespace | Desired, ready, available, current and updated replicas; generation/observed generation; update strategy and rolling-update partition | Replica/update shortfalls and direct Pod ownership |
| DaemonSet | `apps/v1` | Namespace | Desired/current/updated scheduled counts; ready, available and misscheduled counts; generation/observed generation; update strategy | Placement, availability and update shortfalls; direct Pod ownership |
| Service | `v1` | Namespace | Type; private selector; port names, numbers, protocols and targets; `publishNotReadyAddresses` | Backend availability and port-definition evidence |
| EndpointSlice | `discovery.k8s.io/v1` | Namespace | Service-name linkage, address type, ports, endpoint ready/serving/terminating conditions and same-namespace Pod target references | Supports Service checks and related-resource context; **no standalone health check** |
| Job | `batch/v1` | Namespace | Condition types, statuses, reasons and transition times | Terminal Job failures and Pod ownership |
| Event, Warning only | `v1` | Namespace | Involved resource identity, reason, count and selected occurrence time | Recent supporting evidence, not proof of an active failure |

### Scope, bounds and freshness

- Nodes are always observed. Other kinds follow the same namespace watch/ignore
  selection. Explicit watch lists use namespace-scoped requests; all/ignore mode
  uses all-namespace requests without discovering Namespace objects.
- Ignored objects are filtered before projection and cache admission. Ignore
  filtering is **not an RBAC boundary**. Node Warning-event tool reads also
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
| Services, EndpointSlices | Approved port and Service settings or aggregate endpoint-condition counts; no raw selectors or endpoint addresses |

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
| `--startup-grace` | `120s` | Pod startup/readiness checks and Service backend/port evidence |
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

### Jobs

Source: [jobs.go](../internal/health/jobs.go).

| Rule ID | Trigger | Evidence severity |
|---|---|---|
| `job.failed` | `Failed=True` or `FailureTarget=True` | `critical`, no grace |

Only one finding is emitted per Job, with `Failed` checked before `FailureTarget`.
Failure counts alone do not trigger this rule. There is no separate deletion
exclusion. Owned Pods can independently supply Pod evidence for the same Job.

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
Pod -> ReplicaSet -> Deployment, or Pod -> StatefulSet/DaemonSet/Job. ReplicaSet
identity is therefore useful even without a ReplicaSet health check. Controller
and replica findings are brought together rather than automatically becoming
one incident per Pod or rule.

For example, a StatefulSet availability shortfall and its Pod's
`CrashLoopBackOff` can contribute to the same controller investigation.
Replacing that Pod does not by itself replace the controller anchor or prove
recovery. StatefulSet/DaemonSet checks complement, rather than replace, the
existing Pod checks.

Services anchor their own backend evidence across EndpointSlice replacement and
can include related Pods/controllers as context. AI still decides which
meaningful problem families, if any, justify incidents. Healthy-looking counts,
the absence of a Warning Event or a temporarily running container do not
automatically resolve an existing incident.

## Keeping this reference current

Update this document when resource registrations, approved projections,
inspection fields, health rules or policy defaults change. Keep exact rule IDs,
thresholds and exclusions aligned with the linked source files; keep operational
setup in the [operations guide](operations.md) and orchestration/lifecycle design
in the [architecture guide](../ARCHITECTURE.md).
