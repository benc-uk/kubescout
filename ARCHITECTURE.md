# KubeScout architecture

[Overview](README.md) · [Operations](docs/operations.md) · [Development](docs/development.md)

KubeScout separates observation, AI investigation and incident persistence.
This guide follows the default **watch-mode** flow, identifies the code handling
each step, and explains the boundaries between ordinary Go logic, AI decisions
and human triage. For commands, configuration and deployment, use the
[operations guide](docs/operations.md).

```text
Kubernetes change
    -> local cache
    -> deterministic evidence
    -> scheduled AI investigation
    -> validated decision
    -> persisted Kubernetes Incident CRD
    -> dashboard
```

**Deterministic code decides what to investigate and whether a decision is safe
to apply. AI decides whether there is an incident and what it means.**

Here, deterministic means ordinary Go logic, not a model call. There is no
deterministic-only incident publisher.
**Kubernetes Incident CRDs are the incident record.** Stdout is readable agent
activity, not an incident capture API. AI transcript files are diagnostic
exchanges, not incident storage; the dashboard reads CRDs only.

## 1. Getting Kubernetes changes into the agent

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **1. Start the process** | Loads configuration, authenticates to Kubernetes and wires together the collector, analyzer, incident store and runner. | [`runWithClient`](cmd/kubescout/main.go) | Which namespaces to observe, which model to use, watch versus once and where to persist incidents. | **Deterministic** |
| **2. Recover existing incidents** | Loads this observer's open Incidents so restarting does not forget unresolved problems. | [`Runner.loadOpen`](internal/runner/runner.go), [`Kubernetes.ListOpen`](internal/incident/store.go) | Rejects stored incidents outside the configured observation scope. | **Deterministic** |
| **3. Subscribe to Kubernetes** | Creates client-go informers. Each performs an initial paginated **LIST**, then a **WATCH** for added, modified and deleted objects. | [`resources` and `Source.runEpoch`](internal/kube/source.go) | Watches supported resource types within the configured scope. Only Warning-type Kubernetes Events are collected. | **Deterministic** |
| **4. Filter and simplify objects** | Converts API objects into a limited internal representation before storing them in the cache. | [`project`](internal/kube/project.go), [`Source.project` and `Source.watch`](internal/kube/source.go) | Rejects invalid identities, filters ignored namespaces and enforces object limits. Retains approved facts, not entire raw objects. | **Deterministic** |
| **5. Wake the runner** | Informer add/update/delete callbacks send a notification. The runner reads the **current cached snapshot**, rather than passing each raw change directly to AI. A timer also wakes it. | [`Source.notify` and informer callbacks](internal/kube/source.go), [`Runner.Watch`](internal/runner/runner.go) | Coalesces notifications. Refuses incomplete collection; a broken watch triggers cache rebuilding, not a healthy result. | **Deterministic** |

The watched resources are Nodes, Pods, Deployments, ReplicaSets, StatefulSets,
DaemonSets, Services, EndpointSlices, PVCs, PVs, VolumeAttachments, Jobs, CronJobs,
HPAs, Ingresses and Warning Events. An explicit
namespace watch list uses namespace-scoped requests. All/ignore mode uses
cluster-wide requests and filters ignored namespaces locally. Nodes, PVs and
VolumeAttachments are always observed cluster-wide; PVCs follow namespace filters.

A Kubernetes **watch notification** is not the same as a Kubernetes **Event
object**. A Pod status update, for example, arrives through the Pod watch; it does
not need a Warning Event to trigger processing.

**A Kubernetes change does not automatically produce a model call.** It first
causes the next steps to re-evaluate the current state.

## 2. Deciding what needs investigation

The [health package](internal/health/) keeps the check registry and `Evaluate`
loop in [checks.go](internal/health/checks.go). Implementations are grouped
consistently by resource category:

| File | Evidence checks |
|---|---|
| [nodes.go](internal/health/nodes.go) | Node readiness and pressure conditions. |
| [pods.go](internal/health/pods.go) | Pod scheduling, readiness and container failures. |
| [deployments.go](internal/health/deployments.go) | Deployment rollout progress. |
| [controllers.go](internal/health/controllers.go) | StatefulSet and DaemonSet replica/rollout state. |
| [services.go](internal/health/services.go) | Service backend availability and EndpointSlice port matching. |
| [volumes.go](internal/health/volumes.go) | PVC pending/lost binding and resize errors; explicit PV failure. |
| [attachments.go](internal/health/attachments.go) | Reported attach errors while unattached and detach errors while deleting/still attached. |
| [hpa.go](internal/health/hpa.go) | HPA unable-to-scale and inactive conditions, excluding intentional disabled scaling. |
| [ingresses.go](internal/health/ingresses.go) | Missing Ingress backend Services or Service port definitions. |
| [jobs.go](internal/health/jobs.go) | Terminal Job failure conditions. |
| [events.go](internal/health/events.go) | Recent Warning Events as supporting evidence. |

See [Resources and health checks](docs/resources-and-checks.md) for the complete
collection inventory, retained facts, rule IDs, thresholds and exclusions.

### Resource semantics

ReplicaSets establish ownership rather than a separate incident category.
CronJobs establish stable ownership for scheduled Jobs and their Pods. Approved
schedule/time-zone, suspension, deadline, concurrency, history and last-run facts
are inspection context; no standalone missed-schedule heuristic runs. A retained
failed Job can describe history after a later success, so AI decides current
impact and may choose no-action. Unresolved legacy Job anchors remain in place
with their own failure evidence and verified CronJob inspection context.
StatefulSet and DaemonSet checks consider observed generations, replica readiness
and update progress, respecting OnDelete and StatefulSet rolling-update
partitions. New-controller shortfalls use rollout grace; these APIs have no
Deployment-style progress deadline, so a shortfall alone does not prove a stall.

Selector-managed Services produce private evidence for absent ready EndpointSlice
backends, informational during startup grace and warning afterward. Ready backends
without a port matching a Service port's name/protocol also produce evidence.
ExternalName and selectorless Services are exempt. EndpointSlice service-name
labels establish same-namespace routing context; controller owner UIDs, when
present, must match the current Service.

Omitted EndpointSlice `ready`/`serving` conditions count as true and omitted
`terminating` as false. Serving/terminating conditions and
`publishNotReadyAddresses` remain context for AI interpretation, not proof of
DNS, connectivity, application-port correctness or successful traffic.
Selector matches provide related Pod/controller handles, not ownership.

Selectors and Pod labels stay private. Selector/label projections and port lists
are bounded to 128 entries each; EndpointSlices retain at most 1,000
readiness/target records and no endpoint addresses. All resource types share
the applicable cluster/namespace scope, pagination, admission, freshness and investigation limits.
Missing access means incomplete collection, not healthy resources.

VolumeAttachments retain attacher/target names, attached/deleting state and error
metadata, not attachment metadata, inline volume specs or raw driver messages.
Inspection adds bounded credential-protected errors and supplied current named PV/Node handles.
Named links are not UID-based ownership; scoped mutual PV/PVC bindings add
consumer context while preserving standalone attachment/PV and workload anchors.
Attach errors use creation-based startup grace; detach errors use deletion-based
startup grace. Stale operation errors, normal pending and deletion alone are not
rules. Error timestamp churn is excluded from fingerprints. Successful attachment
does not prove mounting, usable storage or application recovery.

PVC Pending is grace-aware evidence, not proof of failed provisioning: delayed
binding can be intentional, and StorageClasses are not read. Lost claims, explicit
resize-error conditions and Failed PVs provide stronger private hints. Normal
resizing and Available/Released PVs do not independently imply incidents.
Storage projections exclude CSI/backend configuration, mount paths and credentials.
Explicit Pod claim names, mutual PVC/PV binding names and PVC UIDs stay private
for relationships; only authorized peer handles reach AI inspection.
PVs/PVCs retain standalone anchors, and storage relationships never replace
Pod/controller ownership. Binding, capacity and storage-condition state changes
invalidate evidence revisions; condition timestamp churn alone does not.

### Correlation and scheduling

HPAs use `autoscaling/v2` and share namespace scope, admission and freshness
limits. Approved condition failures use startup grace, while normal scaling
limits, stabilization, replica differences and `ScalingDisabled` are not
independent failures. Scale-target references provide same-namespace current
Deployment/StatefulSet/ReplicaSet context, never synthetic ownership.
Metric selectors are excluded; approved metric/behavior facts come from the HPA
GET only, without metrics API or scale-subresource access. Current metric samples,
last-scale times and condition timestamp churn do not independently invalidate
no-action fingerprints; inspected resource versions still gate stale decisions.

Ingresses use `networking.k8s.io/v1` with the same scope, freshness and shared
limits. Complete collection supports grace-aware missing backend Service/port
evidence; denied or stale reads never imply a missing backend. Routes retain
bounded class/host/path and Service port facts, custom-backend presence and
TLS/address counts, not annotations, Secret names or raw addresses.
Related Services, EndpointSlices, Pods and verified controllers provide context
without changing anchors or duplicating Service readiness rules. Controller
Warning Events are supporting evidence. Missing class, empty load-balancer
status and TLS configuration are not health rules; no class/Secret reads or
DNS/HTTP probes are performed. AI can use `service-availability` or no-action.

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **6. Evaluate evidence rules** | Runs checks for nodes, Pods, controllers, Services, Ingresses, storage, autoscaling, Jobs and Warning events. Produces internal findings. | [`Evaluate`](internal/health/checks.go), for example [`Services`](internal/health/services.go) | Detects conditions such as missing route backends, no ready backends, a failed Pod or a stalled rollout. Assigns informational/warning/critical **hints**, not final incident severity. | **Deterministic** |
| **7. Group related evidence** | Groups findings under a controller or standalone resource, called the **anchor**. Adds related resources and existing open incidents, even when their original findings have disappeared. | [`BuildInvestigationContexts` and `anchor`](internal/incident/evidence.go) | Which observations belong together. For example, related replica failures belong under their controller rather than becoming separate candidates. | **Deterministic** |
| **8. Schedule an investigation** | Compares evidence fingerprints and chooses a due candidate. Only one investigation runs at a time; collection continues meanwhile. | [`updateCandidates` and `Runner.Watch`](internal/runner/runner.go) | Batches changes, applies cooldowns, accelerates reviews when urgency increases, revisits unchanged cases and delays failed investigations. No findings and no existing incident generally means no candidate. | **Deterministic** |

A **candidate** tracks scheduling and recovery for a group of evidence, not a
confirmed incident. Its `investigation` field holds an
[`incident.InvestigationContext`](internal/incident/types.go): the anchor,
related resource handles, signals and private host-side observation data.
The runner copies this context and fetches current incident details into its
`Incidents` field immediately before calling AI.

The candidate's other fields track the last review, next due time, failures and
continuous recovery. Its evidence fingerprint detects relevant observation
changes; it is not the incident's persistent identity.

## 3. Where AI actually enters

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **9. Start the model investigation** | Sends grouped signals, approved resource handles and known incident conclusions to the Responses agent with the embedded instructions. | [`Analyzer.Investigate`](internal/analysis/analysis.go), [instructions.txt](internal/analysis/instructions.txt), [`InvestigationContext`](internal/incident/types.go) | Code controls the model, input size, deadline and request quota. Private identity mappings and cached objects are not serialized into the initial prompt. | **Deterministic setup; AI reasoning begins** |
| **10. Gather more evidence if needed** | The model can request `inspect_resource` or `related_warning_events`. Local Go tools make scoped Kubernetes reads and return bounded, redacted results. This may repeat. | [`InspectResource.Call`](internal/investigation/inspect_resource.go), [`RelatedWarningEvents.Call`](internal/investigation/related_warning_events.go), [shared session](internal/investigation/session.go) | **AI chooses** which supplied resource to inspect. **Code enforces** allowed handles, namespace scope, identity, tool limits and returned fields. No shell or workload writes. | **Mixed** |
| **11. Propose a conclusion** | Model returns structured JSON: `no_action`, or actions to `raise`, `update` or `resolve`. Updating a resolved incident can reopen it. | [Decision instructions](internal/analysis/instructions.txt), [`Action` and `Decision`](internal/incident/types.go) | Whether a meaningful problem exists; its family, severity, impact, diagnosis, explanation and recommendations; whether recovery warrants resolution. | **AI** |

**Even a deterministic critical hint may legitimately result in AI `no_action`.**
No-action leaves any existing incident unchanged; it does not mean resolved.

The model does not receive arbitrary access to the cluster. A resource handle
refers to an already-authorized resource in this investigation. The host keeps
the mapping to Kubernetes identity private and validates tool requests against it.

### Tools and model data

The [Responses agent](internal/analysis/analysis.go) has two typed local tools:

- [`inspect_resource`](internal/investigation/inspect_resource.go) returns approved
  state, scheduling/capacity facts, container state and bounded credential-protected condition
  messages. It includes controller template images/resources/container ports,
  Service port definitions, EndpointSlice readiness counts and approved PVC/PV
  facts with authorized binding handles, not raw templates, volume sources,
  claim references, selectors, endpoint addresses or target UIDs. CronJob scheduling
  facts and nested Job-template container specifications are approved; raw active
  Job references, commands and environment values are excluded. HPA inspection
  returns bounded metric/scaling facts, credential-protected conditions and an optional
  already-authorized supported workload handle, never metric selectors or arbitrary reads.
- [`related_warning_events`](internal/investigation/related_warning_events.go)
  returns bounded credential-protected Warning events for an authorized resource. Node/PV-event
  reads use the same namespace watch/ignore scope. Reads are limited to 20 pages
  and 20 retained events per call; incomplete/oversized results fail explicitly.

Both use the [shared tool session](internal/investigation/session.go) for handle
authorization, normalized arguments and shared call/byte budgets. Tools do not
accept arbitrary namespaces, selectors, URLs or API paths. Unknown handles/tools
and malformed arguments fail before API access.

### Native incident descriptions

Before the first model request, [describe capture](internal/investigation/describe.go)
selects the anchor and up to three related resources, prioritizing symptom-bearing
handles. The [pinned kubectl library](go.mod) describes each selected
resource through [a guarded REST transport](internal/investigation/describe_transport.go).
Native describe is the sole resource-detail payload in AI's initial evidence and
the Incident record. State and Warning events are already in that text; separate
typed state/event copies are neither collected nor stored. The two typed tools
remain available to AI on demand, without appending duplicate stored payloads.
The explicit Kubernetes connection supplies the same private authentication.
No executable, shell tool, new workload permission or dashboard workload access
is introduced.

The transport allows only GETs for approved APIs and supplied resource identities.
Related lists honor namespace watch/ignore selection, enforce complete bounded
pagination and return only authorized resources; events remain Warning-only and
incarnation-scoped. Additional requests such as Node Leases are denied locally.
All library reads share a 16-HTTP-attempt and 1 MiB raw-response budget, with a
128 KiB per-response cap. Context-aware transport reads and per-resource timeouts
bound kubectl implementations that use `context.TODO()`. Inspected versions join
the same stale-result checks as the existing tools.

[Describe capture](internal/investigation/describe.go) escapes control characters
before rendering and retains native operational sections without a heading allowlist.
Labels, addresses, selectors, commands, probes, storage details and non-UID
identifiers remain available as emitted by kubectl. Credential values are redacted;
annotations, environment and UIDs remain excluded. Diagnostic words such as
`secret`, `token`, `password` and `authorization` no longer remove whole messages.
Failed event/related reads remain explicit even where kubectl would otherwise ignore
the error or print `<none>`. Unsupported kinds, including VolumeAttachment in the
pinned library, produce unavailable output rather than a typed or raw-object fallback.

The `kubectl-describe/v1` envelope contains one entry per selected resource:
`resource`, `source`, `evidence_id`, `observed_at` and `output` (`status`/`text`).
These entries are stored in `spec.evidence.descriptions`, defined and validated
by [the description contract](internal/incident/descriptions.go).
There is no nested supplemental `describe`, typed `state` or `warning_events`.
The exact credential-protected text reaches AI before its decision and is
persisted with the same material Incident revision. Native text is capped at
8 KiB per resource and the entire serialized envelope at 16 KiB, shrinking further
to the remaining initial-context budget. Resource identity, citation and coverage
metadata are retained; insufficient metadata space fails explicitly.
Capture counts toward the shared 32 KiB evidence allowance, not AI tool
invocation slots, and introduces no extra model round or duplicate typed reads.
Versions and negative reads participate in stale-result rejection, including
changes between describe and later tools. Missing/unavailable evidence never
establishes recovery. The previous evidence format is not supported or migrated.

Raw API objects, UIDs, annotations, credentials and environment variables are not
model input. Tools cannot read Secrets, ConfigMaps, Pod logs or arbitrary custom
resources, execute commands, modify workloads or deliver notifications.
Approved names, image references, scheduling facts and messages can still reveal
sensitive operational details. Credential filtering is not a general sanitization
policy or a guarantee that arbitrary text is credential-free. Tool results are
untrusted evidence, not instructions.

The tool loop has independent request, tool-call, byte and deadline limits,
including a transport ceiling counting actual HTTP attempts. Automatic HTTP
retries, provider storage, conversations, hosted tools and background continuations
are disabled. History exists only within an investigation, not as durable model
memory. See [limits](docs/operations.md#limits-and-scheduling) for values and flags.

## 4. Turning the conclusion into a stored Incident

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **12. Validate the response** | Parses strict JSON and checks the proposed actions. | [`analysis.Validate`](internal/analysis/analysis.go), [`incident.Validate`](internal/incident/types.go) | Rejects malformed output, unknown citations/handles, duplicate incidents, invalid values and resolution without recovery eligibility. This validates the contract, not the truth of the diagnosis. | **Deterministic** |
| **13. Reject stale decisions** | Compares current observations with the investigation's evidence fingerprint and inspected resource versions. | [`Runner.current` and `toolsCurrent`](internal/runner/runner.go) | If relevant evidence changed or collection became incomplete while AI was working, discards the result rather than publishing stale conclusions. | **Deterministic** |
| **14. Reconcile and save** | Converts accepted actions into incident changes, using stable identity and concurrency preconditions. | [`Reconciler.Apply`, `next` and `Kubernetes.Save`](internal/incident/store.go) | Suppresses wording-only/nonmaterial updates; advances revision for accepted changes; preserves identity across recurrence. Both authentication modes create/patch Incidents; there is no runtime simulation mode. | **Deterministic execution of an AI decision** |
| **15. Explain and display persisted results** | Agent logs readable activity, identifying AI conclusions separately from Go validation and confirmed CRD writes. The dashboard reads stored Incidents. | [`Writer.Log`](internal/output/writer.go), [`Runner.persisted`](internal/runner/runner.go), [`Reader.List`](internal/dashboard/incidents.go), [browser `refresh`](internal/dashboard/web/app.js) | Normal activity goes to stdout; warnings/errors go to stderr. Neither records incidents. Dashboard polls approximately every 15 seconds while visible; it reads CRDs, not logs, and does not call AI. | **Deterministic rendering with explicitly labelled AI-authored content** |

### Identity and lifecycle

Incident identity combines the observer, anchor resource incarnation and problem
family. Verified ownership groups Pod evidence through ReplicaSets to Deployments
or directly under StatefulSets, DaemonSets and Jobs; scheduled Jobs and their Pods
share the verified CronJob anchor, except unresolved legacy Job records kept in place.
Standalone resources use
their own identity. Services remain stable anchors across EndpointSlice replacement.
Problem families cover scheduling, container failure, rollout, Job failure,
node health, Service availability, storage availability and autoscaling; this is bounded correlation, not
unrestricted semantic grouping across unrelated workloads.

Names use `inc-<identity-hash>`. Individual finding IDs, model wording and
per-run identifiers do not determine incident identity.

Incidents are cluster-scoped and have no storage namespace. Namespace fields
remain on workload anchors and affected resources. The agent's runtime namespace
contains its Deployment, ServiceAccount and model Secret, not its incident
records. Agent Incident access uses a dedicated ClusterRole/ClusterRoleBinding;
observer ownership is validated by the application, not isolated by RBAC.

| [Incident CRD](deploy/incidents-crd.yaml) field | Responsibility |
|---|---|
| `spec.identity` | Immutable observer, verified anchor and problem family |
| `spec.resources` | Affected resource identities |
| `spec.analysis` | AI title, severity, impact, diagnosis, explanation, citations, recommendations, uncertainty and provenance |
| `spec.evidence` | Latest bounded native kubectl descriptions shown to AI for this revision; no duplicate typed state/event copies |
| `spec.lifecycle` | `state` (`open`/`resolved`), material revision, timestamps and recurrence count |
| `status` | Human triage and reserved delivery-controller state; not written by the agent |

Raise creates an incident. Update changes a material conclusion; updating a
matching resolved incident reopens it. Resolve needs continuous fresh recovery
and an AI decision. No-action leaves existing state unchanged. Human dismissal
is status, not another lifecycle state.

Rewording, event/restart counts or timestamps alone do not update a CR.
Same-state updates require AI's material-change decision and a change to severity,
impact, stable diagnosis key or affected resources; otherwise prior wording stays.
The host attaches the exact reviewed description envelope only after this material
gate, on raise, update, reopen and resolve. No-action and description timestamp/text
churn do not write Incidents or change human dismissal. Only the latest envelope
is retained; it is not automatically replayed as model memory on subsequent
investigations. Old-format records must be removed before switching; no migration
or compatibility layer is provided.
The indexing label is `kubescout.benc-uk.github.com/state`, matching
`spec.lifecycle.state`; Pod `status.phase` is unrelated.

The agent reloads its open index and looks up known families, including resolved
incidents, before investigation. Reconciliation rereads ambiguous writes under
the same name and uses UID/resourceVersion preconditions instead of creating a
second record or repeating a model investigation. Unreconcilable writes fail
explicitly. There is no automatic deletion, Pod owner reference or historical
version archive.

Both authentication modes persist accepted changes. Incident state is
restart-aware; no-action suppression, scheduling and quotas are process-local.
The design is single-replica, not HA or exactly-once delivery.

A model, tool or collection failure is a diagnostic, never a synthetic incident.
Incomplete collection also resets recovery tracking; missing observations are
not evidence of health.

## 5. Example: a Service with no backends

| Event | Deterministic part | AI part |
|---|---|---|
| Service has no backends | The Service rule finds zero ready endpoints, creates evidence and schedules investigation. | Decides whether this is a meaningful availability problem and whether to raise an Incident. |
| You create a matching backend Pod | Pod and EndpointSlice changes arrive through watches; cached evidence is recomputed. | Nothing necessarily happens immediately. |
| Ready endpoints appear | The original warning disappears. The existing open incident remains a candidate. Continuous fresh recovery starts being tracked. | Still no automatic resolution. |
| Recovery remains stable | After the recovery window, code permits a recovery review. | Reviews current evidence and chooses `resolve` or `no_action`. |
| AI chooses `resolve` | Code validates freshness and writes `state: resolved`; the dashboard sees it on refresh. | AI supplied the resolution decision. |

Ready endpoints are configuration/readiness evidence, not proof of working DNS,
network connectivity or successful application traffic.

## 6. Timing, once mode and human dismissal

The runner batches new evidence, applies ordinary same-case cooldowns and
periodically revisits unresolved/no-action cases. Increased urgency can bypass
ordinary cooldown, but not request or scope limits. Counter churn alone does not
defeat no-action suppression. Failed investigations receive bounded backoff,
eventually waiting for scheduled review rather than being cached as no-action.

Collection continues while AI runs. Before applying decisions, the runner checks
current evidence and inspected resource versions. Recovery stays under review
even after the initial signals disappear; short healthy phases in a crash loop
and incomplete collection cannot satisfy continuous recovery.

[Scheduling settings](docs/operations.md#limits-and-scheduling) are eligibility
controls, **not guaranteed resolution times**. Budgets, investigation duration,
failures, new observations and the AI conclusion can delay or prevent a change.

- **Once mode:** [`Runner.Once`](internal/runner/runner.go) performs a bounded
  collection and assessment instead of running the informer loop. One snapshot
  cannot establish continuous recovery, so it does not automatically resolve an
  existing incident. It logs progress and a readable completed/incomplete
  summary. Accepted changes are stored in CRDs; zero changes is a valid result,
  not a synthetic healthy incident.
- **Human dismissal:** [`Reader.SetDismissed`](internal/dashboard/incidents.go)
  writes revision-scoped triage status, not the AI lifecycle. The agent continues
  tracking the incident. A material AI change or recurrence advances the lifecycle
  revision and makes the old dismissal ineffective.
- **Dashboard separation:** the dashboard is a separate process and reads stored
  Incidents, including those persisted by local agent runs. Test-only
  fixture repositories are not visible in the dashboard.

### Dashboard contracts

The [dashboard](internal/dashboard/) reads all observers' cluster-scoped incidents.
Workload-namespace filtering uses anchor and affected-resource namespaces without
changing that cluster-wide API scope. Incident summaries and descriptions have no
storage-namespace field. Details expose only the approved descriptions already
stored in the Incident; summaries omit their text. The dashboard cannot read
workloads, AI credentials, live tool evidence or revision history, call AI,
create/delete incidents or write their specs. Description text is safely rendered
and copyable on the Resources tab, with capture times and explicit partial,
unavailable and omitted coverage. It is non-atomic historical evidence, not a
live resource view.

| Route | Contract |
|---|---|
| `GET /api/incidents` | Complete bounded list in an `incidents` array |
| `GET /api/incidents/{name}` | Detail in an `incident` object |
| `POST /api/incidents/{name}/triage` | Bounded JSON with displayed opaque `version` and `dismissed` boolean; returns `incident` |

Browser writes require same-origin checks and `X-KubeScout-Action: triage`.
UID/resourceVersion preconditions prevent triaging a newer revision or recreated
object. Only `status.triage` is patched; spec, labels, lifecycle and other status
fields are preserved. Uncertain writes are not retried automatically.
RBAC permits status-subresource patches more broadly than one field; the
application enforces triage-only ownership. Incident access uses dedicated
ClusterRoles and ClusterRoleBindings; workload filters and observer labels are
not authorization boundaries.

Reads fail on incomplete, malformed or oversized snapshots, not partial success.
The browser renders text safely and retains last successful data with an explicit
stale state on refresh failure. These protections do not provide authentication:
see [dashboard access](docs/operations.md#deploying-the-dashboard) and
[usage/limits](docs/operations.md#using-the-dashboard).

## 7. Observing the agent and model exchanges

[Console events](internal/output/writer.go) label origin (`GO` or `AI`), phase,
level and run/investigation correlation. Collection, deterministic checks,
scheduling, HTTP dispatch/errors, local tool execution, validation and persistence
are Go activity. Tool selection and incident/no-action conclusions come from AI.
An AI conclusion is not a stored incident until reconciliation succeeds.

The analyzer captures [each bounded HTTP exchange](internal/analysis/analysis.go),
not just each investigation. [Transcript projection](internal/analysis/transcript.go)
retains model-visible input, instructions, tool-round history and returned model
content without authentication metadata or raw provider error bodies.
[The shared tool session](internal/investigation/session.go) records normalized
authorized arguments and approved credential-protected results for both tools. Call and byte
budgets are shared across resource inspection and Warning event reads, not
reset per tool. Runner dispositions distinguish
accepted, stale, failed and nonmaterial outcomes.

[File capture and rotation](internal/output/transcript.go) are bounded and
independent of investigation success. Capture failures warn and retry without
changing model budgets, request counts, scheduling or incident decisions.
Files are diagnostic history, never replayed model memory or an incident store.
See [logging and transcripts](docs/operations.md#logs-and-ai-transcripts)
for flags, privacy, retention, ephemeral storage and retrieval.

## Code map

| Package / directory | Responsibility |
|---|---|
| [cmd/kubescout](cmd/kubescout/) | Agent CLI, explicit auth/configuration, process lifecycle and exit codes |
| [cmd/kubescout-dashboard](cmd/kubescout-dashboard/) | Standalone dashboard CLI, authentication and HTTP lifecycle |
| [internal/config](internal/config/) | Validated options, limits, namespace selection and root dotenv loading |
| [internal/kube](internal/kube/) | Scoped collection, approved projection, ownership and freshness |
| [internal/health](internal/health/) | Pure evidence rules, not incident publication |
| [internal/investigation](internal/investigation/) | Typed scoped read-only tools, native describe, shared budgets and credential protection |
| [internal/analysis](internal/analysis/) | Agent Framework Responses loop, budgets, decision validation and transcript projection |
| [internal/incident](internal/incident/) | Correlation, domain types, lifecycle and Kubernetes reconciliation |
| [internal/runner](internal/runner/) | Candidate scheduling, recovery review and stale-result rejection |
| [internal/output](internal/output/) | Human-readable activity logs and bounded diagnostic transcripts |
| [internal/dashboard](internal/dashboard/) | Cluster-scoped Incident reads, human triage and embedded browser UI |
| [build/](build/) | Separate pinned images and allow-listed build contexts |
| [deploy/](deploy/) | Incident schema, agent Deployment and RBAC |
| [deploy/dashboard/](deploy/dashboard/) | Independent dashboard Deployment, Service and triage RBAC |

**Bottom line: rules detect evidence; AI interprets it; deterministic safeguards
control CRD persistence.** Validation checks the decision contract, not the truth
of a diagnosis. Stable identities and bounded evidence do not guarantee semantic
correctness. External notifications, remediation and automatic incident retention
are not implemented.
