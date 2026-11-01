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
DaemonSets, Services, EndpointSlices, Jobs and Warning Events. An explicit
namespace watch list uses namespace-scoped requests. All/ignore mode uses
cluster-wide requests and filters ignored namespaces locally. Nodes are always
observed.

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
| [jobs.go](internal/health/jobs.go) | Terminal Job failure conditions. |
| [events.go](internal/health/events.go) | Recent Warning Events as supporting evidence. |

### Resource semantics

ReplicaSets establish ownership rather than a separate incident category.
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
namespace scope, pagination, admission, freshness and investigation limits.
Missing access means incomplete collection, not healthy resources.

### Correlation and scheduling

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **6. Evaluate evidence rules** | Runs checks for nodes, Pods, controllers, Services, Jobs and Warning events. Produces internal findings. | [`Evaluate`](internal/health/checks.go), for example [`Services`](internal/health/services.go) | Detects conditions such as no ready backends, a failed Pod or a stalled rollout. Assigns informational/warning/critical **hints**, not final incident severity. | **Deterministic** |
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
  state, scheduling/capacity facts, container state and bounded redacted condition
  messages. It includes controller template images/resources/container ports,
  Service port definitions and EndpointSlice readiness counts, not raw templates,
  volume claims, selectors, endpoint addresses or target UIDs.
- [`related_warning_events`](internal/investigation/related_warning_events.go)
  returns bounded redacted Warning events for an authorized resource. Node-event
  reads use the same namespace watch/ignore scope. Reads are limited to 20 pages
  and 20 retained events per call; incomplete/oversized results fail explicitly.

Both use the [shared tool session](internal/investigation/session.go) for handle
authorization, normalized arguments and shared call/byte budgets. Tools do not
accept arbitrary namespaces, selectors, URLs or API paths. Unknown handles/tools
and malformed arguments fail before API access.

Raw API objects, UIDs, annotations, credentials and environment variables are not
model input. Tools cannot read Secrets, ConfigMaps, Pod logs or arbitrary custom
resources, execute commands, modify workloads or deliver notifications.
Approved names, image references, scheduling facts and messages can still reveal
sensitive operational details. Redaction reduces risk; it cannot prove arbitrary
text secret-free. Tool results are untrusted evidence, not instructions.

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
or directly under StatefulSets, DaemonSets and Jobs. Standalone resources use
their own identity. Services remain stable anchors across EndpointSlice replacement.
Problem families cover scheduling, container failure, rollout, Job failure,
node health and Service availability; this is bounded correlation, not
unrestricted semantic grouping across unrelated workloads.

Names use `inc-<identity-hash>`. Individual finding IDs, model wording and
per-run identifiers do not determine incident identity.

| [Incident CRD](deploy/incidents-crd.yaml) field | Responsibility |
|---|---|
| `spec.identity` | Immutable observer, verified anchor and problem family |
| `spec.resources` | Affected resource identities |
| `spec.analysis` | AI title, severity, impact, diagnosis, explanation, citations, recommendations, uncertainty and provenance |
| `spec.lifecycle` | `state` (`open`/`resolved`), material revision, timestamps and recurrence count |
| `status` | Human triage and reserved delivery-controller state; not written by the agent |

Raise creates an incident. Update changes a material conclusion; updating a
matching resolved incident reopens it. Resolve needs continuous fresh recovery
and an AI decision. No-action leaves existing state unchanged. Human dismissal
is status, not another lifecycle state.

Rewording, event/restart counts or timestamps alone do not update a CR.
Same-state updates require AI's material-change decision and a change to severity,
impact, stable diagnosis key or affected resources; otherwise prior wording stays.
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

The [dashboard](internal/dashboard/) reads all observers' incidents from one
storage namespace. Workload-namespace filtering uses anchor and affected-resource
namespaces without widening that API scope. It cannot read workloads, AI
credentials, tool evidence or revision history, call AI, create/delete incidents
or write their specs.

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
application enforces triage-only ownership.

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
authorized arguments and approved redacted results for both tools. Call and byte
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
| [internal/investigation](internal/investigation/) | Typed scoped read-only tools, shared budgets and redaction |
| [internal/analysis](internal/analysis/) | Agent Framework Responses loop, budgets, decision validation and transcript projection |
| [internal/incident](internal/incident/) | Correlation, domain types, lifecycle and Kubernetes reconciliation |
| [internal/runner](internal/runner/) | Candidate scheduling, recovery review and stale-result rejection |
| [internal/output](internal/output/) | Human-readable activity logs and bounded diagnostic transcripts |
| [internal/dashboard](internal/dashboard/) | Namespaced Incident reads, human triage and embedded browser UI |
| [build/](build/) | Separate pinned images and allow-listed build contexts |
| [deploy/](deploy/) | Incident schema, agent Deployment and RBAC |
| [deploy/dashboard/](deploy/dashboard/) | Independent dashboard Deployment, Service and triage RBAC |

**Bottom line: rules detect evidence; AI interprets it; deterministic safeguards
control CRD persistence.** Validation checks the decision contract, not the truth
of a diagnosis. Stable identities and bounded evidence do not guarantee semantic
correctness. External notifications, remediation and automatic incident retention
are not implemented.
