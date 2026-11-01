# KubeScout: from Kubernetes changes to incidents

This guide follows the default **watch-mode** flow, identifies the code handling
each step, and separates deterministic decisions from AI decisions.

```text
Kubernetes change
    -> local cache
    -> deterministic evidence
    -> scheduled AI investigation
    -> validated decision
    -> Incident
    -> dashboard
```

**Deterministic code decides what to investigate and whether a decision is safe
to apply. AI decides whether there is an incident and what it means.**

Here, deterministic means ordinary Go logic, not a model call. There is no
deterministic-only incident publisher.

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
| **10. Gather more evidence if needed** | The model can request `inspect_resource` or `related_warning_events`. Local Go tools make scoped Kubernetes reads and return bounded, redacted results. This may repeat. | [`Tool.Call`](internal/investigation/tools.go) | **AI chooses** which supplied resource to inspect. **Code enforces** allowed handles, namespace scope, identity, tool limits and returned fields. No shell or workload writes. | **Mixed** |
| **11. Propose a conclusion** | Model returns structured JSON: `no_action`, or actions to `raise`, `update` or `resolve`. Updating a resolved incident can reopen it. | [Decision instructions](internal/analysis/instructions.txt), [`Action` and `Decision`](internal/incident/types.go) | Whether a meaningful problem exists; its family, severity, impact, diagnosis, explanation and recommendations; whether recovery warrants resolution. | **AI** |

**Even a deterministic critical hint may legitimately result in AI `no_action`.**
No-action leaves any existing incident unchanged; it does not mean resolved.

The model does not receive arbitrary access to the cluster. A resource handle
refers to an already-authorized resource in this investigation. The host keeps
the mapping to Kubernetes identity private and validates tool requests against it.

## 4. Turning the conclusion into a stored Incident

| Step | What happens | Code responsible | Decisions made | Deterministic or AI? |
|---|---|---|---|---|
| **12. Validate the response** | Parses strict JSON and checks the proposed actions. | [`analysis.Validate`](internal/analysis/analysis.go), [`incident.Validate`](internal/incident/types.go) | Rejects malformed output, unknown citations/handles, duplicate incidents, invalid values and resolution without recovery eligibility. This validates the contract, not the truth of the diagnosis. | **Deterministic** |
| **13. Reject stale decisions** | Compares current observations with the investigation's evidence fingerprint and inspected resource versions. | [`Runner.current` and `toolsCurrent`](internal/runner/runner.go) | If relevant evidence changed or collection became incomplete while AI was working, discards the result rather than publishing stale conclusions. | **Deterministic** |
| **14. Reconcile and save** | Converts accepted actions into incident changes, using stable identity and concurrency preconditions. | [`Reconciler.Apply`, `next` and `Kubernetes.Save`](internal/incident/store.go) | Suppresses wording-only/nonmaterial updates; advances revision for accepted changes; preserves identity across recurrence. Both authentication modes create/patch Incidents; there is no runtime simulation mode. | **Deterministic execution of an AI decision** |
| **15. Publish and display** | Agent emits accepted material changes on stdout. Separately, the dashboard reads stored Incidents through its own server. | [`Writer.WriteChange`](internal/output/writer.go), [`Reader.List`](internal/dashboard/incidents.go), [browser `refresh`](internal/dashboard/web/app.js) | Diagnostics/no-action go to stderr. Dashboard polls approximately every 15 seconds while visible; it does not receive the agent's stream or call AI. | **Deterministic** |

Incident identity combines the observer, anchor resource identity and problem
family. The reconciler rereads ambiguous writes and uses UID/resourceVersion
preconditions instead of creating a second name on retry.

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

Defaults are defined in [configuration parsing](internal/config/config.go).

| Control | Default | Purpose |
|---|---|---|
| `--batch-window` | 5 seconds | Groups bursts of changes before investigation. |
| `--incident-cooldown` | 2 minutes | Limits ordinary changed-evidence reviews for the same candidate. Increased urgency can bypass this cooldown. |
| `--review-interval` | 15 minutes | Revisits unresolved/no-action cases even without new evidence. |
| `--recovery-window` | 2 minutes | Requires continuous fresh recovery before AI may resolve an Incident. |
| `--check-interval` | 15 seconds | Configures time-based re-evaluation. The watch loop ticks at the smaller of this and the batch window. |
| `--model-calls-per-hour` | 60 | Caps actual model HTTP attempts, including tool rounds, in a rolling hour. |

These are scheduling controls, **not guaranteed resolution times**. Model budgets,
investigation duration, retries, new observations and the AI conclusion can delay
or prevent a lifecycle change.

- **Once mode:** [`Runner.Once`](internal/runner/runner.go) performs a bounded
  collection and assessment instead of running the informer loop. One snapshot
  cannot establish continuous recovery, so it does not automatically resolve an
  existing incident. It emits one execution envelope, including an empty changes
  array when no changes are accepted.
- **Human dismissal:** [`Reader.SetDismissed`](internal/dashboard/incidents.go)
  writes revision-scoped triage status, not the AI lifecycle. The agent continues
  tracking the incident. A material AI change or recurrence advances the lifecycle
  revision and makes the old dismissal ineffective.
- **Dashboard separation:** the dashboard is a separate process and reads stored
  Incidents, including those persisted by local agent runs. Test-only
  fixture repositories are not visible in the dashboard.

**Bottom line: rules detect evidence; AI interprets it; deterministic safeguards
control publication.**
