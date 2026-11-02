# Operating KubeScout

[Overview](../README.md) · [Architecture](../ARCHITECTURE.md) · [Development](development.md)

This guide covers running the agent and dashboard, configuring their scope,
deploying them, and diagnosing operational problems. Run Make commands from the
repository root. Model-calling commands may incur charges; setup and dashboard
commands do not call AI.

## Configuration

### Model connection

The agent requires these environment variables:

| Variable | Value |
|---|---|
| `AZURE_OPENAI_ENDPOINT` | HTTPS endpoint with exactly `/openai/v1/` as its path, on port 443 |
| `AZURE_OPENAI_API_KEY` | API key for that endpoint |
| `AZURE_AI_MODEL_DEPLOYMENT_NAME` | Model deployment name |

Use a deployment supporting the Responses API and function tools. The endpoint
cannot contain credentials, query parameters or a fragment. Missing or invalid
model settings fail explicitly; there is no non-AI runtime mode.

For local runs, the agent loads this repository's root `.env`; existing
environment variables win, including explicitly empty values. It does not load
arbitrary parent-directory dotenv files. The dashboard does not load `.env` or
need model credentials.

Keep credentials out of command arguments, images, committed manifests and logs.
For `make model-secret`, use plain `KEY=value` lines without `export` or surrounding
quotes: kubectl reads literal file values, not the shell's environment overrides.
Every `.env` entry becomes a Secret key, so include only intended runtime settings.

`ALLOW_MODEL=yes` is a Make confirmation gate, not application configuration or
a budget control. It is required for `run`, `once`, `deploy` and the model-calling
live tests. Direct binary execution bypasses that gate and is itself a request
to run AI; it still requires valid model configuration.

### Kubernetes access

Authentication is explicit and never falls back between modes:

| Mode | Agent | Dashboard |
|---|---|---|
| `kubeconfig` | Uses current context; persists incidents | Binds `127.0.0.1:8000`; reads incidents and permits human triage |
| `in-cluster` | Uses its ServiceAccount; persists incidents | Binds `:8000`; uses its separate ServiceAccount |

Direct executable runs support `--context` and `--kubeconfig` in kubeconfig mode,
otherwise standard `KUBECONFIG`/default-file loading applies. In-cluster mode
rejects kubeconfig flags. Local Make run/deploy/dashboard targets use the current
context; `CONTEXT` is only a Make variable for live integration tests.

The agent needs read access to Nodes, Pods, Deployments, ReplicaSets, StatefulSets,
DaemonSets, Services, EndpointSlices, PVCs, PVs, VolumeAttachments, Jobs, CronJobs,
HPAs, Ingresses and Warning Events. See
[Resources and health checks](resources-and-checks.md) for the collected facts
and checks, including grace periods and exceptions. Refer to the
[agent RBAC](../deploy/rbac.yaml) for exact resource verbs. PV reads are cluster-wide;
PVC reads follow namespace selection. Existing installations need the updated
read RBAC and Incident CRD's `storage-availability` family before running the
rebuilt agent; applying schema/RBAC changes alone need not activate the Deployment.
No storage source, StorageClass or Secret access is added. Incident permissions
are `get`, `list`, `create` and `patch` on cluster-scoped Incidents:

```bash
kubectl auth can-i get incidents.kubescout.benc-uk.github.com
kubectl auth can-i list incidents.kubescout.benc-uk.github.com
kubectl auth can-i create incidents.kubescout.benc-uk.github.com
kubectl auth can-i patch incidents.kubescout.benc-uk.github.com
```

CronJob support also requires `get`, `list` and `watch` on `batch/cronjobs`
within the workload scope; update existing read RBAC before running the rebuilt
agent. It reuses the existing `job-failure` incident family. No workload writes
or automatic migration of existing Job incidents are added.

HPA support requires `get`, `list` and `watch` on
`autoscaling/horizontalpodautoscalers` using `autoscaling/v2`. Existing
installations also need the updated Incident CRD's `autoscaling` family.
Update the read RBAC and CRD before running the rebuilt agent; these changes
do not authorize metrics APIs, `/scale` access or workload writes.

Ingress support requires `get`, `list` and `watch` on
`networking.k8s.io/ingresses` using `networking.k8s.io/v1` within the workload
scope. Update existing read RBAC before running the rebuilt agent. It reuses
`service-availability`; no new Incident family is needed for Ingress.
No IngressClass, Secret, certificate or external-probe access is added.

VolumeAttachment support requires cluster-wide `get`, `list` and `watch` on
`storage.k8s.io/volumeattachments`. The existing `kubescout-nodes` ClusterRole
also grants these attachment reads alongside Node/PV reads; update its RBAC
before running the rebuilt agent. It reuses `storage-availability` and does not
add CSI access, `/scale`, attachment metadata output or workload/storage writes.
Clusters without attachable storage may return an empty list; denied reads are
incomplete collection, not an empty healthy inventory.

`make incidents-setup` requires namespace/CRD apply permissions. It installs the
`kubescout` runtime namespace and cluster-scoped Incident CRD and waits for establishment, but does not
grant workload/incident permissions, create a model Secret or start an agent.

The dashboard needs cluster-scoped `get`/`list` on Incidents.
Dismissal/restore additionally requires `patch` on `incidents/status`. Read-only
credentials allow browsing; write failures appear explicitly. Human triage is
a real Kubernetes status write in either authentication mode.

Agent and dashboard Incident permissions use separate ClusterRoles and
ClusterRoleBindings. Their ServiceAccounts remain in the runtime namespace.
No additional workload write permissions are granted. Observer labels and
workload-namespace filters do not isolate incident access between teams.

### Namespace scope and observer identity

| Selection | Flag or environment setting |
|---|---|
| Watch only named namespaces | `--namespace-watch=apps,backend` / `NAMESPACE_WATCH=apps,backend` |
| Watch all except named namespaces | `--namespace-ignore=kube-system,kube-public` / `NAMESPACE_IGNORE=kube-system,kube-public` |
| Watch all, including newly created namespaces | Neither list set |

The two lists are mutually exclusive, including combinations of environment
variables and flags. Flags replace their corresponding environment list, not the
opposite list; unset the opposite variable when switching modes. Empty environment
values count as unset. Names are trimmed and deduplicated; wildcards, invalid
names and empty comma-separated entries are rejected.

Explicit watch lists use namespace-scoped requests and can use Roles in each
watched namespace. All/ignore mode uses cluster-wide requests and does not require
Namespace-object discovery. Ignored objects are filtered before cache admission
and model input, but **ignore is not an RBAC boundary**. Nodes, PVs and
VolumeAttachments are always observed cluster-wide, including those related to
claims outside the workload scope.
Those claims and their Pods are not collected or exposed as related handles.
Node/PV/attachment Warning-event reads still respect the selected event namespaces.
The checked-in workload ClusterRole grants reads across namespaces.

`--observer-id` is required: choose a stable DNS label, at most 63 lowercase
letters, digits or hyphens, beginning/ending with a letter or digit. Keep it
unique to this cluster and observation scope and retain it across restarts.
Changing it creates separate incident identities; it does not adopt old records.
Use a new ID when changing scope, and do not run competing replicas with one ID.
Out-of-scope stored incidents are not treated as recovered.

Incidents are cluster-scoped and have no `metadata.namespace`. The agent's
`--incident-namespace` and dashboard's `--namespace` options have been removed.
The dashboard reads all observers and lifecycle states; its workload-namespace
filter uses anchor and affected-resource namespaces without changing Kubernetes
API scope. Agent workload namespace selection is unchanged.

## Local operation

After building and configuring the model as in the [README](../README.md#run-locally):

```bash
kubectl config current-context
make incidents-setup
make run NAMESPACE_WATCH=apps,backend OBSERVER_ID=local-apps-backend ALLOW_MODEL=yes
```

Stop with Ctrl+C. Use `make once` with the same scope/identity for one bounded
assessment instead. Both persist accepted AI decisions; neither modifies
workloads. Once mode cannot establish continuous recovery from one snapshot.

Extra runtime flags go in `ARGS`; never put credentials there:

```bash
make once NAMESPACE_WATCH=apps,backend OBSERVER_ID=local-apps-backend \
  ALLOW_MODEL=yes ARGS='--model-calls-per-hour=6 --log-level=debug'
```

Make's default observer is `local-dev-<username>`; prefer an explicit
cluster/scope-specific ID. Avoid overriding Make's auth/context/identity flags
through `ARGS`. A direct run can select a context explicitly:

```bash
./build/bin/kubescout --auth-mode=kubeconfig --context=kind-kind \
  --namespace-watch=apps,backend --observer-id=kind-apps-backend
```

`make dash-run` builds and serves the cluster-wide incident dashboard at
http://127.0.0.1:8000. No image, Deployment or port-forward is needed locally.
The CRD must exist; running the dashboard does not start the agent.

## Kubernetes deployment

### Registry images

Review the current kubeconfig context before any apply/create command. Configure
images for your node architecture and registry access before deploying:

| Component | Image configuration | Checked-in selection | Pull policy |
|---|---|---|---|
| Agent | [Kustomization](../deploy/kustomization.yaml), image name `kubescoutImage` | `ghcr.io/benc-uk/kubescout:arm64` | `Always` |
| Dashboard | [Kustomization](../deploy/dashboard/kustomization.yaml), image name `localhost/kubescout-dashboard` | `localhost/kubescout-dashboard:dev` | `Never` |

The agent tag is architecture-specific, not a promise of a multi-platform release.
Build/push an appropriate image if needed; see [image builds](development.md#container-images).
Set `newName`/`newTag` in the appropriate Kustomization and review the Deployment's
pull policy. For registry-based dashboard deployment, replace the local image
selection and `Never` policy. Private registries need appropriate cluster pull access.

`IMAGE` and `DASHBOARD_IMAGE` control Make build/import targets, **not manifest
substitution**. Likewise, `NAMESPACE_WATCH`, `NAMESPACE_IGNORE`, `OBSERVER_ID` and
`ARGS` affect local runs, not `make deploy`.

### Starting the agent

Review [the Deployment](../deploy/deployment.yaml) and [RBAC](../deploy/rbac.yaml):

- Observer `kind-all`; change it for your cluster/scope.
- All namespaces by default. Configure at most one explicit `NAMESPACE_WATCH`
  or `NAMESPACE_IGNORE` environment entry. These entries override same-named
  keys from the Secret's `envFrom`.
- Cluster-scoped Incidents; workload reads and incident writes have separate RBAC.
- Model budget **300 requests/hour** in the manifest, versus the binary's default 60.
- One active replica, with a Recreate strategy; the checked-in replica count is zero.

With model settings in `.env`, run:

```bash
make model-secret
make deploy ALLOW_MODEL=yes
kubectl -n kubescout rollout status deployment/kubescout
kubectl get incidents
kubectl -n kubescout logs deployment/kubescout -c agent
```

`make model-secret` applies the namespace, then creates `kubescout-model` using
`kubectl create secret --from-env-file=.env`. It requires namespace-apply and
Secret-create permissions, fails if the Secret exists, and does not start AI.
Rotate credentials deliberately using standard Kubernetes tooling.

`make deploy` checks that the Secret is accessible without printing its contents,
applies the agent bundle and scales to one replica. This starts ongoing model
calls. Applying the bundle alone leaves the agent at zero replicas.

The images use pinned bases and non-root users. Deployments use read-only root
filesystems, dropped capabilities and RuntimeDefault seccomp. The agent's only
writable application mount is bounded ephemeral transcript storage.

### Local kind alternative

For a development kind cluster, first change the agent Kustomization to
`localhost/kubescout:dev` and its Deployment pull policy to `Never`.
The dashboard already uses the corresponding local development defaults.
Then build/import the images you need:

```bash
make image
make kind-load KIND_CLUSTER=kind
make dash-image
make dash-kind-load KIND_CLUSTER=kind
```

These commands do not deploy anything. Follow the agent and dashboard activation
steps separately. `KIND_CLUSTER` is a kind cluster name, independent of the current
kubeconfig context; ensure they identify the same cluster. The default container
engine and kind provider are Podman; override `CONTAINER_ENGINE` and `KIND_PROVIDER`
if your cluster uses Docker.

### Deploying the dashboard

The [dashboard bundle](../deploy/dashboard/) is excluded from the agent bundle.
It needs the existing `kubescout` namespace, Incident CRD and an accessible or
locally imported image:

```bash
make dash-deploy
make dash-forward
```

Open http://127.0.0.1:8000. The independent Deployment, ClusterIP Service,
ServiceAccount and cluster-scoped incident RBAC do not start/scale the agent or mount model
credentials.

**There is no application login or TLS.** Use localhost port-forward in a trusted
cluster, not public ingress. ClusterIP is not authentication: other reachable
in-cluster clients can read incidents and dismiss/restore them. Same-origin
browser protections are not access control. Browser clients never receive the
ServiceAccount token.

There is no incident-storage namespace to select. To move the dashboard itself,
update Deployment, Service and ServiceAccount namespaces and the
ClusterRoleBinding subject's namespace. Keep the ClusterRole and
ClusterRoleBinding metadata namespace-free. Create the destination namespace
separately and adjust port-forward commands; the Make helpers target `kubescout`.

### Updates and shutdown

Rebuilding/importing an image does not restart a running Pod. After preparing
updated manifests/images, explicitly roll out the relevant Deployment; when
reusing a development tag, import it before restarting. Apply updated CRDs/RBAC
before running a version that needs new fields or resource access.

To stop only the agent while retaining its Deployment:

```bash
kubectl -n kubescout scale deployment/kubescout --replicas=0
```

To remove the agent or dashboard independently:

```bash
make undeploy
make dash-undeploy
```

These preserve incidents, the model Secret, namespace and CRD. **Do not use
`kubectl delete -k deploy` as a shutdown command:** deleting the CRD deletes
all Incident records. Deleting the runtime namespace no longer deletes
cluster-scoped Incidents, but still destroys namespaced runtime resources,
including model credentials.

**Development-only scope reset:** namespaced Incident installations cannot be
upgraded by applying the new scope. Stop the agent and dashboard, explicitly
delete the old Incident CRD and its records, then install the new cluster-scoped
CRD and updated RBAC/binaries. Remove obsolete namespaced incident
Roles/RoleBindings separately. This is destructive: there is no migration,
record restoration or preservation of history/triage. No Make target or agent
startup performs the reset automatically; use a fresh development cluster if
you do not want to reset an existing one.

For older installations, stop the old producer before activating this agent.
Legacy HealthReport records are not converted, modified or deleted; export or
delete them only through an explicitly approved migration. Remove obsolete
report-write RBAC deliberately. Lifecycle records using `phase` require explicit
migration to `state`; there is no automatic fallback.

Removed flags include `--deterministic-only`, `--report-namespace`, `--report-file`,
`--output-file`, `--simulate-incidents` and `--incident-namespace`; the dashboard
also rejects its former `--namespace` option. Use AI configuration and workload
namespace filters; diagnostic transcripts are not an incident export format.
The current dashboard resources are named `kubescout-dashboard`. Deploying them
does not retire an older `k8s-dashboard` installation; remove that separately
after verification.

## Using the dashboard

The desktop-oriented interface shows undismissed open incidents, highest severity
then most recently updated. Severity uses text as well as color; resolved is a
state, not a severity. The initial light/dark theme follows the system and an
explicit choice is remembered.

- Search by title, incident name, observer or affected resource. Combine state,
  severity, problem-family and workload-namespace filters.
- Sort by severity, title, raised time or updated time; the adjacent arrow reverses
  the order. Results show 50 per page.
- Open a row or activate its title with the keyboard for **Focused review**.
  **Overview** shows diagnosis, explanation, recommendations and uncertainty;
  **Resources** lists affected resources; **Record** shows lifecycle, provenance
  and copyable identifiers.
- The **Problem namespace** belongs to the anchor resource. Incident records
  themselves have no namespace; Node, PV and VolumeAttachment problems show `Cluster-scoped`.
- Use Left/Right or Home/End within the tabs. Refresh retains the tab and scroll
  position, including each expanded describe panel's reading position and keyboard
  focus within the same lifecycle revision. A material revision resets resource
  expansion and describe scroll; another incident opens on Overview. Escape, the backdrop or Close
  returns focus to the list. The modal is designed for desktop, not a separate
  mobile layout.
- Record-tab copy buttons copy only the identifier. If clipboard access fails,
  the value is selected for manual copying with an explicit message.

**Dismiss incident** hides its current revision from the default list and open
severity counts. **Dismissed** includes dismissed open and resolved incidents;
**Open** and **Resolved** exclude them; **All states** includes everything.
Use **Restore incident** to show a dismissed item again.

Dismissal is shared and persists across dashboard restarts. It is not resolution
or investigation suppression: a material AI update, resolution or recurrence
advances the revision and makes the old dismissal ineffective.

Refresh runs every 15 seconds while visible, with a manual refresh button.
Failures retain the last successful data with a stale warning; initial failures
and deleted/missing incidents are explicit. **An empty list does not prove cluster
health or agent activity.**

The dashboard reads one complete cluster-wide snapshot, bounded to 1,000 stored incidents
(including resolved), 100-record Kubernetes pages and a 10-second deadline.
Upstream and encoded responses are capped at 8 MiB. Oversized/incomplete snapshots
fail rather than displaying a partial list. Larger histories require deliberate
external archival; the dashboard never deletes records.

Evidence citations such as `inspect-resource-1` and `related-warning-events-2`
identify calls within one investigation. Older `tool-N` references remain
unchanged. They do not provide access to raw evidence, logs or revision history.

### Resource descriptions

The Resources tab shows **native kubectl describe output** captured by Go before
AI assessment. This is the only resource-detail text stored in `spec.evidence`
with each accepted raise, material update, reopen and resolve. State and Warning
events are already part of describe, so no duplicate typed state/event copies
are collected or stored. Only the latest revision's description set is retained.

Each resource starts collapsed, with its name, kind, namespace and section
coverage visible in the summary. Expand resources individually or use Expand all /
Collapse all. Open resources remain open across refreshes of the same lifecycle
revision. Each resource has one native text panel, one coverage status, one
observation time and one citation. Display and Copy preserve the same stored
text, without compacting or reformatting it. Partial and unavailable coverage
remain visible even while collapsed.
Captured text starts directly with native output, without added policy or
partial-read preambles. Coverage remains in `output.status`, with unavailable
sections identified inline.

Each set covers the anchor and up to three authorized related resources,
prioritizing symptom-bearing resources, with a count of omitted context resources.
Output comes directly from `k8s.io/kubectl` v0.35.9, not from an AI renderer or a
reconstruction of typed inspection fields. The `kubectl-describe/v1` envelope
stores `resource`, `source`, `observed_at`, `evidence_id` and `output`
(`status`/`text`) per entry in `spec.evidence.descriptions`. The previous field is
not accepted as an alias; apply the renamed CRD alongside the updated binaries.
The exact text shown to AI is stored and displayed. The AI can still request
additional facts through `inspect_resource` and `related_warning_events`; their
results are citations, not additional stored state/event copies.

Describe uses the same explicitly selected Kubernetes authentication, but its
transport independently restricts APIs, identities, namespaces, deadlines and
response sizes. Related objects must already be authorized investigation handles;
Warning events must match the resource incarnation. No Secrets, ConfigMaps,
logs, commands, Node Leases or other unapproved API reads are added. No RBAC
expansion is needed. An unsupported native kind (currently VolumeAttachment)
is explicitly unavailable, never a typed or generic raw-object fallback.

Native output no longer uses a section allowlist. Labels, addresses, selectors,
commands, probes, storage details and non-UID identifiers remain visible as emitted
by kubectl. Only credential values are redacted from operational text, preserving
field names and surrounding diagnostics; words such as `secret`, `token`,
`password` and `authorization` alone do not trigger redaction. Annotations,
environment values and UIDs remain excluded from model input. Control characters
are escaped before native formatting; browser text remains safely rendered.
This does not guarantee arbitrary operational text is credential-free.
Omitted fields and scoped totals are not proof
of absence or health. Additional read failures are labelled even when kubectl
would otherwise hide an event error.

Output status explicitly distinguishes captured, partial, unavailable and absent
observations. API denial/timeouts or event limits do not become "no problems";
scope, incarnation and protocol violations still abort the investigation.
Resource disappearance and missing evidence never independently establish recovery.
Capture time is not dashboard refresh time, and separate reads are not an atomic
cluster snapshot. The model does not author or regenerate the stored text.

Preparation also runs for investigations that ultimately choose no-action, but
no-action and description-only changes never write an Incident. Automatic reads are
logged as `GO DESCRIBE`, not AI tool requests. They share the investigation
deadline and evidence-byte budget without consuming AI tool invocation slots.
Plain UTF-8 avoids base64 overhead; it is not encrypted, and redaction cannot
guarantee arbitrary operational text is secret-free. Anyone able to read
Incidents or access the trusted dashboard can read these descriptions.

**Rollout:** apply the updated [Incident CRD](../deploy/incidents-crd.yaml) before
running the updated agent, and remove old-format Incident records before switching.
The previous evidence layout has no migration or compatibility layer. No records
are deleted automatically and no RBAC expansion is needed. The agent checks that
create/patch responses retain its descriptions and reports schema pruning explicitly.
Avoid mixed-version writers. Rebuild agent/dashboard binaries and images to
include the new instructions and embedded UI.

The envelope is capped at 16 KiB of serialized JSON, including escaped text and
metadata, and shrinks further to fit the remaining initial AI context budget.
Mandatory coverage metadata must fit or the investigation fails explicitly.
Describe uses at most 16 actual HTTP attempts per investigation,
128 KiB per raw response and 1 MiB cumulatively. Its lists use 20-item pages,
with at most 20 Warning events or 50 authorized related objects per complete list.
Each resource's describe has its own request-timeout window inside the same
investigation deadline. Each resource's native text is capped at 8 KiB; the
16 KiB serialized envelope is available to descriptions without reserving space
for duplicate typed evidence. Text may be explicitly shortened without relaxing
any model limit and counts toward the shared 32 KiB evidence allowance.
The two existing tool invocation and result-size limits are unchanged.
List summaries omit text, but Kubernetes pages still carry full Incident specs;
100 maximal description envelopes add roughly 1.56 MiB per upstream page.

## Limits and scheduling

These are binary defaults from [configuration parsing](../internal/config/config.go),
not guaranteed investigation or resolution times. The Deployment overrides the
hourly quota as noted above.

| Control | Default / setting |
|---|---|
| Concurrent investigations | 1 |
| Model HTTP attempts per rolling hour | 60; `--model-calls-per-hour` |
| Model requests per investigation | 6, including tool rounds and synthesis |
| Tool calls per investigation | 64 shared across both tools; `--max-tool-calls` |
| New-evidence batching | 5s; `--batch-window` |
| Ordinary same-case cooldown | 2m; `--incident-cooldown` |
| Continuous fresh recovery before review | 2m; `--recovery-window` |
| Unchanged unresolved/no-action review | 15m; `--review-interval` |
| Time-based evidence re-evaluation | 15s; `--check-interval`; watch ticks at the smaller of this and the batch window |
| API/model request timeout | 10s; `--request-timeout` |
| Investigation deadline | 120s; `--investigation-timeout` |
| Once / initial informer synchronization deadline | 120s; `--run-timeout` |
| Watch connection lifetime | 5m; `--watch-timeout` |
| Initial evidence budget | 20,000 bytes; `--max-input-chars` |
| Automatic resource descriptions | At most 4 resources, 16 KiB serialized total within remaining initial budget; explicit omissions |
| Native kubectl describe reads | 8 KiB text per resource; 16 API HTTP attempts / 128 KiB per raw response / 1 MiB cumulative |
| Tool result / cumulative evidence output | 8 KiB per AI tool result / 32 KiB including prepared descriptions |
| Model output per request | 2,000 tokens; `--max-output-tokens`, plus cumulative byte checks |
| Retained objects / pending cases / open index | Bounded by `--max-objects`, default 5,000 |

Numeric budget flags have no environment aliases. Use local `ARGS` or edit the
agent Deployment's `args`. Raising one limit does not raise independent request,
byte or deadline ceilings. Run `./build/bin/kubescout --help` for the full flag list.

## Logs and AI transcripts

**Read incidents from Kubernetes, not logs:**

```bash
kubectl get incidents
kubectl get incident <name> -o yaml
```

Console events have UTC timestamps, levels, correlation IDs and origin/phase
labels. `GO` covers collection, evidence, scheduling, HTTP dispatch, tool
execution, validation and persistence. `AI` identifies model-selected tools and
conclusions. A proposal is not persisted until the confirmed incident-write event.

Normal activity goes to stdout; warnings/errors go to stderr. `--log-level=info`
is the default; `debug` adds collection/check details, and `warn`/`error` reduce
verbosity. Repeated unchanged evidence is coalesced into at most one summary per
minute. `--log-color=always` is the default even for redirected output; use
`--log-color=never` for plain text.

### Transcript capture

Capture is on by default, independently of console verbosity:

| Setting | Behavior |
|---|---|
| `--ai-log-dir=logs` | Local default, relative to working directory; repository `logs/` is gitignored |
| `--ai-log-dir=/var/log/kubescout/ai` | Checked-in Deployment path |
| `--ai-log=false` | Disables capture explicitly |
| Retention | `ai-transcript.log` plus `.1` through `.9`, at most 10 MiB each / 100 MiB total |

Newly created directories/files are private. Transcripts contain instructions,
model-visible context, actual request/tool rounds, normalized authorized tool
arguments, approved credential-protected results, model responses, validation and final
disposition. They exclude authentication metadata, raw objects, rejected raw
arguments and provider error bodies. Protect access: operational text may still
be sensitive. Unvalidated model content is not an accepted incident.

Files append across restarts and are not replayed as model memory. Rotation,
crashes and capture failures mean this is not a complete audit archive.
Capture failures warn and retry on the next record after a 30-second backoff;
recovery marks the gap. They do not stop investigations or change model budgets.

Locally, open `logs/ai-transcript.log` in your editor. The agent Pod uses a 128 MiB
disk-backed `emptyDir` at `/var/log/kubescout`, writable by UID/GID 65532.
Files survive container restart within the same Pod, **not Pod replacement**.
No persistent volume is provisioned.

The distroless image has no shell, `cat` or `tar`; ordinary `kubectl cp` and
`kubectl exec ... cat` do not work. If separately authorized, an ephemeral debug
container can mount the existing `ai-transcripts` volume read-only as UID 65532.
Use an approved debug image and a `kubectl debug --custom` profile containing:

```json
{
  "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "runAsNonRoot": true},
  "volumeMounts": [{"name": "ai-transcripts", "mountPath": "/var/log/kubescout", "readOnly": true}]
}
```

Read `/var/log/kubescout/ai/ai-transcript.log` from that helper. Adding a debug
container requires explicit cluster authorization; the agent grants no debugging
permissions and exposes no log HTTP endpoint.

## Troubleshooting

| Symptom | What to check |
|---|---|
| Invalid configuration | Required model settings/auth/observer ID, endpoint format, conflicting namespace filters; exit 2 |
| Bootstrap or collection failure | Selected context, namespace/CRD, supported API access and RBAC; missing observations are not healthy evidence |
| No incidents | Agent logs, scope, model availability and AI no-action; an empty CRD list is not a health check |
| Investigation deferred | Safe failure category, counters and next eligibility time in `[GO][SCHEDULE]` diagnostics |
| Local quota exhausted | Actual HTTP attempts, including tool rounds; `retry_not_before` indicates earliest request-slot expiry |
| Provider HTTP failure | Authentication (401/403), rate limit (429) or HTTP category; provider bodies are intentionally excluded |
| Tool limit reached | `local_tool_call_limit`, other `local_tool_*` or `local_cumulative_tool_bytes` categories; examine observed/allowed amounts |
| Existing incident not resolving | Watch mode, continuous fresh recovery, budgets and an AI resolution decision are required |
| Dashboard stale or failing | Cluster-scoped CRD/read permissions, ClusterRoleBinding and complete-snapshot limits |
| Dismiss/restore fails | Status-patch permission or stale displayed version; refresh before retrying |
| Capture incomplete | Writable transcript directory, storage capacity and capture-recovery messages |
| Image fails to start/pull | Architecture, registry access, rendered image tag, pull policy and kind import destination |

Failed investigations do not create incidents or resolve existing ones.
Observation continues while AI runs; stale decisions are discarded.
Short healthy phases in a crash loop do not establish continuous recovery.

Failure diagnostics include safe categories, model/tool counts and limits,
rolling-hour use, byte counts and timeouts. Watch backoff is 2s, 4s, then the
review interval. `next_retry_at` is scheduled eligibility, not a promise of a
request at that time; fresh evidence, serial work and budgets still matter.
Valid provider retry headers supply `retry_not_before`, with the latest hint
winning; invalid/missing hints are omitted. These hints do not change scheduler
backoff, and HTTP 429 alone does not identify which provider quota was exhausted.

| Exit | Once | Watch |
|---|---|---|
| 0 | Completed assessment, including no-action or open incidents | Graceful signal |
| 1 | Incomplete assessment or console failure | Bootstrap/unrecoverable console failure |
| 2 | Invalid configuration | Invalid configuration |

Recoverable watch failures remain diagnostics. Transcript failure alone does
not change the exit status. Once prints a readable summary, not a JSON envelope.
