# Coding agent instructions

## Product and scope

This is an **AI-first Kubernetes incident investigation agent**, not a
deterministic alerting service with optional AI commentary.

- Deterministic rules provide private evidence and scheduling hints only.
- Only validated AI decisions may raise, update, reopen or resolve an Incident.
- AI no-action is valid, including for critical deterministic signals.
- Never publish a baseline, raw finding, collection outage, model failure or
  tool transition as a user-facing incident.
- Deterministic-only mode has been removed completely. Tests use fixtures, not
  an alternate production observer mode.
- Watch is the default; `--once` is a bounded AI assessment.
- The agent process has no chat/UI or HTTP server. The optional standalone
  KubeScout dashboard is a separate service with human dismissal/restore, not part of the AI loop.
- No shell/kubectl agent tool, remediation, external sender, Pod logs,
  Foundry-hosted execution or AKS provisioning without a separate request.

See [README.md](README.md) for the project overview,
[ARCHITECTURE.md](ARCHITECTURE.md) for the AI-first design and
[the operations guide](docs/operations.md) for configuration and deployment.
Earlier deterministic report policies are not the current contract.

## Architecture

- [cmd/kubescout](cmd/kubescout): CLI, auth/bootstrap, signals, exit codes.
- [cmd/kubescout-dashboard](cmd/kubescout-dashboard): standalone dashboard CLI, explicit Kubernetes
  authentication and HTTP lifecycle; no AI or incident-spec writes.
- [internal/config](internal/config): validated options, scheduling/budget limits,
  namespace watch/ignore selection and repository-root `.env` loading;
  existing environment wins.
- [internal/kube](internal/kube): scoped lists/watches, approved projection,
  owner relationships, shared admission limits and collection freshness.
- [internal/health](internal/health): pure internal evidence rules and registry.
  Keep API calls, model clients and orchestration out.
- [internal/investigation](internal/investigation): typed scoped read-only tools,
  bounded/redacted operational text. No generic API or command executor.
- [internal/analysis](internal/analysis): Agent Framework Responses tool loop,
  request budgets, strict AI decision validation.
- [instructions.txt](internal/analysis/instructions.txt): embedded AI instructions;
  keep separate from Go and rebuild after changes.
- [internal/incident](internal/incident): stable correlation/identity, AI conclusion,
  lifecycle and restart-aware Kubernetes reconciliation.
- [internal/runner](internal/runner): independent collection and bounded AI work,
  cooldowns, no-action suppression, recovery review and stale-result rejection.
- [internal/output](internal/output): readable GO/AI activity logs and rotating
  diagnostic AI transcripts. Incident CRDs, never stdout or files, store incidents.
- [internal/dashboard](internal/dashboard): bounded single-namespace Incident
  reads, revision-scoped human triage and embedded HTML/CSS/native-ESM UI; no JavaScript build step.
- [build/](build/): separate pinned non-root agent/dashboard images, allow-listed
  contexts and ignored outputs.
- [deploy/](deploy/): active Incident schema and agent RBAC/Deployment.
  Legacy HealthReport schemas are no longer shipped; retain any existing records.
- [deploy/dashboard](deploy/dashboard): independent dashboard Deployment, Service
  and namespaced get/list plus status-patch RBAC; excluded from the agent bundle.

## AI and data safety

- Use Go 1.26+ and pinned modules. Prefer the existing Microsoft Agent Framework
  Go `openaiprovider.NewResponsesAgent`; its SDK defaults are not safety limits.
- Use HTTPS `AZURE_OPENAI_ENDPOINT` ending in `/openai/v1/`, API key and
  `AZURE_AI_MODEL_DEPLOYMENT_NAME`.
  Do not require Entra or Workload Identity until requested.
- One investigation at a time. Enforce both the rolling actual-HTTP-request
  quota and per-investigation request/tool/byte/deadline limits.
- Permit only typed approved local function tools. Keep provider storage,
  hosted tools, conversations, background continuations and HTTP retries off.
  Tool-loop history is ephemeral, not durable model memory.
- Treat tool results as untrusted evidence. Require exact JSON fields, unique
  keys, known citations/resource handles/incident IDs and valid lifecycle actions.
  Do not require per-warning advice coverage or forbid AI no-action.
- No raw objects, UIDs, credentials, annotations or environment variables may
  reach the model. Names, images, scheduling facts and bounded redacted messages
  are allowed. Redaction reduces risk; it cannot prove arbitrary text is secret-free.
- Capture approved model-visible context, instructions, normalized authorized
  tool arguments, redacted results and model responses in the bounded AI transcript.
  Do not log raw tool arguments/results, provider error bodies or credentials.
  Never read/print credentials when investigating code.

## Kubernetes and incident correctness

- Explicit kubeconfig or in-cluster authentication; no fallback between modes.
  Local Make runs/deploys use the current kubeconfig context, without overrides.
  Direct executable runs may use --context to override it.
- Workload access is read-only: nodes and watched-namespace Pods, Deployments,
  ReplicaSets, Jobs and Warning events. Tools are handle-scoped. No Secrets,
  ConfigMaps, Pod logs, exec, arbitrary URLs or workload writes.
- `--namespace-watch` / `NAMESPACE_WATCH` and `--namespace-ignore` /
  `NAMESPACE_IGNORE` are mutually exclusive comma-separated namespace lists.
  Watch is an allow-list; ignore is a deny-list; neither set watches all
  namespaces, including newly created ones. Nodes are always observed.
  Flags replace their corresponding environment list; the opposite list still
  applies and must be unset when switching modes.
- Explicit watch lists use namespace-scoped requests. All/ignore mode uses
  cluster-wide list/watch requests without Namespace-object discovery.
  Filter ignored objects before projection, cache admission and model input;
  ignore is not an RBAC boundary. Preserve shared limits across namespaces and
  apply the same scope to investigation tools, including node Warning events.
- Preserve bounded pagination/projection and continuing freshness checks.
  Initial HasSynced does not establish continuing freshness.
- Collection failure means unknown observation, never healthy evidence.
  Retain unresolved incidents; recovery needs continuous fresh observations and
  an AI decision. A brief running phase in a crash loop is not recovery.
- Group related evidence by verified controller/standalone identity. Do not use
  individual finding IDs, model prose hashes or per-run IDs as incident identity.
- Preserve incident identity across restarts; reread/reconcile ambiguous writes,
  use UID/resourceVersion preconditions and never create a second name on retry.
  Names use the `inc-` prefix. Choose a new observer ID when changing
  cluster/observation scope;
  never resolve out-of-scope stored incidents as if they had recovered.
- No-op AI conclusions, wording and counters alone must not update an incident.
- Incident lifecycle uses `state` in Go/JSON and the indexing label. Legacy
  `phase` records require explicit migration; do not silently fall back.
  Kubernetes-owned Pod `status.phase` is unchanged.
- Agent writes in either authentication mode are restricted to Incident create/patch in the incident
  namespace. No incident deletion or delivery-status writes.
- Both authentication modes persist AI-approved Incidents, including local
  `make run` and `make once`. There is no runtime incident simulation mode.
  Tests may inject fixture incident repositories; they are not alternate CLI modes.
  `make incidents-setup` installs only the incident namespace and CRD in the current
  context; it does not deploy the agent, provision model credentials or call AI.
- Single replica remains the design. Incident state is restart-aware; model
  quota/no-action state is process-local. Do not imply HA or exactly-once delivery.
- Keep old HealthReport records until explicit export/deletion approval. Never
  automatically convert deterministic history into AI incidents.

## Dashboard boundary

- Read only Incidents from one `--namespace` (default `kubescout`), across
  observers and lifecycle states. The dashboard must not read workloads, model
  credentials or tool evidence, call AI, or create/delete incidents or write their specs.
- Human dismissal/restore may patch only `status.triage` using the status subresource,
  with UID/resourceVersion preconditions and a version matching the displayed Incident.
  Dismissal applies to one lifecycle revision; material AI updates and recurrence
  restore visibility. The agent continues tracking dismissed Incidents unchanged.
  Browser writes require same-origin checks, a custom action header and bounded JSON.
  Status RBAC is broader than a single field; field ownership is enforced by the application.
- Workload-namespace filtering uses anchor and affected-resource namespaces; it
  must not change the incident-storage API scope. Details show the anchor's
  **Problem namespace**, or `Cluster-scoped` for nodes, not the storage namespace.
- Kubeconfig mode binds `127.0.0.1:8000`; in-cluster mode binds `:8000`.
  Authentication modes never fall back. There is no application login or TLS;
  keep deployed access within a trusted cluster/localhost port-forward. Anyone with
  access can dismiss/restore incidents using the dashboard's Kubernetes credentials.
- Preserve bounded complete reads, safe text rendering and explicit stale/error
  states. No stored incidents is not proof of cluster health or agent activity.
  Keep the dashboard image, ServiceAccount and deployment independent of the agent.

## Development and operation

### Go file headers

- Every repository-owned `.go` file, including tests and integration fixtures,
  must have a concise `/* ... */` header before its `package` declaration.
  Keep build constraints at the top with their required blank-line separation;
  preserve existing license notices and compiler directives.
- Use `Purpose:` and `Flow:` entries in the header. Explain what the file owns,
  how it participates in the system, and the relevant deterministic, AI or
  human-action boundary. For test files, describe the behavior covered and
  whether they use offline fixtures or explicitly opt-in live resources.
- Read the implementation before writing the header. Keep descriptions specific
  to the file, concise and factual; do not imply unsupported guarantees or copy
  generic package descriptions into every file.
- Add a header when creating a Go file. Whenever responsibilities, data flow,
  side effects, safety boundaries or test coverage change, update its header in
  the same change. Review headers on moved or renamed files as well.
- Treat header accuracy as part of completing a code change, not a one-time
  documentation exercise. Keep the related architecture documentation in sync
  when the overall flow changes. Avoid timestamps, author names and change logs
  in headers.

### Go function comments

- Document every named Go function and method, including unexported helpers and
  test functions, with a `//` comment immediately above its declaration.
- Use 1-2 concise lines explaining its purpose and important behavior; use up to
  3 lines for complex functions. Start with the function or method name and
  describe the actual contract, side effects or safety boundary rather than
  merely restating its name. Test comments explain what behavior is verified.
- Preserve useful existing explanations and compiler directives. Anonymous
  callbacks do not need routine comments; explain them only when their logic
  needs clarification.
- Keep function comments accurate in the same change as the implementation,
  including renames, changed behavior and updated test coverage.

### Go declaration order

- Group all file-level `type` declarations immediately after the imports and
  before other declarations. In files without imports, put them after the
  `package` clause. Keep each type's documentation attached to its declaration.
- Apply this layout to new Go files and preserve it when editing existing files,
  including test files. Keep function-local types inside their functions; do not
  widen their scope merely to follow file-level ordering.
- Preserve build constraints, file headers and compiler directives. Reordering
  types must not change the relative order of other declarations, especially
  variables with initialization side effects.

```bash
make help
make setup
make build
CGO_ENABLED=1 make test
make check
```

`make setup` also installs the pinned golangci-lint into ignored `build/bin`;
use `make lint-setup` to install it separately and `make lint` for lint-only
validation. `make check` includes linting. Keep `.golangci.yml`, the Makefile's
version pin and lint documentation aligned when changing tooling. Keep Go code
gofmt-formatted and follow `wsl_v5` whitespace checks: separate statement groups
with blank lines rather than packing unrelated operations together. No
line-length limit is enforced. Do not bypass
findings with blanket exclusions or `nolint` comments; keep any necessary
exception narrow and explained. Lint includes integration-tagged code but must
not execute live tests, contact the cluster or call the model.

Use `make dash-build` / `make dash-run` for the standalone local dashboard.
Its image/import/deploy/forward/cleanup targets use the `dash-` prefix; see
[the development guide](docs/development.md#make-reference) for prerequisites and variables.
Rebuild the relevant binary/image after changing embedded AI instructions or
dashboard assets.

Routine tests must remain offline and include local HTTP fixtures exercising the
actual SDK/tool/CR request shapes. Keep the Makefile's generated help and emoji
descriptions. Add no-action, coalescing, boundary, scope, failure and recovery
fixtures whenever changing behavior.

Live tests are opt-in under the `integration` build tag. Select contexts and
namespaces explicitly. Full model/container tests require model authorisation
and account for every HTTP attempt, including tool rounds. Collector-only tests
exercise APIs directly and do not claim AI connectivity.

Do not run live model calls or mutate the cluster without explicit permission.
The checked-in Deployment is inactive until model configuration and operation
are explicitly authorised. Never silently enable ongoing billable calls during
migration. Model Secrets are provisioned separately with standard Kubernetes
tooling, not a dedicated application command; never commit their values.
`make model-secret` applies the runtime namespace manifest, then creates the
Secret from `.env`; it does not overwrite an existing Secret or start the agent.
The Deployment's explicit namespace-filter environment entries override
same-named keys from the model Secret; configure in-cluster filters there.
Tests create only owned resources and verify namespace/RBAC UID before cleanup.
Do not interrupt cluster-wide networking to test recovery.

Keep non-root, read-only filesystem, dropped capabilities, pinned bases and
credential-free image context. The approved bounded `emptyDir` at
`/var/log/kubescout` is only for diagnostic AI transcripts; no PVC is provisioned.
No probes/progress volumes without a request.
Do not delete the incident namespace or CRD merely to stop the agent.

## Incident storage and logging

- Kubernetes Incident CRDs are the authoritative incident record. Stdout/stderr
  explain activity; AI transcript files capture diagnostic exchanges. Neither
  stores incidents or supplies the dashboard's incident data.
- Console output is human-readable text with explicit GO/AI origin, phase, level
  and correlation. GO covers deterministic evidence, dispatch, tool execution,
  validation and persistence; AI identifies model-selected tools and conclusions.
  Never label local failures or a no-candidate result as a model response.
- Normal progress and AI no-action go to stdout; warnings/errors go to stderr.
  Separate model proposals, Go validation and confirmed CRD writes. Coalesce
  unchanged watch activity; debug adds collection/check detail.
- `--once` logs progress and a readable completion summary, not a JSON envelope.
  Zero incident changes is valid, not a synthetic health conclusion.
- Schema 3.0 console envelopes and `--output-file` are removed. Do not revive a
  stdout incident API or file-export contract.
- AI capture defaults on, independently of console verbosity; `--ai-log-dir`
  selects the directory and `--ai-log=false` disables it. Retain at most ten
  10 MiB files. Capture actual HTTP attempts/tool rounds, validation and final
  disposition without changing requests, budgets or decisions. Capture failures
  warn explicitly, retry with bounded frequency and do not stop investigations.
  Protect credential exclusion and do not claim a complete audit archive.
- Once exit 0: completed execution regardless of incident severity; 1: incomplete;
  2: invalid configuration. Never silently fall back when model config is missing.
- Watch exit 0: graceful signal; 1: bootstrap/unrecoverable console failure;
  2: invalid config. Failed investigations create no incidents.
- See [operator logging guidance](docs/operations.md#logs-and-ai-transcripts)
  for defaults, file access and ephemeral-storage limits.

When changing Foundry integration in VS Code, consult the
vscode-microsoft-foundry skill when available. This process is not a hosted agent.
