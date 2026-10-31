# Coding agent instructions

## Product and scope

This is an **AI-first Kubernetes incident investigation agent**, not a
deterministic alerting service with optional AI commentary.

- Deterministic rules provide private evidence and scheduling hints only.
- Only validated AI decisions may raise, update, reopen or resolve a ClusterIssue.
- AI no-action is valid, including for critical deterministic signals.
- Never publish a baseline, raw finding, collection outage, model failure or
  tool transition as a user-facing incident.
- Deterministic-only mode has been removed completely. Tests use fixtures, not
  an alternate production observer mode.
- Watch is the default; `--once` is a bounded AI assessment.
- The agent process has no chat/UI or HTTP server. The optional standalone
  `issue-dash` dashboard is a separate read-only service, not part of the AI loop.
- No shell/kubectl agent tool, remediation, external sender, Pod logs,
  Foundry-hosted execution or AKS provisioning without a separate request.

See [README.md](README.md) for the current architecture and
[IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md) for historical decisions and
the superseding AI-first design. Earlier deterministic report policies are not
the current contract.

## Architecture

- [cmd/k8s-health](cmd/k8s-health): CLI, auth/bootstrap, signals, exit codes.
- [cmd/issue-dash](cmd/issue-dash): standalone dashboard CLI, explicit Kubernetes
  authentication and HTTP lifecycle; no AI or incident writes.
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
  lifecycle, restart-aware reconciliation and kubeconfig simulation.
- [internal/runner](internal/runner): independent collection and bounded AI work,
  cooldowns, no-action suppression, recovery review and stale-result rejection.
- [internal/report](internal/report): schema 3.0 incident/execution output and
  separate stderr diagnostics, not a deterministic assessment publisher.
- [internal/dashboard](internal/dashboard): bounded single-namespace ClusterIssue
  reads and embedded HTML/CSS/native-ESM UI; no JavaScript build step.
- [build/](build/): separate pinned non-root agent/dashboard images, allow-listed
  contexts and ignored outputs.
- [deploy/](deploy/): active ClusterIssue schema and agent RBAC/Deployment.
  Legacy HealthReport schemas are no longer shipped; retain any existing records.
- [deploy/dashboard](deploy/dashboard): independent dashboard Deployment, Service
  and namespaced read-only RBAC; excluded from the agent bundle.

## AI and data safety

- Use Go 1.26+ and pinned modules. Prefer the existing Microsoft Agent Framework
  Go `openaiprovider.NewResponsesAgent`; its SDK defaults are not safety limits.
- Use HTTPS `AZURE_OPENAI_ENDPOINT` ending in `/openai/v1/`, API key and
  `AZURE_AI_MODEL_DEPLOYMENT_NAME` (`FOUNDRY_MODEL_NAME` fallback).
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
- Do not log raw tool arguments/results, model error bodies or credentials.
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
  applies and must be unset when switching modes. `--namespace` is a deprecated
  repeatable watch-list alias.
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
  New names use `issue-`; existing `incident-` ClusterIssue names remain supported
  in place. Choose a new observer ID when changing cluster/observation scope;
  never resolve out-of-scope stored incidents as if they had recovered.
- No-op AI conclusions, wording and counters alone must not update an incident.
- In-cluster writes are restricted to ClusterIssue create/patch in the incident
  namespace. No incident deletion or delivery-status writes.
- Kubeconfig mode runs AI but only simulates incident operations in memory.
- Single replica remains the design. Incident state is restart-aware; model
  quota/no-action state is process-local. Do not imply HA or exactly-once delivery.
- Keep old HealthReport records until explicit export/deletion approval. Never
  automatically convert deterministic history into AI incidents.

## Dashboard boundary

- Read only ClusterIssues from one `--namespace` (default `k8s-health`), across
  observers and lifecycle phases. The dashboard must not read workloads, model
  credentials or tool evidence, call AI, or write/delete incidents.
- Workload-namespace filtering uses anchor and affected-resource namespaces; it
  must not change the issue-storage API scope. Details show the anchor's
  **Problem namespace**, or `Cluster-scoped` for nodes, not the storage namespace.
- Kubeconfig mode binds `127.0.0.1:8000`; in-cluster mode binds `:8000`.
  Authentication modes never fall back. There is no application login or TLS;
  keep deployed access within a trusted cluster/localhost port-forward.
- Preserve bounded complete reads, safe text rendering and explicit stale/error
  states. No stored issues is not proof of cluster health or agent activity.
  Keep the dashboard image, ServiceAccount and deployment independent of the agent.

## Development and operation

```bash
make help
make setup
make build
CGO_ENABLED=1 make test
make check
```

Use `make dash-build` / `make dash-run` for the standalone local dashboard.
Its image/import/deploy/forward/cleanup targets use the `dash-` prefix; see
[README.md](README.md#make-targets-and-variables) for prerequisites and variables.
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
credential-free image context. No probes/progress volumes without a request.
Do not delete the incident namespace or CRD merely to stop the agent.

## Output contract

- Watch stdout contains only accepted material incident changes.
- Diagnostics, no-action reasons, collection/tool/model failures and quotas belong
  on stderr, never as synthetic incident records.
- `--once`: exactly one schema 3.0 execution envelope while stdout is writable;
  an empty changes array is a valid no-action result.
- Once exit 0: completed execution regardless of incident severity; 1: incomplete;
  2: invalid configuration. Never silently fall back when model config is missing.
- Watch exit 0: graceful signal; 1: bootstrap/unrecoverable output failure;
  2: invalid config. Failed investigations publish nothing.

When changing Foundry integration in VS Code, consult the
vscode-microsoft-foundry skill when available. This process is not a hosted agent.
