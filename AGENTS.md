# Coding agent instructions

## Project and scope

This is a Go Kubernetes health observer, not a Foundry-hosted chat service.
Watch mode is the default; `--once` performs a bounded manual assessment.
Use Go 1.26+ and pinned modules in [go.mod](go.mod) and [go.sum](go.sum).
The Python implementation has been replaced, not retained as an alternate runtime.

Runtime, offline/live tests, container packaging and a local in-cluster Deployment
are implemented through Stage 3. AKS deployment requires a separate request.
Follow [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md).

## Architecture

- [cmd/k8s-health](cmd/k8s-health): CLI, signals and exit codes.
- [internal/config](internal/config): validated options and root `.env` loading.
- [internal/kube](internal/kube): read-only pagination, projection, shared budget
  and scoped client-go shared informers.
- [internal/health](internal/health): allow-listed snapshots and pure rules in
  the `Checks` registry. Keep API calls, model clients and orchestration out.
- [internal/runner](internal/runner): coalescing work queue, timed checks, finding
  changes and independent model scheduling.
- [internal/analysis](internal/analysis): Microsoft Agent Framework Go Responses
  provider, bounded model call and strict analysis validation.
- [instructions.txt](internal/analysis/instructions.txt): embedded runtime prompt.
  Keep it separate from Go code; rebuild after changes.
- [internal/report](internal/report): schema 2.0 JSON lines and atomic file output.
- [build/](build/): Dockerfile, build-context allow-list, generated binaries under
  `build/bin/` and image archives under `build/out/`. Keep generated outputs ignored.
- [deploy/](deploy/): all Kubernetes manifests and deployment assets.

Do not add a human/chat interface, HTTP server, remediation or Foundry hosting.
The internal watch loop and timed re-evaluation are intentional. Do not replace
resource watches with Kubernetes Event objects alone.

## Development

```bash
make help
make setup
make build
make test
make check
```

Routine tests must remain offline. Use local HTTP fixtures for Kubernetes and model
protocol tests. `make test` includes the race detector. Preserve the Makefile's
awk-generated `##` help and emoji descriptions.

Live tests in [internal/integration](internal/integration) require the
`integration` build tag and explicit context/write authorisation. The
`integration-model` Make target additionally requires model authorisation and
makes two real requests. Test writes belong only in this harness, never in the
observer runtime. Use unique namespaces, verify ownership/UID before cleanup,
and do not interrupt cluster-wide networking to test observer recovery.
`integration-deploy` additionally requires an imported container image. Optional
model validation uses a temporary Secret and one real request. Clean only
test-owned namespaces and node RBAC. Never print Secret values.

Live commands must explicitly select the context/namespaces. `make run` watches,
`make once` exits. Both default to deterministic-only. Do not contact a real
model or create, modify or delete cluster resources without authorisation.
The local manifest leaves a deterministic-only observer in namespace `k8s-health`,
watching `default` plus nodes. Do not silently enable ongoing model calls.

## Safety and correctness

- Load `.env` only from this repository's root; existing environment wins.
  Never read or print credentials when investigating code.
- Use Microsoft Agent Framework Go `openaiprovider.NewResponsesAgent` with
  `AZURE_OPENAI_ENDPOINT` ending in `/openai/v1/`, `AZURE_OPENAI_API_KEY` and
  `AZURE_AI_MODEL_DEPLOYMENT_NAME` (`FOUNDRY_MODEL_NAME` fallback).
  Do not require Entra or Workload Identity until requested.
- Keep dependencies pinned; Agent Framework Go is preview. Test real request
  shapes against local HTTP fixtures after SDK upgrades.
- Select kubeconfig with an explicit context, or in-cluster authentication.
  Never silently fall back between modes.
- Kubernetes access is only `list/watch`: nodes and selected-namespace pods,
  deployments, Jobs and Warning events. No Secrets, logs, exec or writes.
- Strip raw objects before caching. Retain only approved fields, including
  configured image references. Never send raw objects, messages or UIDs to the model.
- Preserve shared object admission limits, pagination checks and deadlines.
  Incomplete or disconnected collection must produce unknown, never healthy.
  Initial informer HasSynced does not prove continuing freshness.
- Preserve last-known findings during collection failure, without false
  resolutions. Only fresh complete assessments may resolve findings.
- Keep detection independent of model latency. Bound model concurrency, input,
  output, attempts and pending work. Supersede stale advice.
- Require strict output schema, unique JSON keys, known finding references and
  actionable-finding coverage. Model analysis cannot change deterministic health.
- Keep model storage, tools, continuations and automatic retries disabled.
  Report model failures, deferrals and rate limits explicitly.
- Keep rules easily extensible and add boundary/transient fixtures with changes.
- Single replica is the current design. Deduplication/quota state is in memory;
  do not imply restart-proof delivery or high availability.
- Keep images non-root and credential-free. Use the build-context allow-list,
  pinned bases, read-only root filesystem, dropped capabilities and runtime credentials.
- Observer probes and progress-file writes are deferred to production hardening.
  Do not reintroduce them without a request. Pod Ready alone does not establish
  collection completeness; verify assessment records instead.

## Output contract

Watch emits schema 2.0 JSON lines for assessments and correlated analysis.
Only assessment records change current health. Analysis records refer to their
original assessment. Diagnostics belong on stderr.

`--once`: exit 0 for complete execution even if unhealthy, 1 for incomplete
collection/analysis/output, 2 for invalid configuration. Exactly one report for
valid invocations where stdout remains writable.

Watch: graceful signal exits 0, bootstrap/output failure exits 1, invalid
configuration exits 2. Collection/model errors are report records while
observation continues. Never claim a deterministic-only test validates model
connectivity.

If you are in VS Code, read the vscode-microsoft-foundry skill first when
working on Foundry integration. This runtime is not a hosted-agent deployment.
