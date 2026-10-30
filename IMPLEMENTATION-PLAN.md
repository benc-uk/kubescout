# Kubernetes health agent implementation plan

## Agreed redesign

The Python run-once/CronJob plan is superseded by a Go rewrite with near-real-time
resource observation. The runtime remains inside Kubernetes. Foundry supplies
model inference, not process hosting.

Decisions approved for this implementation:

- Microsoft Agent Framework Go, using the Responses provider and API keys.
- Watch by default, with optional `--once`.
- Informers for scoped Kubernetes resource state and a coalescing work queue.
- Five-second bounded batching and 15-second timed-rule evaluation.
- One analysis at a time, 60 attempts per rolling hour, explicit deferred and
  rate-limited output.
- JSON lines for initial state, finding changes, collection failures/recovery
  and asynchronous analysis results.
- Remove Python after Go validation. No compatibility runtime or line-by-line port.
- **Runtime and tests only**. No container, manifests, cloud provisioning,
  real model calls or live-cluster changes in this task.

The initial restriction above applied to the Go rewrite. Stage 2 was subsequently
authorised for context `kind-kind`, uniquely named test namespaces, at most 50
burst pods at a time, cleanup of owned fixtures, and up to four model requests.
Two real model requests were used in Stage 2. Stage 3 was subsequently authorised
for local container/in-cluster validation and a persistent deterministic-only
observer, with build assets under `build/` and Kubernetes assets under `deploy/`.
Azure provisioning and AKS deployment remain unauthorised.

## Stage 1: Go runtime and offline validation

- [x] Root Go module, checksums, CLI and Makefile build/test/check commands.
- [x] Explicit Kubernetes authentication and namespace selection.
- [x] Repository-root dotenv loading with environment precedence.
- [x] Scoped paginated reads and shared object admission limits.
- [x] Allow-listed projection before objects enter informer caches.
- [x] Shared informers and bounded/coalesced work scheduling.
- [x] Time-based re-evaluation independent of resource changes.
- [x] Cache failure/expiry handling, bounded watches and full cache recovery.
- [x] Independent deterministic node/pod/deployment/Job/event checks.
- [x] Stable finding identities, evidence fingerprints and resolution tracking.
- [x] Versioned schema 2.0 JSON lines and atomic optional latest-record file.
- [x] Embedded separate model instructions and Agent Framework Go integration.
- [x] Strict JSON/schema/reference/coverage validation and size/deadline limits.
- [x] Independent single model worker, rolling quota and latest-evidence deferral.
- [x] Suppress repeated analysis of unchanged evidence; discard stale advice.
- [x] `--once` reports and execution outcomes; graceful watch cancellation.
- [x] Unit and local-HTTP integration tests, including race-detector runs.

### Acceptance boundaries

Tests verify immediate image-pull failures, configured image evidence, startup
and rollout grace boundaries, timed evaluation without events, watch updates,
deletion, `410` recovery, namespace/verb restrictions, object limits, pagination
failures, model request shape, strict validation, no retry, non-blocking
detection, stale-analysis suppression and reporting failures.

These are offline tests. They do not establish live Kubernetes/Foundry
connectivity, image size, production memory usage or a production latency SLO.

The current implementation conservatively rebuilds all scoped caches after a
watch closes, including periodic watch renewal. This may emit a brief unknown
and recovery pair. It intentionally avoids trusting a previously synced cache
through a disconnected stream. Measure renewal cost before production use.

## Stage 2: Explicit local kind and model validation

Completed with explicit target, fixture, cleanup and model-call approval:

- [x] Verify the intended kubeconfig context and namespace selection.
- [x] Run read-only `--once` and watch modes against existing nodes and controlled workloads.
- [x] Create isolated test namespaces with controlled failure,
  recovery, rollout and Job fixtures. Never modify application workloads.
- [x] Measure detection latency during steady changes and event bursts.
- [x] Verify cache-loss and reconnect behaviour, including a silent connection.
- [x] Verify the object cap, development RSS limit and shutdown with 50 pods present.
- [x] Validate a real model response separately from deterministic-only runs.
- [x] Verify live model failure preserves health and reports analysis failure.
- [x] Remove only explicitly created test fixtures and verify namespace deletion.
- [x] Add repeatable opt-in Make targets and build-tagged integration tests.

### Stage 2 acceptance record: 25 October 2026

Target: `kind-kind`, four Ready Kubernetes 1.36.1 nodes. No existing application
workloads, node configuration or cluster networking were modified.
The observer executable remained unchanged; changes in this stage are the
live-test harness, developer commands, test dependencies and documentation.

Commands:

```bash
make integration CONTEXT=kind-kind
make integration-model CONTEXT=kind-kind ALLOW_MODEL=yes
make test
make check
go vet -tags=integration ./tests/cluster
```

The full deterministic suite passed. Its workload test was then rerun with the
stricter loaded-shutdown assertion; it also passed. The model suite passed with
two requests: one validated response and one deliberately invalid deployment.

| Check | Latest passing measurement | Gate |
|---|---|---|
| Image-pull failure after API confirmation | 4.829s | <= 10s |
| Image recovery after API confirmation | 2.098s | <= 10s |
| Failed Job after API confirmation | 4.198s | <= 10s |
| Timed Pending rule, resource version unchanged | Pod age 8.135s | 8s to 15s |
| First 50-pod burst, last create to report | 4.067s | <= 10s |
| Second 50-pod burst, last create to report | 4.577s | <= 10s |
| Object limit in once and watch | Unknown/incomplete, not healthy | Enforced |
| Peak observer RSS across acceptance runs | 31.3 MiB | < 256 MiB |
| Graceful shutdown with 50 burst pods present | 4ms | <= 5s |
| Broken connection to unknown | 1ms | <= 10s |
| Reconnection to complete collection | 6.102s | <= 15s |
| Silent-stream test change to unknown | 2.890s | <= 10s in accelerated test |
| Silent-stream recovery | 6.102s | <= 15s |
| Renewal unknown window | 6.105s | <= 15s |
| Real model success | One validated item for one finding | Passed |
| Real model failure | Exit 1, analysis failed, unhealthy findings retained | Passed |

Timing interpretation: container/Job measurements start at the test client's
first API confirmation, not the exact server-side transition. Matching reports
already emitted at that point add zero latency. The default 5s batching window
was retained. Test-only pod grace/check interval were 8s/2s, deployment grace 5s,
and fault-test watch lifetimes 6s/8s. Renewal produced ten list and ten watch
requests across two generations for the five-resource test scope.

Two harness issues were found and fixed during validation: recovery must allow
retained informational Warning events, and the authenticated test proxy must use
TLS because client-go does not send kubeconfig bearer credentials over plain
HTTP. No production runtime defect was found in these scenarios.

The current full-cache renewal strategy passed the development gate, with an
approximately six-second unknown window. This is a measured trade-off, not
zero-gap observation. Production-scale API cost and long-duration memory
behaviour remain Stage 4 work. Container and in-cluster ServiceAccount execution
were subsequently validated in Stage 3 below.

## Stage 3: Container and in-cluster Deployment

Completed on the explicitly approved `kind-kind` context:

- [x] Add a digest-pinned multi-stage Go build and minimal non-root runtime image.
- [x] Exclude `.env`, credentials, local outputs and development files from builds.
- [x] Validate the image locally and with a scoped, short-lived Kubernetes token.
- [x] Add a single-replica Deployment using `Recreate`, not a CronJob.
- [x] Add a dedicated service account and namespace-scoped list/watch permissions
  for pods/events/deployments/Jobs, plus node-only cluster permissions.
- [x] Inject model credentials at runtime in a temporary validation pod; commit no values.
- [x] Configure resource limits, read-only root filesystem and restricted security context.
- [x] Verify positive and negative RBAC, including denied Secrets/exec/writes.
- [x] Validate graceful SIGTERM and termination grace periods.
- [x] Verify pod-log output retrieval and document its non-durable retention.
- [x] Keep build files/binaries/archives in `build/`, Kubernetes assets in `deploy/`.
- [x] Leave the deterministic-only observer running in namespace `k8s-health`.

### Stage 3 acceptance record: 25 October 2026

This records the original Stage 3 validation. Observer probes were subsequently
removed at the user's request; see the scope adjustment below.

- Built a static Linux/amd64 binary using digest-pinned Go 1.26.10, with a
  digest-pinned distroless runtime. Image size is approximately 30.6 MB
  uncompressed. Runtime UID/GID is 65532.
- Audited the saved image layers: no source tree, module cache, `.env`,
  kubeconfig, model environment configuration or credentials.
- Successfully ran the image with networking disabled for CLI validation, then
  read-only against kind with verified TLS and a short-lived scoped
  ServiceAccount kubeconfig mounted from a private temporary directory.
- Imported the image to kind using an archive under `build/out/`. Repeated
  imports replace the archive atomically; Podman's refusal to overwrite an
  existing archive was found and fixed in the Make target.
- The validated manifest used one `Recreate` replica, non-root identity, RuntimeDefault
  seccomp, no privilege escalation, all capabilities dropped and a read-only
  root filesystem, with a 1 MiB progress volume (subsequently removed).
- In a temporary namespace, withheld the namespace RoleBinding: liveness passed
  while readiness failed. Granting it restored readiness without a container restart.
- Verified actual allowed node/scoped-pod reads and forbidden Secret/other-namespace
  reads under the ServiceAccount. Authorisation reviews additionally confirmed
  denied writes, pod logs and exec.
- Validated JSON assessment retrieval from observer pod logs.
- Made **one successful real model request from inside a pod**, injecting model
  settings through a temporary Secret. The application continued to authenticate
  to Kubernetes using its ServiceAccount, independently of model credentials.
- Scaled the test Deployment to zero and verified pod removal within 724ms on
  the final deterministic validation run, below the 15-second termination grace.
- Removed temporary test namespaces, node RBAC and model credentials.
- Left `Deployment/k8s-health` running in namespace `k8s-health`, watching
  `default` and nodes. It is deterministic-only, with no model Secret or ongoing
  model calls. At that point readiness required complete observation; the
  current deployment no longer has that probe.

Validation commands are documented in [README.md](README.md). Routine race tests,
static manifest-contract tests and `make check` passed. Stale/malformed progress
and state-write failures were tested offline; dependency-related readiness was
verified with live exec probes before probe removal.

Pod logs are retrievable but not durable retention. There is no central log
store, persistent finding history or restart-proof deduplication in this stage.
The local manifest uses an imported development tag and `imagePullPolicy: Never`;
production immutable-registry rollout remains Stage 4.

### Stage 3 scope adjustment: observer probes deferred

Removed the probe implementation, CLI/flag, runner progress recording,
startup/liveness/readiness settings and writable progress volume at the user's
request. Existing non-root/read-only settings, RBAC and resource limits remain.
The Deployment stays deterministic-only.

Deployment validation now checks incomplete and recovered assessment records
in pod logs instead of executing probes or treating Pod Ready as proof of
collection completeness. A crashed process is still restarted by Kubernetes;
a hung process requires manual detection/restart until production hardening.
Informer reconnects, time-based checks, unknown-health handling and model
scheduling are unchanged.

Validation after removal: targeted race tests, `make check`, image build and
deterministic `integration-deploy` passed. With namespace access withheld, the
observer emitted incomplete/unknown reports; restoring access produced complete
reports without a restart. The updated persistent Deployment has no probes or
progress volume and emits complete deterministic assessments with zero restarts.
No model calls were made, and temporary test resources were removed.

## Stage 4: AKS production readiness

Confirm target subscription/cluster, namespace scope, latency requirements,
model quota/data policy, identities, network reachability, registry and ownership
before provisioning or deployment.

- [ ] Pin a tested image digest and validate pull permissions.
- [ ] Select API-key handling or separately approve a Workload Identity migration.
- [ ] Validate private-cluster and model DNS/TLS/egress without weakening TLS.
- [ ] Measure resource usage and load behaviour at the intended cluster size.
- [ ] Revisit full-cache renewal and recovery policy using measured results.
- [ ] Configure central report retention and access controls.
- [ ] Add and validate observer startup/liveness/readiness checks based on actual
  evaluation progress and collection completeness, including stale progress,
  dependency recovery and state-write failures.
- [ ] Add external observer-health monitoring, independent of the monitored cluster.
- [ ] Decide whether single replica/restart duplication is acceptable. Leader
  election needs Lease writes and is outside the read-only design.
- [ ] Set operational alerts for incomplete collection, output/model failures,
  sustained rate limits and missing observer activity.
- [ ] Document rollout/rollback, credential rotation and failure investigation.

## Deferred features

- Teams/webhook/queue notifications and durable delivery.
- Durable finding history, cross-restart quota and deduplication.
- Leader election and high availability.
- Metrics, utilisation, restart rates, ownership correlation and broader checks.
- Model-driven investigation tools, logs, remediation or Foundry-hosted execution.

See [README.md](README.md) for runnable commands, precise rule semantics, output
schema, limits and current operational constraints.
