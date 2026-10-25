# Kubernetes health agent

A read-only Kubernetes health observer written in Go. It runs **watch mode by
default**, using client-go shared informers and a coalescing work queue. Pure
health rules produce deterministic findings. Microsoft Agent Framework Go
optionally analyses new or changed actionable findings without blocking detection.

This is a Kubernetes-hosted process, not a Foundry-hosted chat service. There is
no HTTP server, human input, conversation history, remediation or scheduler
dependency. `--once` supports manual snapshot assessments.

The Go runtime, live validation, container packaging and local Kubernetes
Deployment are implemented. Validation passed against kind (four Kubernetes
1.36.1 nodes), including a model call from a restricted in-cluster pod.
AKS deployment remains a separate stage in the
[implementation plan](IMPLEMENTATION-PLAN.md).

## Development

Requires **Go 1.26+** (the Agent Framework Go dependency requires 1.26).
Dependency versions and checksums are pinned in [go.mod](go.mod) and
[go.sum](go.sum). Python and uv are no longer used.

```bash
make help
make setup
make build
make test
make check
```

Routine tests use fixtures and local HTTP servers exercising the actual Kubernetes and
Agent Framework SDKs. They do not connect to a real cluster or model.
`make test` includes Go's race detector and requires a C compiler.

### Opt-in live integration tests

The tests in [internal/integration](internal/integration) are excluded from
routine tests by the `integration` build tag. They run the compiled observer
binary, not a substitute implementation.

```bash
# Authorises creation and deletion of isolated test fixtures on this context.
make integration CONTEXT=kind-kind ALLOW_WRITES=yes

# Separately authorises two real model requests, including an intentional failure.
make integration-model CONTEXT=kind-kind ALLOW_WRITES=yes ALLOW_MODEL=yes
```

Requirements: a reachable development cluster, explicit kubeconfig context,
permission to create/delete a namespace and its pods/deployments/Jobs, and
Linux `/proc` for memory measurement. The nodes must initially report healthy.
The cluster needs to pull `busybox:1.37.0`; a deliberately nonexistent tag
exercises image-pull failures. Model tests use the existing root `.env` or
injected environment configuration.

Each test creates a unique `k8s-health-stage2-*` namespace. Cleanup verifies its
UID and ownership label before deletion, then waits until it is gone. No existing
application workloads, nodes, cluster-wide configuration or RBAC are modified.
If the test process is forcibly killed, check the exact namespace/UID printed by
that run before manually cleaning up; never delete namespaces by a broad prefix.

The deterministic suite covers:

- Real `--once` and watch runs, image failure/recovery, image/tag evidence,
  deployment rollout findings and failed Jobs.
- Timed rules while the Pending pod's resource version remains unchanged.
- Two bursts of 50 scheduling-gated pods, with no more than 50 burst pods present
  at a time. These do not run containers or consume worker CPU/memory requests.
- Shared object limits in once/watch modes, RSS measurements and graceful
  shutdown while all 50 burst pods are still present.
- Observer-only disconnects, silent streams and renewal using a scoped,
  authenticated loopback TLS proxy. Upstream Kubernetes TLS verification is
  preserved. The proxy permits only GET requests for nodes and the test scope.

The model suite makes exactly two attempts: a real validated analysis followed
by an invalid-deployment request. The latter must retain unhealthy findings and
return exit 1 with failed analysis. It never enables watch-mode model calls.
These requests can incur model charges and send approved fixture/node findings
to the configured service.

| Development gate | Limit |
|---|---|
| State-change detection with the default five-second batch window | 10 seconds |
| Timed rule | Grace + check interval + 5 seconds |
| Recovery after connectivity returns | 15 seconds |
| Graceful shutdown | 5 seconds |
| Observer peak RSS during these tests | 256 MiB |

Container/Job latency is measured from the test client's first API confirmation;
already-emitted matching reports count as zero additional delay. Burst latency
is measured from the last create response. These are development measurements,
not precise server-side transition timestamps or production SLOs.

To keep tests bounded, pod grace/check interval are accelerated to 8s/2s for the
timed test, deployment rollout grace to 5s, and watch renewal to 6s/8s for fault
tests. The normal five-second batching window is retained. This is not a
long-duration soak test or a production-scale memory-leak assessment.

Latest passing results and their limits are recorded in the
[Stage 2 acceptance record](IMPLEMENTATION-PLAN.md#stage-2-acceptance-record-25-october-2026).

### Container build and local Deployment

Build assets and Kubernetes assets are deliberately separate:

| Location | Contents |
|---|---|
| [build/Dockerfile](build/Dockerfile) | Multi-stage Go build and minimal non-root runtime |
| [build/Dockerfile.dockerignore](build/Dockerfile.dockerignore) | Allow-listed build context, excluding credentials, tests and generated files |
| `build/bin/` | Generated local binary, ignored by Git |
| `build/out/` | Generated image archive, ignored by Git |
| [deploy/](deploy/) | Kustomize entry point, namespace, ServiceAccount, RBAC and Deployment |

The image uses digest-pinned Go 1.26.10 and distroless bases, with a static,
trimmed binary. Stage 3 validated Linux/amd64 using Podman and kind. Other
architectures require appropriate base-image digests and separate validation.
No source, `.env`, kubeconfig, Go module cache or model credentials are present
in the runtime layers.

```bash
make image
make kind-load KIND_CLUSTER=kind

# Validate the image and RBAC in a disposable namespace.
make integration-deploy CONTEXT=kind-kind ALLOW_WRITES=yes

# Optionally include one real in-cluster model request.
make integration-deploy CONTEXT=kind-kind ALLOW_WRITES=yes ALLOW_MODEL=yes

# Leave the deterministic-only observer running.
make deploy CONTEXT=kind-kind ALLOW_WRITES=yes
kubectl --context kind-kind -n k8s-health rollout status deployment/k8s-health
kubectl --context kind-kind -n k8s-health logs deployment/k8s-health --tail=20
```

The Makefile defaults to `CONTAINER_ENGINE=podman`, `KIND_PROVIDER=podman` and
`IMAGE=localhost/k8s-health:stage3`. Set `KIND` or `KUBECTL` to an executable
path if those commands are not on PATH. `kind-load` requires an explicit kind
cluster name and safely replaces an existing image archive.

The supplied manifests are a **local kind configuration**: one replica,
`Recreate`, runtime namespace `k8s-health`, observed namespace `default`, and
`imagePullPolicy: Never`. Import the image before deploying. If you change the
`IMAGE` used for build/import, update the manifest's image too. To change observed
namespaces, update both the arguments and the corresponding Roles/RoleBindings.
The root Makefile's `NAMESPACE` variable applies to local run commands, not to
these versioned manifests.

After rebuilding/reimporting the same development tag, restart the Deployment
to use the new image:

```bash
kubectl --context kind-kind -n k8s-health rollout restart deployment/k8s-health
```

The observer runs as UID/GID 65532, with a read-only root filesystem, all
capabilities dropped, privilege escalation disabled and RuntimeDefault seccomp.
There are no writable application volumes or progress-file writes.
Requests are 25m CPU/64 MiB; limits are 250m CPU/128 MiB. These are development
settings, not production sizing results.

The ServiceAccount can list/watch nodes and the selected namespaced resources.
It cannot read Secrets, pod logs, unselected namespaces, exec into pods or modify
resources. Kubernetes mounts its projected API token separately. The default
Deployment has **no model Secret and makes no model calls**.

`integration-deploy` tests the actual manifests, initially withholding the
namespace RoleBinding. It verifies unknown/incomplete assessment records, grants
access and waits for a complete assessment in pod logs without a restart.
It also validates permission denials,
pod-log report output, a local read-only container using a short-lived scoped
token, and graceful in-cluster termination. With `ALLOW_MODEL=yes`, it creates
a temporary Secret from the existing model configuration and runs one model
assessment in a one-shot pod. All temporary namespaces, node RBAC and model
credentials are removed afterwards. The harness reads only observer pod logs;
it does not exec into pods.

#### Observer monitoring (deferred)

Startup, liveness and readiness probes have been removed from this development
runtime and deferred to Stage 4 production hardening. The `probe` subcommand,
`--probe-file` option and progress volume are no longer provided.

Kubernetes still restarts an exited/crashed container. It does not automatically
detect an evaluation loop that hangs without exiting. Pod `Ready` now indicates
container readiness only, **not** complete observation. Use JSON assessment
records (`collection_complete`, `execution_status`, `health_status`) to verify
observation. Collection failures still produce `unknown` and trigger recovery;
informer watches, timed checks and model scheduling are unchanged.

Reports go to container stdout and can be retrieved from pod logs. Pod logs
are not durable report storage. Replacement,
eviction and log rotation can remove output. Deduplication and model quota reset
on process restart. Central retention and independent observer monitoring remain
Stage 4 work.

### Local runs

Select the intended context and namespace explicitly:

```bash
# Watch continuously, without model calls. Ctrl+C stops cleanly.
make run CONTEXT=kind-kind NAMESPACE=default

# Collect one snapshot and exit.
make once CONTEXT=kind-kind NAMESPACE=default

# Enable model analysis, using the same Kubernetes scope.
make run CONTEXT=kind-kind NAMESPACE=default ARGS=""

# Additional namespaces and an atomic latest-report file.
make run CONTEXT=kind-kind NAMESPACE=default \
  ARGS="--deterministic-only --namespace apps --report-file /tmp/k8s-health.json"
```

`make run` now watches instead of exiting. Both Make targets default to
`--deterministic-only`, and suppress command echo so stdout contains JSON only.

The binary can also be run directly:

```bash
./build/bin/k8s-health --auth-mode kubeconfig --context kind-kind \
  --namespace default --deterministic-only
./build/bin/k8s-health --help
```

There is no default context or all-namespaces shortcut. `--namespace` is
repeatable. `--kubeconfig /path/to/config` overrides the normal kubeconfig lookup.
Kubeconfig is never modified.

For in-cluster execution, select `--auth-mode in-cluster --namespace apps`.
That mode uses the service account and rejects `--context` and `--kubeconfig`.
Authentication never falls back to another mode.

### Model configuration

```dotenv
AZURE_OPENAI_ENDPOINT="https://<resource-name>.openai.azure.com/openai/v1/"
AZURE_OPENAI_API_KEY="<resource-api-key>"
AZURE_AI_MODEL_DEPLOYMENT_NAME="<deployment-name>"
```

Put these in the repository-root `.env` or inject them as environment variables.
Existing environment variables take precedence. The application locates the
checkout by its module declaration, starting from the working directory or
binary location. It loads only that checkout's root `.env`, never a nested
application `.env`. A standalone installed binary outside a checkout relies on
injected environment variables. Invalid `.env` files fail explicitly.

`FOUNDRY_MODEL_NAME` is supported as a fallback deployment name.
`--deterministic-only` needs no model configuration and never contacts the model.
Healthy assessments also skip analysis.

The model must support the **Responses API**. The application explicitly uses
Agent Framework Go's `openaiprovider.NewResponsesAgent`, with an API key and the
Azure OpenAI v1 endpoint. It does not create a server-side Foundry agent or need
Entra authentication. The framework is a pinned public-preview dependency;
upgrades must pass the HTTP contract tests.

The prompt is separate in [instructions.txt](internal/analysis/instructions.txt)
and embedded into the Go binary. Rebuild after editing it. Output validation is
enforced by application code, not trusted to the prompt.

Never commit credentials or put them in a build context. Image references,
resource names and reports are operational data and may reveal private
registry/repository names.

## Watch behaviour

```text
Explicit scoped lists -> allow-listed informer caches
                               |
Resource changes -> bounded batching/work queue -> pure health checks
Time-based checks -------------------------------^
                               |
                    finding changes -> JSON lines
                               |
           new/changed actionable findings -> single model worker
                               |
                 validated, correlated analysis result
```

- One informer for nodes, plus pods, deployments, Jobs and Warning events in
  each selected namespace. This watches resource state, not just Event objects.
- Initial lists are paginated and share one object-admission budget. Streaming
  initial lists are explicitly disabled so they cannot bypass that budget.
  Only projected health fields enter informer caches.
- Changes are batched for at most five seconds by default. Continuous changes
  cannot reset the deadline indefinitely. Timed rules run every 15 seconds,
  including when a Pending pod emits no new changes as its grace expires.
- Initial state, changes in finding severity/evidence, collection failures and
  collection recovery generate assessment reports. Unchanged assessments do
  not generate repeated reports.
- On a failed, closed, expired (`410`) or timed-out watch, the cache generation
  becomes incomplete immediately. Every scoped informer is cancelled and
  rebuilt from fresh lists. Initial `HasSynced` is not treated as proof that a
  disconnected cache is current.
- Watches are deliberately renewed by a full cache rebuild after five minutes
  by default. This bounds silent stale connections and also provides a full
  refresh. It can produce a brief `unknown`/recovery pair during normal renewal.
  Recovery retries back off from one second up to 30 seconds on repeated startup
  failures. Healthy periods reset the backoff.
- Empty node lists, admission-limit breaches, invalid list metadata, repeated
  pagination tokens and API errors produce **unknown**, never healthy. Last-known
  findings are retained but not resolved until collection is complete again.
- A snapshot across different resource caches is not an atomic cluster-wide
  transaction. Normal batching adds up to five seconds of detection latency;
  timed rules add up to one check interval. API failures/recovery add latency.

### Analysis scheduling

- One model request at a time, separate from detection. While it is busy,
  pending work is derived from the latest findings, not an unbounded event queue.
- New or materially changed critical/warning findings are batched together.
  Stable IDs identify findings; an evidence/severity fingerprint detects changes.
  Watch-mode calls analyse that pending subset, not every unchanged problem.
- The default limit is 60 attempts per rolling hour. Deferred and rate-limited
  states are explicit; pending current findings are reconsidered on timed checks.
  No model failure is reported as successful analysis.
- Each evidence revision is attempted once. Failed calls are not automatically
  retried indefinitely; changed evidence or a process restart permits another
  attempt. Input-limit failures count as attempts.
- A response is published against its originating assessment. If its findings
  changed/resolved or collection is incomplete, advice is discarded and the
  result is marked `superseded`.
- State, deduplication and quota are **in memory**. They reset on restart.
  Use one process/replica; high availability, leader election and durable
  notification delivery are not implemented.
- In `--once`, analysis is synchronous and covers all actionable findings in the
  snapshot, with informational findings available as supporting evidence.

Model requests use `store=false`, no sessions, no tools, no continuation, no
automatic retries, a timeout and bounded input/output. Raw response bodies are
bounded too. Validation rejects duplicate JSON keys, unexpected fields, blank
content, unknown finding references, missing actionable coverage, truncated and
empty responses. The model cannot set health or execute suggested checks.

## Reports

Watch mode emits **newline-delimited JSON** to stdout. Diagnostics go to stderr.
`--once` emits exactly one JSON report for valid configuration, including
execution failures when stdout remains writable.

Schema version **2.0** intentionally replaces the Python 1.0 contract:

| Field | Meaning |
|---|---|
| `schema_version`, `report_id` | Contract version and unique output record ID |
| `assessment_id` | Assessment identity, shared by its later analysis records |
| `type` | `assessment`, `analysis_started` or `analysis` |
| `observed_at`, `emitted_at` | Evidence observation and record publication timestamps |
| `cluster_identifier`, `namespaces` | Selected cluster and namespace scope |
| `collection_complete` | Whether the assessment used complete, synchronised collection |
| `execution_status` | `completed` or `incomplete`, separate from health |
| `health_status` | `healthy`, `unhealthy` or `unknown` |
| `findings` | IDs, rule IDs, severity, resource identity, summary and approved evidence |
| `changes` | `new`, `updated` and `resolved` finding IDs; empty on analysis records |
| `analysis` | Status, reason, items, and analysed `finding_ids` on result records |
| `errors` | Sanitised execution errors, never raw API/model errors |

Analysis states are `pending`, `deferred`, `rate_limited`, `completed`, `failed`,
`skipped` or `superseded`. A pending/deferred assessment's `completed` execution
means detection/reporting completed, **not** that analysis finished.

Only `assessment` records change current health. Analysis records retain the
originating snapshot and must be correlated using `assessment_id`, not mistaken
for a new observation. Finding IDs remain stable for a resource UID/rule/subject
within this implementation, but are not guaranteed to match the Python hashes.

`--report-file` atomically replaces a private (0600) file with the **latest
record**, not a history or necessarily the latest assessment. Relative paths
resolve from the current working directory (the repository root for Make).
The parent directory must exist. File failures are reflected on stdout.
Output failures are fatal; no records are silently dropped.

| Mode | Exit behaviour |
|---|---|
| `--once` | `0`: complete, even if unhealthy. `1`: collection/analysis/output incomplete. `2`: invalid configuration. |
| Watch | `0`: graceful SIGINT/SIGTERM. `1`: client initialisation or output failure. `2`: invalid configuration. Collection/model failures are reported while observation continues. |

Observer probes, notifications and durable storage are deferred. External
monitoring is required later to detect failure of this observer or its host cluster.

## Scope and permissions

Only `list` (`--once`) and `list,watch` (watch mode) are used:

- Cluster-wide core `nodes`.
- Core `pods` and `events`, apps `deployments`, batch `jobs`, only in explicitly
  selected namespaces. Events use `fieldSelector=type=Warning`.

There are no writes, Secret/ConfigMap reads, pod logs, exec or remediation.
Full objects arrive from the API but arbitrary metadata, environment variables,
event/condition messages and workload configuration are not retained.
Model evidence excludes Kubernetes UIDs and credentials.

Configured container images (including init containers) are explicitly retained.
The pod spec image takes precedence over status and appears as `image=...`.
Older retained events still consume the collection budget even if too old to
create findings. Empty namespaced lists do not prove the namespace exists.

## Health rules

| Area | Behaviour |
|---|---|
| Nodes | Missing/False/Unknown Ready is critical. Reported memory/disk/PID pressure other than False warns. |
| Pods | Failed is critical, Unknown warns. Pending, unscheduled and unready states warn after grace. Deleting and Succeeded pods are excluded. |
| Containers | Known waiting failures, including ErrImagePull/ImagePullBackOff, and current non-zero terminations warn immediately, including init containers. |
| Restarts | Cumulative threshold is informational; it does not imply a recent restart rate. |
| Deployments | Compare desired, available and updated replicas. Ignore scale-to-zero. Paused deployments still need availability but not updated replicas. |
| Rollouts | Use grace for recent progress. Ignore old-generation failure conditions until the current generation is observed. Explicit progress deadlines/replica failures are critical. |
| Jobs | Failed/FailureTarget=True is critical. Active retries and completed successful Jobs do not create Job findings. |
| Events | Recent Warning occurrences are informational evidence, not proof of current failure. Exclude missing/future/stale timestamps. |

Pod grace uses creation and relevant condition transition times. Explicit
container failures bypass grace. Missing timestamps do not suppress findings.
Deployment grace uses the latest creation/progress/unavailability timestamp,
which is a heuristic rather than a historical rollout monitor. Event recency
uses occurrence/series timestamps, never object creation as a substitute.
Only warning/critical findings make health unhealthy.

CPU/memory utilisation, trend analysis, broader storage/network checks and
ownership correlation remain out of scope.

## Configuration limits

Duration flags use Go units, for example `10s` or `5m`, not bare numbers.

| Option | Default |
|---|---|
| `--request-timeout` | `10s` |
| `--run-timeout` (once deadline / cache initialisation deadline) | `2m` |
| `--batch-window` | `5s` |
| `--check-interval` | `15s` |
| `--watch-timeout` | `5m` (plus request timeout as a local deadline) |
| `--max-objects` | `5000` shared |
| `--max-input-chars` | `20000` |
| `--max-output-tokens` | `1000` |
| `--model-calls-per-hour` | `60` |
| `--startup-grace` | `2m` |
| `--rollout-grace` | `5m` |
| `--event-lookback` | `1h` |
| `--restart-threshold` | `3` |

## Structure and extending checks

| Package | Responsibility |
|---|---|
| [cmd/k8s-health](cmd/k8s-health) | CLI, authentication bootstrap, signals and exit codes |
| [internal/config](internal/config) | Validated options and repository-root dotenv loading |
| [internal/kube](internal/kube) | Scoped paginated reads, projection, informer lifecycle and budget |
| [internal/health](internal/health) | Snapshot types, finding identities and pure rules |
| [internal/runner](internal/runner) | Work queue, timed evaluation, diffs and independent analysis scheduling |
| [internal/analysis](internal/analysis) | Agent Framework adapter, embedded prompt and strict validation |
| [internal/report](internal/report) | Versioned JSON and atomic file output |

Add a pure `func(health.Snapshot, health.Policy) []health.Finding` rule to the
`health.Checks` registry and add table-driven failure/transient tests. No entry
point or model changes are needed. Extend projection explicitly if a rule needs
new evidence, with allow-list tests and a permissions review.
