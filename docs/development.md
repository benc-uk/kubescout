# Developing KubeScout

[Overview](../README.md) · [Architecture](../ARCHITECTURE.md) · [Operations](operations.md)

KubeScout has two Go executables: the investigation agent and the independent
dashboard. Read the [architecture guide](../ARCHITECTURE.md) for component
ownership and the boundary between evidence, AI decisions and human triage.

## Setup and validation

Requires Go **1.26+**, Make, module download access, and a C compiler for race
tests. Modules are pinned in [go.mod](../go.mod) and [go.sum](../go.sum).

```bash
make help
make setup
make build
CGO_ENABLED=1 make test
make check
```

`make setup` downloads/verifies modules and installs the pinned linter locally.
Routine tests use offline fixtures, not a cluster or model. They exercise actual
Kubernetes/Responses HTTP request shapes, tool rounds, decisions and incident
writes. Coverage includes no-action on critical evidence, scope/credential protection,
budgets, coalescing, failures, recovery, restart reconciliation and material updates.

Keep production behavior AI-first: fixtures are test repositories, not an
alternate runtime mode. See [repository coding conventions](../AGENTS.md) for Go
headers, function comments, declaration order and package boundaries.

### Formatting and linting

`make check` checks gofmt, module tidiness/integrity and lint diagnostics.
Run `make lint-setup` independently to install the linter, or `make lint` for
lint-only validation.

The [Makefile](../Makefile) pins **golangci-lint v2.15.0**, built with the selected
Go toolchain into ignored `build/bin/golangci-lint/v2.15.0/`. It is not a global
installation or runtime dependency. Reinstall after changing Go toolchains.

[.golangci.yml](../.golangci.yml) enables `govet`, `ineffassign`, Staticcheck's
`SA*` checks, `wsl_v5` whitespace checks and gofmt. Use blank lines between
statement groups; there is no line-length limit. Findings are not restricted to
new code. Broad unchecked-error enforcement is not enabled.

Lint includes tests and integration-tagged fixtures without executing them,
contacting Kubernetes or calling the model. Once cached, dependencies and the
linter need no network access. Keep the version pin, configuration and this
guide aligned; do not hide findings with blanket exclusions.

## Working on the agent

Build `build/bin/kubescout` with `make build`. For actual execution, follow
[local operation](operations.md#local-operation): model calls and cluster writes
require explicit authorization, even when running from a developer machine.

The AI instructions are embedded from
[instructions.txt](../internal/analysis/instructions.txt); rebuild the binary/image
after editing them. Changes to tool or decision contracts should include offline
fixtures through the real SDK/request path, not only isolated parser tests.
When changing behavior, cover no-action, scope/boundaries, coalescing, failure and
recovery as applicable.

## Working on the dashboard

```bash
make dash-build
make dash-run
```

The latter uses Kubernetes and serves http://127.0.0.1:8000; it is not an offline
fixture host. The dashboard embeds [HTML/CSS/native ESM](../internal/dashboard/web/)
with no Node, package manager, framework, CDN or JavaScript build step.
Rebuild its binary/image after asset changes.

Focused offline validation:

```bash
CGO_ENABLED=1 go test -race -timeout 120s ./cmd/kubescout-dashboard ./internal/dashboard ./tests/cluster
kubectl kustomize deploy/dashboard
```

The Kustomize command renders locally; it does not apply manifests.

### Browser fixtures

Start the opt-in, loopback-only browser regression host:

```bash
K8S_DASHBOARD_BROWSER=yes go test -count=1 -run '^TestBrowserFixture$' -v -timeout 10m ./internal/dashboard
```

Open http://127.0.0.1:8000/__test/browser and wait for `PASS` in the page title.
Open http://127.0.0.1:8000/ for fixture-backed visual and keyboard checks; stop
the host with Ctrl+C. If port 8000 is occupied, set
`K8S_DASHBOARD_BROWSER_PORT=0`; the fixture prints the selected loopback URL.

Use a desktop browser. The suite covers filtering, sorting, pagination, safe text
rendering (including one exact-copy native description per resource and partial/unavailable coverage),
details, stale/deleted/error states, response races, refresh, themes,
text sizes and contrast. Go fixtures also cover auth/context selection,
cluster-scoped Incident requests, workload namespaces and status-only triage writes. Browser fixtures are test-only, skipped by
routine runs and absent from production images.

## Container images

The [agent](../build/Dockerfile) and [dashboard](../build/Dockerfile.dashboard)
have separate pinned non-root images and credential-free allow-listed contexts.
Do not add `.env`, kubeconfigs, logs or local build outputs to those contexts.

```bash
make image
make dash-image
```

Defaults use Podman, target Linux and the host Go architecture. Override
`CONTAINER_ENGINE=docker` for Docker. Cross-compile for a specific architecture:

```bash
make image TARGETARCH=arm64 IMAGE=ghcr.io/your-org/kubescout:arm64
podman push ghcr.io/your-org/kubescout:arm64
```

Use your own authorized registry path. The Go builder runs on the host platform;
the runtime base is selected for the target. Each build produces one image, not
a multi-platform manifest. Dashboard builds accept the same target overrides
with their own `DASHBOARD_IMAGE`.

Build/import variables do not update deployment manifests. See
[deployment](operations.md#kubernetes-deployment) for registry selection,
pull policies and the [kind workflow](operations.md#local-kind-alternative).
Image rebuild/import does not restart a running Pod.

## Opt-in live tests

These tests contact a real cluster and create/delete owned resources. Choose the
context explicitly and use a development cluster where the required operations
are authorized. Collector tests do not call AI; model/container tests require
model settings and explicit authorization for **at most six model HTTP requests**,
including tool rounds.

```bash
make integration CONTEXT=kind-kind
make integration-model CONTEXT=kind-kind ALLOW_MODEL=yes
make integration-deploy CONTEXT=kind-kind ALLOW_MODEL=yes
```

- `integration` exercises collector/evidence APIs directly. Its RSS measurements
  describe the test process, not standalone agent memory.
- `integration-model` uses live collection/AI with a test-only incident repository;
  it does not write live Incidents.
- `integration-deploy` uses a bounded in-cluster AI Pod with temporary owned
  namespace, Secret and RBAC. Import the intended `IMAGE` and install the Incident
  CRD beforehand; this target does not build/import the image.

The harness uses the `integration` build tag and uniquely named namespaces,
not the local namespace filters. Credentials need permissions to create and
clean up its fixtures; container validation also manages owned Node/Incident RBAC
and cluster-scoped Incidents. Namespace removal stops the fixture agent before
explicit incident cleanup, which verifies observer ownership and uses
UID/resourceVersion preconditions. AI no-action is valid: tests do not require a
manufactured incident. Routine offline checks authorize none of these operations.

### Failure-injection fixtures

The [failure-injection script](../scripts/inject-failures.sh) creates optional
development failures in the current kubeconfig context:

```bash
kubectl config current-context
bash scripts/inject-failures.sh your-test-namespace
```

Choose a namespace watched by the agent. The script offers owned cleanup;
inspect prompts before authorizing resource creation/deletion. Option 7 creates
an OOM fixture: a Python container allocates 128 MiB against a 64 MiB limit, with
CPU limited to 100m and `restartPolicy: Never` to retain the termination.
Image download and working memory-limit enforcement are required; inspect Pod
status to confirm `OOMKilled`. Option 5 creates all five failures; option 6
removes matching fixtures after confirmation. An AI incident is not guaranteed.

## Make reference

Run commands from the repository root. `make` without a target displays help;
it never starts the agent. Detailed operational procedures live in the
[operations guide](operations.md).

### Targets

| Target | Purpose and prerequisites |
|---|---|
| `help` | Generated command list and configuration reminders |
| `setup` | Download/verify modules and install the pinned linter |
| `lint-setup` | Install the pinned linter into ignored `build/bin`; requires module access |
| `lint` | Lint Go, tests and integration fixtures without running them |
| `build` | Build `build/bin/kubescout`; do not run it |
| `test` | Offline race tests; requires C compiler and CGO |
| `check` | Check formatting, modules and lint; no live tests |
| `run` | Rebuild/watch using kubeconfig; needs model configuration, CRD, permissions, scope/identity and `ALLOW_MODEL=yes`; persists Incidents |
| `once` | Same as `run`, for one bounded assessment |
| `incidents-setup` | Install namespace/CRD and wait for establishment; no RBAC, Secret, agent or model calls |
| `model-secret` | Ensure namespace and create Secret from `.env`; does not overwrite an existing Secret |
| `image` | Build the agent `IMAGE`; does not import/deploy |
| `kind-load` | Save/import existing `IMAGE` into `KIND_CLUSTER`; does not deploy |
| `deploy` | Check Secret, apply manifests and scale agent to one; requires image availability and `ALLOW_MODEL=yes` |
| `undeploy` | Remove agent Deployment/RBAC; retain incidents, Secret, namespace and CRD |
| `dash-build` | Build `build/bin/kubescout-dashboard` with embedded assets |
| `dash-run` | Build/serve local dashboard using current context and cluster-scoped Incidents; no model calls, but human triage writes status |
| `dash-image` | Build separate `DASHBOARD_IMAGE` |
| `dash-kind-load` | Save/import existing dashboard image; does not deploy |
| `dash-deploy` | Apply dashboard only; requires namespace, CRD and image |
| `dash-forward` | Forward localhost:8000 to dashboard Service in `kubescout` |
| `dash-undeploy` | Remove dashboard Deployment/Service/RBAC; retain agent and shared resources |
| `integration` | Live collector/connectivity tests with owned fixtures; requires `CONTEXT` |
| `integration-model` | Live AI test; `CONTEXT`, model configuration and `ALLOW_MODEL=yes`; at most six requests |
| `integration-deploy` | Live AI container test; same opt-ins plus imported `IMAGE` and installed CRD |

### Variables

Make accepts command-line overrides and inherited environment values; command-line
values win. **Make does not load `.env` into its variables.** The agent loads it
for local runs; `model-secret` passes it directly to kubectl.

| Variable | Default | Scope |
|---|---|---|
| `GO` | `go` | Go executable for builds, setup, checks and tests |
| `CONTEXT` | Empty | Required for live tests only; not local run/deploy/import |
| `NAMESPACE_WATCH` | Empty | `run`/`once` allow-list; also an agent environment variable |
| `NAMESPACE_IGNORE` | Empty | `run`/`once` deny-list; mutually exclusive with watch; also an agent environment variable |
| `OBSERVER_ID` | `local-dev-<username>` | `run`/`once`; choose stable cluster/scope-specific identity |
| `ALLOW_MODEL` | Empty | Must be `yes` for model-calling Make targets; not a runtime setting |
| `ARGS` | Empty | Extra `run`/`once` flags, not credentials or deployment configuration |
| `CONTAINER_ENGINE` | `podman` | Image build/save executable |
| `IMAGE` | `localhost/kubescout:dev` | Agent build/import/container-test image; not manifest substitution |
| `DASHBOARD_IMAGE` | `localhost/kubescout-dashboard:dev` | Dashboard build/import image; not manifest substitution |
| `TARGETOS` | `linux` | Image target OS |
| `TARGETARCH` | Host Go architecture | Agent/dashboard image target architecture |
| `KIND` | `kind` | kind executable |
| `KIND_CLUSTER` | `kind` | Image-import destination, separate from kubeconfig context |
| `KIND_PROVIDER` | `podman` | kind provider; must match cluster creation provider |
| `KUBECTL` | `kubectl` | Kubernetes setup/deploy/forward/cleanup executable |

For live model tests, Make forwards `ALLOW_MODEL` as `KUBESCOUT_LIVE_MODEL` for
the harness's additional opt-in. Runtime budgets are independent of this gate.
Local/deployment/dashboard operations use the current kubeconfig context;
integration tests require `CONTEXT` explicitly.
