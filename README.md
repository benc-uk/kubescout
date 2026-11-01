# KubeScout

**Your AI Kubernetes investigator.**

<img src="internal/dashboard/web/logo.png" alt="KubeScout logo" width="180">

KubeScout is an AI agent for Kubernetes incident investigation. It investigates
cluster issues and provides actionable insights.

The existing `k8s-health` namespace and `health.k8s-ops-agent.io` ClusterIssue
API group are retained for compatibility; this rebrand does not migrate stored
incidents or the agent's Kubernetes resource identities. Dashboard resources now
use `kubescout-dashboard`; see the deployment note below.

Deterministic rules are internal evidence producers. They neither decide what
deserves user attention nor create Kubernetes incidents. The AI can investigate,
correlate observations, conclude **no action**, raise a problem, update a known
problem or confirm recovery.

For a step-by-step table linking each stage to its code and separating
deterministic logic from AI decisions, see
[From Kubernetes changes to incidents](ARCHITECTURE.md).

```text
Scoped Kubernetes informers and pure evidence rules
                 |
Correlated candidates + known incidents + bounded scheduling
                 |
AI investigation <-> allow-listed read-only Kubernetes tools
                 |
Validated no-action / raise / update / resolve decision
                 |
ClusterIssue reconciliation (or kubeconfig simulation)
                 |
Future external notification component -- not implemented
```

There is no chat server, remediation, arbitrary kubectl/shell tool, Pod-log access,
Foundry-hosted process or external sender. Watch is the default; `--once` performs
a bounded manual AI assessment. **Deterministic-only mode has been removed.**

An optional [ClusterIssue dashboard](#clusterissue-dashboard) runs as a separate
Go service with human dismissal/restore. It does not add an HTTP server to the agent
or call AI, and cannot change AI conclusions or lifecycle states.

## Development and validation

Requires Go 1.26+ and the pinned modules in [go.mod](go.mod) and [go.sum](go.sum).

```bash
make help
make setup
make build
CGO_ENABLED=1 make test
make check
```

Routine tests are offline: local Kubernetes/Responses HTTP fixtures exercise the
actual SDK, function-tool round trips, final decisions and incident API writes.
They also verify no-action on critical evidence, redaction/scope limits, model
budgets, restart reconciliation, meaningful updates and recovery scheduling.
Race tests require a C compiler.

### Go linting

Run `make lint-setup` once (also included in `make setup`), then `make lint`.
`make check` includes linting alongside formatting and module checks.

The Makefile pins **golangci-lint v2.15.0**, built with the selected Go toolchain
into ignored `build/bin/golangci-lint/v2.15.0/`; no global installation or runtime
module dependency is added. Installation requires module download access.
Once dependencies and the linter are cached, linting needs no cluster or model
connection and does not run tests or apply fixes.

[.golangci.yml](.golangci.yml) enables `govet`, `ineffassign`, Staticcheck's
`SA*` correctness checks, `wsl_v5` whitespace checks and `gofmt` formatting checks
across production code, tests and integration-tagged fixtures. The maintained
`wsl_v5` linter separates statement groups with blank lines for readability.
No line-length limit is enforced.
Broad unchecked-error enforcement is not enabled.
All findings from the enabled checks are reported, without a new-issues-only
baseline. Keep the version pin and configuration in sync when upgrading; rerun
`make lint-setup` after changing Go toolchains.

The collector observes nodes and watched-namespace Pods, Deployments,
ReplicaSets, StatefulSets, DaemonSets, Services, EndpointSlices, Jobs and Warning
events. It retains an allow-listed projection,
enforces shared object limits and pagination, rebuilds disconnected informer
caches and re-evaluates time-based rules independently of AI latency.
ReplicaSets provide owner relationships; they are not another alert category.

StatefulSet and DaemonSet evidence includes observed generations, ready/available
replicas and update progress, with OnDelete and StatefulSet rolling-update
partitions respected. Replica Pod evidence is grouped by verified controller
ownership. New-controller shortfalls use the rollout grace; these APIs do not
provide a Deployment-style progress deadline, so a shortfall is not proof of a
stalled rollout.

Selector-managed Services get private evidence when no ready EndpointSlice
backends are observed, informational during startup grace and warning thereafter.
Ready backends that lack an EndpointSlice port matching a Service port's name and
protocol also produce private evidence, such as an unresolved named target port.
ExternalName and selectorless Services are exempt. EndpointSlice service-name
labels establish same-namespace routing context; controller owner UIDs, when
present, must match the current Service. Matching Pods and their verified owners
are available as scoped handles. Endpoint readiness follows Kubernetes semantics
(omitted `ready` and `serving` conditions are interpreted as true, and omitted
`terminating` as false); serving/terminating
conditions and `publishNotReadyAddresses` remain context for AI interpretation.
This does **not** establish DNS, connectivity, correct application ports or
successful traffic. AI no-action remains valid for all of this evidence.

Selectors/Pod labels are retained privately for matching, not sent to AI.
Their projections and port lists are bounded to 128 entries each; EndpointSlice
projections retain at most 1,000 endpoint readiness/target records and no endpoint
addresses. All new resource types share the existing namespace scope, pagination,
object admission, freshness and investigation budgets.

Before rolling out a rebuilt agent with this coverage, update the ClusterIssue
CRD for `service-availability` and the collector RBAC for the new resource types.
Missing API access makes collection incomplete; it never means those resources
are healthy. These repository changes do not activate or restart the agent.

## ClusterIssue dashboard

The [KubeScout dashboard host](cmd/issue-dash/main.go) embeds a small
[HTML/CSS/native-ESM interface](internal/dashboard/web/) and listens on **port
8000**, designed for desktop browsers. There is no Node, JavaScript package manager,
framework, CDN or frontend build step. It uses the existing pinned Go dependencies.
Rebuild the binary/image after editing the embedded assets.

- Shows undismissed open issues by default, highest severity then most recently updated.
- Uses the Signal design: a graphite incident desk with orange accents, a compact
  severity strip and a flat, readable table. A light theme is also available.
  The initial theme follows the
  system preference; an explicit choice is remembered in the browser.
- Severity badges use both color and text: critical/red, high/orange,
  medium/amber and low/blue. Resolved is a separate state, not a severity.
- Search by title, issue name, observer or affected resource. Combine state,
  severity, problem-family and workload-namespace filters; sort by severity,
  title, raised time or updated time. Results display 50 per page.
- Click anywhere in an issue row (or activate its title button with the keyboard)
  for explanation, diagnosis, recommendations, uncertainty,
  affected resources, problem namespace, lifecycle and model provenance.
  Problem namespace comes from the incident's anchor resource, not its storage
  namespace; node problems display `Cluster-scoped`. Evidence IDs are references
  only: the dashboard cannot fetch raw evidence, logs or revision history.
  Click outside the details sidebar or use its Close button to close it.
- **Dismiss issue** in the details panel hides the current revision from the default
  list and open severity counts. Select **State: Dismissed** to view dismissed
  issues (whether open or resolved) and use **Restore issue** to show them again.
  **Open** and **Resolved** exclude dismissed issues; **All states** includes
  everything. This combined UI filter does not change the stored AI lifecycle.
  Dismissal is shared across browsers/users and persists across dashboard restarts.
  A material AI update, resolution or recurrence advances the lifecycle revision,
  automatically clearing the effective dismissal; unchanged refreshes do not.
  Dismissal is not resolution or agent suppression: the agent continues investigating.
- Refreshes every 15 seconds while visible, with a manual refresh button.
  Refresh failures retain any last successful list/details with a **stale**
  warning. Missing/deleted issues and initial-load errors are explicit.

The server reads all observers' open and resolved ClusterIssues in **one
issue-storage namespace**, configured with `--namespace` (default `k8s-health`).
The workload-namespace filter only filters those issues' affected resources; it
does not grant access to other issue namespaces or workloads.

Reads use either in-cluster authentication or the current kubeconfig context
(with an optional override), with no automatic fallback between modes, `.env` loading or model
credentials. Each collection has a 10-second deadline and
100-record Kubernetes pages. Snapshots are limited to **1,000 stored issues**
(including resolved issues); upstream responses and encoded API responses are
limited to **8 MiB**. Oversized, malformed or incomplete snapshots fail explicitly,
not as partial lists. Larger histories need explicit external archival or a future
pagination design; the dashboard never deletes records.

**No stored issues does not establish cluster health or prove the agent is
currently observing.** This is a view of durable conclusions, not a health probe.

### Run locally, outside the cluster

Build and run **`kubescout-dashboard`** using your kubeconfig:

```bash
make dash-build
./build/bin/kubescout-dashboard --auth-mode=kubeconfig --namespace=k8s-health
# Open http://127.0.0.1:8000
```

Or build and run with one Make target:

```bash
make dash-run
```

Kubeconfig mode uses the current context by default. Direct executable runs
can override it with `--context=kind-kind`; Make targets use the current context.
Add `--kubeconfig=/path/to/config` for a specific file, or
use the standard `KUBECONFIG` environment variable/default kubeconfig location.
The issue namespace comes from `--namespace`, not the context's namespace.
Your selected credentials need `get`/`list` access to ClusterIssues there.
Dismiss/restore additionally needs `patch` on `clusterissues/status` there.
These are real Kubernetes writes in kubeconfig mode, not the agent's simulated writes.
Read-only credentials still allow browsing; dismissal failures are shown explicitly.

Local mode binds only **`127.0.0.1:8000`** because there is no app login.
No dashboard Deployment, image or port-forward is needed for local operation.
The cluster must already have the ClusterIssue CRD and issues to display;
running the dashboard does not start the agent.

### Independent deployment

The [dashboard manifests](deploy/dashboard/) are deliberately excluded from the
[agent bundle](deploy/kustomization.yaml). They create a separate Deployment,
ClusterIP Service and ServiceAccount with a namespaced Role allowing **`get` and
`list` on ClusterIssues, plus `patch` on `clusterissues/status`**. The image is non-root; the container uses a
read-only root filesystem and drops all capabilities. No Secret is mounted.
The container executable is `/kubescout-dashboard`; its image is
`localhost/kubescout-dashboard:dev`. The Deployment, Service, ServiceAccount,
Role and RoleBinding use `kubescout-dashboard`. Applying these manifests creates
new dashboard resources; it does not rename or remove an existing `k8s-dashboard`
installation. After explicitly deploying and verifying the new dashboard, retire
the old dashboard resources separately. `make dash-forward` and
`make dash-undeploy` target only the new resources.
The Deployment explicitly selects `--auth-mode=in-cluster`
(also the executable default), which listens on `:8000` and uses its ServiceAccount.
Kubeconfig flags are rejected in in-cluster mode.

Prerequisites: the existing `k8s-health` namespace and ClusterIssue CRD must
already be installed. The default local-kind manifests use
`localhost/kubescout-dashboard:dev` with `imagePullPolicy: Never`, so import the image
before applying them. These commands do not start or scale the agent:

```bash
make dash-build
make dash-image
make dash-kind-load KIND_CLUSTER=kind
make dash-deploy
make dash-forward
# Open http://127.0.0.1:8000
```

For an existing installation, apply the updated `deploy/clusterissues-crd.yaml`
and dashboard RBAC, and rebuild/import the dashboard image before rolling it out.
No incident migration or agent change is needed for dismissal.

The only write route is `POST /api/issues/{name}/triage`, with a bounded JSON
body containing the displayed issue's opaque `version` and a `dismissed` boolean.
It requires `X-KubeScout-Action: triage` and same-origin browser requests.
Writes use UID/resourceVersion preconditions and patch only `status.triage`;
stale requests fail rather than dismissing a newer revision or recreated issue.
Other status fields, labels, spec and lifecycle are preserved. Uncertain writes
are not retried automatically; refresh before retrying.
RBAC restricts writes to the status subresource, not to an individual status field;
the application enforces the narrower triage-only contract.

Dashboard run/deploy/forward/undeploy targets use the current kubeconfig context.
`DASHBOARD_IMAGE` controls image build/import only, not manifest
substitution. For a registry deployment, edit the dashboard Kustomization's image
and Deployment pull policy. For another issue-storage namespace, change the
Deployment's `--namespace` argument and the Role/RoleBinding namespaces. The
RoleBinding subject must still identify the dashboard ServiceAccount in its
deployment namespace. If moving the dashboard itself, also update the
Deployment, Service and ServiceAccount namespaces and use an explicit
`kubectl --context ... -n ... port-forward --address=127.0.0.1 ... 8000:8000`.
Create any new namespace separately.

**Trust boundary:** there is no application login or TLS termination. For a deployed
dashboard, use the ClusterIP Service with localhost port-forward, not ingress/public exposure.
ClusterIP is not authentication: other network-reachable in-cluster clients can
read the same issue data **and dismiss/restore issues**. Same-origin protection
prevents cross-site browser writes but is not authentication. Add appropriate access controls before using it outside
a trusted cluster. Browser clients never receive the service account token.

Remove only the dashboard with:

```bash
make dash-undeploy
```

This preserves the agent, issues, namespace, CRD and model Secret.

### Offline dashboard validation

```bash
CGO_ENABLED=1 go test -race -timeout 120s ./cmd/issue-dash ./internal/dashboard ./tests/cluster
make check
kubectl kustomize deploy/dashboard
```

A browser-native regression suite uses an opt-in, loopback-only Go fixture host:

```bash
K8S_DASHBOARD_BROWSER=yes go test -count=1 -run '^TestBrowserFixture$' -v -timeout 10m ./internal/dashboard
# Open http://127.0.0.1:8000/__test/browser and wait for PASS in the page title.
# Open http://127.0.0.1:8000/ for fixture-backed visual/keyboard checks.
# Stop the fixture host with Ctrl+C.
```

If port 8000 is occupied, set `K8S_DASHBOARD_BROWSER_PORT=0` to choose an available
loopback port; the fixture prints the URL.

The fixture exercises filtering, ordering, pagination, safe text rendering,
details, stale/error/deleted states, response races, automatic refresh, theme
switching/persistence, text sizes and light/dark contrast.
Use a desktop browser for visual and keyboard checks. Fixture routes and data exist
only in tests: they are neither embedded nor available in the production image.
Routine Go tests skip this interactive host and stay offline.
Offline kubeconfig fixtures also verify current context and explicit context/file selection, local
HTTP serving, namespace scope, status-only triage writes, credential projection and no auth fallback.

## Make targets and variables

The [Makefile](Makefile) wraps builds, local runs and explicitly requested live
operations. Run commands from the repository root. `make` without a target
displays `make help`; it does not start the agent.

### Targets

| Target | What it does / prerequisites |
|---|---|
| `help` | Lists targets, a local-run example and configuration reminders. |
| `setup` | Downloads and verifies pinned Go modules and installs the pinned linter; requires module download access. |
| `lint-setup` | Builds golangci-lint at the pinned version into ignored `build/bin`; does not modify runtime modules or install globally. |
| `lint` | Runs configured correctness linters over all Go code, including tests and integration fixtures, without executing them. Requires `make lint-setup` first. |
| `build` | Builds `build/bin/kubescout`; does not run it or build an image. |
| `test` | Runs all routine tests offline with the race detector; requires a C compiler and CGO enabled. |
| `check` | Checks Go formatting, module tidiness/integrity and runs `lint`, including integration-tagged code; does not run live tests. |
| `run` | Rebuilds the binary, then watches in kubeconfig mode. Requires model configuration, `ALLOW_MODEL=yes` and a valid context/scope/identity. Calls AI; simulates issue writes. |
| `once` | Same prerequisites as `run`, but performs one bounded assessment and exits. |
| `integration` | Rebuilds the binary and runs live collector/connectivity tests. Creates/deletes owned workload fixtures; no model calls. |
| `integration-model` | Rebuilds the binary and runs a live CLI AI test in an owned namespace. Requires model configuration and `ALLOW_MODEL=yes`; at most six model requests. |
| `integration-deploy` | Rebuilds the local binary and tests an in-cluster AI Pod with owned namespace/Secret/RBAC. Requires an already imported `IMAGE`, separately installed ClusterIssue CRD, model configuration and `ALLOW_MODEL=yes`; at most six model requests. Does not build/import an image. |
| `image` | Builds `IMAGE` using `CONTAINER_ENGINE` and the allow-listed Docker context. Does not import or deploy it. |
| `kind-load` | Saves `IMAGE` to `build/out/kubescout.tar` and imports it into `KIND_CLUSTER`. Requires an existing image and kind cluster; does not deploy. |
| `model-secret` | Ensures the `k8s-health` namespace exists, then runs `kubectl create secret` directly from the repository-root `.env`. Requires namespace-apply and Secret-create permissions; does not overwrite the Secret, deploy or call AI. |
| `deploy` | Checks the model Secret, applies the manifests and scales the Deployment to one replica. Requires the imported manifest image and `ALLOW_MODEL=yes`; starts ongoing AI calls. Does not build/import an image or create model credentials. |
| `undeploy` | Deletes the checked-in Deployment and RBAC resources. Preserves issues, model Secret, namespace and CRDs. |
| `dash-build` | Builds the separate `build/bin/kubescout-dashboard` with embedded HTML/CSS/ESM. |
| `dash-run` | Builds and runs the KubeScout dashboard locally at `127.0.0.1:8000` using the current kubeconfig context and the agent's `k8s-health` issue-storage namespace. No model calls; explicit human dismissal/restore writes triage status. |
| `dash-image` | Builds `DASHBOARD_IMAGE` using its own credential-free build-context allow-list. |
| `dash-kind-load` | Saves/imports the existing `DASHBOARD_IMAGE` into `KIND_CLUSTER`; does not deploy. |
| `dash-deploy` | Applies only `deploy/dashboard` in the current context; requires imported image, namespace and CRD. No AI calls. |
| `dash-forward` | Forwards `127.0.0.1:8000` to the dashboard Service in `k8s-health` using the current context. |
| `dash-undeploy` | Deletes only dashboard Deployment/Service/RBAC in the current context. Preserves shared resources. |

Local runs, `model-secret`, `deploy`, `undeploy` and dashboard targets use the
current kubeconfig context, without Make context overrides.
Live integration tests still require an explicit `CONTEXT`.
Live tests use their own uniquely named namespaces, not the namespace filters. See
[live tests](#opt-in-live-tests) for the required permissions and cleanup boundaries.

### Variables

Override variables on the command line, for example `make once NAMESPACE_WATCH=apps,backend`.
The `?=` defaults also accept inherited environment values; command-line values
take precedence. These are **Make variables, not application configuration
environment variables**, except the namespace filters described below. Make does
not load `.env` into its variables. The agent loads it for local runs;
`model-secret` passes the file directly to kubectl.

| Variable | Make default | Used for |
|---|---|---|
| `GO` | `go` | Go executable for setup/build/test/check, linter installation and live-test runners. |
| `CONTEXT` | Empty (required for live tests) | Explicit kubeconfig context for live integration tests only. Not used by local run/deploy targets or kind image imports. |
| `NAMESPACE_WATCH` | Empty (all namespaces) | Comma-separated allow-list for `run`/`once`; also an agent environment variable. Mutually exclusive with `NAMESPACE_IGNORE`. |
| `NAMESPACE_IGNORE` | Empty | Comma-separated deny-list for `run`/`once`; all other namespaces are watched. Also an agent environment variable. |
| `OBSERVER_ID` | `local-dev-<username>` | Passed as `--observer-id` by `run`/`once`; stable issue ownership and correlation identity. Choose a cluster/scope-specific value instead of relying on the username default. |
| `ALLOW_MODEL` | Empty | Must be exactly `yes` for `run`, `once`, `integration-model`, `integration-deploy` and `deploy`. Make-only confirmation of potentially billable AI calls. |
| `ARGS` | Empty | Extra CLI flags appended by `run`/`once` only. Quote multiple flags together; never put credentials here. |
| `CONTAINER_ENGINE` | `podman` | Container executable used to build and save images (`image`/`kind-load`). |
| `IMAGE` | `localhost/kubescout:dev` | Image tag for `image`, `kind-load` and `integration-deploy`. Does not change the Deployment manifest. |
| `DASHBOARD_IMAGE` | `localhost/kubescout-dashboard:dev` | Separate dashboard image build/import tag; does not change manifests. |
| `KIND` | `kind` | kind executable used by `kind-load`. |
| `KIND_CLUSTER` | `kind` | kind cluster name used by image imports. Independent of the current kubeconfig context; ensure it matches the deployment cluster. |
| `KIND_PROVIDER` | `podman` | Sets `KIND_EXPERIMENTAL_PROVIDER` for `kind-load`; must match the provider that created the cluster. |
| `KUBECTL` | `kubectl` | Executable for agent and dashboard Kubernetes setup/deploy/forward/undeploy targets. |

`ALLOW_MODEL` does not enable/configure AI in the binary or impose its request
budget. It is a Make confirmation gate; the Go runtime never reads it. For model
tests, Make forwards it as `K8S_HEALTH_LIVE_MODEL` for the harness's additional
opt-in check. Direct binary execution bypasses the Make gate and still requires
model configuration. Watch/deploy authorises ongoing calls; once/model tests are
bounded. See [scheduling limits](#scheduling-and-failure-behavior) for quotas.

For additional watched namespaces or runtime limits:

```bash
mkdir -p build/out
make once NAMESPACE_WATCH=default,apps OBSERVER_ID=local-default-apps \
  ALLOW_MODEL=yes ARGS='--model-calls-per-hour=6 --output-file=build/out/latest.json'
```

Create the output directory first if using `--output-file`. The identity in this
example covers both `default` and `apps`.
Runtime flags are listed by `./build/bin/kubescout --help`; avoid overriding
Make's auth/context/identity flags through `ARGS`.

**Deployment variables are not manifest substitutions.** `NAMESPACE_WATCH`,
`NAMESPACE_IGNORE`, `OBSERVER_ID` and `ARGS` affect local `run`/`once`, not `deploy`. The checked-in
[Deployment](deploy/deployment.yaml) watches all namespaces, stores issues in
`k8s-health`, uses observer `kind-all` and image `localhost/kubescout:dev`.
Review the manifest and matching [RBAC](deploy/rbac.yaml) to change that scope;
setting `IMAGE=...` alone does not update the Deployment. See the
[deployment sequence](#container-deployment-and-migration) before activating it.

For in-cluster selection, edit the explicit `NAMESPACE_WATCH` or
`NAMESPACE_IGNORE` environment entry in the Deployment. Both default to empty,
so all namespaces are watched. These entries override same-named keys supplied
through the model Secret's `envFrom`; putting filters only in that Secret does
not change the checked-in Deployment's observation scope.

## Running the agent

Set model configuration through the repository-root `.env` for local development
or injected environment variables. Existing environment variables win.

```dotenv
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com/openai/v1/
AZURE_OPENAI_API_KEY=<runtime-key>
AZURE_AI_MODEL_DEPLOYMENT_NAME=<deployment>
```

`FOUNDRY_MODEL_NAME` remains a fallback deployment name. The endpoint must be
HTTPS `/openai/v1/` on port 443. Missing model configuration is an error, not a
fallback into deterministic monitoring. Never put credentials in source, images,
command arguments or committed manifests.

```bash
make run NAMESPACE_WATCH=default,apps \
  OBSERVER_ID=local-default-apps ALLOW_MODEL=yes

make once NAMESPACE_IGNORE=kube-system,kube-public \
  OBSERVER_ID=local-no-system ALLOW_MODEL=yes
```

Make requires explicit model authorisation. Running the executable directly is
also an explicit request to run AI:

```bash
./build/bin/kubescout --auth-mode=kubeconfig \
  --namespace-watch=default,apps --observer-id=local-default-apps
```

### Namespace selection

Configure exactly one list, or omit both to watch **all namespaces**, including
namespaces created while the agent is running:

- `--namespace-watch=apps,backend` or `NAMESPACE_WATCH=apps,backend`: watch only
  those namespaces.
- `--namespace-ignore=kube-system,kube-public` or
  `NAMESPACE_IGNORE=kube-system,kube-public`: watch every other namespace.
- Neither set: watch all namespaces. Empty environment values count as unset.

Lists contain explicit Kubernetes namespace names separated by commas.
Whitespace around names is trimmed and duplicate names are removed. Wildcards,
empty list entries and invalid names are rejected. Both lists being nonempty is
an invalid configuration, including combinations of flags and environment
variables. A flag replaces its corresponding environment list, not the opposite
list; unset the opposite environment variable when switching modes.
`--namespace` remains a deprecated, repeatable alias for `--namespace-watch`;
the old Make `NAMESPACE` variable is replaced by `NAMESPACE_WATCH`.

Explicit watch lists use namespace-scoped API requests and can use read-only
Roles bound separately in each watched namespace. All/ignore mode requires
cluster-wide `list`/`watch` permissions on the approved workload types, plus
`get` access for handle-scoped resource inspection; no Namespace-object discovery
or permissions are needed. Ignore filtering occurs
before projection, cache admission and model input, but it is an application
filter, **not an RBAC boundary**: the Kubernetes API returns ignored objects in
cluster-wide responses. Ignored objects do not consume the retained-object
budget. Nodes remain cluster-scoped and are always observed.

The checked-in workload ClusterRole grants read-only access across namespaces.
ClusterIssue create/patch permissions remain confined to the single
`--incident-namespace`. Changing observation scope requires a new observer ID;
out-of-scope stored incidents are not silently resolved or adopted. The dashboard
still reads only its configured issue-storage namespace; its **Workload namespace**
dropdown filters issues using their anchor and affected-resource namespaces
(with a separate cluster-scoped option), without changing API scope.

`--observer-id` is required: choose a stable DNS
label (up to 63 lowercase letters, digits or hyphens, starting/ending with a
letter or digit) unique to this cluster and observed scope. It contributes to
the issue name's identity hash and the ownership label used to reload open issues.
Retain it across restarts; changing it creates a separate issue identity and does
not adopt the previous observer's records. Do not reuse it for a different
cluster/scope or run competing replicas. Kubeconfig simulation still uses the
identity, but does not persist records across process exits.
Authentication is explicitly kubeconfig or in-cluster; it never falls back.

### Kubeconfig mode

Uses kubeconfig's current context by default, with the standard `KUBECONFIG`
environment variable/default file loading. For direct executable runs,
`--context=kind-kind` overrides the context; `--kubeconfig=/path/to/config`
selects a specific file. A missing/invalid current context fails explicitly rather
than falling back to another context or in-cluster credentials.

The full AI investigation still runs, but incident persistence is simulated in
memory. Accepted changes log would-create/update operations and their lifecycle
state to stderr and emit versioned incident changes on stdout. **No cluster
writes occur.** Simulated incident state is lost on process exit.

### In-cluster mode

Accepted AI decisions reconcile `ClusterIssue` CRs in `--incident-namespace`
(default `k8s-health`). Workloads remain read-only. The agent can read/list,
create and patch its incident resource, but cannot delete incidents or write
the delivery-status subresource.

Despite its name, `ClusterIssue` is namespaced, not cluster-scoped.

## Incident lifecycle and identity

**Breaking field rename:** incident lifecycle JSON now uses
`spec.lifecycle.state` (`open` or `resolved`), and the agent's indexing label is
`health.k8s-ops-agent.io/state`. The dashboard API, model input and incident
output likewise use `lifecycle.state`; there is no legacy `phase` fallback.
Dismissed remains human triage status, not an AI lifecycle state.

Existing records require an explicit, coordinated migration before the new agent
or dashboard is used: back up the records, stop writers, use a temporary CRD
schema accepting both fields, copy each `spec.lifecycle.phase` value to
`spec.lifecycle.state`, replace the corresponding `phase` label with `state`,
and remove the old field/label before applying the final CRD. Preserve names,
identity, revision, timestamps and `status.triage` so dismissal and recurrence
history remain intact. Update external consumers of incident JSON as well.
Do not apply the final schema first: it no longer defines the old field.
This repository change does not migrate or delete cluster records.
Kubernetes-owned Pod `status.phase` is unchanged.

[ClusterIssue](deploy/clusterissues-crd.yaml) replaces HealthReport:

- **No action:** nothing is created or updated.
- **Raise:** one CR for a recognised problem.
- **Update:** the same CR receives a materially changed AI conclusion.
- **Resolve:** only after stable fresh recovery and an explicit AI decision.
- **Recur:** AI updates the matching resolved incident, reopening the same CR.

Initial correlation groups use verified controller ownership (Pod -> ReplicaSet
-> Deployment, or Pod -> StatefulSet/DaemonSet/Job) or the standalone resource UID.
Services remain their own stable anchors across EndpointSlice replacement;
selector matches provide context, not ownership of Pods. Stable names derive from observer
identity, anchor incarnation and problem family: scheduling, container failure,
rollout, Job failure, node health or service availability. Multiple rules/replica Pods in that family
do not create independent incidents. This intentionally bounded taxonomy is not
unrestricted semantic grouping across unrelated workloads.

New CR names use `issue-<identity-hash>`. Existing `ClusterIssue` records named
`incident-<identity-hash>` are still loaded and updated under their original
names, including resolution and recurrence; they are not renamed or duplicated.
The identity hash is unchanged. Rebuild the binary/image and explicitly roll out
the new image to use the new prefix for newly raised issues.

Each CR contains:

| Field | Meaning |
|---|---|
| `spec.identity` | Immutable observer, verified anchor and problem family |
| `spec.resources` | Affected resource identities |
| `spec.analysis` | AI title, severity, impact, diagnosis, explanation, citations, recommendations, uncertainty and provenance |
| `spec.lifecycle` | Open/resolved state, material revision, timestamps and recurrence count |
| `status` | Human triage (`triage.dismissedRevision`) and future delivery-controller state; not written by the AI agent |

AI severity is independent of rule hints. A critical deterministic signal can
legitimately lead to AI no-action.

Prose rewording, event/restart counts and timestamps alone do not update a CR.
Same-state updates require AI's material-change decision and a change in severity,
impact, stable diagnosis key or affected-resource set. The prior wording remains
when the conclusion is unchanged.

The agent loads its open incident index before publishing and looks up known
families, including resolved incidents, before investigation. Updates use UID and
resourceVersion preconditions. Ambiguous writes are read back/reconciled under
the same name, without asking the model again. Unreconcilable persistence fails
explicitly instead of producing duplicates.

There is no automatic deletion, Pod owner reference or historical version archive.
Current incidents survive agent restart. No-action suppression and model quotas
are in memory. This is single-replica, restart-aware reconciliation, **not**
HA, exactly-once delivery or a restart-proof model quota.

## Investigation tools and data boundary

The Responses agent has only two typed local tools:

- `inspect_resource`: approved state, scheduling/capacity fields, container state
  and bounded redacted condition messages for a supplied resource handle.
  Controller template images/resources/container ports, Service port definitions,
  and EndpointSlice readiness counts are included; raw templates, selectors,
  volume claims, endpoint addresses and target UIDs are not exposed.
- `related_warning_events`: bounded redacted Warning events for an authorised
  resource; node events use the same namespace watch/ignore scope as collection.
  Event reads are bounded to 20 API pages and 20 retained events per tool call;
  oversized or incomplete results fail explicitly.

Owner/replica and node context is supplied through scoped resource handles.
No user-controlled namespace, URL, selector or API path is accepted by a tool.
Unknown tools/handles and malformed arguments fail before API access.

Tools cannot read Secrets, ConfigMaps, Pod logs or arbitrary custom resources,
exec into Pods, modify workloads or deliver notifications. Raw API objects,
annotations, environment variables and UIDs are not sent to the model; UIDs stay
host-side behind opaque handles.

Operational text is untrusted. Messages are bounded and credential-like text is
redacted; raw tool/model error bodies are not logged. This reduces exposure but
does not guarantee arbitrary messages contain no sensitive information. Names,
image references, scheduling facts and approved messages can reveal operational
details and are part of the model data boundary.

The prompt is embedded from [instructions.txt](internal/analysis/instructions.txt).
Rebuild after editing it. Validation enforces exact JSON fields, unique keys,
known resource/incident references and cited input/tool evidence. Intermediate
reasoning and tool transcripts are never incident records.

## Scheduling and failure behavior

| Control | Default |
|---|---|
| Concurrent investigations | 1 |
| Actual model HTTP attempts | 60 per rolling hour |
| Model requests per investigation | Maximum 6, including synthesis |
| Tool invocations per investigation | Maximum 8, serial |
| New evidence batching | 5 seconds |
| Ordinary same-case cooldown | 2 minutes |
| Fresh stable recovery before AI review | 2 minutes |
| Unresolved/no-action review | 15 minutes |
| API/model request timeout | 10 seconds |
| Whole investigation / once timeout | 120 seconds |
| Initial evidence budget | 20,000 bytes |
| Tool result / cumulative tool output | 8 KiB / 32 KiB |
| Model output | 2,000 tokens per request, plus cumulative byte checks |
| Observed objects / pending cases / open incident index | Bounded by `--max-objects` (5,000) |

The SDK tool loop is explicitly bounded, with an independent transport ceiling
counting every model attempt. Automatic HTTP retries, server-side storage,
provider conversations, hosted tools and background continuations stay disabled.
Only ephemeral local tool history is used.

Counter churn does not defeat no-action suppression. Worsening triage can bypass
ordinary cooldown, not request/scope limits. Repeated failures receive bounded
backoff; unchanged failed investigations eventually wait for scheduled review,
rather than being cached as a successful no-action.

Observation continues while AI runs. Affected evidence is rechecked before
publication; stale decisions are discarded. Recovery is reviewed even after all
triggering signals disappear. Short healthy phases in a crash loop do not satisfy
continuous recovery; incomplete collection resets the recovery window.

Collection outages, model/tool failures, quota exhaustion and output failures
are **operator diagnostics, never deterministic incidents**. Existing incidents
remain unresolved when evidence or AI is unavailable. A missing CR does not
prove a healthy cluster or a working agent.

`--once` cannot prove continuous recovery from one snapshot, so it does not
automatically resolve an existing incident. Use watch mode for recovery review.

## Output

Schema **3.0** deliberately replaces the old assessment/report stream.

- Watch stdout: accepted incident changes only, with operation/name/incident and
  a simulation marker. No baseline, cache lifecycle or no-action records.
- Stderr: operator diagnostics, including no-action reasons and model/tool failures.
- `--once` stdout: exactly one execution-result envelope with zero or more changes.
  Successful no-action has an empty `changes` array, not a synthetic healthy CR.
- `--output-file`: atomically replace a private latest-output file; not an archive.

```json
{"schema_version":"3.0","type":"execution","execution_status":"completed","changes":[],"errors":[]}
```

Once exits 0 for completed execution, including an open incident or no-action;
1 for incomplete execution/output; 2 for invalid configuration. Watch exits 0 on
graceful signal, 1 on bootstrap/unrecoverable output failure, 2 on invalid config.
Recoverable model/collection problems remain diagnostics while observation runs.
Kubernetes persistence and stdout/file output are not transactional.

## Container deployment and migration

Build assets live in [build/](build/); manifests in [deploy/](deploy/).
Images remain digest-pinned at the base, non-root, credential-free, with a
read-only root filesystem, dropped capabilities and RuntimeDefault seccomp.
No probes or writable progress files are introduced.

**The checked-in Deployment has zero replicas.** It must not silently enable
billable model calls when applied over the old deterministic installation.

1. Stop the old Deployment explicitly before migrating.
2. Build/import the new image.
3. Create the runtime namespace and provision Secret `k8s-health-model`
   separately, without committing credential values.
4. Review `--observer-id`, namespace watch/ignore lists and corresponding RBAC.
5. Explicitly authorise deployment/model traffic.

```bash
make image IMAGE=localhost/kubescout:dev
make kind-load KIND_CLUSTER=kind IMAGE=localhost/kubescout:dev
make model-secret
make deploy ALLOW_MODEL=yes
kubectl -n k8s-health get clusterissues
kubectl -n k8s-health logs deployment/k8s-health -c agent
```

`make deploy` checks that the Secret exists without printing its values, applies
the zero-replica manifests, then explicitly scales the single Deployment to one.
These commands use your current context. Select it with
`kubectl config use-context ...` before running them.
`make model-secret` applies the namespace manifest before provisioning the Secret.
Rebuilding/importing an image alone does not restart a running Pod. If reusing
the `dev` tag, explicitly roll out the new image after import; `imagePullPolicy:
Never` requires the image to be present on the cluster nodes.

`make model-secret` runs `kubectl apply -f deploy/namespace.yaml`, then
`kubectl -n k8s-health create secret generic k8s-health-model --from-env-file=.env`
in the current context. No Go command or script is involved. It requires
namespace-apply and Secret-create permissions; the agent's ServiceAccount gets
neither permission.

Every `.env` entry becomes a Secret key, so keep the file to intended runtime
settings. Use plain `KEY=value` lines without `export` or surrounding quotes:
kubectl uses file values literally, not shell environment overrides. Creation
fails if the Secret already exists; rotate credentials deliberately with standard
Kubernetes tooling. The target does not start the agent or call a model, and
credential values are not placed in command arguments or printed.

Old flags `--deterministic-only`, `--report-namespace` and `--report-file` are
unsupported. Use required AI configuration, `--incident-namespace` and
`--output-file` respectively.

Legacy HealthReport schemas are no longer shipped in this repository; the active
bundle installs only the ClusterIssue CRD. Existing HealthReport CRs are **not converted into AI
incidents, deleted or modified**. Remove obsolete report-write RBAC explicitly
after stopping the old producer. Export/delete historical data only with separate
approval. A new image cannot infer AI conclusions from old deterministic reports.

`ClusterIssue` also replaces the earlier `AIIncident` API name. Kubernetes treats
`clusterissues` as a new resource type; existing `aiincidents` are not automatically
migrated or deleted. If that earlier version already has records, migrate them
separately before activating the new writer to preserve deduplication. The
`--incident-namespace` flag, payload fields and stable identity hash are unchanged
by the API naming change. New CRs now use the `issue-` prefix, while existing
`ClusterIssue` names using `incident-` remain supported in place.

`make undeploy` removes the new Deployment/RBAC but preserves namespace and CRDs.
Deleting a namespace/CRD destroys its incident data. Do not use `delete -k deploy`
as a shutdown command. Rollback requires deliberate image/model-policy selection;
do not silently restart the old noisy producer.

## Opt-in live tests

```bash
# Scoped collector/evidence tests; no AI calls or deterministic runtime mode.
make integration CONTEXT=kind-kind

# Full CLI AI run, at most six real model requests.
make integration-model CONTEXT=kind-kind ALLOW_MODEL=yes

# Imported image, preinstalled ClusterIssue CRD, temporary owned namespace/Secret.
# One bounded in-cluster AI pod, at most six model requests.
make integration-deploy CONTEXT=kind-kind ALLOW_MODEL=yes
```

The build-tagged harness creates uniquely named owned namespaces and checks UID/
ownership before cleanup. Container validation also cleans its owned node RBAC.
The collector tests now invoke collector/evidence APIs directly; their RSS is
test-process RSS, not standalone agent memory. Full agent tests validate an AI
decision; no-action is valid and does not require a CR to be manufactured.

The [failure-injection script](scripts/inject-failures.sh) still creates optional
development fixtures with an explicit context/namespace and owned cleanup.
No live tests, model requests or new deployment are automatically authorised by
running the offline suite.

## Code map

| Package | Responsibility |
|---|---|
| [cmd/kubescout](cmd/kubescout) | AI runtime, explicit auth/configuration and exit codes |
| [cmd/issue-dash](cmd/issue-dash) | Standalone dashboard CLI, auth selection and HTTP lifecycle |
| [internal/config](internal/config) | Validation, limits and root dotenv handling |
| [internal/kube](internal/kube) | Scoped collection, projection, ownership and freshness |
| [internal/health](internal/health) | Pure internal evidence rules; no notification authority |
| [internal/investigation](internal/investigation) | Typed bounded read-only tools and redaction |
| [internal/analysis](internal/analysis) | Agent Framework tool loop, budgets and AI decision validation |
| [internal/incident](internal/incident) | Correlation, AI incident domain, lifecycle and reconciliation |
| [internal/runner](internal/runner) | Bounded candidate scheduling, recovery review and freshness checks |
| [internal/output](internal/output) | Versioned accepted-change output and separate diagnostics |
| [internal/dashboard](internal/dashboard) | Bounded single-namespace ClusterIssue reads and embedded browser UI |
| [build/](build/) | Separate pinned non-root images and allow-listed build contexts |
| [deploy/](deploy/) | ClusterIssue schema, agent Deployment and workload/incident RBAC |
| [deploy/dashboard/](deploy/dashboard/) | Independent dashboard Deployment, Service and status-only triage RBAC |

AI can still misdiagnose or correlate incorrectly. Bounded references and stable
identity are safeguards, not a guarantee of semantic correctness. A future sender
should consume meaningful incident revisions and own delivery status; exactly-once
notifications/history are separate work. AKS, HA, remediation, logs, automatic
retention and independent agent-health monitoring remain out of scope.
