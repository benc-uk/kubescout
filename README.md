# KubeScout

**Your AI Kubernetes investigator.**

<img src="internal/dashboard/web/logo.png" alt="KubeScout logo" width="180">

KubeScout monitors your Kubernetes cluster and uses AI to investigate problems.
It brings related observations together, gathers supporting evidence, and
records a diagnosis, impact and recommended next steps as a Kubernetes incident.
As conditions change, it revisits the incident and assesses recovery.

Use it to understand failing workloads, investigate cluster issues, and review
ongoing problems in a dashboard without piecing together every resource yourself.
It investigates and recommends; it does not remediate workloads.

## What it can do

- **Investigate common Kubernetes problems:** container failures, unschedulable
  Pods, unhealthy nodes, rollout and replica shortfalls, failed Jobs, and Services
  without ready backends or matching backend ports.
- **Configure checks for additional APIs and CRDs:** select status facts and write
  bounded CEL wellness rules in YAML, without resource-specific Go changes. See
  [extra-resource configuration](docs/resources-and-checks.md#configured-extra-resources)
  and the [cert-manager example](examples/cert-manager.yaml).
- **Connect related evidence:** group replica failures under their controller and
  inspect relevant resources and Warning events instead of treating every signal
  as a separate incident.
- **Explain its conclusions:** provide severity, impact, diagnosis, supporting
  references, recommendations and uncertainty.
- **Track the lifecycle:** update meaningful changes, reopen recurring problems,
  and resolve incidents after sustained recovery and an AI review.
- **Support human review:** search and filter incidents in the optional dashboard,
  inspect affected resources, and dismiss or restore items without changing the
  agent's conclusions.
- **Work within a chosen scope:** observe selected namespaces, exclude namespaces,
  or watch the whole cluster, with bounded model and tool usage.

## How it works

![How KubeScout works: cluster evidence feeds AI investigation with scoped read-only tools; validated AI decisions may change Incident resources, which the optional dashboard displays. No action is also valid.](docs/how-it-works.svg)

Evidence checks identify cases worth investigating; AI decides whether they
warrant an incident. **No action is a valid conclusion**, even when a check finds
something concerning. Decisions are validated against the available evidence
and current observations before being applied.

Incidents are cluster-scoped `Incident` custom resources in the
`kubescout.benc-uk.github.com/v1alpha1` API group. They survive agent restarts and
can be read with kubectl or the dashboard. Console logs, AI proposal logs and transcripts help
explain agent activity; the Kubernetes resources are the incident record.
An explicit read-only assessment can instead log diagnostic AI proposals without
reading or writing Incidents; it does not simulate or persist incident lifecycle.

The agent is a Go process using Azure OpenAI through the Microsoft Agent
Framework. The dashboard is a separate, optional Go service; it reads incidents
and records human triage, but does not call AI.

See [Architecture](ARCHITECTURE.md) for the investigation flow, lifecycle and
implementation boundaries.

## Run locally

### Prerequisites

- Go **1.26+**, Git and Make.
- kubectl and a kubeconfig for the cluster you want to observe.
- Read permission for observed resources. Normal incident tracking additionally
  requires Incident CRD/namespace setup and Incident create/patch permissions.
  See [Kubernetes access](docs/operations.md#kubernetes-access).
- An Azure OpenAI model deployment supporting the Responses API and function tools,
  with its endpoint, deployment name and API key.

Local runs use your **current kubeconfig context**, make potentially billable
model calls and normally **persist real incidents** in the cluster. Workloads
remain read-only; explicit read-only assessments log proposals instead.

### Build and configure

```bash
git clone https://github.com/benc-uk/kubescout.git
cd kubescout
make build
```

Create a repository-root `.env` with the following settings, or supply them as
environment variables. Existing environment values take precedence.

```dotenv
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com/openai/v1/
AZURE_OPENAI_API_KEY=<your-api-key>
AZURE_AI_MODEL_DEPLOYMENT_NAME=<your-deployment>
```

Keep credentials out of source control. The endpoint must use HTTPS and end in
`/openai/v1/`. See [model configuration](docs/operations.md#model-connection) for details.

### Start investigating

Check the selected context, install the runtime namespace and cluster-scoped CRD, then start
watching the `default` namespace:

```bash
kubectl config current-context
make incidents-setup
make run NAMESPACE_WATCH=default OBSERVER_ID=local-default
```

Choose a stable observer ID for this cluster and observation scope; keep it
across restarts and choose a new one if the scope changes. Namespace lists can
contain multiple comma-separated names. Omitting both namespace filters watches
all namespaces; nodes are always observed.

Watch mode runs until stopped with Ctrl+C. For one bounded assessment instead:

```bash
make once NAMESPACE_WATCH=default OBSERVER_ID=local-default
```

Running the agent uses AI; model configuration is required. A completed
assessment may produce no incidents. Watch mode is needed to establish
continuous recovery for resolution. One configurable `--recovery-window`
(default 5 minutes) covers cleared problem signals and confirmed anchor absence;
AI still decides whether to resolve. See [Incident recovery](docs/operations.md#incident-recovery).

AI proposals are also captured by default in `logs/ai-proposals.log`, separately
from confirmed Kubernetes writes. Use `--proposal-log=false` to disable this in
normal mode, or `--proposal-log-dir` to choose another directory. By default it
follows `--ai-log-dir`. See [proposal capture](docs/operations.md#ai-proposal-capture).

### Assess without Kubernetes writes

For read-only Kubernetes access, skip `make incidents-setup` and select the
explicit proposal-only mode:

```bash
make once NAMESPACE_WATCH=default OBSERVER_ID=local-default ARGS="--read-only --proposal-log-dir=logs"
# Use make run with the same ARGS for continuous read-only assessment.
```

AI remains required and potentially billable. Validated fresh proposals, including
diagnosis and recommendations, go to private rotating `logs/ai-proposals.log` files
instead of Incident CRDs. No Incident API permissions or CRD are needed.
These are diagnostics, not stored incidents: they do not appear in the dashboard,
resume lifecycle tracking or resolve previously stored Incidents.
See [read-only assessment details](docs/operations.md#read-only-ai-assessments).

### Review incidents

In another terminal:

```bash
kubectl get incidents
make dash-run
```

Open **http://127.0.0.1:8000**. The dashboard shows open incidents by default,
with filters for severity, state, problem family and workload namespace.
Open an incident for its diagnosis, recommendations and affected resources.
Dismissal hides the current revision; a material AI update brings it back.

The local dashboard needs incident read permissions, plus status-patch
permission for dismissal/restore. It does not start the agent or make model calls.
See the [operations guide](docs/operations.md) for authentication, configuration
and dashboard details.

For a separate local test consumer that POSTs newly created Incidents to your
own endpoint, see the [alert-test watcher](docs/development.md#alert-delivery-test-watcher).
It is not part of the AI agent or dashboard.

## Deploy to Kubernetes

The [agent manifests](deploy/) install the Incident CRD, namespace, RBAC and
Deployment. The [dashboard manifests](deploy/dashboard/) are independent.

Before deploying:

1. Select the intended kubeconfig context.
2. Set an appropriate image and tag in [the agent Kustomization](deploy/kustomization.yaml).
   The checked-in value is `ghcr.io/benc-uk/kubescout:arm64`; choose an image built
   for your nodes and accessible to the cluster.
3. Review the observer ID, namespace scope and model budget in
   [the Deployment](deploy/deployment.yaml), and the corresponding [RBAC](deploy/rbac.yaml).
4. Prepare the model settings in `.env`, using plain `KEY=value` lines.

Then provision the model Secret and deploy the agent:

```bash
make model-secret
make deploy
kubectl get incidents
kubectl -n kubescout logs deployment/kubescout -c agent
```

The Secret-creation command does not overwrite an existing Secret. The agent
Deployment starts one replica when applied; model calls are part of normal operation.

For Certificate checks loaded from a read-only ConfigMap, use the opt-in
[extra-resource Kustomize example](examples/deploy/kustomization.yaml) and its
[deployment instructions](docs/operations.md#configmap-backed-kubernetes-example).
It reuses the single-replica agent Deployment and adds Certificate read RBAC.
Use the example's commands instead of
`make deploy`, which applies only the base manifests.

For the optional dashboard, configure its image and pull policy first: it
currently defaults to a locally imported development image, not a registry image.
Then run:

```bash
make dash-deploy
make dash-forward
```

Open **http://127.0.0.1:8000**. There is no dashboard login or TLS; keep access
within a trusted cluster or localhost port-forward.

See [Kubernetes deployment](docs/operations.md#kubernetes-deployment) for registry
and local-kind workflows, image configuration, dashboard access and safe shutdown.
`make undeploy` stops/removes the agent while retaining incidents; do not delete
the namespace or CRD to stop it.

Incident storage has no namespace setting. Workload namespace filters still
control observation, and the agent/dashboard components still run in `kubescout`.
Existing namespaced Incident installations require a destructive development
reset, not an in-place upgrade; no migration or history preservation is provided.
See [updates and shutdown](docs/operations.md#updates-and-shutdown).

## Important boundaries

- Workload access is read-only. The agent cannot run shell commands, read Pod
  logs or Secrets, or remediate resources. Its writes are limited to incidents.
- Approved resource facts, bounded operational messages and native describe
  details are sent to the configured model. Credential values are redacted,
  not entire messages or diagnostic sections. Operational details can be sensitive.
- AI can misdiagnose problems. No incidents is not proof of cluster health or
  agent activity; collection/model failures are diagnostics, not incidents.
- Run one agent replica per observer identity. Incident state survives restarts;
  model quotas and scheduling state are process-local.
- Anyone with dashboard access can use its granted triage permissions.
  Dismissal is not resolution and does not stop investigation.

## Documentation and development

| Guide | What it covers |
|---|---|
| [Architecture](ARCHITECTURE.md) | Components, evidence, AI investigation, incident lifecycle and data boundaries |
| [Resources and health checks](docs/resources-and-checks.md) | Collected Kubernetes resources, retained facts, check rules, thresholds and exceptions |
| [Operations](docs/operations.md) | Configuration, deployment, dashboard use, logs, troubleshooting and upgrades |
| [Development](docs/development.md) | Builds, tests, linting, images and the complete Make reference |

For contributor setup and validation, start with the development guide.
Run `make help` to see the available commands.
