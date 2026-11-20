# Examples

This directory contains an example of loading in extra resource checks on top of the base/core Kubernetes resources that are monitored by Kubescout.

Cert manager is a very common Kubernetes add-on for managing TLS certificates, and this example demonstrates how to monitor its resources

- [cert-manager.yaml](cert-manager.yaml): CEL checks for Certificate readiness and
  expiry. Use with `--extra-resources=examples/cert-manager.yaml` for local runs.
- [deploy/](deploy/): Kustomize overlay that mounts this policy through a ConfigMap
  and adds read-only Certificate RBAC to the existing agent deployment.

## Kubernetes deployment

Run these commands from the repository root.

Preview the manifests without contacting the cluster:

```bash
kubectl kustomize examples/deploy --load-restrictor=LoadRestrictionsNone
```

Before deploying, ensure the Certificate API and model Secret are available, and
review the image, observer ID and namespace scope. Applying starts **one replica**
and ongoing AI investigations, which can incur model charges.

```bash
kubectl kustomize examples/deploy --load-restrictor=LoadRestrictionsNone \
  | kubectl apply -f -
```

The loading flag permits reading the policy from the parent directory; use it
only with trusted Kustomizations. After editing the policy, rerun the deployment
command to roll out the changes.

See the [operations guide](../docs/operations.md#configmap-backed-kubernetes-example)
for prerequisites, configuration and stopping the agent.
