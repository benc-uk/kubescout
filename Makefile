.DEFAULT_GOAL := help

# Make overrides are not loaded from .env; see README.md for defaults and scope.
GO ?= go
# Local run/once options; deployment scope and identity come from the manifests.
NAMESPACE_WATCH ?=
NAMESPACE_IGNORE ?=
ARGS ?=
OBSERVER_ID ?= local-dev-$(shell whoami)
# Confirmation for model-calling Make targets, not a Go runtime setting.
ALLOW_MODEL ?=
CONTAINER_ENGINE ?= podman
IMAGE ?= localhost/k8s-health:dev
DASHBOARD_IMAGE ?= localhost/k8s-dashboard:dev
KIND ?= kind
# kind's cluster name is separate from the current kubeconfig context.
KIND_CLUSTER ?= kind
KIND_PROVIDER ?= podman
KUBECTL ?= kubectl

.PHONY: help setup build run once test check integration integration-model integration-deploy image kind-load model-secret deploy undeploy
.PHONY: dashboard-build dashboard-run dashboard-image dashboard-kind-load dashboard-deploy dashboard-forward dashboard-undeploy

help: ## 📖 Show available targets
	@awk 'BEGIN {FS = ":.*## "; print "\n☸ Kubernetes AI incident agent\n"} \
		/^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2} \
		END {print "\nExample (real AI calls, simulated issue writes):\n  make once NAMESPACE_WATCH=default,apps OBSERVER_ID=local-default-apps ALLOW_MODEL=yes\n\nLocal defaults: current kubeconfig context, all namespaces, OBSERVER_ID=local-dev-<username>\nNAMESPACE_WATCH and NAMESPACE_IGNORE are mutually exclusive comma-separated lists.\nChoose a stable OBSERVER_ID unique to the cluster and watched scope.\nALLOW_MODEL=yes is a Make confirmation gate; model credentials are still required.\nARGS adds runtime flags to run/once only. deploy uses the checked-in manifests.\nRoutine tests stay offline. Live tests require CONTEXT and create/delete owned cluster fixtures.\nSee README.md: Make targets and variables, prerequisites and deployment steps.\n"}' $(MAKEFILE_LIST)

setup: ## 📦 Download pinned Go dependencies
	$(GO) mod download
	$(GO) mod verify

build: ## 🔨 Build the Go binary
	@$(GO) build -trimpath -o build/bin/k8s-health ./cmd/k8s-health

run: ## 👀 Watch with AI in kubeconfig mode (ALLOW_MODEL=yes)
once: ## 🚀 Run one AI assessment in kubeconfig mode (ALLOW_MODEL=yes)

run once: build
	@test -n "$(OBSERVER_ID)" && test "$(ALLOW_MODEL)" = yes || { printf '❌ Set OBSERVER_ID and ALLOW_MODEL=yes\n' >&2; exit 2; }
	@./build/bin/k8s-health --auth-mode kubeconfig \
		$(if $(strip $(NAMESPACE_WATCH)),--namespace-watch "$(NAMESPACE_WATCH)",) \
		$(if $(strip $(NAMESPACE_IGNORE)),--namespace-ignore "$(NAMESPACE_IGNORE)",) \
		--observer-id "$(OBSERVER_ID)" $(if $(filter once,$@),--once,) $(ARGS)

test: ## 🧪 Run tests without a cluster or model connection
	$(GO) test -race -timeout 120s ./...

integration: ## ☸ Live collector tests with owned fixtures (CONTEXT; no model calls)
integration-model: ## 🤖 Live AI test, at most six model requests (CONTEXT, ALLOW_MODEL=yes)

integration integration-model: build
	@test -n "$(CONTEXT)" || { printf '❌ Set CONTEXT explicitly\n' >&2; exit 2; }
	@if [ "$@" = integration-model ]; then test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise up to six model requests\n' >&2; exit 2; }; fi
	@K8S_HEALTH_LIVE_CONTEXT="$(CONTEXT)" \
		K8S_HEALTH_LIVE_MODEL="$(ALLOW_MODEL)" $(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '$(if $(filter integration-model,$@),^TestLiveModel$$,^TestLive(Kubernetes|Connectivity)$$)' ./tests/cluster

integration-deploy: build ## 🧪 Live in-cluster AI test (imported IMAGE, CRD, CONTEXT, ALLOW_MODEL=yes)
	@test -n "$(CONTEXT)" || { printf '❌ Set CONTEXT explicitly\n' >&2; exit 2; }
	@test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise up to six model requests\n' >&2; exit 2; }
	@K8S_HEALTH_LIVE_CONTEXT="$(CONTEXT)" \
		K8S_HEALTH_LIVE_MODEL="$(ALLOW_MODEL)" K8S_HEALTH_LIVE_CONTAINER=yes \
		K8S_HEALTH_CONTAINER_ENGINE="$(CONTAINER_ENGINE)" K8S_HEALTH_IMAGE="$(IMAGE)" \
		$(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '^TestLiveDeployment$$' ./tests/cluster

check: ## 🔍 Check formatting, modules and static diagnostics
	@test -z "$$(gofmt -l cmd internal tests)" || { printf '❌ Run gofmt -w cmd internal tests\n' >&2; exit 1; }
	$(GO) mod tidy -diff
	$(GO) mod verify
	$(GO) vet -tags=integration ./...

image: ## 📦 Build the non-root container image
	$(CONTAINER_ENGINE) build $(if $(filter podman,$(notdir $(CONTAINER_ENGINE))),--ignorefile build/Dockerfile.dockerignore,) \
		-f build/Dockerfile -t "$(IMAGE)" .

kind-load: ## ☸ Import IMAGE into kind's KIND_CLUSTER (does not deploy)
	@test -n "$(KIND_CLUSTER)" || { printf '❌ Set KIND_CLUSTER explicitly\n' >&2; exit 2; }
	@mkdir -p build/out
	@trap 'rm -f build/out/k8s-health.tar.tmp' EXIT; \
		$(CONTAINER_ENGINE) save "$(IMAGE)" > build/out/k8s-health.tar.tmp && \
		mv build/out/k8s-health.tar.tmp build/out/k8s-health.tar
	KIND_EXPERIMENTAL_PROVIDER="$(KIND_PROVIDER)" $(KIND) load image-archive build/out/k8s-health.tar --name "$(KIND_CLUSTER)"

model-secret: ## 🔐 Ensure namespace and create model Secret from .env (current context)
	$(KUBECTL) apply -f deploy/namespace.yaml
	$(KUBECTL) -n k8s-health create secret generic k8s-health-model --from-env-file=.env

deploy: ## 🚀 Apply manifests and start ongoing AI (current context, Secret, ALLOW_MODEL=yes)
	@test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise ongoing AI investigations\n' >&2; exit 2; }
	@$(KUBECTL) -n k8s-health get secret k8s-health-model -o name || { \
		printf '❌ Model Secret is unavailable in the current context. Check access or run: make model-secret\n' >&2; exit 1; }
	$(KUBECTL) apply -k deploy
	$(KUBECTL) -n k8s-health scale deployment/k8s-health --replicas=1

undeploy: ## 🧹 Remove Deployment/RBAC; retain issues, Secret, namespace and CRDs (current context)
	$(KUBECTL) delete -f deploy/deployment.yaml -f deploy/rbac.yaml

dash-build: ## 🔨 Build the standalone dashboard (no JavaScript build step)
	$(GO) build -trimpath -o build/bin/issue-dash ./cmd/issue-dash

dash-run: dash-build ## 🌐 Run issue-dash locally on 127.0.0.1:8000 (current context)
	./build/bin/issue-dash --auth-mode=kubeconfig --namespace=k8s-health

dash-image: ## 📦 Build the separate non-root DASHBOARD_IMAGE
	$(CONTAINER_ENGINE) build $(if $(filter podman,$(notdir $(CONTAINER_ENGINE))),--ignorefile build/Dockerfile.dashboard.dockerignore,) \
		-f build/Dockerfile.dashboard -t "$(DASHBOARD_IMAGE)" .

dash-kind-load: ## ☸ Import DASHBOARD_IMAGE into KIND_CLUSTER (does not deploy)
	@test -n "$(KIND_CLUSTER)" || { printf '❌ Set KIND_CLUSTER explicitly\n' >&2; exit 2; }
	@mkdir -p build/out
	@trap 'rm -f build/out/k8s-dashboard.tar.tmp' EXIT; \
		$(CONTAINER_ENGINE) save "$(DASHBOARD_IMAGE)" > build/out/k8s-dashboard.tar.tmp && \
		mv build/out/k8s-dashboard.tar.tmp build/out/k8s-dashboard.tar
	KIND_EXPERIMENTAL_PROVIDER="$(KIND_PROVIDER)" $(KIND) load image-archive build/out/k8s-dashboard.tar --name "$(KIND_CLUSTER)"

dash-deploy: ## 🚀 Deploy only the dashboard (current context; imported image, namespace and CRD)
	$(KUBECTL) apply -k deploy/dashboard

dash-forward: ## 🌐 Open localhost:8000 access (current context)
	$(KUBECTL) -n k8s-health port-forward --address=127.0.0.1 service/k8s-dashboard 8000:8000

dash-undeploy: ## 🧹 Remove only dashboard resources; retain agent and issues (current context)
	$(KUBECTL) delete -f deploy/dashboard/deployment.yaml \
		-f deploy/dashboard/service.yaml -f deploy/dashboard/rbac.yaml