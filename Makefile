.DEFAULT_GOAL := help

# Make overrides are not loaded from .env; see docs/development.md for the Make reference.
GO ?= go
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT := $(CURDIR)/build/bin/golangci-lint/$(GOLANGCI_LINT_VERSION)/golangci-lint
# Local run/once options; deployment scope and identity come from the manifests.
NAMESPACE_WATCH ?=
NAMESPACE_IGNORE ?=
ARGS ?=
OBSERVER_ID ?= local-dev-$(shell whoami)
# Confirmation for model-calling Make targets, not a Go runtime setting.
ALLOW_MODEL ?=
CONTAINER_ENGINE ?= podman
IMAGE ?= localhost/kubescout:dev
DASHBOARD_IMAGE ?= localhost/kubescout-dashboard:dev
TARGETOS ?= linux
TARGETARCH ?= $(shell $(GO) env GOARCH)
KIND ?= kind
# kind's cluster name is separate from the current kubeconfig context.
KIND_CLUSTER ?= kind
KIND_PROVIDER ?= podman
KUBECTL ?= kubectl

.PHONY: help setup lint-setup lint build run once test check integration integration-model integration-deploy image kind-load incidents-setup model-secret deploy undeploy
.PHONY: dash-build dash-run dash-image dash-kind-load dash-deploy dash-forward dash-undeploy

help: ## 📖 Show available targets
	@awk 'BEGIN {FS = ":.*## "; print "\n☸ KubeScout · Your AI Kubernetes investigator.\n"} \
		/^[a-zA-Z0-9_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2} \
		END {print "\nExample (real AI calls and persisted Incidents):\n  make incidents-setup\n  make once NAMESPACE_WATCH=default,apps OBSERVER_ID=local-default-apps ALLOW_MODEL=yes\n\nLocal defaults: current kubeconfig context, all namespaces, OBSERVER_ID=local-dev-<username>\nLocal credentials need get/list/create/patch access to Incidents in kubescout.\nNAMESPACE_WATCH and NAMESPACE_IGNORE are mutually exclusive comma-separated lists.\nChoose a stable OBSERVER_ID unique to the cluster and watched scope.\nALLOW_MODEL=yes is a Make confirmation gate; model credentials are still required.\nARGS adds runtime flags to run/once only. deploy uses the checked-in manifests.\nRoutine tests stay offline. Live tests require CONTEXT and create/delete owned cluster fixtures.\nSee README.md for getting started; docs/operations.md for configuration and deployment;\ndocs/development.md for the Make reference, prerequisites and tests.\n"}' $(MAKEFILE_LIST)

setup: ## 📦 Download pinned Go dependencies and install the linter
	$(GO) mod download
	$(GO) mod verify
	$(MAKE) lint-setup

lint-setup: ## 🔧 Install the pinned Go linter into ignored build/bin
	GOBIN="$(dir $(GOLANGCI_LINT))" $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

lint: ## 🧹 Lint all Go code and integration fixtures without running them
	@test -x "$(GOLANGCI_LINT)" || { printf '❌ Linter missing; run make lint-setup (or make setup)\n' >&2; exit 1; }
	"$(GOLANGCI_LINT)" run --config .golangci.yml ./...

build: ## 🔨 Build the Go binary
	@$(GO) build -trimpath -o build/bin/kubescout ./cmd/kubescout

run: ## 👀 Watch with AI and persist Incidents locally (ALLOW_MODEL=yes)
once: ## 🚀 Run one AI assessment and persist Incidents locally (ALLOW_MODEL=yes)

run once: build
	@test -n "$(OBSERVER_ID)" && test "$(ALLOW_MODEL)" = yes || { printf '❌ Set OBSERVER_ID and ALLOW_MODEL=yes\n' >&2; exit 2; }
	@./build/bin/kubescout --auth-mode kubeconfig \
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
	@KUBESCOUT_LIVE_CONTEXT="$(CONTEXT)" \
		KUBESCOUT_LIVE_MODEL="$(ALLOW_MODEL)" $(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '$(if $(filter integration-model,$@),^TestLiveModel$$,^TestLive(Kubernetes|Connectivity)$$)' ./tests/cluster

integration-deploy: build ## 🧪 Live in-cluster AI test (imported IMAGE, CRD, CONTEXT, ALLOW_MODEL=yes)
	@test -n "$(CONTEXT)" || { printf '❌ Set CONTEXT explicitly\n' >&2; exit 2; }
	@test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise up to six model requests\n' >&2; exit 2; }
	@KUBESCOUT_LIVE_CONTEXT="$(CONTEXT)" \
		KUBESCOUT_LIVE_MODEL="$(ALLOW_MODEL)" KUBESCOUT_LIVE_CONTAINER=yes \
		KUBESCOUT_CONTAINER_ENGINE="$(CONTAINER_ENGINE)" KUBESCOUT_IMAGE="$(IMAGE)" \
		$(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '^TestLiveDeployment$$' ./tests/cluster

check: ## 🔍 Check formatting, modules and Go lint diagnostics
	@test -z "$$(gofmt -l cmd internal tests)" || { printf '❌ Run gofmt -w cmd internal tests\n' >&2; exit 1; }
	$(GO) mod tidy -diff
	$(GO) mod verify
	$(MAKE) lint

image: ## 📦 Build the non-root container image
	$(CONTAINER_ENGINE) build $(if $(filter podman,$(notdir $(CONTAINER_ENGINE))),--ignorefile build/Dockerfile.dockerignore,) \
		--platform "$(TARGETOS)/$(TARGETARCH)" \
		--build-arg TARGETOS="$(TARGETOS)" --build-arg TARGETARCH="$(TARGETARCH)" \
		-f build/Dockerfile -t "$(IMAGE)" .

kind-load: ## ☸ Import IMAGE into kind's KIND_CLUSTER (does not deploy)
	@test -n "$(KIND_CLUSTER)" || { printf '❌ Set KIND_CLUSTER explicitly\n' >&2; exit 2; }
	@mkdir -p build/out
	@trap 'rm -f build/out/kubescout.tar.tmp' EXIT; \
		$(CONTAINER_ENGINE) save "$(IMAGE)" > build/out/kubescout.tar.tmp && \
		mv build/out/kubescout.tar.tmp build/out/kubescout.tar
	KIND_EXPERIMENTAL_PROVIDER="$(KIND_PROVIDER)" $(KIND) load image-archive build/out/kubescout.tar --name "$(KIND_CLUSTER)"

incidents-setup: ## ☸ Install Incident namespace and CRD only (current context; no agent/model)
	$(KUBECTL) apply -f deploy/namespace.yaml
	$(KUBECTL) apply -f deploy/incidents-crd.yaml
	$(KUBECTL) wait --for=condition=Established --timeout=60s crd/incidents.kubescout.benc-uk.github.com

model-secret: ## 🔐 Ensure namespace and create model Secret from .env (current context)
	$(KUBECTL) apply -f deploy/namespace.yaml
	$(KUBECTL) -n kubescout create secret generic kubescout-model --from-env-file=.env

deploy: ## 🚀 Apply manifests and start ongoing AI (current context, Secret, ALLOW_MODEL=yes)
	@test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise ongoing AI investigations\n' >&2; exit 2; }
	@$(KUBECTL) -n kubescout get secret kubescout-model -o name || { \
		printf '❌ Model Secret is unavailable in the current context. Check access or run: make model-secret\n' >&2; exit 1; }
	$(KUBECTL) apply -k deploy
	$(KUBECTL) -n kubescout scale deployment/kubescout --replicas=1

undeploy: ## 🧹 Remove Deployment/RBAC; retain Incidents, Secret, namespace and CRDs (current context)
	$(KUBECTL) delete -f deploy/deployment.yaml -f deploy/rbac.yaml

dash-build: ## 🔨 Build the standalone dashboard (no JavaScript build step)
	$(GO) build -trimpath -o build/bin/kubescout-dashboard ./cmd/kubescout-dashboard

dash-run: dash-build ## 🌐 Run the KubeScout dashboard locally on 127.0.0.1:8000 (current context)
	./build/bin/kubescout-dashboard --auth-mode=kubeconfig --namespace=kubescout

dash-image: ## 📦 Build the separate non-root DASHBOARD_IMAGE
	$(CONTAINER_ENGINE) build $(if $(filter podman,$(notdir $(CONTAINER_ENGINE))),--ignorefile build/Dockerfile.dashboard.dockerignore,) \
		--platform "$(TARGETOS)/$(TARGETARCH)" \
		--build-arg TARGETOS="$(TARGETOS)" --build-arg TARGETARCH="$(TARGETARCH)" \
		-f build/Dockerfile.dashboard -t "$(DASHBOARD_IMAGE)" .

dash-kind-load: ## ☸ Import DASHBOARD_IMAGE into KIND_CLUSTER (does not deploy)
	@test -n "$(KIND_CLUSTER)" || { printf '❌ Set KIND_CLUSTER explicitly\n' >&2; exit 2; }
	@mkdir -p build/out
	@trap 'rm -f build/out/kubescout-dashboard.tar.tmp' EXIT; \
		$(CONTAINER_ENGINE) save "$(DASHBOARD_IMAGE)" > build/out/kubescout-dashboard.tar.tmp && \
		mv build/out/kubescout-dashboard.tar.tmp build/out/kubescout-dashboard.tar
	KIND_EXPERIMENTAL_PROVIDER="$(KIND_PROVIDER)" $(KIND) load image-archive build/out/kubescout-dashboard.tar --name "$(KIND_CLUSTER)"

dash-deploy: ## 🚀 Deploy only the dashboard (current context; imported image, namespace and CRD)
	$(KUBECTL) apply -k deploy/dashboard

dash-forward: ## 🌐 Open localhost:8000 access (current context)
	$(KUBECTL) -n kubescout port-forward --address=127.0.0.1 service/kubescout-dashboard 8000:8000

dash-undeploy: ## 🧹 Remove only dashboard resources; retain agent and Incidents (current context)
	$(KUBECTL) delete -f deploy/dashboard/deployment.yaml \
		-f deploy/dashboard/service.yaml -f deploy/dashboard/rbac.yaml