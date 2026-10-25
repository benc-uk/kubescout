.DEFAULT_GOAL := help

GO ?= go
CONTEXT ?=
NAMESPACE ?=
ARGS ?= --deterministic-only
ALLOW_WRITES ?=
ALLOW_MODEL ?=
CONTAINER_ENGINE ?= podman
IMAGE ?= localhost/k8s-health:stage3
KIND ?= kind
KIND_CLUSTER ?=
KIND_PROVIDER ?= podman
KUBECTL ?= kubectl

.PHONY: help setup build run once test check integration integration-model integration-deploy image kind-load deploy

help: ## 📖 Show available targets
	@awk 'BEGIN {FS = ":.*## "; print "\n☸ Kubernetes health agent\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2} END {print "\nWatch: make run CONTEXT=kind-dev NAMESPACE=default\nOnce:  make once CONTEXT=kind-dev NAMESPACE=default\nModel: override ARGS=\"\" to enable analysis\n"}' $(MAKEFILE_LIST)

setup: ## 📦 Download pinned Go dependencies
	$(GO) mod download
	$(GO) mod verify

build: ## 🔨 Build the Go binary
	@$(GO) build -trimpath -o build/bin/k8s-health ./cmd/k8s-health

run: ## 👀 Watch resource health (requires CONTEXT and NAMESPACE)
once: ## 🚀 Collect and report once (requires CONTEXT and NAMESPACE)

run once: build
	@test -n "$(CONTEXT)" || { printf '❌ Set CONTEXT, for example CONTEXT=kind-dev\n' >&2; exit 2; }
	@test -n "$(NAMESPACE)" || { printf '❌ Set NAMESPACE, for example NAMESPACE=default\n' >&2; exit 2; }
	@./build/bin/k8s-health --auth-mode kubeconfig --context "$(CONTEXT)" \
		--namespace "$(NAMESPACE)" $(if $(filter once,$@),--once,) $(ARGS)

test: ## 🧪 Run tests without a cluster or model connection
	$(GO) test -race -timeout 120s ./...

integration: ## ☸ Validate live kind with isolated fixtures (requires CONTEXT, ALLOW_WRITES=yes)
integration-model: ## 🤖 Validate two real model requests (also requires ALLOW_MODEL=yes)

integration integration-model: build
	@test -n "$(CONTEXT)" || { printf '❌ Set CONTEXT explicitly\n' >&2; exit 2; }
	@test "$(ALLOW_WRITES)" = yes || { printf '❌ Set ALLOW_WRITES=yes to authorise isolated fixture creation and cleanup\n' >&2; exit 2; }
	@if [ "$@" = integration-model ]; then test "$(ALLOW_MODEL)" = yes || { printf '❌ Set ALLOW_MODEL=yes to authorise two model requests\n' >&2; exit 2; }; fi
	@K8S_HEALTH_LIVE_CONTEXT="$(CONTEXT)" K8S_HEALTH_LIVE_WRITES="$(ALLOW_WRITES)" \
		K8S_HEALTH_LIVE_MODEL="$(ALLOW_MODEL)" $(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '$(if $(filter integration-model,$@),^TestLiveModel$$,^TestLive(Kubernetes|Connectivity)$$)' ./internal/integration

integration-deploy: build ## 🧪 Validate the imported image, RBAC and reports (requires CONTEXT, ALLOW_WRITES=yes)
	@test -n "$(CONTEXT)" && test "$(ALLOW_WRITES)" = yes || { printf '❌ Set CONTEXT and ALLOW_WRITES=yes\n' >&2; exit 2; }
	@K8S_HEALTH_LIVE_CONTEXT="$(CONTEXT)" K8S_HEALTH_LIVE_WRITES="$(ALLOW_WRITES)" \
		K8S_HEALTH_LIVE_MODEL="$(ALLOW_MODEL)" K8S_HEALTH_LIVE_CONTAINER=yes \
		K8S_HEALTH_CONTAINER_ENGINE="$(CONTAINER_ENGINE)" K8S_HEALTH_IMAGE="$(IMAGE)" \
		$(GO) test -tags=integration -count=1 -v \
		-timeout 10m -run '^TestLiveDeployment$$' ./internal/integration

check: ## 🔍 Check formatting, modules and static diagnostics
	@test -z "$$(gofmt -l cmd internal)" || { printf '❌ Run gofmt -w cmd internal\n' >&2; exit 1; }
	$(GO) mod tidy -diff
	$(GO) mod verify
	$(GO) vet -tags=integration ./...

image: ## 📦 Build the non-root container image
	$(CONTAINER_ENGINE) build $(if $(filter podman,$(notdir $(CONTAINER_ENGINE))),--ignorefile build/Dockerfile.dockerignore,) \
		-f build/Dockerfile -t "$(IMAGE)" .

kind-load: ## ☸ Import the built image (requires KIND_CLUSTER)
	@test -n "$(KIND_CLUSTER)" || { printf '❌ Set KIND_CLUSTER explicitly\n' >&2; exit 2; }
	@mkdir -p build/out
	@trap 'rm -f build/out/k8s-health.tar.tmp' EXIT; \
		$(CONTAINER_ENGINE) save "$(IMAGE)" > build/out/k8s-health.tar.tmp && \
		mv build/out/k8s-health.tar.tmp build/out/k8s-health.tar
	KIND_EXPERIMENTAL_PROVIDER="$(KIND_PROVIDER)" $(KIND) load image-archive build/out/k8s-health.tar --name "$(KIND_CLUSTER)"

deploy: ## 🚀 Apply the local Deployment (requires CONTEXT, ALLOW_WRITES=yes)
	@test -n "$(CONTEXT)" && test "$(ALLOW_WRITES)" = yes || { printf '❌ Set CONTEXT and ALLOW_WRITES=yes\n' >&2; exit 2; }
	$(KUBECTL) --context "$(CONTEXT)" apply -k deploy
