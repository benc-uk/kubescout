# ===== Stage 1: Build the Go binary =====
ARG BUILDPLATFORM
ARG TARGETPLATFORM
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.26-alpine@sha256:27f2d3b83cfc5803c8078f8e730af0f7c431bf067afaec8f60a36fb763187cba AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -mod=readonly -trimpath -buildvcs=false \
    -ldflags="-s -w -buildid=" -o /out/kubescout ./cmd/kubescout

# ===== Stage 2: Create the final non-root image =====
FROM --platform=$TARGETPLATFORM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=builder --chown=65532:65532 /out/kubescout /kubescout
USER 65532:65532
WORKDIR /
STOPSIGNAL SIGTERM

ENTRYPOINT ["/kubescout"]
