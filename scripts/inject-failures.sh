#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 || -z "$1" || -z "$2" ]]; then
  printf 'Usage: %s CONTEXT NAMESPACE\nUse a development cluster and a namespace watched by the observer.\n' "$0" >&2
  exit 2
fi

CTX=$1
NS=$2
TEST="health-demo-$(date +%s)-$RANDOM"
LABEL=kubescout-demo=true
if [[ ${#NS} -gt 63 || ! "$NS" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  printf 'Error: invalid namespace: %s\n' "$NS" >&2
  exit 2
fi

printf '%s\n' \
  '1) Image pull failure' \
  '2) Crash loop' \
  '3) Unschedulable pod' \
  '4) Failed Job' \
  '5) All five failures' \
  '6) Remove all injected failures' \
  '7) OOM-killed container (64 MiB limit)' \
  '0) Cancel'
if ! read -r -p 'Choose failures, separated by spaces (e.g. 1 3): ' -a choices; then
  printf 'Error: no selection received.\n' >&2
  exit 2
fi
if ((${#choices[@]} == 0)); then
  printf 'Error: choose at least one failure.\n' >&2
  exit 2
fi
for choice in "${choices[@]}"; do
  case "$choice" in
    0) printf 'Cancelled.\n'; exit 0 ;;
    1|2|3|4|5|6|7) ;;
    *) printf 'Error: invalid choice: %s\n' "$choice" >&2; exit 2 ;;
  esac
done
selection=" ${choices[*]} "
printf 'Context: %s\nNamespace: %s\n' "$CTX" "$NS"
if [[ "$selection" == *" 6 "* ]]; then
  if ((${#choices[@]} != 1)); then
    printf 'Error: choose removal on its own, without injection options.\n' >&2
    exit 2
  fi
  if ! read -r -p "Remove all health-demo failure fixtures in $CTX/$NS, including older unlabelled fixtures? [y/N] " confirm ||
    [[ "$confirm" != y && "$confirm" != Y ]]; then
    printf 'Cancelled.\n'
    exit 0
  fi
  fixtures=$(kubectl --context "$CTX" -n "$NS" get pods,jobs \
    -o 'jsonpath={range .items[*]}{.kind}{"|"}{.metadata.name}{"|"}{.metadata.uid}{"|"}{.metadata.labels.kubescout-demo}{"\n"}{end}')
  removed=0
  while IFS='|' read -r kind name uid owner; do
    [[ "$name" =~ ^health-demo-[0-9]+(-[0-9]+)?-(image|crash|pending|job|oom)$ ]] || continue
    [[ -z "$owner" || "$owner" == true ]] || continue
    case "$kind:$name" in
      Pod:*-image|Pod:*-crash|Pod:*-pending|Pod:*-oom)
        resource=pod
        path="/api/v1/namespaces/$NS/pods/$name"
        ;;
      Job:*-job)
        resource=job
        path="/apis/batch/v1/namespaces/$NS/jobs/$name"
        ;;
      *) continue ;;
    esac
    if [[ ! "$uid" =~ ^[a-zA-Z0-9-]+$ ]]; then
      printf 'Error: missing or invalid UID for %s/%s; refusing deletion.\n' "$resource" "$name" >&2
      exit 1
    fi
    kubectl --context "$CTX" -n "$NS" delete --raw="$path" -f - >/dev/null <<EOF
{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"$uid"},"propagationPolicy":"Foreground"}
EOF
    kubectl --context "$CTX" -n "$NS" wait --for=delete "$resource/$name" --timeout=60s
    removed=$((removed + 1))
  done <<< "$fixtures"
  printf 'Removed %s injected failure fixture(s).\n' "$removed"
  exit 0
fi
if [[ "$selection" == *" 5 "* ]]; then
  selection=" 1 2 3 4 7 "
fi

printf 'Fixture prefix: %s\n' "$TEST"

# Image pull failure.
if [[ "$selection" == *" 1 "* ]]; then
  printf 'Cleanup when finished: kubectl --context %q -n %q delete pod %q\n' "$CTX" "$NS" "$TEST-image"
  kubectl --context "$CTX" -n "$NS" run "$TEST-image" \
    --labels="$LABEL" \
    --image=busybox:deliberately-nonexistent-health-demo \
    --restart=Never
fi

# Crash loop.
if [[ "$selection" == *" 2 "* ]]; then
  printf 'Cleanup when finished: kubectl --context %q -n %q delete pod %q\n' "$CTX" "$NS" "$TEST-crash"
  kubectl --context "$CTX" -n "$NS" run "$TEST-crash" \
    --labels="$LABEL" \
    --image=busybox:1.37.0 \
    --restart=Always \
    --command -- sh -c 'sleep 2; exit 1'
fi

# Unschedulable pod.
if [[ "$selection" == *" 3 "* ]]; then
  printf 'Cleanup when finished: kubectl --context %q -n %q delete pod %q\n' "$CTX" "$NS" "$TEST-pending"
  kubectl --context "$CTX" -n "$NS" run "$TEST-pending" \
    --labels="$LABEL" \
    --image=busybox:1.37.0 \
    --restart=Never \
    --overrides='{"spec":{"nodeSelector":{"health-demo.invalid/node":"does-not-exist"}}}' \
    --command -- sleep 600
  printf 'The unschedulable finding needs the default 120s grace.\n'
fi

# Failed Job.
if [[ "$selection" == *" 4 "* ]]; then
  printf 'Cleanup when finished: kubectl --context %q -n %q delete job %q\n' "$CTX" "$NS" "$TEST-job"
  kubectl --context "$CTX" -n "$NS" create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $TEST-job
  labels:
    kubescout-demo: "true"
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 60
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: fail
          image: busybox:1.37.0
          command: ["sh", "-c", "exit 1"]
EOF
fi

# OOM kill retained in the terminated state rather than hidden by a quick restart.
if [[ "$selection" == *" 7 "* ]]; then
  printf 'Cleanup when finished: kubectl --context %q -n %q delete pod %q\n' "$CTX" "$NS" "$TEST-oom"
  kubectl --context "$CTX" -n "$NS" create -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $TEST-oom
  labels:
    kubescout-demo: "true"
spec:
  automountServiceAccountToken: false
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: oom
      image: python:3.13.9-alpine
      command: ["python3", "-c", "data = bytearray(128 * 1024 * 1024)"]
      resources:
        requests:
          cpu: 10m
          memory: 16Mi
        limits:
          cpu: 100m
          memory: 64Mi
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: ["ALL"]
EOF
  printf 'Allocates 128 MiB against a 64 MiB container limit; expected termination reason: OOMKilled.\n'
  printf 'Inspect termination: kubectl --context %q -n %q get pod %q -o yaml\n' "$CTX" "$NS" "$TEST-oom"
fi

printf '\nSelected failures created.\n'
