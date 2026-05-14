#!/usr/bin/env bash
#
# Tear down the e2e test environment.
#
# Usage:
#   ./e2e/teardown.sh          # Delete just the K8s resources
#   ./e2e/teardown.sh --all    # Delete the entire minikube cluster

set -euo pipefail

MINIKUBE_PROFILE="vm-migrator-e2e"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()  { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC}  $*"; }

DELETE_CLUSTER=false
for arg in "$@"; do
    case "$arg" in
        --all) DELETE_CLUSTER=true ;;
    esac
done

# Ensure we're using the right context
minikube update-context -p "${MINIKUBE_PROFILE}" 2>/dev/null || true
kubectl config use-context "${MINIKUBE_PROFILE}" 2>/dev/null || true

log "Cleaning up worker jobs..."
kubectl delete jobs -n vm-migration -l app=vm-migrator --ignore-not-found=true 2>/dev/null || true

log "Cleaning up migrator job..."
kubectl delete job e2e-migrator -n vm-migration --ignore-not-found=true 2>/dev/null || true

log "Cleaning up datagen job..."
kubectl delete job e2e-datagen -n e2e-source --ignore-not-found=true 2>/dev/null || true

log "Cleaning up ConfigMap..."
kubectl delete configmap e2e-migrator-config -n vm-migration --ignore-not-found=true 2>/dev/null || true

log "Deleting VictoriaMetrics deployments..."
kubectl delete -f "$(dirname "$0")/manifests/source-vm.yaml" --ignore-not-found=true 2>/dev/null || true
kubectl delete -f "$(dirname "$0")/manifests/dest-vm.yaml" --ignore-not-found=true 2>/dev/null || true

log "Deleting RBAC..."
kubectl delete -f "$(dirname "$0")/manifests/rbac.yaml" --ignore-not-found=true 2>/dev/null || true

log "Deleting namespaces..."
kubectl delete -f "$(dirname "$0")/manifests/namespaces.yaml" --ignore-not-found=true 2>/dev/null || true

if $DELETE_CLUSTER; then
    log "Deleting minikube cluster..."
    minikube delete -p "${MINIKUBE_PROFILE}" 2>/dev/null || true
    ok "Minikube cluster deleted"
else
    ok "K8s resources cleaned up. Minikube cluster still running."
    log "To delete the cluster: $0 --all"
fi

ok "Teardown complete"
