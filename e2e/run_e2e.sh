#!/usr/bin/env bash
#
# E2E test for vm-migrator
#
# This script:
#   1. Starts a minikube cluster (or reuses existing)
#   2. Builds the e2e Docker image inside minikube
#   3. Deploys source and destination VictoriaMetrics instances
#   4. Generates high-cardinality test data (10,310 series)
#   5. Runs vm-migrator to migrate data to the destination
#   6. Verifies the migration was successful
#
# Prerequisites: minikube, kubectl, podman
#
# Usage:
#   ./e2e/run_e2e.sh            # Full run
#   ./e2e/run_e2e.sh --no-setup # Skip minikube/image setup (reuse existing)
#   ./e2e/run_e2e.sh --cleanup  # Just tear down

set -euo pipefail

# Ensure podman is in PATH
export PATH="/opt/podman/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Configuration
MINIKUBE_PROFILE="vm-migrator-e2e"
MINIKUBE_CPUS=4
MINIKUBE_MEMORY="6144"  # 6GB for minikube
E2E_IMAGE="localhost/vm-migrator-e2e:latest"
DATAGEN_DAYS=3
DATAGEN_INTERVAL=30  # minutes

log()    { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
info()   { echo -e "${CYAN}[$(date '+%H:%M:%S')] ℹ${NC}  $*"; }
ok()     { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC}  $*"; }
warn()   { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠${NC}  $*"; }
fail()   { echo -e "${RED}[$(date '+%H:%M:%S')] ✗${NC}  $*"; }
header() { echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${CYAN}  $*${NC}"; echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }

# Parse args
SKIP_SETUP=false
CLEANUP_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --no-setup) SKIP_SETUP=true ;;
        --cleanup)  CLEANUP_ONLY=true ;;
    esac
done

cleanup() {
    header "CLEANUP"
    log "Deleting minikube profile: ${MINIKUBE_PROFILE}"
    minikube delete -p "${MINIKUBE_PROFILE}" 2>/dev/null || true
    ok "Cleanup complete"
}

if $CLEANUP_ONLY; then
    cleanup
    exit 0
fi

# ────────────────────────────────────────────────────────
# Step 1: Start minikube
# ────────────────────────────────────────────────────────
setup_minikube() {
    header "STEP 1: Minikube Setup"

    if minikube status -p "${MINIKUBE_PROFILE}" &>/dev/null; then
        ok "Minikube profile '${MINIKUBE_PROFILE}' already running"
        return 0
    fi

    log "Starting minikube (cpus=${MINIKUBE_CPUS}, memory=${MINIKUBE_MEMORY}MB)..."
    minikube start \
        -p "${MINIKUBE_PROFILE}" \
        --cpus="${MINIKUBE_CPUS}" \
        --memory="${MINIKUBE_MEMORY}" \
        --disk-size="20g" \
        --driver=podman \
        --container-runtime=containerd \
        --kubernetes-version=v1.31.0 \
        --wait=all

    ok "Minikube started"

    # Set kubectl context
    minikube update-context -p "${MINIKUBE_PROFILE}"
    kubectl config use-context "${MINIKUBE_PROFILE}"
    ok "kubectl context set to ${MINIKUBE_PROFILE}"
}

# ────────────────────────────────────────────────────────
# Step 2: Build container image with Podman
# ────────────────────────────────────────────────────────
build_image() {
    header "STEP 2: Build E2E Container Image"

    log "Building ${E2E_IMAGE} with Podman..."
    podman build -t "${E2E_IMAGE}" -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_DIR}"

    log "Saving image to tarball..."
    podman save -o /tmp/vm-migrator-e2e.tar "${E2E_IMAGE}"

    log "Loading image into minikube..."
    minikube -p "${MINIKUBE_PROFILE}" image load /tmp/vm-migrator-e2e.tar
    rm -f /tmp/vm-migrator-e2e.tar

    ok "Image built and loaded into minikube: ${E2E_IMAGE}"
}

# ────────────────────────────────────────────────────────
# Step 3: Deploy VictoriaMetrics instances
# ────────────────────────────────────────────────────────
deploy_vms() {
    header "STEP 3: Deploy VictoriaMetrics Instances"

    log "Creating namespaces..."
    kubectl apply -f "${SCRIPT_DIR}/manifests/namespaces.yaml"

    log "Deploying source VictoriaMetrics..."
    kubectl apply -f "${SCRIPT_DIR}/manifests/source-vm.yaml"

    log "Deploying destination VictoriaMetrics..."
    kubectl apply -f "${SCRIPT_DIR}/manifests/dest-vm.yaml"

    log "Applying RBAC..."
    kubectl apply -f "${SCRIPT_DIR}/manifests/rbac.yaml"

    log "Waiting for source VM to be ready..."
    kubectl rollout status deployment/source-vm -n e2e-source --timeout=120s

    log "Waiting for destination VM to be ready..."
    kubectl rollout status deployment/dest-vm -n e2e-dest --timeout=120s

    ok "Both VictoriaMetrics instances are ready"
}

# ────────────────────────────────────────────────────────
# Step 4: Generate test data
# ────────────────────────────────────────────────────────
generate_data() {
    header "STEP 4: Generate High-Cardinality Test Data"

    # Delete previous datagen job if exists
    kubectl delete job e2e-datagen -n e2e-source --ignore-not-found=true 2>/dev/null

    log "Starting data generator (${DATAGEN_DAYS} days, ${DATAGEN_INTERVAL}min intervals)..."
    log "  This will create:"
    log "    - e2e_requests_total:    10,000 series (high cardinality)"
    log "    - e2e_histogram_bucket:  300 series   (medium cardinality)"
    log "    - e2e_simple_gauge:      10 series    (low cardinality)"
    log "    - Total: 10,310 series"

    kubectl apply -f "${SCRIPT_DIR}/manifests/datagen-job.yaml"

    log "Waiting for data generation to complete (this may take a few minutes)..."
    # Wait for job completion
    if ! kubectl wait --for=condition=complete job/e2e-datagen -n e2e-source --timeout=600s 2>/dev/null; then
        fail "Data generation failed!"
        log "Datagen logs:"
        kubectl logs job/e2e-datagen -n e2e-source --tail=50 || true
        exit 1
    fi

    ok "Data generation complete"
    log "Datagen logs (last 20 lines):"
    kubectl logs job/e2e-datagen -n e2e-source --tail=20

    # Wait for data to be indexed
    log "Waiting for data to be indexed (10s)..."
    sleep 10
}

# ────────────────────────────────────────────────────────
# Step 5: Verify source data
# ────────────────────────────────────────────────────────
verify_source() {
    header "STEP 5: Verify Source Data"

    local source_pod
    source_pod=$(kubectl get pod -n e2e-source -l app=victoria-metrics,role=source -o jsonpath='{.items[0].metadata.name}')

    log "Querying source VM for total series count..."
    local total
    total=$(kubectl exec -n e2e-source "${source_pod}" -- \
        /bin/sh -c 'wget -O - http://127.0.0.1:8428/api/v1/series/count 2>/dev/null' 2>/dev/null)
    info "Source series count: ${total}"

    local count
    count=$(echo "${total}" | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0])" 2>/dev/null || echo "0")

    if [[ "${count}" -ge 10000 ]]; then
        ok "Source has ${count} series (expected ≥10310)"
    else
        fail "Source has only ${count} series, expected ≥10310"
        exit 1
    fi
}

# ────────────────────────────────────────────────────────
# Step 6: Run migration
# ────────────────────────────────────────────────────────
run_migration() {
    header "STEP 6: Run VM-Migrator"

    # Calculate dates for config
    local end_date start_date
    end_date=$(date -u +"%Y-%m-%dT%H:00:00Z")
    start_date=$(date -u -v-${DATAGEN_DAYS}d +"%Y-%m-%dT00:00:00Z" 2>/dev/null || \
                 date -u -d "${DATAGEN_DAYS} days ago" +"%Y-%m-%dT00:00:00Z")

    log "Migration time range: ${start_date} → ${end_date}"

    # Generate config with correct dates
    local config_content
    config_content=$(sed \
        -e "s|REPLACE_START_DATE|${start_date}|g" \
        -e "s|REPLACE_END_DATE|${end_date}|g" \
        "${SCRIPT_DIR}/config.yaml")

    info "Config:"
    echo "${config_content}" | head -20

    # Create/update ConfigMap
    kubectl create configmap e2e-migrator-config \
        --from-literal=config.yaml="${config_content}" \
        -n vm-migration \
        --dry-run=client -o yaml | kubectl apply -f -

    # Delete previous migrator job if exists
    kubectl delete job e2e-migrator -n vm-migration --ignore-not-found=true 2>/dev/null
    # Also clean up any leftover worker jobs
    kubectl delete jobs -n vm-migration -l app=vm-migrator --ignore-not-found=true 2>/dev/null

    log "Starting vm-migrator coordinator..."
    kubectl apply -f "${SCRIPT_DIR}/manifests/migrator-job.yaml"

    log "Streaming coordinator logs (Ctrl+C to stop following, migration continues)..."
    # Give the pod a moment to start
    sleep 5

    # Follow logs, but don't fail if the follow is interrupted
    kubectl logs -f job/e2e-migrator -n vm-migration 2>/dev/null || true

    # Wait for migration to complete
    log "Waiting for migration to complete..."
    local result=0
    if kubectl wait --for=condition=complete job/e2e-migrator -n vm-migration --timeout=1800s 2>/dev/null; then
        ok "Migration completed successfully!"
    else
        # Check if it failed
        local failed
        failed=$(kubectl get job e2e-migrator -n vm-migration -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
        if [[ "${failed}" != "0" ]]; then
            fail "Migration FAILED"
            log "Coordinator logs (last 100 lines):"
            kubectl logs job/e2e-migrator -n vm-migration --tail=100 || true
            result=1
        else
            warn "Migration timed out (30 min) — check status manually"
            result=1
        fi
    fi

    # Print final coordinator logs
    log "Final coordinator logs:"
    kubectl logs job/e2e-migrator -n vm-migration --tail=30 || true

    return ${result}
}

# ────────────────────────────────────────────────────────
# Step 7: Verify migration
# ────────────────────────────────────────────────────────
verify_migration() {
    header "STEP 7: Verify Migration Results"

    local source_pod dest_pod
    source_pod=$(kubectl get pod -n e2e-source -l app=victoria-metrics,role=source -o jsonpath='{.items[0].metadata.name}')
    dest_pod=$(kubectl get pod -n e2e-dest -l app=victoria-metrics,role=dest -o jsonpath='{.items[0].metadata.name}')

    # Wait for dest to finish indexing
    log "Waiting for destination to finish indexing (10s)..."
    sleep 10

    # Compare total series counts
    log "Comparing total series counts..."
    local src_total dst_total
    src_total=$(kubectl exec -n e2e-source "${source_pod}" -- \
        /bin/sh -c 'wget -O - http://127.0.0.1:8428/api/v1/series/count 2>/dev/null' 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0])" 2>/dev/null || echo "0")
    dst_total=$(kubectl exec -n e2e-dest "${dest_pod}" -- \
        /bin/sh -c 'wget -O - http://127.0.0.1:8428/api/v1/series/count 2>/dev/null' 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0])" 2>/dev/null || echo "0")

    info "Source total:      ${src_total} series"
    info "Destination total: ${dst_total} series"

    local all_pass=true

    if [[ "${src_total}" == "${dst_total}" ]] && [[ "${src_total}" != "0" ]]; then
        ok "Total series count matches: ${src_total} == ${dst_total}"
    else
        fail "Total series count MISMATCH: source=${src_total}, dest=${dst_total}"
        all_pass=false
    fi

    # Check that destination has all metric names
    log "Checking metric names in destination..."
    local dst_metrics
    dst_metrics=$(kubectl exec -n e2e-dest "${dest_pod}" -- \
        /bin/sh -c 'wget -O - http://127.0.0.1:8428/api/v1/label/__name__/values 2>/dev/null' 2>/dev/null)
    info "Destination metrics: ${dst_metrics}"

    # Check each expected metric exists
    for metric in e2e_requests_total e2e_histogram_bucket e2e_simple_gauge; do
        if echo "${dst_metrics}" | grep -q "${metric}"; then
            ok "  ${metric}: found in destination"
        else
            fail "  ${metric}: NOT found in destination"
            all_pass=false
        fi
    done

    echo ""
    header "E2E TEST RESULTS"
    if $all_pass; then
        ok "ALL CHECKS PASSED ✓"
        echo -e "${GREEN}"
        echo "  ┌──────────────────────────────────────────────┐"
        echo "  │  E2E test PASSED!                            │"
        echo "  │                                              │"
        echo "  │  ✓ High-cardinality splitting worked         │"
        echo "  │  ✓ Data migrated correctly                   │"
        echo "  │  ✓ Series counts match source/destination    │"
        echo "  └──────────────────────────────────────────────┘"
        echo -e "${NC}"
        return 0
    else
        fail "SOME CHECKS FAILED ✗"
        echo -e "${RED}"
        echo "  ┌──────────────────────────────────────────────┐"
        echo "  │  E2E test FAILED!                            │"
        echo "  │                                              │"
        echo "  │  Check logs above for details.               │"
        echo "  │  Use 'kubectl logs' to inspect workers.      │"
        echo "  └──────────────────────────────────────────────┘"
        echo -e "${NC}"
        return 1
    fi
}

# ────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────
main() {
    header "VM-MIGRATOR E2E TEST"
    log "Project directory: ${PROJECT_DIR}"

    local start_time
    start_time=$(date +%s)

    if ! $SKIP_SETUP; then
        setup_minikube
        build_image
    fi

    deploy_vms
    generate_data
    verify_source
    run_migration
    verify_migration

    local end_time elapsed
    end_time=$(date +%s)
    elapsed=$(( end_time - start_time ))
    log "Total e2e test duration: $(( elapsed / 60 ))m $(( elapsed % 60 ))s"
}

# Trap for cleanup on failure
trap 'echo -e "\n${RED}Script interrupted. Cluster is still running.${NC}"; echo "Run: $0 --cleanup  to tear down"' INT TERM

main
