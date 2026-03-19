#!/bin/bash
# Blockchain QuickStart: Auto-configure N validators + M citizens on local Docker
# Usage: quickstart.sh <validators> <citizens> <channel> <image>
#
# This script uses `docker cp` instead of `-v` bind mounts so it works in
# Docker-in-Docker (DinD) environments where the inner Docker daemon shares
# the host socket and bind-mount paths resolve on the *host* filesystem.
set -e

NUM_VALIDATORS=${1:-${QS_VALIDATORS:-4}}
NUM_CITIZENS=${2:-${QS_CITIZENS:-0}}
CHANNEL=${3:-${QS_CHANNEL:-seoul}}
IMAGE=${4:-${QS_IMAGE:-20.20.0.13:80/iconloop-enterprise/goloop:v1.2.5-seoul-test}}
BLOCKCHAIN_SRC=${BLOCKCHAIN_SRC:-/blockchain}
WORK_DIR=${WORK_DIR:-/tmp/blockchain-data}
ISSUER_KS="${BLOCKCHAIN_SRC}/issuer.json"
ISSUER_PW="${QS_ISSUER_PW:-gochain}"
KS_PW="${QS_KS_PW:-gochain}"
NETWORK="${QS_NETWORK:-orch-internal}"
BASE_P2P_PORT=${QS_P2P_PORT:-7100}
BASE_RPC_PORT=${QS_RPC_PORT:-9100}
LOG_LEVEL="${QS_LOG_LEVEL:-trace}"

# Auto-detect platform for cross-arch images
PLATFORM_FLAG=""
HOST_ARCH=$(uname -m)
if [ "${HOST_ARCH}" = "arm64" ] || [ "${HOST_ARCH}" = "aarch64" ]; then
    PLATFORM_FLAG="--platform linux/amd64"
fi

echo "=== Blockchain QuickStart ==="
echo "Validators: ${NUM_VALIDATORS}, Citizens: ${NUM_CITIZENS}, Channel: ${CHANNEL}"
echo "Image: ${IMAGE}"
echo "Work dir: ${WORK_DIR}"

# Helper: run goloop command in a temporary container using docker cp
# Usage: run_goloop <local_work_dir> <commands...>
# Creates a temp container, copies files in, runs command, copies results back.
run_goloop() {
    local LOCAL_DIR="$1"
    shift
    local TMP_NAME="qs-tmp-$$-${RANDOM}"
    local GOLOOP_CMD="$*"
    # Create a stopped container; use --user root + sh -c to ensure /work is writable
    docker create ${PLATFORM_FLAG} --user root --name "${TMP_NAME}" \
        --entrypoint "sh" "${IMAGE}" \
        -c "mkdir -p /work && chmod 777 /work && ${GOLOOP_CMD}" > /dev/null 2>&1
    # Copy input files to /work inside the container
    if [ -d "${LOCAL_DIR}" ] && [ "$(ls -A ${LOCAL_DIR} 2>/dev/null)" ]; then
        docker cp "${LOCAL_DIR}/." "${TMP_NAME}:/work" 2>/dev/null || true
    fi
    # Start and wait
    docker start -a "${TMP_NAME}" 2>&1
    local EXIT_CODE=$?
    # Copy results back
    docker cp "${TMP_NAME}:/work/." "${LOCAL_DIR}/" 2>/dev/null || true
    docker rm -f "${TMP_NAME}" > /dev/null 2>&1 || true
    return ${EXIT_CODE}
}

# Helper: run goloop license gen with issuer keystore
# Usage: run_goloop_license <local_work_dir> <issuer_json_path> <commands...>
run_goloop_license() {
    local LOCAL_DIR="$1"
    local ISSUER_FILE="$2"
    shift 2
    local TMP_NAME="qs-tmp-$$-${RANDOM}"
    # Prepare readable copy of issuer.json
    local ISSUER_TMP="/tmp/.qs-issuer-$$"
    mkdir -p "${ISSUER_TMP}"
    cp "${ISSUER_FILE}" "${ISSUER_TMP}/issuer.json"
    chmod 644 "${ISSUER_TMP}/issuer.json"
    local GOLOOP_CMD="$*"
    # Create container with --user root for mkdir permissions
    docker create ${PLATFORM_FLAG} --user root --name "${TMP_NAME}" \
        --entrypoint "sh" "${IMAGE}" \
        -c "mkdir -p /issuer /work && chmod 777 /issuer /work && cp /goloop/conf/issuer.json /issuer/issuer.json && ${GOLOOP_CMD}" > /dev/null 2>&1
    # Copy issuer.json into /goloop/conf (writable path inside the image)
    docker cp "${ISSUER_TMP}/issuer.json" "${TMP_NAME}:/goloop/conf/issuer.json"
    rm -rf "${ISSUER_TMP}"
    docker start -a "${TMP_NAME}" 2>&1
    local EXIT_CODE=$?
    docker cp "${TMP_NAME}:/work/." "${LOCAL_DIR}/" 2>/dev/null || true
    docker rm -f "${TMP_NAME}" > /dev/null 2>&1 || true
    return ${EXIT_CODE}
}

# ========== Phase 0: Clean up previous deployment ==========
echo "[0/7] Cleaning up previous deployment..."
docker ps -a --filter "label=blockchain.channel=${CHANNEL}" -q | xargs -r docker rm -f 2>&1 || true
docker ps -a --filter "label=blockchain.channel" -q | xargs -r docker rm -f 2>&1 || true
# Clean temp containers
docker ps -a --filter "name=qs-tmp-" -q | xargs -r docker rm -f 2>&1 || true

if [ -d "${WORK_DIR}" ]; then
    rm -rf "${WORK_DIR:?}"/* "${WORK_DIR:?}"/.* 2>&1 || true
    echo "  Cleaned previous data"
fi
mkdir -p "${WORK_DIR}"
chmod 777 "${WORK_DIR}"

# ========== Phase 1: Generate Keystores ==========
echo "[1/7] Generating keystores..."
ADDRESSES=()
TOTAL=$((NUM_VALIDATORS + NUM_CITIZENS))

for i in $(seq 0 $((TOTAL - 1))); do
    NODE_DIR="${WORK_DIR}/node-${i}"
    CONF_DIR="${NODE_DIR}/conf"
    mkdir -p "${CONF_DIR}" "${NODE_DIR}/data"
    chmod 777 "${CONF_DIR}" "${NODE_DIR}/data"

    if [ ! -f "${CONF_DIR}/keystore.json" ]; then
        run_goloop "${CONF_DIR}" \
            goloop ks gen --out /work/keystore.json --password "${KS_PW}"
        echo -n "${KS_PW}" > "${CONF_DIR}/keysecret"
    fi

    ADDR=$(python3 -c "import json; print(json.load(open('${CONF_DIR}/keystore.json'))['address'])")
    ADDRESSES+=("${ADDR}")
    echo "  Node ${i}: ${ADDR}"
done

# ========== Phase 2: Genesis ==========
echo "[2/7] Generating genesis..."
VALIDATOR_ADDRS=("${ADDRESSES[@]:0:${NUM_VALIDATORS}}")
GOD_ADDR="${VALIDATOR_ADDRS[0]}"
GENESIS_DIR="${WORK_DIR}/node-0/conf"

VAL_ARGS=""
for addr in "${VALIDATOR_ADDRS[@]}"; do
    VAL_ARGS="${VAL_ARGS} ${addr}"
done

run_goloop "${GENESIS_DIR}" \
    goloop gn gen \
    --out /work/genesis.json \
    --god "${GOD_ADDR}" \
    --config "revision=0x8,minimizeBlockGen=0x1" \
    ${VAL_ARGS}

echo "  Genesis created with ${NUM_VALIDATORS} validators, god=${GOD_ADDR}"

# ========== Phase 3: Genesis Storage (gs.zip) ==========
echo "[3/7] Generating gs.zip..."
run_goloop "${GENESIS_DIR}" \
    goloop gs gen --input /work/genesis.json --out /work/gs.zip

# Distribute gs.zip to all nodes
for i in $(seq 1 $((TOTAL - 1))); do
    cp "${GENESIS_DIR}/gs.zip" "${WORK_DIR}/node-${i}/conf/gs.zip"
done
echo "  gs.zip distributed to ${TOTAL} nodes"

# ========== Phase 4: License ==========
echo "[4/7] Generating license..."
ALL_ADDR_ARGS=""
for addr in "${ADDRESSES[@]}"; do
    ALL_ADDR_ARGS="${ALL_ADDR_ARGS} ${addr}"
done

run_goloop_license "${WORK_DIR}" "${ISSUER_KS}" \
    goloop lc gen \
    --keystore /issuer/issuer.json \
    --password "${ISSUER_PW}" \
    --out /work/license.json \
    --duration infinite \
    --subject "${CHANNEL}" \
    ${ALL_ADDR_ARGS}

for i in $(seq 0 $((TOTAL - 1))); do
    cp "${WORK_DIR}/license.json" "${WORK_DIR}/node-${i}/conf/license.json"
done
echo "  License generated for ${TOTAL} nodes"

# ========== Phase 5: Start Containers ==========
echo "[5/7] Starting containers..."
CONTAINER_IDS=()

for i in $(seq 0 $((TOTAL - 1))); do
    NODE_DIR="${WORK_DIR}/node-${i}"
    P2P_PORT=$((BASE_P2P_PORT + i))
    RPC_PORT=$((BASE_RPC_PORT + i))
    CONTAINER_NAME="blockchain-${CHANNEL}-${i}"
    ROLE="validator"
    [ $i -ge $NUM_VALIDATORS ] && ROLE="citizen"

    docker rm -f "${CONTAINER_NAME}" 2>&1 || true

    # Build seed nodes
    if [ $i -lt $NUM_VALIDATORS ]; then
        if [ $NUM_VALIDATORS -eq 1 ]; then
            SEEDS=""
        elif [ $NUM_VALIDATORS -eq 2 ]; then
            SEED_1=$(( (i + 1) % NUM_VALIDATORS ))
            SEEDS="blockchain-${CHANNEL}-${SEED_1}:8080"
        else
            SEED_1=$(( (i + 1) % NUM_VALIDATORS ))
            SEED_2=$(( (i + 2) % NUM_VALIDATORS ))
            SEEDS="blockchain-${CHANNEL}-${SEED_1}:8080,blockchain-${CHANNEL}-${SEED_2}:8080"
        fi
    else
        if [ $NUM_VALIDATORS -ge 2 ]; then
            SEEDS="blockchain-${CHANNEL}-0:8080,blockchain-${CHANNEL}-1:8080"
        else
            SEEDS="blockchain-${CHANNEL}-0:8080"
        fi
    fi

    # Create goloop.env
    cat > "${NODE_DIR}/goloop.env" <<EOF
GOLOOP_NODE_DIR=/goloop/data
GOLOOP_ENGINES=python
GOLOOP_P2P=${CONTAINER_NAME}:8080
GOLOOP_P2P_LISTEN=:8080
GOLOOP_RPC_ADDR=:9080
GOLOOP_RPC_DUMP=false
GOLOOP_KEY_STORE=/goloop/conf/keystore.json
GOLOOP_KEY_SECRET=/goloop/conf/keysecret
GOLOOP_LICENSE_FILE=/goloop/conf/license.json
GOLOOP_CONSOLE_LEVEL=warn
GOLOOP_LOG_LEVEL=${LOG_LEVEL}
GOLOOP_LOG_WRITER_FILENAME=/goloop/data/log/goloop.log
GOLOOP_LOG_WRITER_COMPRESS=true
GOLOOP_LOG_WRITER_MAXSIZE=100
EOF

    # Append user-defined env vars
    env | grep '^QS_ENV_' | while IFS='=' read -r key val; do
        REAL_KEY="${key#QS_ENV_}"
        echo "${REAL_KEY}=${val}" >> "${NODE_DIR}/goloop.env"
    done

    chmod -R 777 "${NODE_DIR}/conf" "${NODE_DIR}/data" 2>&1 || true

    # Create container (without volume mounts), then copy files in
    # Use --user root because docker cp sets root ownership on copied files
    CID=$(docker create ${PLATFORM_FLAG} \
        --user root \
        --name "${CONTAINER_NAME}" \
        --network "${NETWORK}" \
        --env-file "${NODE_DIR}/goloop.env" \
        -p "${P2P_PORT}:8080" \
        -p "${RPC_PORT}:9080" \
        --ulimit nofile=98304:98304 \
        -l "blockchain.role=${ROLE}" \
        -l "blockchain.channel=${CHANNEL}" \
        -l "blockchain.index=${i}" \
        -l "blockchain.p2p_port=${P2P_PORT}" \
        -l "blockchain.rpc_port=${RPC_PORT}" \
        "${IMAGE}")

    # Copy conf and data into the container
    docker cp "${NODE_DIR}/conf/." "${CONTAINER_NAME}:/goloop/conf/"
    docker cp "${NODE_DIR}/data/." "${CONTAINER_NAME}:/goloop/data/" 2>/dev/null || true

    # Start the container
    docker start "${CONTAINER_NAME}"

    CONTAINER_IDS+=("${CID:0:12}")
    echo "  ${CONTAINER_NAME} (${ROLE}) -> p2p:${P2P_PORT} rpc:${RPC_PORT}"
done

# ========== Phase 6: Wait & Join Chains ==========
echo "[6/7] Waiting for containers to initialize..."
sleep 8

echo "  Joining chains..."
for i in $(seq 0 $((TOTAL - 1))); do
    CONTAINER_NAME="blockchain-${CHANNEL}-${i}"

    if [ $i -lt $NUM_VALIDATORS ]; then
        if [ $NUM_VALIDATORS -eq 1 ]; then
            SEED_PART=""
        elif [ $NUM_VALIDATORS -eq 2 ]; then
            SEED_1=$(( (i + 1) % NUM_VALIDATORS ))
            SEED_PART="--seed blockchain-${CHANNEL}-${SEED_1}:8080"
        else
            SEED_1=$(( (i + 1) % NUM_VALIDATORS ))
            SEED_2=$(( (i + 2) % NUM_VALIDATORS ))
            SEED_PART="--seed blockchain-${CHANNEL}-${SEED_1}:8080,blockchain-${CHANNEL}-${SEED_2}:8080"
        fi
        JOIN_CMD="goloop chain join --genesis /goloop/conf/gs.zip ${SEED_PART} --channel ${CHANNEL}"
    else
        if [ $NUM_VALIDATORS -ge 2 ]; then
            SEED_PART="--seed blockchain-${CHANNEL}-0:8080,blockchain-${CHANNEL}-1:8080"
        else
            SEED_PART="--seed blockchain-${CHANNEL}-0:8080"
        fi
        JOIN_CMD="goloop chain join --genesis /goloop/conf/gs.zip ${SEED_PART} --channel ${CHANNEL} --role 0"
    fi

    docker exec "${CONTAINER_NAME}" sh -c "${JOIN_CMD}" 2>&1 && \
        echo "  ${CONTAINER_NAME}: joined" || \
        echo "  ${CONTAINER_NAME}: join failed (may already be joined)"

    docker exec "${CONTAINER_NAME}" goloop chain start "${CHANNEL}" 2>&1 && \
        echo "  ${CONTAINER_NAME}: started" || \
        echo "  ${CONTAINER_NAME}: start failed (may already be running)"
done

# ========== Phase 7: Verify ==========
echo "[7/7] Verifying..."
sleep 3
for i in $(seq 0 $((TOTAL - 1))); do
    CONTAINER_NAME="blockchain-${CHANNEL}-${i}"
    STATUS=$(docker exec "${CONTAINER_NAME}" goloop chain ls 2>&1 | tail -1 || echo "error")
    echo "  ${CONTAINER_NAME}: ${STATUS}"
done

echo ""
echo "=== Blockchain QuickStart Complete ==="
echo "Validators: ${NUM_VALIDATORS}, Citizens: ${NUM_CITIZENS}"
echo "Channel: ${CHANNEL}"
echo "RPC endpoints: localhost:${BASE_RPC_PORT} ~ localhost:$((BASE_RPC_PORT + TOTAL - 1))"
echo "API: http://localhost:${BASE_RPC_PORT}/api/v3/${CHANNEL}"

# Output JSON result
python3 -c "
import json
result = {
    'success': True,
    'channel': '${CHANNEL}',
    'validators': ${NUM_VALIDATORS},
    'citizens': ${NUM_CITIZENS},
    'containers': [$(printf '"%s",' "${CONTAINER_IDS[@]}" | sed 's/,$//')],
    'rpc_base_port': ${BASE_RPC_PORT},
    'p2p_base_port': ${BASE_P2P_PORT},
}
print(json.dumps(result))
" > "${WORK_DIR}/quickstart-result.json"
