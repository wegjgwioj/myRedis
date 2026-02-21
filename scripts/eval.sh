#!/usr/bin/env bash
set -euo pipefail

# MyRedis 一键评估脚本（Linux / macOS / WSL2）
#
# 说明：
# - 这是 scripts/eval.ps1 的简化 Bash 版本，主目标是“可复现验证闭环”。
# - 功能验收以 go test + 内置 eval_client 为主；性能基准（redis-benchmark）仅记录不设阈值。
#
# 可选环境变量：
#   BASE_PORT=6399 NODE_COUNT=3 VNODES=160 MAX_BYTES=104857600 SKIP_BENCHMARK=1

BASE_PORT="${BASE_PORT:-6399}"
NODE_COUNT="${NODE_COUNT:-3}"
VNODES="${VNODES:-160}"
MAX_BYTES="${MAX_BYTES:-104857600}"
SKIP_BENCHMARK="${SKIP_BENCHMARK:-1}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TS="$(date +%Y%m%d-%H%M%S)"
ARTIFACTS_ROOT="${ROOT_DIR}/artifacts/eval/${TS}"
BIN_DIR="${ROOT_DIR}/artifacts/bin"

mkdir -p "${ARTIFACTS_ROOT}" "${BIN_DIR}"

wait_port() {
  local host="$1" port="$2" timeout="${3:-10}"
  local end=$((SECONDS + timeout))
  while ((SECONDS < end)); do
    if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timeout waiting for ${host}:${port}" >&2
  return 1
}

ensure_stopped() {
  local client_bin="$1"; shift
  local addrs=("$@")
  for a in "${addrs[@]}"; do
    "${client_bin}" --addr "${a}" SHUTDOWN >/dev/null 2>&1 || true
  done
}

pushd "${ROOT_DIR}" >/dev/null

{
  echo "{"
  echo "  \"timestamp\": \"$(date -Iseconds)\","
  echo "  \"os\": \"$(uname -a | sed 's/\"/\\\\\"/g')\","
  echo "  \"go_version\": \"$(go version | sed 's/\"/\\\\\"/g')\","
  if git rev-parse HEAD >/dev/null 2>&1; then
    echo "  \"git_commit\": \"$(git rev-parse HEAD)\","
  else
    echo "  \"git_commit\": \"\","
  fi
  echo "  \"note\": \"bench 为参考值，不设阈值\""
  echo "}"
} >"${ARTIFACTS_ROOT}/env.json"

go test ./... 2>&1 | tee "${ARTIFACTS_ROOT}/go_test.txt"

SERVER_BIN="${BIN_DIR}/myredis-server"
CLIENT_BIN="${BIN_DIR}/myredis-eval-client"
go build -o "${SERVER_BIN}" ./cmd
go build -o "${CLIENT_BIN}" ./cmd/eval_client

addrs=()
for ((i = 0; i < NODE_COUNT; i++)); do
  addrs+=("127.0.0.1:$((BASE_PORT + i))")
done
nodes_arg="$(IFS=,; echo "${addrs[*]}")"

for eviction in lru lfu; do
  run_dir="${ARTIFACTS_ROOT}/${eviction}"
  log_dir="${run_dir}/logs"
  aof_dir="${run_dir}/aof"
  mkdir -p "${log_dir}" "${aof_dir}"

  pids=()
  # 启动 3 节点（首次：清空 AOF）
  for a in "${addrs[@]}"; do
    port="${a##*:}"
    aof_file="${aof_dir}/node-${port}.aof"
    rm -f "${aof_file}"
    "${SERVER_BIN}" \
      --addr "${a}" \
      --nodes "${nodes_arg}" \
      --aof "${aof_file}" \
      --appendfsync everysec \
      --eviction "${eviction}" \
      --max-bytes "${MAX_BYTES}" \
      --vnodes "${VNODES}" \
      >"${log_dir}/node-${port}.out.log" 2>"${log_dir}/node-${port}.err.log" &
    pids+=("$!")
  done
  for a in "${addrs[@]}"; do
    wait_port "127.0.0.1" "${a##*:}" 10
  done

  # 分布式功能验收
  "${CLIENT_BIN}" --addr "${addrs[0]}" --nodes "${nodes_arg}" --vnodes "${VNODES}" --scenario distributed \
    | tee "${run_dir}/functional.json" >/dev/null

  if ! grep -q '"ok"[[:space:]]*:[[:space:]]*true' "${run_dir}/functional.json"; then
    echo "functional scenario failed (${eviction})" >&2
    ensure_stopped "${CLIENT_BIN}" "${addrs[@]}"
    for pid in "${pids[@]}"; do kill -9 "${pid}" >/dev/null 2>&1 || true; done
    exit 1
  fi

  cat >"${run_dir}/functional.md" <<EOF
# Functional Check (scenario=distributed)

- eviction: ${eviction}
- nodes: ${nodes_arg}
- result: true
- details: functional.json
EOF

  # AOF 恢复验收
  "${CLIENT_BIN}" --addr "${addrs[0]}" SET aofKey value >/dev/null
  "${CLIENT_BIN}" --addr "${addrs[0]}" EXPIRE aofKey 5 >/dev/null
  sleep 2

  ensure_stopped "${CLIENT_BIN}" "${addrs[@]}"
  for pid in "${pids[@]}"; do wait "${pid}" >/dev/null 2>&1 || true; done

  # 重启（保留 AOF）
  pids=()
  for a in "${addrs[@]}"; do
    port="${a##*:}"
    aof_file="${aof_dir}/node-${port}.aof"
    "${SERVER_BIN}" \
      --addr "${a}" \
      --nodes "${nodes_arg}" \
      --aof "${aof_file}" \
      --appendfsync everysec \
      --eviction "${eviction}" \
      --max-bytes "${MAX_BYTES}" \
      --vnodes "${VNODES}" \
      >>"${log_dir}/node-${port}.out.log" 2>>"${log_dir}/node-${port}.err.log" &
    pids+=("$!")
  done
  for a in "${addrs[@]}"; do
    wait_port "127.0.0.1" "${a##*:}" 10
  done

  get_json="$("${CLIENT_BIN}" --addr "${addrs[0]}" GET aofKey)"
  echo "${get_json}" | grep -q '"type"[[:space:]]*:[[:space:]]*"bulk"' || { echo "AOF restore GET type failed"; exit 1; }
  echo "${get_json}" | grep -q '"value"[[:space:]]*:[[:space:]]*"value"' || { echo "AOF restore GET value failed"; exit 1; }

  ttl_json="$("${CLIENT_BIN}" --addr "${addrs[0]}" TTL aofKey)"
  echo "${ttl_json}" | grep -q '"type"[[:space:]]*:[[:space:]]*"int"' || { echo "AOF restore TTL type failed"; exit 1; }

  ensure_stopped "${CLIENT_BIN}" "${addrs[@]}"
  for pid in "${pids[@]}"; do wait "${pid}" >/dev/null 2>&1 || true; done

  # 可选性能基准
  bench_path="${run_dir}/bench.txt"
  if [[ "${SKIP_BENCHMARK}" == "1" ]]; then
    echo "SkipBenchmark enabled." >"${bench_path}"
  else
    if command -v redis-benchmark >/dev/null 2>&1; then
      {
        echo "redis-benchmark found: $(command -v redis-benchmark)"
        redis-benchmark -p "${BASE_PORT}" -c 50 -n 100000 -t set,get
        redis-benchmark -p "${BASE_PORT}" -c 50 -n 100000 -t set,get -P 16
      } >"${bench_path}" 2>&1
    else
      echo "redis-benchmark not found, skip benchmark." >"${bench_path}"
    fi
  fi

  cat >"${run_dir}/summary.md" <<EOF
# MyRedis Evaluation Summary

## 对齐图片描述验收
- [x] 分布式 KV（3 节点分片 + 透明转发）
- [x] RESP Pipeline/拆包（由 go test ./... 覆盖）
- [x] AOF EverySec + 优雅关闭 + 重启恢复
- [x] 内存管理（${eviction} + TTL 惰性+定期删除）

## 本次运行参数
- eviction: ${eviction}
- nodes: ${nodes_arg}
- vnodes: ${VNODES}
- max-bytes: ${MAX_BYTES}

## 关键产物
- ../env.json
- ../go_test.txt
- functional.json / functional.md
- bench.txt
- logs/
EOF
done

echo "Eval finished. Artifacts: ${ARTIFACTS_ROOT}"

popd >/dev/null

