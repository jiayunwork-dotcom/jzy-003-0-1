#!/usr/bin/env bash
# End-to-end demo against a locally built kvstore release binary.
# Usage: ./scripts/demo.sh [port]
set -euo pipefail

PORT="${1:-18080}"
BASE="http://127.0.0.1:${PORT}"
DATA_DIR="$(mktemp -d -t kvstore-demo-XXXXXX)"
BIN="${CARGO_TARGET_DIR:-target}/release/kvstore"

cd "$(dirname "$0")/.."

if [[ ! -x "$BIN" ]]; then
  echo ">> building release binary ..."
  cargo build --release
fi

echo ">> data dir: $DATA_DIR"
DATA_DIR="$DATA_DIR" LISTEN_ADDR="127.0.0.1:${PORT}" SEED_ON_FRESH=true RUST_LOG=info \
  "$BIN" &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT

echo ">> waiting for server ..."
for _ in $(seq 1 50); do
  curl -fsS "$BASE/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done

echo
echo "== 1. 预置算例：顺序写入后的状态（alice=100/bob=50 为初始顺序写入）"
curl -s "$BASE/v1/status"
echo; echo
echo "== 2. 多键原子事务的结果：alice=70, bob=80, 转账记录已落盘"
for k in user:1:name user:1:balance user:2:name user:2:balance ledger:transfer:1; do
  printf '  %-20s -> ' "$k"
  curl -s "$BASE/v1/kv/$k"
  echo
done

echo
echo "== 3. 前缀查询 user:"
curl -s "$BASE/v1/kv?prefix=user:"
echo; echo

echo "== 4. 区间查询 [user:1:balance, user:2:name)"
curl -s "$BASE/v1/kv?start=user:1:balance&end=user:2:name"
echo; echo

echo "== 5. CAS 失败返回明确的 409 冲突（不静默成功）"
curl -s -w "\n  HTTP %{http_code}\n" -X PUT "$BASE/v1/kv/user:1:balance/cas" \
  -H 'content-type: application/json' \
  -d '{"expected":"WRONG","value":"0"}'

echo
echo "== 6. CAS 重试成功（读出当前值再条件写入）"
curl -s -X PUT "$BASE/v1/kv/user:1:balance/cas" \
  -H 'content-type: application/json' \
  -d '{"expected":"70","value":"60"}'
echo; echo

echo "== 7. 多键事务回滚演示：第二个 CAS 失败，第一个 put 对外不可见"
curl -s -w "\n  HTTP %{http_code}\n" -X POST "$BASE/v1/txn" \
  -H 'content-type: application/json' \
  -d '{"ops":[
       {"op":"put","key":"rollback-demo","value":"should-never-appear"},
       {"op":"cas","key":"user:1:balance","expected":"STALE","value":"1"}
     ]}'
printf '  rollback-demo -> '
curl -s "$BASE/v1/kv/rollback-demo"
echo

echo
echo "== 8. 矛盾事务（同键两个操作）在提交前被拒"
curl -s -w "\n  HTTP %{http_code}\n" -X POST "$BASE/v1/txn" \
  -H 'content-type: application/json' \
  -d '{"ops":[{"op":"put","key":"x","value":"1"},{"op":"delete","key":"x"}]}'

echo
echo "== 9. 触发同步快照压缩"
curl -s -X POST "$BASE/v1/compact?wait=true"
echo

echo
echo "== 10. 运行状态与 Prometheus 指标（节选）"
curl -s "$BASE/v1/status"
echo; echo
curl -s "$BASE/metrics" | grep -E 'kvstore_(lsn|keys_current|commits_total|compactions_total)'

echo
echo ">> demo finished; stopping server (data left in $DATA_DIR for inspection)"
trap - EXIT
kill "$PID" 2>/dev/null || true
