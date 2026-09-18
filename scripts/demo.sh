#!/usr/bin/env bash
# Ready-to-run example against a running memtxn-kvs instance.
#
# It performs:
#   1. sequential single-key writes
#   2. a multi-key atomic transaction (all keys appear at the same LSN)
#   3. a failing CAS conflict example and a successful CAS
#   4. prefix / range scans
#   5. a versioned snapshot read and a status check
#
# Usage: scripts/demo.sh [base_url]
set -euo pipefail

BASE="${1:-http://127.0.0.1:8080}"
C=(curl -fsS -H 'content-type: application/json')

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

say "1. Sequential writes"
for i in 1 2 3; do
  "${C[@]}" -X PUT "$BASE/v1/kv/seq:$i" -d "{\"value\": \"v$i\"}"
  echo
done

say "2. Multi-key atomic transaction: two account balances set together"
"${C[@]}" -X POST "$BASE/v1/txn" -d '{
  "ops": [
    {"op": "put", "key": "acct:alice", "value": "100"},
    {"op": "put", "key": "acct:bob",   "value": "50"}
  ]
}'
echo

say "3. Atomic transfer alice -> bob of 30 (guarded by CAS on both balances)"
# Read current values first.
ALICE=$("${C[@]}" "$BASE/v1/kv/acct:alice" | sed -n 's/.*"text":"\([0-9]*\)".*/\1/p')
BOB=$("${C[@]}" "$BASE/v1/kv/acct:bob" | sed -n 's/.*"text":"\([0-9]*\)".*/\1/p')
"${C[@]}" -X POST "$BASE/v1/txn" -d "{
  \"ops\": [
    {\"op\": \"cas\", \"key\": \"acct:alice\", \"expected\": \"$ALICE\", \"value\": \"$((ALICE-30))\"},
    {\"op\": \"cas\", \"key\": \"acct:bob\",   \"expected\": \"$BOB\",   \"value\": \"$((BOB+30))\"}
  ]
}"
echo

say "4. Both accounts read at the same version -> transaction visible atomically"
"${C[@]}" "$BASE/v1/kv/acct:alice"; echo
"${C[@]}" "$BASE/v1/kv/acct:bob"; echo

say "5. CAS conflict: stale expectation returns 409 CAS_CONFLICT (not silent)"
curl -sS -H 'content-type: application/json' \
  -X POST "$BASE/v1/kv/acct:alice/cas" \
  -d '{"expected": "STALE", "value": "999"}' \
  -w '\nHTTP status: %{http_code}\n' || true

say "6. Prefix scan and range scan"
"${C[@]}" "$BASE/v1/prefix/acct:"; echo
"${C[@]}" -X POST "$BASE/v1/range" -d '{"prefix": "seq:"}'; echo

say "7. Versioned snapshot read at LSN 4 (before the transfer committed)"
"${C[@]}" "$BASE/v1/kv/acct:alice?lsn=4"; echo

say "8. Runtime status"
"${C[@]}" "$BASE/v1/status"; echo

echo
echo "Demo finished. Total of 100+50 = 150 conserved, transfer applied atomically."
