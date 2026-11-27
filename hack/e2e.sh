#!/usr/bin/env bash
# End-to-end check against a real Envoy. Starts, on localhost:
#
#   backend, Redis, a fake chain node, the hosted payer-state service
#   (x402-state, or hack/fakestate with E2E_STATE=fake), x402-filter (hosted
#   source first, RPC fallback),
#   x402-settler, a mock facilitator, and Envoy in Docker (host network)
#
# then exercises free, unpaid, paid, replayed, failing, overspent, sync and
# settled requests, and takes the hosted service away mid-run.
#
# No real chain is involved: the fake node gives every address the same
# balance and the mock facilitator invents transaction hashes.
#
# Requires: go, docker, python3, curl, redis-server, redis-cli. With Node.js it
# also pays through the reference @x402/fetch client (hack/compat).
# Ports 6390, 8080, 8402, 8545, 9002, 9102, 9103, 9444, 9445, 10000 must be free.
set -euo pipefail
cd "$(dirname "$0")/.."

ENVOY_IMAGE=${ENVOY_IMAGE:-envoyproxy/envoy:v1.34-latest}
# Hardhat's well-known test account #1. Never fund it on a real network.
export X402_PRIVATE_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
GW=http://127.0.0.1:10000
REDIS_PORT=6390
E2E_PORTS=(6390 8080 8402 8545 9002 9102 9103 9444 9445 10000)
# A port already taken means something else answers in place of this run's
# servers (on a reused CI machine, a cancelled run's leftovers), and the
# checks would test that instead. Refuse to start.
for p in "${E2E_PORTS[@]}"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "e2e: port $p is already in use; stop what listens there (a previous run?) and retry" >&2
    exit 1
  fi
done
LOGS=$(mktemp -d)
pids=()

cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f x402-e2e-envoy >/dev/null 2>&1 || true
  if [[ ${failed:-1} -ne 0 ]]; then
    for f in filter settler state envoy; do
      echo "--- $f log"; tail -40 "$LOGS/$f.log" 2>/dev/null | sed 's/x402s_[A-Za-z0-9_-]*/x402s_<redacted>/' || true
    done
  fi
  rm -rf "$LOGS"
}
trap cleanup EXIT

go build -o bin/ ./cmd/...
go build -o bin/mockfacilitator ./hack/mockfacilitator
go build -o bin/fakenode ./hack/fakenode
go build -o bin/fakestate ./hack/fakestate

# Backend: echoes the request headers it received; /search/fail returns 500;
# /bulk is slow, so concurrent requests overlap.
python3 - >"$LOGS/backend.log" 2>&1 <<'PY' &
import json, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/bulk"):
            time.sleep(0.6)
        code = 500 if self.path.startswith("/search/fail") else 200
        body = json.dumps({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}}).encode()
        if self.path.startswith("/search/broken"):
            body = b'{"error":"upstream timed out"}'  # an error dressed as a 200
        if self.path.startswith("/search/empty"):
            body = b""  # a 200 with no body: Envoy ends the stream on the headers
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", 8080), H).serve_forever()
PY
pids+=($!)

redis-server --port "$REDIS_PORT" --bind 127.0.0.1 --save "" --appendonly no >"$LOGS/redis.log" 2>&1 &
pids+=($!)
rcli() { redis-cli -p "$REDIS_PORT" "$@"; }
for _ in $(seq 1 50); do rcli ping >/dev/null 2>&1 && break; sleep 0.1; done

# Every wallet holds 0.30 USDC on the fake chain.
bin/fakenode -listen 127.0.0.1:8545 -balance 300000 >"$LOGS/node.log" 2>&1 &
pids+=($!)
RPC=base-sepolia=http://127.0.0.1:8545

# hack/fakestate stands in for the hosted payer-state service.
bin/fakestate -rpc "$RPC" -poll-interval 500ms -grpc 127.0.0.1:9444 >"$LOGS/state.log" 2>&1 &
state_pid=$!
pids+=("$state_pid")
for _ in $(seq 1 50); do grep -q 'x402s_' "$LOGS/state.log" 2>/dev/null && break; sleep 0.1; done
X402_HOSTED_STATE_KEY=$(grep -o 'x402s_[A-Za-z0-9_-]*' "$LOGS/state.log" | head -1)
export X402_HOSTED_STATE_KEY

# Settlements to this address are refused, to exercise a failed sync payment.
BAD_PAYTO=0x000000000000000000000000000000000000dEaD
bin/mockfacilitator -listen 127.0.0.1:8402 -refuse-payto "$BAD_PAYTO" -node http://127.0.0.1:8545 >"$LOGS/facilitator.log" 2>&1 &
pids+=($!)
printf 'providers:\n  - name: default\n    backends:\n      - {type: facilitator, url: "http://127.0.0.1:8402"}\n' >"$LOGS/providers.yaml"

cp deploy/example/routes.yaml "$LOGS/routes.yaml"
cat >>"$LOGS/routes.yaml" <<ROUTES
  - name: doomed
    match: {pathPrefix: /doomed}
    price: {amount: "0.01", currency: USD}
    accepts:
      - {network: base-sepolia, asset: USDC, payTo: "$BAD_PAYTO"}
    settlement: {mode: sync}
  - name: bulk
    match: {pathPrefix: /bulk}
    price: {amount: "0.05", currency: USD}
    accepts:
      - {network: base-sepolia, asset: USDC, payTo: "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"}
  - name: cheap
    match: {pathPrefix: /cheap}
    price: {amount: "0.001", currency: USD}
    accepts:
      - {network: base-sepolia, asset: USDC, payTo: "0x209693Bc6afc0C5328bA36FaF03C514EF312287C", schemes: [exact, batch-settlement]}
# Batch settlement: the claim key below is Anvil's well-known test key #2.
batch:
  receiverAuthorizer: "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
  withdrawDelay: 86400
ROUTES
printf '0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a' >"$LOGS/claim-key"

# Hosted source first, direct RPC as the fallback.
bin/x402-filter -listen 127.0.0.1:9002 -metrics 127.0.0.1:9102 -config "$LOGS/routes.yaml" \
  -redis "127.0.0.1:$REDIS_PORT" -hosted-state http://127.0.0.1:9444 -rpc "$RPC" -answer-check enforce >"$LOGS/filter.log" 2>&1 &
filter_pid=$!
pids+=("$filter_pid")

# -rpc for the batch claim job; with a chain reader the settler blocks a
# payer after a refusal it can attribute, and the "doomed" route refuses on
# purpose, so blocking lasts 1 ms here (the same payer runs every check).
bin/x402-settler -max-attempts 1 -metrics 127.0.0.1:9103 -redis "127.0.0.1:$REDIS_PORT" \
  -providers "$LOGS/providers.yaml" -rpc "$RPC" -block-payer-for 1ms \
  -batch-authorizer-key "$LOGS/claim-key" -claim-interval 2s -claim-min-amount 0 >"$LOGS/settler.log" 2>&1 &
pids+=($!)

docker rm -f x402-e2e-envoy >/dev/null 2>&1 || true
docker run -d --name x402-e2e-envoy --network host \
  -v "$PWD/deploy/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro" "$ENVOY_IMAGE" >/dev/null
(docker logs -f x402-e2e-envoy >"$LOGS/envoy.log" 2>&1 &)

for _ in $(seq 1 50); do
  curl -fsS -o /dev/null "$GW/free" 2>/dev/null && break
  sleep 0.2
done

pass=0; fail=0
check() { # name, condition...
  local name=$1; shift
  if "$@"; then echo "ok   $name"; pass=$((pass+1)); else echo "FAIL $name"; fail=$((fail+1)); fi
}
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
logged() { grep -c "$2" "$LOGS/$1.log" 2>/dev/null || true; }
metric() { curl -s "http://127.0.0.1:$1/metrics" | grep -E "$2" | head -1 | awk '{print $NF}'; }
drained() { for _ in $(seq 1 80); do [[ "$(rcli zcard x402:settle:queue)" = 0 ]] && return 0; sleep 0.1; done; return 1; }

check "free route passes through"          test "$(status $GW/free)" = 200
check "priced route answers 402"           test "$(status $GW/search)" = 402
check "402 carries PAYMENT-REQUIRED"       bash -c "curl -s -D- -o /dev/null $GW/search | grep -qi '^payment-required: '"
check "preflight answered, not charged"    test "$(status -X OPTIONS -H 'Origin: https://app.example' -H 'Access-Control-Request-Method: GET' $GW/search)" = 204
check "plain OPTIONS is priced"           test "$(status -X OPTIONS $GW/search)" = 402
check "garbage payment answers 400"        test "$(status -H 'PAYMENT-SIGNATURE: %%%' $GW/search)" = 400

out=$(bin/x402-pay "$GW/search?q=hello" 2>"$LOGS/pay.err") || true
check "paid request is served"             grep -q '^< 200' "$LOGS/pay.err"
check "response carries PAYMENT-RESPONSE"  grep -q 'PAYMENT-RESPONSE: {"success":true' "$LOGS/pay.err"
check "backend sees the payer"             bash -c "echo '$out' | grep -qi 'x-x402-payer.*0x70997970C51812dc3A010C7d01b50e0d17dc79C8'"
check "backend never sees the signature"   bash -c "! echo '$out' | grep -qi payment-signature"
check "balance came from the hosted source" grep -q '"msg":"payment reserved".*"state_source":"hosted"' "$LOGS/filter.log"

bin/x402-pay "$GW/search/fail" >/dev/null 2>"$LOGS/fail.err" || true
check "backend 500 is passed through"      grep -q '^< 500' "$LOGS/fail.err"
check "backend 500 is not charged"         grep -q '"msg":"payment released".*backend_status_500' "$LOGS/filter.log"
bin/x402-pay "$GW/search/broken" >"$LOGS/broken.out" 2>"$LOGS/broken.err" || true
check "unusable 200 answer gets 503"        grep -q '^< 503' "$LOGS/broken.err"
check "unusable answer is not charged"      grep -q '"msg":"payment released".*answer_error_only' "$LOGS/filter.log"
check "unusable answer is not passed on"    bash -c "! grep -q 'upstream timed out' '$LOGS/broken.out'"
bin/x402-pay "$GW/search/empty" >/dev/null 2>"$LOGS/empty.err" || true
check "empty 200 answer gets 503"           grep -q '^< 503' "$LOGS/empty.err"
check "empty answer is not charged"         grep -q '"msg":"payment released".*answer_empty' "$LOGS/filter.log"
bin/x402-pay -X HEAD "$GW/search" >/dev/null 2>"$LOGS/head.err" || true
check "paid HEAD is refused (405)"           grep -q '^< 405' "$LOGS/head.err"
check "exactly one payment committed"      test "$(logged filter '"msg":"payment committed"')" = 1

for _ in $(seq 1 50); do [[ "$(logged settler '"msg":"payment settled"')" = 1 ]] && break; sleep 0.1; done
check "the served payment is settled"      test "$(logged settler '"msg":"payment settled"')" = 1
check "settlement frees the payer's hold"  bash -c "redis-cli -p $REDIS_PORT --scan --pattern 'x402:*:held' | xargs -r redis-cli -p $REDIS_PORT mget | grep -qx 0"
check "settlement queue is empty"          test "$(rcli zcard x402:settle:queue)" = 0

check "filter exports request metrics"     test "$(metric 9102 '^x402_requests_total\{.*decision="served"')" = 1
check "settler exports settled revenue"    test "$(metric 9103 '^x402_settled_atomic_total\{.*route="search"')" = 10000

bin/x402-pay "$GW/reports/q3" >"$LOGS/sync.out" 2>"$LOGS/sync.err" || true
check "sync route is served after settling" grep -q '^< 200' "$LOGS/sync.err"
check "sync response names the transaction" grep -Eq 'PAYMENT-RESPONSE: \{"success":true.*"transaction":"0x[0-9a-f]{64}"' "$LOGS/sync.err"
# A repeat of the same paid GET (a buyer whose connection dropped signs a new
# payment): the kept answer of the settled payment, with its receipt, and the
# new payment is never reserved or charged.
commits=$(logged filter '"msg":"payment committed"')
bin/x402-pay "$GW/reports/q3" >"$LOGS/again.out" 2>"$LOGS/again.err" || true
check "repeat gets the kept answer"         bash -c "grep -q '^< 200' '$LOGS/again.err' && test -s '$LOGS/sync.out' && cmp -s '$LOGS/sync.out' '$LOGS/again.out'"
check "repeat carries the original receipt" bash -c "diff <(grep PAYMENT-RESPONSE '$LOGS/sync.err') <(grep PAYMENT-RESPONSE '$LOGS/again.err')"
after=$(logged filter '"msg":"payment committed"')
check "repeat is not charged"              bash -c "grep -q 'paid answer redelivered; new payment not charged' '$LOGS/filter.log' && test '$after' = '$commits'"

bin/x402-pay "$GW/doomed" >"$LOGS/doomed.out" 2>"$LOGS/doomed.err" || true
check "failed sync payment gets 402"        grep -q '^< 402' "$LOGS/doomed.err"
check "failed sync payment says why"        grep -q 'PAYMENT-RESPONSE: {"success":false,"errorReason":"insufficient_funds"' "$LOGS/doomed.err"
check "failed sync payment gets no content" bash -c "! grep -q '\"path\"' '$LOGS/doomed.out'"

check "route-name match prices /maps"       bash -c "curl -s $GW/maps/tiles | grep -q '\"amount\":\"2000\"'"

# The demo's acceptance checks (x402-check): challenge, payment, replay, and
# ten concurrent 0.05 payments against a 0.30 balance.
drained
check "x402-check: challenge, payment, replay"       bash -c "bin/x402-check -url $GW/search >'$LOGS/check.out' 2>&1"
drained
check "x402-check: overspend serves exactly 6 of 10" bash -c "bin/x402-check -url $GW/bulk -replay=false -overspend 10 -expect-served 6 >'$LOGS/over.out' 2>&1"

# Reference client compatibility, when Node.js is available: @x402/fetch pays
# an async and a sync route. Results feed docs/COMPATIBILITY.md.
if command -v node >/dev/null 2>&1 && (cd hack/compat && { [[ -d node_modules ]] || npm install --no-audit --no-fund >/dev/null 2>&1; }); then
  drained
  (cd hack/compat && node x402-fetch.mjs "$GW/search?client=x402-fetch" "$GW/reports/x402-fetch") >"$LOGS/compat.out" 2>&1 && compat=0 || compat=$?
  check "@x402/fetch: pays async and sync routes" test "$compat" = 0
  cp "$LOGS/compat.out" "${COMPAT_OUT:-/dev/null}" 2>/dev/null || true
  [[ $compat -eq 0 ]] || cat "$LOGS/compat.out"
else
  echo "skip @x402/fetch compatibility (node or its packages unavailable)"
fi

# The x402 Foundation's Go client (hack/compat/go, its own module so the
# gateway never depends on it). GOTOOLCHAIN=auto: it may want a newer Go.
if command -v go >/dev/null 2>&1 && (cd hack/compat/go && GOTOOLCHAIN=auto go build -o "$LOGS/compat-go" . >"$LOGS/compat-go-build.out" 2>&1); then
  drained
  "$LOGS/compat-go" "$GW/search?client=go-sdk" "$GW/reports/go-sdk" >"$LOGS/compat-go.out" 2>&1 && cgo=0 || cgo=$?
  check "x402 Go client: pays async and sync routes" test "$cgo" = 0
  [[ $cgo -eq 0 ]] || cat "$LOGS/compat-go.out"
else
  echo "skip x402 Go client compatibility (could not build hack/compat/go)"; cat "$LOGS/compat-go-build.out" 2>/dev/null | tail -5
fi

# Batch settlement with the same client: a deposit opens a channel, later
# requests pay by voucher, the settler claims and pays out, then a refund
# returns what is left.
if [[ -x "$LOGS/compat-go" ]]; then
  drained
  X402_RPC=http://127.0.0.1:8545 "$LOGS/compat-go" -batch -salt 7 "$GW/cheap?n=1" "$GW/cheap?n=2" "$GW/cheap?n=3" >"$LOGS/batch.out" 2>&1 && bat=0 || bat=$?
  check "batch: Go client pays three requests"        test "$bat" = 0
  check "batch: the first request deposits"           bash -c "sed -n 1p '$LOGS/batch.out' | grep -q '\"amount\":\"100000\"'"
  check "batch: later requests pay by voucher only"   bash -c "sed -n 3p '$LOGS/batch.out' | grep -q '\"transaction\":\"\".*\"chargedCumulativeAmount\":\"3000\"'"
  for _ in $(seq 1 100); do grep -q '"msg":"claimed funds paid out' "$LOGS/settler.log" && break; sleep 0.1; done
  check "batch: charges claimed and paid out"         grep -q '"msg":"claimed funds paid out' "$LOGS/settler.log"
  X402_RPC=http://127.0.0.1:8545 "$LOGS/compat-go" -batch -salt 7 -refund "$GW/cheap" >"$LOGS/refund.out" 2>&1 && ref=0 || ref=$?
  check "batch: cooperative refund of what is left"   bash -c "test $ref = 0 && grep -q '\"amount\":\"97000\"' '$LOGS/refund.out'"
  [[ $bat -eq 0 && $ref -eq 0 ]] || { cat "$LOGS/batch.out" "$LOGS/refund.out"; grep -i -E "batch|voucher|deposit|refund|claim" "$LOGS/filter.log" "$LOGS/settler.log" | tail -20; }
fi

# The x402 Foundation's Python client (hack/compat/python, pinned in
# requirements.txt), in a throwaway virtualenv.
if command -v python3 >/dev/null 2>&1 && python3 -m venv "$LOGS/pyvenv" >/dev/null 2>&1 &&
   "$LOGS/pyvenv/bin/pip" install -q -r hack/compat/python/requirements.txt >"$LOGS/compat-py-install.out" 2>&1; then
  drained
  "$LOGS/pyvenv/bin/python" hack/compat/python/pay.py "$GW/search?client=python-sdk" "$GW/reports/python-sdk" >"$LOGS/compat-py.out" 2>&1 && cpy=0 || cpy=$?
  check "x402 Python client: pays async and sync routes" test "$cpy" = 0
  [[ $cpy -eq 0 ]] || cat "$LOGS/compat-py.out"
else
  echo "skip x402 Python client compatibility (python3 venv or the x402 package unavailable)"; tail -5 "$LOGS/compat-py-install.out" 2>/dev/null || true
fi

# The hosted service goes away. The gateway must carry on from its RPC source.
drained
kill "$state_pid"; wait "$state_pid" 2>/dev/null || true
sleep 6   # past the balance freshness bound, so cached hosted data cannot be used
bin/x402-pay "$GW/search?after=outage" >/dev/null 2>"$LOGS/outage.err" || true
check "hosted service down: request still served"      grep -q '^< 200' "$LOGS/outage.err"
check "hosted service down: decided on the RPC source" grep -q '"msg":"payment reserved".*"state_source":"rpc"' "$LOGS/filter.log"

# Fail closed: with the filter gone, priced and free routes are both refused.
kill "$filter_pid"; wait "$filter_pid" 2>/dev/null || true
check "filter down: request is not served" test "$(status $GW/search)" = 500

if [[ $fail -ne 0 ]]; then
  for f in check over; do echo "--- x402-check ($f)"; cat "$LOGS/$f.out" 2>/dev/null || true; done
fi
echo "$pass passed, $fail failed"
failed=$fail
exit "$fail"
