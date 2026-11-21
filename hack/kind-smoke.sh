#!/usr/bin/env bash
# Cluster smoke test: Envoy Gateway + the x402 chart on a kind cluster.
#
# Checks that a PaymentPolicy reaches Programmed, the route answers a
# decodable 402, a payment is verified and served, a replayed payment is
# refused, and deleting the policy frees the route (re-applying prices it).
#
# No chain and no facilitator are involved: there is no -rpc (balances are
# not checked) and the settlement provider points at an unreachable address,
# so nothing leaves the cluster and settlement simply stays pending. The
# payer key is generated here, used from outside the cluster, and thrown away.
#
# Requires: kind (a cluster already created), kubectl, helm, docker, go,
# openssl, curl. Usage: hack/kind-smoke.sh [kind-cluster-name]
set -euo pipefail

CLUSTER=${1:-kind}
EG_VERSION=v1.10.0
IMAGE=docker.io/apivend/gateway:ci
PAYTO=0x209693Bc6afc0C5328bA36FaF03C514EF312287C   # x402 spec example address
LOCAL_PORT=18080
cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
PF_PID=""
cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

step() { echo; echo "== $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
# until_code PATH CODE SECONDS: poll the gateway until PATH answers CODE.
until_code() {
  local deadline=$((SECONDS + $3)) code=""
  while [ $SECONDS -lt $deadline ]; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$LOCAL_PORT$1" || true)
    [ "$code" = "$2" ] && { echo "$1 -> $2 after $(( $3 - (deadline - SECONDS) ))s"; return 0; }
    sleep 2
  done
  fail "$1 answered $code, want $2 within $3s"
}

# The image builds while Envoy Gateway installs; neither needs the other.
step "Image (building in the background)"
docker build -q -t "$IMAGE" . >"$WORK/build.log" 2>&1 &
BUILD_PID=$!

step "Envoy Gateway $EG_VERSION (with its Gateway API CRDs: kind has none)"
helm install eg oci://docker.io/envoyproxy/gateway-helm --version "$EG_VERSION" \
  -n envoy-gateway-system --create-namespace --wait --timeout 5m >/dev/null

step "Image"
wait "$BUILD_PID" || { cat "$WORK/build.log" >&2; fail "docker build"; }
kind load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null

step "Redis and the chart"
kubectl apply -f hack/dev-cluster/redis.yaml >/dev/null
kubectl -n x402-system rollout status deploy/redis --timeout=180s
helm install x402 deploy/helm/x402 -n x402-system \
  --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never \
  --set redis.url=redis://redis.x402-system.svc:6379/0 --wait --timeout 5m >/dev/null
kubectl -n x402-system get pods

step "Routes and policies"
kubectl apply -f hack/dev-cluster/routes.yaml >/dev/null
sed -e "s/PAYTO_ADDRESS/$PAYTO/" -e "s#https://x402.org/facilitator#http://facilitator.invalid#" \
  hack/dev-cluster/policies.yaml > "$WORK/policies.yaml"
kubectl apply -f "$WORK/policies.yaml" >/dev/null
kubectl -n prod wait --for=condition=Programmed paymentpolicy/search paymentpolicy/search-sync --timeout=180s
# Not the Gateway's Programmed condition: kind has no load balancer, so the
# Gateway never gets an address. Wait for its Envoy deployment instead.
deadline=$((SECONDS + 180))
until kubectl -n envoy-gateway-system get deploy -l gateway.envoyproxy.io/owning-gateway-name=public -o name | grep -q .; do
  [ $SECONDS -lt $deadline ] || fail "Envoy Gateway created no proxy for gateway prod/public"
  sleep 2
done
kubectl -n envoy-gateway-system wait --for=condition=Available deploy -l gateway.envoyproxy.io/owning-gateway-name=public --timeout=180s

step "Every filter replica has the prices"
# Each replica watches the price table through the API, so
# this takes about a second; a replica that has not loaded both routes would
# serve a priced route free.
deadline=$((SECONDS + 60))
for pod in $(kubectl -n x402-system get pod -l app.kubernetes.io/component=filter -o name); do
  until kubectl -n x402-system logs "$pod" | grep '"routes":' | tail -1 | grep -q '"routes":2'; do
    [ $SECONDS -lt $deadline ] || fail "$pod never loaded both priced routes"
    sleep 2
  done
  echo "$pod: 2 routes"
done

step "Reach the gateway"
SVC=$(kubectl -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=public -o jsonpath='{.items[0].metadata.name}')
kubectl -n envoy-gateway-system port-forward "svc/$SVC" "$LOCAL_PORT:80" >"$WORK/pf.log" 2>&1 &
PF_PID=$!
until_code / 402 180
until_code /sync 402 180

step "Acceptance checks (throwaway key, from outside the cluster)"
go build -o "$WORK/x402-check" ./cmd/x402-check
KEY=0x$(openssl rand -hex 32)
X402_PRIVATE_KEY=$KEY "$WORK/x402-check" -url "http://127.0.0.1:$LOCAL_PORT/"
# Sync mode holds the response while settlement is attempted. With no
# facilitator reachable the settlement stays pending, and after the 10 s wait
# the response is served with the async header. That only
# works if Envoy's message timeout (12 s, set by the controller) outlasts the
# wait; otherwise the client gets a 5xx after paying.
start=$SECONDS
X402_PRIVATE_KEY=$KEY "$WORK/x402-check" -url "http://127.0.0.1:$LOCAL_PORT/sync" -replay=false
echo "sync payment served after $((SECONDS - start))s (settlement pending)"

step "Deleting the policy frees the route; re-applying prices it again"
kubectl -n prod delete paymentpolicy search >/dev/null
until_code / 200 180
kubectl apply -f "$WORK/policies.yaml" >/dev/null
until_code / 402 180

echo
echo "kind smoke test passed"
