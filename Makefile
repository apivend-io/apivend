.PHONY: build test e2e vet manifests image proto hooks

build:
	go build -o bin/ ./cmd/...

vet:
	go vet ./...

# Tests that need Redis or PostgreSQL start their own and skip when the
# binaries are not installed.
test: vet
	go test -race ./...

# Real Envoy (Docker) in front of the filter, with Redis, the settler and a
# mock facilitator.
e2e:
	hack/e2e.sh

# Regenerate deepcopy code and CRDs after changing api/.
manifests:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.23.0 object crd paths=./api/... output:crd:dir=deploy/crds
	cp deploy/crds/*.yaml deploy/helm/x402/crds/

image:
	docker build -t x402:dev .

# Regenerate the hosted service's gRPC code after editing api/proto. Needs
# buf, protoc-gen-go and protoc-gen-go-grpc on PATH (all `go install`-able).
proto:
	cd api/proto && buf generate

# Local pre-push checks (gofmt, go vet); see hack/git-hooks/pre-push.
hooks:
	git config core.hooksPath hack/git-hooks
