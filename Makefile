MODULE := github.com/harshithgowdakt/speech
GOBIN  := $(shell go env GOPATH)/bin

.PHONY: generate build test test-integration lint run tidy

generate: ## Regenerate protobuf + gRPC code from proto/asr.proto
	PATH="$(GOBIN):$$PATH" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/asr.proto

tidy:
	go mod tidy

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test ./test/integration/... -count=1

lint:
	go vet ./...

run:
	go run ./cmd/gateway
