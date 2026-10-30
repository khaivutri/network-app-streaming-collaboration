export PATH := $(PATH):/usr/local/go/bin:$(HOME)/go/bin

GO ?= go
GOFMT ?= gofmt
BIN_DIR := bin
ADDR ?= :9000
SERVER ?= 127.0.0.1:9000

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check vet test test-proto test-tcp test-rudp test-int race cover check build server-tcp client-tcp server-udp client-udp smoke-tcp smoke-udp clean

help: ## Show available commands
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'
fmt: ## Format Go source
	$(GOFMT) -s -w .
fmt-check: ## Verify Go formatting
	@test -z "$$($(GOFMT) -s -l .)" || (echo "Run make fmt" && exit 1)
vet: ## Run Linux and Windows static checks
	$(GO) vet ./...
	GOOS=windows GOARCH=amd64 $(GO) vet ./...
test: ## Run all tests
	$(GO) test -v ./...
test-proto: ## Test protocol package
	$(GO) test -v -race ./pkg/protocol/...
test-tcp: ## Test transport and TCP packages
	$(GO) test -v -race ./pkg/transport ./pkg/transport/tcp_handshake/...
test-rudp: ## Test RUDP package
	$(GO) test -v -race ./pkg/transport/rudp/...
test-int: ## Test integrations
	$(GO) test -v -race ./tests/integration/...
race: ## Run all tests with race detector
	$(GO) test -v -race -count=2 ./...
cover: ## Print coverage summary
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out
check: fmt fmt-check vet race ## Run the quality gate
build: ## Build server and client
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/server ./cmd/server
	$(GO) build -o $(BIN_DIR)/client ./cmd/client
server-tcp: ## Run TCP server
	$(GO) run ./cmd/server --mode=step2-tcp --addr=$(ADDR)
client-tcp: ## Run TCP client
	$(GO) run ./cmd/client --mode=step2-tcp --server=$(SERVER)
server-udp: ## Run UDP server
	$(GO) run ./cmd/server --mode=rudp --addr=$(ADDR)
client-udp: ## Run UDP client
	$(GO) run ./cmd/client --mode=rudp --server=$(SERVER)
smoke-tcp: build ## Run TCP smoke test
	@./$(BIN_DIR)/server --mode=step2-tcp --addr=127.0.0.1:19000 & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; ./$(BIN_DIR)/client --mode=step2-tcp --server=127.0.0.1:19000
smoke-udp: build ## Run UDP smoke test
	@./$(BIN_DIR)/server --mode=rudp --addr=127.0.0.1:19001 & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; ./$(BIN_DIR)/client --mode=rudp --server=127.0.0.1:19001
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out
