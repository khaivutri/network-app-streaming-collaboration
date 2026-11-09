export PATH := $(PATH):/usr/local/go/bin:$(HOME)/go/bin

GO ?= go
GOFMT ?= gofmt
BIN_DIR := bin
ADDR ?= :9000
SERVER ?= 127.0.0.1:9000
CLIENT1_UI_ADDR ?= 127.0.0.1:8081
CLIENT2_UI_ADDR ?= 127.0.0.1:8082
CLIENT1_STORAGE_DIR ?= storage/client-8081
CLIENT2_STORAGE_DIR ?= storage/client-8082

.DEFAULT_GOAL := help
.PHONY: help fmt fmt-check vet test test-proto test-tcp test-rudp test-int race cover check build server-tcp client-tcp server-udp client-udp server-step3-auth client-step3-auth server-step4-concurrency client-step4-concurrency server-step5-chat client-step5-chat server-step6-file client-step6-file server-step7-resilience client-step7-resilience server-step8-security client-step8-security server-step9-http-ui client-step9-http-ui client-step9-http-ui-1 client-step9-http-ui-2 test-step4 test-step5 test-step6 test-step7 test-step8 test-step9 smoke-step4 smoke-step5 smoke-step6 smoke-step7 smoke-step8 smoke-step9 smoke-tcp smoke-udp clean clean-storage

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
server-step3-auth: ## Run Step 3 authentication server
	$(GO)  run ./cmd/server --mode=step3-auth --addr=$(ADDR) --users=configs/users.json
client-step3-auth: ## Run Step 3 authentication client
	$(GO)  run ./cmd/client --mode=step3-auth --server=$(SERVER) --user=hung.vo --pass=Password@123
server-step4-concurrency: ## Run Step 4 concurrent server
	$(GO) run ./cmd/server --mode=step4-concurrency --addr=$(ADDR) --users=configs/users.json
client-step4-concurrency: ## Run Step 4 concurrent clients
	$(GO) run ./cmd/client --mode=step4-concurrency --server=$(SERVER) --count=20 --user=Alice --pass=Alice@123
test-step4: ## Run Step 4 concurrency tests with race detector
	$(GO) test -v -race -run=TestStep4 ./tests/integration/...
smoke-step4: build ## Run Step 4 end-to-end smoke test
	@./$(BIN_DIR)/server --mode=step4-concurrency --addr=127.0.0.1:19004 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep4_ConcurrentHandshakeAndAuth_25Clients ./tests/integration/...
server-step5-chat: ## Run Step 5 streaming and messaging server
	$(GO) run ./cmd/server --mode=step5-chat --addr=$(ADDR) --users=configs/users.json
client-step5-chat: ## Run Step 5 interactive chat client
	$(GO) run ./cmd/client --mode=step5-chat --server=$(SERVER) --user=hung.vo --pass=Password@123
test-step5: ## Run Step 5 streaming and messaging tests with race detector
	$(GO) test -v -race -run=TestStep5 ./tests/integration/... ./pkg/protocol/... ./pkg/transport/rudp/... ./internal/client/...
smoke-step5: build ## Run Step 5 chat smoke test
	@./$(BIN_DIR)/server --mode=step5-chat --addr=127.0.0.1:19005 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep5_BroadcastChat_EndToEnd ./tests/integration/...
server-step6-file: ## Run Step 6 reliable file transfer server
	$(GO) run ./cmd/server --mode=step6-file --addr=$(ADDR) --users=configs/users.json
client-step6-file: ## Run Step 6 interactive file transfer client
	$(GO) run ./cmd/client --mode=step6-file --server=$(SERVER) --user=hung.vo --pass=Password@123
test-step6: ## Run Step 6 protocol, transfer, and integration tests
	$(GO) test -v -race -run=TestStep6 ./tests/integration/... ./pkg/protocol/... ./internal/client/transfer/... ./internal/server/handlers/...
smoke-step6: build ## Run Step 6 file transfer smoke test
	@./$(BIN_DIR)/server --mode=step6-file --addr=127.0.0.1:19006 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep6_FileTransfer_EndToEnd_Integrity_BR08 ./tests/integration/...
server-step7-resilience: ## Run Step 7 resilience server
	$(GO) run ./cmd/server --mode=step7-resilience --addr=$(ADDR) --users=configs/users.json
client-step7-resilience: ## Run Step 7 resilient client
	$(GO) run ./cmd/client --mode=step7-resilience --server=$(SERVER) --user=hung.vo --pass=Password@123
test-step7: ## Run Step 7 resilience tests
	$(GO) test -v -race -run=TestStep7 ./tests/integration/... ./pkg/protocol/... ./pkg/transport/rudp/... ./internal/client/...
smoke-step7: build ## Run Step 7 malformed-packet smoke test
	@./$(BIN_DIR)/server --mode=step7-resilience --addr=127.0.0.1:19007 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep7_Fuzzing_MalformedPacketsResilience ./tests/integration/...
server-step8-security: ## Run Step 8 encrypted server
	$(GO) run ./cmd/server --mode=step8-security --addr=$(ADDR) --users=configs/users.json
client-step8-security: ## Run Step 8 encrypted client
	$(GO) run ./cmd/client --mode=step8-security --server=$(SERVER) --user=hung.vo --pass=Password@123
server-step9-http-ui: ## Run Step 9 encrypted backend server
	$(GO) run ./cmd/server --mode=step9-http-ui --addr=$(ADDR) --users=configs/users.json
client-step9-http-ui: ## Run Step 9 local HTTP UI client
	$(GO) run ./cmd/client --mode=step9-http-ui --server=$(SERVER) --ui-addr=127.0.0.1:8081
client-step9-http-ui-1: ## Run Step 9 Client 1 UI (hung.vo) on port 8081
	$(GO) run ./cmd/client --mode=step9-http-ui --server=$(SERVER) --ui-addr=$(CLIENT1_UI_ADDR) --storage-dir=$(CLIENT1_STORAGE_DIR)
client-step9-http-ui-2: ## Run Step 9 Client 2 UI (Alice) on port 8082
	$(GO) run ./cmd/client --mode=step9-http-ui --server=$(SERVER) --ui-addr=$(CLIENT2_UI_ADDR) --storage-dir=$(CLIENT2_STORAGE_DIR)
test-step8: ## Run Step 8 cryptographic security tests
	$(GO) test -v -race -run=TestStep8 ./tests/integration/... ./pkg/crypto/... ./pkg/transport/rudp/... ./internal/client/...
test-step9: ## Run Step 9 bridge and UI tests
	$(GO) test -v -race -run=TestStep9 ./tests/integration/... ./internal/client/... ./web/...
smoke-step8: build ## Run Step 8 security smoke test
	@./$(BIN_DIR)/server --mode=step8-security --addr=127.0.0.1:19008 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep8 ./tests/integration/...
smoke-step9: build ## Run Step 9 HTTP UI smoke test
	@./$(BIN_DIR)/server --mode=step9-http-ui --addr=127.0.0.1:19009 --users=configs/users.json & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; $(GO) test -v -race -run=TestStep9 ./tests/integration/...
clean-storage: ## Remove partial and completed local transfer files
	rm -rf storage/staging/* storage/downloads/*
smoke-tcp: build ## Run TCP smoke test
	@./$(BIN_DIR)/server --mode=step2-tcp --addr=127.0.0.1:19000 & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; ./$(BIN_DIR)/client --mode=step2-tcp --server=127.0.0.1:19000
smoke-udp: build ## Run UDP smoke test
	@./$(BIN_DIR)/server --mode=rudp --addr=127.0.0.1:19001 & SRV_PID=$$!; trap "kill $$SRV_PID 2>/dev/null || true" EXIT; sleep 0.5; ./$(BIN_DIR)/client --mode=rudp --server=127.0.0.1:19001
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out
