BINARY_NAME=vm-migrator
MODULE=github.com/yilmazo/victoriametrics-data-migrator
VERSION?=dev
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

.PHONY: all build test lint clean run help e2e e2e-cleanup proto

all: lint test build ## Run lint, test, and build

build: ## Build the binary
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/vm-migrator/

test: ## Run tests
	go test -v -race -count=1 ./...

test-short: ## Run tests (short mode)
	go test -short -count=1 ./...

lint: ## Run linters
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, running go vet instead"; \
		go vet ./...; \
	fi

clean: ## Clean build artifacts
	rm -rf bin/
	rm -f migration-state-*.json
	rm -f migration_report.json

run: build ## Build and run with default config
	./bin/$(BINARY_NAME) migrate --config deploy/examples/config.yaml

dry-run: build ## Build and run in dry-run mode
	./bin/$(BINARY_NAME) migrate --config deploy/examples/config.yaml --dry-run

docker-build: ## Build Docker image
	docker build -t $(BINARY_NAME):$(VERSION) -f e2e/Dockerfile .

proto: ## Generate Go code from proto files
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/worker.proto

e2e: ## Run end-to-end tests (requires minikube)
	./e2e/run_e2e.sh

e2e-rerun: ## Re-run e2e without setup (reuse existing minikube)
	./e2e/run_e2e.sh --no-setup

e2e-cleanup: ## Tear down e2e environment
	./e2e/teardown.sh --all

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
