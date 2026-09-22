# koios-pool-meta-fetcher — Makefile

SHELL := /bin/bash
BINARY := pool-meta-fetcher
PKG    := github.com/cardano-community/koios-artifacts/pool-meta-fetcher
GO     ?= go
TAG    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(TAG)

.PHONY: all build install uninstall test test-race lint fmt vet clean docker run help

all: build

build: ## Compile the binary to ./$(BINARY)
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/$(BINARY)

install: build ## Install to /usr/local/bin
	install -m 0755 $(BINARY) /usr/local/bin/$(BINARY)

uninstall: ## Remove the installed binary
	rm -f /usr/local/bin/$(BINARY)

test: ## Run unit tests
	$(GO) test ./...

test-race: ## Run unit tests with -race
	$(GO) test -race ./...

lint: ## Run golangci-lint (install separately)
	golangci-lint run ./...

fmt: ## Format all sources
	$(GO) fmt ./...

vet: ## Run go vet
	$(GO) vet ./...

clean: ## Remove build artifacts
	rm -f $(BINARY)
	$(GO) clean -cache -testcache 2>/dev/null || true

docker: ## Build the distroless image
	docker build --build-arg VERSION=$(TAG) -t koios-pool-meta-fetcher:$(TAG) -t koios-pool-meta-fetcher:latest .

run: build ## Run the daemon using ./pool-meta-fetcher.yaml (set CONF=path to override)
	CONF?=./pool-meta-fetcher.yaml
	./$(BINARY) --config $(CONF)

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-15s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
