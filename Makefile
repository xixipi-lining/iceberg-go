# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Project variables
PROJECT_NAME := iceberg-go
CLI_NAME := iceberg
CLI_DIR := cmd/iceberg
WEBSITE_DIR := website
DOCKER_COMPOSE_FILE := internal/recipe/docker-compose.yml

# Go variables
GO := go
GOPATH := $(shell $(GO) env GOPATH)
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

# Build variables
BUILD_DIR := build
DIST_DIR := dist
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Ldflags for version info
LDFLAGS := -ldflags "-X github.com/apache/iceberg-go.Version=$(VERSION) -X github.com/apache/iceberg-go.Commit=$(COMMIT) -X github.com/apache/iceberg-go.BuildTime=$(BUILD_TIME)"

# Default target
.PHONY: all
all: build

# Help target
.PHONY: help
help: ## Display this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build targets
.PHONY: build
build: ## Build the CLI binary
	@echo "Building $(CLI_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(CLI_NAME) ./$(CLI_DIR)

.PHONY: build-all
build-all: ## Build for all platforms
	@echo "Building for all platforms..."
	@mkdir -p $(DIST_DIR)
	# Linux
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/$(CLI_NAME)-linux-amd64 ./$(CLI_DIR)
	GOOS=linux GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/$(CLI_NAME)-linux-arm64 ./$(CLI_DIR)
	# macOS
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/$(CLI_NAME)-darwin-amd64 ./$(CLI_DIR)
	GOOS=darwin GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/$(CLI_NAME)-darwin-arm64 ./$(CLI_DIR)
	# Windows
	GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/$(CLI_NAME)-windows-amd64.exe ./$(CLI_DIR)

.PHONY: install
install: ## Install the CLI binary to GOPATH/bin
	@echo "Installing $(CLI_NAME)..."
	$(GO) install $(LDFLAGS) ./$(CLI_DIR)

# Test targets
.PHONY: test
test: ## Run unit tests
	@echo "Running unit tests..."
	$(GO) test -v ./...

.PHONY: test-race
test-race: ## Run unit tests with race detection
	@echo "Running unit tests with race detection..."
	$(GO) test -race -v ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	$(GO) test -coverprofile=coverage.out -v ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: test-integration
test-integration: docker-up ## Run integration tests
	@echo "Running integration tests..."
	@sleep 10  # Wait for services to be ready
	$(GO) test -tags integration -v -run="^TestScanner" ./table
	$(GO) test -tags integration -v ./io
	$(GO) test -tags integration -v -run="^TestRestIntegration$$" ./catalog/rest

.PHONY: test-integration-cleanup
test-integration-cleanup: test-integration docker-down ## Run integration tests and cleanup

# Code quality targets
.PHONY: fmt
fmt: ## Format Go code
	@echo "Formatting Go code..."
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	@echo "Running golangci-lint..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --timeout 5m; \
	else \
		echo "golangci-lint not found. Install it with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with auto-fix
	@echo "Running golangci-lint with auto-fix..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --fix --timeout 5m; \
	else \
		echo "golangci-lint not found. Install it with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

.PHONY: check
check: fmt vet lint test ## Run all checks (format, vet, lint, test)

# License check
.PHONY: license-check
license-check: ## Check license headers
	@echo "Checking license headers..."
	./dev/check-license

# Docker targets
.PHONY: docker-up
docker-up: ## Start Docker services for integration tests
	@echo "Starting Docker services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "Provisioning tables..."
	docker compose -f $(DOCKER_COMPOSE_FILE) exec -T spark-iceberg ipython ./provision.py

.PHONY: docker-down
docker-down: ## Stop Docker services
	@echo "Stopping Docker services..."
	docker compose -f $(DOCKER_COMPOSE_FILE) down

.PHONY: docker-logs
docker-logs: ## Show Docker logs
	docker compose -f $(DOCKER_COMPOSE_FILE) logs

# Website targets
.PHONY: website-build
website-build: ## Build the website
	@echo "Building website..."
	@if command -v mdbook >/dev/null 2>&1; then \
		cd $(WEBSITE_DIR) && mdbook build; \
	else \
		echo "mdbook not found. Install it with: cargo install mdbook"; \
		exit 1; \
	fi

.PHONY: website-serve
website-serve: ## Serve the website locally
	@echo "Serving website..."
	@if command -v mdbook >/dev/null 2>&1; then \
		cd $(WEBSITE_DIR) && mdbook serve; \
	else \
		echo "mdbook not found. Install it with: cargo install mdbook"; \
		exit 1; \
	fi

# Dependency management
.PHONY: deps
deps: ## Download dependencies
	@echo "Downloading dependencies..."
	$(GO) mod download

.PHONY: deps-update
deps-update: ## Update dependencies
	@echo "Updating dependencies..."
	$(GO) mod tidy
	$(GO) mod verify

.PHONY: deps-vendor
deps-vendor: ## Vendor dependencies
	@echo "Vendoring dependencies..."
	$(GO) mod vendor

# Development targets
.PHONY: dev-setup
dev-setup: ## Set up development environment
	@echo "Setting up development environment..."
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Development environment setup complete!"

.PHONY: run
run: build ## Build and run the CLI
	@echo "Running $(CLI_NAME)..."
	./$(BUILD_DIR)/$(CLI_NAME) --help

# Clean targets
.PHONY: clean
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -rf $(DIST_DIR)
	rm -f coverage.out coverage.html

.PHONY: clean-all
clean-all: clean docker-down ## Clean everything including Docker containers
	@echo "Cleaning all artifacts..."
	$(GO) clean -cache -modcache -testcache

# Release targets
.PHONY: release-check
release-check: ## Run release checks
	@echo "Running release checks..."
	./dev/release/run_rat.sh

.PHONY: archive
archive: ## Create release archive
	@echo "Creating release archive..."
	@VERSION_NUM=$$(echo $(VERSION) | sed 's/^v//'); \
	RC=$${RC:-1}; \
	ARCHIVE_NAME="apache-iceberg-go-$${VERSION_NUM}-rc$${RC}"; \
	git archive HEAD --prefix "$${ARCHIVE_NAME}/" --output "$${ARCHIVE_NAME}.tar.gz"; \
	sha512sum "$${ARCHIVE_NAME}.tar.gz" > "$${ARCHIVE_NAME}.tar.gz.sha512"; \
	echo "Created: $${ARCHIVE_NAME}.tar.gz"

# Info targets
.PHONY: version
version: ## Show version information
	@echo "Project: $(PROJECT_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Commit: $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(shell $(GO) version)"

.PHONY: info
info: version ## Show project information
	@echo "GOOS: $(GOOS)"
	@echo "GOARCH: $(GOARCH)"
	@echo "GOPATH: $(GOPATH)"
