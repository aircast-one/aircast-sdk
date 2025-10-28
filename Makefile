# Aircast SDK Makefile

# declare targets that are not files
.PHONY: all test test.coverage test.coverage.html test.coverage.stats lint clean version version.patch version.minor version.major version.dev version.alpha version.rc

# Version management
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
NEW_VERSION = $(subst v,,$(VERSION))
BUILD_VERSION := $(shell git describe --tags --always --dirty)

# Go module name (update this to match your actual module name)
MODULE_NAME := github.com/pavliha/aircast-sdk

all: test

# Test the SDK
test:
	@echo "Testing SDK..."
	go test ./... -v

# Run tests with coverage and output to coverage.out
test.coverage:
	@echo "Running tests with coverage..."
	go test ./... -coverprofile=coverage.out

# Generate HTML coverage report and open it in a browser
test.coverage.html: test.coverage
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"
	@if command -v open > /dev/null; then \
		open coverage.html; \
	elif command -v xdg-open > /dev/null; then \
		xdg-open coverage.html; \
	else \
		echo "Please open coverage.html in your browser"; \
	fi

# Show coverage statistics in the terminal
test.coverage.stats: test.coverage
	go tool cover -func=coverage.out

# Lint the SDK
lint:
	@echo "Linting..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
		golangci-lint run; \
	fi

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	go vet ./...

# Run all checks (format, vet, lint, test)
check: fmt vet lint test

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	go mod tidy

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download

# Clean generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@go clean -cache
	@echo "Clean complete"

# Display current version
version:
	@echo "Tag version: $(VERSION)"
	@echo "Build version: $(BUILD_VERSION)"

# Create a patch version (e.g., v1.0.0 -> v1.0.1)
version.patch:
	@echo "Current version: $(VERSION)"
	@BASE_VERSION=$$(echo "$(NEW_VERSION)" | cut -d'-' -f1); \
	MAJOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$1}'); \
	MINOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$2}'); \
	PATCH=$$(echo "$$BASE_VERSION" | awk -F. '{print $$3}'); \
	while true; do \
		NEW_VERSION="v$$MAJOR.$$MINOR.$$PATCH"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		PATCH=$$((PATCH + 1)); \
	done; \
	echo "New version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Release $$NEW_VERSION" && \
	git push --follow-tags

# Create a minor version (e.g., v1.0.0 -> v1.1.0)
version.minor:
	@echo "Current version: $(VERSION)"
	@BASE_VERSION=$$(echo "$(NEW_VERSION)" | cut -d'-' -f1); \
	MAJOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$1}'); \
	MINOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$2}'); \
	while true; do \
		NEW_VERSION="v$$MAJOR.$$MINOR.0"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		MINOR=$$((MINOR + 1)); \
	done; \
	echo "New version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Release $$NEW_VERSION" && \
	git push --follow-tags

# Create a major version (e.g., v1.0.0 -> v2.0.0)
version.major:
	@echo "Current version: $(VERSION)"
	@BASE_VERSION=$$(echo "$(NEW_VERSION)" | cut -d'-' -f1); \
	MAJOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$1}'); \
	while true; do \
		NEW_VERSION="v$$MAJOR.0.0"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		MAJOR=$$((MAJOR + 1)); \
	done; \
	echo "New version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Release $$NEW_VERSION" && \
	git push --follow-tags

# Create a development version (e.g., v1.0.0-dev.1)
version.dev:
	@echo "Current version: $(VERSION)"
	@if echo "$(VERSION)" | grep -q "dev"; then \
		BASE_VERSION=$$(echo "$(VERSION)" | sed 's/-dev\.[0-9]*$$//'); \
		DEV_NUM=$$(echo "$(VERSION)" | grep -o 'dev\.[0-9]*' | cut -d. -f2); \
		NEXT_DEV=$$((DEV_NUM + 1)); \
	else \
		BASE_VERSION_RAW=$$(echo "$(VERSION)" | sed 's/-alpha\.[0-9]*$$//' | sed 's/-beta\.[0-9]*$$//' | sed 's/-rc\.[0-9]*$$//' | sed 's/^v//'); \
		MAJOR=$$(echo "$$BASE_VERSION_RAW" | awk -F. '{print $$1}'); \
		MINOR=$$(echo "$$BASE_VERSION_RAW" | awk -F. '{print $$2}'); \
		PATCH=$$(echo "$$BASE_VERSION_RAW" | awk -F. '{print $$3}'); \
		PATCH=$$((PATCH + 1)); \
		BASE_VERSION="v$$MAJOR.$$MINOR.$$PATCH"; \
		NEXT_DEV=1; \
	fi; \
	while true; do \
		NEW_VERSION="$${BASE_VERSION}-dev.$$NEXT_DEV"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		NEXT_DEV=$$((NEXT_DEV + 1)); \
	done; \
	echo "New development version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create development release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Development Release $$NEW_VERSION" && \
	git push --follow-tags

# Create an alpha version (e.g., v1.0.0-alpha.1)
version.alpha:
	@echo "Current version: $(VERSION)"
	@if echo "$(VERSION)" | grep -q "alpha"; then \
		BASE_VERSION=$$(echo "$(VERSION)" | sed 's/-alpha\.[0-9]*$$//'); \
		ALPHA_NUM=$$(echo "$(VERSION)" | grep -o 'alpha\.[0-9]*' | cut -d. -f2); \
		NEXT_ALPHA=$$((ALPHA_NUM + 1)); \
	else \
		BASE_VERSION="$(VERSION)"; \
		NEXT_ALPHA=1; \
	fi; \
	while true; do \
		NEW_VERSION="$${BASE_VERSION}-alpha.$$NEXT_ALPHA"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		NEXT_ALPHA=$$((NEXT_ALPHA + 1)); \
	done; \
	echo "New version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Release $$NEW_VERSION" && \
	git push --follow-tags

# Create a release candidate version (e.g., v1.0.0-rc.1)
version.rc:
	@echo "Current version: $(VERSION)"
	@BASE_VERSION=$$(echo "$(NEW_VERSION)" | cut -d'-' -f1); \
	if echo "$(VERSION)" | grep -q "rc"; then \
		RC_NUM=$$(echo "$(VERSION)" | grep -o 'rc\.[0-9]*' | cut -d. -f2); \
		NEXT_RC=$$((RC_NUM + 1)); \
	else \
		MAJOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$1}'); \
		MINOR=$$(echo "$$BASE_VERSION" | awk -F. '{print $$2}'); \
		PATCH=$$(echo "$$BASE_VERSION" | awk -F. '{print $$3}'); \
		PATCH=$$((PATCH + 1)); \
		BASE_VERSION="$$MAJOR.$$MINOR.$$PATCH"; \
		NEXT_RC=1; \
	fi; \
	while true; do \
		NEW_VERSION="v$$BASE_VERSION-rc.$$NEXT_RC"; \
		if ! git rev-parse "$$NEW_VERSION" >/dev/null 2>&1; then \
			break; \
		fi; \
		NEXT_RC=$$((NEXT_RC + 1)); \
	done; \
	echo "New version will be: $$NEW_VERSION"; \
	read -p "Are you sure you want to create release $$NEW_VERSION? [y/N] " confirm && [ "$$confirm" = "y" ] && \
	git tag -a "$$NEW_VERSION" -m "Release $$NEW_VERSION" && \
	git push --follow-tags

# Help target
help:
	@echo "Available targets:"
	@echo "  all            - Run tests (default)"
	@echo "  test           - Run tests"
	@echo "  test.coverage  - Run tests with coverage"
	@echo "  test.coverage.html - Generate HTML coverage report"
	@echo "  test.coverage.stats - Show coverage statistics"
	@echo "  lint           - Run linter"
	@echo "  fmt            - Format code"
	@echo "  vet            - Run go vet"
	@echo "  check          - Run all checks (fmt, vet, lint, test)"
	@echo "  tidy           - Tidy dependencies"
	@echo "  deps           - Download dependencies"
	@echo "  clean          - Clean generated files"
	@echo "  version        - Show current version"
	@echo "  version.patch  - Create patch version"
	@echo "  version.minor  - Create minor version"
	@echo "  version.major  - Create major version"
	@echo "  version.dev    - Create development version"
	@echo "  version.alpha  - Create alpha version"
	@echo "  version.rc     - Create release candidate version"
	@echo "  help           - Show this help message"
