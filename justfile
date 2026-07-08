# aircast-sdk task runner. Run `just` to list recipes.

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

export GOWORK := "off"

module_name := "github.com/pavliha/aircast-sdk"

version := `git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"`
build_version := `git describe --tags --always --dirty`

# List available recipes
default:
    @just --list

# Run the test suite (verbose)
[group('test')]
test:
    @echo "Testing SDK..."
    go test ./... -v

# Run tests with coverage and output to coverage.out
[group('test')]
test-coverage:
    @echo "Running tests with coverage..."
    go test ./... -coverprofile=coverage.out

# Generate or show a coverage report. FORMAT: html|stats
[group('test')]
coverage FORMAT: test-coverage
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{FORMAT}}" in
      html)
        go tool cover -html=coverage.out -o coverage.html
        echo "Coverage report generated: coverage.html"
        if command -v open > /dev/null; then
          open coverage.html
        elif command -v xdg-open > /dev/null; then
          xdg-open coverage.html
        else
          echo "Please open coverage.html in your browser"
        fi
        ;;
      stats)
        go tool cover -func=coverage.out
        ;;
      *)
        echo "Unknown format: {{FORMAT}} (expected html|stats)" >&2
        exit 1
        ;;
    esac

# Lint the SDK (installs golangci-lint if missing)
[group('check')]
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Linting..."
    if command -v golangci-lint > /dev/null; then
      golangci-lint run
    else
      echo "golangci-lint not found. Installing..."
      go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
      golangci-lint run
    fi

# Format code
[group('check')]
fmt:
    @echo "Formatting code..."
    go fmt ./...

# Vet code
[group('check')]
vet:
    @echo "Vetting code..."
    go vet ./...

# Run all checks (format, vet, lint, test)
[group('check')]
check: fmt vet lint test

# Tidy dependencies
[group('build')]
tidy:
    @echo "Tidying dependencies..."
    go mod tidy

# Download dependencies
[group('build')]
deps:
    @echo "Downloading dependencies..."
    go mod download

# Clean generated files
[group('build')]
clean:
    @echo "Cleaning..."
    @rm -f coverage.out coverage.html
    @go clean -cache
    @echo "Clean complete"

# Display current version
[group('release')]
version:
    @echo "Tag version: {{version}}"
    @echo "Build version: {{build_version}}"

# Create and push a release tag. TYPE: patch|minor|major|dev|alpha|rc
[group('release')]
release TYPE:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION="{{version}}"
    NEW_VERSION="${VERSION#v}"
    case "{{TYPE}}" in
      patch)
        echo "Current version: $VERSION"
        BASE_VERSION=$(echo "$NEW_VERSION" | cut -d'-' -f1)
        MAJOR=$(echo "$BASE_VERSION" | awk -F. '{print $1}')
        MINOR=$(echo "$BASE_VERSION" | awk -F. '{print $2}')
        PATCH=$(echo "$BASE_VERSION" | awk -F. '{print $3}')
        while true; do
          NEW_VERSION="v$MAJOR.$MINOR.$PATCH"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          PATCH=$((PATCH + 1))
        done
        echo "New version will be: $NEW_VERSION"
        read -p "Are you sure you want to create release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      minor)
        echo "Current version: $VERSION"
        BASE_VERSION=$(echo "$NEW_VERSION" | cut -d'-' -f1)
        MAJOR=$(echo "$BASE_VERSION" | awk -F. '{print $1}')
        MINOR=$(echo "$BASE_VERSION" | awk -F. '{print $2}')
        while true; do
          NEW_VERSION="v$MAJOR.$MINOR.0"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          MINOR=$((MINOR + 1))
        done
        echo "New version will be: $NEW_VERSION"
        read -p "Are you sure you want to create release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      major)
        echo "Current version: $VERSION"
        BASE_VERSION=$(echo "$NEW_VERSION" | cut -d'-' -f1)
        MAJOR=$(echo "$BASE_VERSION" | awk -F. '{print $1}')
        while true; do
          NEW_VERSION="v$MAJOR.0.0"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          MAJOR=$((MAJOR + 1))
        done
        echo "New version will be: $NEW_VERSION"
        read -p "Are you sure you want to create release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      dev)
        echo "Current version: $VERSION"
        if echo "$VERSION" | grep -q "dev"; then
          BASE_VERSION=$(echo "$VERSION" | sed 's/-dev\.[0-9]*$//')
          DEV_NUM=$(echo "$VERSION" | grep -o 'dev\.[0-9]*' | cut -d. -f2)
          NEXT_DEV=$((DEV_NUM + 1))
        else
          BASE_VERSION_RAW=$(echo "$VERSION" | sed 's/-alpha\.[0-9]*$//' | sed 's/-beta\.[0-9]*$//' | sed 's/-rc\.[0-9]*$//' | sed 's/^v//')
          MAJOR=$(echo "$BASE_VERSION_RAW" | awk -F. '{print $1}')
          MINOR=$(echo "$BASE_VERSION_RAW" | awk -F. '{print $2}')
          PATCH=$(echo "$BASE_VERSION_RAW" | awk -F. '{print $3}')
          PATCH=$((PATCH + 1))
          BASE_VERSION="v$MAJOR.$MINOR.$PATCH"
          NEXT_DEV=1
        fi
        while true; do
          NEW_VERSION="${BASE_VERSION}-dev.$NEXT_DEV"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          NEXT_DEV=$((NEXT_DEV + 1))
        done
        echo "New development version will be: $NEW_VERSION"
        read -p "Are you sure you want to create development release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Development Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      alpha)
        echo "Current version: $VERSION"
        if echo "$VERSION" | grep -q "alpha"; then
          BASE_VERSION=$(echo "$VERSION" | sed 's/-alpha\.[0-9]*$//')
          ALPHA_NUM=$(echo "$VERSION" | grep -o 'alpha\.[0-9]*' | cut -d. -f2)
          NEXT_ALPHA=$((ALPHA_NUM + 1))
        else
          BASE_VERSION="$VERSION"
          NEXT_ALPHA=1
        fi
        while true; do
          NEW_VERSION="${BASE_VERSION}-alpha.$NEXT_ALPHA"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          NEXT_ALPHA=$((NEXT_ALPHA + 1))
        done
        echo "New version will be: $NEW_VERSION"
        read -p "Are you sure you want to create release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      rc)
        echo "Current version: $VERSION"
        BASE_VERSION=$(echo "$NEW_VERSION" | cut -d'-' -f1)
        if echo "$VERSION" | grep -q "rc"; then
          RC_NUM=$(echo "$VERSION" | grep -o 'rc\.[0-9]*' | cut -d. -f2)
          NEXT_RC=$((RC_NUM + 1))
        else
          MAJOR=$(echo "$BASE_VERSION" | awk -F. '{print $1}')
          MINOR=$(echo "$BASE_VERSION" | awk -F. '{print $2}')
          PATCH=$(echo "$BASE_VERSION" | awk -F. '{print $3}')
          PATCH=$((PATCH + 1))
          BASE_VERSION="$MAJOR.$MINOR.$PATCH"
          NEXT_RC=1
        fi
        while true; do
          NEW_VERSION="v$BASE_VERSION-rc.$NEXT_RC"
          if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
            break
          fi
          NEXT_RC=$((NEXT_RC + 1))
        done
        echo "New version will be: $NEW_VERSION"
        read -p "Are you sure you want to create release $NEW_VERSION? [y/N] " confirm && [ "$confirm" = "y" ] && \
        git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION" && \
        git push --follow-tags
        ;;
      *)
        echo "Unknown release type: {{TYPE}} (expected patch|minor|major|dev|alpha|rc)" >&2
        exit 1
        ;;
    esac
