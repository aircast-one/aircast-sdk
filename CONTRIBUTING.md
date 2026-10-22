# Contributing to aircast-sdk

Go SDK for the Aircast WebSocket messaging protocol (`pkg/message`), used as a
library by other Aircast services and agents.

## Branch

The active line is `consolidate-to-master`, not `master` — the repo is mid
consolidation and `master` is stale. Branch from and push to
`consolidate-to-master` until that migration lands.

## Setup

```bash
go mod download
lefthook install   # brew install lefthook, or: go install github.com/evilmartians/lefthook@latest
```

Pinned Go version is `1.25.0` (see `go.mod`'s `go` directive) — build with
that toolchain, not whatever is on `PATH`.

## Workflow

```bash
make check   # fmt, vet, lint, test — run this before committing
make test             # go test ./...
make test.coverage     # with coverage
go test -bench=. ./pkg/message/   # benchmarks for perf-sensitive changes
```

`lefthook` runs `go fmt`, `go mod tidy`, `golangci-lint`, and `go test -race`
on every commit, plus the full race+coverage suite on push. Don't bypass it
with `--no-verify` — if a hook fails, fix the underlying issue.

Keep test coverage near the existing bar (check `coverage.out` /
`make test.coverage.html`) and add benchmarks alongside any change to hot
paths (buffer pooling, JSON parsing, queue flush).

## Versioning

Releases are tagged via `make version.patch` / `version.minor` / `version.major`
(see `Makefile`). Don't tag manually.

## PRs

Small, focused commits. Update `MESSAGE_ROUTING.md` when you change
destination-routing behavior, and the README's usage examples when you change
the public `message.Client` / `message.QueuedClient` API.
