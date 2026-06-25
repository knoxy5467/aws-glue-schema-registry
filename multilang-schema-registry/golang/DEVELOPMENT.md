# Development setup

This file covers the local-machine setup for working on the Go port of the
AWS Glue Schema Registry client. For contribution conventions, see
[`CONTRIBUTING.md`](./CONTRIBUTING.md).

## Prerequisites

- **Go 1.21+** (the host toolchain on Amazon cloud desktops at the time of
  writing is Go 1.26). Install from <https://go.dev/dl/> if `toolbox install go`
  is not available — the static tarball at the top of that page works on
  Amazon Linux. Add the install location to `$PATH`:
  ```sh
  export PATH=/usr/local/go/bin:$PATH
  ```
- **`gh`** for GitHub auth — `gh auth login` once, then `git` works over HTTPS.
- **Docker / `docker compose`** for the integration suite's Kafka container.
- **Make** for the convenience targets in [`Makefile`](./Makefile).

## Module layout

```
multilang-schema-registry/golang/
├── go.mod                                # outer module (legacy CGO bridge)
├── pkg/gsrserde-go/core/   go.mod        # inner module (pure-Go core, in progress)
└── integration-tests/      go.mod        # IT module
```

Each `go.mod` is independent. Run `go test ./...` from inside the module you
want to exercise, not from the workspace root.

## Corp-network Go setup (required on Amazon hosts)

`proxy.golang.org` resolves to a sinkhole on the corp network, so the default
module proxy fails with TLS errors before it can negotiate. The supported
workaround is to bypass the proxy and disable checksum-DB lookups:

```sh
go env -w GOPROXY=direct GOSUMDB=off
```

You only need to run this once per `$HOME`. Confirm with `go env GOPROXY GOSUMDB`.

## Running tests

### Unit tests (no AWS, no Docker)

```sh
# from multilang-schema-registry/golang/pkg/gsrserde-go/core
go test ./...
```

The format-layer test packages under
`pkg/gsrserde-go/{serializer,deserializer}/{avro,json,protobuf}` currently fail
to link because they import a CGO bridge whose native shared library is not
checked in. That bridge is being deleted — see Phase 2 of the GSR Go plan.

### Integration tests (real Kafka + real AWS Glue)

Integration tests are double-gated. They need both the `integration` build
tag and the `AWS_INTEGRATION=1` env var to run:

```sh
# from multilang-schema-registry/golang/integration-tests
docker compose -f docker-compose.yml up -d        # local Kafka
AWS_INTEGRATION=1 go test -tags integration ./tests/...
```

A default `go test ./tests/...` (no tag) skips them entirely:

```
$ go test ./tests/...
go: warning: "./tests/..." matched no packages
no packages to test
```

AWS credentials must be on the SDK chain (e.g., via `ada` or an instance role).
LocalStack does not emulate Glue Schema Registry — these tests hit a real beta
account.

## Make targets

The [Makefile](./Makefile) wraps the most common operations:

| Target                     | What it does                                             |
| -------------------------- | -------------------------------------------------------- |
| `make build`               | `go build` + `go mod tidy`, packs artifacts into `build/`|
| `make test`                | unit tests with coverage                                 |
| `make test-integ`          | integration tests against local Kafka + real Glue        |
| `make test-integ-dockerized` | runs the IT suite inside a docker container            |
| `make generate-protos`     | regenerates `integration-tests/testpb/` from `.proto`    |
