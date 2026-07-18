# Quickstart & Validation: WebSocket ASR Streaming Gateway

A run/validation guide proving the feature works end to end. Implementation details
live in `tasks.md` and the code; this file is how you *verify* it.

## Prerequisites

- Go 1.24+
- `buf` CLI (for protobuf codegen) — or `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc`
- Make

## Setup

```bash
# From repo root
go mod download
make generate     # buf generate -> internal/genproto/asr from proto/asr.proto
go build ./...
```

## Configuration

All endpoints, timeouts, and buffer bounds are configurable (FR-013). Defaults for
local dev:

| Env var | Default | Meaning |
|---------|---------|---------|
| `ASR_LISTEN_ADDR` | `:8080` | WebSocket + `/metrics` listen address |
| `ASR_INFERENCE_ADDR` | `localhost:50051` | gRPC inference server target |
| `ASR_SESSION_TIMEOUT` | `30s` | Idle/stalled-stream timeout |
| `ASR_AUDIO_BUFFER` | `32` | Bounded up-pump channel capacity |
| `ASR_TRANSCRIPT_BUFFER` | `32` | Bounded down-pump channel capacity |
| `ASR_MAX_FRAME_BYTES` | `65536` | Max audio frame size |
| `ASR_WRITE_TIMEOUT` | `5s` | Slow-consumer termination threshold |

## Validation scenarios

Each scenario maps to a spec acceptance criterion. The automated versions live under
`test/` and `internal/*/*_test.go`; run them with `make test`.

### V1 — Full round trip (User Story 1 / SC-002)

Proves audio in → transcript out over one WebSocket, against the mock inference server.

```bash
make test-integration   # go test ./test/integration/... -run RoundTrip
```

Expected: a known audio fixture is streamed in chunks; the concatenated final
transcript equals the fixture's reference text; the connection closes with code 1000.

### V2 — Interim before final (User Story 2 / FR-004, FR-005)

Expected: for a multi-word utterance the client receives ≥1 `transcript` with
`is_final:false` **before** the `is_final:true` message; no interim follows a final for
the same segment.

```bash
go test ./test/integration/... -run InterimResults
```

### V3 — Resource safety on teardown (User Story 3 / SC-004)

Expected: after N connect→stream→disconnect cycles (including abrupt disconnects),
goroutine and active-session counts return to baseline (no leak). The mock server's
stream is observed cancelled on each client disconnect.

```bash
go test ./internal/session/... -run TeardownLeak
```

### V4 — Session isolation (User Story 3 / SC-005)

Expected: with one client sending malformed frames / refusing to read, other concurrent
sessions complete normally and unaffected.

```bash
go test ./test/integration/... -run Isolation
```

### V5 — Backpressure / slow consumer (FR-009)

Expected: a client that stops reading transcripts is terminated with close code 1008
within `ASR_WRITE_TIMEOUT`; memory stays bounded (no unbounded buffering).

```bash
go test ./internal/session/... -run Backpressure
```

### V6 — Contract tests (Principle V)

```bash
go test ./internal/transport/... -run Protocol   # WS message schema
go test ./internal/inference/...  -run Contract   # gRPC .proto stubs
buf lint && buf breaking --against '.git#branch=main'   # proto contract guard
```

## Manual smoke test (optional)

```bash
# Terminal 1: run the mock inference server (dev helper)
go run ./internal/inference/mock/cmd

# Terminal 2: run the gateway
ASR_INFERENCE_ADDR=localhost:50051 go run ./cmd/gateway

# Terminal 3: a tiny client (see test helpers) streams a wav and prints transcripts
go run ./test/tools/wsclient -file testdata/hello.wav
# -> prints interim then final transcript lines, exits 0
```

## Observability check (Principle IV / FR-011)

```bash
curl -s localhost:8080/metrics | grep '^asr_'
# expect: asr_active_sessions, asr_audio_bytes_in_total,
#         asr_transcript_latency_seconds, asr_inference_errors_total, asr_sessions_total
```

## Done when

- V1–V6 pass (V1 round trip is the merge gate — Constitution Principle V).
- `/metrics` exposes all five `asr_*` series.
- No goroutine/session leak across repeated connect/disconnect cycles.
