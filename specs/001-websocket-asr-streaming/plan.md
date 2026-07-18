# Implementation Plan: WebSocket ASR Streaming Gateway

**Branch**: `001-websocket-asr-streaming` | **Date**: 2026-07-18 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-websocket-asr-streaming/spec.md`

## Summary

Build a Go gateway that accepts audio from clients over WebSocket and relays it, frame
by frame, to an ASR inference server over a gRPC **bidirectional** stream, returning
interim and final transcripts to the client on the same WebSocket connection in real
time. The design is three separable packages behind interfaces — WebSocket transport,
gRPC inference client, and session orchestration — with the `.proto` and the WebSocket
message schema as the contracts. A mock inference server enables a full round-trip
integration test without the real model.

## Technical Context

**Language/Version**: Go 1.24

**Primary Dependencies**:
- `github.com/coder/websocket` — WebSocket server (context-native; success to `nhooyr/websocket`)
- `google.golang.org/grpc` + `google.golang.org/protobuf` — gRPC inference client & codegen
- `github.com/prometheus/client_golang` — metrics
- `log/slog` (stdlib) — structured logging
- `github.com/stretchr/testify` — test assertions

**Storage**: N/A (stateless gateway; no persistence in v1)

**Testing**: `go test` — unit, contract (WS protocol + `.proto`), integration (round-trip vs. mock inference server via bufconn)

**Target Platform**: Linux server (containerizable), also runs on macOS/arm64 for local dev

**Project Type**: Single-project network service (Go module)

**Performance Goals**: ≥100 concurrent streaming sessions per instance; first interim transcript within 300ms p95 of audio availability, beyond model inference time

**Constraints**: ≤300ms p95 added gateway latency (Principle I); bounded per-session buffers, no unbounded memory (Principle III); deterministic teardown on disconnect/error/timeout

**Scale/Scope**: v1 targets a single gateway instance; horizontal scale is stateless (any instance serves any client) but load balancing / autoscaling is out of scope for this feature

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Status |
|-----------|------|--------|
| I. Streaming-First & Low Latency | Design relays frames without full-utterance buffering; interim transcripts forwarded immediately; ≤300ms p95 budget tracked | ✅ PASS — bidi gRPC stream, per-frame relay, no buffering step |
| II. Clean Transport/Inference Separation | WS transport, gRPC client, session orchestration are separate packages behind interfaces; `.proto` in repo as source of truth | ✅ PASS — see Project Structure; `InferenceClient` + `Transport` interfaces |
| III. Connection Lifecycle & Resource Safety | Per-session `context`, bounded channels, errgroup teardown on any exit; sessions isolated | ✅ PASS — one `context.Context` per session cancels all goroutines + gRPC stream; bounded send/recv channels |
| IV. Observability | slog with per-session correlation ID; Prometheus metrics for active conns, bytes in, latency, gRPC errors | ✅ PASS — `internal/metrics` + structured logging |
| V. Test Discipline | Contract tests for WS schema + `.proto`; round-trip integration test vs. mock inference server | ✅ PASS — mock server over bufconn; CI gate on integration test |

**Result**: PASS (initial). No violations → Complexity Tracking left empty. Re-checked after Phase 1: still PASS (design introduces no new dependencies or boundary crossings).

## Project Structure

### Documentation (this feature)

```text
specs/001-websocket-asr-streaming/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── asr.proto        # gRPC inference contract (source of truth)
│   └── websocket-protocol.md  # WebSocket client<->gateway message schema
├── checklists/
│   └── requirements.md  # Spec quality checklist (from /speckit-specify)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
cmd/
└── gateway/
    └── main.go              # Wiring: config -> transport + inference client -> serve

internal/
├── config/
│   └── config.go            # Env/file config: addrs, timeouts, buffer bounds
├── transport/               # Principle II boundary: WebSocket transport
│   ├── transport.go         # Transport interface + coder/websocket server impl
│   ├── protocol.go          # WS message encode/decode (binary audio, JSON control/transcript)
│   └── transport_test.go    # Contract tests for the WS message protocol
├── inference/               # Principle II boundary: gRPC inference client
│   ├── client.go            # InferenceClient interface + gRPC bidi-stream impl
│   ├── client_test.go       # Contract tests against generated stubs
│   └── mock/
│       └── server.go        # Mock inference server (bufconn) for integration tests
├── session/                 # Principle II boundary: orchestration
│   ├── session.go           # Session lifecycle: pumps audio->gRPC, transcripts->WS
│   ├── manager.go           # Active-session registry, isolation, teardown
│   └── session_test.go      # Unit tests for lifecycle/backpressure/cancellation
├── metrics/
│   └── metrics.go           # Prometheus collectors (Principle IV)
└── genproto/
    └── asr/                 # Generated Go from contracts/asr.proto (committed)

test/
└── integration/
    └── roundtrip_test.go    # Full audio-in -> transcript-out vs. mock inference server

proto/
└── asr.proto -> symlink or copy of contracts/asr.proto (buf/protoc source)

buf.yaml / buf.gen.yaml       # Codegen config (reproducible generation)
Makefile                      # generate, test, lint, run targets
go.mod / go.sum
```

**Structure Decision**: Single Go module, standard `cmd/` + `internal/` layout. The
three constitution boundaries (transport, inference, session) are distinct packages
under `internal/`, each fronted by an interface so `session` depends on abstractions,
not on `coder/websocket` or `grpc` concrete types. Generated protobuf code is committed
under `internal/genproto/` and regenerated via `buf` from `proto/asr.proto` (the same
file surfaced in `contracts/` as the design source of truth).

## Complexity Tracking

> No constitution violations. Section intentionally empty.
