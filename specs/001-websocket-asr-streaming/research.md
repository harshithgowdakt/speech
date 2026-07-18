# Phase 0 Research: WebSocket ASR Streaming Gateway

All Technical Context unknowns are resolved below. No open NEEDS CLARIFICATION remain.

## R1. WebSocket library

**Decision**: `github.com/coder/websocket` (formerly `nhooyr.io/websocket`).

**Rationale**:
- Context-native API: reads/writes take a `context.Context`, so a per-session context
  cancels blocked reads/writes directly — exactly what Principle III (deterministic
  teardown) needs. No manual `SetReadDeadline` juggling.
- Minimal, idiomatic surface; first-class `net.Conn` and message-type support for the
  mixed binary-audio / text-control protocol.
- Actively maintained successor to `nhooyr/websocket`.

**Alternatives considered**:
- `github.com/gorilla/websocket` — most widely deployed and battle-tested, but its API
  predates `context` and relies on deadline-based cancellation, making clean
  context-driven teardown more error-prone. Acceptable fallback if a coder/websocket
  issue surfaces; the `Transport` interface keeps the choice swappable.
- `golang.org/x/net/websocket` — deprecated, not RFC-6455-complete. Rejected.

## R2. gRPC streaming pattern for ASR

**Decision**: Bidirectional streaming RPC — `rpc StreamingRecognize(stream AudioChunk) returns (stream Transcript)`.

**Rationale**:
- ASR is inherently full-duplex: audio flows up continuously while transcripts flow
  down asynchronously (interim results arrive mid-utterance). Bidi streaming maps 1:1
  onto the WebSocket duplex and satisfies Principle I (no buffering, immediate interim
  forwarding).
- The first client message carries a `RecognitionConfig` (sample rate, encoding,
  language) so the server configures the recognizer before audio flows — mirrors the
  Google Cloud Speech streaming convention (a `StreamingRecognizeRequest` oneof of
  config-then-audio).

**Alternatives considered**:
- Server-streaming only (unary request with all audio) — breaks streaming-first; can't
  send audio incrementally. Rejected.
- Client-streaming only (stream audio, single response) — no interim transcripts.
  Rejected (violates FR-004).

## R3. Session concurrency & teardown model

**Decision**: One `context.Context` per session (derived from the connection), an
`errgroup.Group` running two pumps — **up-pump** (WS→gRPC audio) and **down-pump**
(gRPC→WS transcripts). First error or EOF cancels the context; `errgroup.Wait()`
guarantees both goroutines and the gRPC stream are torn down before the session is
removed from the manager.

**Rationale**:
- Deterministic teardown (Principle III): cancel once, everything unwinds. No orphan
  goroutines or leaked gRPC streams.
- `errgroup` propagates the first failure and cancels siblings — matches "any exit
  reason tears down the whole session."
- Session isolation: each session owns its context, channels, and stream; the manager
  holds only a registry keyed by session ID, so one session's failure can't touch
  another's state.

**Alternatives considered**:
- Manual `sync.WaitGroup` + shared error variable — reimplements errgroup, more
  error-prone. Rejected.
- One goroutine doing select over both directions — serializes up/down, adds latency
  and complicates backpressure. Rejected.

## R4. Backpressure & bounded buffers

**Decision**: Bounded channels between the WS layer and the gRPC pumps (default
capacity configurable, e.g. 32 frames each direction). When the down-pump can't write
to a slow client within the configured write timeout, the session is terminated with a
`slow-consumer` error rather than buffering unboundedly.

**Rationale**:
- Directly enforces FR-009 and Principle III (bounded memory, terminate misbehaving
  sessions). A slow/absent consumer becomes a fast, observable failure, not a leak.

**Alternatives considered**:
- Unbounded channels / slices — classic memory-growth footgun. Rejected outright by
  Principle III.
- Dropping frames silently — corrupts transcription and is unobservable. Rejected.

## R5. WebSocket message framing (audio vs. control vs. transcript)

**Decision**: Hybrid framing.
- **Client → gateway**: binary WebSocket frames carry raw audio bytes; text (JSON)
  frames carry control messages (`start` with config, `stop`/end-of-stream).
- **Gateway → client**: text (JSON) frames carry transcript messages (`text`,
  `is_final`) and error/close messages.

**Rationale**:
- Binary frames avoid base64 overhead for the high-volume audio path (latency +
  bandwidth). JSON control/transcript frames are easy for any client to parse and
  version. WebSocket's native binary/text opcode distinction makes routing trivial.
- Full schema is pinned in `contracts/websocket-protocol.md` (the client-facing
  contract, Principle II).

**Alternatives considered**:
- All-JSON (base64 audio) — simpler, but ~33% bandwidth overhead and encode/decode
  cost on the hot path. Rejected for the audio path.
- All-binary (custom length-prefixed protocol) — most compact but harder for clients to
  implement and evolve. Rejected for control/transcript.

## R6. Codegen toolchain

**Decision**: `buf` (`buf generate`) driving `protoc-gen-go` + `protoc-gen-go-grpc`,
config in `buf.gen.yaml`; generated code committed under `internal/genproto/`.

**Rationale**:
- Reproducible generation (Principle II: generated code reproducible from `.proto`),
  built-in lint/breaking-change checks guard the contract, no local `protoc` install
  drift. Committing generated code keeps `go build` working without a codegen step.

**Alternatives considered**:
- Raw `protoc` with hand-managed plugin paths — reproducibility and lint gaps. Usable
  fallback if `buf` unavailable.

## R7. Observability stack

**Decision**: `log/slog` (stdlib) for structured JSON logs with a per-session
correlation ID injected via context; `prometheus/client_golang` for metrics exposed on
`/metrics`. gRPC context carries the correlation ID as metadata for cross-service
tracing.

**Rationale**:
- Satisfies Principle IV / FR-011 / FR-012 with minimal dependencies (slog is stdlib).
  Prometheus is the de-facto Go metrics standard and pairs with standard dashboards.
- Metrics defined: `asr_active_sessions` (gauge), `asr_audio_bytes_in_total` (counter),
  `asr_transcript_latency_seconds` (histogram), `asr_inference_errors_total` (counter
  by code), `asr_sessions_total` (counter by outcome).

**Alternatives considered**:
- OpenTelemetry end-to-end — richer tracing but heavier for v1; can be layered later
  since context propagation is already in place. Deferred.

## Resolved unknowns summary

| Unknown (from Technical Context) | Resolution |
|----------------------------------|------------|
| WebSocket library | `coder/websocket` (R1) |
| gRPC streaming shape | Bidirectional `StreamingRecognize` (R2) |
| Concurrency/teardown | per-session context + errgroup two-pump (R3) |
| Backpressure strategy | bounded channels + slow-consumer termination (R4) |
| WS framing | binary audio / JSON control+transcript (R5) |
| Codegen | buf → protoc-gen-go(+grpc), committed (R6) |
| Observability | slog + Prometheus + context correlation ID (R7) |
