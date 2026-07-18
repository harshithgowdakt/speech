# Phase 1 Data Model: WebSocket ASR Streaming Gateway

Entities are in-memory only (stateless gateway; no persistence in v1). Field types are
Go-oriented but the shapes map directly onto the `.proto` and WebSocket contracts.

## Session

The unit of one client's live interaction. Owns every resource that must be released on
teardown.

| Field | Type | Notes |
|-------|------|-------|
| `ID` | `string` (UUID) | Correlation ID; appears in every log line and gRPC metadata |
| `State` | `SessionState` | See state machine below |
| `Config` | `RecognitionConfig` | Established from the client `start` message |
| `Ctx` | `context.Context` | Per-session; cancellation tears everything down |
| `Cancel` | `context.CancelFunc` | Called on any exit reason |
| `AudioIn` | `chan []byte` (bounded) | WS→gRPC frames; capacity from config |
| `Transcripts` | `chan Transcript` (bounded) | gRPC→WS results; capacity from config |
| `StartedAt` | `time.Time` | For latency/duration metrics |
| `BytesIn` | `int64` (atomic) | Audio volume ingested (FR-011) |

**Validation rules**:
- `Config` MUST be received (valid `start` message) before any audio frame is accepted (FR-014); audio before config → reject session with protocol error.
- `AudioIn` / `Transcripts` capacities MUST be > 0 and bounded (Principle III / FR-009).
- A session MUST be removed from the `Manager` registry exactly once, after teardown completes.

### SessionState (state machine)

```text
        start(valid config)         audio/EOF or error
  Opening ───────────────► Streaming ───────────────► Ending ───► Closed
     │                         │                         │
     │ invalid start /         │ client disconnect /     │ teardown
     │ timeout                 │ inference error /        │ complete
     └──────────────► Closed ◄─┴ slow-consumer / timeout ─┘
```

- **Opening**: connection accepted, awaiting `start` control message.
- **Streaming**: config accepted, gRPC stream open, audio flowing up, transcripts flowing down.
- **Ending**: client signaled end-of-stream (`stop`) or an error occurred; draining final transcripts.
- **Closed**: all goroutines joined, gRPC stream closed, resources released, removed from registry.

Any state may transition directly to **Closed** on client disconnect, inference error,
or timeout (FR-008).

## RecognitionConfig

Session-start parameters agreed once, sent by the client and forwarded to the inference
server before audio.

| Field | Type | Notes |
|-------|------|-------|
| `SampleRateHz` | `int32` | e.g. 16000 |
| `Encoding` | `AudioEncoding` enum | e.g. `LINEAR16`, `OGG_OPUS` |
| `LanguageCode` | `string` | e.g. `en-US` |
| `InterimResults` | `bool` | Request interim transcripts (default true) |

**Validation**: `SampleRateHz` > 0; `Encoding` non-`UNSPECIFIED`; `LanguageCode`
non-empty. Invalid config → protocol error, session refused (FR-010, FR-014).

## AudioChunk

A unit of audio sent by the client, forwarded to the inference server. On the WebSocket
it is a raw **binary** frame; on gRPC it is a message.

| Field | Type | Notes |
|-------|------|-------|
| `Data` | `[]byte` | Raw audio payload in the negotiated encoding |
| `Seq` | `uint64` (gateway-assigned) | Monotonic per session; for ordering/diagnostics |

**Validation**: chunk size ≤ configured max (reject/terminate if exceeded, edge case
"oversized chunks"); chunks are relayed in receive order.

## Transcript

A recognition result returned to the client (gRPC message → WS **text/JSON** frame).

| Field | Type | Notes |
|-------|------|-------|
| `Text` | `string` | Recognized text for this result |
| `IsFinal` | `bool` | Interim (false) vs. final (true) — FR-005 |
| `Confidence` | `float32` | Optional model confidence (0..1); omit if unavailable |
| `Stability` | `float32` | Optional interim stability (0..1) |

**Rules**: A final transcript for a segment MUST NOT be followed by an interim update
for that same segment (FR + User Story 2). Interim results only emitted when
`Config.InterimResults` is true.

## SessionError

Structured error surfaced to the client (WS JSON frame) and to logs/metrics.

| Field | Type | Notes |
|-------|------|-------|
| `Code` | `string` enum | `invalid_config`, `protocol_violation`, `inference_error`, `timeout`, `slow_consumer`, `internal` |
| `Message` | `string` | Human-readable detail |

Maps to a WebSocket close code + a final JSON error message where the connection still
permits (FR-010).

## Manager (registry)

Not a wire entity — the in-process owner of active sessions.

| Field | Type | Notes |
|-------|------|-------|
| `sessions` | `map[string]*Session` guarded by `sync.RWMutex` | Active sessions by ID |
| `Count()` | method | Backs `asr_active_sessions` gauge (FR-011) |

**Rules**: registration on session open, removal on Closed; the map is the only shared
state and is the isolation boundary between sessions (FR-007).
