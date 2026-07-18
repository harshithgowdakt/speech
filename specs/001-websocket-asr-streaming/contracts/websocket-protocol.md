# Contract: WebSocket Client ⟷ Gateway Protocol

This is the client-facing contract (Constitution Principle II). It is versioned with
the feature and validated by contract tests in `internal/transport`.

## Connection

- **Endpoint**: `GET /v1/stream` (WebSocket upgrade). Subprotocol: `asr.v1`.
- **Framing**: WebSocket **binary** frames carry audio; **text** frames carry JSON
  control and result messages. Frame opcode disambiguates the two paths.
- **Lifecycle**: `connect` → `start` (config) → binary audio frames → `stop` →
  transcripts drain → close. See the Session state machine in `data-model.md`.

## Client → Gateway messages

### 1. `start` (text/JSON) — REQUIRED first message

Establishes recognition config. MUST precede any audio frame (FR-014). Sending audio
first is a protocol violation and the session is refused.

```json
{
  "type": "start",
  "config": {
    "encoding": "LINEAR16",
    "sample_rate_hz": 16000,
    "language_code": "en-US",
    "interim_results": true
  }
}
```

Field rules: `encoding` ∈ {`LINEAR16`,`FLAC`,`OGG_OPUS`,`MULAW`}; `sample_rate_hz` > 0;
`language_code` non-empty. Invalid → `error` message (`code: "invalid_config"`) then close.

### 2. Audio (binary) — zero or more frames

Raw audio bytes in the encoding declared in `start`. Each binary frame is relayed as one
`audio_content` message on the gRPC stream, in order. Frames larger than the configured
max are rejected (`code: "protocol_violation"`), session terminated.

### 3. `stop` (text/JSON) — end-of-stream

```json
{ "type": "stop" }
```

Signals the client has finished sending audio. The gateway half-closes the gRPC send
direction, drains remaining transcripts, sends them, then closes cleanly.

## Gateway → Client messages

### 1. `transcript` (text/JSON)

```json
{
  "type": "transcript",
  "text": "hello world",
  "is_final": false,
  "confidence": 0.0,
  "stability": 0.8
}
```

- `is_final: false` = interim (may change); `is_final: true` = final (will not change
  for that segment). A final for a segment is never followed by an interim for the same
  segment (User Story 2).
- Interim messages are only sent when `start.config.interim_results` is true.

### 2. `error` (text/JSON) — terminal

```json
{ "type": "error", "code": "inference_error", "message": "upstream stream aborted" }
```

`code` ∈ {`invalid_config`, `protocol_violation`, `inference_error`, `timeout`,
`slow_consumer`, `internal`}. Sent (when the connection still permits) immediately
before the WebSocket close.

## WebSocket close codes

| Situation | Close code | Notes |
|-----------|-----------|-------|
| Normal end after `stop` | 1000 (Normal) | Final transcripts delivered first |
| Invalid config / protocol violation | 1002 (Protocol error) | Preceded by `error` msg |
| Inference failure | 1011 (Internal error) | Preceded by `error` msg |
| Timeout (idle / stalled inference) | 1001 (Going away) or 1011 | Configurable timeout |
| Slow consumer (backpressure) | 1008 (Policy violation) | Session terminated (FR-009) |

## Ordering & concurrency guarantees

- Audio frames are relayed to the inference server strictly in receive order.
- Transcript messages are delivered to the client in the order the inference server
  produced them.
- One WebSocket connection = one Session = one gRPC stream (no multiplexing in v1).
- A misbehaving client (slow read, malformed frame, abrupt disconnect) affects only its
  own session (FR-007).
