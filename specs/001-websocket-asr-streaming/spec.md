# Feature Specification: WebSocket ASR Streaming Gateway

**Feature Branch**: `001-websocket-asr-streaming`

**Created**: 2026-07-18

**Status**: Draft

**Input**: User description: "Clients connect to a WebSocket server and send audio; the gateway streams the audio to an inference server over gRPC and streams the recognized text back to the client over the same WebSocket connection."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-time transcription of a live audio stream (Priority: P1)

A client application opens a WebSocket connection to the gateway, streams microphone
audio in small chunks as the user speaks, and receives text transcripts back on the
same connection while the user is still talking. When the user stops and the stream
ends, the client receives a final transcript and the connection closes cleanly.

**Why this priority**: This is the core value of the product. Without live audio-in →
text-out over a single WebSocket connection, nothing else matters. It is the smallest
slice that delivers a usable, demonstrable ASR service.

**Independent Test**: Open a WebSocket connection, send a known audio recording in
chunks, and assert that transcript messages are received and that the concatenated
final transcript matches the expected text within an accuracy tolerance. Fully
exercisable against a mock inference server.

**Acceptance Scenarios**:

1. **Given** a running gateway and inference server, **When** a client connects and streams valid audio chunks, **Then** the client receives one or more transcript messages containing recognized text.
2. **Given** an in-progress audio stream, **When** the inference server emits an interim (partial) transcript, **Then** the client receives that interim transcript before the audio stream has ended.
3. **Given** a client that has finished sending audio and signals end-of-stream, **When** the inference server produces its final result, **Then** the client receives a final transcript flagged as final and the session ends cleanly.

---

### User Story 2 - Interim vs. final transcript distinction (Priority: P2)

While speaking, the client receives frequently-updated interim transcripts that may
change, and can visually distinguish them from finalized transcript segments that will
not change.

**Why this priority**: Interim results make the experience feel real-time, but the
client must know which text is stable. This builds directly on P1 and sharpens its
value, but P1 is usable (final-only) without it.

**Independent Test**: Stream audio and assert that each transcript message carries an
`is_final` indicator, that at least one interim (`is_final=false`) message precedes
the final message for a multi-word utterance, and that final messages are never
retracted.

**Acceptance Scenarios**:

1. **Given** an active stream, **When** the gateway forwards transcripts, **Then** each transcript message indicates whether it is interim or final.
2. **Given** a sequence of transcripts for one utterance, **When** a final transcript is delivered, **Then** no further interim update for that same segment is sent.

---

### User Story 3 - Graceful handling of client and inference failures (Priority: P3)

When a client disconnects abruptly, sends malformed data, or the inference server
becomes unavailable or errors mid-stream, the affected session is torn down cleanly,
the client receives a meaningful error/close signal where possible, and no other
client's session is affected.

**Why this priority**: Essential for production reliability and directly enforces the
constitution's resource-safety principle, but the happy path (P1/P2) can be
demonstrated first.

**Independent Test**: Simulate (a) an abrupt client disconnect mid-stream, (b) a
malformed client message, and (c) an inference-server error, and assert in each case
that the session's resources are released and other concurrent sessions continue
unaffected.

**Acceptance Scenarios**:

1. **Given** an active session, **When** the client disconnects abruptly, **Then** the gateway cancels the corresponding inference stream and releases all session resources.
2. **Given** an active session, **When** the inference server returns an error, **Then** the gateway closes the client connection with an error indication and cleans up.
3. **Given** a client that sends a malformed message, **When** the gateway detects it, **Then** the gateway rejects that session with an error and continues serving other clients.

---

### Edge Cases

- **Empty / silent audio**: Client connects and sends no audio (or only silence) before closing — the gateway must close the session cleanly without emitting a spurious transcript or leaking resources.
- **Client stops reading**: Client keeps sending audio but stops consuming transcripts — backpressure must bound memory and eventually terminate the misbehaving session rather than grow without limit.
- **Inference server slow or stalled**: The inference stream produces no output within a configured timeout — the session times out with an observable error.
- **Oversized / high-rate audio chunks**: Client sends chunks larger or faster than expected — the gateway enforces bounds and rejects or throttles rather than crashing.
- **Reconnection**: Client drops and reconnects — treated as a brand-new independent session (no server-side resume in v1).
- **Unsupported audio format / sample rate**: Client sends audio in a format the inference server does not accept — the gateway surfaces a clear error to the client.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST accept client connections over WebSocket and allow a client to stream audio data in sequential chunks over a single connection.
- **FR-002**: The system MUST forward received audio to the inference server as a stream, without waiting for the full utterance to be received before forwarding begins.
- **FR-003**: The system MUST receive recognized text from the inference server as a stream and deliver each transcript back to the originating client over the same WebSocket connection.
- **FR-004**: The system MUST deliver interim (partial) transcripts to the client as soon as they are produced, before the audio stream has ended.
- **FR-005**: Each transcript delivered to the client MUST indicate whether it is interim or final.
- **FR-006**: The system MUST support a client-initiated end-of-stream signal, after which it delivers the final transcript(s) and ends the session cleanly.
- **FR-007**: The system MUST isolate each client session such that the failure, slowness, or misbehavior of one session does not affect any other concurrent session.
- **FR-008**: The system MUST release all resources associated with a session (including the corresponding inference stream) when the client disconnects, the inference stream errors, or a timeout occurs.
- **FR-009**: The system MUST bound the amount of audio/transcript data buffered per session (backpressure), and MUST terminate a session that violates these bounds rather than growing memory without limit.
- **FR-010**: The system MUST communicate errors to the client where the connection still permits (e.g., malformed input, inference failure, timeout) and then close the session.
- **FR-011**: The system MUST expose operational signals: count of active sessions, volume of audio ingested, transcript delivery latency, and inference error counts.
- **FR-012**: The system MUST record structured, per-session diagnostic information sufficient to trace a dropped connection or inference failure to its cause.
- **FR-013**: The system MUST allow its network endpoints, timeouts, and buffer bounds to be configured without code changes.
- **FR-014**: The system MUST validate incoming client messages against the defined message protocol and reject non-conforming input.

### Key Entities *(include if feature involves data)*

- **Session**: A single client's live interaction. Has a unique correlation identifier, a lifecycle (open → streaming → ending → closed), an associated inference stream, and bounded buffers. Owns all resources that must be released on teardown.
- **Audio Chunk**: A unit of audio sent by the client. Has ordering and a payload; carries or implies format metadata (sample rate, encoding) established at session start.
- **Transcript**: A unit of recognized text returned to the client. Carries the text and an interim/final indicator, and is associated with a session.
- **Session Metrics**: Per-session and aggregate operational data (active count, bytes ingested, latency, error counts) surfaced for observability.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A client streaming live audio receives its first interim transcript within 300ms (p95) of the corresponding audio being available at the gateway, beyond the inference server's own processing time.
- **SC-002**: For a supported test recording, the final transcript matches the reference transcription within the accuracy tolerance of the inference model (the gateway introduces no transcription errors of its own).
- **SC-003**: The gateway sustains at least 100 concurrent streaming sessions on a single instance without transcript latency degrading beyond the SC-001 target.
- **SC-004**: When a session ends for any reason (normal close, client disconnect, inference error, timeout), 100% of that session's resources are released — verified by no growth in goroutine/connection/memory counts across repeated connect/disconnect cycles.
- **SC-005**: A single misbehaving client (abrupt disconnect, malformed input, or refusal to read) affects 0 other concurrent sessions.
- **SC-006**: 100% of dropped connections and inference failures are observable through metrics and structured logs.

## Assumptions

- Clients are responsible for capturing and chunking audio; the gateway does not perform audio capture or resampling in v1 (audio is forwarded in the format the inference server accepts).
- The audio encoding, sample rate, and channel layout accepted by the inference server are fixed/known and agreed via configuration or session-start metadata; format negotiation is out of scope for v1.
- The ASR inference server exists and exposes a bidirectional streaming interface; this feature builds the gateway and the client-facing contract, not the acoustic model.
- There is no server-side session resume/persistence in v1; a dropped connection means a new session on reconnect.
- Authentication/authorization of clients is out of scope for this feature and, if required, will be layered in a separate feature.
- One inference stream corresponds to one client session (no fan-in/fan-out of a single client to multiple models in v1).
