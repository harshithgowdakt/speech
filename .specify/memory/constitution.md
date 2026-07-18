<!--
Sync Impact Report
==================
Version change: (none) → 1.0.0
Rationale: Initial ratification of the project constitution (MAJOR bump from unversioned template).

Modified principles: N/A (initial adoption)
Added principles:
  - I. Streaming-First & Low Latency
  - II. Clean Transport/Inference Separation
  - III. Connection Lifecycle & Resource Safety
  - IV. Observability
  - V. Test Discipline (NON-NEGOTIABLE)
Added sections:
  - Technology & Architecture Constraints
  - Development Workflow & Quality Gates
  - Governance

Templates requiring updates:
  - .specify/templates/plan-template.md ✅ reviewed (Constitution Check gate is principle-agnostic; compatible)
  - .specify/templates/spec-template.md ✅ reviewed (no principle-specific slots; compatible)
  - .specify/templates/tasks-template.md ✅ reviewed (task categories cover contract/integration/observability; compatible)

Follow-up TODOs: none
-->

# ASR Gateway Constitution

The ASR Gateway is a Go service that accepts audio from clients over WebSocket,
streams that audio to an ASR inference server over gRPC bidirectional streaming,
and streams recognized text transcripts back to the WebSocket client in real time.

## Core Principles

### I. Streaming-First & Low Latency

Audio and transcripts MUST flow as bidirectional streams end to end
(WebSocket ⟷ gateway ⟷ gRPC). The gateway MUST NOT buffer a full utterance before
forwarding: audio frames are relayed to the inference server as they arrive, and
partial/interim transcripts are emitted to the client as soon as the inference
server produces them.

The gateway MUST add no more than 300ms of latency (p95), measured end to end,
beyond the inference server's own processing time. Any design choice that trades
latency for throughput MUST be justified in the plan's Complexity Tracking section.

**Rationale**: Real-time ASR is only useful if transcripts appear while the user is
still speaking. Buffering or synchronous request/response patterns defeat the purpose.

### II. Clean Transport/Inference Separation

The WebSocket transport layer, the gRPC inference client, and the session
orchestration logic MUST be separately testable packages, with interfaces defined
at each boundary. No package may reach across a boundary into another's concrete
types.

The gRPC contract (`.proto`) is the SINGLE SOURCE OF TRUTH for the inference API,
lives in the repository, and generated code MUST be reproducible from it. Changes to
the wire protocol (WebSocket message schema or `.proto`) MUST be made in the contract
first, then propagated to implementations.

**Rationale**: Boundaries that are interfaces can be mocked, and mockable boundaries
are what make the round-trip integration test (Principle V) possible without a real
model.

### III. Connection Lifecycle & Resource Safety

Every client session MUST have deterministic setup and teardown. On client
disconnect, inference error, or timeout, ALL goroutines and the associated gRPC
stream for that session MUST be cleaned up — verified via context cancellation, not
best-effort.

Per-connection state MUST be isolated: a slow, malformed, or crashing client MUST
NOT affect any other client. Backpressure MUST be explicit — bounded channels and
context deadlines only. Unbounded buffers or unbounded goroutine growth are
PROHIBITED.

**Rationale**: A gateway multiplexes untrusted, unreliable clients onto shared
inference capacity. Without strict lifecycle and backpressure rules, one bad client
takes down the service or leaks the process to death.

### IV. Observability

Structured logging (with a per-session correlation ID), metrics, and context
propagation for distributed tracing are MANDATORY, not optional add-ons.

At minimum the service MUST expose: active connection count, audio bytes ingested,
transcript emission latency, and gRPC error counts. Every dropped connection and
every inference failure MUST be observable through logs and metrics.

**Rationale**: Streaming failures are often silent (a stalled stream, a leaked
goroutine). You cannot operate what you cannot see.

### V. Test Discipline (NON-NEGOTIABLE)

Contract tests MUST validate both the WebSocket message protocol and the gRPC
`.proto` interface. An integration test MUST exercise a full audio-in →
transcript-out round trip against a MOCK inference server.

No change merges unless the round-trip integration test passes. Tests for a new
boundary or contract change are written and reviewed BEFORE the implementation that
satisfies them.

**Rationale**: The core value of this system is the end-to-end stream. A green unit
test suite that never proves audio in produces text out is not evidence the system
works.

## Technology & Architecture Constraints

- **Language/Runtime**: Go 1.24+.
- **WebSocket**: A single, vetted WebSocket library (e.g. `github.com/coder/websocket`
  or `github.com/gorilla/websocket`) chosen in the first plan and used consistently.
- **Inference transport**: `google.golang.org/grpc` with Protocol Buffers; the
  inference API uses bidirectional streaming.
- **Contract location**: `.proto` files and the WebSocket message schema live in the
  repository under a versioned path; generated Go code is committed and reproducible.
- **Configuration**: All endpoints, timeouts, and buffer bounds are configurable
  (env or config file) — no hardcoded network addresses or magic timeout constants.
- **Dependencies**: New third-party dependencies MUST be justified; prefer the
  standard library and the already-chosen core libraries.

## Development Workflow & Quality Gates

- Every feature follows the Spec-Kit flow: **specify → (clarify) → plan → tasks →
  implement**, with the Constitution Check gate in the plan enforced before and after
  design.
- Code review MUST verify compliance with all five principles; a reviewer MAY block a
  change that violates a principle without a documented, approved justification.
- `go vet` and `go test ./...` (including the round-trip integration test) MUST pass
  before merge.
- Any deviation from a principle MUST be recorded in the plan's Complexity Tracking
  table with the simpler alternative that was rejected and why.

## Governance

This constitution supersedes other development practices for the ASR Gateway. When a
practice and this document conflict, this document wins.

- **Amendments** require a documented change, an updated version number, and a Sync
  Impact Report noting which templates or artifacts need follow-up.
- **Versioning** follows semantic versioning: MAJOR for backward-incompatible
  governance/principle removals or redefinitions, MINOR for a newly added or
  materially expanded principle/section, PATCH for clarifications and wording.
- **Compliance review**: Every plan runs the Constitution Check gate; every PR review
  confirms the five principles hold. Complexity that violates a principle must be
  justified or removed.

**Version**: 1.0.0 | **Ratified**: 2026-07-18 | **Last Amended**: 2026-07-18
