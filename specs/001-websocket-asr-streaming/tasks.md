---

description: "Task list for WebSocket ASR Streaming Gateway"
---

# Tasks: WebSocket ASR Streaming Gateway

**Input**: Design documents from `specs/001-websocket-asr-streaming/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: INCLUDED — Constitution Principle V (Test Discipline) is non-negotiable:
contract tests (WS protocol + `.proto`) and a full round-trip integration test against a
mock inference server are required. Test tasks are therefore part of every story.

**Organization**: Tasks are grouped by user story (from spec.md) to enable independent
implementation, testing, and delivery.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1 / US2 / US3 (setup, foundational, and polish tasks carry no story label)

## Path Conventions

Single Go module at repository root. Source under `cmd/` and `internal/`; tests colocated
in `*_test.go` plus `test/integration/`. Contracts mirrored from
`specs/001-websocket-asr-streaming/contracts/` into `proto/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and toolchain

- [x] T001 Initialize Go module `github.com/harshithgowda/asr` (`go mod init`) and create the directory tree from plan.md (`cmd/gateway`, `internal/{config,transport,inference,inference/mock,session,metrics,genproto}`, `test/integration`, `proto`)
- [x] T002 Add dependencies to go.mod: `github.com/coder/websocket`, `google.golang.org/grpc`, `google.golang.org/protobuf`, `github.com/prometheus/client_golang`, `golang.org/x/sync/errgroup`, `github.com/google/uuid`, `github.com/stretchr/testify`
- [x] T003 [P] Copy `contracts/asr.proto` to `proto/asr.proto` and add `buf.yaml` + `buf.gen.yaml` configuring `protoc-gen-go` and `protoc-gen-go-grpc` output to `internal/genproto/asr`
- [x] T004 [P] Add a `Makefile` with targets `generate` (buf generate), `build`, `test`, `test-integration`, `lint` (go vet + buf lint), `run`
- [x] T005 [P] Add `.gitignore` (Go build artifacts, `.claude/` per Spec-Kit security note) and `.golangci`/formatting config

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure every user story depends on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T006 Run codegen (`make generate`) to produce `internal/genproto/asr/*.pb.go` from `proto/asr.proto`; commit generated code
- [x] T007 [P] Implement configuration loader in `internal/config/config.go` — env vars from quickstart.md (`ASR_LISTEN_ADDR`, `ASR_INFERENCE_ADDR`, `ASR_SESSION_TIMEOUT`, `ASR_AUDIO_BUFFER`, `ASR_TRANSCRIPT_BUFFER`, `ASR_MAX_FRAME_BYTES`, `ASR_WRITE_TIMEOUT`) with defaults and validation
- [x] T008 [P] Implement observability primitives in `internal/metrics/metrics.go` — Prometheus collectors `asr_active_sessions`, `asr_audio_bytes_in_total`, `asr_transcript_latency_seconds`, `asr_inference_errors_total`, `asr_sessions_total`; plus a `slog` JSON handler and a context helper carrying the per-session correlation ID
- [x] T009 Define boundary interfaces and shared domain types in `internal/session/types.go` — `RecognitionConfig`, `Transcript`, `SessionError`, `SessionState`; the `Transport` interface (`internal/transport`) and `InferenceClient` interface (`internal/inference`) so `session` depends on abstractions (Principle II)
- [x] T010 [P] Implement the gRPC `InferenceClient` in `internal/inference/client.go` — dial `ASR_INFERENCE_ADDR`, open `StreamingRecognize` bidi stream, send config-then-audio, expose channels/methods matching the `InferenceClient` interface; propagate correlation ID via gRPC metadata
- [x] T011 [P] Implement the mock inference server in `internal/inference/mock/server.go` — implements `ASRServiceServer` over `bufconn`, echoes deterministic interim+final transcripts for given audio, and supports injectable behaviors (error, stall, slow) for US3 tests
- [x] T012 [P] Implement the WebSocket `Transport` in `internal/transport/transport.go` + message codec in `internal/transport/protocol.go` — `coder/websocket` upgrade at `GET /v1/stream`, binary=audio / JSON=control per `contracts/websocket-protocol.md`, close-code mapping

**Checkpoint**: Config, metrics/logging, generated stubs, both boundary implementations, and the mock server exist — session orchestration and user stories can now be built.

---

## Phase 3: User Story 1 - Real-time transcription of a live audio stream (Priority: P1) 🎯 MVP

**Goal**: Audio in over WebSocket → text out over the same WebSocket, end to end, via a bidi gRPC stream to the (mock) inference server.

**Independent Test**: Stream a known audio fixture in chunks; assert transcript messages are received and the concatenated final transcript matches the reference within tolerance; connection closes with code 1000. (`make test-integration -run RoundTrip`)

### Tests for User Story 1 (write first, must FAIL before implementation) ⚠️

- [x] T013 [P] [US1] Contract test for the WS protocol happy path in `internal/transport/protocol_test.go` — `start` → binary audio → `stop` encode/decode round-trips per `contracts/websocket-protocol.md`
- [x] T014 [P] [US1] Contract test for the gRPC client in `internal/inference/client_test.go` — config-then-audio ordering and transcript reception against generated stubs via bufconn
- [x] T015 [P] [US1] Round-trip integration test in `test/integration/roundtrip_test.go` — full audio-in→transcript-out against the mock server (the merge gate, quickstart V1)

### Implementation for User Story 1

- [x] T016 [US1] Implement the Session type and lifecycle in `internal/session/session.go` — per-session `context`, bounded `AudioIn`/`Transcripts` channels, up-pump (WS→gRPC) and down-pump (gRPC→WS) via `errgroup`, Opening→Streaming→Ending→Closed transitions (depends on T009–T012)
- [x] T017 [US1] Implement the Manager registry in `internal/session/manager.go` — register on open, remove on Closed, `Count()` backing the active-sessions gauge (depends on T016)
- [x] T018 [US1] Wire the gateway entrypoint in `cmd/gateway/main.go` — load config, construct transport + inference client + manager, serve `/v1/stream` and `/metrics`, graceful shutdown (depends on T007, T008, T010, T012, T017)
- [x] T019 [US1] Emit US1 metrics/logs — increment `asr_audio_bytes_in_total`, observe `asr_transcript_latency_seconds`, set `asr_active_sessions`, structured log lines with correlation ID (depends on T016, T018)

**Checkpoint**: MVP — a client can stream audio and receive transcripts over one WebSocket; T015 round-trip test passes.

---

## Phase 4: User Story 2 - Interim vs. final transcript distinction (Priority: P2)

**Goal**: Client receives frequently-updated interim transcripts and can distinguish them from finalized segments.

**Independent Test**: Stream a multi-word utterance; assert ≥1 `is_final:false` precedes the `is_final:true` message and no interim follows a final for the same segment. (`test/integration -run InterimResults`)

### Tests for User Story 2 (write first, must FAIL) ⚠️

- [x] T020 [P] [US2] Integration test in `test/integration/interim_test.go` — asserts interim-before-final ordering and no-interim-after-final; honors `interim_results` config flag

### Implementation for User Story 2

- [x] T021 [US2] Propagate `is_final` (and optional `confidence`/`stability`) through the down-pump transcript mapping in `internal/session/session.go` and the JSON encoder in `internal/transport/protocol.go` (depends on T016)
- [x] T022 [US2] Honor `RecognitionConfig.InterimResults` — suppress interim frames when the client did not request them; forward the flag on the gRPC config message (depends on T010, T016)
- [x] T023 [US2] Extend the mock server (`internal/inference/mock/server.go`) to emit a realistic interim→final sequence for the test fixture (depends on T011)

**Checkpoint**: US1 + US2 both work independently; interim/final semantics verified.

---

## Phase 5: User Story 3 - Graceful handling of client and inference failures (Priority: P3)

**Goal**: Clean, isolated teardown on client disconnect, malformed input, inference error, timeout, and slow consumer — no cross-session impact, no leaks.

**Independent Test**: Inject (a) abrupt disconnect, (b) malformed frame, (c) inference error, (d) slow consumer; assert each session's resources release, correct close codes, and other sessions unaffected. (`internal/session -run Teardown|Backpressure`, `test/integration -run Isolation`)

### Tests for User Story 3 (write first, must FAIL) ⚠️

- [x] T024 [P] [US3] Teardown/leak test in `internal/session/session_test.go` — repeated connect→stream→disconnect (incl. abrupt) returns goroutine + active-session counts to baseline (quickstart V3, SC-004)
- [x] T025 [P] [US3] Backpressure test in `internal/session/session_test.go` — a non-reading client is terminated with close 1008 within `ASR_WRITE_TIMEOUT`; buffers stay bounded (quickstart V5, FR-009)
- [x] T026 [P] [US3] Isolation integration test in `test/integration/isolation_test.go` — one malformed/slow client does not affect concurrent healthy sessions (quickstart V4, SC-005)

### Implementation for User Story 3

- [x] T027 [US3] Implement error/timeout handling and close-code mapping in `internal/session/session.go` + `internal/transport/protocol.go` — `invalid_config`/`protocol_violation`/`inference_error`/`timeout`/`slow_consumer` → `error` message + WS close code per contract; increment `asr_inference_errors_total` and finalize `asr_sessions_total{outcome}` (depends on T016, T027-adjacent)
- [x] T028 [US3] Enforce backpressure & frame-size limits — bounded-channel blocking with `ASR_WRITE_TIMEOUT` slow-consumer termination and `ASR_MAX_FRAME_BYTES` rejection in `internal/session/session.go` / `internal/transport/protocol.go` (depends on T016, T007)
- [x] T029 [US3] Enforce session timeout via context deadline and validate `start`-before-audio protocol ordering in `internal/session/session.go` (depends on T016)

**Checkpoint**: All three user stories independently functional; failure modes covered and observable.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Hardening and validation across stories

- [x] T030 [P] Add `buf lint` + `buf breaking` to CI/Make and a `README.md` documenting run, config, and the two contracts
- [x] T031 [P] Add a dev CLI for the mock server (`internal/inference/mock/cmd/main.go`) and a tiny WS test client (`test/tools/wsclient`) for the manual smoke test in quickstart.md
- [x] T032 Run `go vet ./...`, `go test ./...`, and full `quickstart.md` validation (V1–V6); confirm `/metrics` exposes all five `asr_*` series
- [x] T033 [P] Load check: verify ≥100 concurrent sessions sustain the SC-001 latency target and SC-003 without leak

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies — start immediately
- **Foundational (Phase 2)**: depends on Setup — BLOCKS all user stories
- **User Stories (Phase 3–5)**: depend on Foundational; then proceed in priority order (P1→P2→P3) or in parallel if staffed
- **Polish (Phase 6)**: depends on the desired user stories being complete

### User Story Dependencies

- **US1 (P1)**: after Foundational — no dependency on other stories (MVP)
- **US2 (P2)**: after Foundational — extends US1's transcript path but is independently testable
- **US3 (P3)**: after Foundational — hardens US1/US2 paths but is independently testable

### Within Each User Story

- Tests (T013–T015, T020, T024–T026) written and failing BEFORE implementation
- Types/interfaces before session; session before manager; manager before entrypoint wiring
- Core happy path before error/backpressure handling

### Parallel Opportunities

- Setup: T003, T004, T005 in parallel
- Foundational: T007, T008, T010, T011, T012 in parallel after T006 (interfaces T009 before T010/T012 consumers)
- US1 tests T013, T014, T015 in parallel; US3 tests T024, T025, T026 in parallel
- With a team: US1 / US2 / US3 in parallel once Foundational is done

---

## Parallel Example: User Story 1

```bash
# Write US1 tests together (they must fail first):
Task: "Contract test WS protocol in internal/transport/protocol_test.go"   # T013
Task: "Contract test gRPC client in internal/inference/client_test.go"      # T014
Task: "Round-trip integration test in test/integration/roundtrip_test.go"   # T015
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Phase 1 Setup → 2. Phase 2 Foundational (CRITICAL) → 3. Phase 3 US1 →
4. **STOP & VALIDATE**: `make test-integration -run RoundTrip` passes → demo the MVP.

### Incremental Delivery

Setup + Foundational → US1 (MVP, round trip) → US2 (interim/final) → US3 (resilience) →
Polish. Each story adds value without breaking the previous.

---

## Notes

- [P] = different files, no incomplete dependencies.
- Round-trip integration test (T015) is the non-negotiable merge gate (Constitution V).
- Generated protobuf (`internal/genproto/asr`) is committed and reproducible via `make generate`.
- Commit after each task or logical group; verify tests fail before implementing.

**Total tasks**: 33 — Setup 5 (T001–T005), Foundational 7 (T006–T012), US1 7 (T013–T019),
US2 4 (T020–T023), US3 6 (T024–T029), Polish 4 (T030–T033).
