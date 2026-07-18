# ASR Gateway

A Go service that accepts audio from clients over **WebSocket**, streams it to an
ASR inference server over **gRPC** (bidirectional streaming), and streams
recognized text transcripts back to the client in real time.

Built with [Spec-Kit](https://github.com/github/spec-kit) — see
[`specs/001-websocket-asr-streaming/`](specs/001-websocket-asr-streaming/) for the
spec, plan, contracts, and tasks, and
[`.specify/memory/constitution.md`](.specify/memory/constitution.md) for the
project principles.

## Architecture

```
client ──WebSocket──► gateway ──gRPC bidi stream──► ASR inference server
       ◄──transcripts──        ◄──transcripts──────
```

Three interface-fronted packages (hexagonal boundaries, Constitution Principle II):

| Package | Role |
|---------|------|
| `internal/transport` | WebSocket adapter (`coder/websocket`) — implements `session.ClientConn` |
| `internal/inference`  | gRPC adapter — implements `session.InferenceClient`; `mock/` is the test server |
| `internal/session`    | Orchestration core — per-session `context` + `errgroup` two-pump lifecycle |
| `internal/config`     | Env-based configuration |
| `internal/metrics`    | Prometheus collectors + structured `slog` logging |

The gRPC contract is [`proto/asr.proto`](proto/asr.proto) (generated code committed
under `internal/genproto/asr`). The client protocol is
[`contracts/websocket-protocol.md`](specs/001-websocket-asr-streaming/contracts/websocket-protocol.md).

## Build & test

```bash
make generate   # regenerate protobuf (requires protoc + protoc-gen-go[-grpc])
make build
make test              # all tests
make test-integration  # the audio-in -> transcript-out round trip (merge gate)
make lint              # go vet
```

## Run locally

```bash
# Terminal 1 — mock inference server
go run ./internal/inference/mock/cmd            # listens on :50051

# Terminal 2 — the gateway
ASR_INFERENCE_ADDR=localhost:50051 go run ./cmd/gateway   # listens on :8080

# Terminal 3 — smoke client (mock echoes the file's bytes back as transcript text)
echo "hello world" > /tmp/hello.txt
go run ./test/tools/wsclient -file /tmp/hello.txt
# -> [interim] hello world
#    [FINAL] hello world
```

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `ASR_LISTEN_ADDR` | `:8080` | WebSocket + `/metrics` listen address |
| `ASR_INFERENCE_ADDR` | `localhost:50051` | gRPC inference server target |
| `ASR_SESSION_TIMEOUT` | `30s` | Idle/stalled-stream timeout |
| `ASR_MAX_FRAME_BYTES` | `65536` | Max audio frame size |
| `ASR_WRITE_TIMEOUT` | `5s` | Slow-consumer termination threshold |
| `ASR_AUDIO_BUFFER` / `ASR_TRANSCRIPT_BUFFER` | `32` | Reserved buffer bounds |

## Observability

`GET /metrics` (Prometheus) exposes: `asr_active_sessions`,
`asr_audio_bytes_in_total`, `asr_transcript_latency_seconds`,
`asr_inference_errors_total`, `asr_sessions_total`. `GET /healthz` returns 200.
Logs are structured JSON via `slog`, each line carrying the per-session
`session_id`.

## Endpoints

- `GET /v1/stream` — WebSocket, subprotocol `asr.v1`
- `GET /metrics` — Prometheus metrics
- `GET /healthz` — liveness
