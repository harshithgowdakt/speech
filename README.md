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

## Inference server

The gateway needs an ASR inference server implementing `asr.v1`. A ready-to-run
one (OpenAI Whisper via faster-whisper, GPU) lives in
[`inference-server/`](inference-server) with its own
[chart](deploy/helm/asr-inference) for AWS EKS GPU nodes. Any server implementing
[`proto/asr.proto`](proto/asr.proto) works.

## Deploy to Kubernetes (Helm)

Full stack, in order (namespace `asr`):

```bash
# 1. KEDA operator (once per cluster) — for the gateway's connection-based autoscaling
helm upgrade --install keda deploy/helm/keda -n keda --create-namespace

# 2. Inference server (GPU node group required)
helm upgrade --install asr-inference deploy/helm/asr-inference -n asr --create-namespace \
  --set image.tag=0.1.0 --set config.whisperModel=small

# 3. Gateway, pointed at the inference service
helm upgrade --install asr deploy/helm/asr-gateway -n asr \
  --set image.tag=0.1.0 \
  --set config.inferenceAddr=asr-inference.asr.svc.cluster.local:50051
```

The gateway chart lives in [`deploy/helm/asr-gateway`](deploy/helm/asr-gateway). Build and push
the image (see [`Dockerfile`](Dockerfile) — distroless, non-root), then install:

```bash
docker build -t ghcr.io/harshithgowdakt/asr-gateway:0.1.0 .
docker push  ghcr.io/harshithgowdakt/asr-gateway:0.1.0

helm upgrade --install asr deploy/helm/asr-gateway \
  --namespace asr --create-namespace \
  --set image.tag=0.1.0 \
  --set config.inferenceAddr=my-asr-model:50051
```

The chart ships: Deployment, Service, ServiceAccount, and opt-in HPA, Ingress
(NGINX WebSocket timeouts preconfigured), ServiceMonitor (Prometheus Operator),
and PodDisruptionBudget. Long-lived WebSocket sessions drain on rollout via
`terminationGracePeriodSeconds` (default 60s). Enable extras as needed:

```bash
helm upgrade --install asr deploy/helm/asr-gateway \
  --set autoscaling.enabled=true \
  --set ingress.enabled=true --set ingress.hosts[0].host=asr.example.com \
  --set metrics.serviceMonitor.enabled=true \
  --set podDisruptionBudget.enabled=true
```

Run chart tests after install: `helm test asr -n asr`.

## Autoscaling & graceful shutdown

WebSocket connections are long-lived and stateful, which breaks naive
CPU-based HPA. This service is built for it:

**Thousands of connections per pod.** Two things make this real rather than
theoretical: (1) a **pool of gRPC connections** (`ASR_INFERENCE_POOL_SIZE`) to
the inference server — a single HTTP/2 connection caps concurrent streams, so
sessions are round-robined across the pool; and (2) a **per-pod connection cap**
(`ASR_MAX_CONNECTIONS`) that sheds overload as HTTP 503 instead of exhausting
memory/FDs. Raise the container file-descriptor limit (`ulimit -n`) accordingly.

**Scale on connections, not CPU.** The gateway is I/O-bound, so the binding
resource is *concurrent sessions*, not CPU. The chart's default autoscaler is
**KEDA** scaling on the `asr_active_sessions` metric via Prometheus
(`autoscaling.mode: keda`). Because existing sessions never rebalance onto
newly-added pods, set the per-pod threshold **below** real capacity for
headroom. A native-HPA-on-CPU fallback (`mode: hpa`) is included but is a weaker
signal. Both use a conservative scale-down (`stabilizationWindowSeconds: 300`)
and aggressive scale-up.

**Graceful drain on scale-down / rollout.** Killing a pod drops its sessions,
so shutdown is staged (all timings are configurable and enforced by the chart to
fit within `terminationGracePeriodSeconds`):

1. **SIGTERM → fail readiness** (`/readyz` → 503) so the load balancer stops
   routing new connections. Liveness (`/healthz`) stays green so K8s doesn't
   kill-restart the draining pod.
2. **Wait `ASR_DRAIN_DELAY`** for Service endpoint removal to propagate (this
   replaces a preStop `sleep`, which the distroless image can't run).
3. **Stop the listener, then drain**: each active session is closed with a
   WebSocket **1001 going-away + `going_away` message**, telling clients to
   reconnect (they land on a surviving pod — sessions are non-resumable by
   design). Bounded by `ASR_SHUTDOWN_TIMEOUT`.

Clients therefore only ever need reconnect-with-backoff. Least-connections
ingress balancing (`load-balance: ewma`) steers new connections to fresh pods.

> The **inference server (GPU-bound) is the real capacity limit** — scale it
> independently on GPU utilization/queue depth; it does not scale 1:1 with the
> gateway.

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `ASR_LISTEN_ADDR` | `:8080` | WebSocket + `/metrics` listen address |
| `ASR_INFERENCE_ADDR` | `localhost:50051` | gRPC inference server target |
| `ASR_INFERENCE_POOL_SIZE` | `8` | Pooled gRPC connections to the inference server (round-robined per session) |
| `ASR_MAX_CONNECTIONS` | `2000` | Max concurrent WebSocket connections per pod (0 = unlimited); over the cap, upgrades get HTTP 503 |
| `ASR_SESSION_TIMEOUT` | `30s` | Idle/stalled-stream timeout |
| `ASR_MAX_FRAME_BYTES` | `65536` | Max audio frame size |
| `ASR_WRITE_TIMEOUT` | `5s` | Slow-consumer termination threshold |
| `ASR_DRAIN_DELAY` | `5s` | On SIGTERM, wait this long (after failing readiness) before draining, so endpoint removal propagates |
| `ASR_SHUTDOWN_TIMEOUT` | `45s` | Max time to drain active sessions on SIGTERM |
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
