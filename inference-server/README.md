# ASR inference server (faster-whisper)

A gRPC server implementing the shared `asr.v1` contract
([`../proto/asr.proto`](../proto/asr.proto)) with
[faster-whisper](https://github.com/SYSTRAN/faster-whisper) (OpenAI Whisper via
CTranslate2). The gateway streams audio to it and receives interim/final
transcripts — no gateway changes needed.

## Streaming model

Whisper is not natively streaming, so this server does pseudo-streaming:

- Accumulates incoming LINEAR16 PCM (16 kHz mono).
- Every `INTERIM_INTERVAL_SEC` of new audio, re-transcribes the utterance buffer
  and emits an **interim** result (`is_final=false`).
- On end-of-stream, emits the **final** result (`is_final=true`).

For very long sessions this re-transcription grows with buffer length; typical
per-utterance WebSocket sessions are short. VAD-based endpointing (finalize on
silence, then reset the buffer) is a natural enhancement.

## Configuration (env)

| Env | Default | Meaning |
|-----|---------|---------|
| `WHISPER_MODEL` | `base` | `tiny`/`base`/`small`/`medium`/`large-v3` (+`.en`) |
| `WHISPER_DEVICE` | `cuda` | `cuda` or `cpu` |
| `WHISPER_COMPUTE` | `float16` | `float16` (GPU), `int8_float16`, `int8` (CPU) |
| `WHISPER_BEAM` | `1` | beam size (1 = greedy, lowest latency) |
| `WHISPER_VAD` | `true` | Silero VAD filter |
| `INTERIM_INTERVAL_SEC` | `1.0` | interim re-transcription cadence |
| `MAX_WORKERS` | `16` | gRPC thread pool (concurrent streams) |
| `PORT` | `50051` | listen port |

## Local dev

```bash
cd inference-server
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt grpcio-tools
./gen_proto.sh                      # generate asr_pb2.py / asr_pb2_grpc.py
python test_service.py              # unit tests (no model download)
WHISPER_DEVICE=cpu WHISPER_COMPUTE=int8 python server.py
```

## Docker (GPU)

```bash
# Optionally bake the model in for fast cold starts: --build-arg PREFETCH_MODEL=base
docker build -f inference-server/Dockerfile -t ghcr.io/harshithgowdakt/asr-inference:0.1.0 .
docker push ghcr.io/harshithgowdakt/asr-inference:0.1.0
```

> **CUDA/cuDNN compatibility is the sensitive part.** The base image is CUDA 12.2
> + cuDNN 8, matched to `ctranslate2==4.3.1`. If you change either, keep them in
> sync (ctranslate2 4.4+ needs cuDNN 9). Verify on your GPU node type.

## Deploy on EKS

Requires a GPU node group (g4dn/g5) with the **NVIDIA device plugin** (or GPU
Operator) installed, so `nvidia.com/gpu` is schedulable.

```bash
helm upgrade --install asr-inference deploy/helm/asr-inference \
  --namespace asr --create-namespace \
  --set image.tag=0.1.0 \
  --set config.whisperModel=small \
  --set nodeSelector."eks\.amazonaws\.com/nodegroup"=gpu
```

Then point the gateway at it:

```bash
helm upgrade --install asr deploy/helm/asr-gateway -n asr \
  --set config.inferenceAddr=asr-inference.asr.svc.cluster.local:50051
```

The chart injects `nvidia.com/gpu` limits, tolerates the NVIDIA taint, uses gRPC
health probes (generous startupProbe for model load), and mounts an emptyDir HF
cache. GPU autoscaling (`autoscaling.enabled`, DCGM GPU-util metric) is optional;
provisioning GPU nodes is typically handled by Karpenter/Cluster Autoscaler.
