# GPU image for the faster-whisper ASR inference server.
#
# IMPORTANT (CUDA/cuDNN compatibility): ctranslate2 (used by faster-whisper) is
# built against a specific cuDNN major version. This image uses CUDA 12.2 +
# cuDNN 8, which matches ctranslate2==4.3.1 (pinned in requirements.txt). If you
# bump ctranslate2 to 4.4+ (cuDNN 9), switch the base image tag to a cudnn9 one.
FROM nvidia/cuda:12.2.2-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/models

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY inference-server/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt "grpcio-tools==1.68.1"

# Generate the gRPC stubs from the shared contract at build time.
COPY proto/asr.proto /proto/asr.proto
COPY inference-server/ /app/
RUN python3 -m grpc_tools.protoc -I /proto --python_out=. --grpc_python_out=. /proto/asr.proto

# Optionally pre-bake the model into the image for fast cold starts:
#   docker build --build-arg PREFETCH_MODEL=base ...
ARG PREFETCH_MODEL=""
RUN if [ -n "$PREFETCH_MODEL" ]; then \
      python3 -c "from faster_whisper import WhisperModel; WhisperModel('$PREFETCH_MODEL', device='cpu', compute_type='int8')"; \
    fi

ENV WHISPER_MODEL=base \
    WHISPER_DEVICE=cuda \
    WHISPER_COMPUTE=float16 \
    PORT=50051 \
    MAX_WORKERS=16 \
    INTERIM_INTERVAL_SEC=1.0

EXPOSE 50051
CMD ["python3", "server.py"]
