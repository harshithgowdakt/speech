"""gRPC ASR inference server backed by faster-whisper.

Implements asr.v1 ASRService.StreamingRecognize so the gateway can stream audio
in and receive interim/final transcripts. Serves a gRPC health service for
Kubernetes probes.
"""

from __future__ import annotations

import logging
import os
import signal
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import asr_pb2_grpc
from service import ASRServicer

log = logging.getLogger("asr.server")


class WhisperTranscriber:
    """faster-whisper-backed transcriber. Imports the model lazily so the rest
    of the app (and tests) don't require the heavy dependency."""

    def __init__(self):
        from faster_whisper import WhisperModel

        model = os.getenv("WHISPER_MODEL", "base")
        device = os.getenv("WHISPER_DEVICE", "cuda")
        compute = os.getenv("WHISPER_COMPUTE", "float16")
        log.info("loading whisper model=%s device=%s compute=%s", model, device, compute)
        self._model = WhisperModel(model, device=device, compute_type=compute)
        self._beam = int(os.getenv("WHISPER_BEAM", "1"))
        self._vad = os.getenv("WHISPER_VAD", "true").lower() == "true"

    def transcribe(self, samples, language: str) -> str:
        segments, _ = self._model.transcribe(
            samples, language=language, beam_size=self._beam, vad_filter=self._vad
        )
        return "".join(seg.text for seg in segments).strip()


def serve() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    port = os.getenv("PORT", "50051")
    workers = int(os.getenv("MAX_WORKERS", "16"))
    interim = float(os.getenv("INTERIM_INTERVAL_SEC", "1.0"))

    # Load the model BEFORE starting the server, so readiness (health) only
    # flips to SERVING once we can actually transcribe.
    transcriber = WhisperTranscriber()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    asr_pb2_grpc.add_ASRServiceServicer_to_server(ASRServicer(transcriber, interim), server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_servicer.set("asr.v1.ASRService", health_pb2.HealthCheckResponse.SERVING)
    log.info("ASR inference server listening on :%s (workers=%d)", port, workers)

    def _shutdown(*_):
        log.info("shutting down")
        health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(grace=10)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
