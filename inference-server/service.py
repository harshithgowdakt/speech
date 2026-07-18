"""gRPC ASRService implementation (asr.v1) with Whisper pseudo-streaming.

Whisper is not natively streaming: it transcribes windows of audio. This service
accumulates incoming PCM, periodically re-transcribes the utterance buffer to
emit interim hypotheses, and finalizes on end-of-stream. The model is injected
(a `Transcriber`) so the streaming logic is testable without downloading Whisper.
"""

from __future__ import annotations

import logging
from typing import Protocol

import numpy as np

import asr_pb2
import asr_pb2_grpc

log = logging.getLogger("asr.service")


class Transcriber(Protocol):
    """Transcribes mono float32 PCM (16 kHz, range [-1, 1]) to text."""

    def transcribe(self, samples: "np.ndarray", language: str) -> str: ...


def _interim(text: str) -> "asr_pb2.StreamingRecognizeResponse":
    return asr_pb2.StreamingRecognizeResponse(
        results=[asr_pb2.StreamingResult(transcript=text, is_final=False, stability=0.8)]
    )


def _final(text: str) -> "asr_pb2.StreamingRecognizeResponse":
    return asr_pb2.StreamingRecognizeResponse(
        results=[asr_pb2.StreamingResult(transcript=text, is_final=True, confidence=0.9)]
    )


def _error(code: str, message: str) -> "asr_pb2.StreamingRecognizeResponse":
    return asr_pb2.StreamingRecognizeResponse(
        error=asr_pb2.StreamError(code=code, message=message)
    )


class ASRServicer(asr_pb2_grpc.ASRServiceServicer):
    def __init__(self, transcriber: Transcriber, interim_interval_sec: float = 1.0):
        self._transcriber = transcriber
        self._interim_interval_sec = interim_interval_sec

    def StreamingRecognize(self, request_iterator, context):
        cfg = None
        pcm = bytearray()
        interim_bytes = 0
        since_interim = 0

        for req in request_iterator:
            kind = req.WhichOneof("streaming_request")
            if kind == "config":
                cfg = req.config
                if cfg.sample_rate_hz != 16000:
                    yield _error(
                        "invalid_config",
                        f"unsupported sample_rate_hz {cfg.sample_rate_hz}; expected 16000",
                    )
                    return
                # 2 bytes/sample (LINEAR16), mono.
                interim_bytes = int(self._interim_interval_sec * cfg.sample_rate_hz * 2)
                log.info(
                    "stream start: lang=%s interim=%s",
                    cfg.language_code,
                    cfg.interim_results,
                )
                continue

            if cfg is None:
                yield _error("invalid_config", "first message must be config")
                return

            pcm.extend(req.audio_content)
            since_interim += len(req.audio_content)

            if cfg.interim_results and interim_bytes and since_interim >= interim_bytes:
                text = self._transcribe(pcm, cfg)
                if text:
                    yield _interim(text)
                since_interim = 0

        # End of stream: emit the final transcript.
        if cfg is not None:
            text = self._transcribe(pcm, cfg)
            yield _final(text)

    def _transcribe(self, pcm: bytearray, cfg) -> str:
        # LINEAR16 = 2 bytes/sample; trim a trailing partial sample at a chunk
        # boundary so np.frombuffer never sees an odd byte count.
        usable = len(pcm) - (len(pcm) % 2)
        if usable < 2:
            return ""
        samples = np.frombuffer(bytes(pcm[:usable]), dtype=np.int16).astype(np.float32) / 32768.0
        language = (cfg.language_code or "en").split("-")[0]
        try:
            return self._transcriber.transcribe(samples, language)
        except Exception:  # noqa: BLE001 - never kill the stream on a model error
            log.exception("transcription failed")
            return ""
