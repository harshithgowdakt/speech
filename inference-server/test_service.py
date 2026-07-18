"""Unit tests for the streaming logic, using a fake transcriber (no model).

Run: python test_service.py   (after generating stubs via gen_proto.sh)
"""

import numpy as np

import asr_pb2
from service import ASRServicer


class FakeTranscriber:
    def __init__(self):
        self.calls = 0

    def transcribe(self, samples: np.ndarray, language: str) -> str:
        self.calls += 1
        assert samples.dtype == np.float32
        assert language == "en"
        return "hello world"


class FakeContext:
    pass


def _config(sample_rate=16000, interim=True):
    return asr_pb2.StreamingRecognizeRequest(
        config=asr_pb2.RecognitionConfig(
            encoding=asr_pb2.AUDIO_ENCODING_LINEAR16,
            sample_rate_hz=sample_rate,
            language_code="en-US",
            interim_results=interim,
        )
    )


def _audio(nbytes):
    return asr_pb2.StreamingRecognizeRequest(audio_content=b"\x01\x00" * (nbytes // 2))


def test_interim_and_final():
    svc = ASRServicer(FakeTranscriber(), interim_interval_sec=0.1)  # 0.1s -> 3200 bytes
    reqs = [_config()] + [_audio(3200) for _ in range(3)]
    out = list(svc.StreamingRecognize(iter(reqs), FakeContext()))
    interims = [r for r in out if r.results and not r.results[0].is_final]
    finals = [r for r in out if r.results and r.results[0].is_final]
    assert len(interims) >= 1, out
    assert len(finals) == 1, out
    assert finals[0].results[0].transcript == "hello world"
    print("test_interim_and_final: OK (%d interim, 1 final)" % len(interims))


def test_config_required():
    svc = ASRServicer(FakeTranscriber())
    out = list(svc.StreamingRecognize(iter([_audio(3200)]), FakeContext()))
    assert out and out[0].error.code == "invalid_config", out
    print("test_config_required: OK")


def test_bad_sample_rate():
    svc = ASRServicer(FakeTranscriber())
    out = list(svc.StreamingRecognize(iter([_config(sample_rate=8000)]), FakeContext()))
    assert out and out[0].error.code == "invalid_config", out
    print("test_bad_sample_rate: OK")


def test_interim_disabled():
    svc = ASRServicer(FakeTranscriber(), interim_interval_sec=0.1)
    reqs = [_config(interim=False)] + [_audio(3200) for _ in range(3)]
    out = list(svc.StreamingRecognize(iter(reqs), FakeContext()))
    interims = [r for r in out if r.results and not r.results[0].is_final]
    finals = [r for r in out if r.results and r.results[0].is_final]
    assert len(interims) == 0, out
    assert len(finals) == 1, out
    print("test_interim_disabled: OK")


if __name__ == "__main__":
    test_interim_and_final()
    test_config_required()
    test_bad_sample_rate()
    test_interim_disabled()
    print("all tests passed")
