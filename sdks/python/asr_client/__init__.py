"""Async Python client SDK for the ASR streaming gateway (asr.v1 protocol).

Streams audio over one WebSocket and yields interim/final transcripts, with
production resilience: automatic reconnect (exponential backoff, immediate on a
server "going away" drain) and a rolling replay buffer so a reconnect resumes
without losing in-flight audio.
"""

from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Awaitable, Callable, List, Optional

import websockets

__all__ = [
    "Config",
    "Transcript",
    "State",
    "ASRClient",
    "Encoding",
]


class Encoding:
    LINEAR16 = "LINEAR16"
    FLAC = "FLAC"
    OGG_OPUS = "OGG_OPUS"
    MULAW = "MULAW"


@dataclass
class Config:
    encoding: str = Encoding.LINEAR16
    sample_rate_hz: int = 16000
    language_code: str = "en-US"
    interim_results: bool = True


@dataclass
class Transcript:
    text: str
    is_final: bool
    confidence: float = 0.0
    stability: float = 0.0


class State(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


TranscriptCb = Callable[[Transcript], None]
ErrorCb = Callable[[str, str], None]
StateCb = Callable[[State], None]

_GOING_AWAY = 1001


class ASRClient:
    """Resilient ASR streaming client.

    Typical use::

        client = ASRClient(url, Config(), on_transcript=print)
        await client.start()
        await client.send_audio(pcm_chunk)
        ...
        await client.stop()
    """

    def __init__(
        self,
        url: str,
        config: Config,
        *,
        on_transcript: Optional[TranscriptCb] = None,
        on_error: Optional[ErrorCb] = None,
        on_state: Optional[StateCb] = None,
        reconnect_base: float = 1.0,
        reconnect_max: float = 32.0,
        max_attempts: int = 0,
        replay: bool = True,
        reconnect: bool = True,
    ) -> None:
        self.url = url
        self.config = config
        self._on_transcript = on_transcript
        self._on_error = on_error
        self._on_state = on_state
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._max_attempts = max_attempts
        self._replay = replay
        self._reconnect = reconnect

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._buffer: List[bytes] = []
        self._write_lock = asyncio.Lock()
        self._stopped = False
        self._closed = False
        self._state = State.DISCONNECTED
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Connect and start the background read/reconnect loop."""
        if self._closed:
            raise RuntimeError("client closed")
        await self._connect()  # surface the first connect error to the caller
        self._task = asyncio.ensure_future(self._run())

    async def send_audio(self, data: bytes) -> None:
        """Queue an audio frame (buffered for replay; sent if connected)."""
        if self._closed or self._stopped:
            raise RuntimeError("client closed")
        if self._replay:
            self._buffer.append(bytes(data))
        ws = self._ws
        if ws is not None:
            async with self._write_lock:
                await ws.send(data)

    async def stop(self) -> None:
        """Send end-of-stream and wait for the server to deliver final
        transcripts and close the connection. Disables reconnection."""
        self._stopped = True
        ws = self._ws
        if ws is not None:
            try:
                async with self._write_lock:
                    # Send end-of-stream but do NOT close: the server sends the
                    # final transcript(s), then closes the connection itself.
                    await ws.send(json.dumps({"type": "stop"}))
            except websockets.ConnectionClosed:
                pass
        await self._join()

    async def close(self) -> None:
        """Tear down immediately without a graceful stop."""
        self._closed = True
        self._stopped = True
        ws = self._ws
        if ws is not None:
            try:
                await ws.close(code=1000)
            except websockets.ConnectionClosed:
                pass
        await self._join()
        self._set_state(State.CLOSED)

    # --- internals ---

    async def _join(self) -> None:
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=self._reconnect_max + 5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()

    async def _connect(self) -> None:
        ws = await websockets.connect(self.url, subprotocols=["asr.v1"])
        # Send start config.
        await ws.send(json.dumps({"type": "start", "config": asdict(self.config)}))
        self._ws = ws
        # Resume: replay audio not yet finalized.
        replay = list(self._buffer)
        async with self._write_lock:
            for chunk in replay:
                await ws.send(chunk)
        self._set_state(State.CONNECTED)

    async def _run(self) -> None:
        attempt = 0
        while True:
            close_code = await self._read_loop()
            self._ws = None

            if self._stopped or self._closed:
                self._set_state(State.CLOSED)
                return
            if not self._reconnect:
                self._set_state(State.DISCONNECTED)
                return

            self._set_state(State.RECONNECTING)
            going_away = close_code == _GOING_AWAY
            delay = 0.0
            if not going_away:
                attempt += 1
                if self._max_attempts and attempt > self._max_attempts:
                    self._set_state(State.CLOSED)
                    return
                delay = _backoff(attempt, self._reconnect_base, self._reconnect_max)

            await asyncio.sleep(delay)
            try:
                await self._connect()
                attempt = 0
            except Exception:
                continue  # count as another failed attempt

    async def _read_loop(self) -> Optional[int]:
        ws = self._ws
        if ws is None:
            return None
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                mtype = msg.get("type")
                if mtype == "transcript":
                    if msg.get("is_final") and self._replay:
                        self._buffer.clear()
                    if self._on_transcript:
                        self._on_transcript(
                            Transcript(
                                text=msg.get("text", ""),
                                is_final=bool(msg.get("is_final")),
                                confidence=float(msg.get("confidence", 0.0)),
                                stability=float(msg.get("stability", 0.0)),
                            )
                        )
                elif mtype == "error":
                    if self._on_error:
                        self._on_error(msg.get("code", ""), msg.get("message", ""))
        except websockets.ConnectionClosed as exc:
            return exc.code
        return None

    def _set_state(self, state: State) -> None:
        if state != self._state:
            self._state = state
            if self._on_state:
                self._on_state(state)


def _backoff(attempt: int, base: float, cap: float) -> float:
    delay = min(cap, base * (2 ** (attempt - 1)))
    # full jitter
    return random.uniform(0, delay)
