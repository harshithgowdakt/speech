# ASR Python SDK

Async Python client for the ASR gateway (`asr.v1`). Requires `websockets`.

```bash
pip install -e sdks/python   # or: pip install websockets and vendor the package
```

```python
import asyncio
from asr_client import ASRClient, Config

async def main():
    c = ASRClient("ws://localhost:8080/v1/stream", Config(),
                  on_transcript=lambda t: print(t.is_final, t.text))
    await c.start()
    await c.send_audio(pcm_chunk)
    await c.stop()  # waits for final transcripts

asyncio.run(main())
```

Auto-reconnect (backoff + immediate on `going_away`) and a replay buffer for
lossless resume are on by default. Run the example:

```bash
python sdks/python/example.py audio.raw
```
