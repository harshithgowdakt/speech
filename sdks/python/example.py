"""Stream a file's bytes as audio via the Python SDK and print transcripts.

Usage: python example.py <file> [--url ws://localhost:8080/v1/stream] [--hold SECS]
"""

import argparse
import asyncio
import sys

from asr_client import ASRClient, Config


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--url", default="ws://localhost:8080/v1/stream")
    ap.add_argument("--hold", type=float, default=0.0)
    args = ap.parse_args()

    with open(args.file, "rb") as f:
        data = f.read()

    def on_transcript(t):
        tag = "FINAL" if t.is_final else "interim"
        print(f"[{tag}] {t.text}", flush=True)

    client = ASRClient(
        args.url,
        Config(),
        on_transcript=on_transcript,
        on_error=lambda code, msg: print(f"[error] {code}: {msg}", flush=True),
        on_state=lambda s: print(f"[state] {s.value}", flush=True),
    )

    await client.start()
    for i in range(0, len(data), 4):
        await client.send_audio(data[i : i + 4])
        await asyncio.sleep(0.02)

    if args.hold > 0:
        print(f"[holding {args.hold}s — drain the gateway to see reconnect+replay]", flush=True)
        await asyncio.sleep(args.hold)
    else:
        await asyncio.sleep(0.3)
    await client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
