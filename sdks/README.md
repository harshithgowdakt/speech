# ASR Gateway client SDKs

Client SDKs for the ASR streaming gateway (`asr.v1` WebSocket protocol) in Go,
Python, and TypeScript/JS. All three implement the same production-resilience
model so reconnects are lossless across pod scale-down, rollouts, and network
blips:

- **Auto-reconnect** with exponential backoff + jitter, and **immediate**
  reconnect on a server `going_away` (1001) drain signal.
- **Rolling replay buffer**: audio sent since the last *final* transcript is
  replayed after a reconnect, so a new pod resumes the utterance without losing
  in-flight audio (the Deepgram/AssemblyAI-style resume pattern).
- **Graceful stop**: `stop()` sends end-of-stream and waits for the server to
  deliver final transcript(s) before the connection closes.

| Language | Path | Transport |
|----------|------|-----------|
| Go | [`go/`](go) | `github.com/coder/websocket` |
| Python | [`python/`](python) | `websockets` (asyncio) |
| TS/JS | [`js/`](js) | native `WebSocket` (browser) / `ws` (Node) |

## Protocol

See [`../specs/001-websocket-asr-streaming/contracts/websocket-protocol.md`](../specs/001-websocket-asr-streaming/contracts/websocket-protocol.md).
Briefly: connect to `/v1/stream` (subprotocol `asr.v1`), send a `start` JSON
message with the recognition config, stream binary audio frames, receive
`transcript` JSON messages (`is_final` distinguishes interim vs final), and send
`stop` to finish.

## Quick start

**Go**
```go
c := asr.New(asr.Options{
    URL:    "ws://localhost:8080/v1/stream",
    Config: asr.Config{Encoding: asr.EncodingLINEAR16, SampleRateHz: 16000, LanguageCode: "en-US", InterimResults: true},
    OnTranscript: func(t asr.Transcript) { fmt.Println(t.IsFinal, t.Text) },
})
c.Start(ctx); c.SendAudio(pcm); c.Stop(ctx)
```

**Python**
```python
client = ASRClient(url, Config(), on_transcript=lambda t: print(t.is_final, t.text))
await client.start(); await client.send_audio(pcm); await client.stop()
```

**TypeScript/JS**
```ts
const c = new ASRClient({ url, config, onTranscript: t => console.log(t.isFinal, t.text) });
await c.start(); c.sendAudio(pcm); await c.stop();
```

Each SDK has a runnable `example` that streams a file and prints transcripts; a
`-hold`/`--hold`/`holdMs` flag keeps the session open so you can drain the
gateway and watch reconnect + replay.
