# ASR TypeScript/JS SDK

Resilient client for the ASR gateway (`asr.v1`). Runs in the browser (native
`WebSocket`) and Node (via `ws`).

```bash
cd sdks/js && npm install && npm run build
```

```ts
import { ASRClient } from "@asr/client";

const c = new ASRClient({
  url: "ws://localhost:8080/v1/stream",
  config: { encoding: "LINEAR16", sample_rate_hz: 16000, language_code: "en-US", interim_results: true },
  onTranscript: (t) => console.log(t.isFinal, t.text),
});
await c.start();
c.sendAudio(pcmChunk);        // Uint8Array | ArrayBuffer
await c.stop();               // waits for final transcripts
```

Auto-reconnect (backoff + immediate on `going_away`) and a replay buffer for
lossless resume are on by default. Run the example (Node 21+ has a global
WebSocket; older Node uses the optional `ws` dependency):

```bash
node sdks/js/example.mjs audio.raw
```
