// Stream a file's bytes as audio via the JS SDK and print transcripts.
// Usage: node example.mjs <file> [url] [holdMs]
import { readFileSync } from "node:fs";
import { ASRClient } from "./dist/index.js";

const file = process.argv[2];
const url = process.argv[3] || "ws://localhost:8080/v1/stream";
const holdMs = Number(process.argv[4] || 0);
const data = new Uint8Array(readFileSync(file));

const client = new ASRClient({
  url,
  config: {
    encoding: "LINEAR16",
    sample_rate_hz: 16000,
    language_code: "en-US",
    interim_results: true,
  },
  onTranscript: (t) => console.log(`[${t.isFinal ? "FINAL" : "interim"}] ${t.text}`),
  onError: (code, msg) => console.log(`[error] ${code}: ${msg}`),
  onState: (s) => console.log(`[state] ${s}`),
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

await client.start();
for (let i = 0; i < data.length; i += 4) {
  client.sendAudio(data.subarray(i, i + 4));
  await sleep(20);
}
if (holdMs > 0) {
  console.log(`[holding ${holdMs}ms — drain the gateway to see reconnect+replay]`);
  await sleep(holdMs);
} else {
  await sleep(300);
}
await client.stop();
await sleep(100);
process.exit(0);
