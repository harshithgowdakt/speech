/**
 * TypeScript/JS client SDK for the ASR streaming gateway (asr.v1 protocol).
 *
 * Streams audio over one WebSocket and delivers interim/final transcripts, with
 * production resilience: automatic reconnect (exponential backoff, immediate on
 * a server "going away" drain) and a rolling replay buffer so a reconnect
 * resumes without losing in-flight audio.
 *
 * Works in the browser (native WebSocket) and in Node (via the `ws` package).
 */

export const Encoding = {
  LINEAR16: "LINEAR16",
  FLAC: "FLAC",
  OGG_OPUS: "OGG_OPUS",
  MULAW: "MULAW",
} as const;

export interface Config {
  encoding: string;
  sample_rate_hz: number;
  language_code: string;
  interim_results: boolean;
}

export interface Transcript {
  text: string;
  isFinal: boolean;
  confidence: number;
  stability: number;
}

export type State = "disconnected" | "connected" | "reconnecting" | "closed";

export interface Options {
  url: string;
  config: Config;
  onTranscript?: (t: Transcript) => void;
  onError?: (code: string, message: string) => void;
  onState?: (s: State) => void;
  /** Base reconnect delay in ms (default 1000). */
  reconnectBaseMs?: number;
  /** Max reconnect delay in ms (default 32000). */
  reconnectMaxMs?: number;
  /** Max reconnect attempts; 0 = unlimited (default 0). */
  maxAttempts?: number;
  /** Keep a replay buffer for lossless resume (default true). */
  replay?: boolean;
  /** Auto-reconnect on unexpected close (default true). */
  reconnect?: boolean;
  /** Explicit WebSocket implementation (defaults to global, then `ws`). */
  webSocketImpl?: unknown;
}

type WSCtor = new (url: string, protocols?: string | string[]) => WebSocketLike;

interface WebSocketLike {
  binaryType: string;
  send(data: string | ArrayBufferView | ArrayBufferLike): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: { code: number; reason: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

const GOING_AWAY = 1001;

export class ASRClient {
  private ws: WebSocketLike | null = null;
  private buffer: Uint8Array[] = [];
  private stopped = false;
  private closed = false;
  private state: State = "disconnected";
  private attempt = 0;
  private ctor: WSCtor | null = null;
  private closeWaiters: Array<() => void> = [];

  constructor(private readonly opts: Options) {}

  /** Connect and start the background read/reconnect loop. */
  async start(): Promise<void> {
    if (this.closed) throw new Error("client closed");
    this.ctor = await resolveWebSocket(this.opts.webSocketImpl);
    await this.connect();
  }

  /** Queue an audio frame (buffered for replay; sent if connected). */
  sendAudio(data: Uint8Array | ArrayBuffer): void {
    if (this.closed || this.stopped) throw new Error("client closed");
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    if (this.opts.replay !== false) this.buffer.push(bytes.slice());
    if (this.ws) this.ws.send(bytes);
  }

  /**
   * Send end-of-stream and resolve once the server has delivered any final
   * transcripts and closed the connection. Disables reconnection.
   */
  stop(): Promise<void> {
    this.stopped = true;
    if (!this.ws) return Promise.resolve();
    // Send end-of-stream but do NOT close: the server sends the final
    // transcript(s), then closes the connection itself.
    try {
      this.ws.send(JSON.stringify({ type: "stop" }));
    } catch {
      return Promise.resolve();
    }
    return new Promise((resolve) => this.closeWaiters.push(resolve));
  }

  /** Tear down immediately without a graceful stop. */
  close(): void {
    this.closed = true;
    this.stopped = true;
    if (this.ws) {
      try {
        this.ws.close(1000, "");
      } catch {
        /* ignore */
      }
    }
    this.setState("closed");
  }

  // --- internals ---

  private connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const Ctor = this.ctor!;
      const ws = new Ctor(this.opts.url, ["asr.v1"]);
      ws.binaryType = "arraybuffer";
      let opened = false;

      ws.onopen = () => {
        opened = true;
        this.ws = ws;
        ws.send(JSON.stringify({ type: "start", config: this.opts.config }));
        if (this.opts.replay !== false) {
          for (const chunk of this.buffer) ws.send(chunk);
        }
        this.setState("connected");
        resolve();
      };

      ws.onmessage = (ev) => this.onMessage(ev.data);

      ws.onerror = () => {
        if (!opened) reject(new Error("asr: connection failed"));
      };

      ws.onclose = (ev) => {
        this.ws = null;
        if (!opened) {
          reject(new Error(`asr: closed before open (code ${ev.code})`));
          return;
        }
        this.scheduleReconnect(ev.code);
      };
    });
  }

  private onMessage(data: unknown): void {
    if (typeof data !== "string") return; // audio path is upstream only
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }
    if (msg.type === "transcript") {
      if (msg.is_final && this.opts.replay !== false) this.buffer = [];
      this.opts.onTranscript?.({
        text: String(msg.text ?? ""),
        isFinal: Boolean(msg.is_final),
        confidence: Number(msg.confidence ?? 0),
        stability: Number(msg.stability ?? 0),
      });
    } else if (msg.type === "error") {
      this.opts.onError?.(String(msg.code ?? ""), String(msg.message ?? ""));
    }
  }

  private scheduleReconnect(closeCode: number): void {
    if (this.stopped || this.closed) {
      this.setState("closed");
      return;
    }
    if (this.opts.reconnect === false) {
      this.setState("disconnected");
      return;
    }
    this.setState("reconnecting");

    // "Going away" (drain/rollout) => reconnect immediately, no backoff.
    let delay = 0;
    if (closeCode !== GOING_AWAY) {
      this.attempt += 1;
      const max = this.opts.maxAttempts ?? 0;
      if (max > 0 && this.attempt > max) {
        this.setState("closed");
        return;
      }
      delay = backoff(
        this.attempt,
        this.opts.reconnectBaseMs ?? 1000,
        this.opts.reconnectMaxMs ?? 32000,
      );
    }

    setTimeout(() => {
      this.connect()
        .then(() => {
          this.attempt = 0;
        })
        .catch(() => this.scheduleReconnect(0)); // failed reconnect -> retry
    }, delay);
  }

  private setState(s: State): void {
    if (s !== this.state) {
      this.state = s;
      this.opts.onState?.(s);
      if (s === "closed") {
        const waiters = this.closeWaiters;
        this.closeWaiters = [];
        for (const w of waiters) w();
      }
    }
  }
}

function backoff(attempt: number, base: number, cap: number): number {
  const d = Math.min(cap, base * 2 ** (attempt - 1));
  return Math.random() * d; // full jitter
}

async function resolveWebSocket(impl?: unknown): Promise<WSCtor> {
  if (impl) return impl as WSCtor;
  const g = globalThis as { WebSocket?: unknown };
  if (typeof g.WebSocket !== "undefined") return g.WebSocket as WSCtor;
  // Node fallback.
  const mod = (await import("ws")) as unknown as { default: WSCtor };
  return mod.default;
}
