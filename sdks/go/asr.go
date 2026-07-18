// Package asr is a Go client SDK for the ASR streaming gateway (asr.v1 protocol).
//
// It streams audio over a single WebSocket and delivers interim/final
// transcripts, with production resilience: automatic reconnect (exponential
// backoff, immediate on a server "going away" drain) and a rolling replay
// buffer so a reconnect resumes without losing in-flight audio.
package asr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Encoding identifiers accepted by the recognizer.
const (
	EncodingLINEAR16 = "LINEAR16"
	EncodingFLAC     = "FLAC"
	EncodingOGGOPUS  = "OGG_OPUS"
	EncodingMULAW    = "MULAW"
)

// Config is the recognition configuration sent when a session starts.
type Config struct {
	Encoding       string `json:"encoding"`
	SampleRateHz   int32  `json:"sample_rate_hz"`
	LanguageCode   string `json:"language_code"`
	InterimResults bool   `json:"interim_results"`
}

// Transcript is a recognition result from the server.
type Transcript struct {
	Text       string  `json:"text"`
	IsFinal    bool    `json:"is_final"`
	Confidence float32 `json:"confidence"`
	Stability  float32 `json:"stability"`
}

// State is the client connection state.
type State int

const (
	StateDisconnected State = iota
	StateConnected
	StateReconnecting
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Options configures a Client.
type Options struct {
	URL    string // e.g. ws://host:8080/v1/stream
	Config Config

	// Callbacks (invoked from the client's read goroutine; keep them quick).
	OnTranscript  func(Transcript)
	OnError       func(code, message string) // server error frame
	OnStateChange func(State)

	// Reconnect tuning. Zero values apply sensible defaults.
	ReconnectBaseDelay   time.Duration // default 1s
	ReconnectMaxDelay    time.Duration // default 32s
	MaxReconnectAttempts int           // 0 = unlimited
	DisableReplay        bool          // disable the resume replay buffer
	DisableReconnect     bool          // close permanently on disconnect
}

// ErrClosed is returned once the client has been stopped/closed.
var ErrClosed = errors.New("asr: client closed")

// Client is a resilient ASR streaming client. Safe for concurrent use.
type Client struct {
	opts Options

	mu       sync.Mutex
	conn     *websocket.Conn
	buffer   [][]byte // audio since last final (replay buffer)
	closed   bool
	stopped  bool // Stop() called: no more reconnects
	state    State
	writeCtx context.Context
	done     chan struct{} // closed when the run loop exits
}

// New creates a client. Call Start to connect.
func New(opts Options) *Client {
	if opts.ReconnectBaseDelay <= 0 {
		opts.ReconnectBaseDelay = time.Second
	}
	if opts.ReconnectMaxDelay <= 0 {
		opts.ReconnectMaxDelay = 32 * time.Second
	}
	return &Client{opts: opts, state: StateDisconnected}
}

// Start connects and runs the session loop in the background until ctx is
// cancelled, Stop is called, or reconnection is exhausted.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.writeCtx = ctx
	c.done = make(chan struct{})
	c.mu.Unlock()

	// First connect is synchronous so callers see immediate connect errors.
	if err := c.connect(ctx); err != nil {
		return err
	}
	go c.run(ctx)
	return nil
}

// SendAudio queues an audio frame. It is buffered for replay and written to the
// current connection if one is up (otherwise it is replayed on reconnect).
func (c *Client) SendAudio(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.stopped {
		return ErrClosed
	}
	buf := append([]byte(nil), b...) // defensive copy
	if !c.opts.DisableReplay {
		c.buffer = append(c.buffer, buf)
	}
	if c.conn != nil {
		return c.conn.Write(c.writeCtx, websocket.MessageBinary, buf)
	}
	return nil // will be replayed on reconnect
}

// Stop sends end-of-stream and waits for the server to deliver any final
// transcripts and close the connection (up to ctx's deadline). It disables
// reconnection. If ctx expires first, the connection is force-closed.
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	conn := c.conn
	done := c.done
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	// Send end-of-stream but do NOT close yet: the server sends final
	// transcript(s) and then closes the connection itself.
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"stop"}`)); err != nil {
		return conn.Close(websocket.StatusNormalClosure, "")
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Server didn't close in time; force it.
		return conn.Close(websocket.StatusNormalClosure, "")
	}
}

// Close tears down the client immediately without a graceful stop.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.stopped = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	c.setState(StateClosed)
	if conn != nil {
		return conn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// --- internals ---

type startMsg struct {
	Type   string `json:"type"`
	Config Config `json:"config"`
}

type serverMsg struct {
	Type       string  `json:"type"`
	Text       string  `json:"text"`
	IsFinal    bool    `json:"is_final"`
	Confidence float32 `json:"confidence"`
	Stability  float32 `json:"stability"`
	Code       string  `json:"code"`
	Message    string  `json:"message"`
}

// connect dials, sends the start config, and replays the buffer. On success the
// connection is stored and ready for reads/writes.
func (c *Client) connect(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, c.opts.URL, &websocket.DialOptions{
		Subprotocols: []string{"asr.v1"},
	})
	if err != nil {
		return fmt.Errorf("asr: dial: %w", err)
	}

	start, _ := json.Marshal(startMsg{Type: "start", Config: c.opts.Config})
	if err := conn.Write(ctx, websocket.MessageText, start); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "start failed")
		return fmt.Errorf("asr: send start: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	replay := append([][]byte(nil), c.buffer...)
	c.mu.Unlock()

	// Resume: replay audio that was not yet finalized.
	for _, chunk := range replay {
		if err := conn.Write(ctx, websocket.MessageBinary, chunk); err != nil {
			_ = conn.Close(websocket.StatusInternalError, "replay failed")
			c.mu.Lock()
			c.conn = nil
			c.mu.Unlock()
			return fmt.Errorf("asr: replay: %w", err)
		}
	}

	c.setState(StateConnected)
	return nil
}

// run reads from the current connection and reconnects on unexpected close.
func (c *Client) run(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		done := c.done
		c.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	attempt := 0
	for {
		err := c.readLoop(ctx)

		c.mu.Lock()
		stopped := c.stopped || c.closed
		c.conn = nil
		c.mu.Unlock()

		if stopped || ctx.Err() != nil {
			c.setState(StateClosed)
			return
		}
		if c.opts.DisableReconnect {
			c.setState(StateDisconnected)
			return
		}

		// A server "going away" (drain/rollout) means: reconnect NOW, no backoff.
		goingAway := websocket.CloseStatus(err) == websocket.StatusGoingAway

		c.setState(StateReconnecting)
		var delay time.Duration
		if !goingAway {
			attempt++
			if c.opts.MaxReconnectAttempts > 0 && attempt > c.opts.MaxReconnectAttempts {
				c.setState(StateClosed)
				return
			}
			delay = backoff(attempt, c.opts.ReconnectBaseDelay, c.opts.ReconnectMaxDelay)
		}

		select {
		case <-ctx.Done():
			c.setState(StateClosed)
			return
		case <-time.After(delay):
		}

		if err := c.connect(ctx); err != nil {
			continue // treat a failed reconnect as another attempt
		}
		attempt = 0 // reset on success
	}
}

func (c *Client) readLoop(ctx context.Context) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errors.New("asr: no connection")
	}

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageText {
			continue
		}
		var m serverMsg
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		switch m.Type {
		case "transcript":
			if m.IsFinal && !c.opts.DisableReplay {
				// Audio up to this final is finalized; drop it from the buffer.
				c.mu.Lock()
				c.buffer = c.buffer[:0]
				c.mu.Unlock()
			}
			if c.opts.OnTranscript != nil {
				c.opts.OnTranscript(Transcript{
					Text: m.Text, IsFinal: m.IsFinal,
					Confidence: m.Confidence, Stability: m.Stability,
				})
			}
		case "error":
			if c.opts.OnError != nil {
				c.opts.OnError(m.Code, m.Message)
			}
		}
	}
}

func (c *Client) setState(s State) {
	c.mu.Lock()
	changed := c.state != s
	c.state = s
	cb := c.opts.OnStateChange
	c.mu.Unlock()
	if changed && cb != nil {
		cb(s)
	}
}

// backoff returns exponential backoff capped at maxDelay (no external rand
// dependency; deterministic-with-jitter via attempt).
func backoff(attempt int, base, max time.Duration) time.Duration {
	d := base << (attempt - 1)
	if d <= 0 || d > max {
		d = max
	}
	// +/- 20% jitter derived from attempt to avoid thundering herds.
	jitter := d / 5
	if attempt%2 == 0 {
		return d - jitter
	}
	return d + jitter
}
