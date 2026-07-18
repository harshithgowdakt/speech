package session

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// SC-004: no goroutine leaks across the whole session test suite.
	goleak.VerifyTestMain(m)
}

// --- Fakes implementing the session ports ---

type fakeConn struct {
	id               string
	frames           [][]byte
	idx              int
	blockAfterFrames bool // if true, block until teardown instead of EOF
	writeHook        func(ctx context.Context, t Transcript) error

	mu      sync.Mutex
	written []Transcript
	closed  *SessionError
	closeOK bool
}

func (f *fakeConn) ID() string { return f.id }

func (f *fakeConn) ReadStart(ctx context.Context) (RecognitionConfig, error) {
	return RecognitionConfig{Encoding: EncodingLINEAR16, SampleRateHz: 16000, LanguageCode: "en-US", InterimResults: true}, nil
}

func (f *fakeConn) ReadAudio(ctx context.Context) ([]byte, error) {
	if f.idx >= len(f.frames) {
		if f.blockAfterFrames {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, io.EOF // graceful client stop
	}
	b := f.frames[f.idx]
	f.idx++
	return b, nil
}

func (f *fakeConn) WriteTranscript(ctx context.Context, t Transcript) error {
	if f.writeHook != nil {
		return f.writeHook(ctx, t)
	}
	f.mu.Lock()
	f.written = append(f.written, t)
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) Close(ctx context.Context, serr *SessionError) error {
	f.mu.Lock()
	f.closed = serr
	f.closeOK = true
	f.mu.Unlock()
	return nil
}

func (f *fakeConn) closeErr() *SessionError {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// fakeInference / fakeStream mimic the mock: echo accumulated audio as interim
// per frame, then a final on CloseSend.
type fakeInference struct {
	behavior string // "echo", "stall", "error"
}

func (f *fakeInference) StartStream(ctx context.Context, cfg RecognitionConfig) (InferenceStream, error) {
	return &fakeStream{out: make(chan Transcript), behavior: f.behavior, ctx: ctx}, nil
}

type fakeStream struct {
	behavior string
	ctx      context.Context
	buf      strings.Builder
	out      chan Transcript
	closed   bool
}

func (s *fakeStream) Send(audio []byte) error {
	if s.behavior == "stall" {
		return nil // receive but never emit
	}
	if s.behavior == "error" {
		return io.ErrUnexpectedEOF
	}
	s.buf.Write(audio)
	select {
	case s.out <- Transcript{Text: s.buf.String(), IsFinal: false, Stability: 0.8}:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	return nil
}

func (s *fakeStream) CloseSend() error {
	if s.behavior != "stall" && !s.closed {
		select {
		case s.out <- Transcript{Text: s.buf.String(), IsFinal: true, Confidence: 0.95}:
		case <-s.ctx.Done():
		}
	}
	if !s.closed {
		s.closed = true
		close(s.out)
	}
	return nil
}

func (s *fakeStream) Recv() (Transcript, error) {
	select {
	case t, ok := <-s.out:
		if !ok {
			return Transcript{}, io.EOF
		}
		return t, nil
	case <-s.ctx.Done():
		return Transcript{}, s.ctx.Err()
	}
}

// --- Tests ---

// T024 / SC-004: repeated sessions complete cleanly with no leaked goroutines
// (goleak in TestMain enforces the leak check).
func TestTeardownNoLeak(t *testing.T) {
	for i := 0; i < 20; i++ {
		conn := &fakeConn{id: "s", frames: [][]byte{[]byte("hello "), []byte("world")}}
		s := newSession(conn, &fakeInference{behavior: "echo"}, Options{WriteTimeout: time.Second})
		outcome := s.Run(context.Background())
		if outcome != "completed" {
			t.Fatalf("iteration %d: outcome = %q, want completed", i, outcome)
		}
	}
}

// T024: abrupt client disconnect mid-stream tears down and reports client_disconnect.
func TestAbruptDisconnect(t *testing.T) {
	conn := &fakeConn{id: "s", frames: [][]byte{[]byte("hi")}}
	// After the first frame, next ReadAudio returns a hard error (not io.EOF).
	conn.frames = [][]byte{[]byte("hi")}
	discErr := &fakeDisconnectConn{fakeConn: conn}
	s := newSession(discErr, &fakeInference{behavior: "echo"}, Options{WriteTimeout: time.Second})
	outcome := s.Run(context.Background())
	if outcome != CodeClientDisconnect {
		t.Fatalf("outcome = %q, want %q", outcome, CodeClientDisconnect)
	}
}

type fakeDisconnectConn struct {
	*fakeConn
	served bool
}

func (f *fakeDisconnectConn) ReadAudio(ctx context.Context) ([]byte, error) {
	if !f.served {
		f.served = true
		return []byte("hi"), nil
	}
	return nil, io.ErrClosedPipe // abrupt disconnect
}

// T025 / FR-009: a client that never reads is terminated as a slow consumer
// within WriteTimeout, with bounded memory.
func TestBackpressureSlowConsumer(t *testing.T) {
	conn := &fakeConn{
		id:     "s",
		frames: [][]byte{[]byte("data")},
		writeHook: func(ctx context.Context, _ Transcript) error {
			<-ctx.Done() // never accept the write
			return ctx.Err()
		},
	}
	s := newSession(conn, &fakeInference{behavior: "echo"},
		Options{WriteTimeout: 100 * time.Millisecond}) // SessionTimeout disabled
	outcome := s.Run(context.Background())
	if outcome != CodeSlowConsumer {
		t.Fatalf("outcome = %q, want %q", outcome, CodeSlowConsumer)
	}
}

// FR + edge case: stalled inference triggers the idle timeout.
func TestSessionTimeout(t *testing.T) {
	conn := &fakeConn{id: "s", blockAfterFrames: true} // no frames: up-pump blocks
	s := newSession(conn, &fakeInference{behavior: "stall"},
		Options{WriteTimeout: time.Second, SessionTimeout: 150 * time.Millisecond})
	start := time.Now()
	outcome := s.Run(context.Background())
	if outcome != CodeTimeout {
		t.Fatalf("outcome = %q, want %q", outcome, CodeTimeout)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("timeout took too long: %s", time.Since(start))
	}
}

// invalid config is rejected before any inference stream opens.
func TestInvalidConfig(t *testing.T) {
	conn := &badStartConn{fakeConn{id: "s"}}
	s := newSession(conn, &fakeInference{behavior: "echo"}, Options{WriteTimeout: time.Second})
	if outcome := s.Run(context.Background()); outcome != CodeInvalidConfig {
		t.Fatalf("outcome = %q, want %q", outcome, CodeInvalidConfig)
	}
}

type badStartConn struct{ fakeConn }

func (b *badStartConn) ReadStart(ctx context.Context) (RecognitionConfig, error) {
	return RecognitionConfig{Encoding: "BOGUS", SampleRateHz: 0}, nil
}
