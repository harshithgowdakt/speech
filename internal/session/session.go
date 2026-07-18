package session

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/harshithgowdakt/speech/internal/metrics"
	"golang.org/x/sync/errgroup"
)

// Options tunes session behavior (mapped from config in main).
type Options struct {
	SessionTimeout time.Duration // idle/stalled-stream timeout (0 disables)
	WriteTimeout   time.Duration // slow-consumer termination threshold
	MaxFrameBytes  int64         // max inbound audio frame size
}

// isEmptyResult lets the inference adapter signal a result-less response to skip
// without importing this package. Set by the inference package via SkipFunc.
type skipFunc func(error) bool

// SkipResult is an optional predicate used by the down-pump to skip result-less
// inference responses. It is wired in main to inference.IsEmptyResult; when nil,
// no responses are skipped.
var SkipResult skipFunc

// Session is one client's live audio-in -> transcript-out interaction.
type Session struct {
	conn      ClientConn
	inference InferenceClient
	opts      Options

	state        SessionState
	cfg          RecognitionConfig
	bytesIn      int64 // atomic
	lastActivity int64 // atomic UnixNano
}

func newSession(conn ClientConn, inf InferenceClient, opts Options) *Session {
	return &Session{conn: conn, inference: inf, opts: opts, state: StateOpening}
}

// Run drives the full session lifecycle and returns the outcome label for
// metrics ("completed", "invalid_config", "inference_error", ...). It always
// releases resources before returning (Constitution Principle III).
func (s *Session) Run(parentCtx context.Context) string {
	log := metrics.Logger(parentCtx)

	sctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Opening: read and validate the start config.
	cfg, err := s.conn.ReadStart(sctx)
	if err != nil {
		return s.finish(parentCtx, classifyRead(err, CodeProtocol))
	}
	if verr := cfg.Validate(); verr != nil {
		return s.finish(parentCtx, newErr(CodeInvalidConfig, "%s", verr.Error()))
	}
	s.cfg = cfg
	s.state = StateStreaming
	log.Info("session started", "encoding", cfg.Encoding, "sample_rate_hz", cfg.SampleRateHz)

	// The errgroup context governs both pumps AND the inference stream, so that
	// the first pump error (or a watchdog timeout) cancels gctx and deterministically
	// tears down the gRPC stream (Constitution Principle III).
	g, gctx := errgroup.WithContext(sctx)

	stream, err := s.inference.StartStream(gctx, cfg)
	if err != nil {
		return s.finish(parentCtx, newErr(CodeInference, "%s", err.Error()))
	}

	s.touch()
	g.Go(func() error { return s.upPump(gctx, stream) })
	g.Go(func() error {
		defer cancel() // when transcripts finish, unblock the up-pump
		return s.downPump(gctx, stream)
	})
	g.Go(func() error { return s.watchdog(gctx) })

	runErr := g.Wait()
	s.state = StateClosed
	return s.finish(parentCtx, asSessionError(runErr))
}

// upPump relays client audio to the inference stream until stop (io.EOF) or error.
func (s *Session) upPump(ctx context.Context, stream InferenceStream) error {
	for {
		audio, err := s.conn.ReadAudio(ctx)
		if err == io.EOF {
			return stream.CloseSend() // graceful client end-of-stream
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil // session ending for another reason
			}
			return classifyRead(err, CodeClientDisconnect)
		}
		if s.opts.MaxFrameBytes > 0 && int64(len(audio)) > s.opts.MaxFrameBytes {
			return newErr(CodeProtocol, "audio frame %d exceeds max %d", len(audio), s.opts.MaxFrameBytes)
		}
		s.touch()
		atomic.AddInt64(&s.bytesIn, int64(len(audio)))
		metrics.AudioBytesIn.Add(float64(len(audio)))
		if err := stream.Send(audio); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return newErr(CodeInference, "send audio: %s", err.Error())
		}
	}
}

// downPump relays inference transcripts to the client until EOF or error.
func (s *Session) downPump(ctx context.Context, stream InferenceStream) error {
	for {
		start := time.Now()
		t, err := stream.Recv()
		if err == io.EOF {
			return nil // server finished normally
		}
		if err != nil {
			if SkipResult != nil && SkipResult(err) {
				continue // result-less response; keep receiving
			}
			if ctx.Err() != nil {
				return nil
			}
			return asSessionError(err)
		}
		s.touch()
		// Honor interim-results opt-out (US2): drop interim frames if not requested.
		if !t.IsFinal && !s.cfg.InterimResults {
			continue
		}
		wctx, wcancel := context.WithTimeout(ctx, s.opts.WriteTimeout)
		werr := s.conn.WriteTranscript(wctx, t)
		wcancel()
		if werr != nil {
			// A write deadline while the session is otherwise healthy => slow consumer.
			if ctx.Err() == nil && errors.Is(werr, context.DeadlineExceeded) {
				return newErr(CodeSlowConsumer, "client did not read within %s", s.opts.WriteTimeout)
			}
			if ctx.Err() != nil {
				return nil
			}
			return newErr(CodeInternal, "write transcript: %s", werr.Error())
		}
		metrics.TranscriptLatency.Observe(time.Since(start).Seconds())
	}
}

// watchdog cancels the session if no audio/transcript activity occurs within the
// configured timeout (stalled inference or idle client).
func (s *Session) watchdog(ctx context.Context) error {
	if s.opts.SessionTimeout <= 0 {
		<-ctx.Done()
		return nil
	}
	tick := s.opts.SessionTimeout / 2
	if tick <= 0 {
		tick = s.opts.SessionTimeout
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			last := time.Unix(0, atomic.LoadInt64(&s.lastActivity))
			if time.Since(last) > s.opts.SessionTimeout {
				return newErr(CodeTimeout, "no activity for %s", s.opts.SessionTimeout)
			}
		}
	}
}

func (s *Session) touch() {
	atomic.StoreInt64(&s.lastActivity, time.Now().UnixNano())
}

// BytesIn reports total audio bytes ingested (for diagnostics/tests).
func (s *Session) BytesIn() int64 { return atomic.LoadInt64(&s.bytesIn) }

// finish sends the terminal signal to the client, records metrics, and returns
// the outcome label. serr == nil means a clean completion.
func (s *Session) finish(parentCtx context.Context, serr *SessionError) string {
	log := metrics.Logger(parentCtx)

	// Use a detached context: parentCtx may already be cancelled at teardown.
	closeCtx, cancel := context.WithTimeout(context.Background(), s.opts.WriteTimeout)
	defer cancel()
	_ = s.conn.Close(closeCtx, serr)

	if serr == nil {
		log.Info("session completed")
		return "completed"
	}
	if serr.Code == CodeInference {
		metrics.InferenceErrors.WithLabelValues(serr.Code).Inc()
	}
	log.Warn("session ended with error", "code", serr.Code, "message", serr.Message)
	return serr.Code
}

// asSessionError coerces any error into a *SessionError (nil stays nil).
func asSessionError(err error) *SessionError {
	if err == nil {
		return nil
	}
	var se *SessionError
	if errors.As(err, &se) {
		return se
	}
	return newErr(CodeInference, "%s", err.Error())
}

// classifyRead maps a client-read error to a session error, preserving an
// existing *SessionError and otherwise using defaultCode.
func classifyRead(err error, defaultCode string) *SessionError {
	var se *SessionError
	if errors.As(err, &se) {
		return se
	}
	return newErr(defaultCode, "%s", err.Error())
}
