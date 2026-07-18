// Package session defines the domain types and ports (interfaces) for an ASR
// streaming session, and orchestrates the audio-in -> transcript-out lifecycle.
//
// It is the hexagonal "core": it declares the ClientConn and InferenceClient
// ports that the transport and inference adapters implement, so orchestration
// depends only on abstractions (Constitution Principle II).
package session

import (
	"context"
	"fmt"
)

// SessionState is the lifecycle state of a session.
type SessionState int

const (
	StateOpening SessionState = iota // connection accepted, awaiting start config
	StateStreaming                   // config accepted, audio/transcripts flowing
	StateEnding                      // draining after client stop or error
	StateClosed                      // torn down, resources released
)

func (s SessionState) String() string {
	switch s {
	case StateOpening:
		return "opening"
	case StateStreaming:
		return "streaming"
	case StateEnding:
		return "ending"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Audio encoding identifiers accepted by the recognizer (mirror the .proto enum).
const (
	EncodingLINEAR16 = "LINEAR16"
	EncodingFLAC     = "FLAC"
	EncodingOGGOPUS  = "OGG_OPUS"
	EncodingMULAW    = "MULAW"
)

// RecognitionConfig is the session-start configuration sent once by the client,
// before any audio, and forwarded to the inference server.
type RecognitionConfig struct {
	Encoding       string
	SampleRateHz   int32
	LanguageCode   string
	InterimResults bool
}

// Validate enforces the config rules from the WebSocket protocol contract.
func (c RecognitionConfig) Validate() error {
	switch c.Encoding {
	case EncodingLINEAR16, EncodingFLAC, EncodingOGGOPUS, EncodingMULAW:
	default:
		return fmt.Errorf("unsupported encoding %q", c.Encoding)
	}
	if c.SampleRateHz <= 0 {
		return fmt.Errorf("sample_rate_hz must be > 0, got %d", c.SampleRateHz)
	}
	if c.LanguageCode == "" {
		return fmt.Errorf("language_code must not be empty")
	}
	return nil
}

// Transcript is a recognition result returned to the client.
type Transcript struct {
	Text       string
	IsFinal    bool
	Confidence float32
	Stability  float32
}

// Error codes surfaced to the client and to metrics/logs.
const (
	CodeInvalidConfig    = "invalid_config"
	CodeProtocol         = "protocol_violation"
	CodeInference        = "inference_error"
	CodeTimeout          = "timeout"
	CodeSlowConsumer     = "slow_consumer"
	CodeClientDisconnect = "client_disconnect"
	CodeGoingAway        = "going_away"
	CodeInternal         = "internal"
)

// SessionError is a structured, terminal session failure.
type SessionError struct {
	Code    string
	Message string
}

func (e *SessionError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newErr(code, format string, args ...any) *SessionError {
	return &SessionError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ClientConn is the port to a connected client (implemented by the transport
// adapter). All methods honor context cancellation for deterministic teardown.
type ClientConn interface {
	// ReadStart reads the mandatory first "start" message and returns its config.
	ReadStart(ctx context.Context) (RecognitionConfig, error)
	// ReadAudio reads the next audio frame. It returns io.EOF when the client
	// sends an explicit "stop" (graceful end-of-stream).
	ReadAudio(ctx context.Context) ([]byte, error)
	// WriteTranscript delivers one transcript to the client.
	WriteTranscript(ctx context.Context, t Transcript) error
	// Close sends a terminal error (if serr != nil) and closes the connection
	// with the appropriate close code.
	Close(ctx context.Context, serr *SessionError) error
	// ID returns the per-session correlation ID.
	ID() string
}

// InferenceClient is the port to the ASR inference server (implemented by the
// gRPC adapter).
type InferenceClient interface {
	// StartStream opens a bidirectional inference stream, sending cfg first.
	StartStream(ctx context.Context, cfg RecognitionConfig) (InferenceStream, error)
}

// InferenceStream is one open bidirectional inference stream.
type InferenceStream interface {
	// Send forwards one audio frame upstream.
	Send(audio []byte) error
	// CloseSend half-closes the send direction (client finished sending audio).
	CloseSend() error
	// Recv returns the next transcript, or io.EOF when the server is done.
	Recv() (Transcript, error)
}
