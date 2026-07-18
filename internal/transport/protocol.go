// Package transport is the WebSocket adapter implementing the session.ClientConn
// port per contracts/websocket-protocol.md: binary frames carry audio, text
// (JSON) frames carry control (start/stop) and results (transcript/error).
package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/harshithgowdakt/speech/internal/session"
)

// --- Wire message shapes (JSON text frames) ---

type startMsg struct {
	Type   string     `json:"type"`
	Config configJSON `json:"config"`
}

type configJSON struct {
	Encoding       string `json:"encoding"`
	SampleRateHz   int32  `json:"sample_rate_hz"`
	LanguageCode   string `json:"language_code"`
	InterimResults bool   `json:"interim_results"`
}

type controlMsg struct {
	Type string `json:"type"`
}

type transcriptMsg struct {
	Type       string  `json:"type"`
	Text       string  `json:"text"`
	IsFinal    bool    `json:"is_final"`
	Confidence float32 `json:"confidence,omitempty"`
	Stability  float32 `json:"stability,omitempty"`
}

type errorMsg struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// wsConn adapts a *websocket.Conn to the session.ClientConn port.
type wsConn struct {
	c  *websocket.Conn
	id string
}

func (w *wsConn) ID() string { return w.id }

// ReadStart reads and validates the mandatory first "start" message.
func (w *wsConn) ReadStart(ctx context.Context) (session.RecognitionConfig, error) {
	typ, data, err := w.c.Read(ctx)
	if err != nil {
		return session.RecognitionConfig{}, mapReadErr(err, session.CodeProtocol)
	}
	if typ != websocket.MessageText {
		return session.RecognitionConfig{}, &session.SessionError{
			Code: session.CodeProtocol, Message: "first frame must be a text 'start' message",
		}
	}
	var m startMsg
	if err := json.Unmarshal(data, &m); err != nil {
		return session.RecognitionConfig{}, &session.SessionError{
			Code: session.CodeProtocol, Message: fmt.Sprintf("invalid start JSON: %v", err),
		}
	}
	if m.Type != "start" {
		return session.RecognitionConfig{}, &session.SessionError{
			Code: session.CodeProtocol, Message: fmt.Sprintf("expected type 'start', got %q", m.Type),
		}
	}
	return session.RecognitionConfig{
		Encoding:       m.Config.Encoding,
		SampleRateHz:   m.Config.SampleRateHz,
		LanguageCode:   m.Config.LanguageCode,
		InterimResults: m.Config.InterimResults,
	}, nil
}

// ReadAudio reads the next audio frame; a "stop" control message yields io.EOF.
func (w *wsConn) ReadAudio(ctx context.Context) ([]byte, error) {
	typ, data, err := w.c.Read(ctx)
	if err != nil {
		return nil, mapReadErr(err, session.CodeClientDisconnect)
	}
	switch typ {
	case websocket.MessageBinary:
		return data, nil
	case websocket.MessageText:
		var m controlMsg
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, &session.SessionError{
				Code: session.CodeProtocol, Message: fmt.Sprintf("invalid control JSON: %v", err),
			}
		}
		if m.Type == "stop" {
			return nil, io.EOF
		}
		return nil, &session.SessionError{
			Code: session.CodeProtocol, Message: fmt.Sprintf("unexpected control message %q", m.Type),
		}
	default:
		return nil, &session.SessionError{Code: session.CodeProtocol, Message: "unknown frame type"}
	}
}

// WriteTranscript sends one transcript as a JSON text frame.
func (w *wsConn) WriteTranscript(ctx context.Context, t session.Transcript) error {
	b, err := json.Marshal(transcriptMsg{
		Type: "transcript", Text: t.Text, IsFinal: t.IsFinal,
		Confidence: t.Confidence, Stability: t.Stability,
	})
	if err != nil {
		return err
	}
	return w.c.Write(ctx, websocket.MessageText, b)
}

// Close sends a terminal error (if any) and closes with the mapped close code.
func (w *wsConn) Close(ctx context.Context, serr *session.SessionError) error {
	if serr == nil {
		return w.c.Close(websocket.StatusNormalClosure, "")
	}
	// Best-effort error frame before closing; the client may already be gone.
	if b, err := json.Marshal(errorMsg{Type: "error", Code: serr.Code, Message: serr.Message}); err == nil {
		_ = w.c.Write(ctx, websocket.MessageText, b)
	}
	return w.c.Close(closeCodeFor(serr.Code), truncateReason(serr.Message))
}

func closeCodeFor(code string) websocket.StatusCode {
	switch code {
	case session.CodeInvalidConfig, session.CodeProtocol:
		return websocket.StatusProtocolError // 1002
	case session.CodeInference, session.CodeInternal:
		return websocket.StatusInternalError // 1011
	case session.CodeTimeout:
		return websocket.StatusGoingAway // 1001
	case session.CodeSlowConsumer:
		return websocket.StatusPolicyViolation // 1008
	default:
		return websocket.StatusNormalClosure // 1000
	}
}

// truncateReason keeps the close reason within the WebSocket 123-byte limit.
func truncateReason(s string) string {
	const max = 120
	if len(s) > max {
		return s[:max]
	}
	return s
}

// mapReadErr converts a websocket read error into a session error, treating a
// normal/going-away close as a graceful client end-of-stream (io.EOF).
func mapReadErr(err error, defaultCode string) error {
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return io.EOF
	}
	var se *session.SessionError
	if errors.As(err, &se) {
		return se
	}
	return &session.SessionError{Code: defaultCode, Message: err.Error()}
}
