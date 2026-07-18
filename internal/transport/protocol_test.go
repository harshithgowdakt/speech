package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/harshithgowdakt/speech/internal/session"
)

// serve starts an httptest server whose handler hands each accepted connection
// to fn (running server-side), and returns a ws:// URL plus cleanup.
func serve(t *testing.T, fn ConnHandler) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(Handler(Options{MaxFrameBytes: 1 << 16}, fn))
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	return url, srv.Close
}

func dial(t *testing.T, ctx context.Context, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"asr.v1"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

// T013: contract test for the WebSocket protocol — start -> binary audio -> stop
// on the client side maps to ReadStart/ReadAudio(EOF)/WriteTranscript server-side.
func TestProtocolRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		cfg    session.RecognitionConfig
		audio  [][]byte
		gotEOF bool
		err    error
	}
	done := make(chan result, 1)

	url, closeSrv := serve(t, func(ctx context.Context, conn session.ClientConn) {
		var r result
		r.cfg, r.err = conn.ReadStart(ctx)
		if r.err != nil {
			done <- r
			return
		}
		for {
			b, err := conn.ReadAudio(ctx)
			if err == io.EOF {
				r.gotEOF = true
				break
			}
			if err != nil {
				r.err = err
				done <- r
				return
			}
			r.audio = append(r.audio, b)
		}
		// Echo a final transcript back to the client.
		_ = conn.WriteTranscript(ctx, session.Transcript{Text: "ok", IsFinal: true, Confidence: 0.9})
		_ = conn.Close(ctx, nil)
		done <- r
	})
	defer closeSrv()

	c := dial(t, ctx, url)

	start, _ := json.Marshal(map[string]any{
		"type": "start",
		"config": map[string]any{
			"encoding": "LINEAR16", "sample_rate_hz": 16000,
			"language_code": "en-US", "interim_results": true,
		},
	})
	if err := c.Write(ctx, websocket.MessageText, start); err != nil {
		t.Fatalf("write start: %v", err)
	}
	for _, chunk := range []string{"foo", "bar"} {
		if err := c.Write(ctx, websocket.MessageBinary, []byte(chunk)); err != nil {
			t.Fatalf("write audio: %v", err)
		}
	}
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"stop"}`)); err != nil {
		t.Fatalf("write stop: %v", err)
	}

	// Read the transcript reply.
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read transcript: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("transcript frame type = %v, want text", typ)
	}
	var tm transcriptMsg
	if err := json.Unmarshal(data, &tm); err != nil {
		t.Fatalf("unmarshal transcript: %v", err)
	}
	if tm.Type != "transcript" || tm.Text != "ok" || !tm.IsFinal {
		t.Errorf("transcript = %+v, want {transcript ok true}", tm)
	}
	c.Close(websocket.StatusNormalClosure, "")

	r := <-done
	if r.err != nil {
		t.Fatalf("server error: %v", r.err)
	}
	if r.cfg.Encoding != "LINEAR16" || r.cfg.SampleRateHz != 16000 || r.cfg.LanguageCode != "en-US" {
		t.Errorf("config = %+v", r.cfg)
	}
	if !r.gotEOF {
		t.Error("server did not observe stop (io.EOF)")
	}
	if len(r.audio) != 2 || string(r.audio[0]) != "foo" || string(r.audio[1]) != "bar" {
		t.Errorf("audio = %v, want [foo bar]", r.audio)
	}
}

// T013: sending audio before the start message is a protocol violation.
func TestProtocolAudioBeforeStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errc := make(chan error, 1)
	url, closeSrv := serve(t, func(ctx context.Context, conn session.ClientConn) {
		_, err := conn.ReadStart(ctx)
		errc <- err
	})
	defer closeSrv()

	c := dial(t, ctx, url)
	if err := c.Write(ctx, websocket.MessageBinary, []byte("audio")); err != nil {
		t.Fatalf("write: %v", err)
	}

	err := <-errc
	var se *session.SessionError
	if !errors.As(err, &se) || se.Code != session.CodeProtocol {
		t.Fatalf("err = %v, want protocol_violation SessionError", err)
	}
}
