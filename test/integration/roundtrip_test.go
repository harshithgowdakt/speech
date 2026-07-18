// Package integration exercises the full audio-in -> transcript-out path across
// the WebSocket transport, session orchestration, and gRPC inference client
// against the mock inference server (Constitution Principle V).
package integration

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/harshithgowda/asr/internal/inference"
	"github.com/harshithgowda/asr/internal/inference/mock"
	"github.com/harshithgowda/asr/internal/session"
	"github.com/harshithgowda/asr/internal/transport"
)

type transcriptMsg struct {
	Type    string `json:"type"`
	Text    string `json:"text"`
	IsFinal bool   `json:"is_final"`
}

// stack wires mock inference -> client -> manager -> transport on an httptest
// server and returns a ws:// URL plus cleanup.
func stack(t *testing.T, behavior mock.Behavior, opts session.Options) (string, func()) {
	t.Helper()
	session.SkipResult = inference.IsEmptyResult

	cc, mockCleanup, err := mock.StartBufconn(behavior)
	if err != nil {
		t.Fatalf("start mock: %v", err)
	}
	client := inference.New(cc)
	mgr := session.NewManager(client, opts)

	srv := httptest.NewServer(transport.Handler(
		transport.Options{MaxFrameBytes: 1 << 16}, mgr.Handle))

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	cleanup := func() {
		srv.Close()
		mockCleanup()
	}
	return url, cleanup
}

func sendStart(t *testing.T, ctx context.Context, c *websocket.Conn, interim bool) {
	t.Helper()
	msg, _ := json.Marshal(map[string]any{
		"type": "start",
		"config": map[string]any{
			"encoding": "LINEAR16", "sample_rate_hz": 16000,
			"language_code": "en-US", "interim_results": interim,
		},
	})
	if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
		t.Fatalf("write start: %v", err)
	}
}

// streamText streams s split into chunks, sends stop, and collects transcripts
// until the final one.
func streamText(t *testing.T, ctx context.Context, url string, interim bool, chunks []string) []transcriptMsg {
	t.Helper()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"asr.v1"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	sendStart(t, ctx, c, interim)
	for _, ch := range chunks {
		if err := c.Write(ctx, websocket.MessageBinary, []byte(ch)); err != nil {
			t.Fatalf("write audio: %v", err)
		}
	}
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"stop"}`)); err != nil {
		t.Fatalf("write stop: %v", err)
	}

	var got []transcriptMsg
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		var m transcriptMsg
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if m.Type != "transcript" {
			continue
		}
		got = append(got, m)
		if m.IsFinal {
			break
		}
	}
	return got
}

// T015 / SC-002: a full round trip yields a final transcript equal to the input.
func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url, cleanup := stack(t, mock.Normal, session.Options{WriteTimeout: 2 * time.Second})
	defer cleanup()

	got := streamText(t, ctx, url, true, []string{"hello ", "world"})
	if len(got) == 0 {
		t.Fatal("no transcripts")
	}
	final := got[len(got)-1]
	if !final.IsFinal || final.Text != "hello world" {
		t.Errorf("final = %+v, want text=%q is_final=true", final, "hello world")
	}
}

// T020 / User Story 2: interim transcripts precede the final one when requested,
// and are suppressed when interim_results=false.
func TestInterimResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url, cleanup := stack(t, mock.Normal, session.Options{WriteTimeout: 2 * time.Second})
	defer cleanup()

	t.Run("interim on", func(t *testing.T) {
		got := streamText(t, ctx, url, true, []string{"a", "b", "c"})
		interims := 0
		for _, m := range got[:len(got)-1] {
			if !m.IsFinal {
				interims++
			}
		}
		if interims == 0 {
			t.Errorf("expected >=1 interim before final, got %d (all: %+v)", interims, got)
		}
		if !got[len(got)-1].IsFinal {
			t.Error("last message is not final")
		}
	})

	t.Run("interim off", func(t *testing.T) {
		got := streamText(t, ctx, url, false, []string{"a", "b", "c"})
		for _, m := range got[:len(got)-1] {
			if !m.IsFinal {
				t.Errorf("received interim despite interim_results=false: %+v", m)
			}
		}
		if len(got) != 1 || !got[0].IsFinal {
			t.Errorf("want a single final transcript, got %+v", got)
		}
	})
}

// T026 / SC-005: a misbehaving client (sends only garbage control) does not
// affect a concurrent healthy client.
func TestIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url, cleanup := stack(t, mock.Normal, session.Options{WriteTimeout: 2 * time.Second})
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	// Misbehaving client: sends an invalid control message after start.
	go func() {
		defer wg.Done()
		c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"asr.v1"}})
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusInternalError, "")
		sendStart(t, ctx, c, true)
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"garbage"}`))
		_, _, _ = c.Read(ctx) // will receive the error frame / close
	}()

	// Healthy client runs concurrently and must succeed.
	got := streamText(t, ctx, url, true, []string{"still ", "works"})
	final := got[len(got)-1]
	if !final.IsFinal || final.Text != "still works" {
		t.Errorf("healthy client final = %+v, want %q", final, "still works")
	}
	wg.Wait()
}
