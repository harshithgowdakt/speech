package transport

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/harshithgowdakt/speech/internal/metrics"
	"github.com/harshithgowdakt/speech/internal/session"
)

// Options configures the WebSocket transport.
type Options struct {
	// MaxFrameBytes bounds a single inbound frame (Constitution Principle III).
	MaxFrameBytes int64
	// MaxConnections caps concurrent connections on this instance; 0 = unlimited.
	// Over the cap, upgrades are refused with 503 so overload sheds cleanly.
	MaxConnections int
}

// ConnHandler receives an accepted client connection for orchestration. The
// context already carries the per-session correlation ID.
type ConnHandler func(ctx context.Context, conn session.ClientConn)

// Handler returns an http.Handler that upgrades requests at /v1/stream to a
// WebSocket and hands each connection to onConn.
func Handler(opts Options, onConn ConnHandler) http.Handler {
	// Buffered channel as a counting semaphore for the connection cap.
	var sem chan struct{}
	if opts.MaxConnections > 0 {
		sem = make(chan struct{}, opts.MaxConnections)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Enforce the concurrency cap BEFORE upgrading, so overload is a cheap 503.
		if sem != nil {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			default:
				metrics.ConnectionsRejected.Inc()
				http.Error(w, "server at capacity", http.StatusServiceUnavailable)
				return
			}
		}

		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols: []string{"asr.v1"},
		})
		if err != nil {
			return // Accept already wrote the HTTP error response
		}
		if opts.MaxFrameBytes > 0 {
			c.SetReadLimit(opts.MaxFrameBytes)
		}

		id := uuid.NewString()
		conn := &wsConn{c: c, id: id}
		ctx := metrics.WithCorrelationID(r.Context(), id)

		// onConn owns the connection lifecycle and is responsible for closing it.
		onConn(ctx, conn)
	})
}
