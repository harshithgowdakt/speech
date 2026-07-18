package session

import (
	"context"
	"sync"

	"github.com/harshithgowda/asr/internal/metrics"
)

// Manager owns the registry of active sessions. The registry map is the only
// shared state between sessions and is the isolation boundary (FR-007).
type Manager struct {
	inference InferenceClient
	opts      Options

	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewManager creates a session manager over the given inference client.
func NewManager(inf InferenceClient, opts Options) *Manager {
	return &Manager{
		inference: inf,
		opts:      opts,
		sessions:  make(map[string]*Session),
	}
}

// Handle runs one client connection to completion: it registers the session,
// drives its lifecycle, and guarantees deregistration and metric accounting on
// exit. It is safe for concurrent use — one goroutine per connection.
func (m *Manager) Handle(ctx context.Context, conn ClientConn) {
	s := newSession(conn, m.inference, m.opts)
	id := conn.ID()

	m.register(id, s)
	metrics.ActiveSessions.Inc()
	defer func() {
		m.remove(id)
		metrics.ActiveSessions.Dec()
	}()

	outcome := s.Run(ctx)
	metrics.SessionsTotal.WithLabelValues(outcome).Inc()
}

// Count returns the number of currently active sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

func (m *Manager) register(id string, s *Session) {
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
}

func (m *Manager) remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}
