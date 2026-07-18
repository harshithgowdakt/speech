// Package metrics holds the observability primitives required by Constitution
// Principle IV: Prometheus collectors plus structured logging with a per-session
// correlation ID carried on the context.
package metrics

import (
	"context"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ActiveSessions is the number of currently open streaming sessions.
	ActiveSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "asr_active_sessions",
		Help: "Number of currently active streaming sessions.",
	})

	// AudioBytesIn counts total audio bytes ingested from clients.
	AudioBytesIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "asr_audio_bytes_in_total",
		Help: "Total audio bytes received from clients.",
	})

	// TranscriptLatency observes gateway forwarding latency for a transcript
	// (time from receiving it upstream to writing it to the client).
	TranscriptLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "asr_transcript_latency_seconds",
		Help:    "Gateway forwarding latency per transcript, in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// InferenceErrors counts inference-stream failures by code.
	InferenceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "asr_inference_errors_total",
		Help: "Total inference stream errors, labeled by code.",
	}, []string{"code"})

	// SessionsTotal counts completed sessions by outcome.
	SessionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "asr_sessions_total",
		Help: "Total sessions that have ended, labeled by outcome.",
	}, []string{"outcome"})
)

type ctxKey struct{}

// InitLogging installs a JSON slog handler as the default logger.
func InitLogging(level slog.Level) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
}

// WithCorrelationID returns a context carrying the per-session correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID extracts the correlation ID from ctx, or "" if absent.
func CorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKey{}).(string); ok {
		return id
	}
	return ""
}

// Logger returns a slog.Logger annotated with the context's correlation ID.
func Logger(ctx context.Context) *slog.Logger {
	return slog.Default().With("session_id", CorrelationID(ctx))
}
