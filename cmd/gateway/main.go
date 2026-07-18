// Command gateway is the WebSocket ASR streaming gateway: it accepts audio over
// WebSocket, relays it to the ASR inference server over gRPC, and streams
// transcripts back to the client.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/harshithgowdakt/speech/internal/config"
	"github.com/harshithgowdakt/speech/internal/inference"
	"github.com/harshithgowdakt/speech/internal/metrics"
	"github.com/harshithgowdakt/speech/internal/session"
	"github.com/harshithgowdakt/speech/internal/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	metrics.InitLogging(slog.LevelInfo)
	log := slog.Default()

	cfg, err := config.Load()
	if err != nil {
		log.Error("config error", "err", err)
		os.Exit(1)
	}

	// Wire the down-pump's result-skip predicate to the inference adapter.
	session.SkipResult = inference.IsEmptyResult

	infClient, err := inference.Dial(cfg.InferenceAddr)
	if err != nil {
		log.Error("inference dial error", "err", err)
		os.Exit(1)
	}
	defer infClient.Close()

	mgr := session.NewManager(infClient, session.Options{
		SessionTimeout: cfg.SessionTimeout,
		WriteTimeout:   cfg.WriteTimeout,
		MaxFrameBytes:  cfg.MaxFrameBytes,
	})

	// ready gates readiness: it flips to false at the start of shutdown so the
	// load balancer stops routing NEW connections while we drain existing ones.
	var ready atomic.Bool
	ready.Store(true)

	mux := http.NewServeMux()
	mux.Handle("/v1/stream", transport.Handler(
		transport.Options{MaxFrameBytes: cfg.MaxFrameBytes},
		mgr.Handle,
	))
	mux.Handle("/metrics", promhttp.Handler())
	// Liveness: process is up. Never fails during drain (avoids a kill-restart).
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Readiness: accepting new connections. Fails during drain.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "draining", http.StatusServiceUnavailable)
	})

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("gateway listening", "addr", cfg.ListenAddr, "inference", cfg.InferenceAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	stop() // restore default handling so a SECOND signal can force-quit the wait

	// A second SIGINT/SIGTERM during drain skips the remaining delay.
	forceCtx, forceStop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer forceStop()

	// 1. Fail readiness so the load balancer stops routing NEW connections.
	ready.Store(false)
	log.Info("shutdown initiated", "drain_delay", cfg.DrainDelay,
		"shutdown_timeout", cfg.ShutdownTimeout, "active_sessions", mgr.Count())

	// 2. Wait for Service endpoint removal to propagate before we stop accepting
	//    connections, so in-flight connection attempts don't get refused.
	//    (Distroless has no shell, so this replaces a preStop sleep hook.)
	if cfg.DrainDelay > 0 {
		select {
		case <-time.After(cfg.DrainDelay):
		case <-forceCtx.Done(): // second signal: skip the wait and drain now
		}
	}

	// 3. Drain, bounded by ShutdownTimeout (keep it below the pod's
	//    terminationGracePeriodSeconds so drain finishes before SIGKILL).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// Stop accepting new connections. WebSocket connections are hijacked and are
	// NOT tracked by http.Server, so this returns promptly — we drain them next.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	// Ask active sessions to close going-away (clients reconnect elsewhere) and
	// wait for them, bounded by the same deadline.
	if remaining := mgr.Drain(shutdownCtx); remaining > 0 {
		log.Warn("shutdown deadline reached with sessions still active", "count", remaining)
	} else {
		log.Info("all sessions drained cleanly")
	}
}
