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
	"syscall"
	"time"

	"github.com/harshithgowda/asr/internal/config"
	"github.com/harshithgowda/asr/internal/inference"
	"github.com/harshithgowda/asr/internal/metrics"
	"github.com/harshithgowda/asr/internal/session"
	"github.com/harshithgowda/asr/internal/transport"
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

	mux := http.NewServeMux()
	mux.Handle("/v1/stream", transport.Handler(
		transport.Options{MaxFrameBytes: cfg.MaxFrameBytes},
		mgr.Handle,
	))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux}

	// Graceful shutdown on SIGINT/SIGTERM.
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
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
	}
}
