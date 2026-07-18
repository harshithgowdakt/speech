// Package config loads gateway configuration from the environment with
// validated defaults (Constitution: no hardcoded addresses or magic timeouts).
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all tunable gateway parameters.
type Config struct {
	ListenAddr        string        // ASR_LISTEN_ADDR: WebSocket + /metrics listen address
	InferenceAddr     string        // ASR_INFERENCE_ADDR: gRPC inference server target
	InferencePoolSize int           // ASR_INFERENCE_POOL_SIZE: gRPC client connections to pool
	MaxConnections    int           // ASR_MAX_CONNECTIONS: max concurrent WebSocket connections (0 = unlimited)
	SessionTimeout    time.Duration // ASR_SESSION_TIMEOUT: idle/stalled-stream timeout
	AudioBuffer       int           // ASR_AUDIO_BUFFER: up-pump bound (reserved)
	TranscriptBuffer  int           // ASR_TRANSCRIPT_BUFFER: down-pump bound (reserved)
	MaxFrameBytes     int64         // ASR_MAX_FRAME_BYTES: max audio frame size
	WriteTimeout      time.Duration // ASR_WRITE_TIMEOUT: slow-consumer termination threshold
	DrainDelay        time.Duration // ASR_DRAIN_DELAY: wait after SIGTERM before draining (endpoint propagation)
	ShutdownTimeout   time.Duration // ASR_SHUTDOWN_TIMEOUT: max time to drain sessions on SIGTERM
}

// Load reads configuration from the environment, applying defaults.
func Load() (Config, error) {
	c := Config{
		ListenAddr:        getEnv("ASR_LISTEN_ADDR", ":8080"),
		InferenceAddr:     getEnv("ASR_INFERENCE_ADDR", "localhost:50051"),
		InferencePoolSize: getInt("ASR_INFERENCE_POOL_SIZE", 8),
		MaxConnections:    getInt("ASR_MAX_CONNECTIONS", 2000),
		SessionTimeout:    getDuration("ASR_SESSION_TIMEOUT", 30*time.Second),
		AudioBuffer:       getInt("ASR_AUDIO_BUFFER", 32),
		TranscriptBuffer:  getInt("ASR_TRANSCRIPT_BUFFER", 32),
		MaxFrameBytes:     getInt64("ASR_MAX_FRAME_BYTES", 65536),
		WriteTimeout:      getDuration("ASR_WRITE_TIMEOUT", 5*time.Second),
		DrainDelay:        getDuration("ASR_DRAIN_DELAY", 5*time.Second),
		ShutdownTimeout:   getDuration("ASR_SHUTDOWN_TIMEOUT", 45*time.Second),
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("ASR_LISTEN_ADDR must not be empty")
	}
	if c.InferenceAddr == "" {
		return fmt.Errorf("ASR_INFERENCE_ADDR must not be empty")
	}
	if c.MaxFrameBytes <= 0 {
		return fmt.Errorf("ASR_MAX_FRAME_BYTES must be > 0, got %d", c.MaxFrameBytes)
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("ASR_WRITE_TIMEOUT must be > 0, got %s", c.WriteTimeout)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("ASR_SHUTDOWN_TIMEOUT must be > 0, got %s", c.ShutdownTimeout)
	}
	if c.InferencePoolSize < 1 {
		return fmt.Errorf("ASR_INFERENCE_POOL_SIZE must be >= 1, got %d", c.InferencePoolSize)
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("ASR_MAX_CONNECTIONS must be >= 0, got %d", c.MaxConnections)
	}
	return nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getInt64(key string, def int64) int64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
