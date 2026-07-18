// Command wsclient is a minimal smoke-test client: it streams a file's bytes as
// audio to the gateway and prints the transcripts it receives. Because the mock
// inference server treats audio bytes as text, feeding it a text file echoes the
// file contents back as the transcript.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/coder/websocket"
)

func main() {
	url := flag.String("url", "ws://localhost:8080/v1/stream", "gateway WebSocket URL")
	file := flag.String("file", "", "path to a file to stream as audio")
	chunk := flag.Int("chunk", 1024, "chunk size in bytes")
	flag.Parse()

	if *file == "" {
		log.Fatal("-file is required")
	}
	data, err := os.ReadFile(*file)
	if err != nil {
		log.Fatalf("read file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, *url, &websocket.DialOptions{Subprotocols: []string{"asr.v1"}})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	start, _ := json.Marshal(map[string]any{
		"type": "start",
		"config": map[string]any{
			"encoding": "LINEAR16", "sample_rate_hz": 16000,
			"language_code": "en-US", "interim_results": true,
		},
	})
	if err := c.Write(ctx, websocket.MessageText, start); err != nil {
		log.Fatalf("write start: %v", err)
	}

	for i := 0; i < len(data); i += *chunk {
		end := i + *chunk
		if end > len(data) {
			end = len(data)
		}
		if err := c.Write(ctx, websocket.MessageBinary, data[i:end]); err != nil {
			log.Fatalf("write audio: %v", err)
		}
	}
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"stop"}`)); err != nil {
		log.Fatalf("write stop: %v", err)
	}

	for {
		typ, msg, err := c.Read(ctx)
		if err != nil {
			// Normal closure ends the loop.
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var m struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			IsFinal bool   `json:"is_final"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(msg, &m) != nil {
			continue
		}
		switch m.Type {
		case "transcript":
			tag := "interim"
			if m.IsFinal {
				tag = "FINAL"
			}
			fmt.Printf("[%s] %s\n", tag, m.Text)
			if m.IsFinal {
				return
			}
		case "error":
			fmt.Printf("[error] %s: %s\n", m.Code, m.Message)
			return
		}
	}
}
