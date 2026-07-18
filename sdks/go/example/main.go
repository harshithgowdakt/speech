// Command example streams a file's bytes as audio via the Go SDK and prints
// transcripts. With -drain it holds the session open so you can observe
// reconnect + replay when the gateway drains.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	asr "github.com/harshithgowdakt/speech/sdks/go"
)

func main() {
	url := flag.String("url", "ws://localhost:8080/v1/stream", "gateway URL")
	file := flag.String("file", "", "file to stream as audio")
	hold := flag.Duration("hold", 0, "keep streaming/idle this long (to observe reconnect)")
	flag.Parse()

	data, err := os.ReadFile(*file)
	if err != nil {
		fmt.Println("read:", err)
		os.Exit(1)
	}

	client := asr.New(asr.Options{
		URL: *url,
		Config: asr.Config{
			Encoding: asr.EncodingLINEAR16, SampleRateHz: 16000,
			LanguageCode: "en-US", InterimResults: true,
		},
		OnTranscript: func(t asr.Transcript) {
			tag := "interim"
			if t.IsFinal {
				tag = "FINAL"
			}
			fmt.Printf("[%s] %s\n", tag, t.Text)
		},
		OnError:       func(code, msg string) { fmt.Printf("[error] %s: %s\n", code, msg) },
		OnStateChange: func(s asr.State) { fmt.Printf("[state] %s\n", s) },
	})

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		fmt.Println("start:", err)
		os.Exit(1)
	}

	for i := 0; i < len(data); i += 4 {
		end := i + 4
		if end > len(data) {
			end = len(data)
		}
		if err := client.SendAudio(data[i:end]); err != nil {
			fmt.Println("send:", err)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if *hold > 0 {
		fmt.Printf("[holding %s — drain the gateway to see reconnect+replay]\n", *hold)
		time.Sleep(*hold)
	} else {
		time.Sleep(300 * time.Millisecond)
	}
	_ = client.Stop(ctx)
	time.Sleep(100 * time.Millisecond)
}
