package inference_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/harshithgowda/asr/internal/inference"
	"github.com/harshithgowda/asr/internal/inference/mock"
	"github.com/harshithgowda/asr/internal/session"
)

// T014: contract test for the gRPC client — config-then-audio ordering and
// interim-then-final transcript reception against the generated stubs.
func TestClientContract(t *testing.T) {
	cc, cleanup, err := mock.StartBufconn(mock.Normal)
	if err != nil {
		t.Fatalf("start mock: %v", err)
	}
	defer cleanup()

	client := inference.New(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StartStream(ctx, session.RecognitionConfig{
		Encoding: session.EncodingLINEAR16, SampleRateHz: 16000,
		LanguageCode: "en-US", InterimResults: true,
	})
	if err != nil {
		t.Fatalf("start stream: %v", err)
	}

	for _, chunk := range []string{"hello ", "world"} {
		if err := stream.Send([]byte(chunk)); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	var got []session.Transcript
	for {
		tr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if inference.IsEmptyResult(err) {
			continue
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got = append(got, tr)
	}

	if len(got) == 0 {
		t.Fatal("no transcripts received")
	}
	final := got[len(got)-1]
	if !final.IsFinal {
		t.Errorf("last transcript is_final = false, want true")
	}
	if final.Text != "hello world" {
		t.Errorf("final text = %q, want %q", final.Text, "hello world")
	}
	// Contract: at least one interim precedes the final.
	sawInterim := false
	for _, tr := range got[:len(got)-1] {
		if !tr.IsFinal {
			sawInterim = true
		}
	}
	if !sawInterim {
		t.Errorf("expected at least one interim transcript before final")
	}
}
