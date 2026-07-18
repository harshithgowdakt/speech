package inference_test

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/harshithgowdakt/speech/internal/genproto/asr"
	"github.com/harshithgowdakt/speech/internal/inference"
	"github.com/harshithgowdakt/speech/internal/inference/mock"
	"github.com/harshithgowdakt/speech/internal/session"
	"google.golang.org/grpc"
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

// The pool serves many concurrent streams against a real TCP server, exercising
// round-robin across pooled connections (the change that lifts the single-HTTP/2
// -connection stream cap).
func TestClientPoolConcurrent(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterASRServiceServer(srv, &mock.Server{Behavior: mock.Normal})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	client, err := inference.Dial(lis.Addr().String(), 4)
	if err != nil {
		t.Fatalf("dial pool: %v", err)
	}
	defer client.Close()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := client.StartStream(ctx, session.RecognitionConfig{
				Encoding: session.EncodingLINEAR16, SampleRateHz: 16000,
				LanguageCode: "en-US", InterimResults: true,
			})
			if err != nil {
				errs <- err
				return
			}
			_ = stream.Send([]byte("hi"))
			_ = stream.CloseSend()
			var final session.Transcript
			for {
				tr, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if inference.IsEmptyResult(err) {
					continue
				}
				if err != nil {
					errs <- err
					return
				}
				final = tr
			}
			if !final.IsFinal || final.Text != "hi" {
				errs <- errBadFinal
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent stream failed: %v", e)
	}
}

var errBadFinal = errorString("final transcript mismatch")

type errorString string

func (e errorString) Error() string { return string(e) }
