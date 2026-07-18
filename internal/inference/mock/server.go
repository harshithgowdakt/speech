// Package mock provides an in-process ASR inference server for tests and local
// dev. It treats each audio frame's bytes as UTF-8 text so a round trip is
// deterministic: the concatenation of audio frames is the reference transcript.
package mock

import (
	"context"
	"io"
	"net"
	"strings"

	pb "github.com/harshithgowda/asr/internal/genproto/asr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Behavior selects how the mock responds, to exercise different session paths.
type Behavior int

const (
	// Normal emits an interim transcript per audio frame, then a final on EOF.
	Normal Behavior = iota
	// ErrorMidStream emits one interim then aborts the stream with an error.
	ErrorMidStream
	// Stall receives audio but never emits anything (exercises session timeout).
	Stall
)

// Server implements pb.ASRServiceServer with the configured Behavior.
type Server struct {
	pb.UnimplementedASRServiceServer
	Behavior Behavior
}

// StreamingRecognize implements the bidirectional inference RPC.
func (s *Server) StreamingRecognize(stream pb.ASRService_StreamingRecognizeServer) error {
	// First message MUST be the config.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	cfg := first.GetConfig()
	if cfg == nil {
		return status.Error(codes.InvalidArgument, "first message must be config")
	}
	interim := cfg.GetInterimResults()

	var b strings.Builder
	frames := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client half-closed: emit the final transcript.
			return stream.Send(finalResponse(b.String()))
		}
		if err != nil {
			return err
		}
		audio := req.GetAudioContent()
		if len(audio) == 0 {
			continue
		}
		b.Write(audio)
		frames++

		switch s.Behavior {
		case Stall:
			// Receive but never send; rely on client/session timeout + ctx cancel.
			continue
		case ErrorMidStream:
			if frames == 1 && interim {
				_ = stream.Send(interimResponse(b.String()))
			}
			return stream.Send(&pb.StreamingRecognizeResponse{
				Error: &pb.StreamError{Code: "internal", Message: "mock injected error"},
			})
		default: // Normal
			if interim {
				if err := stream.Send(interimResponse(b.String())); err != nil {
					return err
				}
			}
		}
	}
}

func interimResponse(text string) *pb.StreamingRecognizeResponse {
	return &pb.StreamingRecognizeResponse{Results: []*pb.StreamingResult{{
		Transcript: text, IsFinal: false, Stability: 0.8,
	}}}
}

func finalResponse(text string) *pb.StreamingRecognizeResponse {
	return &pb.StreamingRecognizeResponse{Results: []*pb.StreamingResult{{
		Transcript: text, IsFinal: true, Confidence: 0.95,
	}}}
}

// StartBufconn starts the mock server on an in-memory bufconn listener and
// returns a connected *grpc.ClientConn plus a cleanup func.
func StartBufconn(behavior Behavior) (*grpc.ClientConn, func(), error) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterASRServiceServer(srv, &Server{Behavior: behavior})

	go func() { _ = srv.Serve(lis) }()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		return nil, nil, err
	}
	cleanup := func() {
		_ = cc.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return cc, cleanup, nil
}
