// Package inference is the gRPC adapter implementing the session.InferenceClient
// port against the ASR inference server's bidirectional StreamingRecognize RPC.
package inference

import (
	"context"
	"fmt"

	pb "github.com/harshithgowdakt/speech/internal/genproto/asr"
	"github.com/harshithgowdakt/speech/internal/metrics"
	"github.com/harshithgowdakt/speech/internal/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client is a gRPC-backed session.InferenceClient.
type Client struct {
	cc  *grpc.ClientConn
	api pb.ASRServiceClient
}

// Dial creates a Client connected to addr (insecure transport for v1).
func Dial(addr string) (*Client, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial inference server %q: %w", addr, err)
	}
	return New(cc), nil
}

// New wraps an existing gRPC connection (used by tests over bufconn).
func New(cc *grpc.ClientConn) *Client {
	return &Client{cc: cc, api: pb.NewASRServiceClient(cc)}
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.cc.Close() }

// StartStream opens a bidirectional stream and sends the recognition config first.
func (c *Client) StartStream(ctx context.Context, cfg session.RecognitionConfig) (session.InferenceStream, error) {
	// Propagate the correlation ID to the inference server for cross-service tracing.
	if id := metrics.CorrelationID(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", id)
	}
	stream, err := c.api.StreamingRecognize(ctx)
	if err != nil {
		return nil, fmt.Errorf("open StreamingRecognize: %w", err)
	}
	req := &pb.StreamingRecognizeRequest{
		StreamingRequest: &pb.StreamingRecognizeRequest_Config{Config: toPBConfig(cfg)},
	}
	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("send config: %w", err)
	}
	return &grpcStream{stream: stream}, nil
}

type grpcStream struct {
	stream pb.ASRService_StreamingRecognizeClient
}

func (s *grpcStream) Send(audio []byte) error {
	return s.stream.Send(&pb.StreamingRecognizeRequest{
		StreamingRequest: &pb.StreamingRecognizeRequest_AudioContent{AudioContent: audio},
	})
}

func (s *grpcStream) CloseSend() error { return s.stream.CloseSend() }

func (s *grpcStream) Recv() (session.Transcript, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return session.Transcript{}, err // includes io.EOF, passed through
	}
	if se := resp.GetError(); se != nil {
		code := se.GetCode()
		if code == "" {
			code = session.CodeInference
		}
		return session.Transcript{}, &session.SessionError{Code: code, Message: se.GetMessage()}
	}
	results := resp.GetResults()
	if len(results) == 0 {
		// Empty response (e.g. keep-alive); signal caller to keep receiving.
		return session.Transcript{}, errEmptyResult
	}
	r := results[0]
	return session.Transcript{
		Text:       r.GetTranscript(),
		IsFinal:    r.GetIsFinal(),
		Confidence: r.GetConfidence(),
		Stability:  r.GetStability(),
	}, nil
}

// errEmptyResult signals a response carrying no results; callers should skip it.
var errEmptyResult = fmt.Errorf("inference: empty result")

// IsEmptyResult reports whether err is the skip-this-response sentinel.
func IsEmptyResult(err error) bool { return err == errEmptyResult }

func toPBConfig(cfg session.RecognitionConfig) *pb.RecognitionConfig {
	return &pb.RecognitionConfig{
		Encoding:       toPBEncoding(cfg.Encoding),
		SampleRateHz:   cfg.SampleRateHz,
		LanguageCode:   cfg.LanguageCode,
		InterimResults: cfg.InterimResults,
	}
}

func toPBEncoding(enc string) pb.AudioEncoding {
	switch enc {
	case session.EncodingLINEAR16:
		return pb.AudioEncoding_AUDIO_ENCODING_LINEAR16
	case session.EncodingFLAC:
		return pb.AudioEncoding_AUDIO_ENCODING_FLAC
	case session.EncodingOGGOPUS:
		return pb.AudioEncoding_AUDIO_ENCODING_OGG_OPUS
	case session.EncodingMULAW:
		return pb.AudioEncoding_AUDIO_ENCODING_MULAW
	default:
		return pb.AudioEncoding_AUDIO_ENCODING_UNSPECIFIED
	}
}
