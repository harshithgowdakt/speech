// Package inference is the gRPC adapter implementing the session.InferenceClient
// port against the ASR inference server's bidirectional StreamingRecognize RPC.
package inference

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	pb "github.com/harshithgowdakt/speech/internal/genproto/asr"
	"github.com/harshithgowdakt/speech/internal/metrics"
	"github.com/harshithgowdakt/speech/internal/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// Client is a gRPC-backed session.InferenceClient. It pools several client
// connections and round-robins streams across them: a single HTTP/2 connection
// caps concurrent streams (SETTINGS_MAX_CONCURRENT_STREAMS) and becomes a
// throughput chokepoint, so pooling is what lets one gateway instance carry
// thousands of concurrent sessions.
type Client struct {
	conns []*grpc.ClientConn
	apis  []pb.ASRServiceClient
	next  atomic.Uint64
}

// Dial creates a Client with a pool of poolSize connections to addr (insecure
// transport for v1). poolSize < 1 is treated as 1.
func Dial(addr string, poolSize int) (*Client, error) {
	if poolSize < 1 {
		poolSize = 1
	}
	c := &Client{}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	}
	for i := 0; i < poolSize; i++ {
		cc, err := grpc.NewClient(addr, opts...)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("dial inference server %q: %w", addr, err)
		}
		c.conns = append(c.conns, cc)
		c.apis = append(c.apis, pb.NewASRServiceClient(cc))
	}
	return c, nil
}

// New wraps existing gRPC connections (used by tests, e.g. over bufconn).
func New(ccs ...*grpc.ClientConn) *Client {
	c := &Client{}
	for _, cc := range ccs {
		c.conns = append(c.conns, cc)
		c.apis = append(c.apis, pb.NewASRServiceClient(cc))
	}
	return c
}

// Close releases all pooled connections.
func (c *Client) Close() error {
	var firstErr error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// pick returns the next pooled client in round-robin order.
func (c *Client) pick() pb.ASRServiceClient {
	i := c.next.Add(1) - 1
	return c.apis[int(i%uint64(len(c.apis)))]
}

// StartStream opens a bidirectional stream and sends the recognition config first.
func (c *Client) StartStream(ctx context.Context, cfg session.RecognitionConfig) (session.InferenceStream, error) {
	// Propagate the correlation ID to the inference server for cross-service tracing.
	if id := metrics.CorrelationID(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", id)
	}
	stream, err := c.pick().StreamingRecognize(ctx)
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
