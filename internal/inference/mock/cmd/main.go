// Command mockasr runs the mock ASR inference server on a real TCP port for
// local development and the manual smoke test in quickstart.md.
package main

import (
	"log"
	"net"
	"os"

	pb "github.com/harshithgowdakt/speech/internal/genproto/asr"
	"github.com/harshithgowdakt/speech/internal/inference/mock"
	"google.golang.org/grpc"
)

func main() {
	addr := os.Getenv("MOCK_ASR_ADDR")
	if addr == "" {
		addr = ":50051"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterASRServiceServer(srv, &mock.Server{Behavior: mock.Normal})
	log.Printf("mock ASR inference server listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
