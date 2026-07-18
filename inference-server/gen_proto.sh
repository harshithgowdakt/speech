#!/usr/bin/env bash
# Generate Python gRPC stubs from the shared asr.proto contract.
set -euo pipefail
cd "$(dirname "$0")"
python3 -m grpc_tools.protoc \
  -I ../proto \
  --python_out=. \
  --grpc_python_out=. \
  ../proto/asr.proto
echo "generated asr_pb2.py and asr_pb2_grpc.py"
