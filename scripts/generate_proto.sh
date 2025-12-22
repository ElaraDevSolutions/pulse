#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed."
    echo "Please install Protocol Buffers compiler:"
    echo "  Mac: brew install protobuf"
    echo "  Linux: apt-get install -y protobuf-compiler"
    exit 1
fi

# Install Go plugins if not present
if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

# Add Go bin to PATH
export PATH=$PATH:$(go env GOPATH)/bin

# Create destination directory
mkdir -p pkg/proto

echo "Generating gRPC code..."

# Generate code
# Input: internal/api/proto/pulse.proto
# Output: pkg/proto/
protoc --proto_path=internal/api/proto \
       --go_out=pkg/proto --go_opt=paths=source_relative \
       --go-grpc_out=pkg/proto --go-grpc_opt=paths=source_relative \
       pulse.proto

echo "Done."
