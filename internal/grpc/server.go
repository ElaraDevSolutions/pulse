package grpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"pulse/internal/broker"
	pb "pulse/pkg/proto"
)

type Server struct {
	pb.UnimplementedPulseServiceServer
	Broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{Broker: b}
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	// We don't have the offset immediately available from Produce because it's async/buffered in some cases,
	// or simply Produce doesn't return it.
	// Let's check Broker.Produce signature.
	// It returns error.
	// For now, we return a success response with 0 offset (or we could update Produce to return it).

	err := s.Broker.Produce(req.Topic, req.Payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to produce message: %v", err)
	}

	return &pb.PublishResponse{
		Id:     "", // We could generate a UUID if needed
		Offset: 0,  // TODO: Update Broker to return the assigned offset
	}, nil
}

func (s *Server) Consume(req *pb.ConsumeRequest, stream pb.PulseService_ConsumeServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	// Ensure consumer is registered if name is provided
	if req.ConsumerName != "" {
		s.Broker.RegisterConsumer(req.Topic, req.ConsumerName)
	}

	// Determine starting offset
	var currentOffset uint64
	if req.Offset > 0 {
		currentOffset = req.Offset
	} else if req.ConsumerName != "" {
		// Get last committed offset
		// We need a method in Broker to get the current offset for a consumer without reading
		// For now, let's assume 0 if not provided, or we'd need to extend Broker.
		// Actually, RegisterConsumer initializes it to 0 if new.
		// If existing, we want to resume.
		// But we don't have a public API to "GetConsumerOffset" yet.
		// Let's default to 0 for now, or the client must provide it.
		// Ideally, we should look it up.
		// Let's assume the client sends 0 to mean "start from where I left off".
		// But we can't easily look it up without extending the Broker API.
		// Let's stick to: if 0, start from 0.
		currentOffset = 0

		// Optimization: If we could read the stored offset, that would be better.
		// But let's proceed with 0 or explicit offset for this iteration.
	}

	// Streaming Loop
	for {
		// Check context cancellation
		if stream.Context().Err() != nil {
			return stream.Context().Err()
		}

		// Read messages
		// We use a small batch size for streaming
		msgs, err := s.Broker.Consume(req.Topic, currentOffset, 10)
		if err != nil {
			// If topic doesn't exist, return error
			return status.Errorf(codes.NotFound, "topic not found or error reading: %v", err)
		}

		if len(msgs) == 0 {
			// No new messages, sleep and poll again
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			resp := &pb.ConsumeResponse{
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
				Payload:   msg.Payload,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			currentOffset = msg.Offset + 1
		}
	}
}
