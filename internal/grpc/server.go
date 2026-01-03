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

	err := s.Broker.Produce(req.Topic, req.Payload, req.Headers)
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
		offset, err := s.Broker.GetConsumerOffset(req.Topic, req.ConsumerName)
		if err != nil {
			// Should not happen as we just registered it
			currentOffset = 0
		} else {
			currentOffset = offset
		}
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
			// No new messages, wait for notification
			if err := s.Broker.WaitForMessage(req.Topic, stream.Context()); err != nil {
				return nil // Context cancelled or other error, just return
			}
			continue
		}

		for _, msg := range msgs {
			resp := &pb.ConsumeResponse{
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
				Payload:   msg.Payload,
				Headers:   msg.Headers,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			currentOffset = msg.Offset + 1
		}
	}
}

func (s *Server) CommitOffset(ctx context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	if req.Topic == "" || req.ConsumerName == "" {
		return nil, status.Error(codes.InvalidArgument, "topic and consumer_name are required")
	}

	err := s.Broker.CommitOffset(req.Topic, req.ConsumerName, req.Offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit offset: %v", err)
	}

	return &pb.CommitOffsetResponse{Success: true}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	// Use defaults if not provided
	retentionBytes := req.RetentionBytes
	if retentionBytes == 0 {
		retentionBytes = s.Broker.Config.DefaultRetentionBytes
	}

	retentionTime := time.Duration(req.RetentionTime)
	if retentionTime == 0 {
		retentionTime = s.Broker.Config.DefaultRetentionTime
	}

	// Use broker defaults for flush/segment settings as they are not in the request yet
	flushThreshold := s.Broker.Config.DefaultFlushThreshold
	flushInterval := s.Broker.Config.DefaultFlushInterval
	segmentSize := s.Broker.Config.DefaultSegmentSize

	err := s.Broker.CreateTopic(
		req.Topic,
		req.Fifo,
		retentionBytes,
		retentionTime,
		flushThreshold,
		flushInterval,
		segmentSize,
	)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	return &pb.CreateTopicResponse{Success: true}, nil
}
