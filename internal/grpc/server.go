package grpc

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"pulse/internal/broker"
	"pulse/pkg/message"
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

func (s *Server) StreamPublish(stream pb.PulseService_StreamPublishServer) error {
	var succeeded uint64
	var failed uint64
	var lastErr string

	batchSize := 100
	// Map topic -> batch of messages
	batches := make(map[string][]*message.Message)

	flushBatch := func() {
		for topic, msgs := range batches {
			if len(msgs) == 0 {
				continue
			}
			err := s.Broker.ProduceBatch(topic, msgs)
			if err != nil {
				failed += uint64(len(msgs))
				lastErr = err.Error()
			} else {
				succeeded += uint64(len(msgs))
			}
			// Clear batch
			batches[topic] = batches[topic][:0]
		}
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			flushBatch()
			return stream.SendAndClose(&pb.PublishSummary{
				SucceededCount: succeeded,
				FailedCount:    failed,
				LastError:      lastErr,
			})
		}
		if err != nil {
			return err
		}

		if req.Topic == "" {
			failed++
			lastErr = "topic is required"
			continue
		}

		msg := message.NewMessage(0, req.Payload, req.Headers)
		batches[req.Topic] = append(batches[req.Topic], &msg)

		if len(batches[req.Topic]) >= batchSize {
			err := s.Broker.ProduceBatch(req.Topic, batches[req.Topic])
			if err != nil {
				failed += uint64(len(batches[req.Topic]))
				lastErr = err.Error()
			} else {
				succeeded += uint64(len(batches[req.Topic]))
			}
			batches[req.Topic] = batches[req.Topic][:0]
		}
	}
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

		// Get notification channel BEFORE reading to avoid race condition (Lost Wakeup)
		notifyChan, err := s.Broker.GetNotifyChannel(req.Topic)
		if err != nil {
			return status.Errorf(codes.NotFound, "topic not found: %v", err)
		}

		// Read messages
		// We use a larger batch size for streaming to improve throughput
		msgs, err := s.Broker.Consume(req.Topic, currentOffset, 1000)
		if err != nil {
			// If topic doesn't exist, return error
			return status.Errorf(codes.NotFound, "topic not found or error reading: %v", err)
		}

		if len(msgs) == 0 {
			// No new messages, wait for notification using the channel we captured BEFORE reading
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case <-notifyChan:
				// New message available (or channel closed/replaced), loop back to read
				continue
			}
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
