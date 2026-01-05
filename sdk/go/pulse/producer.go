package pulse

import (
	"context"
	"fmt"
	"sync"

	pb "pulse/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	client pb.PulseServiceClient
	conn   *grpc.ClientConn
	stream pb.PulseService_StreamPublishClient
	mu     sync.Mutex
}

func NewProducer(opts ...ConsumerOptions) (*Producer, error) {
	cfg := GetConfig()
	host := cfg.Broker.Host
	port := cfg.Broker.GRPCPort

	if len(opts) > 0 {
		if opts[0].Host != "" {
			host = opts[0].Host
		}
		if opts[0].Port != 0 {
			port = opts[0].Port
		}
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewPulseServiceClient(conn)
	stream, err := client.StreamPublish(context.Background())
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Producer{
		client: client,
		conn:   conn,
		stream: stream,
	}, nil
}

func (p *Producer) Send(ctx context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stream.Send(&pb.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
}

func (p *Producer) Close() {
	if p.stream != nil {
		p.stream.CloseAndRecv()
	}
	p.conn.Close()
}
