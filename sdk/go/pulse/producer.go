package pulse

import (
	"context"
	"fmt"

	pb "pulse/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	client pb.PulseServiceClient
	conn   *grpc.ClientConn
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

	return &Producer{
		client: pb.NewPulseServiceClient(conn),
		conn:   conn,
	}, nil
}

func (p *Producer) Send(ctx context.Context, topic string, payload []byte) error {
	_, err := p.client.Publish(ctx, &pb.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

func (p *Producer) Close() {
	p.conn.Close()
}
