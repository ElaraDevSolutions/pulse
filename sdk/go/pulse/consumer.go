package pulse

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "pulse/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Handler func(ctx context.Context, msg *Message) error

type consumerConfig struct {
	topic      string
	handler    Handler
	host       string
	port       int
	group      string
	autoCommit bool
}

var consumers []*consumerConfig
var mu sync.Mutex

type ConsumerOptions struct {
	Host          string
	Port          int
	ConsumerGroup string
	AutoCommit    *bool
}

func Consumer(topic string, handler Handler, opts ...ConsumerOptions) {
	cfg := GetConfig()

	c := &consumerConfig{
		topic:      topic,
		handler:    handler,
		host:       cfg.Broker.Host,
		port:       cfg.Broker.GRPCPort,
		group:      cfg.Client.ID,
		autoCommit: cfg.Client.AutoCommit,
	}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.Host != "" {
			c.host = opt.Host
		}
		if opt.Port != 0 {
			c.port = opt.Port
		}
		if opt.ConsumerGroup != "" {
			c.group = opt.ConsumerGroup
		}
		if opt.AutoCommit != nil {
			c.autoCommit = *opt.AutoCommit
		}
	}

	mu.Lock()
	consumers = append(consumers, c)
	mu.Unlock()
}

func Run() {
	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)
		go func(c *consumerConfig) {
			defer wg.Done()
			runConsumer(c)
		}(c)
	}
	wg.Wait()
}

// Context key for commit function
type commitKey struct{}

func Commit(ctx context.Context) error {
	fn, ok := ctx.Value(commitKey{}).(func() error)
	if !ok {
		return fmt.Errorf("commit called outside of consumer context")
	}
	return fn()
}

func runConsumer(c *consumerConfig) {
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	log.Printf("Starting consumer for topic '%s' (group: %s) on %s", c.topic, c.group, addr)

	for {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to %s: %v. Retrying...", addr, err)
			time.Sleep(5 * time.Second)
			continue
		}

		client := pb.NewPulseServiceClient(conn)
		stream, err := client.Consume(context.Background(), &pb.ConsumeRequest{
			Topic:        c.topic,
			ConsumerName: c.group,
			Offset:       0,
		})
		if err != nil {
			log.Printf("Failed to start stream for %s: %v. Retrying...", c.topic, err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Printf("Stream error for %s: %v. Reconnecting...", c.topic, err)
				break
			}

			m := newMessage(msg)
			committed := false

			commitFn := func() error {
				if committed {
					return nil
				}
				_, err := client.CommitOffset(context.Background(), &pb.CommitOffsetRequest{
					Topic:        c.topic,
					ConsumerName: c.group,
					Offset:       msg.Offset + 1,
				})
				if err == nil {
					committed = true
				}
				return err
			}

			ctx := context.WithValue(context.Background(), commitKey{}, commitFn)

			err = c.handler(ctx, m)
			if err != nil {
				log.Printf("Handler error: %v", err)
				continue
			}

			if c.autoCommit && !committed {
				commitFn()
			}
		}
		conn.Close()
		time.Sleep(5 * time.Second)
	}
}
