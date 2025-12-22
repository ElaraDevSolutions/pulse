package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"pulse/internal/api"
	"pulse/internal/broker"
	"pulse/internal/config"
	internalgrpc "pulse/internal/grpc"
	pb "pulse/pkg/proto"
)

// TestContext holds the running server info
type TestContext struct {
	Broker     *broker.Broker
	HttpAddr   string
	GrpcAddr   string
	DataDir    string
	GrpcServer *grpc.Server
	HttpServer *http.Server
	Config     *config.Config
}

// setupServer starts the Pulse broker (HTTP + gRPC) on random ports.
// If dataDir is empty, a temp dir is created.
func setupServer(t *testing.T, dataDir string) *TestContext {
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	// 1. Configure
	cfg := config.NewDefault()
	cfg.DataDir = dataDir
	cfg.Port = 0     // Random port
	cfg.GRPCPort = 0 // Random port
	// Speed up flushing for tests
	cfg.DefaultFlushInterval = 10 * time.Millisecond
	cfg.DefaultFlushThreshold = 1

	// 2. Init Broker
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("Failed to init broker: %v", err)
	}

	// 3. Start HTTP Server
	apiServer := api.NewServer(b)
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen HTTP: %v", err)
	}
	httpServer := &http.Server{
		Handler: apiServer.Routes(),
	}
	go httpServer.Serve(httpListener)

	// 4. Start gRPC Server
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen gRPC: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterPulseServiceServer(grpcServer, internalgrpc.NewServer(b))
	go grpcServer.Serve(grpcListener)

	return &TestContext{
		Broker:     b,
		HttpAddr:   httpListener.Addr().String(),
		GrpcAddr:   grpcListener.Addr().String(),
		DataDir:    dataDir,
		GrpcServer: grpcServer,
		HttpServer: httpServer,
		Config:     cfg,
	}
}

func (tc *TestContext) Teardown() {
	tc.GrpcServer.Stop()
	tc.HttpServer.Close()
	tc.Broker.Close()
}

// Helper to create a gRPC client
func (tc *TestContext) NewGrpcClient(t *testing.T) (pb.PulseServiceClient, *grpc.ClientConn) {
	conn, err := grpc.NewClient(tc.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to gRPC: %v", err)
	}
	return pb.NewPulseServiceClient(conn), conn
}

// --- SCENARIO 1: Pure gRPC Flow (Publish & Stream Consume) ---
func TestE2E_GrpcFlow(t *testing.T) {
	tc := setupServer(t, "")
	defer tc.Teardown()

	client, conn := tc.NewGrpcClient(t)
	defer conn.Close()

	ctx := context.Background()
	topic := "grpc-flow"

	// 1. Create Topic (via Broker directly or API, let's use Broker for speed, or API to be true E2E)
	// Let's use API to be true E2E
	createTopicHTTP(t, tc.HttpAddr, topic)

	// 2. Start Consumer (Streaming)
	stream, err := client.Consume(ctx, &pb.ConsumeRequest{
		Topic:        topic,
		ConsumerName: "e2e-consumer",
		Offset:       0,
	})
	if err != nil {
		t.Fatalf("Failed to start consume stream: %v", err)
	}

	// 3. Publish Messages
	go func() {
		time.Sleep(100 * time.Millisecond) // Wait for consumer to be ready (optional, but good for test stability)
		for i := 0; i < 5; i++ {
			_, err := client.Publish(ctx, &pb.PublishRequest{
				Topic:   topic,
				Payload: []byte(fmt.Sprintf("msg-%d", i)),
			})
			if err != nil {
				t.Errorf("Failed to publish: %v", err)
			}
		}
	}()

	// 4. Verify Messages Received
	for i := 0; i < 5; i++ {
		msg, err := stream.Recv()
		if err == io.EOF {
			t.Fatal("Stream closed unexpectedly")
		}
		if err != nil {
			t.Fatalf("Stream error: %v", err)
		}
		expected := fmt.Sprintf("msg-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("Expected payload %s, got %s", expected, msg.Payload)
		}
	}
}

// --- SCENARIO 2: Hybrid Flow (HTTP Publish -> gRPC Consume) ---
func TestE2E_HttpToGrpc(t *testing.T) {
	tc := setupServer(t, "")
	defer tc.Teardown()

	grpcClient, conn := tc.NewGrpcClient(t)
	defer conn.Close()

	topic := "http-to-grpc"
	createTopicHTTP(t, tc.HttpAddr, topic)

	// 1. Publish via HTTP
	httpPublish(t, tc.HttpAddr, topic, "payload-from-http")

	// 2. Consume via gRPC
	stream, err := grpcClient.Consume(context.Background(), &pb.ConsumeRequest{
		Topic:        topic,
		ConsumerName: "hybrid-consumer",
		Offset:       0,
	})
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if string(msg.Payload) != "payload-from-http" {
		t.Errorf("Unexpected payload: %s", msg.Payload)
	}
}

// --- SCENARIO 3: Hybrid Flow (gRPC Publish -> HTTP Consume) ---
func TestE2E_GrpcToHttp(t *testing.T) {
	tc := setupServer(t, "")
	defer tc.Teardown()

	grpcClient, conn := tc.NewGrpcClient(t)
	defer conn.Close()

	topic := "grpc-to-http"
	createTopicHTTP(t, tc.HttpAddr, topic)

	// 1. Publish via gRPC
	_, err := grpcClient.Publish(context.Background(), &pb.PublishRequest{
		Topic:   topic,
		Payload: []byte("payload-from-grpc"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 2. Consume via HTTP
	// Need to wait a bit for flush/availability if async
	time.Sleep(50 * time.Millisecond)
	msgs := httpConsume(t, tc.HttpAddr, topic, "http-consumer")
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Payload != "payload-from-grpc" {
		t.Errorf("Unexpected payload: %s", msgs[0].Payload)
	}
}

// --- SCENARIO 4: Persistence (Restart Server) ---
func TestE2E_Persistence(t *testing.T) {
	dataDir := t.TempDir()
	topic := "persistent-topic"

	// Phase 1: Start, Create Topic, Publish
	{
		tc := setupServer(t, dataDir)
		createTopicHTTP(t, tc.HttpAddr, topic)

		client, conn := tc.NewGrpcClient(t)
		_, err := client.Publish(context.Background(), &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte("persistent-data"),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		conn.Close()

		// Wait for flush to disk
		time.Sleep(100 * time.Millisecond)
		tc.Teardown()
	}

	// Phase 2: Restart Server, Consume
	{
		tc := setupServer(t, dataDir)
		defer tc.Teardown()

		client, conn := tc.NewGrpcClient(t)
		defer conn.Close()

		stream, err := client.Consume(context.Background(), &pb.ConsumeRequest{
			Topic:        topic,
			ConsumerName: "restart-consumer",
			Offset:       0,
		})
		if err != nil {
			t.Fatalf("Consume failed: %v", err)
		}

		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv failed: %v", err)
		}
		if string(msg.Payload) != "persistent-data" {
			t.Errorf("Data lost or corrupted. Got: %s", msg.Payload)
		}
	}
}

// --- Helpers ---

func createTopicHTTP(t *testing.T, addr, topic string) {
	url := fmt.Sprintf("http://%s/topic", addr)
	body := map[string]interface{}{
		"name":            topic,
		"fifo":            true,
		"retention_bytes": 1024,
		"retention_time":  1000000000000, // large enough
	}
	jsonBody, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Create topic failed: %s", resp.Status)
	}
}

func httpPublish(t *testing.T, addr, topic, payload string) {
	url := fmt.Sprintf("http://%s/publish?topic=%s", addr, topic)
	resp, err := http.Post(url, "text/plain", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("HTTP Publish failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP Publish status: %s", resp.Status)
	}
}

type httpMsg struct {
	Payload string `json:"payload"`
}

func httpConsume(t *testing.T, addr, topic, consumer string) []httpMsg {
	url := fmt.Sprintf("http://%s/consume?topic=%s&consumer=%s", addr, topic, consumer)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("HTTP Consume failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP Consume status: %s", resp.Status)
	}

	var msgs []httpMsg
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("Failed to decode HTTP response: %v", err)
	}
	return msgs
}
