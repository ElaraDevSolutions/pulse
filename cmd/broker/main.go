package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"pulse/internal/api"
	"pulse/internal/broker"
	"pulse/internal/config"
	internalgrpc "pulse/internal/grpc"
	pb "pulse/pkg/proto"
)

func main() {
	// Initialize Config with defaults
	cfg := config.NewDefault()

	// Server Flags
	flag.IntVar(&cfg.Port, "port", cfg.Port, "Port to listen on (HTTP)")
	flag.IntVar(&cfg.GRPCPort, "grpc-port", cfg.GRPCPort, "Port to listen on (gRPC)")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Directory to store data")

	// Topic Default Flags
	flag.Int64Var(&cfg.DefaultRetentionBytes, "default-retention-bytes", cfg.DefaultRetentionBytes, "Default retention in bytes")
	flag.DurationVar(&cfg.DefaultRetentionTime, "default-retention-time", cfg.DefaultRetentionTime, "Default retention time")
	flag.Int64Var(&cfg.DefaultSegmentSize, "default-segment-size", cfg.DefaultSegmentSize, "Default segment size in bytes")
	flag.IntVar(&cfg.DefaultFlushThreshold, "default-flush-threshold", cfg.DefaultFlushThreshold, "Default flush threshold (messages)")
	flag.DurationVar(&cfg.DefaultFlushInterval, "default-flush-interval", cfg.DefaultFlushInterval, "Default flush interval")

	// Performance Flags
	flag.IntVar(&cfg.NumWorkers, "num-workers", cfg.NumWorkers, "Number of background workers for non-FIFO topics")
	flag.IntVar(&cfg.FIFOChanSize, "fifo-chan-size", cfg.FIFOChanSize, "Buffer size for FIFO topic channels")
	flag.IntVar(&cfg.WorkerChanSize, "worker-chan-size", cfg.WorkerChanSize, "Buffer size for worker channels")
	flag.DurationVar(&cfg.RetentionCheckInterval, "retention-check-interval", cfg.RetentionCheckInterval, "How often to check for expired segments")

	// API Flags
	flag.IntVar(&cfg.DefaultMaxConsume, "default-max-consume", cfg.DefaultMaxConsume, "Default number of messages to return if 'max' is not specified")

	flag.Parse()

	fmt.Printf("Pulse Broker starting...\n")
	fmt.Printf("HTTP API: port %d\n", cfg.Port)
	fmt.Printf("gRPC API: port %d\n", cfg.GRPCPort)
	fmt.Printf("Data directory: %s\n", cfg.DataDir)

	// Initialize Broker
	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("Failed to init broker: %v", err)
	}
	defer b.Close()

	// Initialize API Server
	server := api.NewServer(b)

	// Start HTTP Server
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: server.Routes(),
	}

	// Start gRPC Server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterPulseServiceServer(grpcServer, internalgrpc.NewServer(b))

	fmt.Printf("Pulse Broker starting on HTTP port %d and gRPC port %d...\n", cfg.Port, cfg.GRPCPort)

	// Run servers in goroutines
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	fmt.Println("Broker is ready to accept connections.")

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutting down...")
	grpcServer.GracefulStop()
	httpServer.Close()
}
