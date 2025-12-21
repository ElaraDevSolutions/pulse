package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"pulse/internal/api"
	"pulse/internal/broker"
)

func main() {
	// Configuration Flags
	port := flag.Int("port", 5555, "Port to listen on")
	dataDir := flag.String("data-dir", "./data", "Directory to store data")
	
	// Default Topic Configs (used when creating topics via API if not specified, 
	// though currently API handles defaults. These could be used to override API defaults if we wanted)
	// For now, we just expose them as info or use them if we implemented a "default topic config" feature.
	
	flag.Parse()

	fmt.Printf("Pulse Broker starting on port %d...\n", *port)
	fmt.Printf("Data directory: %s\n", *dataDir)

	// Initialize Broker
	b, err := broker.New(*dataDir)
	if err != nil {
		log.Fatalf("Failed to init broker: %v", err)
	}
	defer b.Close()

	// Initialize API Server
	server := api.NewServer(b)

	// Start HTTP Server
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: server.Routes(),
	}

	// Graceful Shutdown
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	fmt.Println("Broker is ready to accept connections.")

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutting down...")
}
