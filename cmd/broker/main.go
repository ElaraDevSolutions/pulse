package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"google.golang.org/grpc"

	"pulse/internal/api"
	"pulse/internal/broker"
	"pulse/internal/config"
	internalgrpc "pulse/internal/grpc"
	pb "pulse/pkg/proto"
)

var (
	pidFile = filepath.Join(os.TempDir(), "pulse.pid")
	logFile = filepath.Join(os.TempDir(), "pulse.log")
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

	// Custom flag for daemon mode (internal use)
	daemonMode := flag.Bool("daemon", false, "Run as daemon (internal)")

	// DEBUG: Print args
	// fmt.Printf("DEBUG: os.Args: %v\n", os.Args)

	// Handle command-line arguments to support "pulse start -flags"
	// We peek at the first argument to see if it's a command.
	command := "start" // Default command
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "start", "stop", "run":
			command = os.Args[1]
			// Remove the command from os.Args so flag.Parse() can handle flags
			// effectively shifting "pulse start -port 8080" to "pulse -port 8080"
			// for the parser, while we remember the command.
			os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
		}
	}

	flag.Parse()

	// If the command wasn't at the start, it might be at the end (e.g. "pulse -port 8080 start")
	// In that case, flag.Args() will contain it.
	if len(flag.Args()) > 0 {
		command = flag.Args()[0]
	}

	// fmt.Printf("DEBUG: command: %s, daemon: %v\n", command, *daemonMode)

	switch command {
	case "start":
		if *daemonMode {
			runServer(cfg)
		} else {
			startDaemon()
		}
	case "stop":
		stopDaemon()
	case "run":
		runServer(cfg)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Usage: pulse [start|stop|run] [flags]")
		os.Exit(1)
	}
}

func startDaemon() {
	// Check if already running
	if _, err := os.Stat(pidFile); err == nil {
		raw, _ := os.ReadFile(pidFile)
		pid, _ := strconv.Atoi(string(raw))
		process, err := os.FindProcess(pid)
		if err == nil {
			if err := process.Signal(syscall.Signal(0)); err == nil {
				fmt.Printf("Pulse is already running (PID %d)\n", pid)
				return
			}
		}
		// Stale PID file
		os.Remove(pidFile)
	}

	// Prepare command to re-run self with -daemon flag
	// We need to preserve all flags passed to the original command
	args := []string{"-daemon"}

	// Reconstruct flags from os.Args
	// This is a bit naive but works for simple cases.
	// Ideally we would iterate visited flags.
	flag.Visit(func(f *flag.Flag) {
		args = append(args, fmt.Sprintf("-%s=%s", f.Name, f.Value.String()))
	})

	// Add "start" command explicitly to match the switch case logic in the child
	args = append(args, "start")

	cmd := exec.Command(os.Args[0], args...)

	// Detach process
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Redirect output to log file
	logF, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	cmd.Stdout = logF
	cmd.Stderr = logF

	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start daemon: %v", err)
	}

	// Write PID file
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		log.Fatalf("Failed to write PID file: %v", err)
	}

	fmt.Printf("Pulse started in background (PID %d)\n", cmd.Process.Pid)
	fmt.Printf("Logs: %s\n", logFile)
}

func stopDaemon() {
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Pulse is not running (PID file not found)")
			return
		}
		log.Fatalf("Failed to read PID file: %v", err)
	}

	pid, err := strconv.Atoi(string(raw))
	if err != nil {
		log.Fatalf("Invalid PID in file: %v", err)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		fmt.Println("Pulse process not found")
		os.Remove(pidFile)
		return
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		fmt.Printf("Failed to stop process: %v\n", err)
		return
	}

	fmt.Printf("Pulse stopped (PID %d)\n", pid)
	os.Remove(pidFile)
}

func runServer(cfg *config.Config) {
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

	// Clean up PID file if we are the daemon
	// We check if the PID file contains our PID
	if raw, err := os.ReadFile(pidFile); err == nil {
		if pid, _ := strconv.Atoi(string(raw)); pid == os.Getpid() {
			os.Remove(pidFile)
		}
	}
}
