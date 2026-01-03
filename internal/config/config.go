package config

import "time"

// Config holds all configuration parameters for the Pulse broker.
type Config struct {
	// Server
	Port     int
	GRPCPort int
	DataDir  string

	// Topic Defaults (used when creating topics via API if not specified)
	DefaultRetentionBytes int64
	DefaultRetentionTime  time.Duration
	DefaultSegmentSize    int64
	DefaultFlushThreshold int
	DefaultFlushInterval  time.Duration

	// Performance / Internals
	NumWorkers             int           // Number of background workers for non-FIFO topics
	FIFOChanSize           int           // Buffer size for FIFO topic channels
	WorkerChanSize         int           // Buffer size for worker channels
	RetentionCheckInterval time.Duration // How often to check for expired segments

	// API
	DefaultMaxConsume int // Default number of messages to return if 'max' is not specified
}

// NewDefault returns a Config with sensible default values.
func NewDefault() *Config {
	return &Config{
		Port:                   5555,
		GRPCPort:               5556,
		DataDir:                "./data",
		DefaultRetentionBytes:  1024 * 1024 * 1024, // 1GB
		DefaultRetentionTime:   7 * 24 * time.Hour,
		DefaultSegmentSize:     128 * 1024 * 1024, // 128MB
		DefaultFlushThreshold:  2000,              // Batch writes before syscall
		DefaultFlushInterval:   100 * time.Millisecond,
		NumWorkers:             5,
		FIFOChanSize:           100,
		WorkerChanSize:         100,
		RetentionCheckInterval: 1 * time.Minute,
		DefaultMaxConsume:      10,
	}
}
