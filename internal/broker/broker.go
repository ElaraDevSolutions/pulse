package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"pulse/internal/config"
	"pulse/internal/logstore"
	"pulse/internal/topic"
	"pulse/pkg/message"
)

// TopicConfig represents the persistent configuration of a topic.
type TopicConfig struct {
	Name           string `json:"name"`
	FIFO           bool   `json:"fifo"`
	RetentionBytes int64  `json:"retention_bytes"` // 0 means infinite
	RetentionTime  int64  `json:"retention_time"`  // Nanoseconds, 0 means infinite
	FlushThreshold int    `json:"flush_threshold"` // Number of messages
	FlushInterval  int64  `json:"flush_interval"`  // Nanoseconds
	SegmentSize    int64  `json:"segment_size"`    // Max bytes per segment
}

// Broker manages topics and routes messages.
type Broker struct {
	DataDir string
	topics  map[string]*topic.Topic
	mu      sync.RWMutex
	Config  *config.Config
}

// ListTopics returns the list of topic names currently known to the broker.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	return names
}

// New creates a new Broker instance.
func New(cfg *config.Config) (*Broker, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	b := &Broker{
		DataDir: cfg.DataDir,
		topics:  make(map[string]*topic.Topic),
		Config:  cfg,
	}

	if err := b.restoreTopics(); err != nil {
		return nil, fmt.Errorf("failed to restore topics: %w", err)
	}

	return b, nil
}

// restoreTopics scans the data directory and restores existing topics.
func (b *Broker) restoreTopics() error {
	entries, err := os.ReadDir(b.DataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		configPath := filepath.Join(b.DataDir, topicName, "config.json")

		// Check if config exists
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			continue // Not a topic directory or corrupted
		}

		// Read config
		data, err := os.ReadFile(configPath)
		if err != nil {
			fmt.Printf("Failed to read config for topic %s: %v\n", topicName, err)
			continue
		}

		var config TopicConfig
		if err := json.Unmarshal(data, &config); err != nil {
			fmt.Printf("Failed to parse config for topic %s: %v\n", topicName, err)
			continue
		}

		// Initialize LogStore
		logConfig := logstore.Config{
			FlushInterval:  time.Duration(config.FlushInterval),
			FlushThreshold: config.FlushThreshold,
			MaxSegmentSize: config.SegmentSize,
		}
		logDir := filepath.Join(b.DataDir, topicName)
		log, err := logstore.New(logDir, logConfig)
		if err != nil {
			fmt.Printf("Failed to open log for topic %s: %v\n", topicName, err)
			continue
		}

		// Create Topic
		t := topic.NewTopic(config.Name, config.FIFO, config.RetentionBytes, time.Duration(config.RetentionTime), log, logDir, b.Config)
		b.topics[config.Name] = t
		fmt.Printf("Restored topic: %s (FIFO: %v)\n", config.Name, config.FIFO)
	}

	return nil
}

// CreateTopic creates a new topic and persists its configuration.
func (b *Broker) CreateTopic(name string, fifo bool, retentionBytes int64, retentionTime time.Duration, flushThreshold int, flushInterval time.Duration, segmentSize int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topicDir := filepath.Join(b.DataDir, name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Save config
	config := TopicConfig{
		Name:           name,
		FIFO:           fifo,
		RetentionBytes: retentionBytes,
		RetentionTime:  int64(retentionTime),
		FlushThreshold: flushThreshold,
		FlushInterval:  int64(flushInterval),
		SegmentSize:    segmentSize,
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal topic config: %w", err)
	}

	configPath := filepath.Join(topicDir, "config.json")
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write topic config: %w", err)
	}

	// Initialize LogStore
	logConfig := logstore.Config{
		FlushInterval:  flushInterval,
		FlushThreshold: flushThreshold,
		MaxSegmentSize: segmentSize,
	}
	log, err := logstore.New(topicDir, logConfig)
	if err != nil {
		return fmt.Errorf("failed to create log store: %w", err)
	}

	// Create Topic
	t := topic.NewTopic(name, fifo, retentionBytes, retentionTime, log, topicDir, b.Config)
	b.topics[name] = t

	return nil
}

// Produce receives a message payload and routes it to the specified topic.
func (b *Broker) Produce(topicName string, payload []byte, headers map[string]string) error {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	// Create message with temporary offset (will be assigned by LogStore)
	msg := message.NewMessage(0, payload, headers)

	t.Publish(&msg)
	return nil
}

// Consume reads messages from a topic starting at the given offset.
func (b *Broker) Consume(topicName string, offset uint64, max int) ([]*message.Message, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	return t.Read(offset, max)
}

// RegisterConsumer registers a consumer for a topic.
func (b *Broker) RegisterConsumer(topicName, consumerID string) error {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	t.RegisterConsumer(consumerID)
	return nil
}

// GetConsumerOffset returns the current committed offset for a consumer.
func (b *Broker) GetConsumerOffset(topicName, consumerID string) (uint64, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("topic %s does not exist", topicName)
	}

	return t.GetConsumerOffset(consumerID)
}

// CommitOffset updates the offset for a consumer.
func (b *Broker) CommitOffset(topicName, consumerID string, offset uint64) error {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	return t.CommitOffset(consumerID, offset)
}

// ReadForConsumer reads messages for a specific consumer.
func (b *Broker) ReadForConsumer(topicName, consumerID string, max int) ([]*message.Message, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	return t.ReadForConsumer(consumerID, max)
}

// GetTopicStats returns the metrics for a specific topic.
func (b *Broker) GetTopicStats(topicName string) (topic.Stats, error) {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return topic.Stats{}, fmt.Errorf("topic %s does not exist", topicName)
	}

	return t.GetStats(), nil
}

// Close shuts down the broker and all topics.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, t := range b.topics {
		t.Close()
	}
}
