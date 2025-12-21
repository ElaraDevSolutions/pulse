package topic

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"pulse/internal/logstore"
	"pulse/pkg/message"
)

// Stats holds monitoring metrics for a topic.
type Stats struct {
	MsgInCount      uint64
	MsgOutCount     uint64
	TotalLatency    int64 // Nanoseconds
	AvgLatency      time.Duration
	ThroughputIn    float64 // Msgs/sec
	ThroughputOut   float64 // Msgs/sec
	PendingMessages map[string]uint64
}

// LogStore defines the interface for the log storage.
type LogStore interface {
	Append(msg *message.Message) (uint64, error)
	Read(offset uint64, max int) ([]*message.Message, error)
	RunRetention(maxBytes int64, maxAge time.Duration) error
	Close() error
	GetGlobalOffset() uint64
}

// Topic represents a message topic.
type Topic struct {
	Name string
	FIFO bool

	RetentionBytes int64
	RetentionTime  time.Duration

	// Dir is the directory where topic data (like consumer offsets) is stored.
	Dir string

	// fifoChan is used when FIFO is true.
	fifoChan chan *message.Message

	// workerChans is used when FIFO is false (slice of writing goroutines).
	workerChans []chan *message.Message

	// log is the reference to the persistent log.
	log LogStore

	// wg is used to wait for goroutines to finish.
	wg sync.WaitGroup

	// stopChan signals background routines to stop
	stopChan chan struct{}

	// consumerOffsets maps consumerID to their last committed offset.
	consumerOffsets map[string]uint64
	offsetsMu       sync.RWMutex

	// Metrics
	msgInCount   atomic.Uint64
	msgOutCount  atomic.Uint64
	totalLatency atomic.Int64 // Nanoseconds
	startTime    time.Time
}

// NewTopic creates a new Topic and initializes its channels and goroutines.
func NewTopic(name string, fifo bool, retentionBytes int64, retentionTime time.Duration, log *logstore.AppendOnlyLog, dir string) *Topic {
	t := &Topic{
		Name:            name,
		FIFO:            fifo,
		RetentionBytes:  retentionBytes,
		RetentionTime:   retentionTime,
		Dir:             dir,
		log:             log,
		stopChan:       make(chan struct{}),
		consumerOffsets: make(map[string]uint64),
		startTime:       time.Now(),
	}

	// Load existing consumer offsets from disk
	t.loadConsumers()

	if fifo {
		// Single channel and single goroutine for FIFO
		t.fifoChan = make(chan *message.Message, 100)
		t.wg.Add(1)
		go t.runFIFO()
	} else {
		// Multiple channels/goroutines for non-FIFO (parallel writes)
		// For MVP, let's create a fixed number of workers, e.g., 5
		numWorkers := 5
		t.workerChans = make([]chan *message.Message, numWorkers)
		for i := 0; i < numWorkers; i++ {
			t.workerChans[i] = make(chan *message.Message, 100)
			t.wg.Add(1)
			go t.runWorker(i)
		}
	}

	// Start retention loop if policies are set
	if retentionBytes > 0 || retentionTime > 0 {
		t.wg.Add(1)
		go t.retentionLoop()
	}

	return t
}

// loadConsumers reads the consumers.json file to restore offsets.
func (t *Topic) loadConsumers() {
	path := filepath.Join(t.Dir, "consumers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("Error loading consumers for topic %s: %v\n", t.Name, err)
		}
		return
	}

	t.offsetsMu.Lock()
	defer t.offsetsMu.Unlock()
	if err := json.Unmarshal(data, &t.consumerOffsets); err != nil {
		fmt.Printf("Error parsing consumers for topic %s: %v\n", t.Name, err)
	}
}

// saveConsumers writes the consumer offsets to disk.
func (t *Topic) saveConsumers() error {
	t.offsetsMu.RLock()
	data, err := json.MarshalIndent(t.consumerOffsets, "", "  ")
	t.offsetsMu.RUnlock()
	if err != nil {
		return err
	}

	path := filepath.Join(t.Dir, "consumers.json")
	return os.WriteFile(path, data, 0644)
}

// RegisterConsumer initializes a consumer with offset 0 if it doesn't exist.
func (t *Topic) RegisterConsumer(consumerID string) {
	t.offsetsMu.Lock()
	defer t.offsetsMu.Unlock()

	if _, exists := t.consumerOffsets[consumerID]; !exists {
		t.consumerOffsets[consumerID] = 0
		// We should save immediately to persist the registration
		// In a high-throughput scenario, we might want to batch saves or save async.
		// For MVP, sync save is fine.
		
		// Internal save logic to avoid deadlock
		data, _ := json.MarshalIndent(t.consumerOffsets, "", "  ")
		path := filepath.Join(t.Dir, "consumers.json")
		os.WriteFile(path, data, 0644)
	}
}

// CommitOffset updates the offset for a consumer and persists it.
func (t *Topic) CommitOffset(consumerID string, offset uint64) error {
	t.offsetsMu.Lock()
	defer t.offsetsMu.Unlock()

	t.consumerOffsets[consumerID] = offset
	
	// Persist to disk
	data, err := json.MarshalIndent(t.consumerOffsets, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(t.Dir, "consumers.json")
	return os.WriteFile(path, data, 0644)
}

// ReadForConsumer reads messages starting from the consumer's last committed offset.
func (t *Topic) ReadForConsumer(consumerID string, max int) ([]*message.Message, error) {
	t.offsetsMu.RLock()
	offset, exists := t.consumerOffsets[consumerID]
	t.offsetsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("consumer %s not registered", consumerID)
	}

	msgs, err := t.log.Read(offset, max)
	if err != nil {
		return nil, err
	}

	// Update metrics
	if len(msgs) > 0 {
		t.msgOutCount.Add(uint64(len(msgs)))
		now := time.Now().UnixNano()
		var totalLat int64
		for _, msg := range msgs {
			lat := now - msg.Timestamp
			if lat > 0 {
				totalLat += lat
			}
		}
		t.totalLatency.Add(totalLat)
	}

	return msgs, nil
}

// runFIFO handles messages sequentially.
func (t *Topic) runFIFO() {
	defer t.wg.Done()
	for msg := range t.fifoChan {
		if _, err := t.log.Append(msg); err != nil {
			fmt.Printf("Error appending message to topic %s: %v\n", t.Name, err)
		}
	}
}

// runWorker handles messages in parallel.
func (t *Topic) runWorker(id int) {
	defer t.wg.Done()
	for msg := range t.workerChans[id] {
		if _, err := t.log.Append(msg); err != nil {
			fmt.Printf("Error appending message to topic %s (worker %d): %v\n", t.Name, id, err)
		}
	}
}

// Publish sends a message to the topic.
func (t *Topic) Publish(msg *message.Message) {
	t.msgInCount.Add(1)
	if t.FIFO {
		t.fifoChan <- msg
	} else {
		// Round-robin distribution for non-FIFO
		workerID := uint64(msg.Timestamp) % uint64(len(t.workerChans))
		t.workerChans[workerID] <- msg
	}
}

// retentionLoop periodically checks for expired segments.
func (t *Topic) retentionLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-t.stopChan:
			return
		case <-ticker.C:
			if err := t.log.RunRetention(t.RetentionBytes, t.RetentionTime); err != nil {
				fmt.Printf("Error running retention for topic %s: %v\n", t.Name, err)
			}
		}
	}
}

// Close stops the topic and waits for all goroutines to finish.
func (t *Topic) Close() {
	close(t.stopChan) // Signal retention loop to stop

	if t.FIFO {
		close(t.fifoChan)
	} else {
		for _, ch := range t.workerChans {
			close(ch)
		}
	}
	t.wg.Wait()
	
	// Final save of consumers
	t.saveConsumers()

	// Close log to ensure all buffered messages are flushed
	if err := t.log.Close(); err != nil {
		fmt.Printf("Error closing log for topic %s: %v\n", t.Name, err)
	}
}

// Read retrieves messages from the log starting at the given offset (Low-level read).
func (t *Topic) Read(offset uint64, max int) ([]*message.Message, error) {
	msgs, err := t.log.Read(offset, max)
	if err != nil {
		return nil, err
	}

	// Update metrics
	if len(msgs) > 0 {
		t.msgOutCount.Add(uint64(len(msgs)))
		now := time.Now().UnixNano()
		var totalLat int64
		for _, msg := range msgs {
			lat := now - msg.Timestamp
			if lat > 0 {
				totalLat += lat
			}
		}
		t.totalLatency.Add(totalLat)
	}

	return msgs, nil
}

// GetStats returns the current metrics for the topic.
func (t *Topic) GetStats() Stats {
	msgIn := t.msgInCount.Load()
	msgOut := t.msgOutCount.Load()
	totalLat := t.totalLatency.Load()
	
	duration := time.Since(t.startTime).Seconds()
	if duration == 0 {
		duration = 1
	}

	var avgLat time.Duration
	if msgOut > 0 {
		avgLat = time.Duration(totalLat / int64(msgOut))
	}

	// Calculate pending messages per consumer
	t.offsetsMu.RLock()
	pending := make(map[string]uint64)
	globalOffset := t.log.GetGlobalOffset()
	for id, offset := range t.consumerOffsets {
		if globalOffset >= offset {
			pending[id] = globalOffset - offset
		} else {
			pending[id] = 0
		}
	}
	t.offsetsMu.RUnlock()

	return Stats{
		MsgInCount:      msgIn,
		MsgOutCount:     msgOut,
		TotalLatency:    totalLat,
		AvgLatency:      avgLat,
		ThroughputIn:    float64(msgIn) / duration,
		ThroughputOut:   float64(msgOut) / duration,
		PendingMessages: pending,
	}
}
