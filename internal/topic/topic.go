package topic

import (
	"fmt"
	"pulse/internal/logstore"
	"pulse/pkg/message"
	"sync"
)

// LogStore defines the interface for the log storage.
type LogStore interface {
	Append(msg *message.Message) (uint64, error)
	Read(offset uint64, max int) ([]*message.Message, error)
}

// Topic represents a message topic.
type Topic struct {
	Name string
	FIFO bool

	// fifoChan is used when FIFO is true.
	fifoChan chan *message.Message

	// workerChans is used when FIFO is false (slice of writing goroutines).
	workerChans []chan *message.Message

	// log is the reference to the persistent log.
	log LogStore

	// wg is used to wait for goroutines to finish.
	wg sync.WaitGroup
}

// NewTopic creates a new Topic and initializes its channels and goroutines.
func NewTopic(name string, fifo bool, log *logstore.AppendOnlyLog) *Topic {
	t := &Topic{
		Name: name,
		FIFO: fifo,
		log:  log,
	}

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

	return t
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
	if t.FIFO {
		t.fifoChan <- msg
	} else {
		// Round-robin distribution for non-FIFO
		// Since offset is assigned by LogStore, we use a simple random or round-robin approach here.
		// For simplicity, we can use the timestamp or just random.
		// Let's use the timestamp as a simple distribution key.
		workerID := uint64(msg.Timestamp) % uint64(len(t.workerChans))
		t.workerChans[workerID] <- msg
	}
}

// Close stops the topic and waits for all goroutines to finish.
func (t *Topic) Close() {
	if t.FIFO {
		close(t.fifoChan)
	} else {
		for _, ch := range t.workerChans {
			close(ch)
		}
	}
	t.wg.Wait()
}

// Read retrieves messages from the log starting at the given offset.
func (t *Topic) Read(offset uint64, max int) ([]*message.Message, error) {
	return t.log.Read(offset, max)
}
