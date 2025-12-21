package topic

import (
	"fmt"
	"testing"
	"time"

	"pulse/internal/logstore"
	"pulse/pkg/message"
)

// MockLogStore for testing Topic without disk I/O
type MockLogStore struct {
	msgs []*message.Message
}

func (m *MockLogStore) Append(msg *message.Message) (uint64, error) {
	msg.Offset = uint64(len(m.msgs))
	m.msgs = append(m.msgs, msg)
	return msg.Offset, nil
}

func (m *MockLogStore) Read(offset uint64, max int) ([]*message.Message, error) {
	if offset >= uint64(len(m.msgs)) {
		return []*message.Message{}, nil
	}
	end := offset + uint64(max)
	if end > uint64(len(m.msgs)) {
		end = uint64(len(m.msgs))
	}
	return m.msgs[offset:end], nil
}

func (m *MockLogStore) RunRetention(maxBytes int64, maxAge time.Duration) error {
	return nil
}

func (m *MockLogStore) Close() error {
	return nil
}

func (m *MockLogStore) GetGlobalOffset() uint64 {
	return uint64(len(m.msgs))
}

func TestTopic_FIFO(t *testing.T) {
	dir := t.TempDir()
	// Use real logstore for integration, or mock?
	// Let's use real logstore to test full stack of topic
	logConfig := logstore.Config{
		FlushInterval:  time.Millisecond,
		FlushThreshold: 1,
		MaxSegmentSize: 1024 * 1024,
	}
	log, _ := logstore.New(dir, logConfig)
	
	topic := NewTopic("test-fifo", true, 0, 0, log, dir)
	defer topic.Close()

	// Publish messages
	count := 100
	for i := 0; i < count; i++ {
		msg := message.NewMessage(0, []byte(fmt.Sprintf("msg-%d", i)))
		topic.Publish(&msg)
	}

	// Wait for processing (async)
	// With FlushThreshold=1, this involves 100 fsyncs, which can be slow.
	time.Sleep(1 * time.Second)

	// Verify order
	msgs, err := topic.Read(0, count)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(msgs) != count {
		t.Errorf("Expected %d messages, got %d", count, len(msgs))
	}

	for i, msg := range msgs {
		if msg.Offset != uint64(i) {
			t.Errorf("Order mismatch at %d", i)
		}
	}
}

func TestTopic_Consumers(t *testing.T) {
	dir := t.TempDir()
	logConfig := logstore.Config{FlushThreshold: 1}
	log, _ := logstore.New(dir, logConfig)
	
	topic := NewTopic("test-consumers", true, 0, 0, log, dir)
	defer topic.Close()

	// Register consumer
	cid := "c1"
	topic.RegisterConsumer(cid)

	// Publish
	topic.Publish(&message.Message{Payload: []byte("m1")})
	time.Sleep(10 * time.Millisecond)

	// Read
	msgs, err := topic.ReadForConsumer(cid, 10)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message")
	}

	// Commit
	topic.CommitOffset(cid, 1)

	// Verify persistence of consumer offset
	// We can check internal state or reload topic
	// Let's reload topic
	topic.Close()
	
	// Reopen
	log2, _ := logstore.New(dir, logConfig)
	topic2 := NewTopic("test-consumers", true, 0, 0, log2, dir)
	defer topic2.Close()

	// Check stats or read again
	stats := topic2.GetStats()
	if pending, ok := stats.PendingMessages[cid]; !ok {
		t.Error("Consumer not restored")
	} else if pending != 0 {
		// Offset 1, Global 1 -> Pending 0
		t.Errorf("Expected 0 pending, got %d", pending)
	}
}

func TestTopic_Metrics(t *testing.T) {
	dir := t.TempDir()
	logConfig := logstore.Config{FlushThreshold: 1}
	log, _ := logstore.New(dir, logConfig)
	
	topic := NewTopic("test-metrics", true, 0, 0, log, dir)
	defer topic.Close()

	topic.Publish(&message.Message{Payload: []byte("m1")})
	time.Sleep(10 * time.Millisecond)

	stats := topic.GetStats()
	if stats.MsgInCount != 1 {
		t.Errorf("Expected 1 MsgIn, got %d", stats.MsgInCount)
	}
	if stats.MsgOutCount != 0 {
		t.Errorf("Expected 0 MsgOut, got %d", stats.MsgOutCount)
	}

	// Read
	topic.Read(0, 1)
	stats = topic.GetStats()
	if stats.MsgOutCount != 1 {
		t.Errorf("Expected 1 MsgOut, got %d", stats.MsgOutCount)
	}
}
