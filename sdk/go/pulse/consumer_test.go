package pulse

import (
"context"
"testing"
)

func TestConsumerRegistration(t *testing.T) {
	// Clear consumers before test
	mu.Lock()
	consumers = nil
	mu.Unlock()

	handler := func(ctx context.Context, msg *Message) error {
		return nil
	}

	Consumer("test-topic", handler, ConsumerOptions{
ConsumerGroup: "test-group",
})

	mu.Lock()
	defer mu.Unlock()

	if len(consumers) != 1 {
		t.Errorf("Expected 1 consumer, got %d", len(consumers))
	}

	c := consumers[0]
	if c.topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", c.topic)
	}
	if c.group != "test-group" {
		t.Errorf("Expected group 'test-group', got '%s'", c.group)
	}
}
