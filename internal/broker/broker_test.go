package broker

import (
	"testing"
	"time"

	"pulse/internal/config"
)

func TestBroker_CreateTopic(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDefault()
	cfg.DataDir = dir
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	err = b.CreateTopic("t1", true, 100, time.Hour, 10, time.Second, 1024)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Duplicate creation
	err = b.CreateTopic("t1", true, 100, time.Hour, 10, time.Second, 1024)
	if err == nil {
		t.Error("Expected error on duplicate topic creation")
	}
}

func TestBroker_ProduceConsume(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDefault()
	cfg.DataDir = dir
	b, _ := New(cfg)
	defer b.Close()

	b.CreateTopic("t1", true, 0, 0, 1, time.Millisecond, 1024)

	// Produce
	if err := b.Produce("t1", []byte("hello")); err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	// Produce to non-existent
	if err := b.Produce("t2", []byte("fail")); err == nil {
		t.Error("Expected error producing to non-existent topic")
	}

	// Consume
	time.Sleep(10 * time.Millisecond)
	msgs, err := b.Consume("t1", 0, 10)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}
}

func TestBroker_Persistence(t *testing.T) {
	dir := t.TempDir()

	// 1. Setup
	cfg1 := config.NewDefault()
	cfg1.DataDir = dir
	b1, _ := New(cfg1)
	b1.CreateTopic("persistent", true, 100, 0, 1, time.Second, 1024)
	b1.Produce("persistent", []byte("data"))
	time.Sleep(10 * time.Millisecond) // Wait for flush
	b1.Close()

	// 2. Restore
	cfg2 := config.NewDefault()
	cfg2.DataDir = dir
	b2, err := New(cfg2)
	if err != nil {
		t.Fatalf("Failed to restore broker: %v", err)
	}
	defer b2.Close()

	// Check topic exists
	if _, err := b2.GetTopicStats("persistent"); err != nil {
		t.Error("Topic not restored")
	}

	// Check data
	msgs, _ := b2.Consume("persistent", 0, 10)
	if len(msgs) != 1 {
		t.Error("Data not restored")
	}
}
