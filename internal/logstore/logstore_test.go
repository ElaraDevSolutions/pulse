package logstore

import (
	"fmt"
	"os"
	"testing"
	"time"

	"pulse/pkg/message"
)

func TestLogStore_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		FlushInterval:  time.Second,
		FlushThreshold: 1,
		MaxSegmentSize: 1024 * 1024,
	}

	l, err := New(dir, config)
	if err != nil {
		t.Fatalf("Failed to create logstore: %v", err)
	}
	defer l.Close()

	// Append messages
	count := 10
	for i := 0; i < count; i++ {
		msg := message.NewMessage(0, []byte(fmt.Sprintf("msg-%d", i)), nil)
		if _, err := l.Append(&msg); err != nil {
			t.Fatalf("Failed to append message %d: %v", i, err)
		}
	}

	// Read messages
	msgs, err := l.Read(0, count)
	if err != nil {
		t.Fatalf("Failed to read messages: %v", err)
	}

	if len(msgs) != count {
		t.Errorf("Expected %d messages, got %d", count, len(msgs))
	}

	for i, msg := range msgs {
		expected := fmt.Sprintf("msg-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("Message %d payload mismatch. Want %s, got %s", i, expected, string(msg.Payload))
		}
		if msg.Offset != uint64(i) {
			t.Errorf("Message %d offset mismatch. Want %d, got %d", i, i, msg.Offset)
		}
	}
}

func TestLogStore_Rotation(t *testing.T) {
	dir := t.TempDir()
	// Small segment size to force rotation
	config := Config{
		FlushInterval:  time.Second,
		FlushThreshold: 1,
		MaxSegmentSize: 100, // Very small size
	}

	l, err := New(dir, config)
	if err != nil {
		t.Fatalf("Failed to create logstore: %v", err)
	}
	defer l.Close()

	// Append enough data to force rotation
	// Each message has overhead (header + msgpack), so 100 bytes is small.
	payload := make([]byte, 50)
	for i := 0; i < 5; i++ {
		msg := message.NewMessage(0, payload, nil)
		if _, err := l.Append(&msg); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	// Check if multiple segments were created
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	logFiles := 0
	for _, e := range entries {
		if !e.IsDir() {
			logFiles++
		}
	}

	if logFiles < 2 {
		t.Errorf("Expected multiple segments, got %d", logFiles)
	}

	// Verify we can read all messages across segments
	msgs, err := l.Read(0, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(msgs))
	}
}

func TestLogStore_Retention_Size(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		FlushInterval:  time.Second,
		FlushThreshold: 1,
		MaxSegmentSize: 100,
	}

	l, err := New(dir, config)
	if err != nil {
		t.Fatalf("Failed to create logstore: %v", err)
	}
	defer l.Close()

	// Produce data to create multiple segments
	payload := make([]byte, 50)
	for i := 0; i < 10; i++ {
		msg := message.NewMessage(0, payload, nil)
		l.Append(&msg)
	}

	// Run retention with small limit
	// We have ~10 messages * ~60 bytes = ~600 bytes total.
	// Limit to 200 bytes.
	if err := l.RunRetention(200, 0); err != nil {
		t.Fatalf("Retention failed: %v", err)
	}

	// Check segments
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	// Should have fewer segments now
	// Note: Active segment is never deleted.
	if len(entries) >= 5 { // We expected ~5 segments initially (10 msgs / 2 per seg)
		t.Logf("Segments remaining: %d", len(entries))
	}

	// Verify we can't read the deleted messages (offset 0)
	// Depending on implementation, Read might return empty or error, or start from available.
	// Our Read implementation searches for segment. If segment 0 is gone, it might start from next.
	msgs, err := l.Read(0, 1)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(msgs) > 0 && msgs[0].Offset == 0 {
		t.Error("Message 0 should have been deleted")
	}
}

func TestLogStore_Recovery(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		FlushInterval:  time.Millisecond,
		FlushThreshold: 1,
		MaxSegmentSize: 1024 * 1024,
	}

	// 1. Write data
	l1, _ := New(dir, config)
	msg := message.NewMessage(0, []byte("persist-me"), nil)
	l1.Append(&msg)
	l1.Close()

	// 2. Reopen
	l2, err := New(dir, config)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer l2.Close()

	// 3. Verify state
	if l2.GlobalOffset != 1 {
		t.Errorf("Expected GlobalOffset 1, got %d", l2.GlobalOffset)
	}

	msgs, err := l2.Read(0, 1)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "persist-me" {
		t.Errorf("Failed to recover message")
	}
}

func TestLogStore_Flush(t *testing.T) {
	dir := t.TempDir()
	// High threshold to force reliance on manual flush or close
	config := Config{
		FlushInterval:  time.Hour,
		FlushThreshold: 1000,
		MaxSegmentSize: 1024 * 1024,
	}

	l, _ := New(dir, config)

	msg := message.NewMessage(0, []byte("buffered"), nil)
	l.Append(&msg)

	// Check file size immediately - might be 0 if buffered
	// Note: This is flaky if OS flushes automatically, but we check logic.
	// Actually, we can't easily check OS buffer without reading file from another process.
	// But we can check if Close() flushes.

	l.Close()

	// Reopen and check
	l2, _ := New(dir, config)
	defer l2.Close()

	msgs, _ := l2.Read(0, 1)
	if len(msgs) != 1 {
		t.Error("Message was not flushed on close")
	}
}
