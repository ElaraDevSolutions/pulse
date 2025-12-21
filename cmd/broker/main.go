package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"pulse/internal/broker"
)

func main() {
	// Clean up previous data for this demo
	os.RemoveAll("./data")

	fmt.Println("Pulse Broker starting...")

	// Initialize Broker
	b, err := broker.New("./data")
	if err != nil {
		log.Fatalf("Failed to init broker: %v", err)
	}
	defer b.Close()

	// Create Topic with retention (e.g., 1MB max size, 24h max age)
	// Flush every 10 messages or every 100ms
	topicName := "events"
	if err := b.CreateTopic(topicName, true, 1024*1024, 24*time.Hour, 10, 100*time.Millisecond); err != nil {
		log.Printf("Note: %v", err)
	}

	// Produce 5 messages
	fmt.Println("Producing 5 messages...")
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf("message %d", i))
		if err := b.Produce(topicName, payload); err != nil {
			log.Printf("Failed to produce message %d: %v", i, err)
		}
	}

	// Wait for async processing
	time.Sleep(500 * time.Millisecond)

	// --- Consumer A ---
	fmt.Println("\n--- Consumer A (Reading first 5) ---")
	consumerA := "consumer-A"
	b.RegisterConsumer(topicName, consumerA)

	msgsA, err := b.ReadForConsumer(topicName, consumerA, 10)
	if err != nil {
		log.Fatalf("Failed to read for consumer A: %v", err)
	}

	var lastOffsetA uint64
	for _, msg := range msgsA {
		fmt.Printf("[Consumer A] Offset: %d, Payload: %s\n", msg.Offset, string(msg.Payload))
		lastOffsetA = msg.Offset
	}

	// Commit offset for A (next message to read)
	if len(msgsA) > 0 {
		newOffset := lastOffsetA + 1
		fmt.Printf("[Consumer A] Committing offset: %d\n", newOffset)
		b.CommitOffset(topicName, consumerA, newOffset)
	}

	// Produce 3 more messages
	fmt.Println("\nProducing 3 more messages...")
	for i := 5; i < 8; i++ {
		payload := []byte(fmt.Sprintf("message %d", i))
		if err := b.Produce(topicName, payload); err != nil {
			log.Printf("Failed to produce message %d: %v", i, err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// --- Consumer A (Reading new messages) ---
	fmt.Println("\n--- Consumer A (Reading new messages) ---")
	msgsA2, err := b.ReadForConsumer(topicName, consumerA, 10)
	if err != nil {
		log.Fatalf("Failed to read for consumer A: %v", err)
	}
	for _, msg := range msgsA2 {
		fmt.Printf("[Consumer A] Offset: %d, Payload: %s\n", msg.Offset, string(msg.Payload))
	}

	// --- Consumer B (Reading from start) ---
	fmt.Println("\n--- Consumer B (Reading from start) ---")
	consumerB := "consumer-B"
	b.RegisterConsumer(topicName, consumerB)

	msgsB, err := b.ReadForConsumer(topicName, consumerB, 100)
	if err != nil {
		log.Fatalf("Failed to read for consumer B: %v", err)
	}
	for _, msg := range msgsB {
		fmt.Printf("[Consumer B] Offset: %d, Payload: %s\n", msg.Offset, string(msg.Payload))
	}
}
