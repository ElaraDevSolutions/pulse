package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulse/internal/broker"
	"pulse/internal/config"
)

func setupBrokerAndServer(t *testing.T) (*broker.Broker, *Server) {
	dir := t.TempDir()
	cfg := config.NewDefault()
	cfg.DataDir = dir
	// Make flush aggressive for tests so writes are visible quickly
	cfg.DefaultFlushThreshold = 1
	cfg.DefaultFlushInterval = 1 * time.Millisecond

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	s := NewServer(b)
	return b, s
}

func TestTopicsAndMessageEndpoints(t *testing.T) {
	b, s := setupBrokerAndServer(t)
	defer b.Close()

	// Create topic and produce
	if err := b.CreateTopic("t1", true, 0, 0, 1, time.Millisecond, 1024); err != nil {
		t.Fatalf("create topic failed: %v", err)
	}
	if err := b.Produce("t1", []byte("hello"), nil); err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	ts := httptest.NewServer(s.Routes())
	defer ts.Close()

	// /topics
	resp, err := http.Get(ts.URL + "/topics")
	if err != nil {
		t.Fatalf("topics get failed: %v", err)
	}
	defer resp.Body.Close()
	var topics []string
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		t.Fatalf("decode topics failed: %v", err)
	}
	found := false
	for _, n := range topics {
		if n == "t1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic t1 not listed")
	}

	// /topic/message?topic=t1&offset=0
	resp2, err := http.Get(ts.URL + "/topic/message?topic=t1&offset=0")
	if err != nil {
		t.Fatalf("topic message get failed: %v", err)
	}
	defer resp2.Body.Close()
	var msg struct {
		Offset    uint64            `json:"offset"`
		Timestamp int64             `json:"timestamp"`
		Payload   string            `json:"payload"`
		Headers   map[string]string `json:"headers"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&msg); err != nil {
		t.Fatalf("decode message failed: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(msg.Payload)
	if err != nil {
		t.Fatalf("base64 decode failed: %v", err)
	}
	if string(decoded) != "hello" {
		t.Fatalf("expected payload 'hello', got '%s'", string(decoded))
	}
}

func TestServer_CreatePublishConsumeStats(t *testing.T) {
	b, s := setupBrokerAndServer(t)
	defer b.Close()

	// Create topic via API
	reqBody := CreateTopicRequest{
		Name: "api-topic",
		FIFO: true,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/topic", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", w.Code)
	}

	// Publish via API
	req = httptest.NewRequest("POST", "/publish?topic=api-topic", bytes.NewBufferString("hello-api"))
	w = httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("publish failed: %d", w.Code)
	}

	// Allow small delay
	time.Sleep(10 * time.Millisecond)

	// Consume via API
	req = httptest.NewRequest("GET", "/consume?topic=api-topic&consumer=c1", nil)
	w = httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("consume failed: %d", w.Code)
	}
	var msgs []struct {
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(w.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode consume failed: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Payload != "hello-api" {
		t.Fatalf("unexpected consume response: %v", msgs)
	}

	// Stats
	req = httptest.NewRequest("GET", "/stats?topic=api-topic", nil)
	w = httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("stats failed: %d", w.Code)
	}
}
