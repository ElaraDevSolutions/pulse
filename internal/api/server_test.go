package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pulse/internal/broker"
)

func setupServer(t *testing.T) (*Server, string) {
	dir := t.TempDir()
	b, err := broker.New(dir)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}
	return NewServer(b), dir
}

func TestServer_CreateTopic(t *testing.T) {
	s, _ := setupServer(t)
	defer s.Broker.Close()

	reqBody := CreateTopicRequest{
		Name: "api-topic",
		FIFO: true,
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/topic", bytes.NewReader(body))
	w := httptest.NewRecorder()

	s.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201 Created, got %d", w.Code)
	}
}

func TestServer_PublishConsume(t *testing.T) {
	s, _ := setupServer(t)
	defer s.Broker.Close()

	// Create topic first
	s.Broker.CreateTopic("api-topic", true, 0, 0, 1, time.Millisecond, 1024)

	// Publish
	req := httptest.NewRequest("POST", "/publish?topic=api-topic", bytes.NewBufferString("hello-api"))
	w := httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Publish failed: %d", w.Code)
	}

	time.Sleep(10 * time.Millisecond)

	// Consume
	req = httptest.NewRequest("GET", "/consume?topic=api-topic&consumer=c1", nil)
	w = httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Consume failed: %d", w.Code)
	}

	var msgs []struct {
		Payload string `json:"payload"`
	}
	json.NewDecoder(w.Body).Decode(&msgs)

	if len(msgs) != 1 || msgs[0].Payload != "hello-api" {
		t.Errorf("Unexpected response: %v", msgs)
	}
}

func TestServer_Stats(t *testing.T) {
	s, _ := setupServer(t)
	defer s.Broker.Close()

	s.Broker.CreateTopic("stats-topic", true, 0, 0, 1, time.Millisecond, 1024)

	req := httptest.NewRequest("GET", "/stats?topic=stats-topic", nil)
	w := httptest.NewRecorder()
	s.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Stats failed: %d", w.Code)
	}
}
