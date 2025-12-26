package api

import (
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"pulse/internal/broker"
)

//go:embed proto/pulse.proto
var protoFile string

type Server struct {
	Broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{Broker: b}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/consume", s.handleConsume)
	mux.HandleFunc("/commit", s.handleCommit)
	mux.HandleFunc("/topic", s.handleTopic)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/proto", s.handleProto)
	return mux
}

func (s *Server) handleProto(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(protoFile))
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic parameter", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	if err := s.Broker.Produce(topic, body, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message published"))
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	consumer := r.URL.Query().Get("consumer")
	maxStr := r.URL.Query().Get("max")

	if topic == "" || consumer == "" {
		http.Error(w, "Missing topic or consumer parameter", http.StatusBadRequest)
		return
	}

	max := s.Broker.Config.DefaultMaxConsume
	if maxStr != "" {
		if m, err := strconv.Atoi(maxStr); err == nil {
			max = m
		}
	}

	// Ensure consumer is registered
	s.Broker.RegisterConsumer(topic, consumer)

	msgs, err := s.Broker.ReadForConsumer(topic, consumer, max)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to JSON response
	type MessageResponse struct {
		Offset    uint64 `json:"offset"`
		Payload   string `json:"payload"` // Assuming string payload for simplicity in JSON
		Timestamp int64  `json:"timestamp"`
	}

	resp := make([]MessageResponse, len(msgs))
	for i, msg := range msgs {
		resp[i] = MessageResponse{
			Offset:    msg.Offset,
			Payload:   string(msg.Payload),
			Timestamp: msg.Timestamp,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	consumer := r.URL.Query().Get("consumer")
	offsetStr := r.URL.Query().Get("offset")

	if topic == "" || consumer == "" || offsetStr == "" {
		http.Error(w, "Missing parameters", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid offset", http.StatusBadRequest)
		return
	}

	if err := s.Broker.CommitOffset(topic, consumer, offset); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Offset committed"))
}

type CreateTopicRequest struct {
	Name           string `json:"name"`
	FIFO           bool   `json:"fifo"`
	RetentionBytes int64  `json:"retention_bytes"`
	RetentionTime  int64  `json:"retention_time"` // Nanoseconds
	FlushThreshold int    `json:"flush_threshold"`
	FlushInterval  int64  `json:"flush_interval"` // Nanoseconds
	SegmentSize    int64  `json:"segment_size"`
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Use defaults if 0
	if req.RetentionBytes == 0 {
		req.RetentionBytes = s.Broker.Config.DefaultRetentionBytes
	}
	if req.RetentionTime == 0 {
		req.RetentionTime = int64(s.Broker.Config.DefaultRetentionTime)
	}
	if req.SegmentSize == 0 {
		req.SegmentSize = s.Broker.Config.DefaultSegmentSize
	}
	if req.FlushThreshold == 0 {
		req.FlushThreshold = s.Broker.Config.DefaultFlushThreshold
	}
	if req.FlushInterval == 0 {
		req.FlushInterval = int64(s.Broker.Config.DefaultFlushInterval)
	}

	err := s.Broker.CreateTopic(req.Name, req.FIFO, req.RetentionBytes, time.Duration(req.RetentionTime), req.FlushThreshold, time.Duration(req.FlushInterval), req.SegmentSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Topic created"))
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic parameter", http.StatusBadRequest)
		return
	}

	stats, err := s.Broker.GetTopicStats(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
