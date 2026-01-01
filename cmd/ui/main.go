package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var (
	pulseURL    string
	port        int
	templateDir string
)

type PageData struct {
	Title string
	Data  interface{}
	Error string
}

type Message struct {
	Offset    uint64 `json:"offset"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

type TopicView struct {
	Name     string
	Messages []Message
	Stats    map[string]interface{}
}

func main() {
	flag.StringVar(&pulseURL, "pulse-url", "http://localhost:5555", "URL of the Pulse broker")
	flag.IntVar(&port, "port", 8080, "Port to run the UI on")
	flag.Parse()

	// Find template directory
	possibleDirs := []string{
		"cmd/ui/templates",
		"templates",
		"../cmd/ui/templates",
	}

	for _, dir := range possibleDirs {
		if _, err := os.Stat(filepath.Join(dir, "layout.html")); err == nil {
			templateDir = dir
			break
		}
	}

	if templateDir == "" {
		log.Fatal("Failed to find templates directory. Make sure you are running from the project root or cmd/ui directory.")
	}

	// Serve static files
	// We assume static files are in cmd/ui/static relative to project root, or ../cmd/ui/static if running from cmd/ui
	staticDir := "cmd/ui/static"
	if _, err := os.Stat(staticDir); os.IsNotExist(err) {
		staticDir = "../cmd/ui/static"
		if _, err := os.Stat(staticDir); os.IsNotExist(err) {
			staticDir = "static" // Try local static dir if running from cmd/ui
		}
	}

	fs := http.FileServer(http.Dir(staticDir))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/topic", handleTopic)
	http.HandleFunc("/create-topic", handleCreateTopic)
	http.HandleFunc("/publish", handlePublish)

	fmt.Printf("Pulse UI running on http://localhost:%d\n", port)
	fmt.Printf("Connected to Pulse at %s\n", pulseURL)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func render(w http.ResponseWriter, tmplName string, data PageData) {
	t, err := template.ParseFiles(
		filepath.Join(templateDir, "layout.html"),
		filepath.Join(templateDir, tmplName),
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("Template error: %v", err), http.StatusInternalServerError)
		return
	}

	if err := t.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	resp, err := http.Get(pulseURL + "/topics")
	if err != nil {
		render(w, "index.html", PageData{Title: "Home", Error: fmt.Sprintf("Failed to connect to Pulse: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		render(w, "index.html", PageData{Title: "Home", Error: fmt.Sprintf("Pulse returned status: %s", resp.Status)})
		return
	}

	var topics []string
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		render(w, "index.html", PageData{Title: "Home", Error: fmt.Sprintf("Failed to decode topics: %v", err)})
		return
	}

	render(w, "index.html", PageData{Title: "Home", Data: topics})
}

func handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	name := r.FormValue("name")
	fifo := r.FormValue("fifo") == "on"

	reqBody := map[string]interface{}{
		"name": name,
		"fifo": fifo,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := http.Post(pulseURL+"/topic", "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		render(w, "index.html", PageData{Title: "Home", Error: fmt.Sprintf("Failed to create topic: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		render(w, "index.html", PageData{Title: "Home", Error: fmt.Sprintf("Failed to create topic: %s", string(body))})
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func handleTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("name")
	if topicName == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	// Get Messages
	// We use a generic consumer ID "pulse-ui-viewer" to view messages.
	// Note: This might affect offset for "pulse-ui-viewer" consumer group if the broker tracks it.
	resp, err := http.Get(fmt.Sprintf("%s/consume?topic=%s&consumer=pulse-ui-viewer&max=50", pulseURL, topicName))
	if err != nil {
		render(w, "topic.html", PageData{Title: topicName, Error: fmt.Sprintf("Failed to fetch messages: %v", err)})
		return
	}
	defer resp.Body.Close()

	var messages []Message
	if resp.StatusCode == http.StatusOK {
		json.NewDecoder(resp.Body).Decode(&messages)
	}

	// Get Stats
	var stats map[string]interface{}
	statsResp, err := http.Get(fmt.Sprintf("%s/stats?topic=%s", pulseURL, topicName))
	if err == nil && statsResp.StatusCode == http.StatusOK {
		json.NewDecoder(statsResp.Body).Decode(&stats)
		statsResp.Body.Close()
	}

	render(w, "topic.html", PageData{Title: topicName, Data: TopicView{
		Name:     topicName,
		Messages: messages,
		Stats:    stats,
	}})
}

func handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	topic := r.FormValue("topic")
	payload := r.FormValue("payload")

	resp, err := http.Post(fmt.Sprintf("%s/publish?topic=%s", pulseURL, topic), "text/plain", bytes.NewBufferString(payload))
	if err != nil {
		render(w, "topic.html", PageData{Title: topic, Error: fmt.Sprintf("Failed to publish: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		render(w, "topic.html", PageData{Title: topic, Error: fmt.Sprintf("Failed to publish: %s", string(body))})
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/topic?name=%s", topic), http.StatusSeeOther)
}
