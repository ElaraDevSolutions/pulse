# Pulse Go SDK

Official Go client for [Pulse Broker](https://github.com/marcosrosa/pulse).

## Installation

```bash
go get pulse-sdk-go
```

## Configuration

The SDK looks for a `pulse.yaml` (or `pulse.yml`) file in your project root. If not found, it defaults to `localhost:5555` (HTTP) and `localhost:5556` (gRPC).

### Example `pulse.yaml`

```yaml
# Connection Settings
broker:
  host: "localhost"
  http_port: 5555
  grpc_port: 5556
  timeout_ms: 5000

# Client Defaults
client:
  id: "my-go-app"
  auto_commit: true       # Automatically commit offsets after successful processing
  max_retries: 3

# Topic Configuration
topics:
  - name: "events"
    create_if_missing: true
    config:
      fifo: false
      retention_bytes: 1073741824  # 1GB
```

## Usage

### Producer

You can send raw bytes.

```go
package main

import (
    "context"
    "log"
    "pulse-sdk-go/pulse"
)

func main() {
    // Initialize (uses pulse.yaml or defaults)
    // You can override settings: pulse.NewProducer(pulse.ConsumerOptions{Host: "10.0.0.1"})
    p, err := pulse.NewProducer()
    if err != nil {
        log.Fatal(err)
    }
    defer p.Close()

    // Send Bytes
    err = p.Send(context.Background(), "events", []byte("hello pulse"))
    if err != nil {
        log.Printf("Failed to send: %v", err)
    }
}
```

### Consumer

Use `pulse.Consumer` to register message handlers and `pulse.Run` to start the loop.

```go
package main

import (
    "context"
    "fmt"
    "pulse-sdk-go/pulse"
)

func main() {
    // Simple Consumer (uses auto_commit from config)
    pulse.Consumer("events", func(ctx context.Context, msg *pulse.Message) error {
        fmt.Printf("Received event: %s\n", string(msg.Payload))
        return nil
    })

    // Manual Commit Consumer
    // Override config params directly in the options
    opts := pulse.ConsumerOptions{
        AutoCommit: boolPtr(false),
    }
    
    pulse.Consumer("transactions", func(ctx context.Context, msg *pulse.Message) error {
        processTransaction(msg)
        
        // Manually commit offset
        if err := pulse.Commit(ctx); err != nil {
            return err
        }
        
        fmt.Printf("Processed transaction %d\n", msg.Offset)
        return nil
    }, opts)

    // Start all registered consumers (blocks forever)
    pulse.Run()
}

func boolPtr(b bool) *bool {
    return &b
}

func processTransaction(msg *pulse.Message) {
    // ...
}
```
