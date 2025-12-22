# Pulse Python SDK

Official Python client for [Pulse Broker](https://github.com/marcosrosa/pulse).

## Installation

```bash
pip install pulse-broker
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
  id: "my-python-app"
  auto_commit: true       # Automatically commit offsets after successful processing
  max_retries: 3

# Topic Configuration
topics:
  - name: "events"
    create_if_missing: true
    config:
      fifo: false
      retention_bytes: 1073741824  # 1GB
    consume:
      auto_commit: true

  - name: "transactions"
    create_if_missing: true
    config:
      fifo: true
    consume:
      auto_commit: false  # Manual commit required

  - name: "logs"
    create_if_missing: true
    config:
      fifo: false
    consume:
      auto_commit: true
```

## Usage

### Producer

You can send dictionaries (automatically serialized to JSON) or raw bytes.

```python
from pulse import Producer

# Initialize (uses pulse.yaml or defaults)
# You can override settings: Producer(host="10.0.0.1", port=9090)
producer = Producer()

# Send JSON
producer.send("events", {"type": "user_created", "id": 123})

# Send Bytes
producer.send("logs", b"raw log line")

producer.close()
```

### Consumer

Use the `@consumer` decorator to register message handlers.

```python
from pulse import consumer, commit, run

# Simple Consumer (uses auto_commit from config)
@consumer("events")
def handle_event(msg):
    print(f"Received event: {msg.payload}")
    # msg.payload is a dict if JSON, else bytes

# Manual Commit Consumer
# Override config params directly in the decorator if needed
@consumer("transactions", auto_commit=False)
def handle_transaction(msg):
    try:
        process_payment(msg.payload)
        commit()  # Manually commit offset
        print(f"Processed transaction {msg.offset}")
    except Exception as e:
        print(f"Failed to process: {e}")
        # Do not commit, message will be redelivered on restart/rebalance

if __name__ == "__main__":
    print("Starting consumers...")
    run()  # Blocks and runs all registered consumers
```
