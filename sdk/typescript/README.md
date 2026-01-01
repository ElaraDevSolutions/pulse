# Pulse TypeScript SDK

Official TypeScript client for [Pulse Broker](https://github.com/marcosrosa/pulse).

## Installation

```bash
npm install pulse-sdk
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
  id: "my-typescript-app"
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

You can send objects (automatically serialized to JSON), strings, or raw buffers.

```typescript
import { Producer } from 'pulse-sdk';

// Initialize (uses pulse.yaml or defaults)
// You can override settings: new Producer("10.0.0.1", 9090)
const producer = new Producer();

async function main() {
  // Send JSON
  await producer.send("events", { type: "user_created", id: 123 });

  // Send String
  await producer.send("logs", "raw log line");

  // Send Buffer
  await producer.send("logs", Buffer.from("binary data"));

  producer.close();
}

main();
```

### Consumer

Use the `consumer` function to register message handlers, and `run` to start the loop.

```typescript
import { consumer, run, commit, Message } from 'pulse-sdk';

// Simple Consumer (uses auto_commit from config)
consumer("events", async (msg: Message) => {
  console.log(`Received event:`, msg.payload);
  // msg.payload is an object if JSON, string if String, else Buffer
});

// Manual Commit Consumer
// Override config params directly in the options object
consumer("transactions", async (msg: Message) => {
  try {
    await processPayment(msg.payload);
    await commit();  // Manually commit offset
    console.log(`Processed transaction ${msg.offset}`);
  } catch (e) {
    console.error(`Failed to process: ${e}`);
  }
}, { autoCommit: false });

// Start all consumers
run();

async function processPayment(data: any) {
  // ... implementation
}
```
