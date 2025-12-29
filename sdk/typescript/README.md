# Pulse SDK — TypeScript

TypeScript client for Pulse Broker (gRPC). This README is written for end users and explains installation, configuration, and basic usage with practical examples.

## Installation

Install from npm (published package):

```bash
npm install pulse-sdk
```

Or install locally from the repository (for development):

```bash
npm install
```

To compile the TypeScript sources (optional for development/CI):

```bash
npm run build
```

Run the lightweight integration tests (a test gRPC server is started automatically):

```bash
npm test
```

## Configuration

The SDK supports configuration via (in priority order):

1. Programmatic: pass a `PulseConfig` object when creating `Producer` or `Consumer`.
2. Environment variables: `PULSE_GRPC_URL`, `PULSE_EVENT_TYPES` (comma-separated), `PULSE_CONSUMER_NAME`.
3. File: a `pulse.yml` or `pulse.yaml` file placed at the process working directory or in `~/.pulse/pulse.yml`.

The SDK exposes two helpers:

- `loadConfig(configPath?: string): PulseConfig` — reads `pulse.yml` / env and returns a merged config object.
- `initFromConfig(cfg: PulseConfig): Promise<void>` — calls the broker `CreateTopic` RPC for every topic declared in `cfg.topics`. This lets the SDK create any required topics automatically at startup (useful for tests or first-run).

Example `pulse.yml`:

```yaml
grpcUrl: localhost:5556
eventTypes:
	- events
	- transactions
consumerName: my-consumer
topics:
	- name: events
		fifo: false
	- name: transactions
		fifo: true
```

Usage example (auto-create topics then start):

```ts
import { loadConfig, initFromConfig, Producer, Consumer } from 'pulse-sdk';

async function main() {
	const cfg = loadConfig(); // reads pulse.yml or environment
	await initFromConfig(cfg); // create topics listed in cfg.topics (no-op if none)

	const producer = new Producer(cfg);
	await producer.send('events', { type: 'user.created', id: 1 });

	const consumer = new Consumer(cfg);
	consumer.on('events', (msg) => console.log('received', msg.payload));
	await consumer.start('events', cfg.consumerName || 'default');
}

main().catch(console.error);
```

Notes:
- `initFromConfig` calls the gRPC `CreateTopic` RPC. If the broker responds with an error for a topic that already exists, the SDK logs a warning and continues.
- Ensure the broker is running and reachable at `cfg.grpcUrl` before calling `initFromConfig`.
- The SDK bundles `pulse.proto` inside the package so consumers do not need to copy proto files into their projects.

## Quick Start

Import from the published package and pass configuration programmatically:

```ts
import { Producer, Consumer } from 'pulse-sdk';

const cfg = { grpcUrl: 'localhost:50052', eventTypes: ['events'] };

// Send a JSON-serializable event
const producer = new Producer(cfg);
await producer.send('events', { type: 'user.created', id: 123 });

// Consume events and register a handler
const consumer = new Consumer(cfg);
consumer.on('events', (msg) => {
	console.log('received', msg.payload);
});
await consumer.start('events', 'my-consumer');
```

### Programmatic override

You can create and pass `PulseConfig` directly in code:

```ts
const cfg = { grpcUrl: '10.0.0.5:50052', eventTypes: ['events'] };
const producer = new Producer(cfg);
```

## Technical Details

- The SDK dynamically loads `pulse.proto` at runtime using `@grpc/proto-loader` and `@grpc/grpc-js`.
- The protobuf field `bytes payload` is used to carry the message body; the SDK serializes JSON payloads into that field by default.
- `Consumer.start()` opens a server stream (`Consume`) and dispatches incoming messages to handlers registered via `consumer.on(topic, handler)`.

## Examples

Producer (send multiple events):

```ts
const producer = new Producer(loadConfig());
await producer.send('events', { type: 'login', userId: 1 });
await producer.send('events', { type: 'logout', userId: 1 });
```

Consumer (simple processing):

```ts
const cfg = loadConfig();
const consumer = new Consumer(cfg);

consumer.on('events', (msg) => {
	console.log('event payload', msg.payload);
});

await consumer.start('events', cfg.consumerName || 'default');
```

## Local Tests

The tests in `tests/` start a lightweight gRPC test server automatically; run them with `npm test`.

## FAQ (short)

- Can I use decorators for handlers? Yes — TypeScript supports decorators, but this SDK does not use them by default. We can add decorator helpers later if desired.
- How do I enable TLS / secure credentials? The client factory currently uses `createInsecure()` by default. We can expose a `credentials` option on `PulseConfig` to accept `grpc.credentials.createSsl(...)` or other credential objects.

**Grouped consumption**

- **What it does:** When `grouped` is `true` (the default) the SDK coalesces consumers inside the same process that use the same `consumerName` into a single streaming connection. Messages are distributed (round-robin) among the registered handlers so each message is delivered to only one handler in the group (1x per client id).
- **Why:** This mirrors the Python SDK behavior where handlers registered with `grouped=True` share a single consumer stream and avoid duplicate processing inside the same client.
- **How to configure:**
	- `pulse.yml` (recommended): set `grouped: true` or `grouped: false` under top-level config.
	- Environment variable: `PULSE_GROUPED=true|false`.
	- Programmatically: pass `grouped` in the `PulseConfig` passed to `Producer`/`Consumer`.
- **Default:** `grouped` defaults to `true`.
- **Grouped=false behavior:** When `grouped` is `false`, the SDK will ensure consumers use unique consumer IDs (if you pass the default client name), so multiple consumers in the same process each receive all messages independently (useful for testing or when you want duplicate consumption).
- **Example `pulse.yml` entry:**

```yaml
grpcUrl: localhost:5556
grouped: true
```

The test-suite includes integration tests that validate both `grouped=true` and `grouped=false` behaviour.

## Contributing

Please open a PR with tests. The existing tests validate basic Producer/Consumer behaviour.

---

If you want this README expanded to mirror the Python SDK more closely (topic-level config, manual offset commits, longer examples), tell me which sections to add and I will update it.
