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

## Configuration (current)

Currently the SDK supports programmatic configuration only: create and pass a `PulseConfig` object when instantiating `Producer` or `Consumer`.

The code exposes a `PulseConfig` interface for strong typing. Environment-file loading (e.g. `pulse.yml` or `PULSE_*` environment variables) is planned but not implemented in this initial version — for now, pass configuration directly in code.

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

## Contributing

Please open a PR with tests. The existing tests validate basic Producer/Consumer behaviour.

---

If you want this README expanded to mirror the Python SDK more closely (topic-level config, manual offset commits, longer examples), tell me which sections to add and I will update it.
