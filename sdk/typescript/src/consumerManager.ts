import { createClient } from './proto/client';
import { Message, runWithContext } from './message';

type Handler = (msg: any) => void;

interface StreamEntry {
  topic: string;
  consumerName: string;
  client: any;
  stream: any | null;
  handlers: Set<Handler>;
  nextIndex?: number;
}

const registry: Map<string, StreamEntry> = new Map();

function keyFor(topic: string, consumerName: string) {
  return `${topic}::${consumerName}`;
}

export function registerSharedHandler(grpcUrl: string, topic: string, consumerName: string, handler: Handler) {
  const k = keyFor(topic, consumerName);
  let entry = registry.get(k);
  if (!entry) {
    const client = createClient(grpcUrl);
    entry = { topic, consumerName, client, stream: null, handlers: new Set() };
    registry.set(k, entry);
    startStream(entry);
  }
  entry.handlers.add(handler);

  return () => {
    // unregister
    const e = registry.get(k);
    if (!e) return;
    e.handlers.delete(handler);
    if (e.handlers.size === 0) {
      // cleanup stream
      try {
        if (e.stream && e.stream.cancel) e.stream.cancel();
      } catch (e) {
        // ignore
      }
      registry.delete(k);
    }
  };
}

function startStream(entry: StreamEntry) {
  const req = { topic: entry.topic, consumer_name: entry.consumerName, offset: 0 };
  const stream = entry.client.Consume(req);
  entry.stream = stream;

  stream.on('data', (msg: any) => {
    const message = new Message(msg);
    const handlers = Array.from(entry.handlers);
    if (handlers.length === 0) return;
    if (entry.nextIndex === undefined) entry.nextIndex = 0;
    const h = handlers[entry.nextIndex % handlers.length];
    entry.nextIndex = (entry.nextIndex + 1) % handlers.length;
    try {
      // run handler inside context providing the stub for commit()
      runWithContext({ stub: entry.client, topic: entry.topic, consumerName: entry.consumerName, offset: message.offset }, () => {
        h(message);
      });
    } catch (e) {
      console.warn('handler error', e);
    }
  });

  stream.on('error', (e: any) => {
    console.warn('shared consumer stream error for', entry.topic, e);
    // attempt restart after short delay
    setTimeout(() => {
      if (registry.has(keyFor(entry.topic, entry.consumerName))) startStream(entry);
    }, 3000);
  });

  stream.on('end', () => {
    // stream ended; attempt restart
    if (registry.has(keyFor(entry.topic, entry.consumerName))) {
      setTimeout(() => startStream(entry), 1000);
    }
  });
}
