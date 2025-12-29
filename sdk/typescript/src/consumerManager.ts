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
  grpcUrl?: string;
}

const registry: Map<string, StreamEntry> = new Map();

function keyFor(grpcUrl: string, topic: string, consumerName: string) {
  return `${grpcUrl}::${topic}::${consumerName}`;
}

export function registerSharedHandler(grpcUrl: string, topic: string, consumerName: string, handler: Handler) {
  const k = keyFor(grpcUrl, topic, consumerName);
  let entry = registry.get(k);
    if (!entry) {
    const client = createClient(grpcUrl);
    entry = { topic, consumerName, client, stream: null, handlers: new Set(), nextIndex: 0, grpcUrl };
    // add handler before starting the stream to avoid losing early messages
    entry.handlers.add(handler);
    registry.set(k, entry);
    startStream(entry);
    return () => {
      // unregister
      const e = registry.get(k);
      if (!e) return;
      e.handlers.delete(handler);
      if (e.handlers.size === 0) {
        try { if (e.stream && e.stream.cancel) e.stream.cancel(); } catch (err) { }
        registry.delete(k);
      }
    };
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
  // debug: console.log('consumerManager.startStream', entry.grpcUrl, entry.topic, entry.consumerName);

  stream.on('data', (msg: any) => {
    const message = new Message(msg);
    const handlers = Array.from(entry.handlers);
    if (handlers.length === 0) return;
    if (entry.nextIndex === undefined) entry.nextIndex = 0;
    const h = handlers[entry.nextIndex % handlers.length];
    entry.nextIndex = (entry.nextIndex + 1) % handlers.length;
    try {
      // run handler inside context providing the stub for commit()
      // debug: console.log('consumerManager.dispatch', entry.topic, 'offset', message.offset, 'handlerIndex', entry.nextIndex);
      runWithContext({ stub: entry.client, topic: entry.topic, consumerName: entry.consumerName, offset: message.offset }, () => {
        h(message);
      });
    } catch (e) {
      console.warn('handler error', e);
    }
  });

  stream.on('error', (e: any) => {
    // Log once and clean up the registry entry to avoid reconnect storms and test leaks
    console.warn('shared consumer stream error for', entry.topic, e);
    try {
      if (entry.stream && entry.stream.cancel) entry.stream.cancel();
    } catch (err) {
      // ignore
    }
    try {
      registry.delete(keyFor(entry.grpcUrl || '', entry.topic, entry.consumerName));
    } catch (err) {
      // ignore
    }
  });

  stream.on('end', () => {
    // stream ended; clean up entry
    try {
      registry.delete(keyFor(entry.grpcUrl || '', entry.topic, entry.consumerName));
    } catch (err) {
      // ignore
    }
  });
}
