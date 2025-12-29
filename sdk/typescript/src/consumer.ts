import { PulseConfig } from './config';
import { createClient } from './proto/client';
import { registerSharedHandler } from './consumerManager';
import { randomUUID } from 'crypto';
import { Message, runWithContext } from './message';

export type EventHandler = (payload: any) => void;

export class Consumer {
  private handlers: Record<string, EventHandler[]> = {};
  private client: any;
  private unregisterFns: Array<() => void> = [];

  constructor(private config: PulseConfig) {
    this.client = createClient(config.grpcUrl);
  }

  on(eventType: string, handler: EventHandler) {
    if (!this.handlers[eventType]) {
      this.handlers[eventType] = [];
    }
    this.handlers[eventType].push(handler);
  }

  async start(topic?: string, consumerName = 'default') {
    const topicName = topic || this.config.eventTypes[0];

    // If grouped is enabled (default true) use shared in-process stream,
    // otherwise create a dedicated stream (each consumer gets all messages).
    const grouped = (this.config as any).grouped !== false;

    if (grouped) {
      // register handlers for this topic to shared stream
      const handlers = this.handlers[topicName] || [];
      for (const h of handlers) {
        const unregister = registerSharedHandler(this.config.grpcUrl, topicName, consumerName, (msg: any, stub?: any, offset?: number) => {
          // run handler with context so commit() works
          // debug: console.log('consumer.wrapper.invoke', consumerName, topicName);
          runWithContext({ stub: stub || this.client, topic: topicName, consumerName, offset: offset ?? msg.offset }, () => {
            try { h(msg); } catch (e) { /* ignore */ }
          });
        });
        this.unregisterFns.push(unregister);
      }

      // Return a promise that never resolves (stream runs until process exits)
        return new Promise<void>(() => {});
      }

    // If grouped is explicitly false, and the consumerName equals the configured
    // client name or the default literal, generate a unique consumer id so each
    // consumer receives messages independently (mirrors Python behaviour).
    if (!grouped) {
      const base = this.config.consumerName || 'default';
      if (consumerName === base || consumerName === 'default') {
        consumerName = `${base}-${randomUUID().replace(/-/g, '')}`;
      }
    }

    const req = { topic: topicName, consumer_name: consumerName, offset: 0 };
    const stream = this.client.Consume(req);

    stream.on('data', (msg: any) => {
      const message = new Message(msg);
      const handlers = this.handlers[req.topic] || [];
      for (const h of handlers) {
        // run handler within AsyncLocalStorage context so commit() can access stub and offset
        runWithContext({ stub: this.client, topic: req.topic, consumerName, offset: message.offset }, () => {
          try { h(message); } catch (e) { /* handler error ignored here */ }
        });
      }
    });

    const p = new Promise<void>((resolve, reject) => {
      stream.on('end', () => resolve());
      stream.on('error', (e: any) => reject(e));
    });
    // prevent unhandled rejections when callers don't await the returned promise
    p.catch(() => {});
    return p;
  }

  // unregister any shared handlers when this consumer is discarded
  close() {
    for (const u of this.unregisterFns) {
      try { u(); } catch (e) { /* ignore */ }
    }
    this.unregisterFns = [];
  }
}
