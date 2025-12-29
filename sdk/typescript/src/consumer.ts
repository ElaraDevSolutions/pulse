import { PulseConfig } from './config';
import { createClient } from './proto/client';

export type EventHandler = (payload: any) => void;

export class Consumer {
  private handlers: Record<string, EventHandler[]> = {};
  private client: any;

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
    const req = { topic: topic || this.config.eventTypes[0], consumer_name: consumerName, offset: 0 };
    const stream = this.client.Consume(req);

    stream.on('data', (msg: any) => {
      const payloadBuf = msg.payload as Buffer;
      let parsed = payloadBuf;
      try {
        parsed = JSON.parse(payloadBuf.toString());
      } catch (e) {
        // keep raw buffer if not JSON
      }
      const handlers = this.handlers[req.topic] || [];
      handlers.forEach(h => h({ offset: msg.offset, timestamp: msg.timestamp, payload: parsed, headers: msg.headers }));
    });

    return new Promise<void>((resolve, reject) => {
      stream.on('end', () => resolve());
      stream.on('error', (e: any) => reject(e));
    });
  }
}
