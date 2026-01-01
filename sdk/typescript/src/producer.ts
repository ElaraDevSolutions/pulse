import { getConfig, PulseConfig } from './config';
import { createClient } from './proto/client';

export class Producer {
  private client: any;
  private config: PulseConfig;

  constructor(host?: string, port?: number) {
    this.config = getConfig();
    const h = host || this.config.broker.host;
    const p = port || this.config.broker.grpc_port;
    const address = `${h}:${p}`;

    this.client = createClient(address);
    this.setupTopics();
  }

  private setupTopics() {
    const topics = this.config.topics || [];
    for (const topicCfg of topics) {
      if (topicCfg.create_if_missing) {
        const req = {
          topic: topicCfg.name,
          fifo: topicCfg.config?.fifo || false,
          retention_bytes: topicCfg.config?.retention_bytes || 0,
        };
        
        this.client.CreateTopic(req, (err: any, res: any) => {
          // Ignore errors (e.g. topic already exists)
          if (err) {
            // console.warn(`Failed to create topic ${topicCfg.name}:`, err.message);
          }
        });
      }
    }
  }

  async send(topic: string, payload: any): Promise<void> {
    let data: Buffer;
    const headers: Record<string, string> = {};

    if (Buffer.isBuffer(payload)) {
      data = payload;
      headers['payload-type'] = 'bytes';
    } else if (typeof payload === 'string') {
      data = Buffer.from(payload, 'utf-8');
      headers['payload-type'] = 'string';
    } else if (typeof payload === 'object') {
      data = Buffer.from(JSON.stringify(payload), 'utf-8');
      headers['payload-type'] = 'json';
    } else {
      throw new Error('Payload must be Buffer, string, or object');
    }

    const req = {
      topic,
      payload: data,
      headers,
    };

    return new Promise((resolve, reject) => {
      this.client.Publish(req, (err: any, res: any) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  close() {
    this.client.close();
  }
}
