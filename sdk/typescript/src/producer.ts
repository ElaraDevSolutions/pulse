import { getConfig, PulseConfig } from './config';
import { createClient } from './proto/client';

export class Producer {
  private client: any;
  private config: PulseConfig;
  private stream: any;

  constructor(host?: string, port?: number) {
    this.config = getConfig();
    const h = host || this.config.broker.host;
    const p = port || this.config.broker.grpc_port;
    const address = `${h}:${p}`;

    this.client = createClient(address);
    this.setupTopics();
    this.initStream();
  }

  private initStream() {
    this.stream = this.client.StreamPublish((err: any, summary: any) => {
      if (err) {
        console.error('StreamPublish ended with error:', err);
      }
    });

    this.stream.on('error', (err: any) => {
      console.error('StreamPublish stream error:', err);
      // Simple reconnect attempt could be added here
    });
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

    // Write to the stream
    // Note: This is now fire-and-forget for performance.
    // We don't wait for server acknowledgement for every message.
    this.stream.write(req);
    return Promise.resolve();
  }

  async streamSend(messages: Array<{ topic: string; payload: any }>): Promise<any> {
    return new Promise((resolve, reject) => {
      const stream = this.client.StreamPublish((err: any, summary: any) => {
        if (err) return reject(err);
        resolve(summary);
      });

      stream.on('error', (err: any) => {
        reject(err);
      });

      for (const msg of messages) {
        let data: Buffer;
        const headers: Record<string, string> = {};

        if (Buffer.isBuffer(msg.payload)) {
          data = msg.payload;
          headers['payload-type'] = 'bytes';
        } else if (typeof msg.payload === 'string') {
          data = Buffer.from(msg.payload, 'utf-8');
          headers['payload-type'] = 'string';
        } else if (typeof msg.payload === 'object') {
          data = Buffer.from(JSON.stringify(msg.payload), 'utf-8');
          headers['payload-type'] = 'json';
        } else {
          stream.end();
          return reject(new Error('Payload must be Buffer, string, or object'));
        }

        stream.write({
          topic: msg.topic,
          payload: data,
          headers,
        });
      }

      stream.end();
    });
  }

  close() {
    if (this.stream) {
      this.stream.end();
    }
    this.client.close();
  }
}
