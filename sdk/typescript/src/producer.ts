import { PulseConfig } from './config';
import { createClient } from './proto/client';

export interface PublishResponse {
  id: string;
  offset: number | string;
}

export class Producer {
  private client: any;

  constructor(private config: PulseConfig) {
    this.client = createClient(config.grpcUrl);
  }

  async send(topic: string, payload: any, headers?: Record<string, string>): Promise<PublishResponse> {
    const req = {
      topic,
      payload: Buffer.from(JSON.stringify(payload)),
      headers: headers || {},
    };

    return new Promise((resolve, reject) => {
      this.client.Publish(req, (err: any, res: any) => {
        if (err) return reject(err);
        resolve({ id: res.id, offset: res.offset });
      });
    });
  }
}
