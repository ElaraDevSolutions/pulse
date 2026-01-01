import { AsyncLocalStorage } from 'async_hooks';

export interface MessageContext {
  stub: any;
  topic: string;
  consumerGroup: string;
  offset: number;
  committed: boolean;
}

const contextStorage = new AsyncLocalStorage<MessageContext>();

export function runWithContext(ctx: MessageContext, fn: () => void | Promise<void>) {
  return contextStorage.run(ctx, fn);
}

export async function commit() {
  const ctx = contextStorage.getStore();
  if (!ctx) {
    throw new Error('commit() called outside of a consumer handler');
  }
  
  if (ctx.committed) {
    return;
  }

  return new Promise<void>((resolve, reject) => {
    const req = {
      topic: ctx.topic,
      consumer_name: ctx.consumerGroup,
      offset: ctx.offset + 1,
    };

    ctx.stub.CommitOffset(req, (err: any, res: any) => {
      if (err) {
        console.error(`Error committing offset: ${err}`);
        return reject(err);
      }
      console.log(`Committed offset ${req.offset} for ${req.consumer_name}`);
      ctx.committed = true;
      resolve();
    });
  });
}

export class Message {
  public offset: number;
  public timestamp: number;
  private _rawPayload: Buffer;
  private _headers: Record<string, string>;

  constructor(protoMsg: any) {
    this.offset = typeof protoMsg.offset === 'string' ? parseInt(protoMsg.offset, 10) : protoMsg.offset;
    this.timestamp = typeof protoMsg.timestamp === 'string' ? parseInt(protoMsg.timestamp, 10) : protoMsg.timestamp;
    this._rawPayload = protoMsg.payload;
    this._headers = protoMsg.headers || {};
  }

  get payload(): any {
    const ptype = this._headers['payload-type'];
    
    if (ptype === 'json') {
      try {
        return JSON.parse(this._rawPayload.toString('utf-8'));
      } catch (e) {
        return this._rawPayload;
      }
    }
    
    if (ptype === 'string') {
      return this._rawPayload.toString('utf-8');
    }
    
    if (ptype === 'bytes') {
      return this._rawPayload;
    }

    // Fallback
    try {
      return JSON.parse(this._rawPayload.toString('utf-8'));
    } catch (e) {
      return this._rawPayload;
    }
  }

  get rawPayload(): Buffer {
    return this._rawPayload;
  }

  get headers(): Record<string, string> {
    return this._headers;
  }
}
