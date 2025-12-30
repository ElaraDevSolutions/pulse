import { AsyncLocalStorage } from 'async_hooks';

export interface IMessage {
  offset: number;
  timestamp: number;
  payload: any;
  headers: Record<string, string>;
}

interface MessageContext {
  stub: any;
  topic: string;
  consumerName: string;
  offset: number;
  committed?: boolean;
}

const storage = new AsyncLocalStorage<MessageContext | undefined>();

export function runWithContext(ctx: MessageContext, fn: () => void) {
  return storage.run(ctx, fn as any);
}

export function getContext(): MessageContext | undefined {
  return storage.getStore();
}

export async function commit() {
  const ctx = storage.getStore();
  if (!ctx) throw new Error('commit() called outside of a consumer handler');
  if (ctx.committed) return;
  return new Promise<void>((resolve, reject) => {
    try {
      ctx.stub.CommitOffset({ topic: ctx.topic, consumer_name: ctx.consumerName, offset: ctx.offset + 1 }, (err: any, res: any) => {
        if (err) return reject(err);
        ctx.committed = true;
        resolve();
      });
    } catch (e) {
      reject(e);
    }
  });
}

export class Message implements IMessage {
  offset: number;
  timestamp: number;
  payload: any;
  headers: Record<string, string>;

  constructor(protoMsg: any) {
    this.offset = protoMsg.offset;
    this.timestamp = protoMsg.timestamp;
    this.headers = {};
    try { this.headers = Object.assign({}, protoMsg.headers); } catch (e) { this.headers = {}; }
    const buf = protoMsg.payload as Buffer;
    const ptype = this.headers['payload-type'];
    if (ptype === 'json') {
      try { this.payload = JSON.parse(buf.toString()); } catch (e) { this.payload = buf; }
    } else if (ptype === 'string') {
      try { this.payload = buf.toString('utf8'); } catch (e) { this.payload = buf; }
    } else if (ptype === 'bytes') {
      this.payload = buf;
    } else {
      try { this.payload = JSON.parse(buf.toString()); } catch (e) { this.payload = buf; }
    }
  }
}
