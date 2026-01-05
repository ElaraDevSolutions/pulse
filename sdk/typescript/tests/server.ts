import path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { loaded as proto } from '../src/proto/client';

export function startTestServer(port = 0): Promise<{ server: grpc.Server; port: number }> {
  const server = new grpc.Server();

  const impl = {
    Publish(call: any, callback: any) {
      const res = { id: 'msg-1', offset: 1 };
      callback(null, res);
    },
    StreamPublish(call: any, callback: any) {
      let count = 0;
      call.on('data', (req: any) => {
        count++;
      });
      call.on('end', () => {
        callback(null, { succeeded_count: count, failed_count: 0, last_error: '' });
      });
    },
    Consume(call: any) {
      // write 2 messages then end
      const msgs = [
        { offset: 1, timestamp: Date.now(), payload: Buffer.from(JSON.stringify({ foo: 'bar1' })), headers: { 'payload-type': 'json' } },
        { offset: 2, timestamp: Date.now(), payload: Buffer.from(JSON.stringify({ foo: 'bar2' })), headers: { 'payload-type': 'json' } },
      ];
      const timers: NodeJS.Timeout[] = [];
      msgs.forEach((m, i) => {
        const t = setTimeout(() => call.write(m), i * 50);
        if (typeof (t as any).unref === 'function') (t as any).unref();
        timers.push(t);
      });
      const endTimer = setTimeout(() => call.end(), msgs.length * 50 + 10);
      if (typeof (endTimer as any).unref === 'function') (endTimer as any).unref();
      timers.push(endTimer);
    },
    CommitOffset(call: any, callback: any) {
      callback(null, { success: true });
    },
    CreateTopic(call: any, callback: any) {
      callback(null, { success: true });
    },
  };

  server.addService(proto.PulseService.service, impl);

  return new Promise((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
      if (err) return reject(err);
      server.start();
      resolve({ server, port: boundPort });
    });
  });
}
