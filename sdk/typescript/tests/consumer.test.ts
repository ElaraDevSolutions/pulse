import { Consumer } from '../src/consumer';
import { PulseConfig } from '../src/config';
import { startTestServer } from './server';

describe('Consumer', () => {
  let server: any;
  let port: number;
  beforeAll(async () => {
    const srv = await startTestServer(0);
    server = srv.server;
    port = srv.port;
  });
  afterAll(() => {
    server.forceShutdown();
  });

  it('should receive streamed events', async () => {
    const config: PulseConfig = { grpcUrl: `localhost:${port}`, eventTypes: ['test'], grouped: false };
    const consumer = new Consumer(config);

    const received: any[] = [];
    consumer.on('test', (payload) => {
      received.push(payload);
    });

    await consumer.start('test', 'test-consumer');
    expect(received.length).toBeGreaterThanOrEqual(1);
    expect(received[0].payload.foo).toBeDefined();
  });
});
