import { Producer } from '../src/producer';
import { PulseConfig } from '../src/config';
import { startTestServer } from './server';

describe('Producer', () => {
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

  it('should send event and receive publish response', async () => {
    const config: PulseConfig = { grpcUrl: `localhost:${port}`, eventTypes: ['test'] };
    const producer = new Producer(config);
    const res = await producer.send('test', { foo: 'bar' });
    expect(res.id).toBe('msg-1');
    expect(Number(res.offset)).toBeGreaterThan(0);
  });
});
