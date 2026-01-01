import { Producer } from '../src/producer';
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

  it('should send event', async () => {
    const producer = new Producer('localhost', port);
    await producer.send('test', { foo: 'bar' });
    // send returns void, so we just check it doesn't throw
    producer.close();
  });
});
