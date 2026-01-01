import { consumer, run, Message } from '../src/consumer';
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
    const received: any[] = [];
    
    consumer('test', async (msg: Message) => {
      received.push(msg.payload);
    }, { 
      host: 'localhost', 
      port: port,
      grouped: false,
      consumerGroup: 'test-consumer'
    });

    // Start the consumer loop
    run();

    // Wait for messages
    await new Promise(resolve => setTimeout(resolve, 500));

    expect(received.length).toBeGreaterThanOrEqual(1);
    expect(received[0].foo).toBe('bar1');
  });
});
