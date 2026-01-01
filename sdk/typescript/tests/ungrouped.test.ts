import { startTestServer } from './server';
import { consumer, run, Message } from '../src/consumer';

test('grouped=false each consumer receives all messages', async () => {
  const { server, port } = await startTestServer();

  let c1count = 0;
  let c2count = 0;

  consumer('events', async (msg: Message) => {
    c1count++;
  }, { host: 'localhost', port, grouped: false, consumerGroup: 'test-client' });

  consumer('events', async (msg: Message) => {
    c2count++;
  }, { host: 'localhost', port, grouped: false, consumerGroup: 'test-client' });

  run();

  // wait for both consumers to receive 2 messages each
  await new Promise(resolve => setTimeout(resolve, 1000));

  expect(c1count).toBeGreaterThanOrEqual(2);
  expect(c2count).toBeGreaterThanOrEqual(2);

  server.forceShutdown();
});
