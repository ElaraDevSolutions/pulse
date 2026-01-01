import { startTestServer } from './server';
import { consumer, run, Message } from '../src/consumer';

test('grouped=true dispatches messages across handlers (one delivery per message)', async () => {
  const { server, port } = await startTestServer();

  let c1count = 0;
  let c2count = 0;

  const p1 = new Promise<void>((resolve) => {
    consumer('events', async (msg: Message) => {
      c1count++;
      if (c1count + c2count >= 2) resolve();
    }, { host: 'localhost', port, grouped: true, consumerGroup: 'test-group' });
  });

  // We can't easily await the second one separately in this setup because the resolve condition depends on total count
  // But we can register the second consumer
  consumer('events', async (msg: Message) => {
    c2count++;
    // We don't have a separate promise here, but the test waits for p1 which checks total count
  }, { host: 'localhost', port, grouped: true, consumerGroup: 'test-group' });

  run();

  // wait for messages
  await new Promise(resolve => setTimeout(resolve, 1000));

  expect(c1count + c2count).toBe(2);
  // with round-robin and 2 messages, each should get 1
  expect(c1count).toBe(1);
  expect(c2count).toBe(1);

  server.forceShutdown();
});
