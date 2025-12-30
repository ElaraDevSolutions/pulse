import { startTestServer } from './server';
import { loadConfig } from '../src/config';
import { Consumer } from '../src/consumer';

test('grouped=true dispatches messages across handlers (one delivery per message)', async () => {
  const { server, port } = await startTestServer();
  const grpcUrl = `localhost:${port}`;

  const cfg = Object.assign(loadConfig(), { grpcUrl, grouped: true, consumerName: 'test-group' });

  const c1 = new Consumer(cfg as any);
  const c2 = new Consumer(cfg as any);

  let c1count = 0;
  let c2count = 0;

  const p1 = new Promise<void>((resolve) => {
    c1.on('events', () => {
      c1count++;
      resolve();
    });
  });

  const p2 = new Promise<void>((resolve) => {
    c2.on('events', () => {
      c2count++;
      resolve();
    });
  });

  // start consumers (they register handlers to shared stream). Attach a
  // catch to the returned promise so any asynchronous stream errors don't
  // result in unhandled rejections when tests don't await start().
  c1.start('events', cfg.consumerName || 'test-group').catch(() => {});
  c2.start('events', cfg.consumerName || 'test-group').catch(() => {});

  // wait for both messages to be processed
  await Promise.race([Promise.all([p1, p2]), new Promise((_, r) => setTimeout(() => r(new Error('timeout')), 2000))]);

  expect(c1count + c2count).toBe(2);
  // with round-robin and 2 messages, each should get 1
  expect(c1count).toBeGreaterThanOrEqual(1);
  expect(c2count).toBeGreaterThanOrEqual(1);

  // cleanup
  c1.close();
  c2.close();
  server.forceShutdown();
});
