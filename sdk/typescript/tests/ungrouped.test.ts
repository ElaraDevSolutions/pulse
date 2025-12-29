import { startTestServer } from './server';
import { loadConfig } from '../src/config';
import { Consumer } from '../src/consumer';

test('grouped=false each consumer receives all messages', async () => {
  const { server, port } = await startTestServer();
  const grpcUrl = `localhost:${port}`;

  const cfg = Object.assign(loadConfig(), { grpcUrl, grouped: false, consumerName: 'test-client' });

  const c1 = new Consumer(cfg as any);
  const c2 = new Consumer(cfg as any);

  let c1count = 0;
  let c2count = 0;

  const p1 = new Promise<void>((resolve) => {
    c1.on('events', () => {
      c1count++;
      if (c1count >= 2) resolve();
    });
  });

  const p2 = new Promise<void>((resolve) => {
    c2.on('events', () => {
      c2count++;
      if (c2count >= 2) resolve();
    });
  });

  // start consumers without specifying unique names; SDK should create unique ids when grouped=false
  c1.start('events', cfg.consumerName || 'test-client');
  c2.start('events', cfg.consumerName || 'test-client');

  // wait for both consumers to receive 2 messages each
  await Promise.race([Promise.all([p1, p2]), new Promise((_, r) => setTimeout(() => r(new Error('timeout')), 3000))]);

  expect(c1count).toBeGreaterThanOrEqual(2);
  expect(c2count).toBeGreaterThanOrEqual(2);

  c1.close();
  c2.close();
  server.forceShutdown();
});
