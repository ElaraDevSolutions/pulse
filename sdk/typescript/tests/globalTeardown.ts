import { shutdownAll } from '../src/consumerManager';

export default async function globalTeardown() {
  try {
    await shutdownAll();
  } catch (err) {
    // ignore teardown errors
    // console.warn('globalTeardown error', err);
  }
}
