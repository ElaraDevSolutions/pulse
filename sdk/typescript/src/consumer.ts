import { getConfig } from './config';
import { createClient } from './proto/client';
import { Message, runWithContext, commit } from './message';
import { randomUUID } from 'crypto';

export { commit, Message };

interface ConsumerOptions {
  host?: string;
  port?: number;
  consumerGroup?: string;
  autoCommit?: boolean;
  grouped?: boolean;
}

interface RegisteredConsumer {
  topic: string;
  host: string;
  port: number;
  group: string;
  autoCommit: boolean;
  handler: (msg: Message) => void | Promise<void>;
  grouped: boolean;
}

const _consumers: RegisteredConsumer[] = [];
const _cleanupFunctions: (() => void)[] = [];

export function stop() {
  _cleanupFunctions.forEach(fn => fn());
  _cleanupFunctions.length = 0;
  _consumers.length = 0;
}

export function consumer(
  topic: string,
  handler: (msg: Message) => void | Promise<void>,
  options: ConsumerOptions = {}
) {
  const config = getConfig();
  
  const host = options.host || config.broker.host;
  const port = options.port || config.broker.grpc_port;
  
  const baseGroup = options.consumerGroup || config.client.id;
  
  const grouped = options.grouped !== undefined ? options.grouped : true;
  
  let group = baseGroup;
  if (!grouped) {
    group = `${baseGroup}-${randomUUID().replace(/-/g, '')}`;
  }

  let autoCommit = options.autoCommit;
  if (autoCommit === undefined) {
    autoCommit = config.client.auto_commit;
    // Check topic specific config
    const topicCfg = config.topics?.find(t => t.name === topic);
    // Note: Python SDK checks topic_config["consume"]["auto_commit"] but our config.ts 
    // currently only has create_if_missing and config (fifo, retention).
    // We'll stick to global client config for now unless we expand TopicConfig.
  }

  _consumers.push({
    topic,
    host,
    port,
    group,
    autoCommit: autoCommit!,
    handler,
    grouped,
  });
}

export async function run() {
  // Group consumers by (topic, host, port, group)
  const groupedMap = new Map<string, {
    topic: string;
    host: string;
    port: number;
    group: string;
    autoCommit: boolean;
    handlers: ((msg: Message) => void | Promise<void>)[];
  }>();

  for (const c of _consumers) {
    const key = `${c.topic}:${c.host}:${c.port}:${c.group}`;
    if (!groupedMap.has(key)) {
      groupedMap.set(key, {
        topic: c.topic,
        host: c.host,
        port: c.port,
        group: c.group,
        autoCommit: c.autoCommit,
        handlers: [],
      });
    }
    groupedMap.get(key)!.handlers.push(c.handler);
  }

  const promises: Promise<void>[] = [];
  for (const groupConfig of groupedMap.values()) {
    promises.push(consumeLoopGroup(groupConfig));
  }

  // We don't await promises here to let them run in background, 
  // but we could if we wanted to block until they all finish (which they won't).
  // However, to keep the process alive, the user should probably await this or we return a promise that never resolves?
  // Python's run() blocks. In Node, usually we just start things.
  // But if the script ends, the process exits.
  // We'll return a promise that never resolves to simulate blocking if awaited.
  return new Promise<void>(() => {});
}

async function consumeLoopGroup(groupConfig: {
  topic: string;
  host: string;
  port: number;
  group: string;
  autoCommit: boolean;
  handlers: ((msg: Message) => void | Promise<void>)[];
}) {
  const address = `${groupConfig.host}:${groupConfig.port}`;
  // console.log(`Starting consumer for topic '${groupConfig.topic}' (group: ${groupConfig.group}) on ${address}`);

  let handlerIdx = 0;
  let currentStream: any = null;
  let retryTimeout: NodeJS.Timeout | null = null;
  let isStopped = false;

  const cleanup = () => {
    isStopped = true;
    if (currentStream) {
      try {
        currentStream.cancel();
      } catch (e) {
        // Ignore cancel errors
      }
      currentStream = null;
    }
    if (retryTimeout) {
      clearTimeout(retryTimeout);
      retryTimeout = null;
    }
  };
  _cleanupFunctions.push(cleanup);

  const startStream = async () => {
    if (isStopped) return;

    try {
      const client = createClient(address);
      const req = {
        topic: groupConfig.topic,
        consumer_name: groupConfig.group,
        offset: 0,
      };

      const stream = client.Consume(req);
      currentStream = stream;

      stream.on('data', async (protoMsg: any) => {
        if (isStopped) return;
        // Pause stream to process message sequentially
        stream.pause();

        const msg = new Message(protoMsg);
        
        if (groupConfig.handlers.length === 0) {
          stream.resume();
          return;
        }

        const handler = groupConfig.handlers[handlerIdx % groupConfig.handlers.length];
        handlerIdx++;

        const ctx = {
          stub: client,
          topic: groupConfig.topic,
          consumerGroup: groupConfig.group,
          offset: msg.offset,
          committed: false,
        };

        try {
          await runWithContext(ctx, async () => {
            await handler(msg);
            
            if (groupConfig.autoCommit && !ctx.committed) {
              await commit();
            }
          });
        } catch (e) {
          console.error(`Error processing message: ${e}`);
        } finally {
          // Resume stream after processing
          stream.resume();
        }
      });

      stream.on('error', (err: any) => {
        if (isStopped) return;
        // 1 = CANCELLED
        if (err.code === 1) return;
        
        console.error(`Connection lost for ${groupConfig.topic}: ${err.message}. Retrying in 5s...`);
        retryTimeout = setTimeout(startStream, 5000);
      });

      stream.on('end', () => {
        if (isStopped) return;
        // console.warn(`Stream ended for ${groupConfig.topic}. Retrying in 5s...`);
        retryTimeout = setTimeout(startStream, 5000);
      });

    } catch (e: any) {
      if (isStopped) return;
      console.error(`Unexpected error in consumer ${groupConfig.topic}: ${e.message}`);
      retryTimeout = setTimeout(startStream, 5000);
    }
  };

  startStream();
}
