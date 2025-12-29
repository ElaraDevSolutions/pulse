import fs from 'fs';
import path from 'path';
import os from 'os';
import yaml from 'js-yaml';
import { createClient } from './proto/client';

export interface TopicConfig {
  name: string;
  fifo?: boolean;
  retention_bytes?: number;
  retention_time?: number; // nanoseconds
}

export interface PulseConfig {
  grpcUrl: string;
  eventTypes: string[];
  consumerName?: string;
  topics?: TopicConfig[];
}

const DEFAULTS: Partial<PulseConfig> = {
  grpcUrl: 'localhost:50052',
  eventTypes: ['events'],
};

export function loadConfig(configPath?: string): PulseConfig {
  const candidates = [
    configPath,
    path.resolve(process.cwd(), 'pulse.yml'),
    path.resolve(process.cwd(), 'pulse.yaml'),
    path.join(os.homedir(), '.pulse', 'pulse.yml'),
  ].filter(Boolean) as string[];

  let fileCfg: Partial<PulseConfig> = {};
  for (const p of candidates) {
    if (p && fs.existsSync(p)) {
      const raw = fs.readFileSync(p, 'utf8');
      const parsed = yaml.load(raw) as any;
      fileCfg = parsed || {};
      break;
    }
  }

  const envCfg: Partial<PulseConfig> = {};
  if (process.env.PULSE_GRPC_URL) envCfg.grpcUrl = process.env.PULSE_GRPC_URL;
  if (process.env.PULSE_EVENT_TYPES) envCfg.eventTypes = process.env.PULSE_EVENT_TYPES.split(',');
  if (process.env.PULSE_CONSUMER_NAME) envCfg.consumerName = process.env.PULSE_CONSUMER_NAME;

  const merged: PulseConfig = Object.assign({}, DEFAULTS, fileCfg, envCfg) as PulseConfig;
  // Ensure eventTypes array exists
  if (!merged.eventTypes) merged.eventTypes = DEFAULTS.eventTypes as string[];
  return merged;
}

// Initialize topics from config using gRPC CreateTopic RPC.
export async function initFromConfig(cfg: PulseConfig): Promise<void> {
  if (!cfg.topics || cfg.topics.length === 0) return;
  const client = createClient(cfg.grpcUrl);

  for (const t of cfg.topics) {
    const req: any = {
      topic: t.name || t['name'],
      fifo: !!t.fifo,
      retention_bytes: t.retention_bytes || 0,
      retention_time: t.retention_time || 0,
    };
    await new Promise<void>((resolve, reject) => {
      client.CreateTopic(req, (err: any, res: any) => {
        if (err) {
          // If topic exists the server may return an error; log and continue
          console.warn('CreateTopic error for', req.topic, err.message || err);
          resolve();
        } else {
          resolve();
        }
      });
    });
  }
}
