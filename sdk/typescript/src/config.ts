import fs from 'fs';
import path from 'path';
import os from 'os';
import yaml from 'js-yaml';

export interface BrokerConfig {
  host: string;
  http_port: number;
  grpc_port: number;
  timeout_ms: number;
}

export interface ClientConfig {
  id: string;
  auto_commit: boolean;
  max_retries: number;
}

export interface TopicConfig {
  name: string;
  create_if_missing?: boolean;
  config?: {
    fifo?: boolean;
    retention_bytes?: number;
  };
}

export interface PulseConfig {
  broker: BrokerConfig;
  client: ClientConfig;
  topics: TopicConfig[];
}

const DEFAULT_CONFIG: PulseConfig = {
  broker: {
    host: 'localhost',
    http_port: 5555,
    grpc_port: 5556,
    timeout_ms: 5000,
  },
  client: {
    id: 'typescript-client',
    auto_commit: true,
    max_retries: 3,
  },
  topics: [],
};

export function loadConfig(configPath?: string): PulseConfig {
  const candidates = [
    configPath,
    path.resolve(process.cwd(), 'pulse.yaml'),
    path.resolve(process.cwd(), 'pulse.yml'),
    path.join(os.homedir(), '.pulse', 'pulse.yml'),
  ].filter(Boolean) as string[];

  let fileCfg: any = {};
  for (const p of candidates) {
    if (p && fs.existsSync(p)) {
      try {
        const raw = fs.readFileSync(p, 'utf8');
        fileCfg = yaml.load(raw) || {};
        break;
      } catch (e) {
        console.warn(`Failed to load config from ${p}:`, e);
      }
    }
  }

  // Deep merge defaults with file config
  const config = JSON.parse(JSON.stringify(DEFAULT_CONFIG)); // Deep copy
  
  if (fileCfg.broker) {
    config.broker = { ...config.broker, ...fileCfg.broker };
  }
  if (fileCfg.client) {
    config.client = { ...config.client, ...fileCfg.client };
  }
  if (fileCfg.topics) {
    config.topics = fileCfg.topics;
  }

  return config;
}

let _config: PulseConfig | null = null;

export function getConfig(): PulseConfig {
  if (!_config) {
    _config = loadConfig();
  }
  return _config;
}
