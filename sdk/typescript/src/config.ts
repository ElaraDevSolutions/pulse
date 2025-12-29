export interface PulseConfig {
  grpcUrl: string;
  eventTypes: string[];
}

export function loadConfig(config: PulseConfig): PulseConfig {
  // Aqui pode ser expandido para ler de arquivo ou env
  return config;
}
