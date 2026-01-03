import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Prefer the cached proto (downloaded from server), then bundled, then repo.
const cachedProto = path.resolve(__dirname, 'pulse.cached.proto');
const bundledProto = path.resolve(__dirname, 'pulse.proto');
const repoProto = path.resolve(__dirname, '../../../../internal/api/proto/pulse.proto');

// Try to update proto from server
// try {
//   const host = process.env.PULSE_HOST || 'localhost';
//   const port = process.env.PULSE_HTTP_PORT || '5555';
//   const url = `http://${host}:${port}/proto`;
//   // Use curl to download with a short timeout (1s)
//   execSync(`curl -s -m 1 -o "${cachedProto}" "${url}"`, { stdio: 'ignore' });
// } catch (e) {
//   // Failed to fetch, will fall back to existing files
// }

let PROTO_PATH = bundledProto;
if (fs.existsSync(cachedProto)) {
  PROTO_PATH = cachedProto;
} else if (!fs.existsSync(bundledProto)) {
  // fallback to repo proto for local development
  PROTO_PATH = repoProto;
}

export const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

export const loaded: any = (grpc.loadPackageDefinition(packageDefinition) as any).pulse.v1;

export type PulseClient = any;

export function createClient(address: string): PulseClient {
  return new loaded.PulseService(address, grpc.credentials.createInsecure());
}
