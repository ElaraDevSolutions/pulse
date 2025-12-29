import fs from 'fs';
import path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Prefer the proto bundled with the package (dist/proto/pulse.proto).
// In development (when running from source), fall back to the repository proto.
const bundledProto = path.resolve(__dirname, 'pulse.proto');
const repoProto = path.resolve(__dirname, '../../../../internal/api/proto/pulse.proto');

let PROTO_PATH = bundledProto;
if (!fs.existsSync(bundledProto)) {
  // fallback to repo proto for local development
  PROTO_PATH = repoProto;
}

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const loaded: any = (grpc.loadPackageDefinition(packageDefinition) as any).pulse.v1;

export type PulseClient = any;

export function createClient(address: string): PulseClient {
  return new loaded.PulseService(address, grpc.credentials.createInsecure());
}
