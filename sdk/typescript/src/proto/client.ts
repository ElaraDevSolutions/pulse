import path from 'path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

const PROTO_PATH = path.resolve(__dirname, '../../../../internal/api/proto/pulse.proto');

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
