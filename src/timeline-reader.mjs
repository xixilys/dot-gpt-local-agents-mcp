// This small read-only helper runs inside Paseo's Electron Node runtime so the
// officially installed client can be loaded from app.asar. It never launches
// agents. Only the trusted parent provides transport separately from request args.
import { connectToDaemon } from '/Applications/Paseo.app/Contents/Resources/app.asar/node_modules/@getpaseo/cli/dist/utils/client.js';
import { nativeTarget, checkNativeIdentity } from './ssh-bridge.mjs';

try {
  const args = JSON.parse(process.argv[2]);
  const transport = process.argv[3] ? JSON.parse(process.argv[3]) : undefined;
  const target = nativeTarget(transport);
  if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(args.agentId)
    || !Number.isInteger(args.limit) || args.limit < 1 || args.limit > 200
    || !['tail', 'before', 'after'].includes(args.direction)
    || Object.keys(args).some(key => !['agentId', 'limit', 'direction', 'cursor', 'timeoutMs'].includes(key))) throw new Error('Invalid reader input');
  const timeoutMs = args.timeoutMs ?? 30000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30000) throw new Error('Invalid read budget');
  const deadline = Date.now() + timeoutMs;
  const remaining = () => { const ms = deadline - Date.now(); if (ms < 1) throw new Error('Read budget exhausted'); return Math.min(10000, ms); };
  if (args.cursor && (typeof args.cursor.epoch !== 'string' || !args.cursor.epoch.length || args.cursor.epoch.length > 128
    || !Number.isSafeInteger(args.cursor.seq) || args.cursor.seq < 0
    || Object.keys(args.cursor).some(key => !['epoch', 'seq'].includes(key)))) throw new Error('Invalid cursor');
  if ((args.direction === 'tail' && args.cursor) || (args.direction !== 'tail' && !args.cursor)) throw new Error('Invalid cursor direction');
  delete process.env.PASEO_PASSWORD;
  const client = await connectToDaemon({ target, timeout: remaining() });
  try {
    await checkNativeIdentity(client, transport);
    const result = await client.fetchAgentTimeline(args.agentId, {
      direction: args.direction, limit: args.limit, projection: 'projected',
      ...(args.cursor ? { cursor: args.cursor } : {}), timeout: remaining(),
    });
    remaining();
    const output = JSON.stringify(result);
    if (Buffer.byteLength(output) > 1024 * 1024) throw new Error('Output limit');
    process.stdout.write(output);
  } finally { await client.close(); }
} catch {
  // No request contents, credentials or daemon exceptions in service logs.
  process.stderr.write('Timeline reader failed\n');
  process.exitCode = 1;
}
