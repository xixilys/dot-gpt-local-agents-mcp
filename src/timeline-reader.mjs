// This small read-only helper runs inside Paseo's Electron Node runtime so the
// officially installed client can be loaded from app.asar. It never launches
// agents and never selects its endpoint from arguments or environment.
import { connectToDaemon } from '/Applications/Paseo.app/Contents/Resources/app.asar/node_modules/@getpaseo/cli/dist/utils/client.js';

try {
  const args = JSON.parse(process.argv[2]);
  if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(args.agentId)
    || !Number.isInteger(args.limit) || args.limit < 1 || args.limit > 200
    || !['tail', 'before', 'after'].includes(args.direction)
    || Object.keys(args).some(key => !['agentId', 'limit', 'direction', 'cursor'].includes(key))) throw new Error('Invalid reader input');
  if (args.cursor && (typeof args.cursor.epoch !== 'string' || !args.cursor.epoch.length || args.cursor.epoch.length > 128
    || !Number.isSafeInteger(args.cursor.seq) || args.cursor.seq < 0
    || Object.keys(args.cursor).some(key => !['epoch', 'seq'].includes(key)))) throw new Error('Invalid cursor');
  if ((args.direction === 'tail' && args.cursor) || (args.direction !== 'tail' && !args.cursor)) throw new Error('Invalid cursor direction');
  const client = await connectToDaemon({ target: { kind: 'endpoint', host: '127.0.0.1:6767' }, timeout: 10_000 });
  try {
    const result = await client.fetchAgentTimeline(args.agentId, {
      direction: args.direction, limit: args.limit, projection: 'projected',
      ...(args.cursor ? { cursor: args.cursor } : {}), timeout: 10_000,
    });
    const output = JSON.stringify(result);
    if (Buffer.byteLength(output) > 1024 * 1024) throw new Error('Output limit');
    process.stdout.write(output);
  } finally { await client.close(); }
} catch {
  // No request contents, credentials or daemon exceptions in service logs.
  process.stderr.write('Timeline reader failed\n');
  process.exitCode = 1;
}
