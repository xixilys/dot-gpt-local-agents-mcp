// Use the already installed Paseo client and fixed local daemon. No new service,
// session import, private session-file reads, or configurable endpoint.
import { connectToDaemon } from '/Applications/Paseo.app/Contents/Resources/app.asar/node_modules/@getpaseo/cli/dist/utils/client.js';
import { waitForAgentResult } from './wait-result.js';

try {
  const args = JSON.parse(process.argv[2]);
  if (!['list_projects', 'wait'].includes(args.action)) throw new Error('Invalid action');
  if (args.action === 'list_projects' && Object.keys(args).length !== 1) throw new Error('Invalid input');
  if (args.action === 'wait' && (
    !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(args.agentId)
    || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(args.requestId)
    || !Number.isInteger(args.timeoutMs) || args.timeoutMs < 1 || args.timeoutMs > 20000
    || !Number.isInteger(args.limit) || args.limit < 1 || args.limit > 200
    || !Number.isFinite(Date.parse(args.submittedAt))
    || (args.turnId !== undefined && (typeof args.turnId !== 'string' || !args.turnId.length || args.turnId.length > 256))
    || Object.keys(args).some(k => !['action', 'agentId', 'requestId', 'submittedAt', 'timeoutMs', 'limit', 'turnId'].includes(k)))) throw new Error('Invalid input');
  const started = Date.now();
  const client = await connectToDaemon({ target: { kind: 'endpoint', host: '127.0.0.1:6767' }, timeout: Math.min(2000, args.timeoutMs ?? 2000) });
  try {
    let result;
    if (args.action === 'list_projects') {
      const payload = await client.listProjects();
      if (!Array.isArray(payload.projects) || payload.error) throw new Error('Invalid projects');
      result = { projects: payload.projects.map(p => ({ projectId: p.projectId, name: p.projectDisplayName, kind: p.projectKind, path: p.projectRootPath })) };
    } else {
      result = await waitForAgentResult(client, { ...args, timeoutMs: Math.max(1, args.timeoutMs - (Date.now() - started)) });
    }
    const output = JSON.stringify(result);
    if (Buffer.byteLength(output) > 1024 * 1024) throw new Error('Output limit');
    process.stdout.write(output);
  } finally { await client.close(); }
} catch {
  process.stderr.write('Daemon reader failed\n');
  process.exitCode = 1;
}
