// Use the installed official client. Transport is fixed by the trusted parent;
// request args and environment never select a daemon.
import { connectToDaemon } from '/Applications/Paseo.app/Contents/Resources/app.asar/node_modules/@getpaseo/cli/dist/utils/client.js';
import { waitForAgentResult } from './wait-result.js';
import { nativeTarget, checkNativeIdentity } from './ssh-bridge.mjs';

try {
  const args = JSON.parse(process.argv[2]);
  const transport = process.argv[3] ? JSON.parse(process.argv[3]) : undefined;
  const target = nativeTarget(transport);
  if (!['list_projects', 'wait', 'status'].includes(args.action)) throw new Error('Invalid action');
  if (['list_projects', 'status'].includes(args.action) && Object.keys(args).length !== 1) throw new Error('Invalid input');
  if (args.action === 'wait' && (
    !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(args.agentId)
    || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(args.requestId)
    || !Number.isInteger(args.timeoutMs) || args.timeoutMs < 1 || args.timeoutMs > 20000
    || !Number.isInteger(args.limit) || args.limit < 1 || args.limit > 200
    || !Number.isFinite(Date.parse(args.submittedAt))
    || (args.turnId !== undefined && (typeof args.turnId !== 'string' || !args.turnId.length || args.turnId.length > 256))
    || Object.keys(args).some(k => !['action', 'agentId', 'requestId', 'submittedAt', 'timeoutMs', 'limit', 'turnId'].includes(k)))) throw new Error('Invalid input');
  const started = Date.now();
  delete process.env.PASEO_PASSWORD;
  const client = await connectToDaemon({ target, timeout: Math.min(5000, args.timeoutMs ?? 5000) });
  try {
    await checkNativeIdentity(client, transport);
    let result;
    if (args.action === 'status') {
      const status = await client.getDaemonStatus({ timeout: 2000 });
      if (typeof status.serverId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(status.serverId)) throw new Error('Invalid daemon identity');
      result = { available: true, serverId: status.serverId };
    } else if (args.action === 'list_projects') {
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
