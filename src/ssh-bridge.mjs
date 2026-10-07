import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { StringDecoder } from 'node:string_decoder';

const HOST = /^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$/;
const USER = /^[A-Za-z_][A-Za-z0-9_.-]{0,63}$/;
const port = value => /^\d{1,5}$/.test(value) && Number(value) >= 1 && Number(value) <= 65535;

/** Owner configuration only. No credentials, offers, shell syntax or extra options. */
export function validateSshTarget(value) {
  if (typeof value !== 'string' || value.length > 400 || /[%\s\\]/u.test(value)) throw new Error('Invalid configured SSH target');
  const match = value.match(/^ssh:\/\/(?:(?<user>[^@/:?]+)@)?(?<host>[^/:?]+)(?::(?<port>\d+))?\/?(?:\?daemonPort=(?<daemon>\d+))?$/);
  if (!match || !HOST.test(match.groups.host) || (match.groups.user && !USER.test(match.groups.user))
    || (match.groups.port && !port(match.groups.port)) || (match.groups.daemon && !port(match.groups.daemon))) throw new Error('Invalid configured SSH target');
  const host = `${match.groups.user ? `${match.groups.user}@` : ''}${match.groups.host}`;
  const sshPort = match.groups.port ? Number(match.groups.port) : undefined;
  const daemonPort = Number(match.groups.daemon ?? 6767);
  return Object.freeze({ host, ...(sshPort ? { sshPort } : {}), daemonPort,
    uri: `ssh://${host}${sshPort ? `:${sshPort}` : ''}${daemonPort !== 6767 ? `?daemonPort=${daemonPort}` : ''}` });
}

export function nativeTarget(config) {
  if (config === undefined) return { kind: 'endpoint', host: '127.0.0.1:6767' };
  if (!config || typeof config !== 'object' || Object.keys(config).some(k => !['target', 'serverId'].includes(k))) throw new Error('Invalid native transport configuration');
  if (config.serverId !== undefined && (typeof config.serverId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(config.serverId))) throw new Error('Invalid native identity');
  return { kind: 'endpoint', host: validateSshTarget(config.target).uri };
}

export async function checkNativeIdentity(client, config) {
  if (!config?.serverId) return;
  const status = await client.getDaemonStatus({ timeout: 2000 });
  if (status.serverId !== config.serverId) throw new Error('Remote daemon identity changed');
}

export function sshArguments(target, { forwarding = false } = {}) {
  const args = ['-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ControlMaster=no', '-o', 'ControlPath=none', '-o', 'PermitLocalCommand=no', '-o', 'RemoteCommand=none',
    '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=2'];
  if (!forwarding) args.push('-o', 'ClearAllForwardings=yes');
  if (target.sshPort) args.push('-p', String(target.sshPort));
  return args;
}

const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
const safeError = reason => Object.assign(new Error(reason), { code: reason });
export function validateNodeCommand(value = 'node') {
  if (value !== 'node' && (typeof value !== 'string' || !/^\/[A-Za-z0-9/._-]+$/.test(value)
    || value.split('/').some(p => p === '..' || p === '.'))) throw new Error('Invalid configured remote Node executable');
  return value;
}

/** Fixed program in argv; every path and file content travels only in stdin JSON. */
export function createSshRunner(target, { spawnImpl = spawn, nodeCommand = 'node' } = {}) {
  validateNodeCommand(nodeCommand);
  const children = new Set();
  let closed = false;
  async function run(script, input, { timeoutMs = 8000, maxBytes = 1024 * 1024, signal } = {}) {
    if (closed) throw safeError('remote_transport_closed');
    signal?.throwIfAborted();
    const data = JSON.stringify(input);
    if (Buffer.byteLength(data) > 1024 * 1024) throw safeError('remote_input_limit');
    return new Promise((resolve, reject) => {
      let child, timer, killTimer, onAbort, done = false, bytes = 0, output = '';
      const decoder = new StringDecoder('utf8');
      const finish = (error, result) => {
        if (done) return;
        done = true; clearTimeout(timer);
        if (onAbort) signal?.removeEventListener('abort', onAbort);
        if (error) {
          try { child?.kill('SIGTERM'); } catch {}
          killTimer = setTimeout(() => { try { child?.kill('SIGKILL'); } catch {} }, 1000); killTimer.unref?.();
          reject(error);
        } else resolve(result);
      };
      try { child = spawnImpl('ssh', [...sshArguments(target), target.host, `${quote(nodeCommand)} --input-type=module -e ${quote(script)}`], { stdio: ['pipe', 'pipe', 'pipe'] }); }
      catch { finish(safeError('remote_connection_failed')); return; }
      children.add(child);
      timer = setTimeout(() => finish(safeError('remote_timeout')), timeoutMs);
      child.stdout.on('data', chunk => {
        bytes += chunk.length;
        if (bytes > maxBytes) finish(safeError('remote_output_limit'));
        else output += decoder.write(chunk);
      });
      child.stderr.on('data', () => {});
      child.stdin.on('error', () => finish(safeError('remote_connection_failed')));
      child.on('error', () => finish(safeError('remote_connection_failed')));
      child.on('close', code => {
        children.delete(child); clearTimeout(killTimer);
        if (code !== 0) finish(safeError('remote_operation_failed'));
        else { try { finish(null, JSON.parse(output + decoder.end())); } catch { finish(safeError('remote_invalid_output')); } }
      });
      onAbort = () => finish(safeError('remote_read_canceled'));
      signal?.addEventListener('abort', onAbort, { once: true });
      if (signal?.aborted) { onAbort(); return; }
      child.stdin.end(data);
    });
  }
  const close = () => { closed = true; for (const child of children) { child.stdin.destroy(); child.kill('SIGTERM'); } };
  return { run, close };
}

const READY_SCRIPT = 'process.stdout.write("bridge-ready\\n");process.stdin.resume();process.stdin.on("end",()=>process.exit(0))';

/** One gateway-owned SSH process. It neither reuses nor controls daemon processes. */
export async function createSshForward(target, { localSocket, remoteSocket, spawnImpl = spawn, onClose = () => {}, timeoutMs = 8000, nodeCommand = 'node' } = {}) {
  validateNodeCommand(nodeCommand);
  let localPort;
  if (!remoteSocket) {
    const reservation = createServer();
    await new Promise((resolve, reject) => { reservation.once('error', reject); reservation.listen(0, '127.0.0.1', resolve); });
    localPort = reservation.address().port;
    await new Promise(resolve => reservation.close(resolve));
  } else if (![remoteSocket, localSocket].every(p => typeof p === 'string' && p.startsWith('/') && !/[:\r\n\0]/.test(p) && Buffer.byteLength(p) < 104)) throw safeError('invalid_socket_path');
  const forwarding = remoteSocket ? ['-R', `${remoteSocket}:${localSocket}`, '-o', 'StreamLocalBindUnlink=no', '-o', 'StreamLocalBindMask=0177']
    : ['-L', `127.0.0.1:${localPort}:127.0.0.1:${target.daemonPort}`];
  return new Promise((resolve, reject) => {
    let child, ready = false, closed = false, output = '', bytes = 0, killTimer;
    const close = () => {
      if (closed) return;
      closed = true;
      try { child.stdin.end(); child.kill('SIGTERM'); } catch {}
      killTimer = setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 1000); killTimer.unref?.();
    };
    const fail = () => { clearTimeout(timer); close(); reject(safeError('remote_tunnel_failed')); };
    try { child = spawnImpl('ssh', [...sshArguments(target, { forwarding: true }), ...forwarding, target.host, `${quote(nodeCommand)} -e ${quote(READY_SCRIPT)}`], { stdio: ['pipe', 'pipe', 'pipe'] }); }
    catch { reject(safeError('remote_tunnel_failed')); return; }
    const timer = setTimeout(fail, timeoutMs);
    child.stderr.on('data', () => {});
    child.stdin.on('error', fail);
    child.on('error', fail);
    child.stdout.on('data', chunk => {
      bytes += chunk.length;
      if (bytes > 256) { fail(); return; }
      output += chunk.toString('utf8');
      if (!ready && output === 'bridge-ready\n') {
        ready = true; clearTimeout(timer);
        resolve({ ...(localPort ? { url: `http://127.0.0.1:${localPort}/mcp/agents` } : {}), close, isAlive: () => ready && !closed });
      }
    });
    child.on('close', () => { clearTimeout(timer); clearTimeout(killTimer); closed = true; if (!ready) reject(safeError('remote_tunnel_failed')); onClose(); });
  });
}

export const REMOTE_PATH_SCRIPT = `
import { realpath, stat } from 'node:fs/promises';
import { posix as path } from 'node:path';
process.stdin.setEncoding('utf8');
let input = ''; for await (const c of process.stdin) { input += c; if (input.length > 65536) process.exit(1); }
try {
  const { value, roots } = JSON.parse(input);
  if (typeof value !== 'string' || !path.isAbsolute(value) || value.includes('\\0') || !Array.isArray(roots) || !roots.length || roots.length > 128) throw Error();
  const resolved = await realpath(value);
  if (!(await stat(resolved)).isDirectory()) throw Error();
  let allowed = false;
  for (const root of roots) {
    if (typeof root !== 'string' || !path.isAbsolute(root)) throw Error();
    try { const canonical = await realpath(root); if (!(await stat(canonical)).isDirectory()) continue;
      const rel = path.relative(canonical, resolved); if (rel === '' || (rel !== '..' && !rel.startsWith('../') && !path.isAbsolute(rel))) allowed = true;
    } catch {}
  }
  if (!allowed) throw Error();
  process.stdout.write(JSON.stringify({ resolved }));
} catch { process.exitCode = 1; }
`;
