import http from 'node:http';
import net from 'node:net';
import { constants } from 'node:fs';
import { mkdir, chmod, lstat, open, link, unlink } from 'node:fs/promises';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { join, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir } from 'node:os';

export const DEFAULT_AGENT_STATE_DIR = join(homedir(), '.local', 'share', 'local-agents-mcp');
export const AGENT_ID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
export const REQUEST_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
export const MAX_MESSAGE_LENGTH = 8192;
const MAX_BODY_BYTES = 64 * 1024;
const uid = () => process.getuid();
const sameFile = (a, b) => a.dev === b.dev && a.ino === b.ino;

function failure(code) { return Object.assign(new Error(code), { code }); }
function agentFile(stateDir, agentId) {
  if (typeof agentId !== 'string' || !AGENT_ID_PATTERN.test(agentId)) throw failure('invalid_agent_id');
  return join(stateDir, 'agent-channels', `${agentId}.json`);
}

async function privateDirectory(path) {
  await mkdir(path, { mode: 0o700, recursive: true });
  const info = await lstat(path);
  if (!info.isDirectory() || info.uid !== uid()) throw failure('unsafe_channel_directory');
  await chmod(path, 0o700);
}

// Reject links and publicly readable capability files, including on restart.
export async function readAgentCapability(stateDir, agentId) {
  for (const path of [stateDir, join(stateDir, 'agent-channels')]) {
    const info = await lstat(path);
    if (!info.isDirectory() || info.uid !== uid() || (info.mode & 0o077)) throw failure('unsafe_channel_directory');
  }
  const handle = await open(agentFile(stateDir, agentId), constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.uid !== uid() || (info.mode & 0o077) || info.size > 1024) throw failure('unsafe_capability_file');
    const value = JSON.parse(await handle.readFile('utf8'));
    if (value.version !== 1 || value.agentId !== agentId || typeof value.key !== 'string' || !/^[a-f0-9]{64}$/.test(value.key)) throw failure('invalid_capability_file');
    return value.key;
  } finally { await handle.close(); }
}

export function validateAgentMessage(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      Object.keys(value).sort().join(',') !== 'agentId,kind,messageId,requestId,text' ||
      typeof value.agentId !== 'string' || !AGENT_ID_PATTERN.test(value.agentId) ||
      typeof value.requestId !== 'string' || !REQUEST_ID_PATTERN.test(value.requestId) ||
      typeof value.messageId !== 'string' || !AGENT_ID_PATTERN.test(value.messageId) ||
      !['message', 'needs_input'].includes(value.kind) ||
      typeof value.text !== 'string' || value.text.length === 0 || value.text.length > MAX_MESSAGE_LENGTH) throw failure('invalid_message');
  return value;
}

async function socketInUse(path) {
  return new Promise((resolve, reject) => {
    const probe = net.createConnection({ path });
    probe.setTimeout(300);
    probe.once('connect', () => { probe.destroy(); resolve(true); });
    probe.once('timeout', () => { probe.destroy(); reject(failure('socket_probe_timeout')); });
    probe.once('error', error => {
      if (error.code === 'ECONNREFUSED') resolve(false);
      else reject(error);
    });
  });
}

export function createAgentChannel({ stateDir, onMessage, verifyAgent, cliPath = fileURLToPath(new URL('../bin/dot-message.mjs', import.meta.url)) }) {
  if (!isAbsolute(stateDir) || typeof onMessage !== 'function' || typeof verifyAgent !== 'function') throw failure('invalid_channel_options');
  const socketPath = join(stateDir, 'agent-channel.sock');
  // Node removes its bind pathname on close without checking its inode. Bind
  // privately and publish a hard link so cleanup cannot remove a replaced entry.
  const bindPath = join(stateDir, `.ac-${randomBytes(6).toString('hex')}.sock`);
  const issues = new Map();
  let socketIdentity;
  let started = false;
  let listenPromise;
  let closePromise;
  const connections = new Set();
  const server = http.createServer(async (request, response) => {
    const reply = (status, value) => { if (!response.destroyed) { response.writeHead(status, { 'content-type': 'application/json' }); response.end(JSON.stringify(value)); } };
    if (request.method !== 'POST' || request.url !== '/message') { reply(404, { error: 'not_found' }); request.resume(); return; }
    let size = 0;
    const chunks = [];
    try {
      for await (const chunk of request) {
        size += chunk.length;
        if (size > MAX_BODY_BYTES) { reply(413, { error: 'message_too_large' }); request.destroy(); return; }
        chunks.push(chunk);
      }
      let value;
      try { value = validateAgentMessage(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reply(400, { error: 'invalid_message' }); return; }
      let key;
      try { key = await readAgentCapability(stateDir, value.agentId); }
      catch { reply(403, { error: 'unauthorized' }); return; }
      const supplied = typeof request.headers.authorization === 'string' ? request.headers.authorization.match(/^Bearer ([a-f0-9]{64})$/)?.[1] : undefined;
      // Both buffers have fixed size; never compare attacker-controlled lengths.
      const tokenMatches = timingSafeEqual(Buffer.from(key, 'hex'), supplied ? Buffer.from(supplied, 'hex') : Buffer.alloc(32));
      if (!supplied || !tokenMatches) { reply(403, { error: 'unauthorized' }); return; }
      let verified;
      try { verified = await verifyAgent(value.agentId, value); } catch { verified = false; }
      if (verified !== true) { reply(403, { error: 'request_not_authorized' }); return; }
      const receipt = await onMessage(value);
      if (!receipt || receipt.messageId !== value.messageId || !['accepted', 'duplicate'].includes(receipt.status)) throw failure('invalid_receipt');
      reply(200, { messageId: receipt.messageId, status: receipt.status });
    } catch { reply(500, { error: 'acceptance_unknown' }); }
  });
  server.requestTimeout = 5000;
  server.headersTimeout = 5000;
  server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  server.on('clientError', (_error, socket) => { socket.destroy(); });
  server.on('error', () => {}); // Listen errors have their own rejecting handler.

  async function issue(agentId) {
    agentFile(stateDir, agentId);
    if (!issues.has(agentId)) issues.set(agentId, (async () => {
      await privateDirectory(stateDir);
      await privateDirectory(join(stateDir, 'agent-channels'));
      try { await readAgentCapability(stateDir, agentId); }
      catch (error) {
        if (error.code !== 'ENOENT') throw error;
        const destination = agentFile(stateDir, agentId);
        const temporary = `${destination}.${randomBytes(8).toString('hex')}.tmp`;
        try {
          const handle = await open(temporary, 'wx', 0o600);
          try {
            await handle.writeFile(JSON.stringify({ version: 1, agentId, key: randomBytes(32).toString('hex') }));
            await handle.sync();
          } finally { await handle.close(); }
          try { await link(temporary, destination); }
          catch (error) { if (error.code !== 'EEXIST') throw error; await readAgentCapability(stateDir, agentId); }
        } finally { await unlink(temporary).catch(() => {}); }
      }
      return { agentId, stateDir, cliPath };
    })().catch(error => { issues.delete(agentId); throw error; }));
    return issues.get(agentId);
  }

  async function listen() {
    if (closePromise) throw failure('channel_closed');
    if (listenPromise) return listenPromise;
    listenPromise = (async () => {
      await privateDirectory(stateDir);
      await privateDirectory(join(stateDir, 'agent-channels'));
      let previous;
      try { previous = await lstat(socketPath); } catch (error) { if (error.code !== 'ENOENT') throw error; }
      if (previous) {
        if (!previous.isSocket() || previous.uid !== uid()) throw failure('unsafe_existing_socket');
        if (await socketInUse(socketPath)) throw failure('channel_already_listening');
        const current = await lstat(socketPath);
        if (!sameFile(previous, current) || !current.isSocket() || current.uid !== uid()) throw failure('socket_changed');
        await unlink(socketPath);
      }
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(bindPath, () => { server.removeListener('error', reject); resolve(); });
      });
      started = true;
      socketIdentity = await lstat(bindPath);
      await chmod(bindPath, 0o600);
      try { await link(bindPath, socketPath); }
      catch (error) { await new Promise(resolve => server.close(resolve)); started = false; throw error; }
      return { socketPath };
    })();
    return listenPromise;
  }

  async function close() {
    if (closePromise) return closePromise;
    closePromise = (async () => {
      if (listenPromise) await listenPromise.catch(() => {});
      if (!started) return;
      for (const connection of connections) connection.destroy();
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      started = false;
      try {
        const current = await lstat(socketPath);
        if (current.isSocket() && current.uid === uid() && sameFile(socketIdentity, current)) await unlink(socketPath);
      } catch (error) { if (error.code !== 'ENOENT') throw error; }
    })();
    return closePromise;
  }
  return { issue, listen, close };
}
