import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createHash, randomBytes } from 'node:crypto';
import { join, posix } from 'node:path';
import { createAgentChannel, readAgentCapability, AGENT_ID_PATTERN } from './agent-channel.js';
import { createSshForward, validateNodeCommand } from './ssh-bridge.mjs';

export function validateRemoteStateDir(value) {
  if (typeof value !== 'string' || !posix.isAbsolute(value) || value === '/' || posix.normalize(value) !== value
    || /[:\r\n\0]/.test(value) || Buffer.byteLength(`${value}/agent-channel.sock`) >= 104) throw new Error('Invalid remote private state directory');
  return value;
}

// Runs only the operations below. The owner-selected directory and all content
// are stdin data, never remote shell text. No agent can choose an operation.
async function remoteOperation() {
  const fs = await import('node:fs/promises');
  const { constants } = await import('node:fs');
  const { posix: path } = await import('node:path');
  const { createHash, randomBytes } = await import('node:crypto');
  const net = await import('node:net');
  process.stdin.setEncoding('utf8');
  let raw = '';
  for await (const chunk of process.stdin) { raw += chunk; if (Buffer.byteLength(raw) > 1024 * 1024) throw Error(); }
  const input = JSON.parse(raw), dir = input.stateDir, identity = input.identity;
  if (typeof dir !== 'string' || !path.isAbsolute(dir) || path.normalize(dir) !== dir || dir === '/' || /[:\r\n\0]/.test(dir)
    || typeof identity !== 'string' || !/^[a-f0-9]{64}$/.test(identity)) throw Error();
  const uid = process.getuid(), same = (a, b) => a.dev === b.dev && a.ino === b.ino;
  let created = false, current = '/';
  for (const component of dir.split('/').filter(Boolean)) {
    current = path.join(current, component);
    try { await fs.mkdir(current, { mode: 0o700 }); if (current === dir) created = true; }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
    const info = await fs.lstat(current);
    if (!info.isDirectory() || ![0, uid].includes(info.uid)
      || ((info.mode & 0o022) && !(info.uid === 0 && (info.mode & 0o1000)))) throw Error();
    if (current === dir && (info.uid !== uid || (info.mode & 0o077))) throw Error();
  }
  async function privateDir(name) {
    const p = path.join(dir, name);
    try { await fs.mkdir(p, { mode: 0o700 }); } catch (e) { if (e.code !== 'EEXIST') throw e; }
    const info = await fs.lstat(p);
    if (!info.isDirectory() || info.uid !== uid || (info.mode & 0o077)) throw Error();
  }
  async function readPrivate(p, limit = 1024 * 1024) {
    const h = await fs.open(p, constants.O_RDONLY | constants.O_NOFOLLOW);
    try { const info = await h.stat(); if (!info.isFile() || info.uid !== uid || (info.mode & 0o077) || info.size > limit) throw Error();
      return { data: await h.readFile('utf8'), info };
    } finally { await h.close(); }
  }
  const marker = path.join(dir, '.bridge-owner.json');
  if (created) await fs.writeFile(marker, JSON.stringify({ version: 1, identity, files: {} }), { flag: 'wx', mode: 0o600 });
  const ownerRecord = await readPrivate(marker, 8192);
  const owner = JSON.parse(ownerRecord.data);
  if (owner.version !== 1 || owner.identity !== identity || !owner.files || typeof owner.files !== 'object') throw Error();
  const socket = path.join(dir, 'agent-channel.sock');
  const digest = data => createHash('sha256').update(data).digest('hex');
  async function replaceOwned(name, data) {
    const p = path.join(dir, name);
    let old;
    try { old = await readPrivate(p); } catch (e) { if (e.code !== 'ENOENT') throw e; }
    if (old && owner.files[name] !== digest(old.data)) throw Error();
    if (!old) { await fs.writeFile(p, data, { flag: 'wx', mode: 0o600 }); }
    else if (old.data !== data) {
      const temp = path.join(dir, `.update-${randomBytes(8).toString('hex')}`);
      try { await fs.writeFile(temp, data, { flag: 'wx', mode: 0o600 });
        if (!same(old.info, await fs.lstat(p))) throw Error();
        await fs.rename(temp, p);
      } finally { await fs.unlink(temp).catch(() => {}); }
    }
    owner.files[name] = digest(data);
  }
  if (input.op === 'install') {
    await privateDir('bin'); await privateDir('src'); await privateDir('agent-channels');
    const names = ['package.json', 'src/agent-channel.js', 'bin/dot-message.mjs', 'dot-message.mjs'];
    if (Object.keys(input.files).sort().join('|') !== [...names].sort().join('|')) throw Error();
    for (const name of names) { if (typeof input.files[name] !== 'string') throw Error(); await replaceOwned(name, input.files[name]); }
    // This marker is owned, O_NOFOLLOW checked above and never contains keys.
    const h = await fs.open(marker, constants.O_WRONLY | constants.O_NOFOLLOW);
    try { if (!same(ownerRecord.info, await h.stat()) || !same(ownerRecord.info, await fs.lstat(marker))) throw Error(); await h.truncate(0); await h.writeFile(JSON.stringify(owner)); }
    finally { await h.close(); }
  } else if (input.op === 'issue') {
    const c = input.capability;
    if (!c || c.version !== 1 || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(c.agentId)
      || !/^[a-f0-9]{64}$/.test(c.key) || Object.keys(c).sort().join(',') !== 'agentId,key,version') throw Error();
    await privateDir('agent-channels');
    const p = path.join(dir, 'agent-channels', `${c.agentId}.json`);
    try { await fs.writeFile(p, JSON.stringify(c), { flag: 'wx', mode: 0o600 }); }
    catch (e) { if (e.code !== 'EEXIST') throw e; const old = JSON.parse((await readPrivate(p, 1024)).data);
      if (old.version !== 1 || old.agentId !== c.agentId || old.key !== c.key) throw Error(); }
  } else if (input.op === 'prepare-socket') {
    let old; try { old = await fs.lstat(socket); } catch (e) { if (e.code !== 'ENOENT') throw e; }
    if (old) {
      if (!old.isSocket() || old.uid !== uid || (old.mode & 0o077)) throw Error();
      const active = await new Promise((resolve, reject) => {
        const s = net.createConnection({ path: socket });
        s.setTimeout(500); s.once('connect', () => { s.destroy(); resolve(true); });
        s.once('timeout', () => { s.destroy(); reject(Error()); });
        s.once('error', e => e.code === 'ECONNREFUSED' ? resolve(false) : reject(Error()));
      });
      if (active || !same(old, await fs.lstat(socket))) throw Error();
      await fs.unlink(socket);
    }
  } else if (input.op === 'socket-ready') {
    if (!/^\.r-[a-f0-9]{16}\.sock$/.test(input.bindName)) throw Error();
    const binding = path.join(dir, input.bindName);
    const info = await fs.lstat(binding);
    if (!info.isSocket() || info.uid !== uid || (info.mode & 0o077)) throw Error();
    // Publish a hard link: SSH can clean up its private binding without ever
    // deleting a replacement of the public message socket on disconnect.
    await fs.link(binding, socket);
    process.stdout.write(JSON.stringify({ dev: info.dev, ino: info.ino })); return;
  } else if (input.op === 'cleanup-socket') {
    const names = [socket];
    if (/^\.r-[a-f0-9]{16}\.sock$/.test(input.bindName ?? '')) names.push(path.join(dir, input.bindName));
    for (const name of names) {
      let info; try { info = await fs.lstat(name); } catch (e) { if (e.code !== 'ENOENT') throw e; }
      if (info && info.isSocket() && info.uid === uid && info.dev === input.socketIdentity?.dev && info.ino === input.socketIdentity?.ino) await fs.unlink(name);
    }
  } else throw Error();
  process.stdout.write(JSON.stringify({ ok: true }));
}

export const REMOTE_CHANNEL_SCRIPT = `try { await (${remoteOperation.toString()})(); } catch { process.exitCode = 1; }`;
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;

export function createRemoteChannel({ host, target, stateDir, runRemote, verifyAgent, onMessage,
  forwardFactory = createSshForward, localChannelFactory = createAgentChannel }) {
  const remoteStateDir = validateRemoteStateDir(host.remoteStateDir);
  const identity = createHash('sha256').update(JSON.stringify({ id: host.id, target: target.uri, remoteStateDir })).digest('hex');
  const command = `${quote(validateNodeCommand(host.nodeCommand))} ${quote(posix.join(remoteStateDir, 'dot-message.mjs'))}`;
  const local = localChannelFactory({ stateDir, verifyAgent, onMessage });
  let tunnel, socketIdentity, bindName, installed, listening, closed = false;
  const operation = (op, data = {}) => runRemote(REMOTE_CHANNEL_SCRIPT, { op, stateDir: remoteStateDir, identity, ...data });
  async function install() {
    if (!installed) installed = (async () => {
      const [library, channel] = await Promise.all([
        readFile(fileURLToPath(new URL('../bin/dot-message.mjs', import.meta.url)), 'utf8'),
        readFile(fileURLToPath(new URL('./agent-channel.js', import.meta.url)), 'utf8'),
      ]);
      await operation('install', { files: {
        'package.json': JSON.stringify({ type: 'module', private: true }),
        'src/agent-channel.js': channel, 'bin/dot-message.mjs': library,
        'dot-message.mjs': `import { runDotMessage } from './bin/dot-message.mjs';\nprocess.exitCode = await runDotMessage(process.argv.slice(2), { stateDir: ${JSON.stringify(remoteStateDir)} });\n`,
      } });
    })().catch(e => { installed = undefined; throw e; });
    return installed;
  }
  async function listen() {
    if (closed) throw new Error('Remote channel closed');
    return local.listen();
  }
  async function prepare() {
    if (closed) throw new Error('Remote channel closed');
    if (listening) return listening;
    listening = (async () => {
      const { socketPath } = await local.listen();
      await install(); await operation('prepare-socket');
      bindName = `.r-${randomBytes(8).toString('hex')}.sock`;
      tunnel = await forwardFactory(target, { localSocket: socketPath, remoteSocket: posix.join(remoteStateDir, bindName),
        onClose: () => { listening = undefined; } });
      if (closed) { tunnel.close(); throw new Error('Remote channel closed'); }
      try { socketIdentity = await operation('socket-ready', { bindName }); }
      catch (e) { tunnel.close(); throw e; }
      return { socketPath: posix.join(remoteStateDir, 'agent-channel.sock') };
    })().catch(e => { listening = undefined; throw e; });
    return listening;
  }
  async function issue(agentId) {
    if (closed || typeof agentId !== 'string' || !AGENT_ID_PATTERN.test(agentId)) throw new Error('Invalid remote channel issue');
    await prepare(); await local.issue(agentId);
    const key = await readAgentCapability(stateDir, agentId);
    await operation('issue', { capability: { version: 1, agentId, key } });
    return { agentId, stateDir: remoteStateDir, cliPath: posix.join(remoteStateDir, 'dot-message.mjs') };
  }
  async function close() {
    if (closed) return; closed = true;
    if (listening) await listening.catch(() => {});
    tunnel?.close();
    if (socketIdentity) await operation('cleanup-socket', { socketIdentity, bindName }).catch(() => {});
    await local.close();
  }
  return { listen, prepare, issue, close, command };
}
