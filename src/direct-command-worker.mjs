// SPDX-License-Identifier: Apache-2.0
// Project-scoped Direct command design inspired by Codex Bridge (see NOTICE).
// Independent Node implementation; no Swift SDK or runtime code is copied.
import { spawn } from 'node:child_process';
import { realpath, stat, lstat, mkdir, readFile, writeFile, chmod } from 'node:fs/promises';
import { resolve, relative, isAbsolute, sep, join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';

const MAX_LINE = 128 * 1024, CHUNK_BYTES = 8192, MAX_WORKER_BUFFER = 64 * 1024;
const commands = new Map(), groups = new Set();
let config, shuttingDown = false, line = '', inputBytes = 0;
const inputDecoder = new StringDecoder('utf8');
const within = (root, value) => { const p = relative(root, value); return p === '' || (!isAbsolute(p) && p !== '..' && !p.startsWith(`..${sep}`)); };
function emit(value, essential = true) {
  if (process.stdout.destroyed) return false;
  if (process.stdout.writableLength > MAX_WORKER_BUFFER) {
    if (!essential) return false;
    // Control replies cannot accumulate without bound either. Losing the
    // transport is explicit at the gateway; it never resubmits the operation.
    shutdown(); return false;
  }
  process.stdout.write(`${JSON.stringify(value)}\n`); return true;
}
function output(record, source, text) {
  // Iterate code points so split UTF-8 characters are never silently corrupted.
  let part = '', bytes = 0;
  const flush = () => {
    if (!part) return;
    if (!emit({ type: 'output', commandId: record.id, source, text: part }, false)) record.dropped++;
    part = ''; bytes = 0;
  };
  for (const character of text) {
    const size = Buffer.byteLength(character);
    if (bytes + size > CHUNK_BYTES) flush();
    part += character; bytes += size;
  }
  flush();
  if (record.dropped && process.stdout.writableLength < MAX_WORKER_BUFFER / 2) {
    emit({ type: 'loss', commandId: record.id, count: record.dropped }); record.dropped = 0;
  }
}
function killGroup(pid, signal) { try { process.kill(-pid, signal); } catch (e) { if (e.code !== 'ESRCH') throw e; } }
function stop(record, reason) {
  if (record.reason) return;
  record.reason = reason;
  if (record.pid) { try { killGroup(record.pid, 'SIGTERM'); } catch {} }
  record.killTimer = setTimeout(() => {
    if (record.pid) { try { killGroup(record.pid, 'SIGKILL'); } catch {} groups.delete(record.pid); }
  }, 300);
}
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const record of commands.values()) stop(record, 'interrupted');
  // Groups are retained even if a command's leader exits before its children.
  for (const pid of groups) { try { killGroup(pid, 'SIGTERM'); } catch {} }
  setTimeout(() => {
    for (const pid of groups) { try { killGroup(pid, 'SIGKILL'); } catch {} }
    process.exit(0);
  }, 400);
}
async function privateRemoteDirectory(base, hostId) {
  if (!isAbsolute(base) || base.split(sep).some(p => p === '..' || p === '.') || base.includes('\0')) throw Error('Invalid remote state directory');
  const dir = join(base, 'direct');
  let current = sep, created = false;
  for (const component of dir.split(sep).filter(Boolean)) {
    current = join(current, component);
    try { await mkdir(current, { mode: 0o700 }); if (current === dir) created = true; } catch (e) { if (e.code !== 'EEXIST') throw e; }
    const s = await lstat(current);
    if (s.isSymbolicLink() || !s.isDirectory()) throw Error('Remote state directory must not contain symlinks');
  }
  const s = await lstat(dir);
  if (process.getuid && s.uid !== process.getuid()) throw Error('Remote worker directory has another owner');
  await chmod(dir, 0o700);
  const marker = join(dir, '.direct-owner.json');
  const expected = JSON.stringify({ hostId, uid: process.getuid?.() ?? null });
  if (!created) {
    const m = await lstat(marker);
    if (!m.isFile() || m.isSymbolicLink() || (process.getuid && m.uid !== process.getuid()) || await readFile(marker, 'utf8') !== expected) throw Error('Remote worker directory marker does not match');
    return;
  }
  try { await writeFile(marker, expected, { flag: 'wx', mode: 0o600 }); }
  catch (e) {
    if (e.code !== 'EEXIST') throw e;
    const m = await lstat(marker);
    if (!m.isFile() || m.isSymbolicLink() || (process.getuid && m.uid !== process.getuid()) || await readFile(marker, 'utf8') !== expected) throw Error('Remote worker directory marker does not match');
  }
}
async function checkedCwd(project, cwd) {
  if (typeof cwd !== 'string' || isAbsolute(cwd) || cwd.includes('\0')) throw Error('cwd must be a project-relative directory');
  const root = await realpath(project.path);
  const roots = await Promise.allSettled(config.allowedRoots.map(path => realpath(path)));
  if (!roots.some(r => r.status === 'fulfilled' && within(r.value, root))) throw Error('Project is outside allowedRoots');
  const path = await realpath(resolve(root, cwd));
  if (!within(root, path) || !(await stat(path)).isDirectory()) throw Error('cwd is outside the project or is not a directory');
  return path;
}
async function run(message) {
  if (shuttingDown) throw Error('Worker is closing');
  if (commands.size >= 16) throw Error('Too many active Direct commands');
  if (commands.has(message.commandId)) throw Error('Duplicate worker command');
  const project = config.projects.find(p => p.id === message.projectId);
  const argv = message.argv;
  if (!project || !Array.isArray(argv) || !argv.length || argv.length > 64 || argv.some(a => typeof a !== 'string' || a.includes('\0')) || Buffer.byteLength(JSON.stringify(argv)) > 16384) throw Error('Invalid Direct command');
  if (project.commandMode !== 'full' && !(project.commandMode === 'registered' && project.commands.some(c => JSON.stringify(c.argv) === JSON.stringify(argv)))) throw Error('Command is not registered with these exact arguments');
  const cwd = await checkedCwd(project, message.cwd);
  if (shuttingDown) throw Error('Worker is closing');
  const child = spawn(argv[0], argv.slice(1), { cwd, stdio: ['pipe', 'pipe', 'pipe'], detached: true, shell: false });
  const record = { id: message.commandId, child, pid: child.pid, dropped: 0, eof: false };
  commands.set(record.id, record);
  if (child.pid) groups.add(child.pid);
  const decoders = { stdout: new StringDecoder('utf8'), stderr: new StringDecoder('utf8') };
  for (const source of ['stdout', 'stderr']) {
    child[source].on('data', bytes => output(record, source, decoders[source].write(bytes)));
    child[source].on('end', () => output(record, source, decoders[source].end()));
  }
  child.stdin.on('error', () => {});
  record.deadline = setTimeout(() => stop(record, 'timed_out'), message.timeoutMs);
  child.once('error', () => { record.spawnError = true; });
  child.once('exit', () => {
    // A finished command cannot leave detached descendants owned by this worker.
    if (!record.reason) stop(record, 'exited');
  });
  child.once('close', (exitCode, signal) => {
    clearTimeout(record.deadline);
    if (record.dropped) emit({ type: 'loss', commandId: record.id, count: record.dropped });
    commands.delete(record.id);
    emit({ type: 'state', commandId: record.id, state: record.spawnError ? 'failed' : record.reason === 'exited' ? 'exited' : record.reason ?? 'exited', exitCode, signal, ...(record.spawnError ? { error: 'Command executable could not be started' } : {}) });
  });
  emit({ type: 'state', commandId: record.id, state: 'running' });
  return { state: 'running' };
}
async function handle(message) {
  if (message.op === 'init') {
    if (config) throw Error('Worker is already initialized');
    if (!Array.isArray(message.config?.projects) || !Array.isArray(message.config?.allowedRoots)) throw Error('Invalid worker configuration');
    if (message.config.remoteStateDir) await privateRemoteDirectory(message.config.remoteStateDir, message.config.hostId);
    config = message.config; return { ready: true };
  }
  if (!config) throw Error('Worker is not initialized');
  if (message.op === 'run') return run(message);
  const record = commands.get(message.commandId);
  if (!record) throw Error('Command is no longer running');
  if (message.op === 'cancel') { stop(record, 'cancelled'); return { cancelRequested: true }; }
  if (message.op === 'input') {
    if (record.eof || record.reason || !record.child.stdin.writable) throw Error('Command stdin is closed');
    if (record.child.stdin.writableLength > 32768) throw Error('Command stdin is backpressured');
    if (message.eof) record.eof = true;
    await new Promise((resolveWrite, reject) => {
      const callback = error => error ? reject(Error('Command stdin write outcome is unknown')) : resolveWrite();
      if (message.eof) record.child.stdin.end(message.text ?? '', callback);
      else record.child.stdin.write(message.text, callback);
    });
    return { state: 'accepted', eof: Boolean(message.eof), delivery: 'Accepted by the stdin pipe; command consumption is not confirmed' };
  }
  throw Error('Unknown worker operation');
}
// Operations are sequential only while validating/spawning. A blocked stdin
// callback must not prevent a cancellation or deadline from terminating it.
let chain = Promise.resolve();
function dispatch(message) {
  const operation = () => {
    if (message.op === 'input') {
      void handle(message).then(result => emit({ replyTo: message.id, ok: true, result }), error => emit({ replyTo: message.id, ok: false, error: error.message }));
      return;
    }
    return handle(message).then(result => emit({ replyTo: message.id, ok: true, result }), error => emit({ replyTo: message.id, ok: false, error: error.message }));
  };
  chain = chain.then(operation).catch(() => shutdown());
}
process.stdin.on('data', bytes => {
  inputBytes += bytes.length;
  if (inputBytes > MAX_LINE) { shutdown(); return; }
  line += inputDecoder.write(bytes);
  let index;
  while ((index = line.indexOf('\n')) >= 0) {
    const packet = line.slice(0, index); line = line.slice(index + 1);
    inputBytes = Buffer.byteLength(line);
    try { dispatch(JSON.parse(packet)); } catch { shutdown(); return; }
  }
});
process.stdin.on('end', shutdown);
process.stdin.on('error', shutdown);
process.stdout.on('error', shutdown);
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
