import { tmpdir } from 'node:os';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, lstat, readFile, writeFile, unlink, symlink, rm, chmod } from 'node:fs/promises';
import { join } from 'node:path';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import net from 'node:net';
import { createRemoteChannel, validateRemoteStateDir } from '../src/remote-channel.js';
import { validateSshTarget } from '../src/ssh-bridge.mjs';

const execute = promisify(execFile);
const A = '11111111-1111-4111-8111-111111111111';
const M = '22222222-2222-4222-8222-222222222222';
async function localRemote(script, input) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['pipe', 'pipe', 'pipe'] });
    let output = ''; child.stdout.setEncoding('utf8'); child.stdout.on('data', c => { output += c; }); child.stderr.on('data', () => {});
    child.on('error', reject); child.on('close', code => { if (code !== 0) reject(Error('remote_operation_failed')); else { try { resolve(JSON.parse(output)); } catch (e) { reject(e); } } });
    child.stdin.end(JSON.stringify(input));
  });
}
async function forward(_target, { localSocket, remoteSocket, onClose = () => {} }) {
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket); socket.once('close', () => sockets.delete(socket));
    const upstream = net.createConnection(localSocket);
    upstream.on('error', () => socket.destroy()); socket.on('error', () => upstream.destroy());
    socket.pipe(upstream); upstream.pipe(socket); socket.once('close', () => upstream.destroy());
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(remoteSocket, resolve); });
  await chmod(remoteSocket, 0o600);
  server.once('close', onClose);
  return { close: () => { for (const socket of sockets) socket.destroy(); return new Promise(resolve => server.close(resolve)); }, isAlive: () => server.listening };
}
async function fixture(t, overrides = {}) {
  const root = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'rc-'));
  const stateDir = join(root, 'l'), remoteStateDir = join(root, 'r');
  const host = { id: 'wsl', target: 'ssh://wsl', remoteStateDir, nodeCommand: process.execPath };
  const receipts = new Map(), messages = [];
  const channel = createRemoteChannel({ host, target: validateSshTarget(host.target), stateDir, runRemote: localRemote, forwardFactory: forward,
    verifyAgent: async (id, message) => id === A && message.requestId === 'request-one',
    onMessage: async value => { messages.push(value); const status = receipts.has(value.messageId) ? 'duplicate' : 'accepted'; receipts.set(value.messageId, value); return { messageId: value.messageId, status }; }, ...overrides });
  t.after(async () => { await channel.close(); await rm(root, { recursive: true, force: true }); });
  return { root, stateDir, remoteStateDir, host, channel, messages };
}

test('fixed remote wrapper delivers and recovers same messageId over its host private socket', async t => {
  const f = await fixture(t);
  await f.channel.listen();
  await assert.rejects(lstat(f.remoteStateDir), { code: 'ENOENT' });
  await f.channel.prepare(); await f.channel.issue(A);
  const localCap = await readFile(join(f.stateDir, 'agent-channels', `${A}.json`), 'utf8');
  const remoteCap = await readFile(join(f.remoteStateDir, 'agent-channels', `${A}.json`), 'utf8');
  assert.equal(localCap, remoteCap);
  assert.equal((await lstat(join(f.remoteStateDir, 'agent-channels', `${A}.json`))).mode & 0o777, 0o600);
  assert.equal((await lstat(f.remoteStateDir)).mode & 0o777, 0o700);
  const command = `${f.channel.command} --request-id request-one --kind message --message-id ${M} --text hello`;
  const options = { env: { ...process.env, PASEO_AGENT_ID: A } };
  const first = JSON.parse((await execute('/bin/sh', ['-c', command], options)).stdout);
  const duplicate = JSON.parse((await execute('/bin/sh', ['-c', command], options)).stdout);
  assert.deepEqual(first, { messageId: M, status: 'accepted' });
  assert.deepEqual(duplicate, { messageId: M, status: 'duplicate' });
  assert.equal(f.messages[0].text, 'hello'); assert.equal(f.messages[0].agentId, A);
  assert.equal(command.includes(JSON.parse(localCap).key), false);
  const wrong = command.replace('request-one', 'request-wrong');
  await assert.rejects(execute('/bin/sh', ['-c', wrong], options), error => {
    assert.equal(JSON.parse(error.stderr.split('\n')[0]).status, 'rejected'); return true;
  });
});

test('a swapped remote capability symlink is rejected and never overwrites its target', async t => {
  const f = await fixture(t); await f.channel.issue(A);
  const capability = join(f.remoteStateDir, 'agent-channels', `${A}.json`), outside = join(f.root, 'unrelated');
  await writeFile(outside, 'preserve', { mode: 0o600 }); await unlink(capability); await symlink(outside, capability);
  await assert.rejects(f.channel.issue(A), /remote_operation_failed/);
  assert.equal(await readFile(outside, 'utf8'), 'preserve');
});

test('remote directory symlinks and directories without gateway ownership are rejected', async t => {
  const f = await fixture(t); const outside = join(f.root, 'outside');
  await mkdir(outside, { mode: 0o700 }); await symlink(outside, f.remoteStateDir);
  await assert.rejects(f.channel.prepare(), /remote_operation_failed/);
  await unlink(f.remoteStateDir); await mkdir(f.remoteStateDir, { mode: 0o700 });
  await writeFile(join(f.remoteStateDir, 'user-data'), 'preserve');
  await assert.rejects(f.channel.prepare(), /remote_operation_failed/);
  assert.equal(await readFile(join(f.remoteStateDir, 'user-data'), 'utf8'), 'preserve');
});

test('remote state directory cannot be reused after a configured host target changes', async t => {
  const f = await fixture(t); await f.channel.prepare(); await f.channel.close();
  const other = createRemoteChannel({ host: { ...f.host, target: 'ssh://other' }, target: validateSshTarget('ssh://other'), stateDir: join(f.root, 'l2'),
    runRemote: localRemote, forwardFactory: forward, verifyAgent: async () => true, onMessage: async () => ({}) });
  t.after(() => other.close());
  await assert.rejects(other.prepare(), /remote_operation_failed/);
});

test('helper files modified outside this gateway are preserved and block reinstall', async t => {
  const f = await fixture(t); await f.channel.prepare(); await f.channel.close();
  const helper = join(f.remoteStateDir, 'dot-message.mjs'); await writeFile(helper, 'owner-modified', { mode: 0o600 });
  const next = createRemoteChannel({ host: f.host, target: validateSshTarget(f.host.target), stateDir: join(f.root, 'l2'), runRemote: localRemote,
    forwardFactory: forward, verifyAgent: async () => true, onMessage: async () => ({}) });
  t.after(() => next.close());
  await assert.rejects(next.prepare(), /remote_operation_failed/); assert.equal(await readFile(helper, 'utf8'), 'owner-modified');
});

test('close preserves a replacement of the public remote socket', async t => {
  const f = await fixture(t); await f.channel.prepare();
  const publicSocket = join(f.remoteStateDir, 'agent-channel.sock');
  await unlink(publicSocket);
  const replacement = net.createServer();
  await new Promise(resolve => replacement.listen(publicSocket, resolve)); await chmod(publicSocket, 0o600);
  t.after(() => new Promise(resolve => replacement.close(resolve)));
  const before = await lstat(publicSocket);
  await f.channel.close();
  assert.equal((await lstat(publicSocket)).ino, before.ino);
});

test('a lost owned reverse forward restores a stale socket and the existing capability without sending a prompt', async t => {
  const forwards = [];
  const f = await fixture(t, { forwardFactory: async (...args) => { const tunnel = await forward(...args); forwards.push(tunnel); return tunnel; } });
  await f.channel.issue(A);
  const capability = join(f.remoteStateDir, 'agent-channels', `${A}.json`);
  const before = await readFile(capability, 'utf8');
  await forwards[0].close();
  assert.equal((await lstat(join(f.remoteStateDir, 'agent-channel.sock'))).isSocket(), true);
  await f.channel.prepare(); await f.channel.issue(A);
  assert.equal(forwards.length, 2); assert.equal(await readFile(capability, 'utf8'), before);
  const { stdout } = await execute('/bin/sh', ['-c', `${f.channel.command} --request-id request-one --kind message --message-id ${M} --text restored`], { env: { ...process.env, PASEO_AGENT_ID: A } });
  assert.deepEqual(JSON.parse(stdout), { messageId: M, status: 'accepted' });
  assert.equal(f.messages.length, 1); assert.equal(f.messages[0].text, 'restored');
});

test('remote state and executable path validation rejects forward syntax injection', () => {
  for (const value of ['relative', '/', '/home/test/../bridge', '/home/test/bridge:x', '/home/test/bridge\n']) assert.throws(() => validateRemoteStateDir(value));
});
