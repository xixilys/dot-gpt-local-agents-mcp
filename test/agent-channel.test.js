import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp, readFile, writeFile, lstat, unlink, rm, symlink, chmod } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { createAgentChannel, readAgentCapability } from '../src/agent-channel.js';
import { runDotMessage } from '../bin/dot-message.mjs';

const agentId = '11111111-1111-4111-8111-111111111111';
const otherId = '22222222-2222-4222-8222-222222222222';
const value = (patch = {}) => ({ agentId, requestId: 'test-request', messageId: randomUUID(), kind: 'message', text: 'private body', ...patch });
async function fixture(t, callbacks = {}) {
  const stateDir = await mkdtemp(join(tmpdir(), 'ac-'));
  const received = new Map();
  const channel = createAgentChannel({ stateDir, verifyAgent: async () => true, onMessage: async message => {
    const duplicate = received.has(message.messageId);
    if (!duplicate) received.set(message.messageId, message);
    return { messageId: message.messageId, status: duplicate ? 'duplicate' : 'accepted', text: 'must not leak this field' };
  }, ...callbacks });
  t.after(async () => { await channel.close(); await rm(stateDir, { recursive: true, force: true }); });
  const info = await channel.issue(agentId);
  return { stateDir, channel, received, info, key: await readAgentCapability(stateDir, agentId), socketPath: join(stateDir, 'agent-channel.sock') };
}
function post(socketPath, key, message) {
  return new Promise((resolve, reject) => {
    const request = http.request({ socketPath, method: 'POST', path: '/message', headers: { authorization: `Bearer ${key}` } }, response => {
      let data = '';
      response.on('data', chunk => { data += chunk; });
      response.once('end', () => resolve({ status: response.statusCode, body: JSON.parse(data) }));
      response.once('error', reject);
    });
    request.once('error', reject);
    request.end(typeof message === 'string' ? message : JSON.stringify(message));
  });
}
function outputs() {
  const result = { stdout: '', stderr: '' };
  return { result, stdout: { write: text => { result.stdout += text; } }, stderr: { write: text => { result.stderr += text; } } };
}
function helperOptions(f, extra = {}) { return { stateDir: f.stateDir, env: { PASEO_AGENT_ID: agentId }, ...extra }; }

test('real private socket accepts durable receipts and replay preserves one message', async t => {
  const f = await fixture(t);
  await f.channel.listen();
  const message = value();
  assert.deepEqual(await post(f.socketPath, f.key, message), { status: 200, body: { messageId: message.messageId, status: 'accepted' } });
  assert.deepEqual(await post(f.socketPath, f.key, message), { status: 200, body: { messageId: message.messageId, status: 'duplicate' } });
  assert.equal(f.received.size, 1);
  assert.deepEqual(f.info, { agentId, stateDir: f.stateDir, cliPath: new URL('../bin/dot-message.mjs', import.meta.url).pathname });
  for (const [path, mode] of [[f.stateDir, 0o700], [join(f.stateDir, 'agent-channels'), 0o700], [join(f.stateDir, 'agent-channels', `${agentId}.json`), 0o600], [f.socketPath, 0o600]]) {
    assert.equal((await lstat(path)).mode & 0o777, mode);
  }
  await f.channel.close();
  await assert.rejects(lstat(f.socketPath), { code: 'ENOENT' });
});

test('wrong capability cannot claim another agent and authorization runs before persistence', async t => {
  const verified = [];
  const f = await fixture(t, { verifyAgent: async (id, message) => { verified.push([id, message.requestId]); return message.requestId === 'test-request'; } });
  await f.channel.issue(otherId);
  await f.channel.listen();
  assert.equal((await post(f.socketPath, 'f'.repeat(64), value())).status, 403);
  assert.equal((await post(f.socketPath, f.key, value({ agentId: otherId }))).status, 403);
  assert.equal(verified.length, 0);
  assert.equal((await post(f.socketPath, f.key, value({ requestId: 'wrong-request' }))).status, 403);
  assert.equal(verified.length, 1);
  assert.equal(f.received.size, 0);
});

test('malformed request, extra fields, oversized text, invalid kind, and non UUID are rejected', async t => {
  const f = await fixture(t);
  await f.channel.listen();
  for (const message of ['{', value({ requestId: '../escape' }), value({ text: 'x'.repeat(8193) }), value({ kind: 'finished' }), value({ messageId: 'wrong' }), { ...value(), token: 'no' }]) {
    assert.equal((await post(f.socketPath, f.key, message)).status, 400);
  }
  assert.equal(f.received.size, 0);
});

test('callback errors and non durable replies reveal no raw input or credentials', async t => {
  const f = await fixture(t, { onMessage: async () => { throw new Error('secret body and capability'); } });
  await f.channel.listen();
  assert.deepEqual(await post(f.socketPath, f.key, value()), { status: 500, body: { error: 'acceptance_unknown' } });
  const bad = await fixture(t, { onMessage: async message => ({ messageId: message.messageId, status: 'delivered' }) });
  await bad.channel.listen();
  assert.deepEqual(await post(bad.socketPath, bad.key, value()), { status: 500, body: { error: 'acceptance_unknown' } });
});

test('issue is atomic, stable across restart, and never exposes the capability in channel info', async t => {
  const f = await fixture(t);
  const infos = await Promise.all(Array.from({ length: 8 }, () => f.channel.issue(agentId)));
  assert.equal(await readAgentCapability(f.stateDir, agentId), f.key);
  assert.equal(JSON.stringify(infos).includes(f.key), false);
  const next = createAgentChannel({ stateDir: f.stateDir, verifyAgent: async () => true, onMessage: async message => ({ messageId: message.messageId, status: 'accepted' }) });
  await next.issue(agentId);
  assert.equal(await readAgentCapability(f.stateDir, agentId), f.key);
  await next.close();
});

test('capability links and permissive files are rejected', async t => {
  const f = await fixture(t);
  const path = join(f.stateDir, 'agent-channels', `${agentId}.json`);
  await chmod(path, 0o644);
  await assert.rejects(readAgentCapability(f.stateDir, agentId), /unsafe_capability_file/);
  await chmod(path, 0o600);
  const content = await readFile(path);
  await unlink(path);
  const target = join(f.stateDir, 'outside.json');
  await writeFile(target, content, { mode: 0o600 });
  await symlink(target, path);
  await assert.rejects(readAgentCapability(f.stateDir, agentId));
});

test('listen and close preserve an unrelated existing file and active socket', async t => {
  const f = await fixture(t);
  await writeFile(f.socketPath, 'unrelated');
  await assert.rejects(f.channel.listen(), /unsafe_existing_socket/);
  await f.channel.close();
  assert.equal(await readFile(f.socketPath, 'utf8'), 'unrelated');
  const active = await fixture(t);
  await active.channel.listen();
  const second = createAgentChannel({ stateDir: active.stateDir, verifyAgent: async () => true, onMessage: async () => {} });
  await assert.rejects(second.listen(), /channel_already_listening/);
  await second.close();
  assert.equal((await post(active.socketPath, active.key, value())).status, 200);
});

test('close checks ownership of published socket and preserves replacement file', async t => {
  const f = await fixture(t);
  await f.channel.listen();
  await unlink(f.socketPath);
  await writeFile(f.socketPath, 'replacement');
  await f.channel.close();
  assert.equal(await readFile(f.socketPath, 'utf8'), 'replacement');
});

test('a verified owned stale socket is reclaimed', async t => {
  const f = await fixture(t);
  const child = spawn(process.execPath, ['--input-type=module', '-e', 'import net from "node:net"; const server=net.createServer();server.listen(process.argv[1],()=>console.log("ready"));', f.socketPath], { stdio: ['ignore', 'pipe', 'ignore'] });
  t.after(() => { if (child.exitCode === null) child.kill('SIGKILL'); });
  await new Promise((resolve, reject) => { child.stdout.once('data', resolve); child.once('error', reject); child.once('exit', () => reject(new Error('stale fixture exited early'))); });
  const exited = new Promise(resolve => child.once('exit', resolve));
  child.kill('SIGKILL');
  await exited;
  assert.equal((await lstat(f.socketPath)).isSocket(), true);
  await f.channel.listen();
  assert.equal((await post(f.socketPath, f.key, value())).status, 200);
});

test('helper reads stdin and emits only messageId and persistent receipt status', async t => {
  const f = await fixture(t);
  await f.channel.listen();
  const output = outputs();
  const messageId = randomUUID();
  const text = 'private stdin 文本';
  const bytes = Buffer.from(text);
  assert.equal(await runDotMessage(['--request-id', 'test-request', '--kind', 'needs_input', '--message-id', messageId], helperOptions(f, { ...output, stdin: Readable.from([...bytes].map(byte => Buffer.from([byte]))) })), 0);
  assert.deepEqual(JSON.parse(output.result.stdout), { messageId, status: 'accepted' });
  assert.equal(output.result.stderr, '');
  assert.equal(f.received.get(messageId).text, text);
  assert.equal(f.received.get(messageId).kind, 'needs_input');
  assert.equal(output.result.stdout.includes(f.key), false);
});

test('helper retries startup with the same ID but does not resend after receipt', async t => {
  const stateDir = await mkdtemp(join(tmpdir(), 'ac-'));
  const received = [];
  const channel = createAgentChannel({ stateDir, verifyAgent: async () => true, onMessage: async message => { received.push(message); return { messageId: message.messageId, status: 'accepted' }; } });
  t.after(async () => { await channel.close(); await rm(stateDir, { recursive: true, force: true }); });
  const output = outputs();
  const running = runDotMessage(['--request-id', 'test-request', '--text', 'text'], { stateDir, env: { PASEO_AGENT_ID: agentId }, ...output, readyTimeoutMs: 500, retryDelayMs: 10 });
  await new Promise(resolve => setTimeout(resolve, 30));
  await channel.issue(agentId);
  await channel.listen();
  assert.equal(await running, 0);
  assert.equal(received.length, 1);
  assert.equal(received[0].messageId, JSON.parse(output.result.stdout).messageId);
});

test('not ready waiting is bounded and invalid identity/CLI override is refused', async t => {
  const f = await fixture(t);
  const output = outputs();
  const messageId = randomUUID();
  const start = Date.now();
  assert.equal(await runDotMessage(['--request-id', 'test-request', '--message-id', messageId, '--text', 'text'], helperOptions(f, { ...output, readyTimeoutMs: 70, retryDelayMs: 10 })), 1);
  assert.ok(Date.now() - start >= 60 && Date.now() - start < 1000);
  assert.deepEqual(JSON.parse(output.result.stderr), { messageId, status: 'channel_not_ready', reason: 'ENOENT' });
  for (const [args, env] of [[['--agent-id', otherId, '--text', 'text'], { PASEO_AGENT_ID: agentId }], [['--request-id', 'test-request', '--text', 'text'], {}], [['--socket', f.socketPath, '--text', 'text'], { PASEO_AGENT_ID: agentId }]]) {
    const invalid = outputs();
    assert.equal(await runDotMessage(args, { stateDir: f.stateDir, env, ...invalid }), 2);
    assert.equal(JSON.parse(invalid.result.stderr).status, 'invalid_input');
  }
});

test('connection loss after acceptance is unknown with reusable ID and no automatic replay', async t => {
  const f = await fixture(t);
  let count = 0;
  const server = http.createServer(async (request, response) => { for await (const _chunk of request) {} count++; response.destroy(); });
  await new Promise(resolve => server.listen(f.socketPath, resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const output = outputs();
  const messageId = randomUUID();
  assert.equal(await runDotMessage(['--request-id', 'test-request', '--message-id', messageId, '--text', 'private text'], helperOptions(f, output)), 1);
  assert.equal(count, 1);
  assert.equal(output.result.stdout, '');
  assert.deepEqual(JSON.parse(output.result.stderr.split('\n')[0]), { messageId, status: 'outcome_unknown', reason: 'local_channel_error' });
  assert.match(output.result.stderr, /same --message-id/);
  assert.equal(output.result.stderr.includes('private text'), false);
  assert.equal(output.result.stderr.includes(f.key), false);
});
