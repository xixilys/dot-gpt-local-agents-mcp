import { tmpdir } from 'node:os';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, symlink, writeFile, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { validateSshTarget, RemotePathPolicy, createRemoteHostAdapter } from '../src/host-transport.js';
import { createSshRunner, createSshForward, nativeTarget, checkNativeIdentity } from '../src/ssh-bridge.mjs';
import { PaseoUpstream } from '../src/upstream.js';
import { pinHost } from '../src/hosts.js';

export function localRemote(script, input) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['pipe', 'pipe', 'pipe'] });
    let output = ''; child.stdout.setEncoding('utf8'); child.stdout.on('data', c => { output += c; });
    child.stderr.on('data', () => {}); child.on('error', reject);
    child.on('close', code => { if (code !== 0) reject(new Error('remote_operation_failed')); else { try { resolve(JSON.parse(output)); } catch (e) { reject(e); } } });
    child.stdin.end(JSON.stringify(input));
  });
}
const host = { id: 'wsl', transport: 'ssh', target: 'ssh://wsl', remoteStateDir: '/home/test/.local/share/bridge/gateway', allowedRoots: ['/home/test/project'] };
const rpcFetch = async (_url, options) => {
  const request = JSON.parse(options.body);
  return new Response(JSON.stringify({ jsonrpc: '2.0', id: request.id, result: request.method === 'tools/list' ? { tools: [] } : { structuredContent: { ok: true } } }), { headers: { 'content-type': 'application/json' } });
};

test('SSH target accepts only owner SSH alias/user/port/daemonPort forms', () => {
  assert.deepEqual(validateSshTarget('ssh://me@wsl:2222?daemonPort=7777'), { host: 'me@wsl', sshPort: 2222, daemonPort: 7777, uri: 'ssh://me@wsl:2222?daemonPort=7777' });
  assert.equal(validateSshTarget('ssh://wsl/').uri, 'ssh://wsl');
  for (const value of ['https://wsl', 'ssh://-x', 'ssh://u:password@wsl', 'ssh://wsl?a=b', 'ssh://wsl?daemonPort=1&daemonPort=2',
    'ssh://wsl?password=x', 'ssh://wsl/path', 'ssh://wsl/../', 'ssh://wsl#offer', 'ssh://%77sl', 'ssh://wsl:0', 'ssh://wsl:65536',
    'ssh://wsl?daemonPort=65536', 'ssh://wsl\n', 'ssh://user;whoami@wsl', 'ssh://wsl$(id)', 'ssh://wsl\\x']) assert.throws(() => validateSshTarget(value), /Invalid/);
  assert.deepEqual(nativeTarget(), { kind: 'endpoint', host: '127.0.0.1:6767' });
  assert.throws(() => nativeTarget({ target: 'ssh://wsl', command: 'id' }), /Invalid/);
});

test('native identity is checked against a fixed parent-provided serverId', async () => {
  await checkNativeIdentity({ getDaemonStatus: async () => ({ serverId: 'srv_one' }) }, { serverId: 'srv_one' });
  await assert.rejects(checkNativeIdentity({ getDaemonStatus: async () => ({ serverId: 'srv_two' }) }, { serverId: 'srv_one' }), /identity changed/);
});

test('remote paths use target canonical roots, reject escape/missing/non-directory and fail closed offline', async t => {
  const dir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'hp-')); t.after(() => rm(dir, { recursive: true, force: true }));
  const allowed = join(dir, 'project'), outside = join(dir, 'outside');
  await mkdir(allowed); await mkdir(outside); await mkdir(join(allowed, 'child'));
  await symlink(outside, join(allowed, 'escape')); await symlink(allowed, join(dir, 'alias'));
  await writeFile(join(allowed, 'file'), 'x');
  const policy = new RemotePathPolicy([join(dir, 'missing-root'), join(dir, 'alias')], localRemote);
  assert.equal(await policy.check(join(allowed, 'child')), join(allowed, 'child'));
  for (const value of [outside, join(allowed, 'escape'), join(allowed, 'absent'), join(allowed, 'file')]) await assert.rejects(policy.check(value), /could not be verified/);
  await assert.rejects(policy.check('relative'), /absolute/);
  await assert.rejects(new RemotePathPolicy([allowed], async () => { throw Error('secret transport detail'); }).check(allowed), /could not be verified/);
});

test('alternate upstream permits only an explicitly owned numeric loopback port with redirects disabled', async () => {
  for (const url of ['http://example.com:1234/mcp/agents', 'http://localhost:1234/mcp/agents', 'http://127.0.0.2:1234/mcp/agents',
    'http://127.0.0.1:65536/mcp/agents', 'http://127.0.0.1:1234/mcp/agents?x=1', 'https://127.0.0.1:1234/mcp/agents']) assert.throws(() => new PaseoUpstream(url, { ownedLoopback: true }));
  assert.throws(() => new PaseoUpstream('http://127.0.0.1:1234/mcp/agents'));
  let selected;
  const upstream = new PaseoUpstream('http://127.0.0.1:1234/mcp/agents', { ownedLoopback: true, fetchImpl: async (url, opts) => { selected = { url, redirect: opts.redirect }; return rpcFetch(url, opts); } });
  assert.deepEqual(await upstream.tools(), []);
  assert.deepEqual(selected, { url: 'http://127.0.0.1:1234/mcp/agents', redirect: 'error' });
});

test('offline host construction and local message listen never contact SSH', async t => {
  const dir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'ho-')); t.after(() => rm(dir, { recursive: true, force: true }));
  let nativeCalls = 0, remoteCalls = 0, forwards = 0;
  const adapter = createRemoteHostAdapter(host, { stateDir: dir, nativeReader: async () => { nativeCalls++; throw Error('secret private host failure'); },
    runRemote: async () => { remoteCalls++; throw Error(); }, forwardFactory: async () => { forwards++; throw Error(); } });
  t.after(() => adapter.close());
  assert.equal(nativeCalls, 0);
  const channel = adapter.channelFactory({ stateDir: dir, verifyAgent: async () => true, onMessage: async () => ({}) });
  await channel.listen(); assert.equal(nativeCalls + remoteCalls + forwards, 0);
  assert.deepEqual(await adapter.status(), { available: false, reason: 'remote_unavailable' });
  await assert.rejects(channel.prepare()); assert.equal(remoteCalls + forwards, 0);
});

test('adapter pins daemon identity, uses one host for all reads/observer and never retries ambiguous calls', async t => {
  const stateDir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'hi-')); t.after(() => rm(stateDir, { recursive: true, force: true }));
  let currentId = 'srv_one', forwards = 0, closed = 0, mutations = 0;
  const reads = [], observed = [];
  class Observer { constructor(options) { observed.push(options); } close() {} }
  const adapter = createRemoteHostAdapter(host, { stateDir, observerClass: Observer, runRemote: async () => ({ resolved: '/home/test/project' }),
    nativeReader: async (script, args, transport) => { reads.push({ script, args, transport }); return args.action === 'status' ? { available: true, serverId: currentId } : { raw: true }; },
    forwardFactory: async target => { forwards++; assert.equal(target.uri, host.target); return { url: 'http://127.0.0.1:32123/mcp/agents', close: () => { closed++; }, isAlive: () => true }; },
    fetchImpl: async (url, opts) => { const req = JSON.parse(opts.body); if (req.method === 'tools/call') { mutations++; throw Error('uncertain'); } return rpcFetch(url, opts); } });
  t.after(() => adapter.close());
  assert.deepEqual(await adapter.status(), { available: true, serverId: 'srv_one' });
  assert.deepEqual(await adapter.readDaemon({ action: 'list_projects' }, 8000), { raw: true });
  assert.deepEqual(await adapter.readTimeline({ agentId: 'agent', direction: 'tail', limit: 10 }), { raw: true });
  adapter.observerFactory({ target: 'ssh://wrong', expectedServerId: 'srv_wrong' });
  assert.equal(observed[0].target, host.target); assert.equal(observed[0].expectedServerId(), 'srv_one');
  assert.equal(reads.every(r => r.transport.target === host.target), true);
  assert.equal(reads.filter(r => r.args.action !== 'status').every(r => r.transport.serverId === 'srv_one'), true);
  await assert.rejects(adapter.upstream.call('create_agent', {})); assert.equal(mutations, 1); assert.equal(forwards, 1);
  currentId = 'srv_two';
  assert.deepEqual(await adapter.status(), { available: false, reason: 'daemon_identity_changed', serverId: 'srv_one' });
  await assert.rejects(adapter.upstream.tools(), /identity changed/); assert.equal(closed, 1);
});

test('restart rejects a replacement daemon before background paths, native reads, tools or channel operations', async t => {
  for (const first of ['path', 'daemon', 'timeline', 'tools', 'call', 'prepare', 'issue']) {
    const stateDir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'hr-')); t.after(() => rm(stateDir, { recursive: true, force: true }));
    await pinHost({ ...host, stateDir }, 'srv_original');
    const bindingPath = join(stateDir, 'host-binding.json'), before = await readFile(bindingPath, 'utf8');
    let statusReads = 0, otherNative = 0, forwards = 0, files = 0, http = 0, channelActions = 0;
    const adapter = createRemoteHostAdapter(host, { stateDir,
      nativeReader: async (_script, args) => { if (args.action === 'status') { statusReads++; return { available: true, serverId: 'srv_replacement' }; } otherNative++; return {}; },
      runRemote: async () => { files++; return { resolved: '/home/test/project' }; },
      forwardFactory: async () => { forwards++; return { url: 'http://127.0.0.1:32123/mcp/agents', close() {}, isAlive: () => true }; },
      fetchImpl: async (...args) => { http++; return rpcFetch(...args); },
      channelFactory: () => ({ prepare: async () => { channelActions++; }, issue: async () => { channelActions++; }, close() {} }),
    });
    t.after(() => adapter.close());
    const channel = adapter.channelFactory({ stateDir });
    const actions = {
      path: () => adapter.paths.check('/home/test/project'),
      daemon: () => adapter.readDaemon({ action: 'list_projects' }),
      timeline: () => adapter.readTimeline({ agentId: 'agent', limit: 1, direction: 'tail' }),
      tools: () => adapter.upstream.tools(), call: () => adapter.upstream.call('create_agent', {}),
      prepare: () => channel.prepare(), issue: () => channel.issue('11111111-1111-4111-8111-111111111111'),
    };
    await assert.rejects(actions[first](), undefined, `initial background ${first} must reject`);
    for (const action of Object.values(actions)) await assert.rejects(action());
    assert.equal(statusReads, 1, `${first} permits only the minimal identity probe`);
    assert.deepEqual({ otherNative, forwards, files, http, channelActions }, { otherNative: 0, forwards: 0, files: 0, http: 0, channelActions: 0 });
    assert.equal(await readFile(bindingPath, 'utf8'), before);
    assert.deepEqual(await adapter.status(), { available: false, reason: 'daemon_identity_changed' });
  }
});

test('missing native server identity never extends or resets the persisted binding', async t => {
  const stateDir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'hm-')); t.after(() => rm(stateDir, { recursive: true, force: true }));
  await pinHost({ ...host, stateDir }, 'srv_original');
  const before = await readFile(join(stateDir, 'host-binding.json'), 'utf8'); let files = 0;
  const adapter = createRemoteHostAdapter(host, { stateDir, nativeReader: async () => ({ available: true }), runRemote: async () => { files++; return {}; } });
  t.after(() => adapter.close());
  await assert.rejects(adapter.paths.check('/home/test/project'));
  assert.equal(files, 0); assert.equal(await readFile(join(stateDir, 'host-binding.json'), 'utf8'), before);
});

function fakeChild() {
  const child = new EventEmitter();
  Object.assign(child, { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kills: [] });
  child.kill = signal => { child.kills.push(signal); queueMicrotask(() => child.emit('close', 1)); return true; };
  return child;
}
test('SSH script runner sends untrusted paths only as stdin JSON and bounds errors/output', async () => {
  const child = fakeChild(); let argv, sent = '';
  child.stdin.on('data', chunk => { sent += chunk; });
  const runner = createSshRunner(validateSshTarget('ssh://wsl'), { spawnImpl: (cmd, args) => { assert.equal(cmd, 'ssh'); argv = args; return child; } });
  const value = "/home/test/project/$(touch /tmp/never)'\n";
  const pending = runner.run('process.stdout.write("{}")', { value });
  assert.equal(argv.join(' ').includes(value), false); assert.deepEqual(JSON.parse(sent), { value });
  child.stdout.write('{}'); child.emit('close', 0); assert.deepEqual(await pending, {}); runner.close();
  const other = fakeChild();
  const bounded = createSshRunner(validateSshTarget('ssh://wsl'), { spawnImpl: () => other });
  const failed = bounded.run('fixed', {}, { maxBytes: 2 }); other.stdout.write('private-secret-output');
  await assert.rejects(failed, error => error.message === 'remote_output_limit'); assert.deepEqual(other.kills, ['SIGTERM']); bounded.close();
});

test('SSH reverse forward owns only its child and uses a private Unix socket binding', async () => {
  const child = fakeChild(); let argv;
  const promise = createSshForward(validateSshTarget('ssh://wsl:2222'), { remoteSocket: '/home/test/bridge/.r-123.sock', localSocket: '/private/tmp/private.sock',
    spawnImpl: (_cmd, args) => { argv = args; return child; } });
  child.stdout.write('bridge-ready\n'); const tunnel = await promise;
  assert.ok(argv.includes('/home/test/bridge/.r-123.sock:/private/tmp/private.sock'));
  assert.ok(argv.includes('StreamLocalBindUnlink=no')); assert.ok(argv.includes('ControlPath=none'));
  assert.equal(tunnel.isAlive(), true); tunnel.close(); assert.deepEqual(child.kills, ['SIGTERM']);
});
