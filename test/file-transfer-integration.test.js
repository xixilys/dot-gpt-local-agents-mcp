import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { join } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import http from 'node:http';
import { createGatewayApp, LocalAgentsOAuthProvider, SCOPE } from '../src/server.js';
import { createAgentChannel } from '../src/agent-channel.js';
import { PathPolicy } from '../src/gateway.js';
import { createBinaryFileTransport } from '../src/binary-file-transport.js';
import { DirectFiles } from '../src/direct-files.js';
import { DirectTransfers } from '../src/direct-transfers.js';

const sha = value => createHash('sha256').update(value).digest('hex');
const tools = JSON.parse(await fs.readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
const observer = () => ({ watchAgentIds() {}, start() {}, async close() {} });
const file = { download_url: 'https://owned-fixture.example/payload', file_id: 'owned-fixture', file_name: 'payload.bin', mime_type: 'application/octet-stream' };

// Only the outbound source is replaced: fixtures write a bounded private spool.
// All subsequent local/remote runtime operations, receipts, MCP and HTTP are real.
async function fixture(t, { payload = Buffer.from('fixture\0bytes'), binaryTransportFactory, lifecycle } = {}) {
  const dir = await fs.realpath(await fs.mkdtemp('/private/tmp/fti-'));
  const stateDir = join(dir, 's');
  const paths = { mac: join(dir, 'mac'), other: join(dir, 'other') };
  await Promise.all(Object.values(paths).map(path => fs.mkdir(path)));
  const resource = new URL('https://gateway.example.com/mcp');
  const provider = new LocalAgentsOAuthProvider({ ownerToken: randomBytes(32).toString('hex'), scopes: [SCOPE],
    allowedRedirectHosts: ['chatgpt.com'], accessTokenTtlSeconds: 60, refreshTokenTtlSeconds: 600 }, resource, stateDir);
  const register = name => {
    const client = provider.clientsStore.registerClient({ client_name: name, redirect_uris: ['http://127.0.0.1:9012/callback'],
      token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] });
    return { client, token: provider.issueTokens(client.client_id, [SCOPE], resource).access_token };
  };
  const grant = register('independent-test'), otherGrant = register('different-owner');
  const upstream = { async tools() { return tools; }, async call(name) {
    if (name === 'list_workspaces') return { structuredContent: { workspaces: [] } };
    throw Error(`Unexpected upstream operation: ${name}`);
  } };
  let downloads = 0;
  const downloadClient = { async downloadTo(url, options) {
    assert.equal(url, file.download_url); assert.equal(options.maxBytes, 256 * 1024 * 1024);
    assert.ok(options.signal instanceof AbortSignal); assert.equal(options.timeoutMs, 120000);
    if (options.sha256) assert.equal(options.sha256.toLowerCase(), sha(payload));
    downloads++; await fs.writeFile(options.destination, payload, { flag: 'wx', mode: 0o600 });
    return { bytes: payload.length, sha256: sha(payload) };
  } };
  const config = { stateDir, publicBaseUrl: resource.origin, allowedRoots: [paths.mac],
    direct: { projects: [{ id: 'same', path: paths.mac, read: true, write: true }] },
    hosts: [{ id: 'other', name: 'isolated second host', transport: 'ssh', target: 'ssh://fixture', allowedRoots: [paths.other], remoteStateDir: '/owned-fixture/state',
      direct: { projects: [{ id: 'same', path: paths.other, read: true, write: true }] } }] };
  let runtime, server; const cleanups = [];
  t.after(async () => {
    for (const cleanup of cleanups) await cleanup();
    if (server) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
    if (runtime) await runtime.close(); else try { provider.close(); } catch {}
    await fs.rm(dir, { recursive: true, force: true });
  });
  if (lifecycle) {
    await fs.mkdir(stateDir, { recursive: true }); await fs.writeFile(join(stateDir, 'transfer-spool'), 'not-directory');
    const originalClose = provider.close.bind(provider);
    provider.close = () => { lifecycle.provider++; originalClose(); throw Error('secondary-provider-close-error'); };
  }
  const open = () => createGatewayApp(config, { upstream, oauthProvider: provider, fileDownloadClient: downloadClient,
    observerFactory: () => ({ ...observer(), async close() { if (lifecycle) lifecycle.observer++; } }),
    binaryTransportFactory: binaryTransportFactory ?? (({ host }) => createBinaryFileTransport({ host: { ...host, transport: 'local' } })),
    hostAdapterFactory: host => ({ upstream, paths: new PathPolicy(host.allowedRoots), readDaemon: async () => ({ projects: [] }),
      readTimeline: async () => ({ entries: [] }), observerFactory: () => ({ ...observer(), async close() { if (lifecycle) lifecycle.observer++; } }),
      channelFactory: options => createAgentChannel(options), async status() { return { available: true, serverId: 'fixture-server' }; },
      async close() { if (lifecycle) lifecycle.adapter++; } }) });
  if (lifecycle) return { open, dir, paths, stateDir };
  runtime = await open();
  server = await new Promise((resolve, reject) => { const s = runtime.app.listen(0, '127.0.0.1', () => resolve(s)); s.on('error', reject); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const rpc = async (method, params = {}, { legacy = false, token = grant.token } = {}) => {
    const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream' };
    if (token) headers.authorization = `Bearer ${token}`;
    if (!legacy) { headers['mcp-protocol-version'] = '2026-07-28'; headers['mcp-method'] = method; if (method === 'tools/call') headers['mcp-name'] = params.name; }
    const response = await fetch(`${origin}/mcp`, { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id: 71, method,
      params: { ...params, ...(!legacy ? { _meta: { 'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {} } } : {}) } }) });
    return { status: response.status, body: await response.json() };
  };
  const call = async (name, args, options) => { const response = await rpc('tools/call', { name, arguments: args }, options); assert.equal(response.status, 200); assert.equal(response.body.error, undefined); return response.body.result; };
  return { dir, paths, stateDir, runtime, rpc, call, origin, grant, otherGrant, cleanups, get downloads() { return downloads; },
    localURL: url => origin + new URL(url).pathname };
}
function success(result) { assert.notEqual(result.isError, true, JSON.stringify(result)); assert.equal(result.structuredContent.ok, true); return result.structuredContent; }
function failure(result, code) { assert.equal(result.isError, true); assert.equal(result.structuredContent.error.code, code); return result.structuredContent; }
async function independentGET(url) {
  return new Promise((resolve, reject) => http.get(url, response => {
    const chunks = []; response.on('data', chunk => chunks.push(chunk)); response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, bytes: Buffer.concat(chunks) }));
    response.on('error', reject);
  }).on('error', reject));
}

test('independent full server: legacy and modern catalogs preserve inline fileParams schema', async t => {
  const f = await fixture(t);
  for (const legacy of [true, false]) {
    const response = await f.rpc('tools/list', {}, { legacy }); assert.equal(response.status, 200); assert.equal(response.body.error, undefined);
    const tool = response.body.result.tools.find(tool => tool.name === 'import_direct_file');
    assert.deepEqual(tool._meta, { 'openai/fileParams': ['file'] });
    assert.deepEqual(Object.keys(tool.inputSchema.properties.file.properties).sort(), ['download_url', 'file_id', 'file_name', 'mime_type']);
    assert.deepEqual(tool.inputSchema.properties.file.required, ['download_url', 'file_id']);
    assert.equal(tool.inputSchema.properties.file.type, 'object'); assert.equal(tool.inputSchema.properties.file.additionalProperties, false);
    assert.ok(!JSON.stringify(tool.inputSchema).includes('$ref')); assert.ok(!JSON.stringify(tool.inputSchema).includes('$defs'));
    assert.deepEqual(tool.inputSchema.properties.hostId.enum, ['mac', 'other']);
  }
  assert.equal((await f.rpc('tools/list', {}, { token: null })).status, 401);
});

test('independent full server: 11 MiB MCP export streams immutable binary through anonymous HTTP', async t => {
  const f = await fixture(t); const payload = Buffer.alloc(11 * 1024 * 1024 + 799);
  for (let i = 0; i < payload.length; i++) payload[i] = i % 251;
  const name = '独立 snapshot.bin'; await fs.writeFile(join(f.paths.mac, name), payload);
  for (const legacy of [true, false]) {
    const result = await f.call('export_direct_file', { projectId: 'same', path: name, requestId: `binary-${legacy}` }, { legacy });
    const value = success(result); assert.equal(value.bytes, payload.length); assert.equal(value.sha256, sha(payload)); assert.equal(value.hostId, 'mac');
    assert.equal(value.state, 'ready'); assert.equal(value.fileName, name); assert.equal(value.mimeType, 'application/octet-stream');
    const link = result.content.find(block => block.type === 'resource_link'); assert.ok(link); assert.equal(link.uri, value.downloadUrl); assert.equal(link.size, payload.length);
    assert.ok(JSON.stringify(result).length < 5000, 'File bytes must not be embedded in MCP JSON');
    await fs.writeFile(join(f.paths.mac, name), 'source replaced after export');
    const response = await independentGET(f.localURL(link.uri)); assert.equal(response.status, 200); assert.deepEqual(response.bytes, payload);
    assert.equal(sha(response.bytes), value.sha256); assert.equal(response.headers['content-length'], String(payload.length));
    assert.equal(response.headers['content-type'], 'application/octet-stream'); assert.equal(response.headers['cache-control'], 'no-store');
    assert.equal(response.headers['x-content-type-options'], 'nosniff'); assert.equal(response.headers['referrer-policy'], 'no-referrer');
    assert.match(response.headers['content-disposition'], /attachment;.*filename\*=UTF-8''/);
    await fs.writeFile(join(f.paths.mac, name), payload);
  }
});

test('independent full server: import uses shared text write lock, writes exact bytes, rejects stale original hash', async t => {
  const payload = Buffer.from([0, 255, 1, 10, 13, 128]); const f = await fixture(t, { payload });
  const direct = f.runtime.hosts.get('mac').directFiles, originalLock = direct.withWriteLock.bind(direct); let locks = 0;
  direct.withWriteLock = execute => { locks++; return originalLock(execute); };
  const input = { projectId: 'same', path: 'received.bin', requestId: 'incoming', expectedSha256: null, sha256: sha(payload), file };
  const imported = success(await f.call('import_direct_file', input)); assert.equal(imported.state, 'completed'); assert.equal(imported.created, true);
  assert.equal(imported.bytes, payload.length); assert.equal(imported.sha256, sha(payload)); assert.deepEqual(await fs.readFile(join(f.paths.mac, input.path)), payload); assert.equal(locks, 1);
  assert.equal((await fs.stat(join(f.paths.mac, input.path))).mode & 0o777, 0o600);
  success(await f.call('write_direct_file', { projectId: 'same', path: 'text.txt', content: 'original', expectedSha256: null })); assert.equal(locks, 2);
  const bad = failure(await f.call('import_direct_file', { ...input, path: 'text.txt', requestId: 'stale', expectedSha256: sha('stale') }), 'hash_conflict');
  assert.equal(bad.state, 'failed'); assert.notEqual(bad.resultUnknown, true); assert.equal(await fs.readFile(join(f.paths.mac, 'text.txt'), 'utf8'), 'original');
  const overwrite = success(await f.call('import_direct_file', { ...input, path: 'text.txt', requestId: 'overwrite', expectedSha256: sha('original') }, { legacy: true }));
  assert.equal(overwrite.created, false); assert.deepEqual(await fs.readFile(join(f.paths.mac, 'text.txt')), payload);
  const replay = success(await f.call('import_direct_file', input)); assert.equal(replay.transferId, imported.transferId); assert.equal(f.downloads, 3); assert.equal(locks, 4);
  const persisted = await fs.readFile(join(f.stateDir, 'direct-transfer-receipts.json'), 'utf8'); assert.ok(!persisted.includes(file.download_url)); assert.ok(!persisted.includes(file.file_id));
  assert.deepEqual(await fs.readdir(join(f.stateDir, 'transfer-spool')), []);
});

test('independent full server: host and owner isolation, revoked ticket, mismatched names and arbitrary paths', async t => {
  const f = await fixture(t), receipts = {};
  for (const hostId of ['mac', 'other']) {
    await fs.writeFile(join(f.paths[hostId], `${hostId}.bin`), hostId);
    receipts[hostId] = success(await f.call('export_direct_file', { hostId, projectId: 'same', path: `${hostId}.bin`, requestId: 'same-id' }));
    assert.equal(receipts[hostId].hostId, hostId); assert.equal((await independentGET(f.localURL(receipts[hostId].downloadUrl))).bytes.toString(), hostId);
    const saved = success(await f.call('get_direct_transfer', { hostId, requestId: 'same-id' })); assert.equal(saved.transferId, receipts[hostId].transferId);
  }
  assert.notEqual(receipts.mac.transferId, receipts.other.transferId);
  failure(await f.call('get_direct_transfer', { requestId: 'same-id' }, { token: f.otherGrant.token }), 'transfer_not_found');
  failure(await f.call('get_direct_transfer', { requestId: 'missing-id' }), 'transfer_not_found');
  const unknown = await f.call('get_direct_transfer', { hostId: 'unknown', requestId: 'same-id' }); assert.equal(unknown.isError, true); assert.match(unknown.content[0].text, /Unknown hostId/);
  const macURL = f.localURL(receipts.mac.downloadUrl), otherName = new URL(receipts.other.downloadUrl).pathname.split('/').at(-1);
  assert.equal((await fetch(macURL.replace(/[^/]+$/, otherName))).status, 404);
  assert.equal((await fetch(macURL.replace(/\/direct-files\/[^/]+\//, '/direct-files/guessed/'))).status, 404);
  assert.equal((await fetch(`${f.origin}/direct-files?path=${encodeURIComponent(join(f.paths.mac, 'mac.bin'))}`)).status, 404);
  assert.equal((await fetch(macURL.replace(/[^/]+$/, '%2Fetc%2Fpasswd'))).status, 404);
  const escaped = failure(await f.call('export_direct_file', { projectId: 'same', path: '../other/other.bin', requestId: 'escape' }), 'invalid_path'); assert.equal(escaped.state, 'failed');
  await f.runtime.provider.revokeToken(f.grant.client, { token: f.grant.token, token_type_hint: 'access_token' });
  assert.equal((await fetch(macURL)).status, 404); assert.equal((await fetch(f.localURL(receipts.other.downloadUrl))).status, 404);
  assert.equal((await f.rpc('tools/list')).status, 401);
});

test('independent full server: SSH runtime commit with lost acknowledgement stays unknown and never reexecutes', async t => {
  const payload = Buffer.from('committed independently\0binary'); let spawns = 0;
  const f = await fixture(t, { payload, binaryTransportFactory: ({ host }) => {
    if (host.id === 'mac') return createBinaryFileTransport({ host });
    return createBinaryFileTransport({ host, spawnImpl(command, args, options) {
      assert.equal(command, 'ssh'); spawns++;
      const remote = args.at(-1), marker = ' --input-type=module -e ';
      const quoted = remote.slice(remote.indexOf(marker) + marker.length), original = quoted.slice(1, -1).replaceAll("'\\''", "'");
      const expected = 'process.stdout.write(JSON.stringify({ ok: true, ...result }))'; assert.ok(original.includes(expected));
      return spawn(process.execPath, ['--input-type=module', '-e', original.replace(expected, "process.stdout.write('')")], options);
    } });
  } });
  const input = { hostId: 'other', projectId: 'same', path: 'committed.bin', requestId: 'ack-lost', expectedSha256: null, file };
  const first = await f.call('import_direct_file', input); assert.equal(first.isError, true); const value = first.structuredContent;
  assert.equal(value.state, 'outcome_unknown'); assert.equal(value.resultUnknown, true); assert.equal(value.error.code, 'remote_invalid_output');
  assert.deepEqual(await fs.readFile(join(f.paths.other, 'committed.bin')), payload); assert.equal(spawns, 1);
  assert.deepEqual((await f.call('import_direct_file', input)).structuredContent, value);
  assert.deepEqual((await f.call('get_direct_transfer', { hostId: 'other', requestId: 'ack-lost' })).structuredContent, value);
  assert.equal(spawns, 1); assert.equal(f.downloads, 1);
  await fs.writeFile(join(f.paths.other, 'known-conflict.bin'), 'old');
  const rejected = failure(await f.call('import_direct_file', { ...input, requestId: 'known-conflict', path: 'known-conflict.bin', expectedSha256: sha('wrong') }), 'hash_conflict');
  assert.equal(rejected.state, 'failed'); assert.notEqual(rejected.resultUnknown, true); assert.equal(await fs.readFile(join(f.paths.other, 'known-conflict.bin'), 'utf8'), 'old');
  assert.equal(spawns, 2);
  // Age the real lost-ACK receipt beyond ordinary retention, then recover it
  // through a new service instance and advance another 48 hours while live.
  await f.runtime.close();
  const receiptPath = join(f.stateDir, 'direct-transfer-receipts.json');
  const rows = JSON.parse(await fs.readFile(receiptPath, 'utf8'));
  const unknownRow = rows.find(row => row.requestId === input.requestId); assert.equal(unknownRow.state, 'outcome_unknown');
  unknownRow.updatedAt = Date.now() - 2 * 86400000;
  await fs.writeFile(receiptPath, JSON.stringify(rows)); let now = Date.now(), reexecutions = 0;
  const recovered = new DirectTransfers({ contexts: [...f.runtime.hosts.values()], stateDir: f.stateDir, publicBaseUrl: 'https://gateway.example.com', now: () => now,
    transportFactory: () => ({ close() {}, importFrom() { reexecutions++; throw Error('Must not replay a lost acknowledgement'); } }) });
  f.cleanups.push(() => recovered.close()); await recovered.ready();
  const { hostId, ...businessInput } = input; const auth = { owner: `local-owner:${f.grant.client.client_id}` };
  for (let age = 0; age < 2; age++) {
    const replay = await recovered.forHost(hostId).call('import_direct_file', businessInput, auth);
    assert.equal(replay.transferId, value.transferId); assert.equal(replay.state, 'outcome_unknown'); assert.equal(replay.resultUnknown, true);
    assert.equal((await recovered.forHost(hostId).call('get_direct_transfer', { requestId: input.requestId }, auth)).transferId, value.transferId);
    now += 2 * 86400000;
  }
  assert.equal(reexecutions, 0); assert.equal(spawns, 2);
});

test('independent lifecycle: failed transfer-spool initialization preserves original error and closes constructed contexts', async t => {
  const counts = { observer: 0, adapter: 0, provider: 0 }; const f = await fixture(t, { lifecycle: counts });
  await assert.rejects(f.open(), error => { assert.equal(error.message, 'Transfer spool must be a private directory'); return true; });
  assert.equal(counts.observer, 2); assert.equal(counts.adapter, 1); assert.equal(counts.provider, 1);
  assert.equal(await fs.readFile(join(f.stateDir, 'transfer-spool'), 'utf8'), 'not-directory');
  const channelPath = join(f.stateDir, 'agent-channel.sock'); await assert.rejects(fs.stat(channelPath), { code: 'ENOENT' });
  await assert.rejects(fs.stat(join(f.stateDir, 'hosts', 'other', 'agent-channel.sock')), { code: 'ENOENT' });
});

test('independent full server: queued import revocation settles while shared text lock remains held', async t => {
  const f = await fixture(t), direct = f.runtime.hosts.get('mac').directFiles;
  let release, entered; const blocked = new Promise(resolve => { release = resolve; }); const holding = new Promise(resolve => { entered = resolve; });
  const held = direct.withWriteLock(async () => { entered(); await blocked; }); await holding;
  t.after(async () => { release(); await held; });
  let queued; const queueReached = new Promise(resolve => { queued = resolve; }); const original = direct.withWriteLock.bind(direct);
  direct.withWriteLock = execute => { queued(); return original(execute); };
  const pending = f.call('import_direct_file', { projectId: 'same', path: 'cannot-commit', requestId: 'held-text-lock', expectedSha256: null, file });
  await queueReached;
  await f.runtime.directTransfers.revokeOwner(`local-owner:${f.grant.client.client_id}`, 'mac');
  let timer;
  try {
    const result = await Promise.race([pending, new Promise((resolve, reject) => { timer = setTimeout(() => reject(Error('Import waited for held text lock after revocation')), 1000); })]);
    assert.equal(result.isError, true); assert.equal(result.structuredContent.state, 'interrupted');
  } finally { clearTimeout(timer); release(); await held; }
  await new Promise(resolve => setImmediate(resolve));
  await assert.rejects(fs.stat(join(f.paths.mac, 'cannot-commit')), { code: 'ENOENT' });
  assert.deepEqual(await fs.readdir(join(f.stateDir, 'transfer-spool')), []);
});

test('independent revocation revision: pre-registration calls stop only for revoked owner and host', async t => {
  const dir = await fs.realpath(await fs.mkdtemp('/private/tmp/ftr-'));
  const project = join(dir, 'p'); await fs.mkdir(project); await fs.writeFile(join(project, 'source'), 'owner-host-scope');
  const contexts = ['mac', 'other'].map(id => {
    const host = { id, transport: 'local', allowedRoots: [project], direct: { projects: [{ id: 'same', path: project, read: true, write: true }] } };
    return { host, directFiles: new DirectFiles({ host }) };
  });
  let exports = 0;
  const manager = new DirectTransfers({ contexts, stateDir: join(dir, 's'), publicBaseUrl: 'https://gateway.example',
    transportFactory(options) { const real = createBinaryFileTransport(options); return { ...real, async exportTo(args) { exports++; return real.exportTo(args); } }; } });
  t.after(async () => { await manager.close(); for (const context of contexts) context.directFiles.close(); await fs.rm(dir, { recursive: true, force: true }); });
  await manager.ready(); const realReady = manager.ready.bind(manager); let unblock;
  const blocked = new Promise(resolve => { unblock = resolve; }); manager.ready = () => blocked.then(realReady);
  const args = { projectId: 'same', path: 'source', requestId: 'same-id' }, owner = { owner: 'local-owner:one' };
  const revoked = manager.forHost('mac').call('export_direct_file', args, owner);
  const differentHost = manager.forHost('other').call('export_direct_file', args, owner);
  const differentOwner = manager.forHost('mac').call('export_direct_file', args, { owner: 'local-owner:two' });
  const revoking = manager.revokeOwner(owner.owner, 'mac'); unblock(); await revoking;
  const stopped = await revoked; assert.equal(stopped.ok, false); assert.equal(stopped.error.code, 'transfer_access_revoked'); assert.equal(stopped.downloadUrl, undefined);
  assert.equal((await differentHost).state, 'ready'); assert.equal((await differentOwner).state, 'ready'); assert.equal(exports, 2);
  assert.equal((await manager.forHost('mac').call('get_direct_transfer', { requestId: args.requestId }, owner)).error.code, 'transfer_not_found');
});

test('independent real transport capability expiry: expired URLs cannot serve bytes or reexecute request', async t => {
  const dir = await fs.realpath(await fs.mkdtemp('/private/tmp/fte-')); const project = join(dir, 'p'); await fs.mkdir(project);
  const host = { id: 'mac', transport: 'local', allowedRoots: [project], direct: { projects: [{ id: 'same', path: project, read: true, write: true }] } };
  const directFiles = new DirectFiles({ host }); let now = Date.now(); let exports = 0;
  const manager = new DirectTransfers({ contexts: [{ host, directFiles }], stateDir: join(dir, 's'), publicBaseUrl: 'https://gateway.example', now: () => now,
    transportFactory(options) { const transport = createBinaryFileTransport(options); return { ...transport, async exportTo(args) { exports++; return transport.exportTo(args); } }; } });
  await manager.ready();
  const server = http.createServer((req, res) => { const parts = new URL(req.url, 'http://fixture').pathname.split('/'); req.params = { token: parts[2], filename: decodeURIComponent(parts[3] ?? '') }; void manager.handleDownload(req, res); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await manager.close(); directFiles.close(); await fs.rm(dir, { recursive: true, force: true }); });
  await fs.writeFile(join(project, 'payload'), 'expires'); const args = { projectId: 'same', path: 'payload', requestId: 'expires' }, auth = { owner: 'local-owner:independent' };
  const exported = await manager.forHost('mac').call('export_direct_file', args, auth); assert.equal(exported.ok, true);
  const url = `http://127.0.0.1:${server.address().port}${new URL(exported.downloadUrl).pathname}`;
  assert.equal((await independentGET(url)).bytes.toString(), 'expires'); now += 600001;
  assert.equal((await independentGET(url)).status, 404); const replay = await manager.forHost('mac').call('export_direct_file', args, auth);
  assert.equal(replay.state, 'expired'); assert.equal(replay.downloadUrl, undefined); assert.equal(exports, 1);
});
