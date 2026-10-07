import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import http from 'node:http';
import express from 'express';
import { DirectFiles } from '../src/direct-files.js';
import { DirectTransfers, DIRECT_TRANSFER_TOOLS } from '../src/direct-transfers.js';
import { createBinaryFileTransport } from '../src/binary-file-transport.js';
const owner = { owner: 'oauth-owner' };
const sha = content => createHash('sha256').update(content).digest('hex');
async function fixture(t, options = {}) {
  const root = await fs.realpath(await fs.mkdtemp(join(tmpdir(), 'direct-transfers-')));
  const project = join(root, 'project'); await fs.mkdir(project);
  const host = { id: 'mac', transport: 'local', allowedRoots: [root], direct: { projects: [{ id: 'p', path: project, read: true, write: true }] } };
  const directFiles = new DirectFiles({ host });
  const contexts = [{ host, directFiles }, { host: { ...host, id: 'other' }, directFiles }];
  const stateDir = join(root, 'state');
  const managers = [];
  const create = overrides => {
    const manager = new DirectTransfers({ contexts, stateDir, publicBaseUrl: 'https://gateway.example', ...options, ...overrides });
    managers.push(manager); return manager;
  };
  const manager = create(); await manager.ready();
  const app = express(); app.all('/direct-files/:token/:filename', manager.handleDownload);
  const server = http.createServer(app); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const localURL = downloadUrl => `http://127.0.0.1:${server.address().port}${new URL(downloadUrl).pathname}`;
  t.after(async () => {
    for (const manager of managers) await manager.close();
    directFiles.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    await fs.rm(root, { recursive: true, force: true });
  });
  const call = (name, args, auth = owner) => manager.forHost('mac').call(name, { ...(name === 'get_direct_transfer' ? {} : { projectId: 'p' }), ...args }, auth);
  return { root, project, host, directFiles, manager, stateDir, create, call, localURL };
}
function downloader(content, inspect) {
  return { async downloadTo(_url, options) { inspect?.(options); await fs.writeFile(options.destination, content, { flag: 'wx', mode: 0o600 }); return { bytes: content.length, sha256: sha(content) }; } };
}

test('11 MiB binary roundtrip: immutable export snapshot, anonymous capability bytes and import metadata', async t => {
  const content = Buffer.alloc(11 * 1024 * 1024); for (let index = 0; index < content.length; index++) content[index] = index % 251;
  let locks = 0;
  const f = await fixture(t, { downloadClient: downloader(content) });
  const originalLock = f.directFiles.withWriteLock.bind(f.directFiles);
  f.directFiles.withWriteLock = execute => { locks++; return originalLock(execute); };
  await fs.writeFile(join(f.project, '数据.bin'), content);
  const exported = await f.call('export_direct_file', { path: '数据.bin', requestId: 'export-11mb' });
  assert.equal(exported.ok, true); assert.equal(exported.state, 'ready'); assert.equal(exported.bytes, content.length); assert.equal(exported.sha256, sha(content));
  assert.equal(exported.fileName, '数据.bin'); assert.equal(exported.mimeType, 'application/octet-stream'); assert.equal(exported.requestId, 'export-11mb');
  await fs.writeFile(join(f.project, '数据.bin'), 'changed after authorization');
  const response = await fetch(f.localURL(exported.downloadUrl));
  assert.equal(response.status, 200); assert.equal(response.headers.get('content-length'), String(content.length));
  assert.equal(response.headers.get('cache-control'), 'no-store'); assert.equal(response.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(response.headers.get('referrer-policy'), 'no-referrer'); assert.equal(response.headers.get('content-type'), 'application/octet-stream');
  assert.match(response.headers.get('content-disposition'), /attachment;.*filename\*=UTF-8''%E6%95%B0/);
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), content);
  const imported = await f.call('import_direct_file', { path: 'roundtrip.bin', requestId: 'import-11mb', expectedSha256: null,
    sha256: sha(content), file: { download_url: 'https://files.example/fixture', file_id: 'reference-only' } });
  assert.equal(imported.ok, true); assert.equal(imported.state, 'completed'); assert.equal(imported.created, true);
  assert.equal(imported.bytes, content.length); assert.equal(imported.sha256, exported.sha256); assert.equal(locks, 1);
  assert.deepEqual(await fs.readFile(join(f.project, 'roundtrip.bin')), content);
  const rows = JSON.parse(await fs.readFile(join(f.stateDir, 'direct-transfer-receipts.json'), 'utf8'));
  const persisted = JSON.stringify(rows);
  assert.ok(!persisted.includes('https://')); assert.ok(!persisted.includes('downloadUrl')); assert.ok(!persisted.includes('reference-only'));
  assert.equal((await fs.stat(join(f.stateDir, 'transfer-spool'))).mode & 0o777, 0o700);
  assert.equal((await fs.stat(join(f.stateDir, 'transfer-spool', `${exported.transferId}.bin`))).mode & 0o777, 0o600);
});

test('inline fileParams schema, permissions, owner/host isolation and unguessable capability', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'fixture'), 'fixture');
  const tool = DIRECT_TRANSFER_TOOLS.find(tool => tool.name === 'import_direct_file');
  assert.deepEqual(tool._meta, { 'openai/fileParams': ['file'] }); assert.deepEqual(tool.inputSchema.properties.file.required, ['download_url', 'file_id']);
  assert.equal(tool.inputSchema.properties.file.$ref, undefined);
  assert.equal((await f.call('export_direct_file', { path: 'fixture', requestId: 'permission' }, {})).error.code, 'owner_required');
  assert.equal((await f.call('export_direct_file', { path: 'fixture', requestId: 'permission', unexpected: true })).error.code, 'invalid_arguments');
  assert.equal((await f.call('export_direct_file', { projectId: 'missing', path: 'fixture', requestId: 'permission' })).error.code, 'project_access_denied');
  const exported = await f.call('export_direct_file', { path: 'fixture', requestId: 'isolated' });
  assert.equal((await f.call('get_direct_transfer', { requestId: 'isolated' }, { owner: 'different-owner' })).error.code, 'transfer_not_found');
  assert.equal((await f.manager.forHost('other').call('get_direct_transfer', { requestId: 'isolated' }, owner)).error.code, 'transfer_not_found');
  assert.equal((await f.call('get_direct_transfer', { requestId: 'isolated' })).downloadUrl, exported.downloadUrl);
  const guessed = f.localURL(exported.downloadUrl).replace(/\/direct-files\/[^/]+\//, '/direct-files/guessed/');
  assert.equal((await fetch(guessed)).status, 404);
  assert.equal((await fetch(`${f.localURL(exported.downloadUrl)}-wrong-filename`)).status, 404);
});

test('HEAD does not consume attempts; single ranges and invalid ranges return correct headers', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'fixture'), '0123456789');
  const exported = await f.call('export_direct_file', { path: 'fixture', requestId: 'range' });
  const url = f.localURL(exported.downloadUrl);
  for (let index = 0; index < 10; index++) { const response = await fetch(url, { method: 'HEAD' }); assert.equal(response.status, 200); assert.equal(response.headers.get('content-length'), '10'); }
  for (const [range, expected, contentRange] of [['bytes=2-5', '2345', 'bytes 2-5/10'], ['bytes=-3', '789', 'bytes 7-9/10'], ['bytes=8-', '89', 'bytes 8-9/10']]) {
    const response = await fetch(url, { headers: { Range: range } }); assert.equal(response.status, 206); assert.equal(response.headers.get('content-range'), contentRange); assert.equal(await response.text(), expected);
  }
  for (const range of ['bytes=99-', 'bytes=0-1,3-4', 'bytes=-0', 'bytes=8-2', 'invalid']) {
    const response = await fetch(url, { headers: { Range: range } }); assert.equal(response.status, 416); assert.equal(response.headers.get('content-range'), 'bytes */10');
  }
  assert.equal((await fetch(url, { method: 'POST' })).status, 405);
});

test('expiry and owner revocation invalidate capabilities without removing unrelated spool files', async t => {
  let now = Date.now(); const f = await fixture(t, { now: () => now }); await fs.writeFile(join(f.project, 'fixture'), 'expiry');
  const unrelated = join(f.stateDir, 'transfer-spool', 'user-file'); await fs.writeFile(unrelated, 'preserve');
  const first = await f.call('export_direct_file', { path: 'fixture', requestId: 'expires' });
  now += 600001;
  assert.equal((await fetch(f.localURL(first.downloadUrl))).status, 404);
  assert.equal((await f.call('get_direct_transfer', { requestId: 'expires' })).state, 'expired');
  const second = await f.call('export_direct_file', { path: 'fixture', requestId: 'revoked' });
  await f.manager.revokeOwner(owner.owner);
  assert.equal((await fetch(f.localURL(second.downloadUrl))).status, 404);
  assert.equal((await f.call('get_direct_transfer', { requestId: 'revoked' })).state, 'revoked');
  await assert.rejects(fs.stat(join(f.stateDir, 'transfer-spool', `${second.transferId}.bin`)), { code: 'ENOENT' });
  assert.equal(await fs.readFile(unrelated, 'utf8'), 'preserve');
});

test('revocation cancels an in-flight anonymous download stream', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'large'), Buffer.alloc(11 * 1024 * 1024, 0xab));
  const exported = await f.call('export_direct_file', { path: 'large', requestId: 'stream-revoke' });
  const response = await new Promise((resolve, reject) => http.get(f.localURL(exported.downloadUrl), resolve).on('error', reject));
  response.pause(); response.on('error', () => {});
  const ended = new Promise(resolve => response.once('close', resolve));
  await f.manager.revokeOwner(owner.owner); response.resume(); await ended;
  assert.equal(response.complete, false);
  assert.equal((await fetch(f.localURL(exported.downloadUrl))).status, 404);
});

test('same requestId joins one transfer, conflicts reject, and completed import recovers after restart', async t => {
  let exports = 0, downloads = 0;
  const content = Buffer.from([0, 1, 255, 12]);
  const f = await fixture(t, { downloadClient: downloader(content, () => { downloads++; }), transportFactory(options) {
    const transport = createBinaryFileTransport(options);
    return { ...transport, exportTo: async args => { exports++; return transport.exportTo(args); } };
  } });
  await fs.writeFile(join(f.project, 'fixture'), 'original');
  const args = { path: 'fixture', requestId: 'deduplicated' };
  const [first, second] = await Promise.all([f.call('export_direct_file', args), f.call('export_direct_file', args)]);
  assert.deepEqual(first, second); assert.equal(exports, 1);
  assert.equal((await f.call('export_direct_file', { ...args, path: './fixture' })).error.code, 'request_id_conflict');
  const input = { path: 'imported', requestId: 'import-deduplicated', expectedSha256: null, file: { download_url: 'https://file.example/one', file_id: 'one' } };
  const [imported, duplicate] = await Promise.all([f.call('import_direct_file', input), f.call('import_direct_file', input)]);
  assert.deepEqual(imported, duplicate); assert.equal(downloads, 1); assert.equal(imported.state, 'completed');
  await f.manager.close(); const restarted = f.create(); await restarted.ready();
  const recovered = await restarted.forHost('mac').call('get_direct_transfer', { requestId: input.requestId }, owner);
  assert.deepEqual(recovered, imported);
  const replay = await restarted.forHost('mac').call('import_direct_file', { projectId: 'p', ...input }, owner);
  assert.deepEqual(replay, imported); assert.equal(downloads, 1);
  const exportReceipt = await restarted.forHost('mac').call('get_direct_transfer', { requestId: args.requestId }, owner);
  assert.equal(exportReceipt.state, 'expired'); assert.equal(exportReceipt.downloadUrl, undefined);
});

test('crash-pending receipt becomes interrupted after restart and never reexecutes', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'fixture'), 'original');
  const args = { path: 'fixture', requestId: 'crash-pending' };
  const exported = await f.call('export_direct_file', args); await f.manager.close();
  const receiptFile = join(f.stateDir, 'direct-transfer-receipts.json');
  const rows = JSON.parse(await fs.readFile(receiptFile, 'utf8')); rows[0].state = 'pending';
  await fs.writeFile(receiptFile, JSON.stringify(rows), { mode: 0o600 });
  await fs.writeFile(join(f.stateDir, 'transfer-spool', `${exported.transferId}.bin`), 'partial');
  const restarted = f.create({ transportFactory: () => ({ close() {}, exportTo() { throw Error('Must not execute'); } }) }); await restarted.ready();
  const result = await restarted.forHost('mac').call('export_direct_file', { projectId: 'p', ...args }, owner);
  assert.equal(result.state, 'interrupted'); assert.equal(result.ok, false);
  await assert.rejects(fs.stat(join(f.stateDir, 'transfer-spool', `${exported.transferId}.bin`)), { code: 'ENOENT' });
});

test('failed upload and wrong overwrite hash preserve destination and clean owned spool', async t => {
  const f = await fixture(t, { downloadClient: downloader(Buffer.from('replacement')) }); await fs.writeFile(join(f.project, 'existing'), 'preserved');
  const args = { path: 'existing', requestId: 'wrong-hash', expectedSha256: sha('wrong'), file: { download_url: 'https://file.example/one', file_id: 'one' } };
  const result = await f.call('import_direct_file', args);
  assert.equal(result.error.code, 'hash_conflict'); assert.equal(await fs.readFile(join(f.project, 'existing'), 'utf8'), 'preserved');
  assert.deepEqual(await fs.readdir(join(f.stateDir, 'transfer-spool')), []);
  const failManager = f.create({ stateDir: join(f.root, 'failure-state'), downloadClient: { async downloadTo(_url, options) {
    await fs.writeFile(options.destination, 'partial', { flag: 'wx', mode: 0o600 }); throw new Error('secret-url-or-stack');
  } } }); await failManager.ready();
  const failure = await failManager.forHost('mac').call('import_direct_file', { projectId: 'p', ...args, requestId: 'failed-upload', expectedSha256: sha('preserved') }, owner);
  assert.equal(failure.error.code, 'transfer_failed'); assert.ok(!JSON.stringify(failure).includes('secret-url'));
  assert.equal(await fs.readFile(join(f.project, 'existing'), 'utf8'), 'preserved');
  assert.deepEqual(await fs.readdir(join(f.root, 'failure-state', 'transfer-spool')), []);
});

test('download remains outside host lock, concurrent imports share text write serialization', async t => {
  let allowDownload; const blocker = new Promise(resolve => { allowDownload = resolve; });
  const content = Buffer.from('binary-new'); let started; const downloading = new Promise(resolve => { started = resolve; });
  const f = await fixture(t, { downloadClient: { async downloadTo(_url, options) {
    started(); await blocker; await fs.writeFile(options.destination, content, { flag: 'wx', mode: 0o600 }); return { bytes: content.length, sha256: sha(content) };
  } } });
  await fs.writeFile(join(f.project, 'target'), 'original');
  const input = { path: 'target', expectedSha256: sha('original'), file: { download_url: 'https://file.example/one', file_id: 'one' } };
  const first = f.call('import_direct_file', { ...input, requestId: 'first' });
  const second = f.call('import_direct_file', { ...input, requestId: 'second' }); await downloading;
  const text = await f.directFiles.call('write_direct_file', { projectId: 'p', path: 'text-created', content: 'text', expectedSha256: null }, owner);
  assert.equal(text.ok, true); allowDownload();
  const results = await Promise.all([first, second]);
  assert.equal(results.filter(result => result.ok).length, 1); assert.equal(results.filter(result => result.error?.code === 'hash_conflict').length, 1);
  assert.equal(await fs.readFile(join(f.project, 'target'), 'utf8'), 'binary-new');
});

test('unknown import outcome persists after actual commit and cannot be replayed', async t => {
  let imports = 0, now = Date.now(); const content = Buffer.from('committed-but-receipt-lost');
  const f = await fixture(t, { now: () => now, downloadClient: downloader(content), transportFactory(options) {
    const real = createBinaryFileTransport(options);
    return { ...real, async importFrom(...args) {
      imports++; await real.importFrom(...args);
      throw Object.assign(new Error('private transport diagnostic'), { safeCode: 'remote_invalid_output', resultUnknown: true });
    } };
  } });
  const input = { projectId: 'p', path: 'unknown', requestId: 'unknown-import', expectedSha256: null, file: { download_url: 'https://file.example/fixture', file_id: 'reference' } };
  const result = await f.manager.forHost('mac').call('import_direct_file', input, owner);
  assert.equal(result.state, 'outcome_unknown'); assert.equal(result.ok, false); assert.equal(result.resultUnknown, true);
  assert.equal(result.error.code, 'remote_invalid_output'); assert.ok(!JSON.stringify(result).includes('private transport'));
  assert.deepEqual(await fs.readFile(join(f.project, 'unknown')), content);
  assert.deepEqual(await f.manager.forHost('mac').call('import_direct_file', input, owner), result); assert.equal(imports, 1);
  now += 86400001;
  assert.deepEqual(await f.manager.forHost('mac').call('import_direct_file', input, owner), result); assert.equal(imports, 1);
  await f.manager.close(); const restarted = f.create(); await restarted.ready();
  assert.deepEqual(await restarted.forHost('mac').call('import_direct_file', input, owner), result); assert.equal(imports, 1);
});

test('concurrent transfer capacity is bounded, get reports pending, close cancels and cleans spool', async t => {
  let starts = 0, started; const bothStarted = new Promise(resolve => { started = resolve; });
  const f = await fixture(t, { downloadClient: { async downloadTo(_url, { destination, signal }) {
    await fs.writeFile(destination, 'partial', { flag: 'wx', mode: 0o600 });
    starts++; if (starts === 2) started();
    await new Promise((resolve, reject) => {
      const abort = () => reject(Object.assign(new Error('cancelled'), { safeCode: 'transfer_aborted' }));
      if (signal.aborted) abort(); else signal.addEventListener('abort', abort, { once: true });
    });
  } } });
  const input = { path: 'capacity', expectedSha256: null, file: { download_url: 'https://file.example/fixture', file_id: 'reference' } };
  const first = f.call('import_direct_file', { ...input, requestId: 'pending-one' });
  const second = f.call('import_direct_file', { ...input, requestId: 'pending-two' }); await bothStarted;
  assert.equal((await f.call('get_direct_transfer', { requestId: 'pending-one' })).state, 'pending');
  assert.equal((await f.call('import_direct_file', { ...input, requestId: 'capacity-three' })).error.code, 'transfer_capacity_exceeded');
  await f.manager.close();
  assert.equal((await first).state, 'interrupted'); assert.equal((await second).state, 'interrupted');
  assert.deepEqual(await fs.readdir(join(f.stateDir, 'transfer-spool')), []);
  await assert.rejects(fs.stat(join(f.project, 'capacity')), { code: 'ENOENT' });
});

test('download retries have a finite byte budget and empty exports support GET/HEAD', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'fixture'), 'bounded');
  const exported = await f.call('export_direct_file', { path: 'fixture', requestId: 'budget' });
  for (let index = 0; index < 3; index++) { const response = await fetch(f.localURL(exported.downloadUrl)); assert.equal(response.status, 200); assert.equal(await response.text(), 'bounded'); }
  assert.equal((await fetch(f.localURL(exported.downloadUrl))).status, 429);
  await fs.writeFile(join(f.project, 'empty'), Buffer.alloc(0));
  const empty = await f.call('export_direct_file', { path: 'empty', requestId: 'empty' });
  const response = await fetch(f.localURL(empty.downloadUrl)); assert.equal(response.status, 200); assert.equal((await response.arrayBuffer()).byteLength, 0);
  assert.equal((await fetch(f.localURL(empty.downloadUrl), { method: 'HEAD' })).headers.get('content-length'), '0');
});

test('revocation wins over calls awaiting ready/sweep and a later reauthorized call works', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'fixture'), 'original');
  for (let index = 0; index < 10; index++) {
    const pending = f.call('export_direct_file', { path: 'fixture', requestId: `before-revoke-${index}` });
    await f.manager.revokeOwner(owner.owner); const result = await pending;
    assert.equal(result.ok, false); assert.equal(result.error.code, 'transfer_access_revoked'); assert.equal(result.downloadUrl, undefined);
  }
  const renewed = await f.call('export_direct_file', { path: 'fixture', requestId: 'reauthorized' });
  assert.equal(renewed.state, 'ready'); assert.equal((await fetch(f.localURL(renewed.downloadUrl))).status, 200);
  await fs.writeFile(join(f.project, `${'a'.repeat(179)}😀`), 'unicode');
  const unicode = await f.call('export_direct_file', { path: `${'a'.repeat(179)}😀`, requestId: 'unicode-boundary' });
  assert.equal(unicode.ok, true); assert.ok(unicode.downloadUrl.includes('%F0%9F%98%80'));
});

test('queued import cancels on revoke/close without waiting for a held text lock or committing later', async t => {
  for (const close of [false, true]) {
    const content = Buffer.from('must-not-commit');
    const f = await fixture(t, { downloadClient: downloader(content) });
    let release; const held = new Promise(resolve => { release = resolve; });
    let locked; const entered = new Promise(resolve => { locked = resolve; });
    const holding = f.directFiles.withWriteLock(async () => { locked(); await held; }); await entered;
    let queued; const queueEntered = new Promise(resolve => { queued = resolve; });
    const originalLock = f.directFiles.withWriteLock.bind(f.directFiles);
    f.directFiles.withWriteLock = execute => { queued(); return originalLock(execute); };
    const result = f.call('import_direct_file', { path: 'no-commit', requestId: 'queued-import', expectedSha256: null,
      file: { download_url: 'https://file.example/fixture', file_id: 'reference' } });
    await queueEntered;
    const finish = close ? f.manager.close() : f.manager.revokeOwner(owner.owner);
    const timeout = new Promise((resolve, reject) => { const timer = setTimeout(() => reject(Error('Cancellation waited for text lock')), 1000); timer.unref(); finish.then(() => { clearTimeout(timer); resolve(); }, reject); });
    await timeout;
    assert.equal((await result).state, 'interrupted'); assert.deepEqual(await fs.readdir(join(f.stateDir, 'transfer-spool')), []);
    release(); await holding; await new Promise(resolve => setImmediate(resolve));
    await assert.rejects(fs.stat(join(f.project, 'no-commit')), { code: 'ENOENT' });
  }
});
