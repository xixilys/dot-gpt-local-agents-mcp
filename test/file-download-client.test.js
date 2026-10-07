import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { EventEmitter } from 'node:events';
import { Readable, PassThrough } from 'node:stream';
import { createHash } from 'node:crypto';
import { SafeFileDownloadClient } from '../src/file-download-client.js';

const sha = buffer => createHash('sha256').update(buffer).digest('hex');
const publicDNS = async () => [{ address: '93.184.216.34', family: 4 }];
async function fixture(t) {
  const root = await fs.mkdtemp(join(tmpdir(), 'safe-download-'));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return join(root, 'download.bin');
}
function fakeRequest({ body = Buffer.from('binary\0bytes'), status = 200, headers = {}, inspect, stream } = {}) {
  return (options, callback) => {
    inspect?.(options);
    const req = new EventEmitter();
    req.destroy = () => {};
    req.end = () => queueMicrotask(() => {
      const res = stream ?? Readable.from([body]);
      res.statusCode = status; res.headers = headers; callback(res);
    });
    return req;
  };
}

test('streaming download pins validated DNS and stores exact 11 MiB binary with private mode', async t => {
  const destination = await fixture(t);
  const body = Buffer.alloc(11 * 1024 * 1024); for (let index = 0; index < body.length; index++) body[index] = index % 251;
  let lookups = 0;
  const client = new SafeFileDownloadClient({ resolve: async (...args) => { lookups++; return publicDNS(...args); },
    request: fakeRequest({ body, headers: { 'content-length': String(body.length) }, inspect(options) {
      assert.equal(options.method, 'GET'); assert.equal(options.rejectUnauthorized, true); assert.equal(options.agent, false);
      assert.equal(options.headers['Accept-Encoding'], 'identity'); assert.equal(options.headers.Authorization, undefined);
      assert.equal(options.headers['User-Agent'], 'Local-Agents-MCP/0.5.0');
      options.lookup('rebinding.example', {}, (error, address, family) => { assert.equal(error, null); assert.equal(address, '93.184.216.34'); assert.equal(family, 4); });
      options.lookup('rebinding.example', { all: true }, (error, addresses) => { assert.equal(error, null); assert.deepEqual(addresses, [{ address: '93.184.216.34', family: 4 }]); });
    } }) });
  const result = await client.downloadTo('https://files.example/file?secret=never-log', { destination, maxBytes: body.length, sha256: sha(body) });
  assert.deepEqual(result, { bytes: body.length, sha256: sha(body) }); assert.equal(lookups, 1);
  assert.deepEqual(await fs.readFile(destination), body); assert.equal((await fs.stat(destination)).mode & 0o777, 0o600);
});

test('unsafe URL and any private/mixed DNS answer are rejected before request', async t => {
  const destination = await fixture(t); let requests = 0;
  const client = new SafeFileDownloadClient({ resolve: async () => [
    { address: '93.184.216.34', family: 4 }, { address: '127.0.0.1', family: 4 },
  ], request() { requests++; throw Error('Must not connect'); } });
  for (const value of ['http://public.example/file', 'https://user:password@public.example/file', 'https://public.example/file#secret']) {
    await assert.rejects(client.downloadTo(value, { destination, maxBytes: 100 }), { code: 'invalid_download_url' });
  }
  for (const value of ['https://public.example/file', 'https://127.0.0.1/file', 'https://[::1]/file', 'https://169.254.169.254/file']) {
    await assert.rejects(client.downloadTo(value, { destination, maxBytes: 100 }), { code: 'unsafe_download_destination' });
  }
  assert.equal(requests, 0); await assert.rejects(fs.stat(destination), { code: 'ENOENT' });
});

test('redirects, encoding, oversize, short content and hash failures remove only owned partials', async t => {
  const destination = await fixture(t);
  const cases = [
    [{ status: 302, headers: { location: 'https://127.0.0.1/private' } }, 'download_redirect_rejected'],
    [{ headers: { 'content-encoding': 'gzip' } }, 'download_encoding_rejected'],
    [{ headers: { 'content-length': '1000' } }, 'file_too_large'],
    [{ body: Buffer.alloc(101) }, 'file_too_large'],
    [{ body: Buffer.from('short'), headers: { 'content-length': '99' } }, 'download_length_mismatch'],
    [{ body: Buffer.from('hash') }, 'hash_mismatch'],
  ];
  for (const [response, code] of cases) {
    const client = new SafeFileDownloadClient({ resolve: publicDNS, request: fakeRequest(response) });
    await assert.rejects(client.downloadTo('https://public.example/private?credential=hidden', { destination, maxBytes: 100,
      ...(code === 'hash_mismatch' ? { sha256: sha('different') } : {}) }), error => {
      assert.equal(error.code, code); assert.ok(!error.message.includes('credential')); return true;
    });
    await assert.rejects(fs.stat(destination), { code: 'ENOENT' });
  }
  await fs.writeFile(destination, 'preexisting');
  const client = new SafeFileDownloadClient({ resolve: publicDNS, request: fakeRequest() });
  await assert.rejects(client.downloadTo('https://public.example/file', { destination, maxBytes: 100 }), { code: 'download_failed' });
  assert.equal(await fs.readFile(destination, 'utf8'), 'preexisting');
});

test('deadline covers stalled DNS and late resolution cannot start a request', async t => {
  const destination = await fixture(t); let finishDNS, requests = 0;
  const client = new SafeFileDownloadClient({ resolve: () => new Promise(resolve => { finishDNS = resolve; }),
    request() { requests++; }, timeoutMs: 15 });
  await assert.rejects(client.downloadTo('https://public.example/file', { destination, maxBytes: 100 }), { code: 'transfer_timeout' });
  finishDNS(await publicDNS()); await new Promise(resolve => setImmediate(resolve));
  assert.equal(requests, 0); await assert.rejects(fs.stat(destination), { code: 'ENOENT' });
});

test('caller abort and whole-stream deadline terminate a slow response and remove partial spool', async t => {
  const destination = await fixture(t);
  for (const callerAbort of [true, false]) {
    const response = new PassThrough(); response.on('error', () => {});
    const client = new SafeFileDownloadClient({ resolve: publicDNS, request: fakeRequest({ stream: response }), timeoutMs: 30 });
    const controller = new AbortController();
    const action = client.downloadTo('https://public.example/file', { destination, maxBytes: 100, signal: controller.signal });
    response.write('partial');
    if (callerAbort) setTimeout(() => controller.abort(), 10);
    await assert.rejects(action, { code: callerAbort ? 'transfer_aborted' : 'transfer_timeout' });
    assert.equal(response.destroyed, true); await assert.rejects(fs.stat(destination), { code: 'ENOENT' });
  }
});
