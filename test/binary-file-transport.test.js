import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { Readable, PassThrough } from 'node:stream';
import { spawn } from 'node:child_process';
import { createBinaryFileTransport } from '../src/binary-file-transport.js';
import { exportBinary } from '../src/binary-files-runtime.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const code = expected => error => { assert.equal(error.safeCode ?? error.code, expected); return true; };
async function fixture(t, ssh = false, overrides = {}) {
  const root = await fs.realpath(await fs.mkdtemp(join(tmpdir(), 'binary-transfer-')));
  const project = join(root, 'project'); await fs.mkdir(project);
  const spawns = [];
  const host = { transport: ssh ? 'ssh' : 'local', target: 'ssh://fixture' };
  // Run the exact fixed remote program through a real Node subprocess; only
  // replace SSH's connection layer. Do not emulate the file implementation.
  const spawnImpl = (command, args, options) => {
    assert.equal(command, 'ssh');
    const commandText = args.at(-1), marker = ' --input-type=module -e ';
    const quotedScript = commandText.slice(commandText.indexOf(marker) + marker.length);
    const originalScript = quotedScript.slice(1, -1).replaceAll("'\\''", "'");
    const script = overrides.transformScript ? overrides.transformScript(originalScript) : originalScript;
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], options);
    spawns.push({ args, child }); return child;
  };
  const transport = createBinaryFileTransport({ host, spawnImpl, ...overrides });
  t.after(async () => { transport.close(); await fs.rm(root, { recursive: true, force: true }); });
  const input = path => ({ projectPath: project, roots: [root], path });
  return { root, project, transport, input, spawns };
}

for (const ssh of [false, true]) {
  const label = ssh ? 'fixed SSH program' : 'local';
  test(`${label}: binary NUL/Unicode and 11+ MiB export/import preserve bytes and snapshot`, async t => {
    const f = await fixture(t, ssh);
    const data = Buffer.alloc(11 * 1024 * 1024 + 799); for (let i = 0; i < data.length; i++) data[i] = i % 251;
    const name = '图片 零\u0000'.replace('\u0000', '') + '.bin';
    await fs.writeFile(join(f.project, name), data);
    const spool = join(f.root, 'snapshot');
    const exported = await f.transport.exportTo({ ...f.input(name), destination: spool });
    assert.deepEqual(exported, { bytes: data.length, sha256: hash(data), fileName: name });
    assert.equal((await fs.stat(spool)).mode & 0o777, 0o600);
    await fs.writeFile(join(f.project, name), 'changed');
    assert.deepEqual(await fs.readFile(spool), data);
    const imported = await f.transport.importFrom({ ...f.input('copy.bin'), expectedSha256: null, bytes: data.length, sha256: exported.sha256 }, (await fs.open(spool)).createReadStream());
    assert.deepEqual(imported, { bytes: data.length, sha256: exported.sha256, path: 'copy.bin' });
    assert.deepEqual(await fs.readFile(join(f.project, 'copy.bin')), data);
    assert.equal((await fs.stat(join(f.project, 'copy.bin'))).mode & 0o777, 0o600);
    if (ssh) {
      assert.equal(f.spawns.length, 2);
      assert.ok(!f.spawns[0].args.at(-1).includes(f.project));
      assert.ok(!f.spawns[1].args.at(-1).includes('copy.bin'));
    }
  });

  test(`${label}: expected hash, mode preservation, and race during receive`, async t => {
    const f = await fixture(t, ssh), target = join(f.project, 'target');
    await fs.writeFile(target, 'before'); await fs.chmod(target, 0o640);
    await assert.rejects(f.transport.importFrom({ ...f.input('target'), expectedSha256: hash('wrong') }, Readable.from([Buffer.from('after')])), code('hash_conflict'));
    await assert.rejects(f.transport.importFrom({ ...f.input('target'), expectedSha256: null }, Readable.from([Buffer.from('after')])), code('file_exists'));
    const result = await f.transport.importFrom({ ...f.input('target'), expectedSha256: hash('before') }, Readable.from([Buffer.from('after')]));
    assert.equal(result.sha256, hash('after')); assert.equal((await fs.stat(target)).mode & 0o777, 0o640);
    let altered = false;
    const stream = Readable.from((async function* () {
      yield Buffer.from('new');
      // Delay until the runtime has opened its temp file and checked old hash.
      for (let n = 0; n < 200; n++) {
        if ((await fs.readdir(f.project)).some(name => name.startsWith('.direct-binary-'))) { altered = true; break; }
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      await fs.writeFile(target, 'external'); yield Buffer.from('content');
    })());
    await assert.rejects(f.transport.importFrom({ ...f.input('target'), expectedSha256: hash('after') }, stream), code('hash_conflict'));
    assert.equal(altered, true); assert.equal(await fs.readFile(target, 'utf8'), 'external');
    assert.deepEqual(await fs.readdir(f.project), ['target']);
  });

  test(`${label}: declared length/hash/limits reject and remove temporary files`, async t => {
    const f = await fixture(t, ssh);
    for (const [name, params, expected] of [
      ['short', { bytes: 9 }, 'length_mismatch'],
      ['overflow', { maxBytes: 2 }, 'binary_file_too_large'],
      ['hash', { sha256: hash('wrong') }, 'hash_mismatch'],
    ]) {
      await assert.rejects(f.transport.importFrom({ ...f.input(name), expectedSha256: null, ...params }, Readable.from([Buffer.from('abc')])), code(expected));
      assert.deepEqual(await fs.readdir(f.project), []);
    }
    await fs.writeFile(join(f.project, 'large'), 'abc');
    const spool = join(f.root, 'failed');
    await assert.rejects(f.transport.exportTo({ ...f.input('large'), maxBytes: 2, destination: spool }), code('binary_file_too_large'));
    await assert.rejects(fs.stat(spool), { code: 'ENOENT' });
    await fs.writeFile(spool, 'untouched');
    await assert.rejects(f.transport.exportTo({ ...f.input('large'), destination: spool }), { code: 'EEXIST' });
    assert.equal(await fs.readFile(spool, 'utf8'), 'untouched');
  });

  test(`${label}: path, symlink, roots, regular-file and Git write protections`, async t => {
    const f = await fixture(t, ssh);
    await fs.mkdir(join(f.project, '.git')); await fs.mkdir(join(f.root, 'outside')); await fs.writeFile(join(f.root, 'outside', 'file'), 'private');
    await fs.symlink(join(f.root, 'outside'), join(f.project, 'link'));
    for (const [path, expected] of [['../outside/file', 'invalid_path'], ['/etc/passwd', 'invalid_path'], ['link/file', 'symlink_rejected'], ['.git/config', 'protected_path']]) {
      await assert.rejects(f.transport.importFrom({ ...f.input(path), expectedSha256: null }, Readable.from([Buffer.from('bad')])), code(expected));
    }
    await assert.rejects(f.transport.exportTo({ ...f.input('.git'), destination: join(f.root, 'dir') }), code('not_regular_file'));
    await assert.rejects(f.transport.importFrom({ ...f.input('new'), roots: [join(f.root, 'outside')], expectedSha256: null }, Readable.from([])), code('outside_allowed_roots'));
    assert.equal(await fs.readFile(join(f.root, 'outside', 'file'), 'utf8'), 'private');
  });

  test(`${label}: empty file and shell-like filename remain data`, async t => {
    const f = await fixture(t, ssh), path = "quote'$(touch SHOULD_NOT_EXIST);.bin";
    const result = await f.transport.importFrom({ ...f.input(path), expectedSha256: null, bytes: 0 }, Readable.from([]));
    assert.equal(result.bytes, 0); assert.equal(result.sha256, hash(''));
    const exported = await f.transport.exportTo({ ...f.input(path), destination: join(f.root, 'empty') });
    assert.equal(exported.fileName, path); assert.equal((await fs.stat(join(f.root, 'empty'))).size, 0);
    assert.deepEqual(await fs.readdir(f.project), [path]);
  });

  test(`${label}: abort and deadline clean partial imports; close prevents reuse`, async t => {
    const f = await fixture(t, ssh);
    const controller = new AbortController(), stream = new PassThrough();
    const pending = f.transport.importFrom({ ...f.input('partial'), expectedSha256: null }, stream, { signal: controller.signal });
    stream.write(Buffer.from('partial'));
    await new Promise(resolve => setTimeout(resolve, ssh ? 250 : 25)); controller.abort();
    await assert.rejects(pending, code('transfer_aborted'));
    assert.deepEqual(await fs.readdir(f.project), []);
    await assert.rejects(f.transport.importFrom({ ...f.input('timeout'), expectedSha256: null }, new PassThrough(), { timeoutMs: ssh ? 250 : 25 }), code('transfer_timeout'));
    assert.deepEqual(await fs.readdir(f.project), []);
    f.transport.close();
    await assert.rejects(f.transport.exportTo({ ...f.input('partial'), destination: join(f.root, 'closed') }), code('binary_transport_closed'));
  });
}

test('SSH disconnect before completion frame cannot commit an unknown-length import', async t => {
  const f = await fixture(t, true);
  const source = new PassThrough();
  const pending = f.transport.importFrom({ ...f.input('disconnected'), expectedSha256: null }, source);
  source.write(Buffer.alloc(65536, 13));
  for (let i = 0; i < 200; i++) {
    if ((await fs.readdir(f.project)).some(name => name.startsWith('.direct-binary-'))) break;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  // Model lost SSH stdin without a normal producer EOF: no terminator arrives.
  f.spawns[0].child.stdin.end();
  await assert.rejects(pending, error => { assert.equal(error.code, 'truncated_stream'); assert.notEqual(error.resultUnknown, true); return true; });
  assert.deepEqual(await fs.readdir(f.project), []);
});

test('source mutation during a streamed export rejects the snapshot', async t => {
  const f = await fixture(t);
  const source = join(f.project, 'changing'); await fs.writeFile(source, Buffer.alloc(200000));
  let changed = false;
  await assert.rejects(exportBinary(f.input('changing'), async () => {
    if (!changed) { changed = true; await fs.appendFile(source, 'extra'); }
  }), code('source_changed'));
});

test('pre-aborted imports destroy their incoming source and deadline validation is positive', async t => {
  const f = await fixture(t);
  const controller = new AbortController(); controller.abort();
  const source = new PassThrough();
  await assert.rejects(f.transport.importFrom({ ...f.input('new'), expectedSha256: null }, source, { signal: controller.signal }), code('transfer_aborted'));
  assert.equal(source.destroyed, true);
  await assert.rejects(f.transport.exportTo({ ...f.input('new'), destination: join(f.root, 'spool'), timeoutMs: 0 }), code('invalid_deadline'));
  assert.deepEqual(await fs.readdir(f.project), []);
});

test('SSH early hash rejection retains its conflict while a large producer is backpressured', async t => {
  const f = await fixture(t, true); await fs.writeFile(join(f.project, 'existing'), 'original');
  const source = Readable.from((async function* () { for (let n = 0; n < 2000; n++) yield Buffer.alloc(65536); })());
  await assert.rejects(f.transport.importFrom({ ...f.input('existing'), expectedSha256: hash('wrong') }, source), code('hash_conflict'));
  assert.equal(await fs.readFile(join(f.project, 'existing'), 'utf8'), 'original');
  assert.deepEqual(await fs.readdir(f.project), ['existing']);
});

for (const mode of ['lost', 'malformed', 'connection-closed', 'timeout']) {
  test(`SSH import committed but ${mode} acknowledgement reports resultUnknown`, async t => {
    const resultWrite = 'process.stdout.write(JSON.stringify({ ok: true, ...result }))';
    const replacement = mode === 'malformed' ? "process.stdout.write('{truncated')"
      : mode === 'connection-closed' ? 'process.exitCode = 255'
      : mode === 'timeout' ? 'await new Promise(resolve => setTimeout(resolve, 5000))'
      : "process.stdout.write('')";
    const f = await fixture(t, true, { transformScript: script => {
      assert.ok(script.includes(resultWrite)); return script.replace(resultWrite, replacement);
    } });
    const payload = Buffer.from('committed binary\0data');
    await assert.rejects(f.transport.importFrom({ ...f.input('committed'), expectedSha256: null }, Readable.from([payload]), { timeoutMs: mode === 'timeout' ? 350 : 5000 }), error => {
      assert.equal(error.resultUnknown, true);
      assert.equal(error.code, mode === 'timeout' ? 'transfer_timeout' : 'remote_invalid_output');
      return true;
    });
    assert.deepEqual(await fs.readFile(join(f.project, 'committed')), payload);
    assert.deepEqual(await fs.readdir(f.project), ['committed']);
  });
}

test('SSH explicit hash rejection stays definite even after all input and terminator were sent', async t => {
  const f = await fixture(t, true);
  await assert.rejects(f.transport.importFrom({ ...f.input('rejected'), expectedSha256: null, sha256: hash('different') }, Readable.from([Buffer.from('input')])), error => {
    assert.equal(error.code, 'hash_mismatch'); assert.notEqual(error.resultUnknown, true); return true;
  });
  assert.deepEqual(await fs.readdir(f.project), []);
});
