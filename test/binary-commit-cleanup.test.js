import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

test('cleanup I/O failure cannot hide a successful exclusive binary commit', async t => {
  const base = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'binary-commit-'));
  t.after(() => rm(base, { recursive: true, force: true }));
  const moduleUrl = new URL('../src/binary-files-runtime.mjs', import.meta.url).href;
  const script = `
    import fs from 'node:fs/promises';
    import { syncBuiltinESMExports } from 'node:module';
    import { Readable } from 'node:stream';
    import { importBinary } from ${JSON.stringify(moduleUrl)};
    const original = fs.unlink;
    let injected = false;
    fs.unlink = async path => {
      if (path.includes('.direct-binary-')) { injected = true; throw Object.assign(new Error('injected cleanup I/O failure'), { code: 'EIO' }); }
      return original(path);
    };
    syncBuiltinESMExports();
    const result = await importBinary({ projectPath: ${JSON.stringify(base)}, roots: [${JSON.stringify(base)}],
      path: 'committed.bin', expectedSha256: null }, Readable.from([Buffer.from('committed\\0bytes')]));
    process.stdout.write(JSON.stringify({ ...result, injected }));
  `;
  const { stdout, stderr } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script]);
  assert.equal(stderr, '');
  assert.equal(JSON.parse(stdout).bytes, 15);
  assert.equal(JSON.parse(stdout).injected, true);
  assert.deepEqual(await readFile(join(base, 'committed.bin')), Buffer.from('committed\0bytes'));
});
