import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, copyFile, writeFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execute = promisify(execFile);

test('a relocated checkout with spaces and an apostrophe produces a working message command', async t => {
  const root = await mkdtemp(join(tmpdir(), "local agents'install-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, 'bin'));
  await writeFile(join(root, 'package.json'), '{"type":"module"}');
  await symlink(fileURLToPath(new URL('../node_modules/', import.meta.url)), join(root, 'node_modules'), 'dir');
  for (const name of ['collaboration.js', 'wait-result.js']) {
    await copyFile(new URL(`../src/${name}`, import.meta.url), join(root, 'src', name));
  }
  // Exercise the real generated shell command without opening an agent channel.
  await writeFile(join(root, 'bin', 'dot-message.mjs'), 'process.stdout.write(JSON.stringify(process.argv.slice(2)));');
  const { Collaboration } = await import(pathToFileURL(join(root, 'src', 'collaboration.js')));
  const instruction = new Collaboration({}).promptInstructions('installation-test');
  const command = instruction.split('\n').find(line => line.startsWith('node '));
  assert.ok(command);
  const { stdout } = await execute('/bin/sh', ['-c', command], { env: { ...process.env, PASEO_AGENT_ID: '' } });
  assert.deepEqual(JSON.parse(stdout), ['--request-id', 'installation-test', '--kind', 'message']);
});
