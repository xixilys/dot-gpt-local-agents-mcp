// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { DirectFiles, DIRECT_FILE_TOOLS } from '../src/direct-files.js';
const owner = { owner: 'test-owner' };
const sha = text => createHash('sha256').update(text).digest('hex');
async function fixture(t, { write = true, read = true } = {}) {
  const root = await fs.realpath(await fs.mkdtemp(join(tmpdir(), 'direct-files-')));
  const project = join(root, 'project'); await fs.mkdir(project);
  const host = { id: 'mac', transport: 'local', allowedRoots: [root], direct: { projects: [{ id: 'p', path: project, write, read }] } };
  const files = new DirectFiles({ host });
  t.after(async () => { files.close(); await fs.rm(root, { recursive: true, force: true }); });
  return { root, project, host, files, call: (name, args) => files.call(name, { projectId: 'p', ...args }, owner) };
}

test('Direct files schemas reject unknown fields and require owner/project permissions', async t => {
  const f = await fixture(t, { write: false });
  assert.equal(DIRECT_FILE_TOOLS.length, 5);
  assert.equal((await f.files.call('list_direct_files', { projectId: 'p' })).error.code, 'owner_required');
  assert.equal((await f.call('list_direct_files', { owner: 'forged' })).error.code, 'invalid_arguments');
  assert.equal((await f.call('list_direct_files', { hostId: 'wsl' })).error.code, 'invalid_arguments');
  assert.equal((await f.call('read_direct_file', { path: 'x', offset: -1 })).error.code, 'invalid_arguments');
  assert.equal((await f.call('edit_direct_file', { path: 'x', expectedSha256: sha(''), oldText: '', newText: '' })).error.code, 'invalid_arguments');
  assert.equal((await f.call('write_direct_file', { path: 'x', content: 'x', expectedSha256: null })).error.code, 'project_access_denied');
  assert.equal((await f.files.call('list_direct_files', { projectId: 'missing' }, owner)).error.code, 'project_access_denied');
  f.files.close(); assert.equal((await f.call('list_direct_files', {})).error.code, 'direct_files_closed');
});

test('exclusive creation, hash conflicts, and atomic overwrite preserve unrelated files', async t => {
  const f = await fixture(t);
  const first = await f.call('write_direct_file', { path: 'file.txt', content: 'one\ntwo\n', expectedSha256: null });
  assert.equal(first.created, true); assert.equal(first.sha256, sha('one\ntwo\n'));
  assert.equal((await f.call('write_direct_file', { path: 'file.txt', content: 'overwrite', expectedSha256: null })).error.code, 'file_exists');
  assert.equal((await f.call('write_direct_file', { path: 'file.txt', content: 'overwrite', expectedSha256: sha('bad') })).error.code, 'hash_conflict');
  assert.equal((await f.call('write_direct_file', { path: 'missing.txt', content: 'x', expectedSha256: sha('bad') })).error.code, 'hash_conflict');
  assert.equal(await fs.readFile(join(f.project, 'file.txt'), 'utf8'), 'one\ntwo\n');
  const next = await f.call('write_direct_file', { path: 'file.txt', content: 'three', expectedSha256: first.sha256.toUpperCase() });
  assert.equal(next.ok, true); assert.equal(next.created, false);
  assert.equal(await fs.readFile(join(f.project, 'file.txt'), 'utf8'), 'three');
  assert.deepEqual(await fs.readdir(f.project), ['file.txt']);
});

test('exact edits are literal, unambiguous by default and explicitly replace all', async t => {
  const f = await fixture(t); const original = 'hello hello\n';
  await fs.writeFile(join(f.project, 'f'), original);
  const args = { path: 'f', expectedSha256: sha(original), oldText: 'hello', newText: '$&$1' };
  assert.equal((await f.call('edit_direct_file', args)).error.code, 'ambiguous_edit');
  assert.equal((await f.call('edit_direct_file', { ...args, oldText: 'absent' })).error.code, 'text_not_found');
  const result = await f.call('edit_direct_file', { ...args, replaceAll: true });
  assert.equal(result.replacements, 2); assert.equal(await fs.readFile(join(f.project, 'f'), 'utf8'), '$&$1 $&$1\n');
  const edited = await f.call('edit_direct_file', { path: 'f', expectedSha256: result.sha256, oldText: '$&$1 $&$1', newText: 'done' });
  assert.equal(edited.replacements, 1); assert.equal(await fs.readFile(join(f.project, 'f'), 'utf8'), 'done\n');
});

test('equivalent-path writes serialize and stale writers conflict', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'f'), 'initial');
  const results = await Promise.all(['f', './f'].map((path, index) => f.call('write_direct_file', { path, expectedSha256: sha('initial'), content: `update-${index}` })));
  assert.equal(results.filter(r => r.ok).length, 1); assert.equal(results.filter(r => r.error?.code === 'hash_conflict').length, 1);
  const newResults = await Promise.all(['new', './new'].map(path => f.call('write_direct_file', { path, expectedSha256: null, content: 'exclusive' })));
  assert.equal(newResults.filter(r => r.ok).length, 1); assert.equal(newResults.filter(r => r.error?.code === 'file_exists').length, 1);
});

test('case aliases cannot concurrently overwrite the same expected version', async t => {
  const f = await fixture(t); const lower = join(f.project, 'case'); const upper = join(f.project, 'CASE');
  await fs.writeFile(lower, 'probe');
  let alias;
  try { alias = await fs.stat(upper); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (!alias || alias.ino !== (await fs.stat(lower)).ino) { t.skip('Temporary volume is case-sensitive'); return; }
  for (let round = 0; round < 30; round++) {
    const initial = `initial-${round}`; await fs.writeFile(lower, initial);
    const results = await Promise.all(['case', 'CASE'].map((path, index) => f.call('write_direct_file', {
      path, expectedSha256: sha(initial), content: `update-${round}-${index}`,
    })));
    assert.equal(results.filter(result => result.ok).length, 1, `round ${round}`);
    assert.equal(results.filter(result => result.error?.code === 'hash_conflict').length, 1, `round ${round}`);
  }
});

test('two registered projects pointing to one directory share the write queue', async t => {
  const f = await fixture(t);
  const files = new DirectFiles({ host: { ...f.host, direct: { projects: [
    { id: 'one', path: f.project, write: true }, { id: 'two', path: `${f.project}/.`, write: true },
  ] } } }); t.after(() => files.close());
  for (let round = 0; round < 10; round++) {
    const initial = `initial-${round}`; await fs.writeFile(join(f.project, 'shared'), initial);
    const results = await Promise.all(['one', 'two'].map((projectId, index) => files.call('write_direct_file', {
      projectId, path: 'shared', expectedSha256: sha(initial), content: `update-${round}-${index}`,
    }, owner)));
    assert.equal(results.filter(result => result.ok).length, 1);
    assert.equal(results.filter(result => result.error?.code === 'hash_conflict').length, 1);
  }
});

test('canonical roots, traversal, symlink targets and parents are rejected', async t => {
  const f = await fixture(t); const outside = join(f.root, 'outside'); await fs.mkdir(outside); await fs.writeFile(join(outside, 'private'), 'outside');
  await fs.symlink(outside, join(f.project, 'escape')); await fs.symlink(join(outside, 'private'), join(f.project, 'link'));
  for (const path of ['../outside/private', '/etc/passwd', 'a/../../outside', 'bad\0name']) assert.equal((await f.call('read_direct_file', { path })).error.code, 'invalid_path');
  for (const path of ['escape/private', 'link']) {
    assert.equal((await f.call('read_direct_file', { path })).error.code, 'symlink_rejected');
    assert.equal((await f.call('write_direct_file', { path, content: 'bad', expectedSha256: null })).error.code, 'symlink_rejected');
  }
  assert.equal((await f.call('write_direct_file', { path: 'escape/new', content: 'bad', expectedSha256: null })).error.code, 'symlink_rejected');
  assert.equal(await fs.readFile(join(outside, 'private'), 'utf8'), 'outside');
  const denied = new DirectFiles({ host: { ...f.host, allowedRoots: [outside] } }); t.after(() => denied.close());
  assert.equal((await denied.call('list_direct_files', { projectId: 'p' }, owner)).error.code, 'outside_allowed_roots');
  const linked = new DirectFiles({ host: { ...f.host, direct: { projects: [{ id: 'p', path: join(f.project, 'escape') }] } } }); t.after(() => linked.close());
  assert.equal((await linked.call('list_direct_files', { projectId: 'p' }, owner)).error.code, 'symlink_rejected');
});

test('text limits, line slicing, binary rejection and bounded output', async t => {
  const f = await fixture(t); await fs.writeFile(join(f.project, 'lines'), 'alpha\nbeta\ngamma');
  const read = await f.call('read_direct_file', { path: 'lines', offset: 1, limit: 1 });
  assert.equal(read.content, 'beta'); assert.equal(read.totalLines, 3); assert.equal(read.truncated, true); assert.equal(read.sha256, sha('alpha\nbeta\ngamma'));
  assert.equal((await f.call('read_direct_file', { path: 'lines', offset: 99 })).content, '');
  await fs.writeFile(join(f.project, 'binary'), Buffer.from([0xff, 0xfe]));
  await fs.writeFile(join(f.project, 'nul'), 'a\0b');
  for (const path of ['binary', 'nul']) assert.equal((await f.call('read_direct_file', { path })).error.code, 'not_text');
  await fs.writeFile(join(f.project, 'large'), 'x'.repeat(204801));
  assert.equal((await f.call('read_direct_file', { path: 'large' })).error.code, 'text_file_too_large');
  assert.equal((await f.call('write_direct_file', { path: 'unicode', content: '中'.repeat(70000), expectedSha256: null })).error.code, 'text_file_too_large');
  await fs.writeFile(join(f.project, 'longline'), '中'.repeat(30000));
  const bounded = await f.call('read_direct_file', { path: 'longline' });
  assert.equal(bounded.truncated, true); assert.ok(Buffer.byteLength(bounded.content) <= 65536); assert.ok(!bounded.content.includes('\ufffd'));
  await fs.writeFile(join(f.project, 'escaped'), '\u0001'.repeat(60000));
  const escaped = await f.call('read_direct_file', { path: 'escaped' });
  assert.equal(escaped.truncated, true); assert.ok(Buffer.byteLength(JSON.stringify(escaped.content)) <= 65538);
  await fs.mkdir(join(f.project, '.git'));
  assert.equal((await f.call('write_direct_file', { path: '.git/config', content: 'bad', expectedSha256: null })).error.code, 'protected_path');
  await fs.writeFile(join(f.project, '.env.test'), 'fixture=ok');
  assert.equal((await f.call('read_direct_file', { path: '.env.test' })).content, 'fixture=ok');
});

test('bounded listing and literal recursive search skip binary/dependencies/symlinks', async t => {
  const f = await fixture(t); await fs.mkdir(join(f.project, 'src')); await fs.mkdir(join(f.project, 'node_modules')); await fs.mkdir(join(f.project, '.git'));
  await fs.writeFile(join(f.project, 'src', 'a'), 'literal .* token\nno match\nliteral .* again');
  await fs.writeFile(join(f.project, 'src', 'b'), 'literal x token');
  await fs.writeFile(join(f.project, 'node_modules', 'ignored'), '.*'); await fs.writeFile(join(f.project, '.git', 'ignored'), '.*');
  await fs.writeFile(join(f.project, 'weights.pt'), '.*'); await fs.writeFile(join(f.project, 'invalid'), Buffer.from([0xff]));
  await fs.symlink(join(f.project, 'src'), join(f.project, 'linked'));
  const list = await f.call('list_direct_files', { limit: 1 }); assert.equal(list.entries.length, 1); assert.equal(list.truncated, true);
  const result = await f.call('search_direct_files', { query: '.*', limit: 1 });
  assert.equal(result.matches.length, 1); assert.equal(result.matches[0].path, 'src/a'); assert.equal(result.matches[0].line, 1); assert.equal(result.truncated, true); assert.ok(result.skipped >= 4);
  const full = await f.call('search_direct_files', { path: 'src', query: '.*' }); assert.equal(full.matches.length, 2); assert.equal(full.truncated, false);
  const empty = await f.call('search_direct_files', { query: '[abc' }); assert.deepEqual(empty.matches, []);
});

test('search counts invalid text against its byte budget', async t => {
  const f = await fixture(t);
  for (let index = 0; index < 12; index++) await fs.writeFile(join(f.project, `invalid-${index}`), Buffer.alloc(200 * 1024, 0xff));
  const result = await f.call('search_direct_files', { query: 'never' });
  assert.equal(result.truncated, true); assert.ok(result.scannedBytes <= 2 * 1024 * 1024); assert.ok(result.skipped >= 10);
});

test('SSH executes fixed helper on the selected target with stdin-only arguments, independent of daemon', async t => {
  const f = await fixture(t); let calls = 0;
  const runRemote = async (script, input, options) => {
    calls++; assert.ok(script.includes('runFileOperation')); assert.ok(!script.includes('ssh-file.txt')); assert.ok(options.maxBytes <= 1024 * 1024);
    assert.equal(input.projectPath, f.project); assert.deepEqual(input.roots, [f.root]);
    return new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['pipe', 'pipe', 'pipe'] }); let output = '', errors = '';
      child.stdout.on('data', c => { output += c; }); child.stderr.on('data', c => { errors += c; }); child.on('error', reject);
      child.on('close', code => code === 0 ? resolve(JSON.parse(output)) : reject(new Error(errors))); child.stdin.end(JSON.stringify(input));
    });
  };
  const remote = new DirectFiles({ host: { ...f.host, id: 'wsl', transport: 'ssh', target: 'ssh://configured-host' }, runRemote }); t.after(() => remote.close());
  const args = { projectId: 'p', path: 'ssh-file.txt', content: 'literal $(touch forbidden)\n', expectedSha256: null };
  assert.equal((await remote.call('write_direct_file', args, owner)).ok, true);
  assert.equal((await remote.call('read_direct_file', { projectId: 'p', path: 'ssh-file.txt' }, owner)).content, args.content);
  assert.equal(calls, 2); assert.equal(await fs.readFile(join(f.project, 'ssh-file.txt'), 'utf8'), args.content);
});

test('read-disabled project and SSH errors return safe errors without payloads', async t => {
  const f = await fixture(t, { read: false });
  assert.equal((await f.call('list_direct_files', {})).error.code, 'project_access_denied');
  const remote = new DirectFiles({ host: { ...f.host, transport: 'ssh', target: 'ssh://configured-host' }, runRemote: async () => { throw new Error('secret payload'); } }); t.after(() => remote.close());
  const result = await remote.call('write_direct_file', { projectId: 'p', path: 'x', content: 'secret content', expectedSha256: null }, owner);
  assert.equal(result.error.code, 'remote_file_operation_failed'); assert.ok(!JSON.stringify(result).includes('secret'));
});
