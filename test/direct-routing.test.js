import { tmpdir } from 'node:os';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { HostRouter } from '../src/host-router.js';
import { DirectFiles } from '../src/direct-files.js';
import { DirectCommands } from '../src/direct-commands.js';
import { configuredHosts } from '../src/hosts.js';

// Host routing uses local workers in this test. Real SSH behavior is checked
// separately; a daemon failure must never gate Direct tool calls.
test('Direct routes same project IDs by host, remains usable offline and enforces owner/schema', async t => {
  const base = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'dr-'));
  const contexts = [];
  for (const id of ['mac', 'wsl']) {
    const path = join(base, id); await mkdir(path);
    const stateDir = join(path, 'state');
    const host = { id, name: id, transport: 'local', stateDir, allowedRoots: [path],
      direct: { projects: [{ id: 'same', path, write: true, commandMode: 'registered', commands: [{ name: 'echo', argv: [process.execPath, '-e', `process.stdout.write('${id}')`] }] }] } };
    const directFiles = new DirectFiles({ host }), directCommands = new DirectCommands({ host });
    contexts.push({ host, directFiles, directCommands,
      gateway: { catalog: new Map(), async refreshTools() { throw Error('daemon offline'); } },
      adapter: { async status() { throw Error('Paseo must not be contacted for Direct'); } } });
  }
  t.after(async () => { await Promise.all(contexts.map(c => c.directCommands.close())); for (const c of contexts) c.directFiles.close(); await rm(base, { force: true, recursive: true }); });
  const router = new HostRouter(contexts); await router.refreshTools();
  const owner = { owner: 'local-owner:test' };
  await assert.rejects(router.call('list_direct_projects'), /owner/);
  assert.equal((await router.call('list_direct_projects', {}, owner)).structuredContent.hostId, 'mac');
  await assert.rejects(router.call('read_direct_file', { projectId: 'same', path: 'x', injected: true }, owner), /arguments/);
  for (const hostId of ['mac', 'wsl']) {
    const create = await router.call('write_direct_file', { hostId, projectId: 'same', path: 'result.txt', content: hostId, expectedSha256: null }, owner);
    assert.equal(create.structuredContent.ok, true); assert.equal(create.structuredContent.hostId, hostId);
    assert.equal(await readFile(join(base, hostId, 'result.txt'), 'utf8'), hostId);
    const denied = await router.call('write_direct_file', { hostId, projectId: 'same', path: 'result.txt', content: 'overwrite', expectedSha256: null }, owner);
    assert.equal(denied.isError, true);
    const argv = contexts.find(c => c.host.id === hostId).host.direct.projects[0].commands[0].argv;
    const run = (await router.call('run_direct_command', { hostId, projectId: 'same', requestId: 'same-request', argv }, owner)).structuredContent;
    let result, cursor = 0, output = '';
    for (let i = 0; i < 20; i++) {
      result = (await router.call('read_direct_command', { hostId, commandId: run.commandId, cursor, waitMs: 1000 }, owner)).structuredContent;
      output += result.events.map(e => e.text).join(''); cursor = result.nextCursor;
      if (!['pending', 'running'].includes(result.state)) break;
    }
    assert.equal(result.state, 'exited'); assert.equal(result.exitCode, 0);
    assert.equal(output, hostId);
    await assert.rejects(router.call('read_direct_command', { hostId, commandId: run.commandId }, { owner: 'local-owner:other' }), /owner/);
  }
});

test('per-host Direct configuration is explicit and invalid permissions fail loading', () => {
  const config = { allowedRoots: ['/private/tmp'], stateDir: '/private/tmp/host-state', direct: { projects: [{ id: 'mac-project', path: '/private/tmp' }] },
    hosts: [{ id: 'wsl', name: 'WSL', transport: 'ssh', target: 'ssh://example', allowedRoots: ['/home/test'], remoteStateDir: '/home/test/bridge', direct: { projects: [{ id: 'wsl-project', path: '/home/test' }] } }] };
  const hosts = configuredHosts(config);
  assert.equal(hosts[0].direct.projects[0].id, 'mac-project'); assert.equal(hosts[1].direct.projects[0].id, 'wsl-project');
  assert.throws(() => configuredHosts({ ...config, direct: { projects: [{ id: 'bad', path: '/private/tmp', write: 'yes' }] } }), /permission/);
});
