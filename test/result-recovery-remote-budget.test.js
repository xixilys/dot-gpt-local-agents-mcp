import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { createRemoteHostAdapter } from '../src/host-transport.js';
import { createSshRunner, validateSshTarget } from '../src/ssh-bridge.mjs';

const host = { id: 'fixture-remote', transport: 'ssh', target: 'ssh://fixture-host', allowedRoots: ['/fixture'], remoteStateDir: '/tmp/fixture-state', nodeCommand: 'node' };
async function fixture(t, extra) {
  const stateDir = await mkdtemp(join(tmpdir(), 'result-budget-remote-'));
  const adapter = createRemoteHostAdapter(host, { stateDir, ...extra });
  t.after(async () => { await adapter.close(); await rm(stateDir, { recursive: true, force: true }); });
  return adapter;
}

test('remote timeline identity and native read share one total deadline and caller cancellation', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1000 });
  const calls = [], controller = new AbortController();
  const adapter = await fixture(t, { nativeReader: async (script, args, transport, timeoutMs, options) => {
    calls.push({ args, timeoutMs, signal: options.signal });
    if (args.action === 'status') { t.mock.timers.tick(40); return { available: true, serverId: 'fixture-server' }; }
    return { entries: [] };
  } });
  await adapter.readTimeline({ agentId: 'fixture-agent', limit: 10, direction: 'tail' }, { timeoutMs: 100, signal: controller.signal });
  assert.equal(calls[0].timeoutMs, 100); assert.equal(calls[1].timeoutMs, 60); assert.equal(calls[1].args.timeoutMs, 60);
  controller.abort(); assert.equal(calls[1].signal.aborted, true);
});

test('remote path identity consumes the supplied deadline before launching its SSH path query', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1000 });
  const queries = [], controller = new AbortController();
  const adapter = await fixture(t, {
    nativeReader: async () => { t.mock.timers.tick(40); return { available: true, serverId: 'fixture-server' }; },
    runRemote: async (script, input, options) => { queries.push(options); return { resolved: '/fixture' }; },
  });
  await adapter.paths.check('/fixture', { timeoutMs: 100, signal: controller.signal });
  assert.equal(queries[0].timeoutMs, 60); assert.equal(queries[0].signal, controller.signal);
});

test('budget exhausted by remote identity launches no later native page or SSH query', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 1000 });
  let pages = 0, queries = 0;
  const adapter = await fixture(t, {
    nativeReader: async (_script, args) => {
      if (args.action === 'status') { t.mock.timers.tick(101); return { available: true, serverId: 'fixture-server' }; }
      pages++; return {};
    }, runRemote: async () => { queries++; return { resolved: '/fixture' }; },
  });
  await assert.rejects(adapter.readTimeline({ agentId: 'fixture-agent', limit: 10, direction: 'tail' }, { timeoutMs: 100 }), /budget exhausted/);
  await assert.rejects(adapter.paths.check('/fixture', { timeoutMs: 100 }), /could not be verified/);
  assert.equal(pages, 0); assert.equal(queries, 0);
});

test('canceling an owned SSH path query terminates that child and does not start another', async t => {
  const children = [];
  const runner = createSshRunner(validateSshTarget('ssh://fixture-host'), { spawnImpl() {
    const child = new EventEmitter();
    Object.assign(child, { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kills: [] });
    child.kill = signal => { child.kills.push(signal); queueMicrotask(() => child.emit('close', 1)); return true; };
    children.push(child); return child;
  } });
  t.after(() => runner.close());
  const controller = new AbortController();
  const pending = runner.run('process.stdout.write("{}");', {}, { timeoutMs: 500, signal: controller.signal });
  controller.abort(); await assert.rejects(pending, /remote_read_canceled/);
  await new Promise(setImmediate);
  assert.equal(children.length, 1); assert.deepEqual(children[0].kills, ['SIGTERM']);
});
