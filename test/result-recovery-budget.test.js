import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Collaboration } from '../src/collaboration.js';
import { ObserverClient } from '../src/observer-client.js';
import { Gateway } from '../src/gateway.js';
import { PaseoUpstream } from '../src/upstream.js';
import { requestMarker } from '../src/wait-result.js';

const agentId = '11111111-1111-4111-8111-111111111111', owner = 'fixture-owner';
const epoch = 'fixture-epoch', submittedAt = '2026-01-01T00:00:00Z';
const request = (requestId, extra = {}) => ({ requestId, owner, agentId, cwd: '/fixture', workspaceId: 'fixture-workspace',
  submittedAt, subscriptionId: 'fixture-sub', state: 'submitted', ...extra });
function fixture(records, { timeline, now, resultReadTimeoutMs } = {}) {
  const saved = [], marked = [], checks = [];
  const snapshot = { id: agentId, cwd: '/fixture', workspaceId: 'fixture-workspace', status: 'idle', activeTurn: null, pendingPermissions: [] };
  const store = {
    get: id => records.find(r => r.requestId === id), forAgent: id => records.filter(r => r.agentId === id), routedRequests: () => records,
    markTurn(id, value) { marked.push({ id, ...value }); const r = this.get(id); if (r.turnId && r.turnId !== value.turnId) return false; Object.assign(r, value); return true; },
    saveResult(id, result, terminalKind, terminalAt) { const r = this.get(id); Object.assign(r, { result, terminalKind, terminalAt }); saved.push(id); return r; },
    pendingMessages: () => [], hasDecision: () => true, sourceSeen: () => true,
  };
  const gateway = { paths: { check: async value => value },
    checkAgent: async (id, timeoutMs, options) => { checks.push({ timeoutMs, options }); return { structuredContent: { snapshot: { ...snapshot, id } } }; },
    timeline: timeline ?? (async () => ({ entries: [], epoch, hasOlder: false })),
  };
  const observer = { watchAgentIds() {}, start() {} };
  const collaboration = new Collaboration({ store, gateway, observer,
    eventStore: { activeSubscriptions: () => [{ id: 'fixture-sub' }] }, delivery: {},
    ...(now ? { now } : {}), ...(resultReadTimeoutMs ? { resultReadTimeoutMs } : {}),
  });
  return { collaboration, gateway, store, saved, marked, snapshot, checks };
}
function entry(type, text, seq, turnId = 'turn-target') {
  return { item: { type, text }, seqStart: seq, seqEnd: seq, timestamp: submittedAt, turnId };
}
function page(entries, extra = {}) {
  return { agentId, epoch, entries, hasOlder: false, hasNewer: false,
    startCursor: { epoch, seq: entries[0]?.seqStart ?? 0 }, endCursor: { epoch, seq: entries.at(-1)?.seqEnd ?? 0 }, ...extra };
}
const flush = () => new Promise(setImmediate);

test('unchanged/reordered watches do not ACK-loop', async t => {
  const children = [];
  const client = new ObserverClient({ startupTimeoutMs: 30000, spawnImpl() {
    const child = new EventEmitter(); Object.assign(child, { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), written: '' });
    child.stdin.on('data', data => { child.written += data; }); child.kill = () => true; children.push(child); return child;
  } });
  t.after(() => client.close());
  const second = '22222222-2222-4222-8222-222222222222';
  client.watchAgentIds([agentId, second]).start();
  children[0].stdout.write(JSON.stringify({ type: 'connection-status', status: 'connected' }) + '\n');
  const first = children[0].written;
  client.watchAgentIds([second, agentId]); client.watchAgentIds([agentId, second]);
  assert.equal(children[0].written, first);
  assert.equal(first.trim().split('\n').length, 1);
});

test('exact request bypasses a blocked unrelated background record and same-agent duplicate scans coalesce', async () => {
  let release, calls = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const old = request('old', { submittedAt: '2026-01-02T00:00:00Z' }), target = request('target');
  const f = fixture([old, target], { timeline: async () => { calls++; if (calls === 1) await gate; return page([]); } });
  const background = f.collaboration.reconcile(agentId);
  await flush();
  const duplicates = Array.from({ length: 20 }, () => f.collaboration.reconcile(agentId));
  const foreground = await f.collaboration.getRequestResult('target', owner);
  assert.equal(foreground.reason, 'request_marker_not_found');
  assert.equal(calls, 2); // first scan remains blocked, foreground ran independently
  release(); await Promise.all([background, ...duplicates]);
  assert.equal(calls, 5); // 20 concurrent hints coalesced into one trailing pass
  assert.equal(f.collaboration.backgroundReads.size, 0);
  assert.equal(f.saved.length, 0);
});

test('same-request concurrent read returns pending immediately and never leaves a queued later scan', async () => {
  let release, calls = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const f = fixture([request('target')], { timeline: async () => { calls++; await gate; return page([]); } });
  const first = f.collaboration.getRequestResult('target', owner);
  await flush();
  const busy = await f.collaboration.getRequestResult('target', owner);
  assert.equal(busy.status, 'pending'); assert.equal(busy.reason, 'result_read_in_progress'); assert.equal(busy.retrySafe, true);
  assert.equal(calls, 1);
  release(); await first; await flush();
  assert.equal(calls, 1); assert.equal(f.collaboration.resultReads.size, 0);
});

test('watching an existing historical request recovers only that request without a changed-watch ACK', async () => {
  const target = request('target'), old = request('unrelated-old');
  const rows = [entry('user_message', requestMarker('target') + 'work', 1), entry('assistant_message', 'historical result', 2)];
  const f = fixture([old, target], { timeline: async args => page(args.limit === 1 ? rows.slice(-1) : rows) });
  f.gateway.upstream = { call: async () => ({ structuredContent: { workspaces: [{ workspaceId: 'fixture-workspace', cwd: '/fixture' }] } }) };
  f.store.bindRoute = (id, routeId, subscriptionId) => Object.assign(f.store.get(id), { routeId, subscriptionId });
  f.collaboration.eventStore.activeSubscriptions = () => [{ id: 'fixture-sub', owner,
    arguments: { workspaceId: 'fixture-workspace', routeId: 'fixture-route' } }];
  // This observer never emits ready. An unchanged watch must not be needed to
  // start the exact-request recovery performed by watchDispatchRequest itself.
  f.collaboration.refreshWatches();
  const recoveries = [], reconcile = f.collaboration.reconcile.bind(f.collaboration);
  f.collaboration.reconcile = (...args) => { const promise = reconcile(...args); recoveries.push({ args, promise }); return promise; };
  const receipt = await f.collaboration.watchDispatchRequest({ requestId: 'target', notificationRouteId: 'fixture-route' }, owner);
  await Promise.all(recoveries.map(r => r.promise));
  assert.equal(receipt.status, 'bound'); assert.equal(recoveries.length, 1);
  assert.equal(recoveries[0].args[2], 'target');
  assert.deepEqual(f.saved, ['target']); assert.equal(f.store.get('unrelated-old').result, undefined);
  assert.equal(f.store.get('target').result.lastMessage, 'historical result');
});

test('large pages shrink at the same cursor, then recover exactly the marked turn', async () => {
  const rows = [entry('user_message', requestMarker('target') + 'work', 1), entry('assistant_message', 'actual result', 2)];
  const calls = [];
  const f = fixture([request('target')], { timeline: async args => {
    calls.push(args);
    if (args.limit === 200) throw Error('Output limit');
    return page(args.limit === 1 ? rows.slice(-1) : rows);
  } });
  const result = await f.collaboration.getRequestResult('target', owner);
  assert.equal(result.savedResult, true); assert.equal(result.lastMessage, 'actual result');
  assert.deepEqual(calls.map(c => c.limit), [200, 100, 1]);
  assert.equal(calls[0].direction, calls[1].direction); assert.equal(calls[1].cursor, undefined);
  assert.equal(result.acceptancePassed, null); assert.deepEqual(f.saved, ['target']);
});

test('budget includes preflight and identity checks; exhaustion starts no timeline read', async () => {
  let clock = 0, reads = 0;
  const f = fixture([request('target')], { now: () => clock, resultReadTimeoutMs: 10, timeline: async () => { reads++; return page([]); } });
  f.gateway.paths.check = async p => { clock += 6; return p; };
  const check = f.gateway.checkAgent;
  f.gateway.checkAgent = async (...args) => { clock += 5; return check(...args); };
  const result = await f.collaboration.getRequestResult('target', owner);
  assert.equal(result.reason, 'call_budget_exhausted'); assert.equal(result.retrySafe, true);
  assert.equal(f.checks[0].timeoutMs, 4); assert.equal(reads, 0); assert.equal(f.saved.length, 0);
});

test('one deadline bounds all serial pages and prevents post-budget paging or storage', async () => {
  let clock = 0;
  const budgets = [];
  const f = fixture([request('target')], { now: () => clock, resultReadTimeoutMs: 10, timeline: async args => {
    budgets.push(args.timeoutMs); clock += 4;
    return page([], { hasOlder: true, startCursor: { epoch, seq: 100 - budgets.length } });
  } });
  const result = await f.collaboration.getRequestResult('target', owner);
  assert.equal(result.reason, 'call_budget_exhausted'); assert.deepEqual(budgets, [9, 5, 1]);
  assert.equal(f.saved.length, 0); assert.equal(f.marked.length, 0);
});

test('deadline aborts an in-flight native read and its late resolution cannot launch a ghost page', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1000 });
  let release, signal, calls = 0;
  const f = fixture([request('target')], { timeline: async args => {
    calls++; signal = args.signal;
    return new Promise(resolve => { release = resolve; });
  } });
  const pending = f.collaboration.getRequestResult('target', owner);
  await flush();
  t.mock.timers.tick(10000);
  const result = await pending;
  assert.equal(result.reason, 'call_budget_exhausted'); assert.equal(signal.aborted, true);
  release(page([], { hasOlder: true, startCursor: { epoch, seq: 1 } })); await flush();
  assert.equal(calls, 1); assert.equal(f.saved.length, 0); assert.equal(f.collaboration.resultReads.size, 0);
});

test('a background handoff waiting for an exact reader expires without a queued read after release', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1000 });
  let release, calls = 0;
  const f = fixture([request('target', { turnId: 'original-turn', markerSeq: 4, epoch })], {
    resultReadTimeoutMs: 100, timeline: async () => { calls++; return new Promise(resolve => { release = resolve; }); },
  });
  const exact = f.collaboration.getRequestResult('target', owner); await flush();
  const terminal = f.collaboration.reconcile(agentId, { type: 'turn_completed', turnId: 'original-turn', timestamp: submittedAt });
  await flush(); t.mock.timers.tick(100); await Promise.all([exact, terminal]);
  release(page([])); await flush();
  assert.equal(calls, 1); assert.equal(f.saved.length, 0);
  assert.equal(f.collaboration.resultReads.size, 0); assert.equal(f.collaboration.backgroundReads.size, 0);
});

test('native end arriving during a busy scan remains attached to its original request', async () => {
  let release, calls = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const f = fixture([request('target', { turnId: 'original-turn', markerSeq: 4, epoch })], {
    timeline: async () => { calls++; if (calls === 1) await gate; throw Error('Output limit'); },
  });
  const first = f.collaboration.reconcile(agentId); await flush();
  const event = f.collaboration.reconcile(agentId, { type: 'turn_completed', turnId: 'original-turn', timestamp: submittedAt });
  release(); await Promise.all([first, event]);
  const result = await f.collaboration.getRequestResult('target', owner);
  assert.equal(result.savedResult, true); assert.equal(result.resultReadStatus, 'needs_pagination');
  assert.equal(result.turnId, 'original-turn'); assert.equal(result.terminalKind, 'turn_completed');
  assert.equal(result.lastMessage, null); assert.equal(result.acceptancePassed, null);
  assert.equal(f.saved.length, 1);
});

test('a terminal arriving in an exhausted background pass receives one fresh-budget recovery', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1000 });
  let calls = 0;
  const f = fixture([request('target', { turnId: 'original-turn', markerSeq: 4, epoch })], {
    resultReadTimeoutMs: 100,
    timeline: async () => { calls++; if (calls === 1) return new Promise(() => {}); throw Error('Output limit'); },
  });
  const first = f.collaboration.reconcile(agentId); await flush();
  const event = f.collaboration.reconcile(agentId, { type: 'turn_completed', turnId: 'original-turn', timestamp: submittedAt });
  t.mock.timers.tick(100); await Promise.all([first, event]);
  assert.equal(f.saved.length, 1); assert.equal(f.store.get('target').result.resultReadStatus, 'needs_pagination');
  assert.equal(f.store.get('target').terminalKind, 'turn_completed');
  const finishedCalls = calls; t.mock.timers.tick(1000); await flush();
  assert.equal(calls, finishedCalls); assert.equal(f.collaboration.backgroundReads.size, 0);
});

test('repeated native failures stop after one trailing pass instead of automatically retrying forever', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1000 });
  let calls = 0;
  const f = fixture([request('target', { turnId: 'original-turn', markerSeq: 4, epoch })], {
    resultReadTimeoutMs: 100, timeline: async () => { calls++; return new Promise(() => {}); },
  });
  const first = f.collaboration.reconcile(agentId); await flush();
  const event = f.collaboration.reconcile(agentId, { type: 'turn_completed', turnId: 'original-turn', timestamp: submittedAt });
  t.mock.timers.tick(100); await flush();
  assert.equal(calls, 2);
  t.mock.timers.tick(100); await Promise.all([first, event]);
  t.mock.timers.tick(1000); await flush();
  assert.equal(calls, 2); assert.equal(f.saved.length, 0); assert.equal(f.collaboration.nativeEnds.size, 1);
  assert.equal(f.collaboration.backgroundReads.size, 0);
});

test('a slow native page leaves finalization time to save a known native end as a partial', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1000 });
  let calls = 0;
  const f = fixture([request('target', { turnId: 'original-turn', markerSeq: 4, epoch })], {
    resultReadTimeoutMs: 100, timeline: async args => {
      calls++; return new Promise((_, reject) => setTimeout(() => reject(Error('native deadline')), args.timeoutMs));
    },
  });
  const ended = f.collaboration.reconcile(agentId, { type: 'turn_completed', turnId: 'original-turn', timestamp: submittedAt });
  await flush(); t.mock.timers.tick(90); await ended;
  assert.equal(calls, 1); assert.equal(f.saved.length, 1);
  assert.equal(f.store.get('target').result.resultReadStatus, 'needs_pagination');
  assert.equal(f.store.get('target').terminalKind, 'turn_completed');
});

test('single oversized entry stays bounded and a combined oversized result is never stored', async () => {
  const rows = [entry('user_message', requestMarker('target') + 'work', 1), entry('assistant_message', 'x'.repeat(1024 * 1024), 2)];
  const f = fixture([request('target')], { timeline: async () => page(rows) });
  const result = await f.collaboration.getRequestResult('target', owner);
  assert.equal(result.reason, 'result_output_limit'); assert.equal(result.resultReadStatus, 'needs_pagination');
  assert.equal(f.saved.length, 0); assert.equal(JSON.stringify(result).includes('xxxx'), false);
});

test('result recovery preserves request owner and agent workspace checks', async () => {
  let reads = 0;
  const f = fixture([request('target')], { timeline: async () => { reads++; return page([]); } });
  await assert.rejects(f.collaboration.getRequestResult('target', 'other-owner'), /another authenticated/);
  f.snapshot.workspaceId = 'other-workspace';
  await assert.rejects(f.collaboration.getRequestResult('target', owner), /persisted workspace/);
  assert.equal(reads, 0);
});

test('Gateway propagates remaining native deadline and cancellation to its owned child', async () => {
  const calls = [], paths = [];
  const gateway = new Gateway({ upstream: {}, store: {}, config: { allowedRoots: ['/fixture'] },
    async runCli(command, args, options) { calls.push({ command, args, options }); return { stdout: JSON.stringify(page([], { agent: { cwd: '/fixture' } })) }; },
  });
  gateway.paths = { async check(p, options) { paths.push(options); return p; } };
  const controller = new AbortController();
  await gateway.timeline({ agentId, limit: 10, timeoutMs: 500, signal: controller.signal });
  assert.ok(calls[0].options.timeout > 0 && calls[0].options.timeout <= 500);
  assert.equal(calls[0].options.signal, controller.signal); assert.equal(calls[0].options.killSignal, 'SIGKILL');
  assert.ok(JSON.parse(calls[0].args[1]).timeoutMs <= 500); assert.ok(paths[0].timeoutMs <= 500);
});

test('upstream combines caller cancellation with the bounded HTTP timeout', async () => {
  const controller = new AbortController(); let signal;
  const upstream = new PaseoUpstream(undefined, { fetchImpl: async (_url, options) => {
    signal = options.signal; controller.abort(); signal.throwIfAborted();
  } });
  await assert.rejects(upstream.call('get_agent_status', { agentId }, { timeoutMs: 500, signal: controller.signal }));
  assert.equal(signal.aborted, true);
});

test('canceling an actual owned child terminates it before any later reconciliation step', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'result-read-child-')), pidPath = join(dir, 'pid');
  t.after(() => rm(dir, { recursive: true, force: true }));
  const controller = new AbortController();
  const run = promisify(execFile);
  const child = run(process.execPath, ['--input-type=module', '-e',
    'import { writeFileSync } from "node:fs"; writeFileSync(process.argv[1], String(process.pid)); setInterval(() => {}, 1000);', pidPath],
  { timeout: 5000, killSignal: 'SIGKILL', signal: controller.signal });
  // Observe a local readiness file, never process-name matching or production PIDs.
  let pid;
  for (let tries = 0; tries < 100; tries++) {
    try { pid = Number(await readFile(pidPath, 'utf8')); break; }
    catch { await new Promise(resolve => setTimeout(resolve, 5)); }
  }
  assert.ok(Number.isSafeInteger(pid) && pid > 0);
  controller.abort(); await assert.rejects(child, error => error.name === 'AbortError');
  let gone = false;
  for (let tries = 0; tries < 100; tries++) {
    try { process.kill(pid, 0); } catch (error) { if (error.code === 'ESRCH') { gone = true; break; } throw error; }
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  assert.equal(gone, true);
});
