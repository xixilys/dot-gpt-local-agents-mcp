import { tmpdir } from 'node:os';
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setImmediate as turn } from 'node:timers/promises';
import { Collaboration, ATTENTION_EVENT } from '../src/collaboration.js';
import { CollaborationStore } from '../src/collaboration-store.js';
import { DispatchStore } from '../src/dispatch-store.js';
import { EventStore } from '../src/event-store.js';
import { EventDelivery } from '../src/event-delivery.js';
import { Gateway } from '../src/gateway.js';
import { createAgentChannel, readAgentCapability } from '../src/agent-channel.js';
import { requestMarker } from '../src/wait-result.js';

const knownAgent = '11111111-1111-4111-8111-111111111111';
const recoveredAgent = '22222222-2222-4222-8222-222222222222';
const owner = 'local-owner:recovery';
const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }

async function fixture(t, { online = true, unknown = false, running = true } = {}) {
  const dir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'cr-')), cwd = join(dir, 'workspace'), stateDir = join(dir, 'state');
  await mkdir(cwd);
  const eventStore = new EventStore(stateDir), receipts = new DispatchStore(stateDir);
  const saved = new CollaborationStore(stateDir);
  const since = new Date(Date.now() - 2000).toISOString();
  eventStore.saveSubscription({ id: 'sub_original', owner, name: ATTENTION_EVENT,
    arguments: { hostId: 'wsl', workspaceId: 'wks_recovery', routeId: 'route_original' },
    url: 'https://receiver.example/callback', secret: 'whsec_' + Buffer.alloc(32, 1).toString('base64'), expiresAt: Date.now() + 3600000 }, Date.now());
  function seed(requestId, agentId) {
    saved.begin({ requestId, owner, workspaceId: 'wks_recovery', cwd, routeId: 'route_original', subscriptionId: 'sub_original', agentId, submittedAt: since });
    saved.submitted(requestId, agentId ? 'submitted' : 'unknown', agentId);
    receipts.begin(requestId, 'fixture-fingerprint-' + requestId, 'create_agent', agentId);
    receipts.finish(requestId, agentId ? 'submitted' : 'unknown', undefined, agentId);
  }
  if (running) seed('original-running', knownAgent);
  if (unknown) seed('original-unknown', undefined);
  // The real local capability predates this Collaboration instance, as on a restart.
  const local = createAgentChannel({ stateDir, verifyAgent: async () => true, onMessage: async () => ({}) });
  await local.issue(knownAgent);
  const originalKey = await readAgentCapability(stateDir, knownAgent);
  saved.close();
  const store = new CollaborationStore(stateDir);
  const f = { dir, cwd, stateDir, online, store, eventStore, receipts, originalKey,
    calls: [], watches: [], prepares: 0, issues: [], bridge: false, remoteKeys: new Map([[knownAgent, originalKey]]), observerStarts: 0, observerClosed: false };
  const upstream = { async tools() { return tools; }, async call(name, args) {
    f.calls.push({ name, args });
    if (!f.online) throw Error('remote offline');
    if (name === 'list_agents') return { structuredContent: { agents: [{ id: recoveredAgent, cwd, labels: { dotRequestId: 'original-unknown' }, createdAt: new Date(Date.now() - 1000).toISOString() }] } };
    if (name === 'get_agent_status') return { structuredContent: { snapshot: { id: args.agentId, cwd, workspaceId: 'wks_recovery', status: 'running', activeTurn: { turnId: 'original-turn' }, pendingPermissions: [] } } };
    throw Error('Unexpected upstream operation: ' + name);
  } };
  const gateway = new Gateway({ upstream, store: receipts, config: { allowedRoots: [cwd] } });
  gateway.readTimeline = async ({ agentId }) => {
    if (!f.online) throw Error('remote offline');
    const requestId = agentId === knownAgent ? 'original-running' : 'original-unknown';
    return { agentId, epoch: 'original-epoch', entries: [{ item: { type: 'user_message', text: requestMarker(requestId) + 'Original task.' },
      turnId: 'original-turn', seqStart: 1, seqEnd: 1, timestamp: new Date(Date.now() - 1000).toISOString() }],
      startCursor: { epoch: 'original-epoch', seq: 1 }, endCursor: { epoch: 'original-epoch', seq: 1 },
      hasOlder: false, hasNewer: false, gap: false, staleCursor: false, reset: false };
  };
  const channel = {
    async prepare() {
      // The actual RemoteChannel reuses its listening Promise while the bridge lives.
      if (f.bridge) return;
      f.prepares++;
      if (f.prepareGate) await f.prepareGate.promise;
      if (!f.online) throw Error('reverse bridge offline');
      f.bridge = true;
    },
    async issue(agentId) {
      assert.equal(f.bridge, true, 'capability must only be restored after bridge preparation');
      f.issues.push(agentId);
      if (f.issueGate) await f.issueGate.promise;
      await local.issue(agentId);
      const key = await readAgentCapability(stateDir, agentId);
      if (f.remoteKeys.has(agentId)) assert.equal(key, f.remoteKeys.get(agentId), 'restart must reuse the original remote capability');
      f.remoteKeys.set(agentId, key);
    },
  };
  const observer = { watchAgentIds(ids) { f.watches.push(ids); }, start() { f.observerStarts++; }, async close() { f.observerClosed = true; } };
  const delivery = new EventDelivery({ store: eventStore, callbackClient: { async post() { throw Error('No callback expected in recovery test'); } } });
  const collaboration = new Collaboration({ store, delivery, eventStore, gateway, channel, observer, hostId: 'wsl' });
  gateway.collaboration = collaboration;
  Object.assign(f, { collaboration, channel, gateway });
  await gateway.refreshTools();
  t.after(async () => {
    f.prepareGate?.resolve(); f.issueGate?.resolve();
    await collaboration.messageRecovery?.catch(() => {});
    if (!collaboration.stopping) await collaboration.close();
    await local.close(); await delivery.stop(); eventStore.close(); receipts.close();
    await rm(dir, { recursive: true, force: true });
  });
  return f;
}
async function drain(f) {
  await turn();
  await f.collaboration.messageRecovery?.catch(() => {});
  await turn();
  await Promise.all([...f.collaboration.agentLocks.values()].map(p => p.catch(() => {})));
}
function assertNoDispatch(f) {
  assert.equal(f.calls.some(c => ['create_agent', 'send_agent_prompt', 'cancel_agent'].includes(c.name)), false);
}

test('startup offline unknown receipt still installs the 30s recovery timer and initializes active watches; reconnect recovers the same receipt read-only', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const f = await fixture(t, { online: false, unknown: true });
  await f.collaboration.start();
  const timer = f.collaboration.recoveryTimer;
  assert.ok(timer); assert.equal(f.collaboration.recovering, false);
  assert.ok(f.watches.some(ids => ids.includes(knownAgent)));
  assert.ok(f.observerStarts > 0);
  assert.equal(f.store.get('original-unknown').agentId, null);
  const firstReadCount = f.calls.filter(c => c.name === 'list_agents').length;
  f.online = true;
  t.mock.timers.tick(29999); await drain(f);
  assert.equal(f.calls.filter(c => c.name === 'list_agents').length, firstReadCount);
  t.mock.timers.tick(1); await drain(f);
  assert.equal(f.collaboration.recoveryTimer, timer);
  assert.equal(f.store.get('original-unknown').agentId, recoveredAgent);
  assert.equal(f.receipts.get('original-unknown').agentId, recoveredAgent);
  assert.equal(f.receipts.get('original-unknown').state, 'unknown');
  assert.equal(f.store.get('original-unknown').routeId, 'route_original');
  assertNoDispatch(f);
  t.mock.timers.tick(30000); await drain(f);
  assertNoDispatch(f);
  assert.equal(f.issues.filter(id => id === recoveredAgent).length, 1, 'an unknown receipt recovery must not reissue its already restored capability on the next timer');
});

test('restart restores the original routed running capability; an isolated bridge drop recovers on the existing timer without reissuing or dispatch', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const f = await fixture(t);
  await f.collaboration.start();
  const timer = f.collaboration.recoveryTimer;
  assert.equal(f.bridge, true); assert.deepEqual(f.issues, [knownAgent]);
  assert.equal(await readAgentCapability(f.stateDir, knownAgent), f.originalKey);
  assert.equal(f.remoteKeys.get(knownAgent), f.originalKey);
  f.bridge = false; const prepares = f.prepares;
  t.mock.timers.tick(30000); await drain(f);
  assert.equal(f.bridge, true); assert.equal(f.prepares, prepares + 1);
  assert.deepEqual(f.issues, [knownAgent]); assert.equal(f.collaboration.recoveryTimer, timer);
  assert.equal(f.store.get('original-running').requestId, 'original-running');
  assertNoDispatch(f);
  await f.collaboration.close();
  const afterClose = f.prepares;
  t.mock.timers.tick(60000); await turn();
  assert.equal(f.prepares, afterClose); assert.equal(f.observerClosed, true);
});

test('overlapping timer/ready recovery calls share one prepare and issue flight for each existing agent', async t => {
  const f = await fixture(t);
  f.prepareGate = deferred(); f.issueGate = deferred();
  const attempts = Array.from({ length: 8 }, () => f.collaboration.recoverMessageChannels());
  await turn();
  assert.equal(f.prepares, 1); assert.equal(f.issues.length, 0);
  f.prepareGate.resolve(); await turn();
  assert.deepEqual(f.issues, [knownAgent]);
  attempts.push(f.collaboration.recoverMessageChannels());
  assert.equal(f.prepares, 1);
  f.issueGate.resolve(); await Promise.all(attempts);
  await f.collaboration.recoverMessageChannels();
  assert.deepEqual(f.issues, [knownAgent]);
  assert.equal(f.remoteKeys.get(knownAgent), f.originalKey); assertNoDispatch(f);
});

test('close while bridge preparation is in flight prevents subsequent capability issue', async t => {
  const f = await fixture(t);
  f.prepareGate = deferred();
  const pending = f.collaboration.recoverMessageChannels();
  await turn(); assert.equal(f.prepares, 1);
  await f.collaboration.close();
  f.prepareGate.resolve(); await pending;
  assert.equal(f.issues.length, 0); assert.equal(f.observerClosed, true);
  await f.collaboration.recoverMessageChannels();
  assert.equal(f.prepares, 1); assertNoDispatch(f);
});

test('dispatch recovery and timer recovery do not concurrently issue the same agent capability', async t => {
  const f = await fixture(t);
  f.bridge = true; f.issueGate = deferred();
  const finished = f.collaboration.finishDispatch(f.receipts.get('original-running'));
  await turn();
  assert.deepEqual(f.issues, [knownAgent]);
  const recovered = f.collaboration.recoverMessageChannels();
  await turn();
  // Resolve both flights before asserting so a failure still drains pending work.
  const issueCount = f.issues.length;
  f.issueGate.resolve(); await Promise.all([finished, recovered]); await drain(f);
  assert.equal(issueCount, 1, 'finishDispatch and timer recovery must share same-agent capability initialization');
  assert.equal(f.remoteKeys.get(knownAgent), f.originalKey); assertNoDispatch(f);
});
