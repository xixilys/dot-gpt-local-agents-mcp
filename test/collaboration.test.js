import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID, randomBytes } from 'node:crypto';
import { EventStore } from '../src/event-store.js';
import { EventDelivery } from '../src/event-delivery.js';
import { CollaborationStore } from '../src/collaboration-store.js';
import { Collaboration, ATTENTION_EVENT } from '../src/collaboration.js';
import { Gateway } from '../src/gateway.js';
import { DispatchStore } from '../src/dispatch-store.js';
import { requestMarker } from '../src/wait-result.js';

const agentId = '11111111-1111-4111-8111-111111111111';
const owner = 'local-owner:test-client';
const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'local-agents-collaboration-'));
  const cwd = join(dir, 'workspace'); await mkdir(cwd);
  const state = join(dir, 'state');
  const eventStore = new EventStore(state), store = new CollaborationStore(state), receipts = new DispatchStore(state);
  const callbacks = [], mutations = [], issued = [], watches = [];
  const snapshot = { id: agentId, cwd, workspaceId: 'wks_test', status: 'idle', activeTurn: null, pendingPermissions: [] };
  let entries = [], sequence = 0;
  const epoch = 'test-epoch';
  const delivery = new EventDelivery({ store: eventStore, callbackClient: {
    async post(url, request) {
      const body = JSON.parse(request.body);
      if (body.type === 'verification') return { status: 200, body: JSON.stringify({ challenge: body.challenge }) };
      callbacks.push({ url, body }); return { status: 202, body: '{}' };
    },
  } });
  const upstream = {
    async tools() { return tools; },
    async call(name, args) {
      if (name === 'list_workspaces') return { structuredContent: { workspaces: [{ workspaceId: 'wks_test', cwd }] } };
      if (name === 'get_agent_status') return { structuredContent: { snapshot: { ...snapshot } } };
      if (name === 'list_agents') return { structuredContent: { agents: [] } };
      mutations.push({ name, args });
      snapshot.status = 'running'; snapshot.activeTurn = { turnId: `turn-${mutations.length}` };
      entries.push({ item: { type: 'user_message', text: args.initialPrompt ?? args.prompt }, turnId: snapshot.activeTurn.turnId,
        seqStart: ++sequence, seqEnd: sequence, timestamp: new Date(Date.now() + 2).toISOString() });
      return { structuredContent: { agentId } };
    },
  };
  const gateway = new Gateway({ upstream, store: receipts, config: { allowedRoots: [cwd] } });
  const collaboration = new Collaboration({ store, delivery, eventStore, gateway,
    channel: { async issue(id) { issued.push(id); } },
    observer: { watchAgentIds(ids) { watches.push(ids); }, start() {}, async close() {} } });
  gateway.collaboration = collaboration;
  gateway.timeline = async ({ limit }) => ({ agentId, agent: { ...snapshot }, entries: limit === 1 ? entries.slice(-1) : [...entries],
    epoch, endCursor: { epoch, seq: sequence }, startCursor: { epoch, seq: entries[0]?.seqStart ?? 0 },
    hasOlder: false, hasNewer: false, gap: false, staleCursor: false, reset: false });
  await gateway.refreshTools();
  t.after(async () => { await delivery.stop(); await collaboration.close(); eventStore.close(); receipts.close(); await rm(dir, { recursive: true, force: true }); });
  const subscribe = (routeId, url = `https://${routeId}.example/callback`, grant = owner) => collaboration.subscribe(grant, {
    name: ATTENTION_EVENT, arguments: { workspaceId: 'wks_test', routeId },
    delivery: { mode: 'webhook', url, secret: 'whsec_' + randomBytes(32).toString('base64') },
  });
  const create = (requestId, routeId) => gateway.call('create_agent', {
    requestId, notificationRouteId: routeId, workspaceId: 'wks_test', provider: 'codex/test', title: 'test', initialPrompt: 'Do the bounded task.',
  }, { owner });
  function finish(requestId, text = 'Actual result') {
    const r = entries.find(e => e.item.type === 'user_message' && e.item.text.startsWith(requestMarker(requestId)));
    entries.push({ item: { type: 'assistant_message', text }, turnId: r.turnId,
      seqStart: ++sequence, seqEnd: sequence, timestamp: new Date().toISOString() });
    snapshot.status = 'idle'; snapshot.activeTurn = null;
    return { type: 'turn_completed', turnId: r.turnId, timestamp: new Date().toISOString() };
  }
  return { dir, cwd, store, eventStore, receipts, delivery, collaboration, gateway, upstream, snapshot, callbacks, mutations, watches,
    subscribe, create, finish, setEntries(value) { entries = value; sequence = value.at(-1)?.seqEnd ?? 0; }, getEntries() { return entries; } };
}

test('one route binds one callback, two chats require explicit dispatch routing and never receive each other results', async t => {
  const f = await fixture(t);
  await f.subscribe('dot-a'); await f.subscribe('dot-b');
  await assert.rejects(f.subscribe('dot-a', 'https://third.example/callback'), /another callback/);
  await assert.rejects(f.gateway.call('create_agent', { requestId: 'ambiguous', workspaceId: 'wks_test', provider: 'codex/test', title: 'test', initialPrompt: 'task' }, { owner }), /More than one/);
  assert.equal(f.mutations.length, 0);
  await f.create('task-a', 'dot-a');
  await f.collaboration.reconcile(agentId, f.finish('task-a', 'A actual result'));
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].url, 'https://dot-a.example/callback');
  assert.equal(f.callbacks[0].body.data.requestId, 'task-a');
  assert.equal(f.callbacks[0].body.data.acceptancePassed, null);
  assert.equal((await f.collaboration.getRequestResult('task-a', owner)).lastMessage, 'A actual result');
  await assert.rejects(f.collaboration.getRequestResult('task-a', 'local-owner:other-client'), /another authenticated/);
});

test('needs-input is persisted while running, wakes only after yielding, and a changed retry requestId cannot reply twice', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('ask-a', 'dot-a');
  const body = { agentId, requestId: 'ask-a', messageId: randomUUID(), kind: 'needs_input', text: 'Which bounded choice should I use?' };
  assert.equal(await f.collaboration.verifyMessage(agentId, body), true);
  assert.equal((await f.collaboration.acceptMessage(body)).status, 'accepted');
  await f.delivery.flushDue(); assert.equal(f.callbacks.length, 0);
  await assert.rejects(f.gateway.call('send_agent_prompt', { agentId, requestId: 'too-early', prompt: 'choice one', replyToMessageId: body.messageId }, { owner }), /still running/);
  await f.collaboration.reconcile(agentId, f.finish('ask-a', 'Waiting for the dot decision.'));
  await f.delivery.flushDue(); assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].body.data.kind, 'needs_input');
  assert.equal(f.callbacks[0].body.data.readyForReply, true);
  const reply = { agentId, requestId: 'reply-one', prompt: 'Choose the first option.', replyToMessageId: body.messageId };
  await f.gateway.call('send_agent_prompt', reply, { owner });
  const replay = await f.gateway.call('send_agent_prompt', { ...reply, requestId: 'reply-retry-new-id' }, { owner });
  assert.equal(replay.structuredContent.requestId, 'reply-one');
  assert.equal(replay.structuredContent.replyAlreadySubmitted, true);
  assert.equal(f.mutations.length, 2);
  await assert.rejects(f.gateway.call('send_agent_prompt', { ...reply, requestId: 'conflicting-reply', prompt: 'Other choice' }, { owner }), /different reply/);
});

test('an older unsaved turn is recovered after a newer turn, and a delayed reply retains the original dot route', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.subscribe('dot-b');
  await f.create('old-a', 'dot-a');
  const body = { agentId, requestId: 'old-a', messageId: randomUUID(), kind: 'needs_input', text: 'Decision for A.' };
  await f.collaboration.acceptMessage(body);
  f.finish('old-a', 'A finished and waiting.');
  await f.gateway.call('send_agent_prompt', { agentId, requestId: 'new-b', notificationRouteId: 'dot-b', prompt: 'Independent B.' }, { owner });
  const terminalB = f.finish('new-b', 'B actual result');
  await f.collaboration.reconcile(agentId, terminalB);
  const old = await f.collaboration.getRequestResult('old-a', owner);
  assert.equal(old.lastMessage, 'A finished and waiting.');
  const reply = await f.gateway.call('send_agent_prompt', { agentId, requestId: 'late-reply-a', prompt: 'Answer A.', replyToMessageId: body.messageId }, { owner });
  assert.equal(reply.structuredContent.state, 'submitted');
  assert.equal(f.store.get('late-reply-a').routeId, 'dot-a');
});

test('a stale idle snapshot cannot freeze a currently running partial assistant chunk as a result', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('partial', 'dot-a');
  const entries = f.getEntries();
  entries.push({ item: { type: 'assistant_message', text: 'Still working, partial progress' }, turnId: 'turn-1', seqStart: 2, seqEnd: 2, timestamp: new Date().toISOString() });
  f.setEntries(entries);
  f.snapshot.status = 'running'; f.snapshot.activeTurn = { turnId: 'turn-1' };
  const old = f.gateway.checkAgent.bind(f.gateway);
  let once = true;
  f.gateway.checkAgent = async id => {
    if (once) { once = false; return { structuredContent: { snapshot: { ...f.snapshot, status: 'idle', activeTurn: null } } }; }
    return old(id);
  };
  await f.collaboration.reconcile(agentId);
  assert.equal(f.store.get('partial').result, undefined);
  assert.equal(f.eventStore.listDeliveries().length, 0);
});

test('unknown creation is located read-only by its unique label and never creates a replacement agent', async t => {
  const f = await fixture(t); await f.subscribe('dot-a');
  const original = f.upstream.call;
  f.upstream.call = async (name, args) => {
    if (name === 'create_agent') { await original(name, args); throw new Error('Lost acceptance response'); }
    if (name === 'list_agents') return { structuredContent: { agents: [{ id: agentId, cwd: f.cwd,
      createdAt: new Date(Date.now() + 2).toISOString(), labels: { dotRequestId: 'lost-create' } }] } };
    return original(name, args);
  };
  assert.equal((await f.create('lost-create', 'dot-a')).structuredContent.state, 'unknown');
  await f.collaboration.recoverUnknownCreations();
  assert.equal(f.store.get('lost-create').agentId, agentId);
  assert.equal(f.receipts.get('lost-create').agentId, agentId);
  assert.equal(f.mutations.length, 1);
});

test('known native end still emits a pagination notice for an oversized result', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('huge', 'dot-a');
  const terminal = f.finish('huge');
  f.store.markTurn('huge', { turnId: 'turn-1', markerSeq: 1, epoch: 'test-epoch' });
  f.gateway.timeline = async () => { throw new Error('Output limit'); };
  await f.collaboration.reconcile(agentId, terminal);
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].body.data.kind, 'result');
  const result = await f.collaboration.getRequestResult('huge', owner);
  assert.equal(result.resultReadStatus, 'needs_pagination');
  assert.equal(result.turnId, 'turn-1');
  assert.equal(result.acceptancePassed, null);
});

test('completed history does not consume the bounded real-time watch set', async t => {
  const f = await fixture(t); const sub = await f.subscribe('dot-a');
  for (let i = 0; i < 300; i++) {
    const id = randomUUID(), requestId = `historic-${i}`;
    f.store.begin({ requestId, owner, workspaceId: 'wks_test', cwd: f.cwd, routeId: 'dot-a', subscriptionId: sub.id,
      agentId: id, submittedAt: new Date().toISOString() });
    f.store.saveResult(requestId, { lastMessage: 'done' }, 'turn_completed', new Date().toISOString());
    f.store.claimSource(`${requestId}:result`);
  }
  await f.create('live', 'dot-a');
  f.collaboration.refreshWatches();
  assert.deepEqual(f.watches.at(-1), [agentId]);
});

test('a persisted result without its outbox publication is delivered once after recovery', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('saved-before-crash', 'dot-a');
  f.finish('saved-before-crash', 'Saved original result.');
  const exact = await f.collaboration.readMatched(f.store.get('saved-before-crash'));
  f.store.saveResult('saved-before-crash', exact, 'turn_completed', new Date().toISOString());
  await f.collaboration.reconcile(agentId);
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  await f.collaboration.reconcile(agentId); await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
});

test('a deferred A decision survives B changing the tail and is emitted exactly once when B yields', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.subscribe('dot-b');
  await f.create('deferred-a', 'dot-a');
  const body = { agentId, requestId: 'deferred-a', messageId: randomUUID(), kind: 'needs_input', text: 'A decision remains pending.' };
  await f.collaboration.acceptMessage(body);
  // Complete the background running-state read before arranging the A→B handoff.
  await f.collaboration.reconcile(agentId);
  const terminalA = f.finish('deferred-a', 'A waiting.');
  await f.gateway.call('send_agent_prompt', { agentId, requestId: 'intervening-b', notificationRouteId: 'dot-b', prompt: 'B work.' }, { owner });
  await f.collaboration.reconcile(agentId, terminalA);
  assert.ok(f.store.get('deferred-a').result);
  assert.equal(f.store.getMessage(body.messageId).emitted, false);
  const terminalB = f.finish('intervening-b', 'B result.');
  await f.collaboration.onObserverEvent({ type: 'lifecycle', eventType: terminalB.type, agentId, turnId: terminalB.turnId, timestamp: terminalB.timestamp });
  await f.delivery.flushDue();
  const messages = f.callbacks.filter(c => c.body.data.messageId === body.messageId);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].url, 'https://dot-a.example/callback');
  assert.equal(messages[0].body.data.readyForReply, true);
});

test('concurrent replies with different request IDs create one binding and one upstream follow-up', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('concurrent-ask', 'dot-a');
  const body = { agentId, requestId: 'concurrent-ask', messageId: randomUUID(), kind: 'needs_input', text: 'Choose one.' };
  await f.collaboration.acceptMessage(body);
  await f.collaboration.reconcile(agentId, f.finish('concurrent-ask', 'Waiting.'));
  const base = { agentId, prompt: 'Choice one.', replyToMessageId: body.messageId };
  const replies = await Promise.all(['concurrent-one','concurrent-two'].map(requestId => f.gateway.call('send_agent_prompt', { ...base, requestId }, { owner })));
  assert.equal(f.mutations.length, 2);
  const saved = f.store.forAgent(agentId).filter(r => r.requestId.startsWith('concurrent-') && r.requestId !== 'concurrent-ask');
  assert.equal(saved.length, 1);
  assert.equal(replies[0].structuredContent.requestId, replies[1].structuredContent.requestId);
  assert.equal(f.store.latest(agentId).requestId, saved[0].requestId);
});

test('same dispatch ID with competing routes retains the actual winning payload and route', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.subscribe('dot-b');
  const results = await Promise.allSettled([f.create('route-race','dot-a'), f.create('route-race','dot-b')]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(f.mutations.length, 1);
  assert.equal(f.store.get('route-race').routeId, 'dot-a');
  await f.collaboration.reconcile(agentId, f.finish('route-race', 'Winning actual task.'));
  await f.delivery.flushDue();
  assert.equal(f.callbacks[0].url, 'https://dot-a.example/callback');
});

test('a newer B permission is never attributed to native end of A', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.subscribe('dot-b');
  await f.create('permission-a', 'dot-a'); const terminalA = f.finish('permission-a', 'A actual result.');
  await f.gateway.call('send_agent_prompt', { agentId, requestId: 'permission-b', notificationRouteId: 'dot-b', prompt: 'B work.' }, { owner });
  f.snapshot.pendingPermissions = [{ requestId: 'B-permission' }];
  await f.collaboration.reconcile(agentId, terminalA); await f.delivery.flushDue();
  assert.equal(f.callbacks.some(c => c.body.data.requestId === 'permission-a' && c.body.data.kind === 'permission'), false);
  assert.ok(f.store.get('permission-a').result);
  await f.collaboration.reconcile(agentId); await f.delivery.flushDue();
  assert.equal(f.callbacks.filter(c => c.body.data.kind === 'permission').length, 1);
  assert.equal(f.callbacks.find(c => c.body.data.kind === 'permission').url, 'https://dot-b.example/callback');
});

test('durable needs-input receipts do not wait for a blocked reconciliation, including a duplicate', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('slow-ack', 'dot-a');
  await f.collaboration.reconcile(agentId);
  const original = f.collaboration.reconcile.bind(f.collaboration);
  let release;
  const blocked = new Promise(resolve => { release = resolve; });
  f.collaboration.reconcile = () => blocked;
  const body = { agentId, requestId: 'slow-ack', messageId: randomUUID(), kind: 'needs_input', text: 'Wait for the original dot decision.' };
  async function receipt() {
    let timer;
    try {
      return await Promise.race([f.collaboration.acceptMessage(body), new Promise(resolve => {
        timer = setTimeout(() => resolve({ status: 'blocked_on_reconciliation' }), 200);
      })]);
    } finally { clearTimeout(timer); }
  }
  try {
    assert.equal((await receipt()).status, 'accepted');
    assert.equal((await receipt()).status, 'duplicate');
    assert.equal(f.store.pendingMessages('slow-ack').length, 1);
    assert.equal(f.store.getMessage(body.messageId).text, body.text);
    await f.delivery.flushDue();
    assert.equal(f.callbacks.length, 0);
  } finally { release(); f.collaboration.reconcile = original; }
  await f.collaboration.reconcile(agentId, f.finish('slow-ack', 'Yielded for the dot.'));
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].body.data.readyForReply, true);
});

test('a failed background reconciliation retains the message until the normal idle notification recovers', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('failed-ack-read', 'dot-a');
  await f.collaboration.reconcile(agentId);
  const original = f.collaboration.reconcile.bind(f.collaboration);
  f.collaboration.reconcile = async () => { throw new Error('Temporary timeline failure'); };
  const body = { agentId, requestId: 'failed-ack-read', messageId: randomUUID(), kind: 'needs_input', text: 'Persist this decision request.' };
  assert.equal((await f.collaboration.acceptMessage(body)).status, 'accepted');
  await f.delivery.flushDue();
  assert.equal(f.store.pendingMessages('failed-ack-read').length, 1);
  assert.equal(f.callbacks.length, 0);
  f.collaboration.reconcile = original;
  await f.collaboration.reconcile(agentId, f.finish('failed-ack-read', 'Now yielded.'));
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].body.data.kind, 'needs_input');
  assert.equal(f.store.pendingMessages('failed-ack-read').length, 0);
  assert.equal(f.store.getMessage(body.messageId).emitted, true);
});

test('exact reads save an unrouted ended request without publishing or changing its receipt', async t => {
  const f = await fixture(t); await f.create('unrouted-read');
  const receipt = f.receipts.get('unrouted-read');
  f.finish('unrouted-read', 'Original unrouted result.');
  await f.collaboration.reconcile(agentId);
  assert.equal(f.store.get('unrouted-read').result, undefined);
  const result = (await f.gateway.call('get_request_result', { requestId: 'unrouted-read' }, { owner })).structuredContent;
  assert.equal(result.savedResult, true);
  assert.equal(result.lastMessage, 'Original unrouted result.');
  assert.equal(result.turnId, 'turn-1');
  assert.equal(f.store.get('unrouted-read').routeId, null);
  assert.deepEqual(f.receipts.get('unrouted-read'), receipt);
  assert.equal(f.eventStore.activeSubscriptions().length, 0);
  assert.equal(f.callbacks.length, 0);
  assert.equal(f.mutations.length, 1);
});

test('unrouted exact reads expose missing markers, shared turns, and the original turn mismatch', async t => {
  const f = await fixture(t); await f.create('missing-marker');
  const original = f.getEntries();
  f.setEntries([]);
  let result = await f.collaboration.getRequestResult('missing-marker', owner);
  assert.equal(result.requestMatched, false);
  assert.equal(result.reason, 'request_marker_not_found');
  assert.equal(result.status, 'unmatched');
  f.setEntries([...original, { ...original[0], item: { type: 'user_message', text: 'Unrelated shared work' }, seqStart: 2, seqEnd: 2 }]);
  f.finish('missing-marker');
  result = await f.collaboration.getRequestResult('missing-marker', owner);
  assert.equal(result.requestMatched, false);
  assert.equal(result.reason, 'shared_turn');
  assert.equal(f.store.get('missing-marker').result, undefined);
  f.setEntries(original);
  f.store.markTurn('missing-marker', { turnId: 'original-other-turn', epoch: 'test-epoch', markerSeq: 1 });
  result = await f.collaboration.getRequestResult('missing-marker', owner);
  assert.equal(result.reason, 'request_turn_mismatch');
  assert.equal(f.store.get('missing-marker').turnId, 'original-other-turn');
});

test('watch binds an existing running request and its normal end notifies once without resubmission', async t => {
  const f = await fixture(t); await f.create('watch-running');
  const receipt = f.receipts.get('watch-running');
  await f.subscribe('dot-a');
  const watched = await f.gateway.call('watch_dispatch_request', { requestId: 'watch-running', notificationRouteId: 'dot-a' }, { owner });
  assert.equal(watched.structuredContent.status, 'bound');
  assert.equal(watched.structuredContent.acceptancePassed, null);
  assert.equal(f.store.get('watch-running').routeId, 'dot-a');
  assert.deepEqual(f.watches.at(-1), [agentId]);
  await f.collaboration.reconcile(agentId);
  await f.delivery.flushDue(); assert.equal(f.callbacks.length, 0);
  await f.collaboration.onObserverEvent({ agentId, event: f.finish('watch-running', 'Watched original result.') });
  await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  await f.gateway.call('watch_dispatch_request', { requestId: 'watch-running', notificationRouteId: 'dot-a' }, { owner });
  await f.collaboration.reconcile(agentId); await f.delivery.flushDue();
  assert.equal(f.callbacks.length, 1);
  assert.equal(f.callbacks[0].body.data.requestId, 'watch-running');
  assert.equal(f.mutations.length, 1);
  assert.deepEqual(f.receipts.get('watch-running'), receipt);
});

test('watch recovers an ended historical request once, including a saved unrouted result', async t => {
  for (const saveFirst of [false, true]) {
    const f = await fixture(t); await f.create('watch-ended');
    f.finish('watch-ended', 'Ended before watching.');
    if (saveFirst) await f.collaboration.getRequestResult('watch-ended', owner);
    await f.subscribe('dot-a');
    const args = { requestId: 'watch-ended', notificationRouteId: 'dot-a' };
    await Promise.all([f.gateway.call('watch_dispatch_request', args, { owner }), f.gateway.call('watch_dispatch_request', args, { owner })]);
    await f.collaboration.reconcile(agentId); await f.delivery.flushDue();
    assert.equal(f.callbacks.length, 1);
    assert.equal(f.callbacks[0].body.data.summary, 'Ended before watching.');
    assert.equal(f.mutations.length, 1);
    assert.deepEqual(f.watches.at(-1), []);
  }
});

test('watch rejects another owner, workspace/path changes, absent routes, rebindings, and unknown agents', async t => {
  const f = await fixture(t); await f.create('watch-safe'); await f.subscribe('dot-a'); await f.subscribe('dot-b');
  const args = { requestId: 'watch-safe', notificationRouteId: 'dot-a' };
  await assert.rejects(f.gateway.call('watch_dispatch_request', args, { owner: 'other-owner' }), /another authenticated/);
  await assert.rejects(f.gateway.call('watch_dispatch_request', { ...args, notificationRouteId: 'absent' }, { owner }), /no active subscription/);
  f.snapshot.workspaceId = 'wks_other';
  await assert.rejects(f.gateway.call('watch_dispatch_request', args, { owner }), /persisted workspace/);
  f.snapshot.workspaceId = 'wks_test';
  const otherPath = join(f.cwd, 'different'); await mkdir(otherPath); f.snapshot.cwd = otherPath;
  await assert.rejects(f.gateway.call('watch_dispatch_request', args, { owner }), /working directory/);
  f.snapshot.cwd = f.cwd;
  await f.gateway.call('watch_dispatch_request', args, { owner });
  await assert.rejects(f.gateway.call('watch_dispatch_request', { ...args, notificationRouteId: 'dot-b' }, { owner }), /different route or subscription/);
  assert.throws(() => f.store.bindRoute('watch-safe', 'dot-a', 'different-subscription'), /different route or subscription/);
  f.store.begin({ requestId: 'unknown-watch', owner, workspaceId: 'wks_test', cwd: f.cwd, submittedAt: new Date().toISOString() });
  await assert.rejects(f.gateway.call('watch_dispatch_request', { ...args, requestId: 'unknown-watch' }, { owner }), /agent_submission_unknown/);
  await assert.rejects(f.gateway.call('watch_dispatch_request', { ...args, owner }, { owner }), /additional properties/);
  assert.equal(f.mutations.length, 1);
});

test('watch cannot borrow a subscription from another owner or workspace, and is a write tool', async t => {
  const f = await fixture(t); await f.create('cross-route');
  await f.subscribe('other-owner-route', 'https://other.example/callback', 'other-owner');
  // Persist a verified subscription for a different workspace to exercise exact filter matching.
  const otherWorkspace = await f.subscribe('other-workspace-route');
  f.eventStore.db.prepare('UPDATE event_subscriptions SET arguments_json=? WHERE id=?').run(JSON.stringify({ workspaceId: 'different-workspace', routeId: 'other-workspace-route' }), otherWorkspace.id);
  for (const route of ['other-owner-route', 'other-workspace-route']) {
    await assert.rejects(f.gateway.call('watch_dispatch_request', { requestId: 'cross-route', notificationRouteId: route }, { owner }), /no active subscription/);
  }
  const catalog = await f.gateway.refreshTools();
  assert.equal(catalog.length, 23);
  assert.equal(catalog.find(tool => tool.name === 'watch_dispatch_request').annotations.readOnlyHint, false);
  assert.equal(catalog.find(tool => tool.name === 'get_request_result').annotations.readOnlyHint, true);
  assert.equal(f.store.get('cross-route').routeId, null);
});

test('an exhausted exact history window reports its actual limit without saving a result', async t => {
  const f = await fixture(t); await f.create('history-window');
  let reads = 0;
  f.gateway.timeline = async () => {
    reads++;
    return { epoch: 'test-epoch', entries: [], hasOlder: true, startCursor: { epoch: 'test-epoch', seq: reads },
      endCursor: { epoch: 'test-epoch', seq: 100 }, gap: false, reset: false, staleCursor: false };
  };
  const result = await f.collaboration.getRequestResult('history-window', owner);
  assert.equal(reads, 4);
  assert.equal(result.requestMatched, false);
  assert.equal(result.reason, 'history_window_exceeded');
  assert.equal(f.store.get('history-window').result, undefined);
});

test('a known shared turn end retains only a pagination notice and never its mixed result', async t => {
  const f = await fixture(t); await f.subscribe('dot-a'); await f.create('shared-ended', 'dot-a');
  await f.collaboration.reconcile(agentId);
  assert.equal(f.store.get('shared-ended').turnId, 'turn-1');
  const original = f.getEntries();
  f.setEntries([...original, { ...original[0], item: { type: 'user_message', text: 'Another dispatch in the same turn' }, seqStart: 2, seqEnd: 2 }]);
  await f.collaboration.reconcile(agentId, f.finish('shared-ended', 'Mixed result must not be returned.'));
  const result = await f.collaboration.getRequestResult('shared-ended', owner);
  assert.equal(result.resultReadStatus, 'needs_pagination');
  assert.equal(result.reason, 'shared_turn');
  assert.equal(result.lastMessage, null);
  assert.deepEqual(result.entries, []);
  assert.equal(result.acceptancePassed, null);
});
