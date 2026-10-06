import test from 'node:test';
import assert from 'node:assert/strict';
import { requestMarker, waitForAgentResult } from '../src/wait-result.js';

const agentId = '11111111-1111-4111-8111-111111111111';
const args = { agentId, requestId: 'followup-2', submittedAt: '2026-10-05T07:00:00Z', timeoutMs: 10000, limit: 100 };
const entry = (seq, type, text, turnId = 'turn-2') => ({ seqStart: seq, seqEnd: seq, turnId,
  timestamp: '2026-10-05T07:00:01Z', item: { type, text } });
const user = entry(4, 'user_message', requestMarker(args.requestId) + 'same prompt');
const assistant = entry(5, 'assistant_message', 'actual new reply');
function fixture(overrides = {}) {
  const calls = [];
  const snapshot = { id: agentId, cwd: '/test', status: 'idle', activeTurn: null, pendingPermissions: [], ...overrides.snapshot };
  const page = { agentId, agent: snapshot, entries: [user, assistant], epoch: 'epoch-1',
    gap: false, reset: false, staleCursor: false, hasOlder: false, hasNewer: false,
    startCursor: { epoch: 'epoch-1', seq: 4 }, endCursor: { epoch: 'epoch-1', seq: 5 }, ...overrides.page };
  const client = {
    async waitForFinish(id, timeout) { calls.push(['wait', id, timeout]); if (overrides.waitError) throw Error('disconnect'); return { status: 'idle', final: snapshot, lastMessage: 'old cached final', ...overrides.wait }; },
    async fetchAgentTimeline(id, options) { calls.push(['timeline', id, options]); if (overrides.readError) throw Error('disconnect'); return overrides.read ? overrides.read(options) : page; },
    async fetchAgent(input) { calls.push(['snapshot', input]); return { agent: snapshot }; },
  };
  return { client, calls, page };
}

test('bounded native wait returns actual marked turn text and tools, not the native cached final', async () => {
  const f = fixture({ page: { entries: [user, entry(5, 'tool_call', 'full tool detail'), { ...assistant, seqStart: 6, seqEnd: 6 }] } });
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'idle');
  assert.equal(r.requestMatched, true);
  assert.equal(r.turnId, 'turn-2');
  assert.equal(r.lastMessage, 'actual new reply');
  assert.equal(r.terminalDetected, true);
  assert.equal(r.terminalKind, 'native_wait_idle');
  assert.equal(r.turnOutcome, 'unknown');
  assert.equal(r.acceptancePassed, null);
  assert.equal(r.timeline.entries[1].item.type, 'tool_call');
  assert.ok(f.calls[0][2] <= 5000);
  assert.equal(r.automaticWakeSupported, false);
});

test('running after native timeout remains running and does not accept a previous final', async () => {
  const f = fixture({ snapshot: { status: 'running', activeTurn: { turnId: 'turn-2' } }, wait: { status: 'timeout' } });
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'running'); assert.equal(r.waitTimedOut, true); assert.equal(r.terminalDetected, false);
  assert.equal(r.lastMessage, undefined);
});

test('permission is a nonterminal response with the actual pending request', async () => {
  const f = fixture({ snapshot: { pendingPermissions: [{ requestId: 'p1', kind: 'shell' }] }, wait: { status: 'permission' } });
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'permission'); assert.equal(r.terminalDetected, false); assert.equal(r.pendingPermissions[0].requestId, 'p1');
});

test('old final and legacy unmarked user never complete the new request', async () => {
  const f = fixture({ page: { entries: [entry(1, 'user_message', 'old prompt', 'turn-1'), entry(2, 'assistant_message', 'old final', 'turn-1')] } });
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'timeout'); assert.equal(r.requestMatched, false); assert.equal(r.terminalDetected, null);
  assert.equal(r.reason, 'request_marker_not_found'); assert.deepEqual(r.timeline.entries, []);
});

test('a marker from before the receipt is not this dispatch', async () => {
  const f = fixture({ page: { entries: [{ ...user, timestamp: '2026-10-05T06:59:59Z' }, assistant] } });
  assert.equal((await waitForAgentResult(f.client, args)).requestMatched, false);
});

test('newer unmarked turn, same-turn steering, and mismatched active turn stay unconfirmed', async () => {
  for (const [patch, reason] of [
    [{ page: { entries: [user, assistant, entry(6, 'user_message', 'external followup', 'turn-3')] } }, 'newer_turn_present'],
    [{ page: { entries: [user, entry(5, 'user_message', 'steering'), { ...assistant, seqStart: 6 }] } }, 'shared_turn'],
    [{ snapshot: { activeTurn: { turnId: 'turn-3' }, status: 'running' } }, 'different_active_turn'],
  ]) {
    const r = await waitForAgentResult(fixture(patch).client, args);
    assert.equal(r.reason, reason); assert.notEqual(r.terminalDetected, true);
  }
});

test('missing turn, missing activeTurn field, and caller turn mismatch do not infer completion', async () => {
  for (const f of [fixture({ page: { entries: [{ ...user, turnId: undefined }, assistant] } }), fixture({ snapshot: { activeTurn: undefined } })]) {
    const r = await waitForAgentResult(f.client, args); assert.equal(r.status, 'timeout'); assert.notEqual(r.terminalDetected, true);
  }
  const r = await waitForAgentResult(fixture().client, { ...args, turnId: 'turn-1' });
  assert.equal(r.reason, 'turn_not_matched'); assert.equal(r.requestMatched, false);
});

test('native error is agent-level information and never an acceptance result', async () => {
  const r = await waitForAgentResult(fixture({ snapshot: { status: 'error', lastError: 'provider failed' }, wait: { status: 'error', error: 'provider failed' } }).client, args);
  assert.equal(r.status, 'error'); assert.equal(r.reason, 'agent_error'); assert.equal(r.acceptancePassed, null); assert.equal(r.terminalDetected, null);
});

test('cancellation residue is only idle, never a completed or successful turn outcome', async () => {
  const r = await waitForAgentResult(fixture().client, args);
  assert.equal(r.status, 'idle'); assert.equal(r.turnOutcome, 'unknown'); assert.equal(r.acceptancePassed, null);
  const timed = await waitForAgentResult(fixture({ wait: { status: 'timeout' } }).client, args);
  assert.equal(timed.status, 'result_available'); assert.equal(timed.terminalDetected, null);
});

test('gap, reset, stale cursor, and cross-page epoch changes never become completion', async () => {
  for (const flag of ['gap', 'reset', 'staleCursor']) {
    const r = await waitForAgentResult(fixture({ page: { [flag]: true } }).client, args);
    assert.equal(r.reason, 'timeline_discontinuity'); assert.notEqual(r.terminalDetected, true);
  }
  const f = fixture();
  f.client.fetchAgentTimeline = async (id, options) => options.direction === 'tail'
    ? { ...f.page, entries: [assistant], hasOlder: true }
    : { ...f.page, epoch: 'replacement-epoch', entries: [user] };
  assert.equal((await waitForAgentResult(f.client, args)).reason, 'timeline_discontinuity');
});

test('reads back over tool pages and preserves the matched turn across pagination', async () => {
  const f = fixture();
  f.client.fetchAgentTimeline = async (id, options) => options.direction === 'tail'
    ? { ...f.page, entries: [entry(6, 'tool_call', 'output'), { ...assistant, seqStart: 7, seqEnd: 7 }], hasOlder: true, startCursor: { epoch: 'epoch-1', seq: 6 } }
    : { ...f.page, entries: [entry(1, 'user_message', 'older', 'turn-1'), user], hasOlder: false, startCursor: { epoch: 'epoch-1', seq: 1 } };
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'idle'); assert.equal(r.timeline.entries.length, 3); assert.equal(r.timeline.pages.length, 2);
});

test('bounded history exhaustion explicitly asks for pagination, not acceptance', async () => {
  const f = fixture({ page: { entries: [entry(9, 'tool_call', 'output')], hasOlder: true } });
  let seq = 20;
  f.client.fetchAgentTimeline = async () => ({ ...f.page, startCursor: { epoch: 'epoch-1', seq: seq-- } });
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.reason, 'history_window_exceeded'); assert.equal(r.timeline.pages.length, 3); assert.notEqual(r.terminalDetected, true);
});

test('wait or result-read disconnection is a safe read failure and never dispatches anything', async () => {
  for (const patch of [{ waitError: true }, { readError: true }]) {
    const f = fixture(patch); const r = await waitForAgentResult(f.client, args);
    assert.equal(r.status, 'error'); assert.equal(r.retrySafe, true); assert.notEqual(r.terminalDetected, true);
    assert.ok(f.calls.every(c => ['wait', 'timeline', 'snapshot'].includes(c[0])));
  }
});

test('a newer turn that finishes between pagination and snapshot cannot confirm the old idle result', async () => {
  const f = fixture();
  let reads = 0;
  f.client.fetchAgentTimeline = async () => ++reads === 1 ? f.page : { ...f.page,
    entries: [entry(9, 'assistant_message', 'newer final', 'turn-3')], endCursor: { epoch: 'epoch-1', seq: 9 } };
  const r = await waitForAgentResult(f.client, args);
  assert.equal(r.status, 'timeout'); assert.equal(r.reason, 'tail_changed_during_read'); assert.notEqual(r.terminalDetected, true);
});
