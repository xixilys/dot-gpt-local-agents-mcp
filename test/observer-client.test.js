import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { ObserverClient } from '../src/observer-client.js';

const A = '11111111-1111-4111-8111-111111111111';
const B = '22222222-2222-4222-8222-222222222222';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
function fixture(options = {}) {
  const children = [], events = [], statuses = [], calls = [];
  const spawnImpl = (...args) => {
    calls.push(args);
    const child = new EventEmitter();
    Object.assign(child, { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), kills: [], writes: '' });
    child.stdin.on('data', chunk => { child.writes += chunk.toString(); });
    child.kill = signal => { child.kills.push(signal); if (signal === 'SIGTERM') queueMicrotask(() => child.emit('exit', 0)); return true; };
    children.push(child); return child;
  };
  const client = new ObserverClient({ spawnImpl, onEvent: e => events.push(e), onStatus: e => statuses.push(e),
    backoffMs: 5, maxBackoffMs: 10, killTimeoutMs: 5, startupTimeoutMs: 1000, ...options });
  const send = (event, index = children.length - 1) => children[index].stdout.write(JSON.stringify(event) + '\n');
  const connect = () => send({ type: 'connection-status', status: 'connected' });
  return { children, events, statuses, calls, client, send, connect };
}
test('spawns only owned helper, subscribes replacement watch set after connection and ACK', t => {
  const f = fixture(); t.after(() => f.client.close());
  f.client.watchAgentIds([A]).start().start();
  assert.equal(f.children.length, 1);
  assert.match(f.calls[0][0], /Paseo Helper$/);
  assert.match(f.calls[0][1][0], /agent-observer\.mjs$/);
  assert.equal(f.calls[0][2].env.ELECTRON_RUN_AS_NODE, '1');
  assert.equal(f.calls[0][2].env.PASEO_AGENT_ID, undefined);
  f.connect();
  assert.deepEqual(JSON.parse(f.children[0].writes.trim()), { action: 'watch', agentIds: [A] });
  assert.equal(f.statuses.some(e => e.status === 'ready'), false);
  f.client.watchAgentIds([B]);
  f.send({ type: 'ready', agentIds: [A], watchVersion: 1 });
  assert.equal(f.statuses.some(e => e.status === 'ready'), false);
  f.send({ type: 'ready', agentIds: [B], watchVersion: 2 });
  assert.deepEqual(f.statuses.at(-1), { status: 'ready', sourceGeneration: 1, agentIds: [B], watchVersion: 2 });
  assert.deepEqual(JSON.parse(f.children[0].writes.trim().split('\n').at(-1)), { action: 'watch', agentIds: [B] });
});
test('filters unwatched identities and does not suppress or invent sequence discontinuities', t => {
  const f = fixture(); t.after(() => f.client.close()); f.client.watchAgentIds([A]).start(); f.connect();
  f.send({ type: 'lifecycle', source: 'native', agentId: B, eventType: 'turn_completed' });
  const marker = { type: 'request_marker', source: 'native', agentId: A, requestId: 'req1', turnId: 't1', seq: 4, epoch: 'epoch1' };
  f.send(marker); f.send(marker);
  f.send({ type: 'discontinuity', agentId: A, reason: 'sequence_gap', seq: 8, epoch: 'epoch1' });
  f.send({ type: 'discontinuity', agentId: A, reason: 'epoch_changed', seq: 0, epoch: 'epoch2' });
  assert.deepEqual(f.events.map(e => e.type), ['request_marker', 'request_marker', 'discontinuity', 'discontinuity']);
  assert.equal(f.events.every(e => e.sourceGeneration === 1), true);
});
test('historical idle/final hints never become native completion', t => {
  const f = fixture(); t.after(() => f.client.close()); f.client.watchAgentIds([A]).start(); f.connect();
  f.send({ type: 'snapshot', source: 'history', agentId: A, agentStatus: 'idle', finalSeen: true, endCursor: { epoch: 'e', seq: 8 }, hasOlder: false, hasNewer: false });
  f.send({ type: 'reconcile', source: 'history', agentId: A, reason: 'idle_final_snapshot', endCursor: { epoch: 'e', seq: 8 } });
  assert.deepEqual(f.events.map(e => e.type), ['snapshot', 'reconcile']);
});
test('unknown properties/raw logs fail closed without exposing contents', async t => {
  const f = fixture(); t.after(() => f.client.close()); f.client.watchAgentIds([A]).start(); f.connect();
  f.send({ type: 'lifecycle', source: 'native', agentId: A, eventType: 'turn_failed', error: 'private-provider-text' });
  assert.equal(JSON.stringify([f.events, f.statuses]).includes('private-provider-text'), false);
  assert.equal(f.events.at(-1).reason, 'invalid_output');
  await wait(20); assert.equal(f.children.length, 2);
  f.children[1].stdout.write('private-unstructured-log\n');
  assert.equal(JSON.stringify([f.events, f.statuses]).includes('private-unstructured-log'), false);
});
test('bounds both complete and unfinished output lines', async t => {
  const f = fixture({ maxLineBytes: 256 }); t.after(() => f.client.close()); f.client.start();
  f.children[0].stdout.write('x'.repeat(257));
  assert.equal(f.events.at(-1).reason, 'output_limit');
  await wait(20);
  f.children[1].stdout.write('x'.repeat(257) + '\n');
  assert.equal(f.events.at(-1).reason, 'output_limit');
});
test('disconnect restarts only collector and restores latest watch without dispatch', async t => {
  const f = fixture(); t.after(() => f.client.close()); f.client.watchAgentIds([A]).start(); f.connect();
  f.send({ type: 'connection-status', status: 'disconnected' });
  f.client.watchAgentIds([B]);
  await wait(20); assert.equal(f.children.length, 2);
  f.connect();
  assert.deepEqual(JSON.parse(f.children[1].writes.trim()), { action: 'watch', agentIds: [B] });
  assert.equal(f.calls.length, 2);
  assert.equal(f.events.some(e => e.type === 'lifecycle'), false);
  assert.equal(f.statuses.at(-1).sourceGeneration, 2);
  f.send({ type: 'lifecycle', source: 'native', agentId: A, eventType: 'turn_completed' }, 0);
  assert.equal(f.events.some(e => e.type === 'lifecycle'), false);
});
test('close stops owned child, cancels restart and is idempotent', async () => {
  const f = fixture(); f.client.start(); const child = f.children[0];
  f.client.close(); f.client.close();
  await wait(20);
  assert.deepEqual(child.kills, ['SIGTERM']); assert.equal(f.children.length, 1);
  assert.equal(f.statuses.at(-1).status, 'closed');
  assert.throws(() => f.client.start(), /closed/);
});
test('replacement does not overlap a collector that has not exited', async t => {
  const f = fixture(); t.after(() => f.client.close()); f.client.start(); f.connect();
  const first = f.children[0];
  first.kill = signal => { first.kills.push(signal); return true; };
  f.send({ type: 'connection-status', status: 'disconnected' });
  await wait(20);
  assert.equal(f.children.length, 1);
  assert.deepEqual(first.kills, ['SIGTERM', 'SIGKILL']);
  first.emit('exit', 1);
  await wait(20); assert.equal(f.children.length, 2);
});
test('closing inside a callback prevents remaining buffered events from delivery', () => {
  const f = fixture(); f.client.watchAgentIds([A]).start(); f.connect();
  f.client.onEvent = event => { f.events.push(event); f.client.close(); };
  const event = { type: 'lifecycle', source: 'native', agentId: A, eventType: 'turn_completed' };
  f.children[0].stdout.write(JSON.stringify(event) + '\n' + JSON.stringify(event) + '\n');
  assert.equal(f.events.length, 1);
});
test('invalid or too many watches are rejected before spawning', () => {
  const f = fixture();
  assert.throws(() => f.client.watchAgentIds(['bad']), /Invalid/);
  assert.throws(() => f.client.watchAgentIds(Array(257).fill(A)), /Invalid/);
  assert.equal(f.children.length, 0); f.client.close();
});
