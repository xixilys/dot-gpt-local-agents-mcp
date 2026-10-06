import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { Webhook } from 'standardwebhooks';
import { EventStore } from '../src/event-store.js';
import { EventDelivery, SafeCallbackClient, CallbackEndpointError, isPublicAddress } from '../src/event-delivery.js';

const secret = `whsec_${Buffer.alloc(32, 65).toString('base64')}`;
const secondSecret = `whsec_${Buffer.alloc(32, 66).toString('base64')}`;
const destination = { mode: 'webhook', url: 'https://callback.example/events', secret };
const args = { workspaceId: 'wks_test', routeId: 'route_test' };
const eventName = 'agent.attention';

async function setup(t, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'local-events-test-'));
  let now = Date.now();
  let store = new EventStore(dir);
  const calls = [];
  let respond = async (url, input) => {
    new Webhook(secret).verify(input.body, input.headers);
    const body = JSON.parse(input.body);
    return { status: 200, body: body.type === 'verification' ? JSON.stringify({ challenge: body.challenge }) : '' };
  };
  const callbackClient = { async post(url, input) { calls.push({ url, ...input }); return respond(url, input); } };
  let delivery = new EventDelivery({ store, callbackClient, now: () => now, ...options });
  t.after(async () => { await delivery.stop(); store.close(); await rm(dir, { recursive: true, force: true }); });
  return { dir, calls, callbackClient, get store() { return store; }, get delivery() { return delivery; },
    now: () => now,
    advance(ms) { now += ms; },
    respond(fn) { respond = fn; },
    async subscribe(patch = {}, filter = args, owner = 'owner-test', ttlMs) {
      return delivery.subscribe(eventName, filter, { ...destination, ...patch }, ttlMs, null, owner);
    },
    publish(id = 'evt_one', filter = sub => sub.owner === 'owner-test') {
      return delivery.publish(eventName, { workspaceId: 'wks_test', message: 'Synthetic event' }, filter,
        { eventId: id, timestamp: '2026-10-05T12:00:00Z' });
    },
    async reopen() {
      await delivery.stop(); store.close();
      store = new EventStore(dir);
      delivery = new EventDelivery({ store, callbackClient, now: () => now, ...options });
    },
  };
}

test('signed challenge must succeed before subscriptions can activate, with categorized safe errors', async t => {
  const f = await setup(t);
  for (const response of [
    { status: 401, body: '' }, { status: 200, body: '{"challenge":"wrong"}' },
    { status: 200, body: 'not JSON' }, { status: 200, body: 'x'.repeat(16 * 1024 + 1) },
  ]) {
    f.respond(async () => response);
    await assert.rejects(f.subscribe(), error => error instanceof CallbackEndpointError && error.code === -32015 &&
      !JSON.stringify(error).includes(secret));
    assert.equal(f.store.activeSubscriptions().length, 0);
  }
  // A receiver that verifies with the wrong key refuses the challenge too.
  f.respond(async (_url, input) => {
    try { new Webhook(secondSecret).verify(input.body, input.headers); return { status: 200, body: '{}' }; }
    catch { return { status: 401, body: '' }; }
  });
  await assert.rejects(f.subscribe(), error => error.data.reason === 'http_error');
  assert.equal(f.store.activeSubscriptions().length, 0);
  f.respond(async (_url, input) => {
    const challenge = new Webhook(secret).verify(input.body, input.headers).challenge;
    assert.match(input.headers['webhook-id'], /^msg_verification_/);
    return { status: 200, body: JSON.stringify({ challenge }) };
  });
  assert.ok((await f.subscribe()).id.startsWith('sub_'));
});

test('canonical refresh and simultaneous same-identity requests retain one subscription across restart', async t => {
  const f = await setup(t);
  const [a, b] = await Promise.all([f.subscribe(), f.subscribe({}, { routeId: 'route_test', workspaceId: 'wks_test' })]);
  assert.equal(a.id, b.id);
  assert.equal(f.calls.length, 1);
  assert.equal(f.store.activeSubscriptions().length, 1);
  assert.equal(f.store.activeSubscriptions()[0].secret, undefined);
  assert.equal(a.cursor, null); assert.equal(a.truncated, false);
  assert.equal(JSON.stringify(a).includes('owner-test'), false);
  const oldExpiry = a.refreshBefore;
  f.advance(5000);
  await f.reopen();
  const c = await f.subscribe();
  assert.equal(c.id, a.id);
  assert.ok(c.refreshBefore > oldExpiry);
  assert.equal(f.calls.length, 1); // bounded verification cache is durable
  assert.equal(f.store.getSubscriptionInternal(c.id).secret, secret);
  for (const filename of ['events.sqlite', 'events.sqlite-wal', 'events.sqlite-shm']) {
    assert.equal((await stat(join(f.dir, filename))).mode & 0o777, 0o600);
  }
});

test('TTL default, finite request, and null use explicit expiration; expired and cancelled events do not send', async t => {
  const f = await setup(t);
  const before = Date.now();
  const d = await f.subscribe();
  assert.ok(Math.abs(Date.parse(d.refreshBefore) - before - 86_400_000) < 500);
  const permanent = await f.subscribe({ url: 'https://callback.example/null' }, args, 'owner-test', null);
  assert.ok(Math.abs(Date.parse(permanent.refreshBefore) - before - 7 * 86_400_000) < 500);
  const finite = await f.subscribe({ url: 'https://callback.example/short' }, args, 'owner-test', 10);
  f.publish();
  f.delivery.unsubscribe(eventName, args, destination, 'owner-test');
  f.delivery.unsubscribe(eventName, args, destination, 'owner-test');
  f.delivery.unsubscribe(eventName, args, { ...destination, url: 'https://callback.example/null' }, 'owner-test');
  f.advance(11);
  const callCount = f.calls.length;
  await f.delivery.flushDue();
  assert.equal(f.calls.length, callCount);
  assert.equal(f.store.listDeliveries({ subscriptionId: finite.id })[0].state, 'expired');
  assert.equal(f.store.listDeliveries({ subscriptionId: d.id })[0].state, 'cancelled');
  await assert.rejects(f.subscribe({}, args, 'owner-test', 0), /ttlMs/);
  await assert.rejects(f.subscribe({}, args, 'owner-test', Infinity), /ttlMs/);
});

test('owner/filter isolation and durable event+subscription deduplication preserve exact body bytes', async t => {
  const f = await setup(t);
  const a = await f.subscribe();
  const b = await f.subscribe({ url: 'https://callback.example/owner2' }, args, 'owner-two');
  f.publish('evt_one');
  f.publish('evt_one');
  assert.equal(f.store.listDeliveries().length, 1);
  assert.equal(f.store.listDeliveries()[0].subscriptionId, a.id);
  await f.reopen();
  await f.delivery.flushDue();
  const eventCall = f.calls.at(-1);
  assert.equal(eventCall.headers['webhook-id'], 'evt_one');
  assert.equal(JSON.parse(eventCall.body).cursor, null);
  new Webhook(secret).verify(eventCall.body, eventCall.headers);
  assert.equal(f.store.listDeliveries()[0].state, 'received');
  const count = f.calls.length;
  f.publish('evt_one'); await f.delivery.flushDue();
  assert.equal(f.calls.length, count);
  assert.equal(f.store.listDeliveries({ subscriptionId: b.id }).length, 0);
  assert.throws(() => f.delivery.publish(eventName, { changed: true }, () => true,
    { eventId: 'evt_one', timestamp: '2026-10-05T12:00:00Z' }), /different event data/);
  // Same event legitimately targeting two subscriptions gets two independent rows.
  f.publish('evt_two', () => true);
  assert.equal(f.store.listDeliveries({ eventId: 'evt_two' }).length, 2);
});

test('transient attempts back off, preserve event ID/body, refresh signing timestamp and stop at five', async t => {
  const f = await setup(t);
  await f.subscribe(); f.publish();
  const outcomes = [new Error('synthetic failure'), 429, 503, 500, 502];
  f.respond(async () => { const value = outcomes.shift(); if (value instanceof Error) throw value; return { status: value, body: '' }; });
  const originalCount = f.calls.length;
  for (let n = 1; n <= 5; n++) {
    await f.delivery.flushDue();
    const row = f.store.listDeliveries()[0];
    assert.equal(row.attempts, n);
    if (n < 5) {
      assert.equal(row.state, 'retry');
      const count = f.calls.length;
      await f.delivery.flushDue(); assert.equal(f.calls.length, count);
      f.advance(1000 * 2 ** (n - 1));
    } else assert.equal(row.state, 'failed');
  }
  f.advance(60_000); await f.delivery.flushDue();
  assert.equal(f.calls.length - originalCount, 5);
  const calls = f.calls.slice(originalCount);
  assert.equal(new Set(calls.map(c => c.body)).size, 1);
  assert.ok(calls.every(c => c.headers['webhook-id'] === 'evt_one'));
  assert.equal(new Set(calls.map(c => c.headers['webhook-timestamp'])).size, 5);
  assert.equal(f.store.deliveryAttempts().length, 5);
  assert.equal(JSON.stringify(f.store.deliveryAttempts()).includes(secret), false);
});

test('410 stops subscription and pending siblings; 413 and other 4xx permanently fail without retry', async t => {
  for (const status of [410, 413, 400, 401]) {
    await t.test(String(status), async t => {
      const f = await setup(t);
      const sub = await f.subscribe(); f.publish('evt_a'); f.publish('evt_b');
      f.respond(async () => ({ status, body: '' }));
      await f.delivery.flushDue();
      const count = f.calls.length;
      f.advance(60_000); await f.delivery.flushDue();
      assert.equal(f.calls.length, count);
      const rows = f.store.listDeliveries();
      assert.equal(rows[0].state, 'failed');
      if (status === 410) {
        assert.equal(f.store.getSubscriptionInternal(sub.id).state, 'stopped');
        assert.equal(rows[1].state, 'cancelled'); assert.equal(rows[1].attempts, 0);
      } else assert.equal(rows[1].state, 'failed');
    });
  }
});

test('secret rotation verifies replacement and temporarily signs both old and new keys', async t => {
  const f = await setup(t);
  const initial = await f.subscribe();
  f.respond(async (_url, input) => {
    const body = new Webhook(secondSecret).verify(input.body, input.headers);
    return { status: 200, body: body.type === 'verification' ? JSON.stringify({ challenge: body.challenge }) : '' };
  });
  const refreshed = await f.subscribe({ secret: secondSecret });
  assert.equal(refreshed.id, initial.id);
  f.publish(); await f.delivery.flushDue();
  const call = f.calls.at(-1);
  assert.equal(call.headers['webhook-signature'].split(' ').length, 2);
  new Webhook(secondSecret).verify(call.body, call.headers);
  new Webhook(secret).verify(call.body, call.headers);
  f.advance(300_001); f.publish('evt_after_rotation'); await f.delivery.flushDue();
  assert.equal(f.calls.at(-1).headers['webhook-signature'].split(' ').length, 1);
  assert.equal(f.calls.at(-1).headers['webhook-signature'], new Webhook(secondSecret).sign('evt_after_rotation',
    new Date(f.now()), f.calls.at(-1).body));
});

test('invalid secrets, filter, schema and payload fail before callback application data', async t => {
  const f = await setup(t, { validateSubscription: (_name, filter) => filter.routeId === 'route_test' });
  for (const value of ['bad', 'whsec_YQ==', `whsec_${'!'.repeat(32)}`, `whsec_${Buffer.alloc(65).toString('base64')}`]) {
    await assert.rejects(f.subscribe({ secret: value }), /Invalid webhook signing secret/);
  }
  await assert.rejects(f.subscribe({}, { workspaceId: 'wks_test' }), /Invalid event filter/);
  assert.equal(f.calls.length, 0);
  await f.subscribe();
  assert.throws(() => f.delivery.publish(eventName, { text: 'x'.repeat(262144) }), /256 KiB/);
  assert.throws(() => f.delivery.enqueue({ eventId: 'x', name: eventName, timestamp: 'y', data: {} }), /Invalid MCP event/);
  assert.throws(() => f.delivery.publish(eventName, { invalid: undefined }), /JSON value/);
  assert.throws(() => f.publish('evt_async', async () => true), /synchronous/);
  assert.equal(f.store.listDeliveries().length, 0);
});

test('unsubscribe during verification cannot activate; revoked owner and cancelled in-flight callbacks cannot retry', async t => {
  const f = await setup(t);
  let release;
  f.respond(async (_url, input) => new Promise(resolve => { release = () => resolve({ status: 200,
    body: JSON.stringify({ challenge: JSON.parse(input.body).challenge }) }); }));
  const subscription = f.subscribe();
  await new Promise(resolve => setImmediate(resolve));
  f.delivery.unsubscribe(eventName, args, destination, 'owner-test');
  release(); await assert.rejects(subscription, /cancelled/);
  assert.equal(f.store.activeSubscriptions().length, 0);
  f.respond(async (_url, input) => ({ status: 200,
    body: JSON.stringify({ challenge: JSON.parse(input.body).challenge }) }));
  await f.subscribe(); // verification success was cached; deliberate new subscribe works
  await f.subscribe({ url: 'https://callback.example/second' }, args, 'another-owner');
  f.publish('evt_inflight');
  f.respond(async () => new Promise(resolve => { release = () => resolve({ status: 503, body: '' }); }));
  const flush = f.delivery.flushDue();
  await new Promise(resolve => setImmediate(resolve));
  f.delivery.revokeOwner('owner-test');
  release(); await flush;
  assert.equal(f.store.listDeliveries()[0].state, 'cancelled');
  assert.equal(f.store.activeSubscriptions().length, 1);
  f.advance(10000); assert.equal((await f.delivery.flushDue()).attempted, 0);
});

function requestFixture(observed, { status = 200, chunks = ['{}'], headers = {}, hang = false } = {}) {
  return (options, callback) => {
    const req = new EventEmitter();
    req.destroy = () => { req.destroyed = true; };
    req.end = body => {
      observed.push({ options, body, req });
      if (hang) return;
      const res = new PassThrough(); res.statusCode = status; res.headers = headers;
      queueMicrotask(() => { callback(res); for (const chunk of chunks) res.write(chunk); res.end(); });
    };
    return req;
  };
}

test('HTTPS client pins checked IP while retaining hostname/SNI/TLS and resolves again for every connection', async () => {
  const observed = [];
  let lookups = 0;
  const client = new SafeCallbackClient({
    resolve: async (hostname, options) => { lookups++; assert.equal(hostname, 'callback.example'); assert.equal(options.all, true);
      return [{ address: lookups === 1 ? '8.8.8.8' : '1.1.1.1', family: 4 }]; },
    request: requestFixture(observed),
  });
  for (let n = 0; n < 2; n++) await client.post(destination.url, { body: '{}', headers: {} });
  assert.equal(lookups, 2);
  for (const [index, { options }] of observed.entries()) {
    assert.equal(options.hostname, 'callback.example'); assert.equal(options.servername, 'callback.example');
    assert.equal(options.rejectUnauthorized, true); assert.equal(options.agent, false);
    options.lookup('callback.example', {}, (error, address, family) => {
      assert.equal(error, null); assert.equal(address, index === 0 ? '8.8.8.8' : '1.1.1.1'); assert.equal(family, 4);
    });
    options.lookup('callback.example', { all: true }, (error, addresses) => {
      assert.equal(error, null); assert.equal(addresses.length, 1);
    });
  }
});

test('bad destination forms and every non-public DNS answer reject before any connection', async () => {
  const observed = [];
  const publicIPs = ['8.8.8.8', '1.1.1.1', '2606:4700:4700::1111'];
  const blockedIPs = ['127.0.0.1', '10.1.2.3', '172.16.0.1', '192.168.0.1', '169.254.169.254',
    '100.64.0.1', '0.0.0.0', '224.0.0.1', '240.0.0.1', '198.18.0.1', '192.0.2.1',
    '::1', 'fe80::1', 'fc00::1', '::ffff:8.8.8.8', '64:ff9b::808:808', '2001:db8::1', '2002:808:808::1', '4000::1'];
  assert.ok(publicIPs.every(isPublicAddress)); assert.ok(blockedIPs.every(ip => !isPublicAddress(ip)));
  const client = new SafeCallbackClient({ resolve: async () => [{ address: '8.8.8.8', family: 4 },
    { address: '127.0.0.1', family: 4 }], request: requestFixture(observed) });
  for (const url of ['http://callback.example/', 'https://u:p@callback.example/', 'https://callback.example/#fragment',
    'https://callback.example/#', 'https://127.1/', 'https://[::ffff:8.8.8.8]/', destination.url]) {
    await assert.rejects(client.post(url, { body: '{}', headers: {} }), error => error instanceof CallbackEndpointError);
  }
  assert.equal(observed.length, 0);
});

test('redirect, response limit and request deadline never follow or send after delayed DNS', async () => {
  for (const fixture of [{ status: 302, reason: 'redirect' },
    { chunks: ['x'.repeat(16 * 1024 + 1)], reason: 'response_too_large' },
    { headers: { 'content-length': '20000' }, reason: 'response_too_large' },
    { hang: true, reason: 'timeout' }]) {
    const observed = [];
    const client = new SafeCallbackClient({ timeoutMs: 20, resolve: async () => [{ address: '8.8.8.8', family: 4 }],
      request: requestFixture(observed, fixture) });
    await assert.rejects(client.post(destination.url, { body: '{}', headers: {} }), error => error.data.reason === fixture.reason);
    assert.equal(observed.length, 1); assert.equal(observed[0].req.destroyed, true);
  }
  let release;
  const observed = [];
  const client = new SafeCallbackClient({ timeoutMs: 20, resolve: () => new Promise(resolve => { release = resolve; }),
    request: requestFixture(observed) });
  await assert.rejects(client.post(destination.url, { body: '{}', headers: {} }), error => error.data.reason === 'timeout');
  release([{ address: '8.8.8.8', family: 4 }]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(observed.length, 0);
});

test('callback cancellation while DNS is pending prevents a late connection and cancellation closes an active request', async () => {
  let release;
  const observed = [];
  const controller = new AbortController();
  const client = new SafeCallbackClient({ resolve: () => new Promise(resolve => { release = resolve; }),
    request: requestFixture(observed) });
  const pending = client.post(destination.url, { body: '{}', headers: {}, signal: controller.signal });
  await new Promise(resolve => setImmediate(resolve));
  controller.abort();
  await assert.rejects(pending, error => error.data.reason === 'subscription_cancelled');
  release([{ address: '8.8.8.8', family: 4 }]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(observed.length, 0);
  const second = new AbortController();
  const hanging = new SafeCallbackClient({ resolve: async () => [{ address: '8.8.8.8', family: 4 }],
    request: requestFixture(observed, { hang: true }) });
  const connected = hanging.post(destination.url, { body: '{}', headers: {}, signal: second.signal });
  await new Promise(resolve => setImmediate(resolve));
  second.abort();
  await assert.rejects(connected, error => error.data.reason === 'subscription_cancelled');
  assert.equal(observed.length, 1); assert.equal(observed[0].req.destroyed, true);
});

test('known 410 survives a bounded callback response failure and still stops the subscription', async t => {
  const f = await setup(t);
  const sub = await f.subscribe(); f.publish();
  f.respond(async () => {
    const error = new CallbackEndpointError('response_too_large'); error.status = 410; throw error;
  });
  await f.delivery.flushDue();
  assert.equal(f.store.getSubscriptionInternal(sub.id).state, 'stopped');
  assert.equal(f.store.listDeliveries()[0].httpStatus, 410);
  assert.equal(f.store.listDeliveries()[0].attempts, 1);
});

test('owner revocation or unsubscribe during asynchronous filter validation prevents activation', async t => {
  for (const ownerRevocation of [false, true]) {
    await t.test(ownerRevocation ? 'revokeOwner' : 'unsubscribe', async t => {
      let release;
      const f = await setup(t, { validateSubscription: () => new Promise(resolve => { release = resolve; }) });
      const pending = f.subscribe();
      if (ownerRevocation) f.delivery.revokeOwner('owner-test');
      else f.delivery.unsubscribe(eventName, args, destination, 'owner-test');
      release(true);
      await assert.rejects(pending, /cancelled/);
      assert.equal(f.calls.length, 0); assert.equal(f.store.activeSubscriptions().length, 0);
    });
  }
});

test('recovered sending lease retries once due, concurrent flushes share one attempt, authorization revocation stops delivery', async t => {
  let authorized = true;
  const f = await setup(t, { authorizeSubscription: () => authorized });
  const sub = await f.subscribe(); f.publish();
  f.store.claimDelivery(sub.id, 'evt_one', f.now());
  await f.reopen();
  assert.equal((await f.delivery.flushDue()).attempted, 0);
  f.advance(30_001);
  await Promise.all([f.delivery.flushDue(), f.delivery.flushDue()]);
  assert.equal(f.store.listDeliveries()[0].attempts, 2);
  assert.equal(f.store.listDeliveries()[0].state, 'received');
  f.publish('evt_revoked'); authorized = false;
  const count = f.calls.length;
  await f.delivery.flushDue();
  assert.equal(f.calls.length, count);
  assert.equal(f.store.activeSubscriptions().length, 0);
  assert.equal(f.store.listDeliveries({ eventId: 'evt_revoked' })[0].state, 'cancelled');
});
