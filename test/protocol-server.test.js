import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createModernHandler, isModernRequest, MODERN_PROTOCOL_VERSION } from '../src/protocol-server.js';
import { CallbackEndpointError, EventDelivery } from '../src/event-delivery.js';

const protocolKey = 'io.modelcontextprotocol/protocolVersion';
const capabilitiesKey = 'io.modelcontextprotocol/clientCapabilities';
const serverInfoKey = 'io.modelcontextprotocol/serverInfo';
const serverInfo = { name: 'test-modern-gateway', version: '1.2.3' };
const event = { name: 'agent.message', delivery: ['webhook'], inputSchema: { type: 'object' }, payloadSchema: { type: 'object' } };
const params = { name: event.name, arguments: { workspaceId: 'workspace-a' }, delivery: {
  mode: 'webhook', url: 'https://receiver.example/callback', secret: 'whsec_example',
}, cursor: null };

async function fixture(t, { owner = 'authenticated-owner', eventService, parsedBody = false } = {}) {
  const calls = [];
  const gateway = {
    async refreshTools() { calls.push(['tools/list']); return [{ name: 'echo', inputSchema: { type: 'object' } }]; },
    async call(name, args, context) {
      calls.push(['tools/call', name, args, context]);
      if (name === 'fail') throw new Error('Tool rejected input');
      return { content: [{ type: 'text', text: args.text ?? 'ok' }] };
    },
  };
  const events = eventService ?? {
    async list(who, args) { calls.push(['events/list', who, args]); return { events: [event] }; },
    async subscribe(who, args) { calls.push(['events/subscribe', who, args]); return { id: 'sub_test', refreshBefore: null, cursor: null, truncated: false }; },
    async unsubscribe(who, args) { calls.push(['events/unsubscribe', who, args]); return {}; },
  };
  const handler = createModernHandler({ gateway, eventService: events, serverInfo });
  const server = createServer(async (req, res) => {
    req.auth = { token: 'middleware-verified', clientId: 'rotating-oauth-client', scopes: ['local-agents'] };
    req.mcpOwner = typeof owner === 'function' ? owner(req) : owner;
    if (parsedBody) {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      req.body = JSON.parse(Buffer.concat(chunks).toString());
    }
    void handler(req, res);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { await handler.close(); await new Promise(resolve => server.close(resolve)); });
  const url = `http://127.0.0.1:${server.address().port}/mcp`;
  async function request(method, businessParams = {}, { headers = {}, body, version = MODERN_PROTOCOL_VERSION, id = 7, path = '/mcp' } = {}) {
    const message = body ?? { jsonrpc: '2.0', id, method, params: {
      ...businessParams, _meta: { [protocolKey]: version, [capabilitiesKey]: {} },
    } };
    const standardHeaders = { 'content-type': 'application/json', 'mcp-protocol-version': version, 'mcp-method': method };
    if (method === 'tools/call') standardHeaders['mcp-name'] = businessParams.name;
    for (const [key, value] of Object.entries(headers)) {
      if (value === null) delete standardHeaders[key]; else standardHeaders[key] = value;
    }
    const response = await fetch(new URL(path, url), { method: 'POST', headers: standardHeaders, body: typeof message === 'string' ? message : JSON.stringify(message) });
    return { status: response.status, headers: response.headers, body: await response.json() };
  }
  return { request, calls, handler };
}

test('modern request classification preserves legacy tool requests', () => {
  assert.equal(isModernRequest({ headers: {}, body: { method: 'tools/list' } }), false);
  assert.equal(isModernRequest({ headers: {}, body: { method: 'initialize' } }), false);
  assert.equal(isModernRequest({ headers: {}, body: { method: { startsWith: 'invalid' } } }), false);
  assert.equal(isModernRequest({ headers: {}, body: { method: 'server/discover' } }), true);
  assert.equal(isModernRequest({ headers: {}, body: { method: 'events/list' } }), true);
  assert.equal(isModernRequest({ headers: { 'mcp-protocol-version': MODERN_PROTOCOL_VERSION }, body: { method: 'tools/list' } }), true);
  assert.equal(isModernRequest({ headers: {}, body: { params: { _meta: { [protocolKey]: MODERN_PROTOCOL_VERSION } } } }), true);
  assert.equal(isModernRequest({ headers: {}, body: { params: { _meta: { [protocolKey]: null } } } }), true);
});

test('official HTTP entry advertises events and tools with modern response identity', async t => {
  const f = await fixture(t);
  const { status, body } = await f.request('server/discover');
  assert.equal(status, 200);
  assert.deepEqual(body.result.capabilities, { tools: {}, events: {} });
  assert.deepEqual(body.result.supportedVersions, [MODERN_PROTOCOL_VERSION]);
  assert.equal(body.result.resultType, 'complete');
  assert.deepEqual(body.result._meta[serverInfoKey], serverInfo);
  assert.equal(body.id, 7);
});

test('HTTP event lifecycle passes only middleware owner and business params', async t => {
  const f = await fixture(t);
  const listed = await f.request('events/list', {}, { headers: { 'x-owner': 'forged-owner' } });
  assert.equal(listed.status, 200);
  assert.deepEqual(listed.body.result.events, [event]);
  assert.deepEqual(listed.body.result._meta[serverInfoKey], serverInfo);
  const subscribed = await f.request('events/subscribe', params);
  assert.equal(subscribed.status, 200);
  assert.equal(subscribed.body.result.id, 'sub_test');
  const unsubscribe = { ...params, delivery: { mode: 'webhook', url: params.delivery.url } };
  delete unsubscribe.cursor;
  const stopped = await f.request('events/unsubscribe', unsubscribe);
  assert.equal(stopped.status, 200);
  assert.deepEqual(stopped.body.result._meta[serverInfoKey], serverInfo);
  assert.deepEqual(f.calls, [['events/list', 'authenticated-owner', {}], ['events/subscribe', 'authenticated-owner', params], ['events/unsubscribe', 'authenticated-owner', unsubscribe]]);
});

test('pre-parsed Express-style bodies are passed to the official entry', async t => {
  const f = await fixture(t, { parsedBody: true });
  const response = await f.request('events/subscribe', params);
  assert.equal(response.status, 200);
  assert.equal(response.body.result.id, 'sub_test');
  assert.deepEqual(f.calls, [['events/subscribe', 'authenticated-owner', params]]);
});

test('tools use the same gateway and modern complete/error result shapes', async t => {
  const f = await fixture(t);
  assert.equal((await f.request('tools/list')).body.result.tools[0].name, 'echo');
  const success = await f.request('tools/call', { name: 'echo', arguments: { text: 'actual reply' } });
  assert.equal(success.body.result.resultType, 'complete');
  assert.equal(success.body.result.content[0].text, 'actual reply');
  assert.deepEqual(success.body.result._meta[serverInfoKey], serverInfo);
  const failure = await f.request('tools/call', { name: 'fail' });
  assert.equal(failure.status, 200);
  assert.equal(failure.body.result.resultType, 'complete');
  assert.equal(failure.body.result.isError, true);
  assert.equal(failure.body.result.content[0].text, 'Tool rejected input');
  assert.deepEqual(f.calls, [
    ['tools/list'],
    ['tools/call', 'echo', { text: 'actual reply' }, { owner: 'authenticated-owner' }],
    ['tools/call', 'fail', {}, { owner: 'authenticated-owner' }],
  ]);
});

test('unsupported revision and header/body mismatches fail before gateway dispatch', async t => {
  const f = await fixture(t);
  const version = await f.request('tools/list', {}, { version: '2026-12-01' });
  assert.equal(version.status, 400);
  assert.equal(version.body.error.code, -32022);
  for (const headers of [
    { 'mcp-protocol-version': '2025-11-25' },
    { 'mcp-method': 'events/list' },
    { 'mcp-method': null },
    { 'mcp-protocol-version': null },
  ]) {
    const response = await f.request('tools/list', {}, { headers });
    assert.equal(response.status, 400);
    assert.equal(response.body.error.code, -32020);
    assert.equal(response.body.id, 7);
  }
  for (const name of ['different', null, '=?base64?invalid?=']) {
    const response = await f.request('tools/call', { name: 'echo' }, { headers: { 'mcp-name': name } });
    assert.equal(response.status, 400);
    assert.equal(response.body.error.code, -32020);
  }
  assert.deepEqual(f.calls, []);
});

test('malformed envelope, unknown method and malformed JSON get standard errors', async t => {
  const f = await fixture(t);
  const missing = await f.request('events/list', {}, { body: { jsonrpc: '2.0', id: 2, method: 'events/list', params: { _meta: { [protocolKey]: MODERN_PROTOCOL_VERSION } } } });
  assert.equal(missing.status, 400);
  assert.equal(missing.body.error.code, -32602);
  const unknown = await f.request('unknown/method');
  assert.equal(unknown.body.error.code, -32601);
  assert.equal(unknown.body.id, 7);
  const malformed = await f.request('events/list', {}, { body: '{' });
  assert.equal(malformed.status, 400);
  assert.equal(malformed.body.error.code, -32700);
  assert.equal(malformed.body.id, null);
});

test('event method schemas reject invalid params and owner injection before business handlers', async t => {
  const f = await fixture(t);
  for (const invalid of [
    { ...params, owner: 'forged' },
    { ...params, delivery: { ...params.delivery, mode: 'stream' } },
    { ...params, delivery: { ...params.delivery, url: 'invalid-url' } },
    { ...params, ttlMs: -1 },
    { ...params, arguments: [] },
  ]) {
    const response = await f.request('events/subscribe', invalid);
    assert.equal(response.body.error.code, -32602);
  }
  assert.deepEqual(f.calls, []);
});

test('event owner cannot be supplied by client headers when middleware omitted it', async t => {
  const f = await fixture(t, { owner: null });
  const response = await f.request('events/list', {}, { headers: { 'x-owner': 'forged', 'mcp-owner': 'forged' } });
  assert.equal(response.body.error.code, -32603);
  assert.deepEqual(f.calls, []);
});

test('concurrent owner requests stay isolated', async t => {
  const seen = [];
  const eventService = { async list(owner) { await new Promise(resolve => setTimeout(resolve, 5)); seen.push(owner); return { events: [] }; } };
  const f = await fixture(t, { owner: req => req.url === '/owner-a' ? 'owner-a' : 'owner-b', eventService });
  const results = await Promise.all([f.request('events/list', {}, { path: '/owner-a' }), f.request('events/list', {}, { path: '/owner-b' })]);
  assert.deepEqual(results.map(r => r.status), [200, 200]);
  assert.deepEqual(seen.sort(), ['owner-a', 'owner-b']);
});

test('callback verification failures preserve the protocol category and reason', async t => {
  const f = await fixture(t, { eventService: {
    async subscribe() { throw new CallbackEndpointError('challenge_failed'); },
  } });
  const response = await f.request('events/subscribe', params);
  assert.equal(response.body.error.code, -32015);
  assert.deepEqual(response.body.error.data, { reason: 'challenge_failed' });
  assert.equal(response.body.id, 7);
  assert.equal(JSON.stringify(response.body).includes(params.delivery.secret), false);
});

test('a failed actual EventDelivery challenge reaches the HTTP client as -32015', async t => {
  let attemptedVerification = false;
  let saved = false;
  const delivery = new EventDelivery({
    store: { isVerified: () => false, saveSubscription: () => { saved = true; } },
    callbackClient: { async post(_url, { body, headers }) {
      const payload = JSON.parse(body);
      assert.equal(payload.type, 'verification');
      assert.equal(typeof payload.challenge, 'string');
      assert.equal(typeof headers['webhook-signature'], 'string');
      attemptedVerification = true;
      return { status: 200, body: JSON.stringify({ challenge: 'wrong challenge' }) };
    } },
  });
  const f = await fixture(t, { eventService: { async subscribe(owner, input) {
    return delivery.subscribe(input.name, input.arguments, input.delivery, input.ttlMs, input.cursor, owner);
  } } });
  const secret = `whsec_${Buffer.alloc(32, 5).toString('base64')}`;
  const response = await f.request('events/subscribe', { ...params, delivery: { ...params.delivery, secret } });
  assert.equal(attemptedVerification, true);
  assert.equal(saved, false);
  assert.equal(response.body.error.code, -32015);
  assert.deepEqual(response.body.error.data, { reason: 'challenge_failed' });
  assert.equal(JSON.stringify(response.body).includes(secret), false);
});
