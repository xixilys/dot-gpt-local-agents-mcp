import { tmpdir } from 'node:os';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { Webhook } from 'standardwebhooks';
import { createGatewayApp, LocalAgentsOAuthProvider, SCOPE } from '../src/server.js';
import { createAgentChannel, readAgentCapability } from '../src/agent-channel.js';
import { PathPolicy } from '../src/gateway.js';
import { ATTENTION_EVENT } from '../src/collaboration.js';
import { subscriptionId } from '../src/event-store.js';
import { runDotMessage } from '../bin/dot-message.mjs';

const agentId = '11111111-1111-4111-8111-111111111111';
const messageId = '22222222-2222-4222-8222-222222222222';
const workspaceId = 'wks_same';
const requestId = 'same-request';
const routeId = 'same-route';
const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
const observer = () => ({ watchAgentIds() {}, start() {}, async close() {} });
const createArgs = { requestId, workspaceId, title: 'bounded fixture', provider: 'codex/test', initialPrompt: 'Bounded task.' };

async function fixture(t, { initiallyOffline = false, remoteTools = tools, legacyOnly = false } = {}) {
  // Keep the actual Unix socket path below macOS's pathname limit.
  const dir = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'mh-'));
  const stateDir = join(dir, 'state');
  const callbacks = [];
  const secret = 'whsec_' + randomBytes(32).toString('base64');
  const resource = new URL('https://gateway.example.com/mcp');
  const states = {};
  for (const id of ['mac', 'wsl']) {
    const cwd = join(dir, id); await mkdir(cwd);
    const state = states[id] = { id, cwd, online: id !== 'wsl' || !initiallyOffline,
      serverId: 'srv_test_wsl', calls: [], entries: [], sequence: 0,
      snapshot: { id: agentId, workspaceId, cwd, status: 'idle', activeTurn: null, pendingPermissions: [] } };
    state.upstream = {
      async tools() { if (!state.online) throw Error('fixture offline'); return id === 'wsl' ? remoteTools : tools; },
      async call(name, args) {
        if (!state.online) throw Error('fixture offline');
        state.calls.push({ name, args: structuredClone(args) });
        if (name === 'list_workspaces') return { structuredContent: { workspaces: [{ workspaceId, cwd }] } };
        if (name === 'get_agent_status') return { structuredContent: { snapshot: structuredClone(state.snapshot) } };
        if (name === 'list_agents') return { structuredContent: { agents: [structuredClone(state.snapshot)] } };
        if (name === 'list_profiles') return { structuredContent: { profiles: [id] } };
        if (name === 'respond_to_permission') return { structuredContent: { acknowledgedBy: id } };
        if (name === 'create_agent' || name === 'send_agent_prompt') {
          state.snapshot.status = 'running'; state.snapshot.activeTurn = { turnId: `turn-${++state.sequence}` };
          state.entries.push({ item: { type: 'user_message', text: args.initialPrompt ?? args.prompt },
            turnId: state.snapshot.activeTurn.turnId, seqStart: state.sequence, seqEnd: state.sequence,
            timestamp: new Date(Date.now() + 1000).toISOString() });
          return { structuredContent: { agentId, acceptedBy: id } };
        }
        throw Error(`Unexpected fixture call: ${name}`);
      },
    };
    state.timeline = async ({ limit = 50 }) => {
      if (!state.online) throw Error('fixture offline');
      return { agentId, agent: structuredClone(state.snapshot), epoch: 'same-epoch',
        entries: structuredClone(state.entries.slice(-limit)),
        startCursor: { epoch: 'same-epoch', seq: state.entries[0]?.seqStart ?? 0 },
        endCursor: { epoch: 'same-epoch', seq: state.sequence },
        hasOlder: false, hasNewer: false, gap: false, staleCursor: false, reset: false };
    };
  }
  const remoteHost = { id: 'wsl', name: 'WSL', transport: 'ssh', target: 'ssh://example-host',
    allowedRoots: [states.wsl.cwd], remoteStateDir: '/home/test/.local/share/gateway' };
  const config = { stateDir, allowedRoots: [states.mac.cwd], publicBaseUrl: resource.origin,
    hosts: legacyOnly ? [] : [remoteHost] };
  let runtime, server, token, client, owner;
  const f = { dir, config, remoteHost, states, callbacks, secret, get runtime() { return runtime; },
    get owner() { return owner; }, get provider() { return runtime.provider; }, get token() { return token; }, get client() { return client; } };
  f.open = async () => {
    const provider = new LocalAgentsOAuthProvider({ ownerToken: 'fixture-owner-token-'.padEnd(64, 'x'), scopes: [SCOPE],
      allowedRedirectHosts: ['chatgpt.com'], accessTokenTtlSeconds: 60, refreshTokenTtlSeconds: 600 }, resource, stateDir);
    if (!client) {
      client = provider.clientsStore.registerClient({ client_name: 'two host fixture', redirect_uris: ['http://127.0.0.1:9012/callback'],
        token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] });
      token = provider.issueTokens(client.client_id, [SCOPE], resource);
      owner = `local-owner:${client.client_id}`;
    }
    const remote = states.wsl;
    runtime = await createGatewayApp(config, { upstream: states.mac.upstream, oauthProvider: provider, observerFactory: observer,
      callbackClient: { async post(url, request) {
        const data = new Webhook(secret).verify(request.body, request.headers);
        if (data.type === 'verification') return { status: 200, body: JSON.stringify({ challenge: data.challenge }) };
        callbacks.push({ url, data, headers: request.headers }); return { status: 202, body: '{}' };
      } },
      hostAdapterFactory: host => ({
        upstream: remote.upstream,
        paths: { async check(path) {
          if (!remote.online) throw Error('Remote path could not be verified');
          return new PathPolicy(host.allowedRoots).check(path);
        } },
        readDaemon: async () => ({ projects: [] }), readTimeline: remote.timeline,
        observerFactory: observer, channelFactory: options => createAgentChannel(options),
        async status() { return { available: remote.online, ...(remote.online ? { serverId: remote.serverId } : {}) }; },
        async close() {},
      }),
    });
    runtime.hosts.get('mac').gateway.readTimeline = states.mac.timeline;
    server = await new Promise((resolve, reject) => {
      const listener = runtime.app.listen(0, '127.0.0.1', () => resolve(listener)); listener.once('error', reject);
    });
    f.url = `http://127.0.0.1:${server.address().port}/mcp`;
  };
  f.close = async () => {
    if (server) { await new Promise(resolve => server.close(resolve)); server = undefined; }
    if (runtime) { await runtime.close(); runtime = undefined; }
  };
  t.after(async () => { await f.close(); await rm(dir, { recursive: true, force: true }); });
  f.rpc = async (method, params = {}, { legacy = false, access = token.access_token, headers = {} } = {}) => {
    const requestHeaders = { 'content-type': 'application/json', accept: 'application/json, text/event-stream', Authorization: 'Bearer ' + access, ...headers };
    if (!legacy) {
      requestHeaders['mcp-protocol-version'] = '2026-07-28'; requestHeaders['mcp-method'] = method;
      if (method === 'tools/call') requestHeaders['mcp-name'] = params.name;
    }
    const response = await fetch(f.url, { method: 'POST', headers: requestHeaders,
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params: { ...params, ...(!legacy ? { _meta: {
        'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {},
      } } : {}) } }) });
    return { status: response.status, ...await response.json() };
  };
  f.call = async (name, args = {}, options) => (await f.rpc('tools/call', { name, arguments: args }, options)).result;
  f.subscribe = async hostId => {
    const response = await f.rpc('events/subscribe', { name: ATTENTION_EVENT,
      arguments: { workspaceId, routeId, ...(hostId ? { hostId } : {}) },
      delivery: { mode: 'webhook', url: 'https://chatgpt.example/callback', secret } });
    assert.equal(response.error, undefined); return response.result;
  };
  f.message = async (hostId, text) => {
    let out = '', error = '';
    const code = await runDotMessage(['--request-id', requestId, '--message-id', messageId], {
      stateDir: runtime.hosts.get(hostId).host.stateDir, env: { PASEO_AGENT_ID: agentId }, stdin: Readable.from([text]),
      stdout: { write(value) { out += value; } }, stderr: { write(value) { error += value; } },
    });
    assert.equal(code, 0, error); return JSON.parse(out);
  };
  f.finish = async (hostId, text) => {
    const state = states[hostId], turnId = state.snapshot.activeTurn.turnId;
    state.entries.push({ item: { type: 'assistant_message', text }, turnId, seqStart: ++state.sequence,
      seqEnd: state.sequence, timestamp: new Date(Date.now() + 1000).toISOString() });
    state.snapshot.status = 'idle'; state.snapshot.activeTurn = null;
    await runtime.hosts.get(hostId).collaboration.reconcile(agentId, { type: 'turn_completed', turnId, timestamp: new Date().toISOString() });
  };
  await f.open();
  return f;
}

function successful(result) { assert.notEqual(result?.isError, true, JSON.stringify(result)); return result.structuredContent; }
function rejected(result, pattern) { assert.equal(result?.isError, true); assert.match(result.content[0].text, pattern); }
function mutations(state) { return state.calls.filter(c => ['create_agent', 'send_agent_prompt', 'respond_to_permission'].includes(c.name)); }

test('HTTP routes identical host IDs independently: dispatch, retries, receipts, timelines and permission response', async t => {
  const f = await fixture(t);
  const listed = successful(await f.call('list_hosts'));
  assert.deepEqual(listed.hosts.map(h => [h.hostId, h.available]), [['mac', true], ['wsl', true]]);
  const catalog = (await f.rpc('tools/list')).result.tools;
  assert.deepEqual(catalog.find(t => t.name === 'create_agent').inputSchema.properties.hostId.enum, ['mac', 'wsl']);
  for (const hostId of ['mac', 'wsl']) {
    const args = { ...createArgs, ...(hostId === 'wsl' ? { hostId } : {}) };
    const receipt = successful(await f.call('create_agent', args));
    assert.equal(receipt.hostId, hostId); assert.equal(receipt.upstreamResult.structuredContent.acceptedBy, hostId);
    const retry = successful(await f.call('create_agent', { ...args, hostId }));
    assert.equal(retry.duplicate, true); assert.equal(retry.submittedAt, receipt.submittedAt);
    const saved = successful(await f.call('get_dispatch_request', { requestId, hostId }, { legacy: true }));
    assert.equal(saved.hostId, hostId); assert.equal(saved.upstreamResult.structuredContent.acceptedBy, hostId);
    await f.finish(hostId, `Result from ${hostId}`);
    const timeline = successful(await f.call('get_agent_result', { hostId, agentId }));
    assert.equal(timeline.hostId, hostId); assert.equal(timeline.agent.cwd, f.states[hostId].cwd);
    assert.equal(timeline.entries.at(-1).item.text, `Result from ${hostId}`);
  }
  assert.deepEqual(['mac', 'wsl'].map(id => mutations(f.states[id]).length), [1, 1]);
  successful(await f.call('respond_to_permission', { hostId: 'wsl', agentId, requestId: 'same-permission', response: { behavior: 'deny' } }));
  assert.equal(mutations(f.states.mac).length, 1);
  assert.equal(mutations(f.states.wsl).at(-1).name, 'respond_to_permission');
  const macLists = successful(await f.call('list_agents', {}, { headers: { 'x-host-id': 'wsl', 'x-owner': 'forged' } }));
  assert.equal(macLists.hostId, 'mac'); assert.equal(macLists.agents[0].cwd, f.states.mac.cwd);
});

test('same workspace/route/message uses distinct signed callbacks and host-private IPC capabilities', async t => {
  const f = await fixture(t);
  const mac = await f.subscribe(), explicitMac = await f.subscribe('mac'), remote = await f.subscribe('wsl');
  assert.equal(mac.id, explicitMac.id);
  assert.equal(mac.id, subscriptionId(f.owner, 'https://chatgpt.example/callback', ATTENTION_EVENT, { workspaceId, routeId }));
  assert.notEqual(remote.id, mac.id);
  assert.deepEqual(f.runtime.hosts.get('mac').eventStore.activeSubscriptions()[0].arguments, { routeId, workspaceId });
  assert.deepEqual(f.runtime.hosts.get('wsl').eventStore.activeSubscriptions()[0].arguments, { hostId: 'wsl', routeId, workspaceId });
  for (const id of ['mac', 'wsl']) successful(await f.call('create_agent', { ...createArgs, hostId: id, notificationRouteId: routeId }));
  const macKey = await readAgentCapability(f.runtime.hosts.get('mac').host.stateDir, agentId);
  const remoteKey = await readAgentCapability(f.runtime.hosts.get('wsl').host.stateDir, agentId);
  assert.notEqual(macKey, remoteKey);
  const spoof = await new Promise((resolve, reject) => {
    const req = http.request({ socketPath: join(f.runtime.hosts.get('wsl').host.stateDir, 'agent-channel.sock'),
      method: 'POST', path: '/message', headers: { authorization: 'Bearer ' + macKey, 'content-type': 'application/json' } }, res => {
      res.resume(); res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject); req.end(JSON.stringify({ agentId, requestId, messageId, kind: 'message', text: 'Cross-host forged message' }));
  });
  assert.equal(spoof, 403);
  assert.equal((await f.message('mac', 'Mac actual message')).status, 'accepted');
  assert.equal((await f.message('wsl', 'WSL actual message')).status, 'accepted');
  assert.equal((await f.message('wsl', 'WSL actual message')).status, 'duplicate');
  for (const context of f.runtime.hosts.values()) await context.delivery.flushDue();
  assert.equal(f.callbacks.length, 2);
  const byHost = new Map(f.callbacks.map(c => [c.data.data.hostId ?? 'mac', c]));
  assert.equal(byHost.get('mac').data.data.summary, 'Mac actual message');
  assert.equal(byHost.get('wsl').data.data.summary, 'WSL actual message');
  assert.notEqual(byHost.get('mac').data.eventId, byHost.get('wsl').data.eventId);
  assert.equal(byHost.get('mac').headers['X-MCP-Subscription-Id'], mac.id);
  assert.equal(byHost.get('wsl').headers['X-MCP-Subscription-Id'], remote.id);
  for (const hostId of ['mac', 'wsl']) {
    const message = successful(await f.call('get_agent_message', { hostId, messageId }));
    assert.equal(message.text, hostId === 'mac' ? 'Mac actual message' : 'WSL actual message');
    rejected(await f.call('get_agent_message', { hostId, messageId }, { access: f.provider.issueTokens(
      f.provider.clientsStore.registerClient({ client_name: 'other', redirect_uris: ['http://127.0.0.1:9012/callback'], token_endpoint_auth_method: 'none', grant_types: ['authorization_code'], response_types: ['code'] }).client_id,
      [SCOPE], new URL(f.config.publicBaseUrl + '/mcp')).access_token }), /another authenticated/);
  }
});

test('offline remote retains local receipt, route and message reads without fallback; Mac remains available', async t => {
  const f = await fixture(t); await f.subscribe('wsl');
  successful(await f.call('create_agent', { ...createArgs, hostId: 'wsl', notificationRouteId: routeId }));
  await f.message('wsl', 'Persisted WSL message');
  await f.finish('wsl', 'Saved WSL result');
  assert.equal(f.runtime.hosts.get('wsl').collaboration.store.get(requestId).result.lastMessage, 'Saved WSL result');
  successful(await f.call('send_agent_prompt', { hostId: 'wsl', agentId, requestId: 'pending-result', prompt: 'Still running' }));
  await f.runtime.hosts.get('wsl').collaboration.reconcile(agentId);
  f.states.wsl.online = false;
  const before = f.states.wsl.calls.length, macBefore = mutations(f.states.mac).length;
  assert.equal(successful(await f.call('get_dispatch_request', { hostId: 'wsl', requestId })).hostId, 'wsl');
  assert.equal(successful(await f.call('list_notification_routes', { hostId: 'wsl' })).routes[0].routeId, routeId);
  assert.equal(successful(await f.call('get_agent_message', { hostId: 'wsl', messageId })).text, 'Persisted WSL message');
  assert.equal(successful(await f.call('get_request_result', { hostId: 'wsl', requestId })).lastMessage, 'Saved WSL result');
  rejected(await f.call('get_request_result', { hostId: 'wsl', requestId: 'pending-result' }), /unavailable|unverified/);
  await assert.rejects(f.runtime.gateway.call('get_request_result', { hostId: 'wsl', requestId }, { owner: 'local-owner:wrong-client' }), /another authenticated/);
  rejected(await f.call('create_agent', { ...createArgs, hostId: 'wsl', requestId: 'offline-new' }), /unavailable|unverified/);
  rejected(await f.call('get_agent_result', { hostId: 'wsl', agentId }), /unavailable|unverified/);
  assert.equal(f.states.wsl.calls.length, before); assert.equal(mutations(f.states.mac).length, macBefore);
  assert.equal(successful(await f.call('list_workspaces')).hostId, 'mac');
  const hosts = successful(await f.call('list_hosts')).hosts;
  assert.deepEqual(hosts.map(h => h.available), [true, false]);
});

test('unknown host, URL injection, host impersonation and another host catalog inputs fail closed', async t => {
  const remoteTools = structuredClone(tools).filter(t => t.name !== 'list_profiles');
  remoteTools.find(t => t.name === 'create_agent').inputSchema.properties.provider = { const: 'remote/only' };
  const f = await fixture(t, { remoteTools });
  for (const hostId of ['unknown', 'ssh://example-host', 'https://attacker.example', null, { id: 'wsl' }]) {
    rejected(await f.call('list_workspaces', { hostId }), /Unknown hostId|Invalid tool input/);
  }
  rejected(await f.call('list_workspaces', { hostId: 'wsl', target: 'ssh://other-host' }), /Invalid tool input/);
  rejected(await f.call('list_profiles', { hostId: 'wsl' }), /not exposed/);
  rejected(await f.call('create_agent', { ...createArgs, hostId: 'wsl' }), /Invalid tool input/);
  successful(await f.call('create_agent', createArgs));
  assert.equal(mutations(f.states.wsl).length, 0); assert.equal(mutations(f.states.mac).length, 1);
  const badEvent = await f.rpc('events/subscribe', { name: ATTENTION_EVENT, owner: 'forged', arguments: { workspaceId, routeId, hostId: 'wsl' },
    delivery: { mode: 'webhook', url: 'https://chatgpt.example/callback', secret: f.secret } });
  assert.ok(badEvent.error); assert.equal(f.runtime.hosts.get('wsl').eventStore.activeSubscriptions().length, 0);
});

test('Mac legacy receipt fingerprint and subscription identity persist when reopening after adding WSL', async t => {
  const f = await fixture(t, { legacyOnly: true });
  assert.equal(f.runtime.hosts.size, 1);
  const original = await f.subscribe();
  const receipt = successful(await f.call('create_agent', { ...createArgs, notificationRouteId: routeId }, { legacy: true }));
  const fingerprint = f.runtime.hosts.get('mac').store.get(requestId).fingerprint;
  await f.close(); f.config.hosts = [f.remoteHost]; await f.open();
  assert.equal(f.runtime.hosts.size, 2);
  assert.equal(f.runtime.hosts.get('mac').store.get(requestId).fingerprint, fingerprint);
  assert.equal(f.runtime.hosts.get('mac').eventStore.activeSubscriptions()[0].id, original.id);
  assert.equal((await f.subscribe('mac')).id, original.id);
  const replay = successful(await f.call('create_agent', { ...createArgs, notificationRouteId: routeId, hostId: 'mac' }));
  assert.equal(replay.duplicate, true); assert.equal(replay.submittedAt, receipt.submittedAt);
  assert.equal(mutations(f.states.mac).length, 1); assert.equal(mutations(f.states.wsl).length, 0);
});

test('OAuth revocation fans out across host subscriptions and cancels queued callbacks', async t => {
  const f = await fixture(t);
  for (const hostId of ['mac', 'wsl']) {
    await f.subscribe(hostId);
    successful(await f.call('create_agent', { ...createArgs, hostId, notificationRouteId: routeId }));
    await f.message(hostId, `${hostId} message`);
  }
  await f.provider.revokeToken(f.client, { token: f.token.access_token });
  for (const context of f.runtime.hosts.values()) {
    assert.equal(context.eventStore.activeSubscriptions().length, 0);
    assert.equal(context.eventStore.listDeliveries()[0].state, 'cancelled');
    await context.delivery.flushDue();
  }
  assert.equal(f.callbacks.length, 0);
  assert.equal((await f.rpc('tools/list')).status, 401);
});

test('an initially offline WSL cannot take over Mac calls or receipt lookup', async t => {
  const f = await fixture(t, { initiallyOffline: true });
  successful(await f.call('create_agent', createArgs));
  assert.equal(successful(await f.call('get_dispatch_request', { requestId, hostId: 'wsl' })).found, false);
  rejected(await f.call('create_agent', { ...createArgs, hostId: 'wsl' }), /unavailable|unverified/);
  assert.equal(mutations(f.states.mac).length, 1); assert.equal(f.states.wsl.calls.length, 0);
});

test('changed daemon serverId and changed SSH target cannot inherit existing dispatch or event authority', async t => {
  const f = await fixture(t); await f.subscribe('wsl');
  successful(await f.call('create_agent', { ...createArgs, hostId: 'wsl', notificationRouteId: routeId }));
  await f.runtime.hosts.get('wsl').collaboration.reconcile(agentId);
  const before = f.states.wsl.calls.length;
  f.states.wsl.serverId = 'srv_replacement';
  rejected(await f.call('create_agent', { ...createArgs, hostId: 'wsl', requestId: 'replacement-request' }), /identity changed/);
  const sub = await f.rpc('events/subscribe', { name: ATTENTION_EVENT, arguments: { workspaceId, routeId: 'other-route', hostId: 'wsl' },
    delivery: { mode: 'webhook', url: 'https://chatgpt.example/callback', secret: f.secret } });
  assert.ok(sub.error); assert.equal(f.states.wsl.calls.length, before);
  assert.equal(successful(await f.call('get_dispatch_request', { hostId: 'wsl', requestId })).found, true);
  assert.equal(successful(await f.call('list_hosts')).hosts.find(h => h.hostId === 'wsl').available, false);
  await f.close();
  f.config.hosts[0].target = 'ssh://replacement-host';
  await assert.rejects(f.open(), /already bound to another daemon target/);
});
