import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes, randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { Webhook } from 'standardwebhooks';
import { createGatewayApp, LocalAgentsOAuthProvider, SCOPE } from '../src/server.js';
import { ATTENTION_EVENT } from '../src/collaboration.js';
import { runDotMessage } from '../bin/dot-message.mjs';

test('same authenticated MCP endpoint runs modern events and legacy tools, routes a Unix message, and revocation stops delivery', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'local-agent-events-app-'));
  const cwd = join(dir, 'workspace'); await mkdir(cwd);
  const resource = new URL('https://gateway.example.com/mcp');
  const provider = new LocalAgentsOAuthProvider({ ownerToken: randomBytes(32).toString('hex'), scopes: [SCOPE],
    allowedRedirectHosts: ['chatgpt.com'], accessTokenTtlSeconds: 60, refreshTokenTtlSeconds: 600 }, resource, dir);
  const client = provider.clientsStore.registerClient({ client_name: 'app test', redirect_uris: ['http://127.0.0.1:9012/callback'], token_endpoint_auth_method: 'none', grant_types: ['authorization_code','refresh_token'], response_types: ['code'] });
  const token = provider.issueTokens(client.client_id, [SCOPE], resource);
  const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
  const agentId = randomUUID();
  const snapshot = { id: agentId, workspaceId: 'wks_app_test', cwd, status: 'running', activeTurn: { turnId: 'turn-app' }, pendingPermissions: [] };
  let prompt;
  const callbacks = [];
  const secret = 'whsec_' + randomBytes(32).toString('base64');
  const runtime = await createGatewayApp({ stateDir: dir, allowedRoots: [cwd], publicBaseUrl: resource.origin }, {
    oauthProvider: provider,
    upstream: { async tools() { return tools; }, async call(name, args) {
      if (name === 'list_workspaces') return { structuredContent: { workspaces: [{ workspaceId: 'wks_app_test', cwd }] } };
      if (name === 'get_agent_status') return { structuredContent: { snapshot: { ...snapshot } } };
      if (name === 'create_agent') { prompt = args.initialPrompt; return { structuredContent: { agentId } }; }
      throw new Error('Unexpected upstream operation');
    } },
    observerFactory: () => ({ watchAgentIds() {}, start() {}, async close() {} }),
    callbackClient: { async post(url, request) {
      const data = new Webhook(secret).verify(request.body, request.headers);
      if (data.type === 'verification') return { status: 200, body: JSON.stringify({ challenge: data.challenge }) };
      callbacks.push({ url, data }); return { status: 202, body: '{}' };
    } },
  });
  runtime.gateway.timeline = async ({ limit }) => {
    const entries = prompt ? [{ item: { type: 'user_message', text: prompt }, turnId: 'turn-app', seqStart: 1, seqEnd: 1,
      timestamp: new Date(Date.now() + 2).toISOString() }] : [];
    return { agentId, agent: { ...snapshot }, entries: limit === 1 ? entries.slice(-1) : entries,
      epoch: 'app-epoch', startCursor: { epoch: 'app-epoch', seq: 1 }, endCursor: { epoch: 'app-epoch', seq: 1 },
      hasOlder: false, hasNewer: false, gap: false, staleCursor: false, reset: false };
  };
  const server = await new Promise(resolve => { const s = runtime.app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(async () => { await new Promise(resolve => server.close(resolve)); await runtime.close(); await rm(dir, { recursive: true, force: true }); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  async function rpc(method, params = {}, modern = true, access = token.access_token) {
    const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream', Authorization: 'Bearer ' + access };
    if (modern) { headers['mcp-protocol-version'] = '2026-07-28'; headers['mcp-method'] = method; }
    if (modern && method === 'tools/call') headers['mcp-name'] = params.name;
    const body = { jsonrpc: '2.0', id: 1, method, params: { ...params, ...(modern ? { _meta: {
      'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': {},
    } } : {}) } };
    const response = await fetch(origin + '/mcp', { method: 'POST', headers, body: JSON.stringify(body) });
    const data = await response.json(); return { status: response.status, ...data };
  }
  assert.equal((await rpc('server/discover')).result.capabilities.events instanceof Object, true);
  const catalog = (await rpc('tools/list')).result.tools;
  assert.equal(catalog.length, 23);
  assert.ok(catalog.some(tool => tool.name === 'get_request_result'));
  assert.equal((await rpc('events/list')).result.events[0].name, ATTENTION_EVENT);
  const created = await rpc('tools/call', { name: 'create_agent', arguments: { requestId: 'app-create',
    workspaceId: 'wks_app_test', title: 'app test', provider: 'codex/test', initialPrompt: 'Only the bounded fixture.' } });
  assert.equal(created.result.structuredContent.state, 'submitted');
  assert.equal(runtime.collaboration.store.get('app-create').subscriptionId, null);
  const subscribed = await rpc('events/subscribe', { name: ATTENTION_EVENT,
    arguments: { workspaceId: 'wks_app_test', routeId: 'app-dot-route' },
    delivery: { mode: 'webhook', url: 'https://chatgpt.example/callback', secret } });
  assert.ok(subscribed.result.id.startsWith('sub_'));
  assert.equal(catalog.find(tool => tool.name === 'watch_dispatch_request').annotations.readOnlyHint, false);
  const watched = await rpc('tools/call', { name: 'watch_dispatch_request', arguments: { requestId: 'app-create', notificationRouteId: 'app-dot-route' } });
  assert.equal(watched.result.structuredContent.status, 'bound');
  assert.equal(runtime.collaboration.store.get('app-create').subscriptionId, subscribed.result.id);
  const replay = await rpc('tools/call', { name: 'watch_dispatch_request', arguments: { requestId: 'app-create', notificationRouteId: 'app-dot-route' } });
  assert.equal(replay.result.structuredContent.status, 'bound');
  const messageId = randomUUID(); let output = '', error = '';
  const rc = await runDotMessage(['--request-id','app-create','--kind','message','--message-id',messageId], {
    env: { PASEO_AGENT_ID: agentId }, stateDir: dir, stdin: Readable.from(['Actual bounded agent message.']),
    stdout: { write(text) { output += text; } }, stderr: { write(text) { error += text; } },
  });
  assert.equal(rc, 0, error);
  assert.equal(JSON.parse(output).status, 'accepted');
  await runtime.delivery.flushDue();
  assert.equal(callbacks.length, 1);
  assert.equal(callbacks[0].data.data.messageId, messageId);
  const fetched = await rpc('tools/call', { name: 'get_agent_message', arguments: { messageId } });
  assert.equal(fetched.result.structuredContent.text, 'Actual bounded agent message.');
  const legacy = await rpc('tools/call', { name: 'get_dispatch_request', arguments: { requestId: 'app-create' } }, false);
  assert.equal(legacy.result.structuredContent.agentId, agentId);
  const call = runtime.gateway.call;
  let rejectedCalls = 0;
  runtime.gateway.call = async () => { rejectedCalls++; throw undefined; };
  try {
    const rejected = await rpc('tools/call', { name: 'get_dispatch_request', arguments: { requestId: 'app-create' } }, false);
    assert.equal(rejected.status, 200);
    assert.equal(rejected.result.isError, true);
    assert.equal(rejected.result.content[0].text, 'Tool execution failed');
    assert.equal(rejectedCalls, 1);
  } finally { runtime.gateway.call = call; }
  const otherClient = provider.clientsStore.registerClient({ client_name: 'other test', redirect_uris: ['http://127.0.0.1:9012/callback'], token_endpoint_auth_method: 'none', grant_types: ['authorization_code','refresh_token'], response_types: ['code'] });
  const other = provider.issueTokens(otherClient.client_id, [SCOPE], resource);
  const denied = await rpc('tools/call', { name: 'get_agent_message', arguments: { messageId } }, true, other.access_token);
  assert.equal(denied.result.isError, true);
  await provider.revokeToken(client, { token: token.access_token });
  assert.equal(runtime.eventStore.activeSubscriptions().length, 0);
  assert.equal((await rpc('tools/list')).status, 401);
});
