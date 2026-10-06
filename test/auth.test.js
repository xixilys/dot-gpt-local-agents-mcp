import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LocalAgentsOAuthProvider, createGatewayApp, SCOPE } from '../src/server.js';

test('OAuth PKCE and owner consent, bearer scope and exact resource are enforced on the actual HTTP path', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'local-agents-auth-test-'));
  const resource = new URL('https://gateway.example.com/mcp');
  const ownerToken = randomBytes(32).toString('base64url');
  const provider = new LocalAgentsOAuthProvider({ ownerToken, scopes: [SCOPE], allowedRedirectHosts: ['chatgpt.com'], accessTokenTtlSeconds: 60, refreshTokenTtlSeconds: 600 }, resource, dir);
  const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
  const runtime = await createGatewayApp({ stateDir: dir, publicBaseUrl: resource.origin, allowedRoots: [dir] }, {
    oauthProvider: provider, upstream: { async tools() { return tools; } },
  });
  const listener = await new Promise(resolve => { const server = runtime.app.listen(0, '127.0.0.1', () => resolve(server)); });
  t.after(async () => { await new Promise(resolve => listener.close(resolve)); runtime.close(); await rm(dir, { recursive: true, force: true }); });
  const origin = `http://127.0.0.1:${listener.address().port}`;
  const postForm = (path, values) => fetch(origin + path, { method: 'POST', body: new URLSearchParams(values), redirect: 'manual' });
  const registration = await fetch(origin + '/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ client_name: 'Acceptance client', redirect_uris: ['http://127.0.0.1:9012/callback'], token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] }) });
  assert.equal(registration.status, 201);
  const client = await registration.json();
  const verifier = randomBytes(32).toString('base64url');
  const challenge = createHash('sha256').update(verifier).digest('base64url');
  const params = { client_id: client.client_id, redirect_uri: client.redirect_uris[0], response_type: 'code', code_challenge: challenge, code_challenge_method: 'S256', resource: resource.href, scope: SCOPE };
  const consent = await fetch(origin + '/authorize?' + new URLSearchParams(params), { redirect: 'manual' });
  assert.equal(consent.status, 200);
  const html = await consent.text();
  assert.ok(html.includes('Connect Local Agents'));
  assert.ok(!html.includes(ownerToken));
  assert.equal((await postForm('/authorize', { ...params, owner_token: 'incorrect' })).status, 401);
  const accepted = await postForm('/authorize', { ...params, owner_token: ownerToken });
  assert.equal(accepted.status, 302);
  const code = new URL(accepted.headers.get('location')).searchParams.get('code');
  const tokenArgs = { client_id: client.client_id, grant_type: 'authorization_code', code, code_verifier: verifier, redirect_uri: client.redirect_uris[0], resource: resource.href };
  assert.equal((await postForm('/token', { ...tokenArgs, code_verifier: 'wrong-verifier' })).status, 400);
  const exchange = await postForm('/token', tokenArgs);
  assert.equal(exchange.status, 200);
  const tokens = await exchange.json();
  assert.equal((await postForm('/token', tokenArgs)).status, 400); // one-time code
  const initialize = { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'test', version: '1' } } };
  const mcp = token => fetch(origin + '/mcp', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', ...(token ? { Authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(initialize) });
  assert.equal((await mcp()).status, 401);
  assert.equal((await mcp(tokens.access_token)).status, 200);
  const wrongScope = provider.issueTokens(client.client_id, [], resource);
  assert.equal((await mcp(wrongScope.access_token)).status, 403);
  const wrongResource = provider.issueTokens(client.client_id, [SCOPE], new URL('https://gateway.example.com/mcp/other'));
  assert.equal((await mcp(wrongResource.access_token)).status, 401);
  assert.equal((await postForm('/token', { client_id: client.client_id, grant_type: 'refresh_token', refresh_token: tokens.refresh_token, resource: 'https://other.example/mcp' })).status, 400);
  const resourceDenied = await postForm('/authorize', { ...params, resource: 'https://gateway.example.com/mcp/other', owner_token: ownerToken });
  assert.equal(new URL(resourceDenied.headers.get('location')).searchParams.get('error'), 'invalid_request');
});
