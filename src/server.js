import { readFile, mkdir, chmod } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { createMcpExpressApp } from '@modelcontextprotocol/sdk/server/express.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { mcpAuthRouter, getOAuthProtectedResourceMetadataUrl } from '@modelcontextprotocol/sdk/server/auth/router.js';
import { requireBearerAuth } from '@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js';
import { InvalidRequestError } from '@modelcontextprotocol/sdk/server/auth/errors.js';
import { SingleUserOAuthProvider } from '@waishnav/devspace/dist/oauth-provider.js';
import { PaseoUpstream, UPSTREAM_URL } from './upstream.js';
import { DispatchStore } from './dispatch-store.js';
import { Gateway } from './gateway.js';
import { EventStore } from './event-store.js';
import { EventDelivery } from './event-delivery.js';
import { CollaborationStore } from './collaboration-store.js';
import { Collaboration } from './collaboration.js';
import { createAgentChannel } from './agent-channel.js';
import { ObserverClient } from './observer-client.js';
import { createModernHandler, isModernRequest } from './protocol-server.js';

export const SCOPE = 'local-agents';

export class LocalAgentsOAuthProvider extends SingleUserOAuthProvider {
  async revokeToken(client, request) {
    const hash = createHash('sha256').update(request.token).digest('base64url');
    const record = this.oauthStore.getAccessToken(hash) ?? this.oauthStore.getRefreshToken(hash);
    await super.revokeToken(client, request);
    if (record?.clientId) await this.onClientRevoked?.(record.clientId);
  }
  async authorize(client, params, res) {
    if (params.resource?.href !== this.resourceServerUrl.href) throw new InvalidRequestError('Invalid or missing OAuth resource');
    if (!params.scopes?.includes(SCOPE)) throw new InvalidRequestError('The local-agents scope is required');
    const send = res.send;
    res.send = function (body) {
      if (typeof body === 'string') body = body.replaceAll('Connect DevSpace', 'Connect Local Agents').replaceAll('Authorize DevSpace', 'Authorize Local Agents').replaceAll('DevSpace MCP endpoint', 'Local Agents MCP endpoint');
      return send.call(this, body);
    };
    try { return await super.authorize(client, params, res); }
    finally { res.send = send; }
  }
  async exchangeAuthorizationCode(client, code, verifier, redirect, resource) {
    if (resource && resource.href !== this.resourceServerUrl.href) throw new InvalidRequestError('Invalid OAuth resource');
    return super.exchangeAuthorizationCode(client, code, verifier, redirect, resource);
  }
  async exchangeRefreshToken(client, token, scopes, resource) {
    if (resource && resource.href !== this.resourceServerUrl.href) throw new InvalidRequestError('Invalid OAuth resource');
    return super.exchangeRefreshToken(client, token, scopes, resource);
  }
}

export async function loadConfig(filename = fileURLToPath(new URL('../config.json', import.meta.url))) {
  const config = JSON.parse(await readFile(filename, 'utf8'));
  const base = new URL(config.publicBaseUrl);
  if (base.protocol !== 'https:' || base.pathname !== '/' || base.search || base.hash || base.username || base.password) throw new Error('publicBaseUrl must be an HTTPS origin');
  if (config.upstreamUrl !== UPSTREAM_URL) throw new Error('upstreamUrl must be the fixed loopback Paseo endpoint');
  if (config.port !== 6768) throw new Error('Gateway port must be 6768');
  for (const field of ['stateDir', 'ownerTokenFile', 'paseoCommand']) if (!isAbsolute(config[field] ?? '')) throw new Error(`${field} must be an absolute path`);
  if (!Array.isArray(config.allowedRoots) || !config.allowedRoots.length || config.allowedRoots.some(root => !isAbsolute(root))) throw new Error('allowedRoots must contain absolute directory paths');
  return config;
}

export async function createGatewayApp(config, { upstream = new PaseoUpstream(config.upstreamUrl), oauthProvider,
  callbackClient, observerFactory = options => new ObserverClient(options) } = {}) {
  await mkdir(config.stateDir, { recursive: true, mode: 0o700 });
  await chmod(config.stateDir, 0o700);
  const resource = new URL('/mcp', config.publicBaseUrl);
  let provider = oauthProvider;
  if (!provider) {
    const auth = JSON.parse(await readFile(config.ownerTokenFile, 'utf8'));
    if (typeof auth.ownerToken !== 'string' || auth.ownerToken.length < 32) throw new Error('A strong ownerToken is required');
    provider = new LocalAgentsOAuthProvider({
      ownerToken: auth.ownerToken, scopes: [SCOPE],
      allowedRedirectHosts: ['chatgpt.com', 'chat.openai.com'],
      accessTokenTtlSeconds: 3600, refreshTokenTtlSeconds: 30 * 24 * 3600,
    }, resource, config.stateDir);
  }
  const store = new DispatchStore(config.stateDir);
  const gateway = new Gateway({ upstream, store, config });
  const eventStore = new EventStore(config.stateDir);
  const collaborationStore = new CollaborationStore(config.stateDir);
  let collaboration;
  const delivery = new EventDelivery({ store: eventStore, ...(callbackClient ? { callbackClient } : {}),
    authorizeSubscription: async (_name, args, _owner) => {
      try { await collaboration.workspace(args.workspaceId); return true; }
      catch (error) {
        if (/outside allowedRoots|not an active/.test(error.message)) return false;
        throw error;
      }
    } });
  collaboration = new Collaboration({ store: collaborationStore, delivery, eventStore, gateway });
  const channel = createAgentChannel({ stateDir: config.stateDir,
    verifyAgent: (agentId, body) => collaboration.verifyMessage(agentId, body),
    onMessage: body => collaboration.acceptMessage(body) });
  const observer = observerFactory({
    onEvent: event => { void collaboration.onObserverEvent(event).catch(() => {}); },
    onStatus: status => {
      if (status.status === 'ready') {
        for (const id of status.agentIds ?? []) void collaboration.reconcile(id).catch(() => {});
      }
    },
  });
  collaboration.setChannel(channel);
  collaboration.setObserver(observer);
  gateway.collaboration = collaboration;
  provider.onClientRevoked = clientId => delivery.revokeOwner(`local-owner:${clientId}`);
  try { await gateway.refreshTools(); }
  catch (error) { store.close(); provider.close(); throw error; }
  const app = createMcpExpressApp({ host: '127.0.0.1', allowedHosts: ['127.0.0.1', 'localhost', new URL(config.publicBaseUrl).hostname] });
  app.disable('x-powered-by');
  app.use(mcpAuthRouter({ provider, issuerUrl: new URL(config.publicBaseUrl), resourceServerUrl: resource, scopesSupported: [SCOPE], resourceName: 'Local Agents' }));
  app.get('/healthz', (_req, res) => res.json({ ok: true, name: 'local-agents-mcp', version: '0.3.0' }));
  const metadata = getOAuthProtectedResourceMetadataUrl(resource);
  const modern = createModernHandler({ gateway, eventService: collaboration,
    serverInfo: { name: 'local-agents-mcp', version: '0.3.0' } });
  app.all('/mcp', requireBearerAuth({ verifier: provider, requiredScopes: [SCOPE], resourceMetadataUrl: metadata }), async (req, res) => {
    // Check the intended resource on every request, not just token issuance.
    if (req.auth?.resource?.href !== resource.href) {
      res.set('WWW-Authenticate', `Bearer error="invalid_token", resource_metadata="${metadata}"`);
      res.status(401).json({ error: 'invalid_token', error_description: 'Token is not valid for this resource' });
      return;
    }
    if (req.method !== 'POST') { res.set('Allow', 'POST').sendStatus(405); return; }
    if (typeof req.auth?.clientId !== 'string' || !req.auth.clientId.length) { res.sendStatus(401); return; }
    req.mcpOwner = `local-owner:${req.auth.clientId}`;
    if (isModernRequest(req)) { await modern(req, res, req.body); return; }
    const server = new Server({ name: 'local-agents-mcp', version: '0.3.0' }, { capabilities: { tools: {} } });
    server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: await gateway.refreshTools() }));
    server.setRequestHandler(CallToolRequestSchema, async request => {
      try { return await gateway.call(request.params.name, request.params.arguments ?? {}, { owner: req.mcpOwner }); }
      catch (error) { return { isError: true, content: [{ type: 'text', text: error.message }] }; }
    });
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    res.on('close', () => { void server.close().catch(() => {}); });
    try {
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch {
      if (!res.headersSent) res.status(500).json({ jsonrpc: '2.0', id: null, error: { code: -32603, message: 'Gateway request failed' } });
      await server.close().catch(() => {});
    }
  });
  app.use((_error, _req, res, _next) => {
    if (!res.headersSent) res.status(400).json({ error: 'Invalid request' });
  });
  await channel.listen();
  delivery.start();
  // This process owns the channel and collector only. Other Paseo agents are untouched.
  void collaboration.start().catch(() => {});
  let closed;
  return { app, gateway, provider, collaboration, eventStore, delivery,
    close() {
      if (!closed) closed = (async () => {
        await modern.close();
        await channel.close();
        await collaboration.close();
        await delivery.stop();
        eventStore.close(); store.close(); provider.close();
      })();
      return closed;
    } };
}

async function main() {
  const config = await loadConfig(process.argv[2]);
  const runtime = await createGatewayApp(config);
  const listener = runtime.app.listen(config.port, '127.0.0.1', () => {
    console.log(JSON.stringify({ event: 'gateway_listening', host: '127.0.0.1', port: config.port, version: '0.3.0' }));
  });
  let stopping = false;
  function stop() {
    if (stopping) return;
    stopping = true;
    listener.close(() => { runtime.close(); });
  }
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { console.error(JSON.stringify({ event: 'gateway_start_failed' })); process.exitCode = 1; });
}
