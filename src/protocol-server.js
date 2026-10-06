import { createMcpHandler, Server, ProtocolError, PROTOCOL_VERSION_META_KEY } from '@modelcontextprotocol/server';
import { toNodeHandler } from '@modelcontextprotocol/node';
import { z } from 'zod';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createRequestDiagnostics } from './request-diagnostics.js';

export const MODERN_PROTOCOL_VERSION = '2026-07-28';

// Classification only. The SDK still validates the claim and headers, so a
// malformed modern request cannot escape into the legacy transport.
export function isModernRequest(req) {
  const body = req.body;
  const method = body?.method;
  const header = req.headers?.['mcp-protocol-version'];
  return (typeof method === 'string' && (method === 'server/discover' || method.startsWith('events/')))
    || (typeof header === 'string' && header >= MODERN_PROTOCOL_VERSION)
    || Boolean(body?.params?._meta && Object.hasOwn(body.params._meta, PROTOCOL_VERSION_META_KEY));
}

const meta = z.record(z.string(), z.unknown()).optional();
const argumentsSchema = z.record(z.string(), z.unknown()).optional();
const delivery = { mode: z.literal('webhook'), url: z.string().url() };
const listParams = z.object({ cursor: z.string().optional(), _meta: meta }).strict();
const subscribeParams = z.object({
  name: z.string().min(1), arguments: argumentsSchema,
  delivery: z.object({ ...delivery, secret: z.string().min(1) }).strict(),
  cursor: z.string().nullable().optional(), ttlMs: z.number().int().positive().nullable().optional(),
  _meta: meta,
}).strict();
const unsubscribeParams = z.object({
  name: z.string().min(1), arguments: argumentsSchema,
  delivery: z.object(delivery).strict(), _meta: meta,
}).strict();

function authenticatedOwner(owner) {
  if (typeof owner !== 'string' || owner.length === 0) {
    throw new ProtocolError(-32603, 'Authenticated event owner is unavailable');
  }
  return owner;
}

/**
 * Modern-only HTTP adapter. Mount after the existing bearer and resource checks.
 * req.mcpOwner must be a stable principal set by trusted authentication middleware;
 * The middleware can bind this identity to a verified OAuth grant/clientId;
 * no request header or body parameter supplies it directly.
 * eventService owns event definitions, argument validation and authorization,
 * callback verification, persistence and delivery. Its methods return MCP results.
 */
export function createModernHandler({ gateway, eventService, serverInfo = { name: 'local-agents-mcp', version: '0.1.0' }, onDiagnostic }) {
  const diagnostics = new AsyncLocalStorage();
  const report = (stage, error) => diagnostics.getStore()?.report(stage, error);
  const observed = callback => async (...args) => {
    try { return await callback(...args); }
    catch (error) { report('handler', error); throw error; }
  };
  const handler = createMcpHandler(({ authInfo }) => {
    const server = new Server(serverInfo, { capabilities: { tools: {}, events: {} } });
    server.onerror = error => report('protocol', error);
    server.setRequestHandler('tools/list', observed(async () => ({ tools: await gateway.refreshTools() })));
    server.setRequestHandler('tools/call', async request => {
      try { return await gateway.call(request.params.name, request.params.arguments ?? {}, { owner: authenticatedOwner(authInfo?.mcpOwner) }); }
      catch (error) {
        report('tool', error);
        return { isError: true, content: [{ type: 'text', text: typeof error?.message === 'string' ? error.message : 'Tool execution failed' }] };
      }
    });
    server.setRequestHandler('events/list', { params: listParams }, observed(async params => {
      const { _meta, ...businessParams } = params;
      return eventService.list(authenticatedOwner(authInfo?.mcpOwner), businessParams);
    }));
    server.setRequestHandler('events/subscribe', { params: subscribeParams }, observed(async params => {
      const { _meta, ...businessParams } = params;
      return eventService.subscribe(authenticatedOwner(authInfo?.mcpOwner), businessParams);
    }));
    server.setRequestHandler('events/unsubscribe', { params: unsubscribeParams }, observed(async params => {
      const { _meta, ...businessParams } = params;
      return eventService.unsubscribe(authenticatedOwner(authInfo?.mcpOwner), businessParams);
    }));
    return server;
  }, { legacy: 'reject', onerror: error => report('http_protocol', error) });

  const nodeHandler = async (req, res, parsedBody = req.body) => {
    // Each wrapper closes over this request's identity, including concurrent calls.
    // toNodeHandler passes authentication through without reading credential headers.
    const observation = createRequestDiagnostics({ body: parsedBody === undefined || typeof parsedBody === 'function' ? req.body : parsedBody,
      once: req.once.bind(req) }, res, { gateway, write: onDiagnostic });
    const adapter = toNodeHandler({ fetch: (request, options) => handler.fetch(request, {
      ...options, authInfo: { ...options?.authInfo, mcpOwner: req.mcpOwner },
    }) }, { onerror: error => observation.report('adapter', error) });
    await diagnostics.run(observation, () => adapter(req, res, typeof parsedBody === 'function' ? req.body : parsedBody));
  };
  nodeHandler.close = () => handler.close();
  return nodeHandler;
}
