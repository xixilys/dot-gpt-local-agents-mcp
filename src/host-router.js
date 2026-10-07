import Ajv from 'ajv';
import { jsonResult } from './gateway.js';
import { ATTENTION_EVENT } from './collaboration.js';
import { directProjects, assertOwner } from './direct-policy.js';
import { DEFAULT_HOST_ID, pinHost } from './hosts.js';

const listHosts = {
  name: 'list_hosts', title: 'List configured agent hosts',
  description: 'List the owner-configured Mac and SSH hosts with current availability. A host is a separate Paseo daemon; a desktop host connection is not imported automatically. Tools accept hostId; omission always means the legacy Mac.',
  inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
};
const listDirectProjects = {
  name: 'list_direct_projects', title: 'List Direct Workspace projects',
  description: 'List owner-configured Direct projects and file/command permissions on the selected host. Direct files and commands do not require Paseo online. full command mode grants the gateway account command execution; it is not an OS sandbox.',
  inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
};
const hostFreeReads = new Set(['get_dispatch_request', 'list_notification_routes', 'get_agent_message']);

function attributed(result, hostId) {
  if (!result || typeof result !== 'object') return result;
  if (!result.structuredContent || typeof result.structuredContent !== 'object') {
    return { ...result, structuredContent: { hostId }, content: [...(result.content ?? []),
      { type: 'text', text: JSON.stringify({ hostId }) }] };
  }
  const data = { ...result.structuredContent, hostId };
  for (const key of ['agents', 'workspaces', 'projects', 'permissions', 'routes']) {
    if (Array.isArray(data[key])) data[key] = data[key].map(item => ({ ...item, hostId }));
  }
  // Wrapper-generated JSON must agree with its structured representation. Keep
  // arbitrary upstream text/tool/media blocks as returned by that daemon.
  const content = (result.content ?? []).map(block => {
    if (block.type === 'text' && block.text === JSON.stringify(result.structuredContent)) {
      return { ...block, text: JSON.stringify(data) };
    }
    return block;
  });
  return { ...result, content, structuredContent: data };
}

export class HostRouter {
  constructor(contexts) {
    this.contexts = new Map(contexts.map(context => [context.host.id, context]));
    this.default = this.contexts.get(DEFAULT_HOST_ID);
    if (!this.default) throw new Error('The legacy Mac context is required');
    this.catalog = new Map();
    this.ajv = new Ajv({ strict: false, validateFormats: false });
  }
  context(id = DEFAULT_HOST_ID) {
    const context = typeof id === 'string' && this.contexts.get(id);
    if (!context) throw new Error('Unknown hostId; use list_hosts and an owner-configured host');
    return context;
  }
  // Preserve the existing app integration/test extension points for the Mac.
  get timeline() { return this.default.gateway.timeline.bind(this.default.gateway); }
  set timeline(value) { this.default.gateway.timeline = value; }
  get collaboration() { return this.default.collaboration; }
  async refreshTools() {
    const contexts = [...this.contexts.values()];
    await Promise.all(contexts.map(async context => {
      try { await context.gateway.refreshTools({ allowOffline: true }); }
      catch { /* One unreachable host must not remove the other host's tools. */ }
    }));
    const definitions = new Map();
    for (const context of contexts) {
      for (const { tool } of context.gateway.catalog.values()) {
        const collected = definitions.get(tool.name) ?? [];
        collected.push(tool); definitions.set(tool.name, collected);
      }
    }
    for (const context of contexts) {
      for (const direct of [context.directFiles, context.directCommands]) {
        for (const tool of direct?.tools() ?? []) {
          const collected = definitions.get(tool.name) ?? [];
          collected.push(tool); definitions.set(tool.name, collected);
        }
      }
    }
    definitions.set(listDirectProjects.name, [listDirectProjects]);
    const next = new Map([[listHosts.name, { tool: listHosts, validate: this.ajv.compile(listHosts.inputSchema) }]]);
    for (const [name, definitionsForTool] of definitions) {
      const first = structuredClone(definitionsForTool[0]);
      const properties = {};
      let required = new Set(first.inputSchema.required ?? []);
      for (const definition of definitionsForTool) {
        required = new Set([...required].filter(key => (definition.inputSchema.required ?? []).includes(key)));
        for (const [key, schema] of Object.entries(definition.inputSchema.properties ?? {})) {
          if (!properties[key]) properties[key] = schema;
          else if (JSON.stringify(properties[key]) !== JSON.stringify(schema)) {
            const alternatives = properties[key].anyOf ?? [properties[key]];
            if (!alternatives.some(item => JSON.stringify(item) === JSON.stringify(schema))) alternatives.push(schema);
            properties[key] = { anyOf: alternatives };
          }
        }
      }
      properties.hostId = { type: 'string', enum: [...this.contexts.keys()],
        description: 'Target host. Omit only for the legacy Mac. Use the same hostId for results, permissions, messages and follow-ups.' };
      first.inputSchema = { type: 'object', properties, required: [...required], additionalProperties: false };
      first.description += ' hostId selects a configured host; omission always means mac. Target-specific inputs are validated by that host.';
      next.set(name, { tool: first, validate: this.ajv.compile(first.inputSchema) });
    }
    this.catalog = next;
    return [...next.values()].map(entry => entry.tool);
  }
  async hostStatus(context) {
    let status;
    try {
      if (context.adapter?.status) status = await context.adapter.status();
      else { await context.gateway.upstream.tools(); status = { available: true }; }
      if (status.available && context.host.id !== DEFAULT_HOST_ID && !status.serverId) throw new Error('Remote daemon identity unavailable');
      if (status.available) await pinHost(context.host, status.serverId);
    } catch { status = { available: false, reason: 'daemon_unavailable_or_identity_mismatch' }; }
    return { hostId: context.host.id, name: context.host.name, transport: context.host.transport,
      available: status.available === true, directConfigured: directProjects(context.host).length > 0, ...(status.reason ? { reason: status.reason } : {}) };
  }
  async call(name, args = {}, options = {}) {
    if (name === 'list_hosts') {
      if (!this.catalog.get(name)?.validate(args)) throw new Error('list_hosts takes no arguments');
      return jsonResult({ hosts: await Promise.all([...this.contexts.values()].map(context => this.hostStatus(context))) });
    }
    if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('Tool arguments must be an object');
    const { hostId = DEFAULT_HOST_ID, ...businessArgs } = args;
    const context = this.context(hostId);
    if (name === listDirectProjects.name || context.directFiles?.tools().some(tool => tool.name === name)
      || context.directCommands?.tools().some(tool => tool.name === name)) {
      assertOwner(options.owner);
      if (!this.catalog.get(name)?.validate(args)) throw new Error('Invalid Direct tool arguments');
      if (name === listDirectProjects.name) return attributed(jsonResult({ projects: directProjects(context.host) }), hostId);
      const service = context.directFiles.tools().some(tool => tool.name === name) ? context.directFiles : context.directCommands;
      const value = await service.call(name, businessArgs, options);
      return attributed({ ...jsonResult(value), ...(value?.ok === false ? { isError: true } : {}) }, hostId);
    }
    // Receipt/message routing never discovers a host from an untrusted agent ID,
    // label, callback payload or request header, even when IDs collide.
    const savedRemoteResult = name === 'get_request_result' && Boolean(context.collaboration.store.get(businessArgs.requestId)?.result);
    if (context.host.id !== DEFAULT_HOST_ID && !hostFreeReads.has(name) && !savedRemoteResult) {
      const status = await context.adapter.status();
      if (!status.available || !status.serverId) throw new Error('Selected host is unavailable or unverified; no fallback or redispatch was attempted');
      await pinHost(context.host, status.serverId);
    }
    if (!context.gateway.catalog.has(name)) await context.gateway.refreshTools();
    return attributed(await context.gateway.call(name, businessArgs, options), context.host.id);
  }
  list(owner) {
    const definition = structuredClone(this.default.collaboration.list(owner).events[0]);
    definition.description = 'An agent on a subscribed host has a message, needs input or permission, or has an observed result. Use its hostId and exact request/message. Legacy payloads without hostId belong to mac; an event is not task acceptance.';
    definition.inputSchema.properties.hostId = { type: 'string', enum: [...this.contexts.keys()], description: 'Target host; omission means mac.' };
    definition.payloadSchema.properties.hostId = { type: 'string', enum: [...this.contexts.keys()] };
    return { events: [definition] };
  }
  eventContext(params) {
    if (params.name !== ATTENTION_EVENT) throw new Error('Unknown event');
    const { hostId = DEFAULT_HOST_ID, ...args } = params.arguments ?? {};
    const context = this.context(hostId);
    // A Mac subscription's old hash must remain byte-for-byte stable. A remote
    // filter keeps a trusted namespace so equal IDs cannot collide externally.
    return { context, params: { ...params, arguments: context.host.id === DEFAULT_HOST_ID ? args : { ...args, hostId: context.host.id } } };
  }
  async subscribe(owner, params) {
    const routed = this.eventContext(params);
    if (routed.context.host.id !== DEFAULT_HOST_ID) {
      const status = await routed.context.adapter.status();
      if (!status.available || !status.serverId) throw new Error('Selected host is unavailable or unverified');
      await pinHost(routed.context.host, status.serverId);
    }
    return routed.context.collaboration.subscribe(owner, routed.params);
  }
  unsubscribe(owner, params) {
    const routed = this.eventContext(params);
    return routed.context.collaboration.unsubscribe(owner, routed.params);
  }
  revokeOwner(owner) {
    return Promise.all([...this.contexts.values()].map(context => context.delivery.revokeOwner(owner)));
  }
}
