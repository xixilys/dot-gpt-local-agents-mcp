import Ajv from 'ajv';
import { realpath, stat } from 'node:fs/promises';
import { isAbsolute, relative, sep } from 'node:path';
import { execFile as execFileCallback } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { dispatchFingerprint, publicDispatchRecord } from './dispatch-store.js';
import { requestMarker } from './wait-result.js';
import { COLLABORATION_TOOLS } from './collaboration.js';

const execFile = promisify(execFileCallback);
const ELECTRON_HELPER = '/Applications/Paseo.app/Contents/Frameworks/Paseo Helper.app/Contents/MacOS/Paseo Helper';
const TIMELINE_READER = fileURLToPath(new URL('./timeline-reader.mjs', import.meta.url));
const DAEMON_READER = fileURLToPath(new URL('./daemon-reader.mjs', import.meta.url));
export const PUBLIC_TOOLS = new Set([
  'create_workspace', 'list_workspaces', 'create_agent', 'send_agent_prompt',
  'get_agent_status', 'list_agents', 'cancel_agent', 'update_agent',
  'list_profiles', 'list_providers', 'list_models', 'inspect_provider',
  'get_agent_activity', 'list_pending_permissions', 'respond_to_permission',
]);
const DISPATCH_TOOLS = new Set(['create_agent', 'send_agent_prompt']);
const READ_TOOLS = new Set(['list_workspaces', 'get_agent_status', 'list_agents', 'list_profiles', 'list_providers', 'list_models', 'inspect_provider', 'get_agent_activity', 'list_pending_permissions', 'get_dispatch_request', 'get_agent_result']);
const requestIdSchema = { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$', description: 'Caller-generated unique id. Reuse exactly this id and identical parameters after a lost response; never retry with a new id without checking the original dispatch.' };
const EXTRA_TOOLS = [
  {
    name: 'list_projects', title: 'List allowed projects',
    description: 'Read registered Paseo projects, including projects without active workspaces. Returns projectId, name, kind and absolute path, filtered through the same allowedRoots policy.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  },
  {
    name: 'wait_for_agent_result', title: 'Wait for this dispatch result',
    description: 'Read-only bounded native wait and actual projected result for a persisted requestId and agentId. Match the real request marker and turn; legacy requests without the marker remain unmatched. idle is an observed end state, not task acceptance. This call does not subscribe or wake a caller; use the agent.attention event subscription for asynchronous dot wakeups. Safe to repeat this read after a timeout/disconnect; never resends the prompt.',
    inputSchema: { type: 'object', properties: {
      requestId: requestIdSchema,
      agentId: { type: 'string', pattern: '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' },
      timeoutMs: { type: 'integer', minimum: 1000, maximum: 20000, default: 10000, description: 'Total call budget, including path/identity check, connection, native wait and result read; maximum 20 seconds.' },
      limit: { type: 'integer', minimum: 1, maximum: 200, default: 100, description: 'Entries per page; reads at most three pages. Use get_agent_result for additional history.' },
      turnId: { type: 'string', minLength: 1, maxLength: 256, description: 'Optional expected turn ID; must match the marked user message.' },
    }, required: ['requestId', 'agentId'], additionalProperties: false },
  },
  {
    name: 'get_dispatch_request', title: 'Recover dispatch request',
    description: 'Read a persisted dispatch receipt or uncertain outcome by requestId. Does not resend anything; an accepted submission is not task completion.',
    inputSchema: { type: 'object', properties: { requestId: requestIdSchema }, required: ['requestId'], additionalProperties: false },
  },
  {
    name: 'get_agent_result', title: 'Read agent result and timeline',
    description: 'Read the actual projected timeline with full assistant text, tool detail, timestamps, turn IDs and pagination cursors. Projected entries merge assistant chunks and tool lifecycle events; this is not the raw event stream. Use before/startCursor for older pages, after/endCursor for newer pages. Inspect the actual task result; idle state alone does not prove success.',
    inputSchema: { type: 'object', properties: {
      agentId: { type: 'string', pattern: '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' },
      limit: { type: 'integer', minimum: 1, maximum: 200, default: 50 },
      direction: { type: 'string', enum: ['tail', 'before', 'after'], default: 'tail' },
      cursor: { type: 'object', properties: {
        epoch: { type: 'string', minLength: 1, maxLength: 128 },
        seq: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
      }, required: ['epoch', 'seq'], additionalProperties: false },
    }, required: ['agentId'], additionalProperties: false },
  },
];

export class PathPolicy {
  constructor(roots) { this.roots = roots; }
  async check(value) {
    if (typeof value !== 'string' || !isAbsolute(value)) throw new Error('An explicit absolute directory path is required');
    const resolved = await realpath(value);
    if (!(await stat(resolved)).isDirectory()) throw new Error('Path must be an existing directory');
    // An unavailable mount removes only that root's authority. It must not
    // prevent a different, reachable root from authorizing this real target.
    const resolutions = await Promise.allSettled(this.roots.map(root => realpath(root)));
    const roots = resolutions.filter(result => result.status === 'fulfilled').map(result => result.value);
    const allowed = roots.some(root => {
      const remainder = relative(root, resolved);
      return remainder === '' || (!isAbsolute(remainder) && remainder !== '..' && !remainder.startsWith(`..${sep}`));
    });
    if (!allowed) throw new Error('Directory is outside allowedRoots (a gateway path check, not an OS sandbox)');
    return resolved;
  }
}

export class Gateway {
  constructor({ upstream, store, config, runCli = execFile, collaboration }) {
    this.upstream = upstream;
    this.store = store;
    this.config = config;
    this.runCli = runCli;
    this.paths = new PathPolicy(config.allowedRoots);
    this.ajv = new Ajv({ strict: false, allErrors: true, validateFormats: false });
    this.catalog = new Map();
    this.collaboration = collaboration;
  }
  async refreshTools() {
    const upstreamTools = await this.upstream.tools();
    if (!Array.isArray(upstreamTools)) throw new Error('Invalid upstream tool catalog');
    const catalog = new Map();
    for (const original of upstreamTools) {
      if (!PUBLIC_TOOLS.has(original.name)) continue;
      const tool = structuredClone(original);
      // Keep upstream property schemas, while rejecting unknown wrapper inputs.
      tool.inputSchema.additionalProperties = false;
      if (DISPATCH_TOOLS.has(tool.name)) {
        delete tool.inputSchema.properties.background;
        delete tool.inputSchema.properties.notifyOnFinish;
        tool.inputSchema.properties.requestId = requestIdSchema;
        if (this.collaboration) {
          tool.inputSchema.properties.notificationRouteId = { ...requestIdSchema, description: 'Subscribed dot route for this workspace. Omit only when there is one active route or this agent already has one. No subscription means no automatic wake.' };
          if (tool.name === 'send_agent_prompt') tool.inputSchema.properties.replyToMessageId = { type: 'string', format: 'uuid', description: 'Persisted message being answered. Reply to its original agent after that agent has yielded.' };
        }
        tool.inputSchema.required = [...new Set([...(tool.inputSchema.required ?? []), 'requestId', ...(tool.name === 'create_agent' ? ['workspaceId'] : [])])];
        tool.description = tool.name === 'create_agent'
          ? 'Create an agent asynchronously in an explicit allowed workspace with required provider/model and requestId. Discover profiles/providers/models first. An active agent.attention route can notify and wake its subscribed dot. Without a subscription, use the returned agentId and read/wait tools.'
          : 'Send a follow-up asynchronously with a required unique requestId. A subscribed agent.attention route can notify its dot; a receipt confirms submission only, not successful completion. Include replyToMessageId when answering an agent message.';
      }
      if (tool.name === 'create_workspace') tool.inputSchema.required = [...new Set([...(tool.inputSchema.required ?? []), 'path'])];
      if (tool.name === 'inspect_provider') tool.inputSchema.required = [...new Set([...(tool.inputSchema.required ?? []), 'cwd'])];
      if (tool.name === 'get_agent_activity') tool.description = 'Read curated recent activity summaries. This is not the complete agent result or raw timeline.';
      tool.annotations = READ_TOOLS.has(tool.name)
        ? { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
        : { readOnlyHint: false, destructiveHint: tool.name !== 'create_workspace', idempotentHint: DISPATCH_TOOLS.has(tool.name), openWorldHint: true };
      catalog.set(tool.name, { tool, validate: this.ajv.compile(tool.inputSchema) });
    }
    for (const original of [...EXTRA_TOOLS, ...(this.collaboration ? COLLABORATION_TOOLS : [])]) {
      const tool = { ...original, annotations: { readOnlyHint: original.name !== 'watch_dispatch_request', destructiveHint: false, idempotentHint: true, openWorldHint: false } };
      catalog.set(tool.name, { tool, validate: this.ajv.compile(tool.inputSchema) });
    }
    // Swap atomically so calls never see a partially loaded catalog.
    this.catalog = catalog;
    return [...catalog.values()].map(entry => entry.tool);
  }
  async call(name, args = {}, { owner } = {}) {
    const entry = this.catalog.get(name);
    if (!entry) throw new Error('Tool is not exposed by this gateway');
    if (!entry.validate(args)) {
      // Do not echo caller values (in particular prompts or credentials).
      throw new Error(`Invalid tool input: ${entry.validate.errors.map(error => `${error.instancePath || '/'} ${error.message}`).join('; ')}`);
    }
    if (name === 'get_dispatch_request') {
      this.collaboration?.assertRequestOwner(args.requestId, owner);
      return jsonResult(publicDispatchRecord(this.store.get(args.requestId)));
    }
    if (name === 'list_notification_routes') return jsonResult({ routes: this.collaboration.routes(owner, args.workspaceId) });
    if (name === 'get_agent_message') return jsonResult(await this.collaboration.getMessage(args.messageId, owner));
    if (name === 'watch_dispatch_request') return jsonResult(await this.collaboration.watchDispatchRequest(args, owner));
    if (name === 'get_request_result') return jsonResult(await this.collaboration.getRequestResult(args.requestId, owner));
    if (name === 'list_projects') {
      const result = await this.readDaemon({ action: 'list_projects' }, 8000);
      if (!Array.isArray(result.projects)) throw new Error('Cannot verify project paths from Paseo');
      const projects = [];
      for (const project of result.projects) {
        try { await this.paths.check(project.path); projects.push(project); } catch { /* Fail closed. */ }
      }
      return jsonResult({ projects, scope: 'allowedRoots', filteringApplied: true });
    }
    if (name === 'wait_for_agent_result') {
      this.collaboration?.assertRequestOwner(args.requestId, owner);
      const timeoutMs = args.timeoutMs ?? 10000;
      let timer;
      try {
        return jsonResult(await Promise.race([
          this.waitForResult(args),
          new Promise(resolve => { timer = setTimeout(() => resolve({ requestId: args.requestId, agentId: args.agentId,
            status: 'timeout', reason: 'call_budget_exhausted', terminalDetected: null, acceptancePassed: null,
            retrySafe: true, notification: 'mcp_wait_response', ...this.notificationState(args.requestId) }), timeoutMs); }),
        ]));
      } finally { clearTimeout(timer); }
    }
    if (DISPATCH_TOOLS.has(name)) return this.dispatch(name, args, owner);
    let status;
    if (args.agentId !== undefined) status = await this.checkAgent(args.agentId);
    if (name === 'get_agent_result') return jsonResult(await this.timeline(args));
    if (name === 'get_agent_status') return status;
    if (name === 'create_workspace') await this.paths.check(args.path);
    if (name === 'inspect_provider') await this.paths.check(args.cwd);
    if (name === 'list_agents' && args.cwd !== undefined) await this.paths.check(args.cwd);
    const result = await this.upstream.call(name, args);
    if (name === 'list_agents' || name === 'list_workspaces') {
      const field = name === 'list_agents' ? 'agents' : 'workspaces';
      const values = result.structuredContent?.[field];
      if (!Array.isArray(values)) throw new Error('Cannot verify listing paths from Paseo');
      const filtered = [];
      for (const value of values) {
        try { await this.paths.check(value.cwd); filtered.push(value); } catch { /* Fail closed for out-of-scope entries. */ }
      }
      return jsonResult({ ...result.structuredContent, [field]: filtered, scope: 'allowedRoots', filteringApplied: true });
    }
    if (name === 'list_pending_permissions') {
      const permissions = result.structuredContent?.permissions;
      if (!Array.isArray(permissions)) throw new Error('Cannot verify pending permission agents from Paseo');
      const filtered = [];
      for (const permission of permissions) {
        try { await this.checkAgent(permission.agentId); filtered.push(permission); } catch { /* Fail closed. */ }
      }
      return jsonResult({ ...result.structuredContent, permissions: filtered, scope: 'allowedRoots', filteringApplied: true });
    }
    return result;
  }
  async checkAgent(agentId, timeoutMs) {
    if (typeof agentId !== 'string' || !agentId.length) throw new Error('An agentId is required');
    const result = await this.upstream.call('get_agent_status', { agentId }, timeoutMs === undefined ? undefined : { timeoutMs });
    const snapshot = result.structuredContent?.snapshot;
    if (result.isError || !snapshot || snapshot.id !== agentId) throw new Error('Cannot verify agent identity and working directory from Paseo; use the complete agentId');
    await this.paths.check(snapshot.cwd);
    return result;
  }
  async dispatch(name, args, owner, lockedRequest = false, lockedReply = false) {
    if (this.collaboration && !lockedRequest) {
      return this.collaboration.locked(this.collaboration.requestLocks, args.requestId,
        () => this.dispatch(name, args, owner, true, false));
    }
    if (this.collaboration && args.replyToMessageId && !lockedReply) {
      return this.collaboration.locked(this.collaboration.replyLocks, `${owner}:${args.replyToMessageId}`,
        () => this.dispatch(name, args, owner, true, true));
    }
    const { requestId, ...payload } = args;
    const fingerprint = dispatchFingerprint(name, payload);
    const existing = this.store.get(requestId);
    if (existing) {
      this.collaboration?.assertRequestOwner(requestId, owner);
      if (existing.fingerprint !== fingerprint) throw new Error('requestId already belongs to different parameters');
      return jsonResult(publicDispatchRecord(existing, true));
    }
    if (this.collaboration && args.replyToMessageId) {
      const replay = this.collaboration.store.replyFor(args.replyToMessageId);
      if (replay) {
        const semantic = dispatchFingerprint(name, { agentId: args.agentId, prompt: args.prompt, replyToMessageId: args.replyToMessageId });
        const m = this.collaboration.store.getMessage(args.replyToMessageId);
        this.collaboration.assertRequestOwner(m?.requestId, owner);
        if (replay.fingerprint !== semantic) throw new Error('This message already has a different reply; inspect its reply receipt');
        const original = this.store.get(replay.requestId);
        return jsonResult(original ? { ...publicDispatchRecord(original, true), replyAlreadySubmitted: true }
          : { found: false, requestId: replay.requestId, state: 'unknown', duplicate: true,
            message: 'A reply was durably reserved before its receipt was saved. Inspect the original request and timeline; do not dispatch another reply.' });
      }
    }
    if (name === 'create_agent') {
      if (Object.hasOwn(payload.labels ?? {}, 'dotRequestId')) throw new Error('dotRequestId is reserved; caller labels must not include it');
      const response = await this.upstream.call('list_workspaces', {});
      const workspaces = response.structuredContent?.workspaces;
      if (!Array.isArray(workspaces)) throw new Error('Cannot verify workspace path from Paseo');
      const workspace = workspaces.find(item => item.workspaceId === payload.workspaceId);
      if (!workspace) throw new Error('workspaceId is not an active Paseo workspace');
      await this.paths.check(workspace.cwd);
      payload.labels = { ...payload.labels, dotRequestId: requestId };
    } else await this.checkAgent(payload.agentId);
    if (this.collaboration) {
      await this.collaboration.prepareDispatch(name, args, owner, { submittedAt: new Date().toISOString() });
      delete payload.notificationRouteId;
      delete payload.replyToMessageId;
      if (args.replyToMessageId) {
        const semantic = dispatchFingerprint(name, { agentId: args.agentId, prompt: args.prompt, replyToMessageId: args.replyToMessageId });
        const reply = this.collaboration.store.claimReply(args.replyToMessageId, requestId, semantic);
        if (!reply.claimed) {
          const original = this.store.get(reply.record.requestId);
          return jsonResult(original ? { ...publicDispatchRecord(original, true), replyAlreadySubmitted: true }
            : { found: false, requestId: reply.record.requestId, state: 'unknown', duplicate: true });
        }
      }
    }
    // Claim durably before the first mutating upstream call, including races
    // between simultaneous identical requests.
    const claim = this.store.begin(requestId, fingerprint, name, payload.agentId);
    if (!claim.claimed) return jsonResult(publicDispatchRecord(claim.record, true));
    const promptField = name === 'create_agent' ? 'initialPrompt' : 'prompt';
    payload[promptField] = requestMarker(requestId) + payload[promptField];
    const binding = this.collaboration?.store.get(requestId);
    if (binding?.subscriptionId) payload[promptField] += this.collaboration.promptInstructions(requestId);
    payload.background = true;
    payload.notifyOnFinish = false;
    let record;
    try {
      const result = await this.upstream.call(name, payload);
      const agentId = result.structuredContent?.agentId ?? payload.agentId;
      record = this.store.finish(requestId, result.isError ? 'rejected' : 'submitted', result, agentId);
    } catch {
      // All transport/RPC ambiguity is retained. Even a timeout may follow an
      // accepted action, so it must never enable a second dispatch.
      record = this.store.finish(requestId, 'unknown', undefined, payload.agentId);
    }
    if (this.collaboration) await this.collaboration.finishDispatch(record).catch(() => {});
    return jsonResult(publicDispatchRecord(record));
  }
  async readDaemon(args, timeoutMs) {
    const { stdout } = await this.runCli(ELECTRON_HELPER, [DAEMON_READER, JSON.stringify(args)], {
      timeout: timeoutMs, maxBuffer: 1024 * 1024, encoding: 'utf8',
      env: { HOME: process.env.HOME, PATH: '/usr/bin:/bin', ELECTRON_RUN_AS_NODE: '1', PASEO_NODE_ENV: 'production' },
    });
    return JSON.parse(stdout);
  }
  async waitForResult({ requestId, agentId, timeoutMs = 10000, limit = 100, turnId }) {
    const deadline = Date.now() + timeoutMs;
    const record = this.store.get(requestId);
    if (!record) throw new Error('Unknown dispatch requestId; use the original receipt');
    if (record.agentId && record.agentId !== agentId) throw new Error('requestId belongs to a different agent');
    const checked = await this.checkAgent(agentId, Math.min(2000, timeoutMs));
    if (!record.agentId && checked.structuredContent.snapshot.labels?.dotRequestId !== requestId) {
      throw new Error('Cannot verify this agent belongs to the uncertain creation receipt');
    }
    if (record.state === 'rejected') return { requestId, agentId, status: 'error', reason: 'dispatch_rejected', terminalDetected: false, acceptancePassed: null };
    const remaining = deadline - Date.now();
    if (remaining <= 0) return { requestId, agentId, status: 'timeout', reason: 'call_budget_exhausted', terminalDetected: null, acceptancePassed: null };
    try {
      const result = await this.readDaemon({ action: 'wait', requestId, agentId, submittedAt: record.submittedAt,
        timeoutMs: remaining, limit, ...(turnId ? { turnId } : {}) }, remaining);
      if (result.agentId !== agentId || result.requestId !== requestId) throw new Error('Invalid wait identity');
      if (result.timeline?.agent) await this.paths.check(result.timeline.agent.cwd);
      return { ...result, receiptState: record.state, fetchedAt: new Date().toISOString(),
        source: 'paseo-native-wait-and-projected-timeline', fullRawEventStream: false, gatewayTruncated: false,
        ...this.notificationState(requestId) };
    } catch {
      return { requestId, agentId, status: Date.now() >= deadline ? 'timeout' : 'error', reason: 'wait_read_failed_or_budget_exhausted',
        terminalDetected: null, acceptancePassed: null, retrySafe: true,
        notification: 'mcp_wait_response', ...this.notificationState(requestId) };
    }
  }
  notificationState(requestId) {
    const r = this.collaboration?.store.get(requestId);
    const active = Boolean(r?.subscriptionId && this.collaboration.eventStore.activeSubscriptions({ owner: r.owner })
      .some(s => s.id === r.subscriptionId));
    return { automaticWakeSupported: active, notificationMode: active ? 'subscribed_events' : 'explicit_read' };
  }
  async timeline({ agentId, limit = 50, direction = 'tail', cursor }) {
    if (direction !== 'tail' && !cursor) throw new Error('before/after timeline reads require a cursor');
    if (direction === 'tail' && cursor) throw new Error('tail reads must omit cursor');
    let stdout;
    try {
      ({ stdout } = await this.runCli(ELECTRON_HELPER,
        [TIMELINE_READER, JSON.stringify({ agentId, limit, direction, ...(cursor ? { cursor } : {}) })],
        { timeout: 30_000, maxBuffer: 1024 * 1024, encoding: 'utf8', env: {
          HOME: process.env.HOME, PATH: '/usr/bin:/bin', ELECTRON_RUN_AS_NODE: '1', PASEO_NODE_ENV: 'production',
        } }));
    } catch {
      throw new Error('Timeline read failed, timed out, or exceeded the 1 MiB output limit; reduce limit and retry this read');
    }
    const timeline = JSON.parse(stdout);
    if (timeline.error || !Array.isArray(timeline.entries) || timeline.agentId !== agentId) throw new Error('Paseo timeline read returned an error or invalid result');
    if (timeline.agent) await this.paths.check(timeline.agent.cwd);
    return {
      ...timeline, fetchedAt: new Date().toISOString(), requestedLimit: limit,
      source: 'paseo-daemon-projected-timeline', fullRawEventStream: false,
      gatewayTruncated: false, outputLimitBytes: 1024 * 1024,
      completionConfirmed: false,
    };
  }
}

export function jsonResult(value) {
  return { content: [{ type: 'text', text: JSON.stringify(value) }], structuredContent: value };
}
