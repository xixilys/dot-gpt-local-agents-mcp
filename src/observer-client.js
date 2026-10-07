import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { StringDecoder } from 'node:string_decoder';
import Ajv from 'ajv';
import { validateSshTarget, nativeTarget } from './ssh-bridge.mjs';

const HELPER = '/Applications/Paseo.app/Contents/Frameworks/Paseo Helper.app/Contents/MacOS/Paseo Helper';
const SCRIPT = fileURLToPath(new URL('./agent-observer.mjs', import.meta.url));
const UUID_PATTERN = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
const UUID = new RegExp(UUID_PATTERN);
const id = { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$' };
const seq = { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER };
const cursor = { anyOf: [{ type: 'null' }, { type: 'object', required: ['epoch', 'seq'], additionalProperties: false, properties: { epoch: id, seq } }] };
const common = { agentId: { type: 'string', pattern: UUID_PATTERN }, turnId: id,
  timestamp: { type: 'string', maxLength: 64 }, seq, epoch: id };
const reasons = ['replacement', 'restored', 'subscription_error', 'epoch_changed', 'sequence_reset', 'sequence_gap', 'history_discontinuity', 'history_failed', 'connection_lost'];
function shape(type, properties, required = []) {
  return { type: 'object', additionalProperties: false, required: ['type', ...required], properties: { type: { const: type }, ...properties } };
}
const validate = new Ajv({ allErrors: false }).compile({ oneOf: [
  shape('connection-status', { status: { enum: ['connected', 'disconnected'] } }, ['status']),
  shape('ready', { agentIds: { type: 'array', maxItems: 256, uniqueItems: true, items: common.agentId }, watchVersion: { type: 'integer', minimum: 1 } }, ['agentIds', 'watchVersion']),
  shape('request_marker', { ...common, source: { enum: ['native', 'history'] }, requestId: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' } }, ['agentId', 'source', 'requestId']),
  shape('lifecycle', { ...common, source: { const: 'native' }, eventType: { enum: ['thread_started', 'turn_started', 'turn_completed', 'turn_failed', 'turn_canceled', 'permission_requested', 'permission_resolved', 'attention_required'] }, reason: { enum: ['finished', 'error', 'permission'] }, shouldNotify: { type: 'boolean' } }, ['agentId', 'source', 'eventType']),
  shape('discontinuity', { ...common, reason: { enum: reasons } }, ['reason']),
  shape('snapshot', { agentId: common.agentId, source: { const: 'history' }, agentStatus: { enum: ['idle', 'running', 'error', 'initializing', 'archived'] }, endCursor: cursor, hasOlder: { type: 'boolean' }, hasNewer: { type: 'boolean' }, finalSeen: { type: 'boolean' } }, ['agentId', 'source', 'agentStatus', 'endCursor', 'hasOlder', 'hasNewer', 'finalSeen']),
  shape('reconcile', { agentId: common.agentId, source: { const: 'history' }, reason: { const: 'idle_final_snapshot' }, endCursor: cursor }, ['agentId', 'source', 'reason', 'endCursor']),
] });

/** Owns one collector process; it never starts, stops or re-dispatches an agent. */
export class ObserverClient {
  constructor({ onEvent = () => {}, onStatus = () => {}, spawnImpl = spawn,
    backoffMs = 500, maxBackoffMs = 30000, maxLineBytes = 32768,
    startupTimeoutMs = 10000, killTimeoutMs = 1000, target, expectedServerId } = {}) {
    for (const n of [backoffMs, maxBackoffMs, maxLineBytes, startupTimeoutMs, killTimeoutMs]) {
      if (!Number.isSafeInteger(n) || n < 1) throw new Error('Invalid observer limits');
    }
    if (maxLineBytes > 65536 || maxBackoffMs < backoffMs) throw new Error('Invalid observer limits');
    Object.assign(this, { onEvent, onStatus, spawnImpl, backoffMs, maxBackoffMs, maxLineBytes, startupTimeoutMs, killTimeoutMs });
    this.target = target === undefined ? undefined : validateSshTarget(target).uri;
    this.expectedServerId = expectedServerId;
    this.agentIds = new Set(); this.generation = 0; this.attempt = 0;
    this.started = false; this.closed = false; this.owner = null; this.retryTimer = null;
  }
  status(status, extra = {}) { this.onStatus({ status, sourceGeneration: this.generation, ...extra }); }
  start() {
    if (this.closed) throw new Error('Observer is closed');
    if (!this.started) { this.started = true; this.launch(); }
    return this;
  }
  watchAgentIds(values) {
    if (this.closed) throw new Error('Observer is closed');
    const ids = [];
    for (const value of values) {
      if (ids.length === 256 || typeof value !== 'string' || !UUID.test(value)) throw new Error('Invalid observer agent IDs');
      ids.push(value.toLowerCase());
    }
    const next = new Set(ids);
    if (next.size === this.agentIds.size && [...next].every(id => this.agentIds.has(id))) return this;
    this.agentIds = next;
    if (this.owner?.connected) this.sendWatch(this.owner);
    return this;
  }
  sendWatch(owner) {
    if (!owner.child.stdin?.writable || owner.child.stdin.destroyed) { this.fail(owner, 'stdin_closed'); return; }
    // stdin backpressure is bounded: fail this collector rather than accumulating
    // arbitrary pending watch replacements. Recovery uses the latest watch set.
    if (owner.child.stdin.writableLength > 65536) { this.fail(owner, 'input_limit'); return; }
    try { owner.child.stdin.write(JSON.stringify({ action: 'watch', agentIds: [...this.agentIds] }) + '\n'); }
    catch { this.fail(owner, 'stdin_closed'); }
  }
  launch() {
    if (this.closed) return;
    this.generation++;
    this.status('starting');
    let child;
    try {
      const env = { ...process.env, ELECTRON_RUN_AS_NODE: '1', PASEO_NODE_ENV: 'production' };
      // These child identity variables belong to an actual agent, not this observer.
      delete env.PASEO_AGENT_ID; delete env.PASEO_AGENT_CWD;
      delete env.PASEO_PASSWORD; delete env.PASEO_HOST;
      let transport;
      if (this.target) {
        const serverId = typeof this.expectedServerId === 'function' ? this.expectedServerId() : this.expectedServerId;
        if (!serverId) throw new Error('Remote daemon identity must be established before observing');
        transport = { target: this.target, serverId };
        nativeTarget(transport);
      }
      child = this.spawnImpl(HELPER, [SCRIPT, ...(transport ? [JSON.stringify(transport)] : [])], { env, stdio: ['pipe', 'pipe', 'pipe'] });
    } catch { this.schedule('spawn_failed'); return; }
    const owner = { child, generation: this.generation, decoder: new StringDecoder('utf8'), input: '', connected: false, failed: false };
    this.owner = owner;
    owner.startupTimer = setTimeout(() => this.fail(owner, 'startup_timeout'), this.startupTimeoutMs);
    owner.startupTimer.unref?.();
    child.stdout.on('data', chunk => this.consume(owner, chunk));
    // Drain without persisting native exceptions, credentials or provider output.
    child.stderr.on('data', () => {});
    child.stdin.on('error', () => this.fail(owner, 'stdin_closed'));
    child.on('error', () => this.fail(owner, 'process_error'));
    const exited = () => {
      if (owner.exited) return;
      owner.exited = true;
      clearTimeout(owner.killTimer); clearTimeout(owner.startupTimer);
      if (!owner.failed) this.fail(owner, 'process_exit', false);
      else if (!this.closed && owner.restartReason) this.schedule(owner.restartReason);
    };
    child.on('exit', exited);
    // Failed spawns emit close without exit.
    child.on('close', exited);
  }
  consume(owner, chunk) {
    if (this.owner !== owner || owner.failed || this.closed) return;
    if (chunk.length > this.maxLineBytes * 4) { this.fail(owner, 'output_limit'); return; }
    owner.input += owner.decoder.write(chunk);
    let newline;
    while ((newline = owner.input.indexOf('\n')) !== -1) {
      if (owner.failed || this.closed || this.owner !== owner) return;
      const line = owner.input.slice(0, newline); owner.input = owner.input.slice(newline + 1);
      if (Buffer.byteLength(line) > this.maxLineBytes) { this.fail(owner, 'output_limit'); return; }
      let event;
      try { event = JSON.parse(line); } catch { this.fail(owner, 'invalid_output'); return; }
      if (!validate(event) || (event.timestamp && !Number.isFinite(Date.parse(event.timestamp)))) { this.fail(owner, 'invalid_output'); return; }
      if (event.agentId && !this.agentIds.has(event.agentId.toLowerCase())) continue;
      if (event.type === 'connection-status') {
        if (event.status === 'disconnected') { this.fail(owner, 'connection_lost'); return; }
        owner.connected = true; clearTimeout(owner.startupTimer);
        this.status('connected'); this.sendWatch(owner);
      } else if (event.type === 'ready') {
        // A stale watch ACK cannot qualify the current replacement watch set.
        if (event.agentIds.length === this.agentIds.size && event.agentIds.every(id => this.agentIds.has(id.toLowerCase()))) {
          this.attempt = 0;
          this.status('ready', { agentIds: [...this.agentIds], watchVersion: event.watchVersion });
        }
      } else this.onEvent({ ...event, sourceGeneration: owner.generation });
    }
    if (Buffer.byteLength(owner.input) > this.maxLineBytes) this.fail(owner, 'output_limit');
  }
  fail(owner, reason, terminate = true) {
    if (owner.failed || this.owner !== owner) return;
    owner.failed = true;
    clearTimeout(owner.startupTimer);
    this.owner = null;
    if (!this.closed) {
      this.onEvent({ type: 'discontinuity', reason, sourceGeneration: owner.generation });
      this.status('disconnected', { reason });
      owner.restartReason = reason;
    }
    // Wait for this owned child to exit before starting its replacement. This
    // preserves one live collector even when graceful shutdown takes time.
    if (terminate) this.terminate(owner);
    else if (!this.closed) this.schedule(reason);
  }
  terminate(owner) {
    try { owner.child.stdin.end(JSON.stringify({ action: 'stop' }) + '\n'); } catch {}
    // child.kill targets only the process object created by this client.
    try { owner.child.kill('SIGTERM'); } catch {}
    if (owner.exited) return;
    owner.killTimer = setTimeout(() => { try { owner.child.kill('SIGKILL'); } catch {} }, this.killTimeoutMs);
    owner.killTimer.unref?.();
  }
  schedule(reason) {
    if (this.closed || this.retryTimer) return;
    const delayMs = Math.min(this.maxBackoffMs, this.backoffMs * 2 ** Math.min(this.attempt++, 16));
    this.status('backoff', { reason, delayMs });
    this.retryTimer = setTimeout(() => { this.retryTimer = null; this.launch(); }, delayMs);
    this.retryTimer.unref?.();
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.retryTimer); this.retryTimer = null;
    const owner = this.owner; this.owner = null;
    if (owner) { owner.failed = true; clearTimeout(owner.startupTimer); this.terminate(owner); }
    this.status('closed');
  }
}
