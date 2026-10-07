// Owned, read-only collector. Only the gateway parent may control its watch set.
// The official installed client owns transport/auth; no session files are read here.
import { StringDecoder } from 'node:string_decoder';
import { nativeTarget, checkNativeIdentity } from './ssh-bridge.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const IDENT = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const MARKER = /^\[Local Agents requestId: ([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\](?:\r?\n|$)/;
const LIFECYCLE = new Set(['thread_started', 'turn_started', 'turn_completed', 'turn_failed', 'turn_canceled', 'permission_requested', 'permission_resolved', 'attention_required']);
const subscriptions = new Map(), cursors = new Map(), historyQueue = new Map();
let client, closing = false, connected = false, version = 0, input = '', queuedWatch = null, applyingWatch = false, readingHistory = false;
let bufferedOutput = 0;
// Never let the native client's logs (which may contain provider data) enter NDJSON.
for (const key of ['log', 'info', 'debug', 'warn', 'error']) console[key] = () => {};
function emit(event) {
  if (closing) return;
  const line = JSON.stringify(event) + '\n';
  const size = Buffer.byteLength(line);
  if (size > 32768 || bufferedOutput + size > 1024 * 1024) { void stop(1); return; }
  bufferedOutput += size;
  process.stdout.write(line, () => { bufferedOutput -= size; });
}
function identity(payload, event = {}) {
  const result = { agentId: payload.agentId };
  if (IDENT.test(event.turnId ?? '')) result.turnId = event.turnId;
  if (typeof payload.timestamp === 'string' && Number.isFinite(Date.parse(payload.timestamp))) result.timestamp = payload.timestamp;
  if (Number.isSafeInteger(payload.seq) && payload.seq >= 0) result.seq = payload.seq;
  if (IDENT.test(payload.epoch ?? '')) result.epoch = payload.epoch;
  return result;
}
function marker(agentId, item, fields, source) {
  if (item?.type !== 'user_message' || typeof item.text !== 'string') return;
  const match = item.text.match(MARKER);
  if (match) emit({ type: 'request_marker', source, ...fields, agentId, requestId: match[1] });
}
function receive(message) {
  const p = message?.payload;
  if (!p || !subscriptions.has(p.agentId)) return;
  if (['agent.timeline.replacement', 'agent.timeline.subscription_restored', 'agent.timeline.error'].includes(message.type)) {
    cursors.delete(p.agentId);
    emit({ type: 'discontinuity', agentId: p.agentId, reason: message.type === 'agent.timeline.replacement' ? 'replacement' : message.type === 'agent.timeline.error' ? 'subscription_error' : 'restored' });
    if (message.type === 'agent.timeline.error') void stop(1);
    return;
  }
  if (message.type !== 'agent_stream' || !p.event) return;
  const fields = identity(p, p.event);
  // seq counts *all* native timeline rows, including rows deliberately not forwarded.
  if (fields.seq !== undefined && fields.epoch) {
    const last = cursors.get(p.agentId);
    if (last && (last.epoch !== fields.epoch || fields.seq !== last.seq + 1)) {
      emit({ type: 'discontinuity', ...fields, reason: last.epoch !== fields.epoch ? 'epoch_changed' : fields.seq <= last.seq ? 'sequence_reset' : 'sequence_gap' });
    }
    cursors.set(p.agentId, { epoch: fields.epoch, seq: fields.seq });
  }
  if (p.event.type === 'timeline') marker(p.agentId, p.event.item, fields, 'native');
  else if (LIFECYCLE.has(p.event.type)) {
    const event = { type: 'lifecycle', source: 'native', ...fields, eventType: p.event.type };
    if (['finished', 'error', 'permission'].includes(p.event.reason)) event.reason = p.event.reason;
    if (typeof p.event.shouldNotify === 'boolean') event.shouldNotify = p.event.shouldNotify;
    emit(event);
  }
}
function cursor(value) {
  return value && IDENT.test(value.epoch ?? '') && Number.isSafeInteger(value.seq) && value.seq >= 0
    ? { epoch: value.epoch, seq: value.seq } : null;
}
async function history(agentId, subscription) {
  try {
    const page = await client.fetchAgentTimeline(agentId, { projection: 'projected', direction: 'tail', limit: 200, timeout: 5000 });
    if (subscriptions.get(agentId) !== subscription || closing) return;
    if (page.agentId !== agentId || page.agent?.id !== agentId || !Array.isArray(page.entries) || page.error) throw new Error();
    if (page.gap || page.reset || page.staleCursor) emit({ type: 'discontinuity', agentId, reason: 'history_discontinuity' });
    for (const entry of page.entries) {
      marker(agentId, entry.item, identity({ agentId, timestamp: entry.timestamp, seq: entry.seqStart, epoch: page.epoch }, entry), 'history');
    }
    const finalSeen = page.entries.some(e => e.item?.type === 'assistant_message');
    const endCursor = cursor(page.endCursor);
    emit({ type: 'snapshot', agentId, source: 'history', agentStatus: page.agent.status,
      endCursor, hasOlder: page.hasOlder === true, hasNewer: page.hasNewer === true, finalSeen });
    // A projected snapshot contains no native turn outcome. The parent must read
    // the complete matched request before deciding to notify or continue work.
    if (page.agent.status === 'idle' && page.agent.activeTurn === null && finalSeen) {
      emit({ type: 'reconcile', agentId, source: 'history', reason: 'idle_final_snapshot', endCursor });
    }
  } catch { if (subscriptions.get(agentId) === subscription) emit({ type: 'discontinuity', agentId, reason: 'history_failed' }); }
}
async function pumpHistory() {
  if (readingHistory) return;
  readingHistory = true;
  try {
    while (historyQueue.size && !closing) {
      const [id, sub] = historyQueue.entries().next().value;
      historyQueue.delete(id);
      if (subscriptions.get(id) === sub) await history(id, sub);
    }
  } finally { readingHistory = false; }
}
async function watch(agentIds) {
  const ids = new Set(agentIds.map(id => id.toLowerCase()));
  if (version && ids.size === subscriptions.size && [...ids].every(id => subscriptions.has(id))) return;
  const watchVersion = ++version;
  for (const [id, sub] of subscriptions) if (!ids.has(id)) {
    subscriptions.delete(id); cursors.delete(id); historyQueue.delete(id); await sub.release();
  }
  const added = [];
  for (const id of ids) if (!subscriptions.has(id)) {
    const sub = client.subscribeAgentTimeline(id, receive);
    subscriptions.set(id, sub);
    added.push(id);
    await sub.ready;
    if (closing) return;
  }
  emit({ type: 'ready', agentIds: [...ids], watchVersion });
  // Subscribe first: agents can finish before dispatch returns their ID.
  for (const id of added) historyQueue.set(id, subscriptions.get(id));
  void pumpHistory();
}
async function applyWatch() {
  if (applyingWatch) return;
  applyingWatch = true;
  try {
    while (queuedWatch && !closing) {
      const ids = queuedWatch; queuedWatch = null;
      await watch(ids);
    }
  } catch { void stop(1); }
  finally { applyingWatch = false; }
}
async function stop(code = 0) {
  if (closing) return;
  closing = true;
  const timer = setTimeout(() => process.exit(code), 1000);
  timer.unref();
  await Promise.allSettled([...subscriptions.values()].map(s => s.release()));
  await client?.close().catch(() => {});
  process.exit(code);
}
function command(line) {
  let cmd;
  try { cmd = JSON.parse(line); } catch { void stop(1); return; }
  if (cmd?.action === 'stop' && Object.keys(cmd).length === 1) { void stop(); return; }
  if (cmd?.action !== 'watch' || Object.keys(cmd).some(k => !['action', 'agentIds'].includes(k))
    || !Array.isArray(cmd.agentIds) || cmd.agentIds.length > 256 || cmd.agentIds.some(id => typeof id !== 'string' || !UUID.test(id))) { void stop(1); return; }
  // Keep only the newest pending replacement; never accumulate commands/history.
  queuedWatch = cmd.agentIds;
  void applyWatch();
}
try {
  const transport = process.argv[2] ? JSON.parse(process.argv[2]) : undefined;
  const target = nativeTarget(transport);
  const { connectToDaemon } = await import('/Applications/Paseo.app/Contents/Resources/app.asar/node_modules/@getpaseo/cli/dist/utils/client.js');
  delete process.env.PASEO_PASSWORD;
  client = await connectToDaemon({ target, timeout: 5000 });
  await checkNativeIdentity(client, transport);
  client.subscribeConnectionStatus(state => {
    const status = state.status === 'connected' ? 'connected' : 'disconnected';
    emit({ type: 'connection-status', status });
    if (connected && status !== 'connected') { emit({ type: 'discontinuity', reason: 'connection_lost' }); void stop(1); }
    if (status === 'connected') connected = true;
  });
  const decoder = new StringDecoder('utf8');
  process.stdin.on('data', chunk => {
    if (chunk.length + Buffer.byteLength(input) > 65536) { void stop(1); return; }
    input += decoder.write(chunk);
    let index;
    while ((index = input.indexOf('\n')) >= 0) { const line = input.slice(0, index); input = input.slice(index + 1); command(line); }
  });
  process.stdin.on('end', () => { void stop(); });
  process.on('SIGTERM', () => { void stop(); });
  process.on('SIGINT', () => { void stop(); });
} catch { process.exitCode = 1; }
