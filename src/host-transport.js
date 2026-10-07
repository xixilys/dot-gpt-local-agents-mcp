import { execFile as execFileCallback } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { posix, isAbsolute } from 'node:path';
import { PaseoUpstream } from './upstream.js';
import { ObserverClient } from './observer-client.js';
import { createRemoteChannel, validateRemoteStateDir } from './remote-channel.js';
import { pinHost } from './hosts.js';
import { validateSshTarget, validateNodeCommand, createSshRunner, createSshForward, REMOTE_PATH_SCRIPT } from './ssh-bridge.mjs';

export { validateSshTarget } from './ssh-bridge.mjs';
const execFile = promisify(execFileCallback);
const HELPER = '/Applications/Paseo.app/Contents/Frameworks/Paseo Helper.app/Contents/MacOS/Paseo Helper';
const DAEMON_READER = fileURLToPath(new URL('./daemon-reader.mjs', import.meta.url));
const TIMELINE_READER = fileURLToPath(new URL('./timeline-reader.mjs', import.meta.url));
const serverIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;

export class RemotePathPolicy {
  constructor(roots, runRemote) {
    if (!Array.isArray(roots) || !roots.length || roots.length > 128
      || roots.some(p => typeof p !== 'string' || !posix.isAbsolute(p) || p.includes('\0'))) throw new Error('Remote allowedRoots must be explicit absolute paths');
    this.roots = [...roots]; this.runRemote = runRemote;
  }
  async check(value, { timeoutMs = 8000, signal } = {}) {
    if (typeof value !== 'string' || !posix.isAbsolute(value) || value.includes('\0') || value.length > 16384) throw new Error('An explicit remote absolute directory path is required');
    let result;
    try { signal?.throwIfAborted(); result = await this.runRemote(REMOTE_PATH_SCRIPT, { value, roots: this.roots }, { timeoutMs, signal, maxBytes: 65536 }); signal?.throwIfAborted(); }
    catch { throw new Error('Remote path could not be verified inside allowedRoots'); }
    if (typeof result?.resolved !== 'string' || !posix.isAbsolute(result.resolved) || result.resolved.includes('\0')) throw new Error('Remote path verification failed');
    return result.resolved;
  }
}

async function defaultNativeReader(script, args, transport, timeoutMs, { signal } = {}) {
  const { stdout } = await execFile(HELPER, [script, JSON.stringify(args), JSON.stringify(transport)], {
    timeout: timeoutMs, killSignal: 'SIGKILL', maxBuffer: 1024 * 1024, encoding: 'utf8', signal,
    env: { HOME: process.env.HOME, PATH: '/usr/bin:/bin', ELECTRON_RUN_AS_NODE: '1', PASEO_NODE_ENV: 'production' },
  });
  return JSON.parse(stdout);
}

/** All endpoints originate in owner config; no tool accepts transport parameters. */
export function createRemoteHostAdapter(host, { stateDir, spawnImpl, runRemote: injectedRunner,
  forwardFactory = createSshForward, nativeReader = defaultNativeReader, fetchImpl,
  observerClass = ObserverClient, channelFactory = createRemoteChannel } = {}) {
  if (!host || host.transport !== 'ssh' || typeof host.id !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(host.id)
    || !isAbsolute(stateDir ?? '')) throw new Error('Invalid remote host adapter configuration');
  const target = validateSshTarget(host.target);
  validateRemoteStateDir(host.remoteStateDir);
  const nodeCommand = validateNodeCommand(host.nodeCommand);
  const runner = injectedRunner ? { run: injectedRunner, close() {} } : createSshRunner(target, { spawnImpl, nodeCommand });
  const paths = new RemotePathPolicy(host.allowedRoots, async (script, input, options) => {
    const deadline = Date.now() + options.timeoutMs;
    await ensureIdentity(Math.min(options.timeoutMs, 8000), { signal: options.signal });
    const timeoutMs = deadline - Date.now();
    if (timeoutMs < 1) throw new Error('Remote path budget exhausted');
    options.signal?.throwIfAborted();
    return runner.run(script, input, { ...options, timeoutMs });
  });
  let closed = false, pinnedServerId, identityMismatch = false, tunnel, tunnelPromise;
  const observers = new Set(), channels = new Set();
  const nativeRequests = new Set();
  const assertOpen = () => { if (closed) throw new Error('Remote host adapter closed'); if (identityMismatch) throw new Error('Remote daemon identity changed'); };
  async function invokeNative(script, args, transport, timeoutMs, { signal } = {}) {
    assertOpen();
    const controller = new AbortController(); nativeRequests.add(controller);
    try { const result = await nativeReader(script, args, transport, timeoutMs, { signal: signal ? AbortSignal.any([controller.signal, signal]) : controller.signal }); signal?.throwIfAborted(); assertOpen(); return result; }
    finally { nativeRequests.delete(controller); }
  }
  async function ensureIdentity(timeoutMs = 8000, { signal } = {}) {
    assertOpen();
    const result = await invokeNative(DAEMON_READER, { action: 'status' }, { target: target.uri }, timeoutMs, { signal });
    if (!result?.available || typeof result.serverId !== 'string' || !serverIdPattern.test(result.serverId)) throw new Error('Remote daemon identity unavailable');
    if (pinnedServerId && result.serverId !== pinnedServerId) { identityMismatch = true; tunnel?.close(); throw new Error('Remote daemon identity changed'); }
    // The disk binding survives gateway restarts and protects background reads,
    // reconciliation and notifications as well as calls through HostRouter.
    // Never extend/reset an existing binding with a missing daemon identity.
    await checkPersistedIdentity(result.serverId);
    pinnedServerId = result.serverId;
    return pinnedServerId;
  }
  async function checkPersistedIdentity(serverId) {
    assertOpen();
    if (!serverIdPattern.test(serverId ?? '')) throw new Error('Remote daemon identity unavailable');
    try { await pinHost({ ...host, stateDir }, serverId); }
    catch {
      identityMismatch = true; tunnel?.close();
      throw new Error('Remote daemon identity changed or its persisted binding could not be verified');
    }
    assertOpen();
  }
  async function channelRemote(...args) {
    assertOpen();
    if (!pinnedServerId) await ensureIdentity();
    await checkPersistedIdentity(pinnedServerId);
    return runner.run(...args);
  }
  async function ensureTunnel(timeoutMs = 8000) {
    assertOpen();
    if (tunnel?.isAlive()) return tunnel;
    if (!tunnelPromise) tunnelPromise = (async () => {
      const owned = await forwardFactory(target, { spawnImpl, nodeCommand, timeoutMs, onClose: () => { tunnel = undefined; } });
      if (closed || identityMismatch) { owned.close(); assertOpen(); }
      tunnel = owned; return owned;
    })().finally(() => { tunnelPromise = undefined; });
    return tunnelPromise;
  }
  async function httpUpstream(timeoutMs, { signal } = {}) {
    const deadline = Date.now() + (timeoutMs ?? 30000);
    const remaining = () => { signal?.throwIfAborted(); const ms = deadline - Date.now(); if (ms < 1) throw new Error('Remote upstream budget exhausted'); return ms; };
    await ensureIdentity(Math.min(remaining(), 8000), { signal });
    const owned = await ensureTunnel(Math.min(remaining(), 8000));
    remaining();
    return new PaseoUpstream(owned.url, { ownedLoopback: true, ...(fetchImpl ? { fetchImpl } : {}) });
  }
  const upstream = {
    async tools() { return (await httpUpstream()).tools(); },
    async call(name, args, options) {
      // Never retry an ambiguous mutation. The DispatchStore retains the receipt.
      const started = Date.now();
      const client = await httpUpstream(options?.timeoutMs, { signal: options?.signal });
      const remaining = (options?.timeoutMs ?? 30000) - (Date.now() - started);
      if (remaining < 1) throw new Error('Remote upstream budget exhausted');
      options?.signal?.throwIfAborted();
      return client.call(name, args, { ...options, timeoutMs: remaining });
    },
  };
  async function readDaemon(args, timeoutMs = 8000) {
    const started = Date.now();
    await ensureIdentity(Math.min(timeoutMs, 8000));
    const remaining = timeoutMs - (Date.now() - started);
    if (remaining < 1) throw new Error('Remote native read budget exhausted');
    return invokeNative(DAEMON_READER, args, { target: target.uri, serverId: pinnedServerId }, remaining);
  }
  async function readTimeline(args, { timeoutMs = 30000, signal } = {}) {
    const started = Date.now();
    await ensureIdentity(Math.min(timeoutMs, 8000), { signal });
    const remaining = timeoutMs - (Date.now() - started);
    if (remaining < 1) throw new Error('Remote timeline read budget exhausted');
    return invokeNative(TIMELINE_READER, { ...args, timeoutMs: remaining },
      { target: target.uri, serverId: pinnedServerId }, remaining, { signal });
  }
  function observerFactory(options = {}) {
    assertOpen();
    const observer = new observerClass({ ...options, ...(spawnImpl ? { spawnImpl } : {}), target: target.uri,
      expectedServerId: () => { assertOpen(); return pinnedServerId; } });
    observers.add(observer); return observer;
  }
  function makeChannel(options) {
    assertOpen();
    const channel = channelFactory({ ...options, host: { ...host, target: target.uri }, target, runRemote: channelRemote,
      forwardFactory: (t, opts) => forwardFactory(t, { ...opts, nodeCommand, ...(spawnImpl ? { spawnImpl } : {}) }) });
    const prepare = channel.prepare;
    if (prepare) channel.prepare = async () => { await ensureIdentity(); return prepare(); };
    const issue = channel.issue;
    channel.issue = async agentId => { await ensureIdentity(); return issue(agentId); };
    channels.add(channel); return channel;
  }
  async function status() {
    if (closed) return { available: false, reason: 'closed' };
    if (identityMismatch) return { available: false, reason: 'daemon_identity_changed', ...(pinnedServerId ? { serverId: pinnedServerId } : {}) };
    try {
      const tools = await upstream.tools();
      if (!Array.isArray(tools)) throw new Error('Invalid catalog');
      return { available: true, serverId: pinnedServerId };
    } catch { return { available: false, reason: identityMismatch ? 'daemon_identity_changed' : 'remote_unavailable', ...(pinnedServerId ? { serverId: pinnedServerId } : {}) }; }
  }
  async function close() {
    if (closed) return; closed = true;
    for (const controller of nativeRequests) controller.abort();
    for (const observer of observers) observer.close();
    await Promise.allSettled([...channels].map(c => c.close()));
    tunnel?.close();
    if (tunnelPromise) await tunnelPromise.catch(() => {});
    runner.close();
  }
  return { upstream, paths, readDaemon, readTimeline, observerFactory, channelFactory: makeChannel, status, close };
}
