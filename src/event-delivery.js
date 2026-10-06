import https from 'node:https';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import ipaddr from 'ipaddr.js';
import { Webhook } from 'standardwebhooks';
import { canonicalJSON, subscriptionId } from './event-store.js';

const DAY_MS = 86_400_000;
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_RESPONSE_BYTES = 16 * 1024;

export class CallbackEndpointError extends Error {
  constructor(reason) {
    super(`Callback endpoint verification failed (${reason})`);
    this.name = 'CallbackEndpointError';
    this.code = -32015;
    this.data = { reason };
  }
}

export function callbackURL(value) {
  let url;
  try { url = new URL(value); } catch { throw new CallbackEndpointError('invalid_url'); }
  if (typeof value !== 'string' || url.protocol !== 'https:' || !url.hostname || url.username || url.password ||
      value.includes('#') || url.hash) throw new CallbackEndpointError('invalid_url');
  return url;
}

export function isPublicAddress(address) {
  if (!isIP(address)) return false;
  const parsed = ipaddr.parse(address);
  if (parsed.range() !== 'unicast') return false;
  // IPv6 unallocated space must not pass merely because ipaddr calls it unicast.
  return parsed.kind() === 'ipv4' || parsed.match(ipaddr.parseCIDR('2000::/3'));
}

/** Outbound-only HTTPS client. Every call resolves all addresses, rejects any
 * non-public answer, and pins a checked address for a fresh TLS connection.
 * Tests may inject resolve/request here; no production endpoint exposes them.
 */
export class SafeCallbackClient {
  constructor({ resolve = lookup, request = https.request, timeoutMs = 10_000 } = {}) {
    this.resolve = resolve;
    this.request = request;
    this.timeoutMs = timeoutMs;
  }
  async post(value, { body, headers, maxResponseBytes = MAX_RESPONSE_BYTES, signal }) {
    const url = callbackURL(value);
    if (Buffer.byteLength(body, 'utf8') > MAX_EVENT_BYTES) throw new CallbackEndpointError('payload_too_large');
    const hostname = url.hostname.replace(/^\[|\]$/g, '');
    // The deadline covers DNS, TCP, TLS, and response. No delayed DNS completion
    // may start a connection after the caller has already timed out.
    return new Promise((resolve, reject) => {
      let finished = false;
      let req;
      let response;
      const aborted = () => finish(new CallbackEndpointError('subscription_cancelled'));
      const finish = (error, result) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', aborted);
        if (error) { req?.destroy(); response?.destroy(); reject(error); }
        else resolve(result);
      };
      const timer = setTimeout(() => finish(new CallbackEndpointError('timeout')), this.timeoutMs);
      if (signal?.aborted) { aborted(); return; }
      signal?.addEventListener('abort', aborted, { once: true });
      const addresses = isIP(hostname)
        ? Promise.resolve([{ address: hostname, family: isIP(hostname) }])
        : Promise.resolve().then(() => this.resolve(hostname, { all: true, verbatim: true }));
      addresses.then(records => {
        if (finished) return;
        if (!Array.isArray(records) || !records.length || records.some(r =>
          !r || !isPublicAddress(r.address) || isIP(r.address) !== r.family)) {
          finish(new CallbackEndpointError('unsafe_destination')); return;
        }
        const pinned = records[0];
        const pinnedLookup = (_name, options, callback) => {
          if (typeof options === 'function') { callback = options; options = {}; }
          if (options?.all) callback(null, [pinned]);
          else callback(null, pinned.address, pinned.family);
        };
        try {
          req = this.request({
            protocol: 'https:', hostname, port: url.port || 443, path: `${url.pathname}${url.search}`,
            method: 'POST', headers: { ...headers, 'Content-Length': Buffer.byteLength(body, 'utf8') },
            lookup: pinnedLookup, family: pinned.family, agent: false,
            servername: isIP(hostname) ? '' : hostname, rejectUnauthorized: true,
          }, res => {
            response = res;
            if (finished) { res.destroy(); return; }
            const status = res.statusCode;
            if (!Number.isInteger(status)) { finish(new CallbackEndpointError('transport_error')); return; }
            if (status >= 300 && status < 400) {
              finish(new CallbackEndpointError('redirect')); return;
            }
            // Delivery acknowledgements have no application response. Limit and
            // discard them too; verification reads at most 16 KiB.
            if (Number(res.headers?.['content-length']) > maxResponseBytes) {
              const error = new CallbackEndpointError('response_too_large');
              error.status = status;
              finish(error); return;
            }
            const chunks = [];
            let size = 0;
            res.on('data', chunk => {
              if (finished) return;
              size += Buffer.byteLength(chunk);
              if (size > maxResponseBytes) {
                const error = new CallbackEndpointError('response_too_large');
                error.status = status;
                finish(error); return;
              }
              chunks.push(Buffer.from(chunk));
            });
            res.on('end', () => finish(null, { status, body: Buffer.concat(chunks).toString('utf8') }));
            res.on('aborted', () => finish(new CallbackEndpointError('transport_error')));
            res.on('error', () => finish(new CallbackEndpointError('transport_error')));
          });
          req.on('error', () => finish(new CallbackEndpointError('transport_error')));
          req.end(body);
        } catch { finish(new CallbackEndpointError('transport_error')); }
      }, () => finish(new CallbackEndpointError('dns_error')));
    });
  }
}

function signingSecret(secret) {
  if (typeof secret !== 'string' || !/^whsec_[A-Za-z0-9+/]+={0,2}$/.test(secret)) {
    throw new TypeError('Invalid webhook signing secret');
  }
  const encoded = secret.slice(6);
  const decoded = Buffer.from(encoded, 'base64');
  if (decoded.length < 24 || decoded.length > 64 ||
      decoded.toString('base64').replace(/=+$/, '') !== encoded.replace(/=+$/, '')) {
    throw new TypeError('Invalid webhook signing secret');
  }
  // Also require the decoder used by the signing library to accept it.
  try { new Webhook(secret); } catch { throw new TypeError('Invalid webhook signing secret'); }
  return secret;
}

function identityInputs(name, args, owner) {
  if (typeof owner !== 'string' || !owner || typeof name !== 'string' || !name ||
      !args || typeof args !== 'object' || Array.isArray(args)) throw new TypeError('Invalid subscription identity');
  canonicalJSON(args);
}

function signedHeaders(subscription, id, body, now) {
  const timestamp = new Date(now);
  const signatures = [new Webhook(subscription.secret).sign(id, timestamp, body)];
  if (subscription.previousSecret && subscription.previousSecretUntil > now) {
    signatures.push(new Webhook(subscription.previousSecret).sign(id, timestamp, body));
  }
  return { 'Content-Type': 'application/json', 'webhook-id': id,
    'webhook-timestamp': String(Math.floor(now / 1000)), 'webhook-signature': signatures.join(' '),
    'X-MCP-Subscription-Id': subscription.id };
}

/** Integration:
 * subscribe(name, args, delivery, ttlMs, cursor, owner) -> protocol result;
 * unsubscribe(name, args, delivery, owner) -> {}.
 * The authenticated caller must validate catalog schemas and authorize filters.
 * Optional validateSubscription/authorizeSubscription receive (name,args,owner).
 * enqueue(event, matching) and publish(name,data,matching,{eventId,timestamp})
 * persist one exact JSON body and an outbox row per matching active subscription.
 * matching(subscription,event) is synchronous and receives secret-free internal
 * owner/filter metadata. Supply it whenever the event is not broadcast data.
 * authorizeSubscription is also checked before every delivery; false revokes.
 * start()/stop() own only a light, in-process worker; flushDue() can run manually.
 * 2xx means received, and never means dot execution or completion.
 */
export class EventDelivery {
  constructor({ store, callbackClient = new SafeCallbackClient(), now = Date.now,
    validateSubscription = () => true, authorizeSubscription = () => true,
    pollIntervalMs = 1000, onError = () => {} }) {
    this.store = store;
    this.callbackClient = callbackClient;
    this.now = now;
    this.validateSubscription = validateSubscription;
    this.authorizeSubscription = authorizeSubscription;
    this.pollIntervalMs = pollIntervalMs;
    this.onError = onError;
    this.verifications = new Map();
    this.subscriptionLocks = new Map();
    this.revisions = new Map();
    this.ownerRevisions = new Map();
    this.activeRequests = new Map();
    this.flushPromise = null;
    this.timer = null;
    this.stopping = false;
  }

  async subscribe(name, args, delivery, ttlMs, _cursor, owner) {
    identityInputs(name, args, owner);
    if (!delivery || delivery.mode !== 'webhook') throw new TypeError('Only webhook delivery is supported');
    const url = callbackURL(delivery.url).href;
    const secret = signingSecret(delivery.secret);
    const lifetime = ttlMs === undefined ? DAY_MS : ttlMs === null ? 7 * DAY_MS : ttlMs;
    if (!Number.isSafeInteger(lifetime) || lifetime <= 0 || !Number.isSafeInteger(this.now() + lifetime) ||
        !Number.isFinite(new Date(this.now() + lifetime).getTime())) {
      throw new TypeError('ttlMs must be a positive, finite lifetime');
    }
    // Capture an immutable filter before any asynchronous validation or callback.
    args = JSON.parse(canonicalJSON(args));
    const id = subscriptionId(owner, url, name, args);
    const revision = this.revisions.get(id) ?? 0;
    const ownerRevision = this.ownerRevisions.get(owner) ?? 0;
    const cancelled = () => (this.revisions.get(id) ?? 0) !== revision ||
      (this.ownerRevisions.get(owner) ?? 0) !== ownerRevision;
    if (await this.validateSubscription(name, args, owner) === false) throw new TypeError('Invalid event filter');
    if (await this.authorizeSubscription(name, args, owner) === false) throw new Error('Event subscription is not authorized');
    const prior = this.subscriptionLocks.get(id) ?? Promise.resolve();
    const action = prior.catch(() => {}).then(async () => {
      if (cancelled()) throw new Error('Subscription was cancelled during verification');
      await this.verifyCallback({ id, owner, url, secret });
      if (cancelled()) throw new Error('Subscription was cancelled during verification');
      // Recheck authorization after verification, which may take ten seconds.
      if (await this.authorizeSubscription(name, args, owner) === false) throw new Error('Event subscription is not authorized');
      if (cancelled()) throw new Error('Subscription was cancelled during verification');
      const now = this.now();
      const expiresAt = now + lifetime;
      this.store.saveSubscription({ id, owner, name, arguments: args, url, secret, expiresAt }, now);
      return { id, refreshBefore: new Date(expiresAt).toISOString(), cursor: null, truncated: false };
    });
    this.subscriptionLocks.set(id, action);
    try { return await action; }
    finally { if (this.subscriptionLocks.get(id) === action) this.subscriptionLocks.delete(id); }
  }

  async verifyCallback(subscription) {
    const key = createHash('sha256').update(canonicalJSON({ owner: subscription.owner,
      url: subscription.url, secret: subscription.secret })).digest('hex');
    if (this.store.isVerified(key, this.now())) return;
    if (this.verifications.has(key)) return this.verifications.get(key);
    const action = (async () => {
      const challenge = randomBytes(32).toString('base64url');
      const body = JSON.stringify({ type: 'verification', challenge });
      const id = `msg_verification_${randomUUID()}`;
      let response;
      try {
        response = await this.callbackClient.post(subscription.url, {
          body, headers: signedHeaders(subscription, id, body, this.now()), maxResponseBytes: MAX_RESPONSE_BYTES,
        });
      } catch (error) {
        throw error instanceof CallbackEndpointError ? error : new CallbackEndpointError('transport_error');
      }
      if (!response || !Number.isInteger(response.status) || response.status < 200 || response.status >= 300) {
        throw new CallbackEndpointError('http_error');
      }
      if (typeof response.body !== 'string' || Buffer.byteLength(response.body, 'utf8') > MAX_RESPONSE_BYTES) {
        throw new CallbackEndpointError('response_too_large');
      }
      let echoed;
      try { echoed = JSON.parse(response.body).challenge; } catch {}
      // Compare fixed-sized hashes to avoid an early-exit length/content compare.
      const expectedHash = createHash('sha256').update(challenge).digest();
      const receivedHash = createHash('sha256').update(typeof echoed === 'string' ? echoed : '').digest();
      if (!timingSafeEqual(expectedHash, receivedHash)) throw new CallbackEndpointError('challenge_failed');
      this.store.rememberVerification(key, this.now() + 5 * 60_000);
    })();
    this.verifications.set(key, action);
    try { await action; }
    finally { if (this.verifications.get(key) === action) this.verifications.delete(key); }
  }

  unsubscribe(name, args, delivery, owner) {
    identityInputs(name, args, owner);
    if (!delivery || delivery.mode !== 'webhook') throw new TypeError('Only webhook delivery is supported');
    const url = callbackURL(delivery.url).href;
    const id = subscriptionId(owner, url, name, args);
    this.stopSubscription(id, owner);
    return {};
  }

  stopSubscription(id, owner, reason = 'unsubscribed') {
    const sub = this.store.getSubscriptionInternal(id);
    if (sub && sub.owner !== owner) return;
    this.revisions.set(id, (this.revisions.get(id) ?? 0) + 1);
    this.store.stopSubscription(id, owner, this.now(), reason);
    this.activeRequests.get(id)?.abort();
  }
  revokeOwner(owner) {
    this.ownerRevisions.set(owner, (this.ownerRevisions.get(owner) ?? 0) + 1);
    for (const sub of this.store.activeSubscriptions({ owner, now: this.now() })) {
      this.stopSubscription(sub.id, owner, 'access_revoked');
    }
  }

  enqueue(event, matching = () => true) {
    if (!event || typeof event.eventId !== 'string' || !/^[A-Za-z0-9._:-]{1,200}$/.test(event.eventId) ||
        typeof event.name !== 'string' || !event.name || typeof event.timestamp !== 'string' ||
        !/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(event.timestamp) || !Number.isFinite(Date.parse(event.timestamp)) ||
        !event.data || typeof event.data !== 'object' || Array.isArray(event.data) ||
        (event.cursor !== undefined && event.cursor !== null) || event.type !== undefined) {
      throw new TypeError('Invalid MCP event');
    }
    // Explicit fields keep application fields inside data and cursor honest.
    const normalized = { eventId: event.eventId, name: event.name, timestamp: event.timestamp,
      data: event.data, cursor: null };
    canonicalJSON(normalized);
    const body = JSON.stringify(normalized);
    if (Buffer.byteLength(body, 'utf8') > MAX_EVENT_BYTES) throw new RangeError('Event payload exceeds 256 KiB');
    const now = this.now();
    const subscriptions = this.store.activeSubscriptions({ name: event.name, now });
    const ids = subscriptions.filter(sub => {
      const result = matching(sub, normalized);
      if (result?.then) throw new TypeError('Event matching must be synchronous');
      return Boolean(result);
    }).map(sub => sub.id);
    return this.store.recordEvent(normalized, ids, now);
  }

  publish(name, data, matching, { eventId = `evt_${randomUUID()}`, timestamp = new Date(this.now()).toISOString() } = {}) {
    return this.enqueue({ eventId, name, timestamp, data, cursor: null }, matching);
  }

  flushDue() {
    if (this.flushPromise) return this.flushPromise;
    const action = this.flushBatch();
    this.flushPromise = action;
    action.finally(() => { if (this.flushPromise === action) this.flushPromise = null; }).catch(() => {});
    return action;
  }
  async flushBatch() {
    let attempted = 0;
    for (const row of this.store.dueDeliveries(this.now())) {
      if (this.stopping) break;
      const sub = this.store.getSubscriptionInternal(row.subscriptionId);
      if (!sub || sub.state !== 'active' || sub.expiresAt <= this.now()) continue;
      if (await this.authorizeSubscription(sub.name, sub.arguments, sub.owner) === false) {
        this.store.stopSubscription(sub.id, sub.owner, this.now(), 'access_revoked'); continue;
      }
      // Claim after asynchronous authorization, so cancellation still wins.
      const job = this.store.claimDelivery(row.subscriptionId, row.eventId, this.now());
      if (!job) continue;
      attempted++;
      let status = null;
      let reason = null;
      const controller = new AbortController();
      this.activeRequests.set(job.subscription.id, controller);
      const remaining = job.subscription.expiresAt - this.now();
      // The client already enforces ten seconds; only schedule a shorter expiry.
      const expiryTimer = remaining <= 10_000 ? setTimeout(() => controller.abort(), Math.max(0, remaining)) : null;
      try {
        const response = await this.callbackClient.post(job.subscription.url, {
          body: job.body, headers: signedHeaders(job.subscription, job.eventId, job.body, this.now()),
          maxResponseBytes: MAX_RESPONSE_BYTES,
          signal: controller.signal,
        });
        if (!Number.isInteger(response.status)) throw new CallbackEndpointError('transport_error');
        status = response.status;
      } catch (error) {
        // Never persist raw errors, request URLs, secrets, or callback response bodies.
        reason = error instanceof CallbackEndpointError ? error.data.reason : 'transport_error';
        // A bounded response failure must not erase a known 410/413 status.
        if (error instanceof CallbackEndpointError && Number.isInteger(error.status)) status = error.status;
      } finally {
        if (expiryTimer) clearTimeout(expiryTimer);
        if (this.activeRequests.get(job.subscription.id) === controller) this.activeRequests.delete(job.subscription.id);
      }
      const transient = status === 429 || (status >= 500 && status < 600) ||
        (status === null && ['transport_error', 'timeout', 'dns_error'].includes(reason));
      const received = status >= 200 && status < 300;
      const retry = transient && job.attempt < 5;
      this.store.finishDelivery(job.subscription.id, job.eventId, job.attempt, {
        state: received ? 'received' : retry ? 'retry' : 'failed', status,
        reason: received ? null : reason ?? (status === 413 ? 'payload_rejected' :
          status === 410 ? 'subscription_gone' : retry ? 'transient_http' : 'http_error'),
        nextAt: retry ? this.now() + 1000 * (2 ** (job.attempt - 1)) : null,
      }, this.now());
      if (status === 410) this.store.stopSubscription(job.subscription.id, job.subscription.owner, this.now(), 'subscription_gone');
    }
    return { attempted };
  }
  start() {
    if (this.timer) return;
    this.stopping = false;
    const tick = () => this.flushDue().catch(() => this.onError({ reason: 'event_worker_error' }));
    this.timer = setInterval(tick, this.pollIntervalMs);
    this.timer.unref();
    tick();
  }
  async stop() {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.flushPromise) await this.flushPromise;
  }
}
