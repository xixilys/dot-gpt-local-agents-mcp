import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdirSync, chmodSync, existsSync, openSync, closeSync } from 'node:fs';
import { join } from 'node:path';

// JSON identity is independent of object key order. Reject values JSON would drop
// or silently change; subscription IDs must describe precisely the saved filter.
export function canonicalJSON(value) {
  function normalize(v) {
    if (v === null || typeof v === 'string' || typeof v === 'boolean') return v;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
    if (Array.isArray(v)) return v.map(normalize);
    if (v && typeof v === 'object' && Object.getPrototypeOf(v) === Object.prototype) {
      return Object.fromEntries(Object.keys(v).sort().map(key => [key, normalize(v[key])]));
    }
    throw new TypeError('Expected a JSON value');
  }
  return JSON.stringify(normalize(value));
}

export function subscriptionId(owner, url, name, args) {
  return `sub_${createHash('sha256').update(canonicalJSON({ owner, url, name, args })).digest('hex')}`;
}

function subscription(row, includeSecrets = false) {
  if (!row) return undefined;
  return {
    id: row.id, owner: row.owner, name: row.name, arguments: JSON.parse(row.arguments_json),
    url: row.callback_url, state: row.state, expiresAt: row.expires_at,
    createdAt: row.created_at, updatedAt: row.updated_at,
    ...(includeSecrets ? { secret: row.secret, previousSecret: row.previous_secret,
      previousSecretUntil: row.previous_secret_until } : {}),
  };
}

/** Internal persistence API: activeSubscriptions is secret-free, but contains
 * owner/filter metadata for routing. Never return it directly from a tool.
 * getSubscriptionInternal/claimDelivery are exclusively for the delivery worker.
 * Delivery/attempt evidence never contains callback secrets or response bodies.
 */
export class EventStore {
  constructor(stateDir) {
    mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    chmodSync(stateDir, 0o700);
    this.filename = join(stateDir, 'events.sqlite');
    // Create with private permissions before SQLite can write any secrets.
    closeSync(openSync(this.filename, 'a', 0o600));
    chmodSync(this.filename, 0o600);
    this.db = new DatabaseSync(this.filename);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS event_subscriptions (
        id TEXT PRIMARY KEY, owner TEXT NOT NULL, name TEXT NOT NULL,
        arguments_json TEXT NOT NULL, callback_url TEXT NOT NULL, secret TEXT NOT NULL,
        previous_secret TEXT, previous_secret_until INTEGER,
        state TEXT NOT NULL, expires_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS callback_verifications (
        cache_key TEXT PRIMARY KEY, expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY, name TEXT NOT NULL, body TEXT NOT NULL,
        identity TEXT NOT NULL, created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS event_outbox (
        subscription_id TEXT NOT NULL REFERENCES event_subscriptions(id),
        event_id TEXT NOT NULL REFERENCES events(event_id),
        state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        next_at INTEGER NOT NULL, last_at INTEGER, http_status INTEGER, reason TEXT,
        received_at INTEGER, PRIMARY KEY (subscription_id, event_id)
      );
      CREATE INDEX IF NOT EXISTS event_outbox_due ON event_outbox(state, next_at);
      CREATE TABLE IF NOT EXISTS event_delivery_attempts (
        subscription_id TEXT NOT NULL, event_id TEXT NOT NULL, attempt INTEGER NOT NULL,
        started_at INTEGER NOT NULL, completed_at INTEGER, http_status INTEGER, reason TEXT,
        PRIMARY KEY(subscription_id,event_id,attempt)
      );`);
    this.secureFiles();
  }

  secureFiles() {
    for (const filename of [this.filename, `${this.filename}-wal`, `${this.filename}-shm`]) {
      if (existsSync(filename)) chmodSync(filename, 0o600);
    }
  }
  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try { const result = fn(); this.db.exec('COMMIT'); this.secureFiles(); return result; }
    catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  getSubscriptionInternal(id) {
    return subscription(this.db.prepare('SELECT * FROM event_subscriptions WHERE id=?').get(id), true);
  }
  activeSubscriptions({ owner, name, now = Date.now() } = {}) {
    return this.db.prepare(`SELECT * FROM event_subscriptions WHERE state='active' AND expires_at>?
      AND (? IS NULL OR owner=?) AND (? IS NULL OR name=?) ORDER BY created_at,id`)
      .all(now, owner ?? null, owner ?? null, name ?? null, name ?? null).map(row => subscription(row));
  }
  saveSubscription({ id, owner, name, arguments: args, url, secret, expiresAt }, now, rotationMs = 300_000) {
    return this.transaction(() => {
      const old = this.getSubscriptionInternal(id);
      const rotated = old && old.secret !== secret && old.state === 'active' && old.expiresAt > now;
      const previousSecret = rotated ? old.secret : (old?.previousSecretUntil > now ? old.previousSecret : null);
      const previousUntil = rotated ? now + rotationMs : (previousSecret ? old.previousSecretUntil : null);
      this.db.prepare(`INSERT INTO event_subscriptions
        (id,owner,name,arguments_json,callback_url,secret,previous_secret,previous_secret_until,
         state,expires_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,'active',?,?,?)
        ON CONFLICT(id) DO UPDATE SET secret=excluded.secret,previous_secret=excluded.previous_secret,
         previous_secret_until=excluded.previous_secret_until,state='active',
         expires_at=excluded.expires_at,updated_at=excluded.updated_at`)
        .run(id, owner, name, canonicalJSON(args), url, secret, previousSecret, previousUntil,
          expiresAt, now, now);
      return subscription(this.db.prepare('SELECT * FROM event_subscriptions WHERE id=?').get(id));
    });
  }
  stopSubscription(id, owner, now = Date.now(), reason = 'unsubscribed') {
    return this.transaction(() => {
      this.db.prepare(`UPDATE event_subscriptions SET state='stopped',updated_at=?
        WHERE id=? AND owner=?`).run(now, id, owner);
      this.db.prepare(`UPDATE event_outbox SET state='cancelled',reason=?
        WHERE subscription_id=? AND state IN ('queued','retry','sending')
        AND EXISTS (SELECT 1 FROM event_subscriptions WHERE id=? AND owner=? AND state='stopped')`)
        .run(reason, id, id, owner);
    });
  }
  revokeOwner(owner, now = Date.now()) {
    for (const sub of this.activeSubscriptions({ owner, now })) {
      this.stopSubscription(sub.id, owner, now, 'access_revoked');
    }
  }
  isVerified(key, now) {
    return Boolean(this.db.prepare('SELECT 1 FROM callback_verifications WHERE cache_key=? AND expires_at>?').get(key, now));
  }
  rememberVerification(key, expiresAt) {
    this.db.prepare(`INSERT INTO callback_verifications VALUES (?,?) ON CONFLICT(cache_key)
      DO UPDATE SET expires_at=excluded.expires_at`).run(key, expiresAt);
    this.secureFiles();
  }
  recordEvent(event, subscriptionIds, now = Date.now()) {
    const body = JSON.stringify(event);
    const identity = canonicalJSON(event);
    return this.transaction(() => {
      const inserted = this.db.prepare('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)')
        .run(event.eventId, event.name, body, identity, now).changes === 1;
      const old = this.db.prepare('SELECT identity FROM events WHERE event_id=?').get(event.eventId);
      if (old.identity !== identity) throw new Error('eventId already belongs to different event data');
      let enqueued = 0;
      for (const id of new Set(subscriptionIds)) {
        enqueued += Number(this.db.prepare(`INSERT OR IGNORE INTO event_outbox
          (subscription_id,event_id,state,next_at) SELECT id,?,'queued',? FROM event_subscriptions
          WHERE id=? AND name=? AND state='active' AND expires_at>?`)
          .run(event.eventId, now, id, event.name, now).changes);
      }
      return { eventId: event.eventId, enqueued, duplicate: !inserted };
    });
  }
  dueDeliveries(now = Date.now(), limit = 50) {
    // Rows retain evidence after expiration/cancellation; refresh never revives them.
    this.db.prepare(`UPDATE event_outbox SET state='expired',reason='subscription_expired'
      WHERE state IN ('queued','retry','sending') AND subscription_id IN
      (SELECT id FROM event_subscriptions WHERE expires_at<=?)`).run(now);
    return this.db.prepare(`SELECT o.subscription_id AS subscriptionId,o.event_id AS eventId
      FROM event_outbox o JOIN event_subscriptions s ON s.id=o.subscription_id
      WHERE s.state='active' AND s.expires_at>? AND o.state IN ('queued','retry','sending')
      AND o.next_at<=? ORDER BY o.next_at,o.event_id LIMIT ?`).all(now, now, limit);
  }
  claimDelivery(subscriptionId, eventId, now = Date.now(), leaseMs = 30_000) {
    return this.transaction(() => {
      const sub = this.getSubscriptionInternal(subscriptionId);
      if (!sub || sub.state !== 'active' || sub.expiresAt <= now) return undefined;
      const old = this.db.prepare('SELECT * FROM event_outbox WHERE subscription_id=? AND event_id=?')
        .get(subscriptionId, eventId);
      if (!old || !['queued', 'retry', 'sending'].includes(old.state) || old.next_at > now) return undefined;
      if (old.attempts >= 5) {
        this.db.prepare(`UPDATE event_outbox SET state='failed',reason='attempt_limit'
          WHERE subscription_id=? AND event_id=?`).run(subscriptionId, eventId);
        return undefined;
      }
      const attempt = old.attempts + 1;
      this.db.prepare(`UPDATE event_outbox SET state='sending',attempts=?,last_at=?,next_at=?
        WHERE subscription_id=? AND event_id=?`).run(attempt, now, now + leaseMs, subscriptionId, eventId);
      this.db.prepare('INSERT INTO event_delivery_attempts VALUES (?,?,?,?,NULL,NULL,NULL)')
        .run(subscriptionId, eventId, attempt, now);
      return { subscription: sub, eventId, attempt,
        body: this.db.prepare('SELECT body FROM events WHERE event_id=?').get(eventId).body };
    });
  }
  finishDelivery(subscriptionId, eventId, attempt, { state, status = null, reason = null, nextAt = null }, now = Date.now()) {
    this.transaction(() => {
      this.db.prepare(`UPDATE event_delivery_attempts SET completed_at=?,http_status=?,reason=?
        WHERE subscription_id=? AND event_id=? AND attempt=?`).run(now, status, reason, subscriptionId, eventId, attempt);
      // Unsubscribe can happen while a request is in flight. Keep its cancelled state.
      this.db.prepare(`UPDATE event_outbox SET state=?,http_status=?,reason=?,next_at=COALESCE(?,next_at),
        received_at=? WHERE subscription_id=? AND event_id=? AND attempts=? AND state='sending'`)
        .run(state, status, reason, nextAt, state === 'received' ? now : null, subscriptionId, eventId, attempt);
    });
  }
  listDeliveries({ subscriptionId: id, eventId } = {}) {
    return this.db.prepare(`SELECT subscription_id AS subscriptionId,event_id AS eventId,state,attempts,
      next_at AS nextAt,last_at AS lastAt,http_status AS httpStatus,reason,received_at AS receivedAt
      FROM event_outbox WHERE (? IS NULL OR subscription_id=?) AND (? IS NULL OR event_id=?)
      ORDER BY event_id,subscription_id`).all(id ?? null, id ?? null, eventId ?? null, eventId ?? null);
  }
  deliveryAttempts({ subscriptionId: id, eventId } = {}) {
    return this.db.prepare(`SELECT subscription_id AS subscriptionId,event_id AS eventId,attempt,
      started_at AS startedAt,completed_at AS completedAt,http_status AS httpStatus,reason
      FROM event_delivery_attempts WHERE (? IS NULL OR subscription_id=?) AND (? IS NULL OR event_id=?)
      ORDER BY started_at,attempt`).all(id ?? null, id ?? null, eventId ?? null, eventId ?? null);
  }
  close() { this.db.close(); }
}
