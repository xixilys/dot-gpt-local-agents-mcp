import { DatabaseSync } from 'node:sqlite';
import { chmodSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

function request(row) {
  if (!row) return undefined;
  return { requestId: row.request_id, owner: row.owner, workspaceId: row.workspace_id,
    cwd: row.cwd, routeId: row.route_id, subscriptionId: row.subscription_id,
    agentId: row.agent_id, submittedAt: row.submitted_at, state: row.state,
    turnId: row.turn_id, markerSeq: row.marker_seq, epoch: row.epoch,
    terminalKind: row.terminal_kind, terminalAt: row.terminal_at,
    ...(row.result_json ? { result: JSON.parse(row.result_json) } : {}) };
}
function message(row) {
  if (!row) return undefined;
  return { messageId: row.message_id, requestId: row.request_id, agentId: row.agent_id,
    kind: row.kind, text: row.text, createdAt: row.created_at, emitted: Boolean(row.emitted) };
}

// A separate database adds routing without migrating or replacing dispatch receipts.
export class CollaborationStore {
  constructor(stateDir) {
    mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    this.filename = join(stateDir, 'collaboration.sqlite');
    this.db = new DatabaseSync(this.filename);
    chmodSync(this.filename, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS collaboration_requests (
        request_id TEXT PRIMARY KEY, owner TEXT NOT NULL, workspace_id TEXT NOT NULL,
        cwd TEXT NOT NULL, route_id TEXT, subscription_id TEXT, agent_id TEXT,
        submitted_at TEXT NOT NULL, state TEXT NOT NULL, turn_id TEXT, marker_seq INTEGER,
        epoch TEXT, terminal_kind TEXT, terminal_at TEXT, result_json TEXT
      );
      CREATE INDEX IF NOT EXISTS collaboration_agent ON collaboration_requests(agent_id, submitted_at);
      CREATE TABLE IF NOT EXISTS agent_messages (
        message_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, agent_id TEXT NOT NULL,
        kind TEXT NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL,
        emitted INTEGER NOT NULL DEFAULT 0, fingerprint TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS source_events (
        source_key TEXT PRIMARY KEY, seen_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS message_replies (
        message_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
        fingerprint TEXT NOT NULL, claimed_at TEXT NOT NULL
      );`);
  }
  get(requestId) { return request(this.db.prepare('SELECT * FROM collaboration_requests WHERE request_id=?').get(requestId)); }
  begin({ requestId, owner, workspaceId, cwd, routeId, subscriptionId, agentId, submittedAt }) {
    const old = this.get(requestId);
    if (old) {
      if (old.owner !== owner || old.workspaceId !== workspaceId || old.agentId && agentId && old.agentId !== agentId) {
        throw new Error('Request belongs to a different authenticated route or agent');
      }
      return old;
    }
    this.db.prepare(`INSERT INTO collaboration_requests
      (request_id,owner,workspace_id,cwd,route_id,subscription_id,agent_id,submitted_at,state)
      VALUES (?,?,?,?,?,?,?,?, 'submitting')`).run(requestId, owner, workspaceId, cwd,
      routeId ?? null, subscriptionId ?? null, agentId ?? null, submittedAt);
    return this.get(requestId);
  }
  bindRoute(requestId, routeId, subscriptionId) {
    const r = this.get(requestId);
    if (!r) throw new Error('Unknown dispatch request');
    if ((r.routeId !== null || r.subscriptionId !== null)
      && (r.routeId !== routeId || r.subscriptionId !== subscriptionId)) {
      throw new Error('Request is already bound to a different route or subscription');
    }
    this.db.prepare(`UPDATE collaboration_requests SET route_id=?,subscription_id=?
      WHERE request_id=? AND route_id IS NULL AND subscription_id IS NULL`).run(routeId, subscriptionId, requestId);
    const bound = this.get(requestId);
    if (bound.routeId !== routeId || bound.subscriptionId !== subscriptionId) throw new Error('Request route binding changed');
    return bound;
  }
  submitted(requestId, state, agentId) {
    this.db.prepare('UPDATE collaboration_requests SET state=?,agent_id=COALESCE(?,agent_id) WHERE request_id=?')
      .run(state, agentId ?? null, requestId);
    return this.get(requestId);
  }
  latest(agentId) {
    return request(this.db.prepare('SELECT * FROM collaboration_requests WHERE agent_id=? ORDER BY submitted_at DESC, rowid DESC LIMIT 1').get(agentId));
  }
  forAgent(agentId) { return this.db.prepare('SELECT * FROM collaboration_requests WHERE agent_id=? ORDER BY submitted_at,rowid').all(agentId).map(request); }
  byTurn(agentId, turnId) {
    return this.db.prepare('SELECT * FROM collaboration_requests WHERE agent_id=? AND turn_id=?').all(agentId, turnId).map(request);
  }
  routedRequests() { return this.db.prepare("SELECT * FROM collaboration_requests WHERE subscription_id IS NOT NULL AND state!='rejected'").all().map(request); }
  unboundRequests() { return this.db.prepare("SELECT * FROM collaboration_requests WHERE agent_id IS NULL AND state IN ('unknown','submitting')").all().map(request); }
  markTurn(requestId, { turnId, markerSeq, epoch }) {
    const r = this.get(requestId);
    if (!r) throw new Error('Unknown routed request');
    if (r.turnId && (r.turnId !== turnId || r.epoch && epoch && r.epoch !== epoch)) return false;
    this.db.prepare('UPDATE collaboration_requests SET turn_id=?,marker_seq=?,epoch=? WHERE request_id=?')
      .run(turnId, markerSeq ?? null, epoch ?? null, requestId);
    return true;
  }
  saveResult(requestId, result, terminalKind, timestamp) {
    const raw = JSON.stringify(result);
    if (Buffer.byteLength(raw) > 1024 * 1024) throw new Error('Matched request result exceeds storage limit');
    this.db.prepare('UPDATE collaboration_requests SET result_json=?,terminal_kind=?,terminal_at=? WHERE request_id=?')
      .run(raw, terminalKind, timestamp, requestId);
    return this.get(requestId);
  }
  putMessage(input) {
    const { messageId, requestId, agentId, kind, text } = input;
    const fingerprint = createHash('sha256').update(JSON.stringify({ requestId, agentId, kind, text })).digest('hex');
    const old = this.db.prepare('SELECT * FROM agent_messages WHERE message_id=?').get(messageId);
    if (old) {
      if (old.fingerprint !== fingerprint) throw new Error('Message ID already belongs to different content');
      return { duplicate: true, message: message(old) };
    }
    this.db.prepare(`INSERT INTO agent_messages(message_id,request_id,agent_id,kind,text,created_at,fingerprint)
      VALUES (?,?,?,?,?,?,?)`).run(messageId, requestId, agentId, kind, text, new Date().toISOString(), fingerprint);
    return { duplicate: false, message: this.getMessage(messageId) };
  }
  getMessage(id) { return message(this.db.prepare('SELECT * FROM agent_messages WHERE message_id=?').get(id)); }
  pendingMessages(requestId) {
    return this.db.prepare('SELECT * FROM agent_messages WHERE request_id=? AND emitted=0 ORDER BY created_at,message_id').all(requestId).map(message);
  }
  markEmitted(id) { this.db.prepare('UPDATE agent_messages SET emitted=1 WHERE message_id=?').run(id); }
  hasDecision(requestId) { return Boolean(this.db.prepare("SELECT 1 FROM agent_messages WHERE request_id=? AND kind='needs_input'").get(requestId)); }
  replyFor(messageId) {
    const r = this.db.prepare('SELECT * FROM message_replies WHERE message_id=?').get(messageId);
    return r ? { messageId, requestId: r.request_id, fingerprint: r.fingerprint } : undefined;
  }
  claimReply(messageId, requestId, fingerprint) {
    const old = this.replyFor(messageId);
    if (old) {
      if (old.fingerprint !== fingerprint) throw new Error('This message already has a different reply; inspect its reply receipt');
      return { claimed: false, record: old };
    }
    this.db.prepare('INSERT INTO message_replies VALUES (?,?,?,?)').run(messageId, requestId, fingerprint, new Date().toISOString());
    return { claimed: true, record: this.replyFor(messageId) };
  }
  claimSource(key) {
    return this.db.prepare('INSERT OR IGNORE INTO source_events(source_key,seen_at) VALUES (?,?)').run(key, new Date().toISOString()).changes === 1;
  }
  sourceSeen(key) { return Boolean(this.db.prepare('SELECT 1 FROM source_events WHERE source_key=?').get(key)); }
  close() { this.db.close(); }
}
