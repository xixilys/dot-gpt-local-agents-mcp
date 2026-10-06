import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdirSync, chmodSync } from 'node:fs';
import { join } from 'node:path';

export function dispatchFingerprint(tool, args) {
  return createHash('sha256').update(JSON.stringify(canonical({ tool, args }))).digest('hex');
}
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  }
  return value;
}

export class DispatchStore {
  constructor(stateDir) {
    mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    chmodSync(stateDir, 0o700);
    const filename = join(stateDir, 'dispatch.sqlite');
    this.db = new DatabaseSync(filename);
    chmodSync(filename, 0o600);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS dispatch_requests (
        request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, tool TEXT NOT NULL,
        submitted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        state TEXT NOT NULL, agent_id TEXT, result_json TEXT
      );`);
    // A crashed process may have sent a request without receiving its response.
    this.db.prepare("UPDATE dispatch_requests SET state='unknown', updated_at=? WHERE state='submitting'")
      .run(new Date().toISOString());
  }
  get(requestId) {
    const row = this.db.prepare('SELECT * FROM dispatch_requests WHERE request_id=?').get(requestId);
    if (!row) return undefined;
    return {
      requestId: row.request_id, fingerprint: row.fingerprint, tool: row.tool,
      submittedAt: row.submitted_at, updatedAt: row.updated_at, state: row.state,
      ...(row.agent_id ? { agentId: row.agent_id } : {}),
      ...(row.result_json ? { upstreamResult: JSON.parse(row.result_json) } : {}),
    };
  }
  begin(requestId, fingerprint, tool, agentId) {
    const now = new Date().toISOString();
    const result = this.db.prepare(`INSERT OR IGNORE INTO dispatch_requests
      (request_id, fingerprint, tool, submitted_at, updated_at, state, agent_id)
      VALUES (?, ?, ?, ?, ?, 'submitting', ?)`).run(requestId, fingerprint, tool, now, now, agentId ?? null);
    const record = this.get(requestId);
    if (record.fingerprint !== fingerprint) throw new Error('requestId already belongs to different parameters');
    return { claimed: result.changes === 1, record };
  }
  finish(requestId, state, upstreamResult, agentId) {
    this.db.prepare(`UPDATE dispatch_requests SET state=?, updated_at=?, result_json=?,
      agent_id=COALESCE(?, agent_id) WHERE request_id=?`).run(
      state, new Date().toISOString(), upstreamResult ? JSON.stringify(upstreamResult) : null,
      agentId ?? null, requestId);
    return this.get(requestId);
  }
  close() { this.db.close(); }
}

export function publicDispatchRecord(record, duplicate = false) {
  if (!record) return { found: false };
  const { fingerprint, ...safe } = record;
  return {
    found: true, ...safe, duplicate,
    ...(record.state === 'unknown' || record.state === 'submitting' ? {
      message: 'Submission outcome is unknown or still pending. This request will not be resent. Use get_dispatch_request; for creation also find the dotRequestId agent label. Do not submit a new requestId until you verify whether the original action ran.',
    } : {}),
    completionConfirmed: false,
  };
}
