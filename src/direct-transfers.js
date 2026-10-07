import Ajv from 'ajv';
import * as fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { join, posix } from 'node:path';
import { randomBytes, createHash } from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { directProjects, projectFor, assertOwner } from './direct-policy.js';
import { createBinaryFileTransport } from './binary-file-transport.js';
import { SafeFileDownloadClient, fileDownloadURL } from './file-download-client.js';

const MAX_FILE = 256 * 1024 * 1024, MAX_SPOOL = 512 * 1024 * 1024;
const MAX_TICKETS = 32, MAX_JOBS = 2, MAX_STREAMS = 4, RECEIPT_LIMIT = 1000, RECEIPT_AGE = 86400000;
const hashSchema = { type: 'string', pattern: '^[a-fA-F0-9]{64}$' };
const requestSchema = { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9._:-]+$' };
const common = { projectId: { type: 'string', minLength: 1, maxLength: 64 }, path: { type: 'string', minLength: 1, maxLength: 2048 }, requestId: requestSchema };
const schema = (properties, required) => ({ type: 'object', properties, required, additionalProperties: false });
export const DIRECT_TRANSFER_TOOLS = [
  { name: 'export_direct_file', description: 'Prepare an immutable binary snapshot (maximum 256 MiB) from a read-enabled Direct project. Returns metadata and a short-lived bearer HTTPS download URL; any holder can download until expiry or owner revocation. Reuse requestId to recover a lost response. File bytes never enter tool JSON.',
    inputSchema: schema(common, ['projectId', 'path', 'requestId']),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } },
  { name: 'import_direct_file', description: 'Import a caller-provided public HTTPS file into a write-enabled Direct project (maximum 256 MiB). file_id is a reference, not proof of origin. expectedSha256:null exclusively creates; overwrite requires the existing hash. Downloads and checks bytes before the shared host write lock. Reuse the original requestId after a lost response; interrupted requests never restart automatically.',
    inputSchema: schema({ ...common, expectedSha256: { anyOf: [hashSchema, { type: 'null' }] }, sha256: hashSchema,
      file: { type: 'object', properties: { download_url: { type: 'string', minLength: 1, maxLength: 8192 }, file_id: { type: 'string', minLength: 1, maxLength: 512 },
        mime_type: { type: 'string', maxLength: 256 }, file_name: { type: 'string', maxLength: 1024 } }, required: ['download_url', 'file_id'], additionalProperties: false } },
    ['projectId', 'path', 'requestId', 'expectedSha256', 'file']), _meta: { 'openai/fileParams': ['file'] },
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: true } },
  { name: 'get_direct_transfer', description: 'Recover the original binary-transfer receipt by requestId on the same host and authenticated owner. Completed and definite-failure receipts are retained for 24 hours. Interrupted or unknown-outcome request IDs remain reserved without automatic replay; receipt capacity is bounded. Pending, interrupted, expired and completed states are reported without restarting any operation.',
    inputSchema: schema({ requestId: requestSchema }, ['requestId']),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } },
];
const ajv = new Ajv({ strict: true });
const validators = new Map(DIRECT_TRANSFER_TOOLS.map(tool => [tool.name, ajv.compile(tool.inputSchema)]));
const failed = code => ({ ok: false, error: { code, message: `Direct binary transfer could not complete (${code}).` } });
const safeCode = error => /^[a-z][a-z0-9_]{0,63}$/.test(error?.safeCode ?? error?.code ?? '') ? (error.safeCode ?? error.code) : 'transfer_failed';
const digest = value => createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_key, item) => item && typeof item === 'object' && !Array.isArray(item)
  ? Object.fromEntries(Object.keys(item).sort().map(key => [key, item[key]])) : item);
const safeFilename = value => (Array.from(posix.basename(value).toWellFormed().replace(/[\x00-\x1f\x7f\\/"<>:|?*]/g, '_')).slice(0, 180).join('') || 'download.bin');
const disposition = value => `attachment; filename="${value.replace(/[^\x20-\x7e]|["\\]/g, '_')}"; filename*=UTF-8''${encodeURIComponent(value).replace(/[!'()*]/g, character => `%${character.charCodeAt(0).toString(16).toUpperCase()}`)}`;

export class DirectTransfers {
  #contexts; #transports = new Map(); #downloadClient; #base; #spool; #receiptFile; #now; #ttl;
  #receipts = new Map(); #tickets = new Map(); #jobs = new Map(); #active = new Set(); #streams = new Set();
  #diskBytes = 0; #reserved = 0; #persistTail = Promise.resolve(); #ready; #timer; #closed = false; #owned = new Set(); #revisions = new Map();
  constructor({ contexts, stateDir, publicBaseUrl, downloadClient = new SafeFileDownloadClient(), transportFactory = createBinaryFileTransport, now = Date.now, ttlMs = 600000 } = {}) {
    this.#contexts = new Map(contexts.map(context => [context.host.id, context]));
    this.#base = new URL(publicBaseUrl);
    if (this.#base.protocol !== 'https:' || this.#base.username || this.#base.password || this.#base.hash || this.#base.search) throw new Error('Direct downloads require a trusted HTTPS public base URL');
    if (!Number.isSafeInteger(ttlMs) || ttlMs < 1 || ttlMs > 600000) throw new Error('Invalid Direct ticket lifetime');
    this.#spool = join(stateDir, 'transfer-spool'); this.#receiptFile = join(stateDir, 'direct-transfer-receipts.json');
    this.#downloadClient = downloadClient; this.#now = now; this.#ttl = ttlMs;
    for (const context of contexts) this.#transports.set(context.host.id, transportFactory({ host: context.host }));
    this.#ready = this.#initialize(stateDir);
    this.handleDownload = this.handleDownload.bind(this);
  }
  async #initialize(stateDir) {
    await fs.mkdir(stateDir, { recursive: true, mode: 0o700 });
    await fs.mkdir(this.#spool, { mode: 0o700 }).catch(error => { if (error.code !== 'EEXIST') throw error; });
    const directory = await fs.lstat(this.#spool);
    if (!directory.isDirectory() || directory.isSymbolicLink() || (directory.mode & 0o077)) throw new Error('Transfer spool must be a private directory');
    let rows = [];
    try {
      const stat = await fs.lstat(this.#receiptFile);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 8 * 1024 * 1024) throw new Error('Invalid transfer receipt storage');
      rows = JSON.parse(await fs.readFile(this.#receiptFile, 'utf8'));
      if (!Array.isArray(rows) || rows.length > RECEIPT_LIMIT) throw new Error('Invalid transfer receipts');
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
    for (const row of rows) {
      if (!row || !/^[a-f0-9]{64}$/.test(row.key ?? '') || !/^[a-f0-9]{32}$/.test(row.transferId ?? '')
        || typeof row.owner !== 'string' || typeof row.hostId !== 'string' || typeof row.requestId !== 'string'
        || !/^[a-f0-9]{64}$/.test(row.fingerprint ?? '') || !Number.isFinite(row.updatedAt)) throw new Error('Invalid transfer receipt');
      // Only names recorded by this module are cleaned. Never scan user trees.
      await fs.unlink(join(this.#spool, `${row.transferId}.bin`)).catch(error => { if (error.code !== 'ENOENT') throw error; });
      if (row.state === 'pending') { row.state = 'interrupted'; if (row.direction === 'import') row.resultUnknown = true; }
      if (row.direction === 'export' && row.state === 'ready') row.state = 'expired';
      if (['interrupted', 'outcome_unknown'].includes(row.state) || this.#now() - row.updatedAt <= RECEIPT_AGE) this.#receipts.set(row.key, row);
    }
    await this.#persist();
    this.#timer = setInterval(() => this.#sweep().catch(() => {}), Math.min(30000, this.#ttl));
    this.#timer.unref();
  }
  ready() { return this.#ready; }
  forHost(hostId) {
    if (!this.#contexts.has(hostId)) throw new Error('Unknown Direct transfer host');
    return { tools: () => DIRECT_TRANSFER_TOOLS, call: (name, args, auth) => this.#call(hostId, name, args, auth),
      revokeOwner: owner => this.revokeOwner(owner, hostId) };
  }
  #key(owner, hostId, requestId) { return digest(JSON.stringify([owner, hostId, requestId])); }
  #revision(owner, hostId) { return this.#revisions.get(JSON.stringify([owner, hostId])) ?? 0; }
  #persist() {
    const task = this.#persistTail.then(async () => {
      const temporary = `${this.#receiptFile}.${randomBytes(12).toString('hex')}.tmp`;
      try {
        await fs.writeFile(temporary, JSON.stringify([...this.#receipts.values()]), { flag: 'wx', mode: 0o600 });
        await fs.rename(temporary, this.#receiptFile);
      } finally { await fs.unlink(temporary).catch(() => {}); }
    });
    this.#persistTail = task.catch(() => {}); return task;
  }
  #result(row) {
    const { owner, hostId, key, fingerprint, updatedAt, errorCode, projectId, ...result } = row;
    const ticket = [...this.#tickets.values()].find(item => item.transferId === row.transferId);
    return { ok: !['failed', 'interrupted', 'outcome_unknown', 'revoked'].includes(row.state), ...result,
      ...(ticket && row.state === 'ready' ? { downloadUrl: ticket.downloadUrl } : {}),
      ...(errorCode ? { error: failed(errorCode).error } : {}) };
  }
  async #call(hostId, name, args, { owner } = {}) {
    try { assertOwner(owner); if (owner.length > 512) throw new Error(); } catch { return failed('owner_required'); }
    // Capture before the first await: revocation must also cancel calls still
    // waiting for storage readiness or expiry cleanup, before a job exists.
    const revision = this.#revision(owner, hostId);
    if (!validators.has(name)) return failed('unknown_tool');
    if (!validators.get(name)(args)) return failed('invalid_arguments');
    try { await this.ready(); } catch { return failed('transfer_storage_unavailable'); }
    if (this.#closed) return failed('direct_transfers_closed');
    try { await this.#sweep(); } catch { return failed('transfer_storage_unavailable'); }
    if (this.#closed) return failed('direct_transfers_closed');
    if (revision !== this.#revision(owner, hostId)) return failed('transfer_access_revoked');
    const key = this.#key(owner, hostId, args.requestId), existing = this.#receipts.get(key);
    if (name === 'get_direct_transfer') return existing ? this.#result(existing) : failed('transfer_not_found');
    const context = this.#contexts.get(hostId);
    let project;
    try { project = projectFor(directProjects(context.host), args.projectId, name === 'import_direct_file' ? 'write' : 'read'); }
    catch { return failed('project_access_denied'); }
    const fingerprint = digest(canonical({ name, args }));
    if (existing) {
      if (existing.fingerprint !== fingerprint) return failed('request_id_conflict');
      return this.#jobs.get(key) ?? this.#result(existing);
    }
    if (name === 'import_direct_file') {
      try { fileDownloadURL(args.file.download_url); } catch { return failed('invalid_download_url'); }
      if (typeof context.directFiles?.withWriteLock !== 'function') return failed('shared_write_lock_unavailable');
    }
    if (this.#active.size >= MAX_JOBS || this.#diskBytes + this.#reserved + MAX_FILE > MAX_SPOOL
      || (name === 'export_direct_file' && this.#tickets.size + [...this.#active].filter(job => job.direction === 'export').length >= MAX_TICKETS)) return failed('transfer_capacity_exceeded');
    this.#trimReceipts();
    if (this.#receipts.size >= RECEIPT_LIMIT) return failed('transfer_receipt_capacity_exceeded');
    const row = { key, owner, hostId, projectId: args.projectId, requestId: args.requestId, fingerprint,
      transferId: randomBytes(16).toString('hex'), direction: name === 'export_direct_file' ? 'export' : 'import',
      state: 'pending', path: args.path, updatedAt: this.#now() };
    const job = { owner, hostId, revision, direction: row.direction, controller: new AbortController() };
    this.#reserved += MAX_FILE; this.#active.add(job); this.#receipts.set(key, row);
    const task = this.#perform(context, project, args, row, job);
    this.#jobs.set(key, task);
    try { return await task; } finally { this.#jobs.delete(key); }
  }
  async #perform(context, project, args, row, job) {
    const destination = join(this.#spool, `${row.transferId}.bin`);
    const signal = job.controller.signal;
    const check = () => {
      if (signal.aborted || job.revision !== this.#revision(job.owner, job.hostId)) throw Object.assign(new Error('Cancelled'), { safeCode: 'transfer_aborted' });
    };
    const timer = setTimeout(() => job.controller.abort(), 120000);
    this.#owned.add(destination);
    let retain = false;
    try {
      await this.#persist(); check();
      const input = { projectPath: project.path, roots: context.host.allowedRoots, path: args.path, maxBytes: MAX_FILE };
      const transport = this.#transports.get(context.host.id);
      let metadata;
      if (row.direction === 'export') {
        metadata = await transport.exportTo({ ...input, destination, signal, timeoutMs: 120000 }); check();
      } else {
        const downloaded = await this.#downloadClient.downloadTo(args.file.download_url, { destination, maxBytes: MAX_FILE, sha256: args.sha256, signal, timeoutMs: 120000 });
        check();
        metadata = await this.#withWriteLock(context.directFiles, signal, async () => {
          check(); projectFor(directProjects(context.host), args.projectId, 'write');
          const source = createReadStream(destination);
          try { return await transport.importFrom({ ...input, expectedSha256: args.expectedSha256, bytes: downloaded.bytes, sha256: downloaded.sha256 }, source, { signal, timeoutMs: 120000 }); }
          finally { source.destroy(); }
        });
        // A completed commit remains complete even if revocation arrives after
        // the atomic commit; reporting an abort here would hide that mutation.
      }
      if (!Number.isSafeInteger(metadata.bytes) || metadata.bytes < 0 || metadata.bytes > MAX_FILE || !/^[a-f0-9]{64}$/.test(metadata.sha256 ?? '')) throw new Error('Invalid transfer metadata');
      Object.assign(row, { bytes: metadata.bytes, sha256: metadata.sha256, fileName: safeFilename(args.path), mimeType: 'application/octet-stream',
        state: row.direction === 'export' ? 'ready' : 'completed', updatedAt: this.#now(), ...(row.direction === 'import' ? { created: args.expectedSha256 === null } : {}) });
      if (row.direction === 'export') {
        const token = randomBytes(32).toString('base64url');
        const expires = this.#now() + this.#ttl;
        row.expiresAt = new Date(expires).toISOString();
        check();
        const ticket = { token, transferId: row.transferId, owner: row.owner, hostId: row.hostId, revision: job.revision, destination, fileName: row.fileName,
          bytes: row.bytes, expires, downloadUrl: new URL(`/direct-files/${token}/${encodeURIComponent(row.fileName)}`, this.#base).href,
          streams: new Set(), attempts: 0, served: 0, row };
        this.#tickets.set(token, ticket); this.#diskBytes += row.bytes; retain = true;
      }
      await this.#persist();
      return this.#result(row);
    } catch (error) {
      // If a receipt flush failed after a known commit, preserve the in-memory
      // completed result. Disk recovery will conservatively say interrupted.
      if (row.direction === 'import' && row.state === 'completed') return this.#result(row);
      if (retain) {
        const ticket = [...this.#tickets.values()].find(item => item.transferId === row.transferId);
        if (ticket) await this.#invalidate(ticket, 'failed'); retain = false;
      }
      row.state = error?.resultUnknown ? 'outcome_unknown' : signal.aborted ? 'interrupted' : 'failed';
      if (error?.resultUnknown) row.resultUnknown = true;
      row.errorCode = signal.aborted ? 'transfer_aborted' : safeCode(error); row.updatedAt = this.#now();
      await this.#persist().catch(() => {});
      return this.#result(row);
    } finally {
      clearTimeout(timer);
      if (!retain) {
        if (!(await this.#removeOwned(destination))) {
          // Failed unlink must not silently release the occupied disk quota.
          this.#diskBytes += (await fs.stat(destination).catch(() => ({ size: MAX_FILE }))).size;
        }
      }
      this.#active.delete(job); this.#reserved -= MAX_FILE;
    }
  }
  #trimReceipts() {
    for (const [key, row] of this.#receipts) if (!['pending', 'ready', 'interrupted', 'outcome_unknown'].includes(row.state)
      && this.#now() - row.updatedAt > RECEIPT_AGE) this.#receipts.delete(key);
  }
  #withWriteLock(directFiles, signal, execute) {
    // Only cancellation of a *queued* lock wait is raced. Once runtime starts,
    // preserve its real completion or unknown-commit result.
    return new Promise((resolve, reject) => {
      let entered = false;
      const abort = () => { if (!entered) reject(Object.assign(new Error('Cancelled'), { safeCode: 'transfer_aborted' })); };
      signal.addEventListener('abort', abort, { once: true });
      const task = directFiles.withWriteLock(async () => {
        if (signal.aborted) throw Object.assign(new Error('Cancelled'), { safeCode: 'transfer_aborted' });
        entered = true; signal.removeEventListener('abort', abort);
        return execute();
      });
      Promise.resolve(task).then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
      if (signal.aborted) abort();
    });
  }
  async #invalidate(ticket, state) {
    if (!this.#tickets.delete(ticket.token)) return;
    for (const stream of ticket.streams) stream.abort();
    ticket.row.state = state; ticket.row.updatedAt = this.#now();
    if (await this.#removeOwned(ticket.destination)) this.#diskBytes -= ticket.bytes;
  }
  async #removeOwned(destination) {
    try { await fs.unlink(destination); }
    catch (error) { if (error.code !== 'ENOENT') return false; }
    this.#owned.delete(destination); return true;
  }
  async #sweep() {
    let changed = false;
    for (const ticket of this.#tickets.values()) if (ticket.expires <= this.#now()) { await this.#invalidate(ticket, 'expired'); changed = true; }
    const count = this.#receipts.size; this.#trimReceipts();
    if (changed || count !== this.#receipts.size) await this.#persist();
  }
  async revokeOwner(owner, hostId) {
    // Bump synchronously, including requests not yet registered in #active.
    for (const target of hostId ? [hostId] : this.#contexts.keys()) {
      const key = JSON.stringify([owner, target]); this.#revisions.set(key, this.#revision(owner, target) + 1);
    }
    for (const job of this.#active) if (job.owner === owner && (!hostId || job.hostId === hostId)) job.controller.abort();
    await this.ready();
    for (const ticket of this.#tickets.values()) if (ticket.owner === owner && (!hostId || ticket.hostId === hostId)) await this.#invalidate(ticket, 'revoked');
    await this.#persist();
  }
  async handleDownload(req, res) {
    const reject = status => { if (!res.headersSent) { res.statusCode = status; res.setHeader('Cache-Control', 'no-store'); res.end(); } else res.destroy(); };
    try {
      await this.ready();
      if (this.#closed) { reject(404); return; }
      const ticket = this.#tickets.get(req.params?.token);
      if (!ticket || ticket.fileName !== req.params?.filename) { reject(404); return; }
      if (ticket.revision !== this.#revision(ticket.owner, ticket.hostId)) { await this.#invalidate(ticket, 'revoked'); await this.#persist(); reject(404); return; }
      if (ticket.expires <= this.#now()) { await this.#invalidate(ticket, 'expired'); await this.#persist(); reject(404); return; }
      if (!['GET', 'HEAD'].includes(req.method)) { res.setHeader('Allow', 'GET, HEAD'); reject(405); return; }
      let start = 0, end = ticket.bytes - 1, partial = false;
      const range = req.headers.range;
      if (range !== undefined) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(range);
        if (!match || (!match[1] && !match[2]) || !ticket.bytes) { res.setHeader('Content-Range', `bytes */${ticket.bytes}`); reject(416); return; }
        if (!match[1]) { const suffix = Number(match[2]); start = Math.max(0, ticket.bytes - suffix); if (suffix < 1) start = ticket.bytes; }
        else { start = Number(match[1]); if (match[2]) end = Math.min(end, Number(match[2])); }
        if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || start >= ticket.bytes) { res.setHeader('Content-Range', `bytes */${ticket.bytes}`); reject(416); return; }
        partial = true;
      }
      const bytes = ticket.bytes ? end - start + 1 : 0;
      if (req.method === 'GET' && (ticket.attempts >= 8 || ticket.served + bytes > Math.max(ticket.bytes * 3, 1)
        || this.#streams.size >= MAX_STREAMS || ticket.streams.size)) { reject(429); return; }
      res.statusCode = partial ? 206 : 200;
      res.setHeader('Content-Type', 'application/octet-stream'); res.setHeader('Content-Disposition', disposition(ticket.fileName));
      res.setHeader('Content-Length', bytes); res.setHeader('Accept-Ranges', 'bytes');
      res.setHeader('Cache-Control', 'no-store'); res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer');
      if (partial) res.setHeader('Content-Range', `bytes ${start}-${end}/${ticket.bytes}`);
      if (req.method === 'HEAD') { res.end(); return; }
      ticket.attempts++; ticket.served += bytes;
      const controller = new AbortController(); ticket.streams.add(controller); this.#streams.add(controller);
      const transmitted = () => { ticket.streams.delete(controller); this.#streams.delete(controller); };
      res.once('finish', transmitted);
      const timer = setTimeout(() => controller.abort(), Math.min(120000, Math.max(1, ticket.expires - this.#now())));
      const disconnected = () => controller.abort(); req.on('aborted', disconnected);
      try {
        if (!bytes) { res.end(); return; }
        await pipeline(createReadStream(ticket.destination, { start, end }), res, { signal: controller.signal });
      } finally {
        clearTimeout(timer); req.removeListener('aborted', disconnected); res.removeListener('finish', transmitted); transmitted();
      }
    } catch { reject(404); }
  }
  async close() {
    if (this.#closed) return;
    this.#closed = true;
    await this.ready(); clearInterval(this.#timer);
    for (const job of this.#active) job.controller.abort();
    for (const stream of this.#streams) stream.abort();
    for (const transport of this.#transports.values()) transport.close();
    await Promise.allSettled([...this.#jobs.values()]);
    for (const ticket of this.#tickets.values()) await this.#invalidate(ticket, 'expired');
    for (const path of this.#owned) await fs.unlink(path).catch(() => {});
    this.#owned.clear(); await this.#persist();
  }
}
