// SPDX-License-Identifier: Apache-2.0
// Project-scoped Direct command design inspired by Codex Bridge (see NOTICE).
// Independent Node implementation; no Swift SDK or runtime code is copied.
import { DatabaseSync } from 'node:sqlite';
import { spawn } from 'node:child_process';
import { randomUUID, createHash } from 'node:crypto';
import { readFileSync, mkdirSync, chmodSync, lstatSync, writeFileSync, rmSync, readdirSync } from 'node:fs';
import { join, isAbsolute } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import Ajv from 'ajv';
import { directProjects, projectFor, commandAllowed, assertOwner } from './direct-policy.js';
import { validateSshTarget, sshArguments, validateNodeCommand } from './ssh-bridge.mjs';

const ACTIVE = new Set(['pending', 'running']);
const id = { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9][A-Za-z0-9._:-]*$' };
const commandId = { type: 'string', pattern: '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' };
function definition(name, description, properties, required, readOnlyHint = false) {
  return { name, description, inputSchema: { type: 'object', properties, required, additionalProperties: false },
    annotations: { readOnlyHint, destructiveHint: !readOnlyHint, idempotentHint: true, openWorldHint: true } };
}
export const DIRECT_COMMAND_TOOLS = [
  definition('run_direct_command', 'Start an owner-authorized argv command in a configured project without a shell wrapper. Returns a persisted receipt immediately. Reuse identical requestId after uncertain delivery; never automatically resend with a new ID. A directory check is not an OS sandbox.', {
    projectId: id, requestId: id, argv: { type: 'array', minItems: 1, maxItems: 64, items: { type: 'string' } },
    timeoutMs: { type: 'integer', minimum: 1, maximum: 3600000, default: 30000 },
    cwd: { type: 'string', maxLength: 4096, description: 'Existing project-relative working directory, checked on the selected host' },
  }, ['projectId', 'requestId', 'argv']),
  definition('read_direct_command', 'Read bounded stdout/stderr events and the original receipt. The cursor counts events, including lost events. A read wait does not stop the command. Output is ephemeral and is unavailable after gateway restart.', {
    commandId, cursor: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
    limit: { type: 'integer', minimum: 1, maximum: 1000 }, waitMs: { type: 'integer', minimum: 0, maximum: 2000 },
  }, ['commandId'], true),
  definition('write_direct_command_input', 'Write at most 16KiB to command stdin or close it with eof=true. inputId is persisted before sending. A duplicate inputId is never rewritten, including after an unknown outcome. Acceptance means the pipe accepted bytes, not that the command consumed them. EOF is irreversible for this command.', {
    commandId, inputId: id, text: { type: 'string', maxLength: 16384 }, eof: { type: 'boolean' },
  }, ['commandId', 'inputId']),
  definition('cancel_direct_command', 'Request termination of this command and its owned process group. Uses TERM then KILL; terminal and uncertain receipts are not restarted.', { commandId }, ['commandId']),
];
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
const safe = message => new Error(message);

export class DirectCommands {
  constructor({ host, projects = directProjects(host), stateDir = host.stateDir, spawnImpl = spawn,
    workerSource = readFileSync(new URL('./direct-command-worker.mjs', import.meta.url), 'utf8'),
    maxOutputBytes = 1024 * 1024, rpcTimeoutMs = 8000 } = {}) {
    this.host = host; this.projects = projects; this.spawnImpl = spawnImpl; this.workerSource = workerSource;
    this.maxOutputBytes = Math.min(1024 * 1024, Math.max(8192, maxOutputBytes)); this.rpcTimeoutMs = rpcTimeoutMs;
    this.runtime = new Map(); this.events = []; this.outputBytes = 0; this.pending = new Map(); this.generations = new Set(); this.closed = false;
    if (!stateDir || !isAbsolute(stateDir)) throw safe('Direct stateDir must be an absolute owner-configured directory');
    mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    const stateStat = lstatSync(stateDir);
    if (stateStat.isSymbolicLink() || !stateStat.isDirectory() || process.getuid && stateStat.uid !== process.getuid()) throw safe('Direct stateDir must be an owned directory without symlinks');
    chmodSync(stateDir, 0o700);
    this.lock = join(stateDir, 'direct.lock');
    try { mkdirSync(this.lock, { mode: 0o700 }); }
    catch (e) {
      if (e.code !== 'EEXIST' || lstatSync(this.lock).isSymbolicLink()) throw safe('Direct state lock is unavailable');
      let prior;
      const markerStat = lstatSync(join(this.lock, 'owner.json'));
      if (!markerStat.isFile() || process.getuid && markerStat.uid !== process.getuid() || readdirSync(this.lock).some(name => name !== 'owner.json')) throw safe('Direct state lock marker is invalid');
      try { prior = JSON.parse(readFileSync(join(this.lock, 'owner.json'), 'utf8')); } catch { throw safe('Direct state lock is unavailable'); }
      if (!Number.isSafeInteger(prior.pid) || prior.pid <= 0) throw safe('Direct state lock is unavailable');
      try { process.kill(prior.pid, 0); throw safe('Direct state directory is already in use'); }
      catch (error) { if (error.code !== 'ESRCH') throw error; }
      rmSync(this.lock, { recursive: true }); mkdirSync(this.lock, { mode: 0o700 });
    }
    writeFileSync(join(this.lock, 'owner.json'), JSON.stringify({ pid: process.pid, hostId: host.id }), { mode: 0o600, flag: 'wx' });
    const filename = join(stateDir, 'direct.sqlite');
    try {
      try { if (!lstatSync(filename).isFile()) throw safe('Direct receipt database must be a regular file'); } catch (e) { if (e.code !== 'ENOENT') throw e; }
      this.db = new DatabaseSync(filename); chmodSync(filename, 0o600);
      this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
        CREATE TABLE IF NOT EXISTS direct_commands (
          command_id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, owner TEXT NOT NULL,
          host_id TEXT NOT NULL, project_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
          state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          exit_code INTEGER, signal TEXT, error TEXT, output_seq INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS direct_inputs (
          command_id TEXT NOT NULL, input_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
          state TEXT NOT NULL, eof INTEGER NOT NULL, PRIMARY KEY(command_id,input_id)
        );`);
      this.db.prepare("UPDATE direct_commands SET state=CASE WHEN state='running' THEN 'interrupted' ELSE 'unknown' END,updated_at=? WHERE state IN ('pending','running') AND host_id=?")
        .run(new Date().toISOString(), host.id);
      this.db.prepare("UPDATE direct_inputs SET state='unknown' WHERE state='sending'").run();
      const ajv = new Ajv({ strict: false, validateFormats: false });
      this.validators = new Map(DIRECT_COMMAND_TOOLS.map(tool => [tool.name, ajv.compile(tool.inputSchema)]));
    } catch (error) { this.db?.close(); rmSync(this.lock, { recursive: true }); throw error; }
  }
  tools() { return DIRECT_COMMAND_TOOLS; }
  receipt(row, duplicate = false) {
    return { commandId: row.command_id, requestId: row.request_id, hostId: row.host_id, projectId: row.project_id,
      state: row.state, createdAt: row.created_at, updatedAt: row.updated_at, duplicate,
      exitCode: row.exit_code, signal: row.signal, ...(row.error ? { error: row.error } : {}),
      ...(['unknown', 'interrupted'].includes(row.state) ? { message: 'Execution outcome is not confirmed. This command will not be resubmitted; inspect the original command before creating a new request.' } : {}) };
  }
  row(commandId, owner) {
    const row = this.db.prepare('SELECT * FROM direct_commands WHERE command_id=?').get(commandId);
    if (!row || row.owner !== owner || row.host_id !== this.host.id) throw safe('Unknown Direct command for this owner and host');
    return row;
  }
  async call(name, args, { owner } = {}) {
    if (this.closed) throw safe('Direct commands are closed');
    assertOwner(owner);
    const validate = this.validators.get(name);
    if (!validate || !validate(args)) throw safe('Invalid Direct command tool arguments');
    if (name === 'run_direct_command') return this.run(args, owner);
    const row = this.row(args.commandId, owner);
    if (name === 'read_direct_command') return this.read(args, row, owner);
    if (name === 'write_direct_command_input') return this.input(args, row);
    if (name === 'cancel_direct_command') {
      if (ACTIVE.has(row.state)) {
        // run() starts initialization first, so cancellation never overtakes it.
        const live = this.runtime.get(row.command_id);
        await live?.submission;
        const current = this.row(row.command_id, owner);
        if (ACTIVE.has(current.state)) {
          try { await this.rpc({ op: 'cancel', commandId: row.command_id }, live?.generation); }
          catch { this.update(row.command_id, 'unknown', { error: 'Cancellation outcome is unknown' }); }
        }
      }
      return { ...this.receipt(this.row(row.command_id, owner)), cancelRequested: ACTIVE.has(row.state) };
    }
  }
  run(args, owner) {
    const project = projectFor(this.projects, args.projectId, 'command');
    const argv = commandAllowed(project, args.argv), cwd = args.cwd ?? '', timeoutMs = args.timeoutMs ?? 30000;
    if (isAbsolute(cwd) || cwd.includes('\0')) throw safe('cwd must be a project-relative directory');
    const fingerprint = hash({ owner, hostId: this.host.id, projectId: project.id, argv, cwd, timeoutMs });
    const old = this.db.prepare('SELECT * FROM direct_commands WHERE request_id=?').get(args.requestId);
    if (old) {
      if (old.owner !== owner || old.host_id !== this.host.id || old.fingerprint !== fingerprint) throw safe('requestId already belongs to different command parameters or owner');
      return this.receipt(old, true);
    }
    if ([...this.runtime.values()].filter(r => ACTIVE.has(r.state)).length >= 16) throw safe('Too many active Direct commands');
    if (this.runtime.size >= 128) {
      for (const [key, value] of this.runtime) { if (!ACTIVE.has(value.state)) this.runtime.delete(key); if (this.runtime.size < 128) break; }
    }
    const commandId = randomUUID(), now = new Date().toISOString();
    this.db.prepare(`INSERT INTO direct_commands (command_id,request_id,owner,host_id,project_id,fingerprint,state,created_at,updated_at)
      VALUES (?,?,?,?,?,?,'pending',?,?)`).run(commandId, args.requestId, owner, this.host.id, project.id, fingerprint, now, now);
    const live = { id: commandId, state: 'pending', seq: 0, entries: [], waiters: new Set() };
    this.runtime.set(commandId, live);
    live.submission = (async () => {
      try {
        const generation = await this.ensureWorker();
        live.generation = generation;
        if (this.closed) { this.update(commandId, 'interrupted'); return; }
        await this.rpc({ op: 'run', commandId, projectId: project.id, argv, cwd, timeoutMs }, generation);
      } catch (error) {
        if (!this.closed) this.update(commandId, error.workerRejected ? 'failed' : 'unknown', { error: error.workerRejected ? error.message : 'Worker submission outcome is unknown' });
      }
    })();
    return this.receipt(this.row(commandId, owner));
  }
  update(commandId, state, detail = {}) {
    if (this.dbClosed) return;
    const live = this.runtime.get(commandId);
    // A later transport error must not replace a confirmed terminal outcome.
    const row = this.db.prepare('SELECT state FROM direct_commands WHERE command_id=?').get(commandId);
    if (!row || !ACTIVE.has(row.state)) return;
    this.db.prepare('UPDATE direct_commands SET state=?,updated_at=?,exit_code=?,signal=?,error=?,output_seq=? WHERE command_id=?')
      .run(state, new Date().toISOString(), detail.exitCode ?? null, detail.signal ?? null, detail.error ?? null, live?.seq ?? 0, commandId);
    if (live) { live.state = state; for (const wake of [...live.waiters]) wake(); }
  }
  addOutput(event, generation) {
    const live = this.runtime.get(event.commandId);
    if (!live || live.generation !== generation || typeof event.text !== 'string' || !['stdout', 'stderr'].includes(event.source)) return;
    const textBytes = Buffer.byteLength(event.text);
    if (textBytes > 8192) { this.transportLost(generation); return; }
    const entry = { seq: ++live.seq, source: event.source, text: event.text };
    const bytes = Buffer.byteLength(JSON.stringify(entry));
    live.entries.push(entry); this.events.push({ live, entry, bytes }); this.outputBytes += bytes;
    while (this.outputBytes > this.maxOutputBytes || this.events.length > 8192) {
      const removed = this.events.shift(); this.outputBytes -= removed.bytes;
      if (removed.live.entries[0] === removed.entry) removed.live.entries.shift();
    }
    for (const wake of [...live.waiters]) wake();
  }
  async read(args, row, owner) {
    const cursor = args.cursor ?? 0, limit = args.limit ?? 100;
    const live = this.runtime.get(row.command_id);
    if (live && cursor >= live.seq && ACTIVE.has(live.state) && args.waitMs) {
      await new Promise(resolve => {
        let timer;
        const wake = () => { clearTimeout(timer); live.waiters.delete(wake); resolve(); };
        live.waiters.add(wake); timer = setTimeout(wake, args.waitMs);
      });
      row = this.row(row.command_id, owner);
    }
    const entries = [], available = live?.entries.filter(e => e.seq > cursor) ?? [];
    let bytes = 0, nextCursor = cursor, lostCount = 0;
    for (const entry of available) {
      const size = Buffer.byteLength(JSON.stringify(entry));
      if (entries.length >= limit || bytes + size > 60 * 1024) break;
      lostCount += Math.max(0, entry.seq - nextCursor - 1);
      entries.push(entry); bytes += size; nextCursor = entry.seq;
    }
    const end = live?.seq ?? row.output_seq;
    if (entries.length === available.length && end > nextCursor) { lostCount += end - nextCursor; nextCursor = end; }
    return { ...this.receipt(row), events: entries, nextCursor, lostCount,
      truncated: lostCount > 0 || available.length > entries.length, outputAvailable: Boolean(live),
      ...(live ? {} : { message: 'Output cache is unavailable after restart or receipt eviction; the persisted receipt remains readable.' }) };
  }
  async input(args, row) {
    const text = args.text ?? '', eof = args.eof ?? false;
    if (Buffer.byteLength(text) > 16384 || (!text && !eof)) throw safe('stdin requires text up to 16KiB or eof=true');
    const fingerprint = hash({ text, eof });
    const old = this.db.prepare('SELECT * FROM direct_inputs WHERE command_id=? AND input_id=?').get(row.command_id, args.inputId);
    if (old) {
      if (old.fingerprint !== fingerprint) throw safe('inputId already belongs to different input');
      return { commandId: row.command_id, inputId: args.inputId, state: old.state, eof: Boolean(old.eof), duplicate: true };
    }
    if (!ACTIVE.has(row.state)) throw safe('Command is no longer running');
    if (this.db.prepare("SELECT 1 FROM direct_inputs WHERE command_id=? AND eof=1 AND state IN ('sending','accepted','unknown')").get(row.command_id)) throw safe('Command stdin is closed or its EOF outcome is unknown');
    this.db.prepare("INSERT INTO direct_inputs(command_id,input_id,fingerprint,state,eof) VALUES (?,?,?,'sending',?)")
      .run(row.command_id, args.inputId, fingerprint, Number(eof));
    let state = 'unknown', result;
    try {
      await this.runtime.get(row.command_id)?.submission;
      result = await this.rpc({ op: 'input', commandId: row.command_id, text, eof }, this.runtime.get(row.command_id)?.generation); state = 'accepted';
    } catch (error) { if (error.workerRejected && !error.message.includes('unknown')) state = 'rejected'; }
    if (!this.dbClosed) this.db.prepare('UPDATE direct_inputs SET state=? WHERE command_id=? AND input_id=?').run(state, row.command_id, args.inputId);
    return { commandId: row.command_id, inputId: args.inputId, state, eof, duplicate: false,
      ...(result?.delivery ? { delivery: result.delivery } : {}),
      ...(state === 'unknown' ? { message: 'Input delivery is unknown. This inputId will not be sent again.' } : {}) };
  }
  ensureWorker() {
    if (this.generation && !this.generation.lost) return this.generation.ready;
    if (this.starting) {
      // A request that already belonged to the lost generation fails normally.
      // Only this *new* request waits for that attempt to settle and reconnects.
      if (this.starting.generation?.lost) return this.starting.promise.catch(() => {}).then(() => this.ensureWorker());
      return this.starting.promise;
    }
    const previous = this.generation, attempt = {};
    this.starting = attempt;
    attempt.promise = (async () => {
      if (previous && !await this.retireGeneration(previous)) throw Object.assign(safe('Previous Direct worker has not exited; this new command was not submitted'), { workerRejected: true });
      if (this.closed) throw safe('Direct commands are closed');
      let executable, argv;
      if (this.host.transport === 'localMac' || this.host.transport === 'local') {
        executable = process.execPath; argv = ['--input-type=module', '-e', this.workerSource];
      } else if (this.host.transport === 'sshWSL' || this.host.transport === 'ssh') {
        if (typeof this.host.remoteStateDir !== 'string' || !isAbsolute(this.host.remoteStateDir)) throw safe('Direct SSH requires an explicit remoteStateDir');
        const target = validateSshTarget(this.host.target);
        executable = 'ssh'; argv = [...sshArguments(target), target.host, `${quote(validateNodeCommand(this.host.nodeCommand))} --input-type=module -e ${quote(this.workerSource)}`];
      } else throw safe('Unsupported Direct host transport');
      const child = this.spawnImpl(executable, argv, { stdio: ['pipe', 'pipe', 'pipe'], shell: false });
      const generation = { child, pending: new Map(), lost: false, exited: false };
      this.generation = generation; this.generations.add(generation); attempt.generation = generation;
      // These aliases remain useful for diagnostics; callbacks always capture
      // their own generation instead of referring to the current worker.
      this.worker = child; this.pending = generation.pending;
      let line = ''; const decoder = new StringDecoder('utf8');
      child.stdout.on('data', bytes => {
        if (generation.lost) return;
        line += decoder.write(bytes);
        if (Buffer.byteLength(line) > 256 * 1024) { this.transportLost(generation); return; }
        let index;
        while ((index = line.indexOf('\n')) >= 0) {
          const packet = line.slice(0, index); line = line.slice(index + 1);
          try {
            const event = JSON.parse(packet);
            if (event.replyTo) {
              const pending = generation.pending.get(event.replyTo);
              if (pending) { clearTimeout(pending.timer); generation.pending.delete(event.replyTo);
                event.ok ? pending.resolve(event.result) : pending.reject(Object.assign(safe(event.error ?? 'Worker rejected operation'), { workerRejected: true })); }
            } else if (event.type === 'output') this.addOutput(event, generation);
            else if (event.type === 'state') {
              if (this.runtime.get(event.commandId)?.generation === generation) this.update(event.commandId, event.state, event);
            } else if (event.type === 'loss') {
              const live = this.runtime.get(event.commandId);
              if (live?.generation === generation && Number.isSafeInteger(event.count) && event.count > 0) live.seq += event.count;
            }
          } catch { this.transportLost(generation); return; }
        }
      });
      child.stderr.on('data', () => {}); // Never log command/transport output.
      child.stdin.on('error', () => this.transportLost(generation));
      child.on('error', () => this.transportLost(generation));
      generation.exit = new Promise(resolve => { child.once('close', () => {
        generation.exited = true; resolve(); this.transportLost(generation);
      }); });
      this.workerExit = generation.exit;
      generation.ready = this.rpc({ op: 'init', config: { hostId: this.host.id, projects: this.projects, allowedRoots: this.host.allowedRoots,
        ...(['sshWSL', 'ssh'].includes(this.host.transport) ? { remoteStateDir: this.host.remoteStateDir } : {}) } }, generation)
        .then(() => generation).catch(error => { this.transportLost(generation); throw error; });
      this.workerReady = generation.ready;
      return generation.ready;
    })().finally(() => { if (this.starting === attempt) this.starting = undefined; });
    return attempt.promise;
  }
  rpc(message, generation = this.generation) {
    if (this.closed || !generation || generation.lost || generation.pending.size >= 64 || generation.child.stdin.writableLength > 65536) return Promise.reject(safe('Direct worker transport is unavailable or backpressured'));
    const id = randomUUID(), data = `${JSON.stringify({ ...message, id })}\n`;
    if (Buffer.byteLength(data) > 128 * 1024) return Promise.reject(safe('Direct worker input limit exceeded'));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        generation.pending.delete(id); reject(safe('Direct worker response outcome is unknown'));
        this.transportLost(generation);
      }, this.rpcTimeoutMs);
      generation.pending.set(id, { resolve, reject, timer });
      try { generation.child.stdin.write(data, error => { if (error) this.transportLost(generation); }); }
      catch { this.transportLost(generation); }
    });
  }
  retireGeneration(generation) {
    if (generation.exited) { this.generations.delete(generation); return Promise.resolve(true); }
    if (generation.cleanup) return generation.cleanup;
    generation.cleanup = (async () => {
      try { generation.child.stdin.end(); generation.child.kill('SIGTERM'); } catch {}
      let killTimer, deadline;
      const exited = await Promise.race([generation.exit.then(() => true), new Promise(resolve => {
        killTimer = setTimeout(() => { try { generation.child.kill('SIGKILL'); } catch {} }, 2000);
        deadline = setTimeout(() => resolve(false), 3000);
      })]);
      clearTimeout(killTimer); clearTimeout(deadline);
      if (exited) this.generations.delete(generation);
      return exited;
    })();
    return generation.cleanup;
  }
  transportLost(generation = this.generation) {
    if (!generation || generation.lost) return;
    generation.lost = true;
    for (const { reject, timer } of generation.pending.values()) { clearTimeout(timer); reject(safe('Direct worker connection was lost')); }
    generation.pending.clear();
    for (const live of this.runtime.values()) if (live.generation === generation) {
      this.update(live.id, this.closed ? 'interrupted' : 'unknown', { error: 'Worker connection was closed; command outcome is not confirmed' });
    }
    void this.retireGeneration(generation);
  }
  close() {
    if (this.closing) return this.closing;
    this.closed = true;
    this.closing = (async () => {
      for (const generation of this.generations) this.transportLost(generation);
      for (const live of this.runtime.values()) this.update(live.id, 'interrupted', { error: 'Gateway was closed' });
      await Promise.allSettled([...this.generations].map(generation => this.retireGeneration(generation)));
      await Promise.allSettled([...this.runtime.values()].map(r => r.submission));
      this.dbClosed = true; this.db.close(); rmSync(this.lock, { recursive: true });
      this.runtime.clear(); this.events = []; this.outputBytes = 0;
    })();
    return this.closing;
  }
}
