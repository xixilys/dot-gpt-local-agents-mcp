// SPDX-License-Identifier: Apache-2.0
// Node adaptation of Fanch-hui/codex-bridge Direct file tool schemas and
// expected_sha256 design; modified for gateway host routing, explicit project
// permissions and bounded UTF-8 I/O. This is not the Swift Bridge runtime.
import Ajv from 'ajv';
import { readFile } from 'node:fs/promises';
import { directProjects, projectFor, assertOwner } from './direct-policy.js';
import { createSshRunner, validateSshTarget } from './ssh-bridge.mjs';
import { runFileOperation } from './direct-files-runtime.mjs';

const projectId = { type: 'string', minLength: 1, maxLength: 64 };
const filePath = { type: 'string', minLength: 1, maxLength: 2048 };
const directoryPath = { type: 'string', maxLength: 2048 };
const expectedSha256 = { type: 'string', pattern: '^[a-fA-F0-9]{64}$' };
const text = { type: 'string', maxLength: 204800 };
const schema = (properties, required) => ({ type: 'object', properties: { projectId, ...properties }, required: ['projectId', ...required], additionalProperties: false });
const readonly = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };
const writable = { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false };
export const DIRECT_FILE_TOOLS = [
  { name: 'list_direct_files', description: 'List one registered project directory on the selected host. Bounded results report skipped entries and truncation.',
    inputSchema: schema({ path: directoryPath, limit: { type: 'integer', minimum: 1, maximum: 1000 } }, []), annotations: readonly },
  { name: 'search_direct_files', description: 'Search UTF-8 text files recursively for a literal string. Skips symlinks, Git/dependency directories and binary or oversized files; reports bounded scan totals.',
    inputSchema: schema({ path: directoryPath, query: { type: 'string', minLength: 1, maxLength: 1024 }, limit: { type: 'integer', minimum: 1, maximum: 100 } }, ['query']), annotations: readonly },
  { name: 'read_direct_file', description: 'Read a UTF-8 text file (maximum 200 KiB), with its full-file SHA-256. offset is a zero-based line index; output is bounded and reports truncation.',
    inputSchema: schema({ path: filePath, offset: { type: 'integer', minimum: 0, maximum: 1000000 }, limit: { type: 'integer', minimum: 1, maximum: 1000 } }, ['path']), annotations: readonly },
  { name: 'write_direct_file', description: 'Atomically write UTF-8 text (maximum 200 KiB) in a write-enabled project. expectedSha256:null exclusively creates a new file; overwrites require the current full-file hash. Writes through this host instance are serialized, with a final race check (not a kernel compare-and-swap).',
    inputSchema: schema({ path: filePath, content: text, expectedSha256: { anyOf: [expectedSha256, { type: 'null' }] } }, ['path', 'content', 'expectedSha256']), annotations: writable },
  { name: 'edit_direct_file', description: 'Replace exact text after checking the full-file SHA-256 in a write-enabled project. oldText must occur exactly once unless replaceAll:true is explicit; replacement text is literal.',
    inputSchema: schema({ path: filePath, expectedSha256, oldText: { ...text, minLength: 1 }, newText: text, replaceAll: { type: 'boolean' } }, ['path', 'expectedSha256', 'oldText', 'newText']), annotations: writable },
];
const ajv = new Ajv({ allErrors: true, strict: true });
const validators = new Map(DIRECT_FILE_TOOLS.map(tool => [tool.name, ajv.compile(tool.inputSchema)]));
const errorResult = (code, message) => ({ ok: false, error: { code, message } });
let remoteScript;
async function fixedRemoteScript() {
  remoteScript ??= readFile(new URL('./direct-files-runtime.mjs', import.meta.url), 'utf8').then(source => `${source}\nlet input = ''; for await (const chunk of process.stdin) { input += chunk; if (Buffer.byteLength(input) > 1024 * 1024) process.exit(1); }\ntry { process.stdout.write(JSON.stringify(await runFileOperation(JSON.parse(input)))); } catch { process.exitCode = 1; }\n`);
  return remoteScript;
}

export class DirectFiles {
  #host; #projects; #runner; #runRemote; #writeTail = Promise.resolve(); #closed = false;
  constructor({ host, projects = directProjects(host), runRemote } = {}) {
    if (!host || !['local', 'ssh'].includes(host.transport)) throw new Error('Direct files require a configured local or SSH host');
    this.#host = host; this.#projects = projects;
    if (host.transport === 'ssh') {
      const target = validateSshTarget(host.target);
      if (runRemote) this.#runRemote = typeof runRemote === 'function' ? runRemote : runRemote.run.bind(runRemote);
      else { this.#runner = createSshRunner(target, { nodeCommand: host.nodeCommand }); this.#runRemote = this.#runner.run; }
    }
  }
  tools() { return DIRECT_FILE_TOOLS; }
  async call(name, args, { owner } = {}) {
    try { assertOwner(owner); } catch { return errorResult('owner_required', 'Authenticated Direct owner is required.'); }
    if (this.#closed) return errorResult('direct_files_closed', 'Direct file access is closed.');
    const validate = validators.get(name);
    if (!validate) return errorResult('unknown_tool', 'Unknown Direct file tool.');
    if (!validate(args)) return errorResult('invalid_arguments', 'Arguments do not match the Direct file tool schema; unknown fields are rejected.');
    const writing = name === 'write_direct_file' || name === 'edit_direct_file';
    let project;
    try { project = projectFor(this.#projects, args.projectId, writing ? 'write' : 'read'); }
    catch { return errorResult('project_access_denied', 'The project is unknown or this operation is disabled by its owner.'); }
    const input = { operation: name, projectPath: project.path, roots: this.#host.allowedRoots, args: { ...args,
      ...(args.expectedSha256 ? { expectedSha256: args.expectedSha256.toLowerCase() } : {}) } };
    const execute = async () => {
      try {
        if (this.#host.transport === 'local') return await runFileOperation(input);
        return await this.#runRemote(await fixedRemoteScript(), input, { timeoutMs: 15000, maxBytes: 1024 * 1024 });
      } catch {
        return errorResult('remote_file_operation_failed', 'Direct file access could not complete on the selected SSH host.');
      }
    };
    if (!writing) return execute();
    // Serialize all writes for this host instance. Path or inode keys can miss
    // case/Unicode aliases, overlapping projects, or an inode replaced by rename.
    const task = this.#writeTail.then(execute);
    this.#writeTail = task.catch(() => {});
    return task;
  }
  close() { this.#closed = true; this.#runner?.close(); }
}
