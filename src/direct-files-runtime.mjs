// SPDX-License-Identifier: Apache-2.0
// Node adaptation of Codex Bridge's project-scoped Direct file tools and
// expected_sha256 design (Fanch-hui/codex-bridge). Modified for this gateway's
// explicit host permissions, bounded text I/O and fixed-program SSH transport.
import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { posix as path } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export const FILE_BYTES = 200 * 1024;
const OUTPUT_BYTES = 64 * 1024;
const SCAN_FILES = 1000;
const SCAN_BYTES = 2 * 1024 * 1024;
const SKIP_DIRS = new Set(['.git', 'node_modules', '.venv', 'venv', '__pycache__']);
const BINARY = /\.(?:safetensors|pt|pth|ckpt|onnx|npy|npz|bin|zip|gz|tar|7z|png|jpe?g|gif|webp|pdf|mp[34]|wav|sqlite3?|db)$/i;
const fail = (code, message) => { throw Object.assign(new Error(message), { safeCode: code }); };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const inside = (root, target) => { const rel = path.relative(root, target); return rel === '' || (rel !== '..' && !rel.startsWith('../') && !path.isAbsolute(rel)); };
const missing = error => error?.code === 'ENOENT';

function relativePath(value, allowEmpty = false) {
  if (typeof value !== 'string' || value.length > 2048 || value.includes('\0') || value.includes('\\') || path.isAbsolute(value)
    || (!value && !allowEmpty) || value.split('/').some(part => part === '..') || value.split('/').length > 32) {
    fail('invalid_path', 'Use a bounded relative project path without parent traversal.');
  }
  const parts = value.split('/').filter(part => part && part !== '.');
  return parts.join('/');
}

async function noSymlinks(absolute, { finalMissing = false } = {}) {
  let cursor = '/';
  const parts = absolute.split('/').filter(Boolean);
  for (let index = 0; index < parts.length; index++) {
    cursor = path.join(cursor, parts[index]);
    let stat;
    try { stat = await fs.lstat(cursor); }
    catch (error) { if (finalMissing && index === parts.length - 1 && missing(error)) return null; throw error; }
    if (stat.isSymbolicLink()) fail('symlink_rejected', 'Symlinks are unavailable through Direct files.');
    if (index < parts.length - 1 && !stat.isDirectory()) fail('invalid_path', 'A parent path is not a directory.');
    if (index === parts.length - 1) return stat;
  }
  return fs.lstat('/');
}

export async function resolveProject(projectPath, roots) {
  if (typeof projectPath !== 'string' || !path.isAbsolute(projectPath) || projectPath.includes('\0') || !Array.isArray(roots) || !roots.length || roots.length > 128) {
    fail('invalid_project', 'The owner must configure an absolute Direct project and allowed roots.');
  }
  const projectStat = await noSymlinks(projectPath);
  if (!projectStat.isDirectory()) fail('invalid_project', 'The configured project is not a directory.');
  const project = await fs.realpath(projectPath);
  let allowed = false;
  for (const root of roots) {
    if (typeof root !== 'string' || !path.isAbsolute(root) || root.includes('\0')) continue;
    try { const canonical = await fs.realpath(root); if ((await fs.stat(canonical)).isDirectory() && inside(canonical, project)) allowed = true; } catch {}
  }
  if (!allowed) fail('outside_allowed_roots', 'The project is outside the configured host roots.');
  return project;
}

export async function resolveTarget(project, relative, { allowEmpty = false, create = false } = {}) {
  const clean = relativePath(relative, allowEmpty);
  const target = path.resolve(project, clean);
  if (!inside(project, target)) fail('invalid_path', 'The path must remain inside the project.');
  const stat = await noSymlinks(target, { finalMissing: create });
  const canonical = await fs.realpath(stat ? target : path.dirname(target));
  if (!inside(project, canonical)) fail('outside_project', 'The path resolves outside the project.');
  return { target, stat, clean };
}

async function readText(target) {
  const handle = await fs.open(target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await handle.stat();
    if (!stat.isFile()) fail('not_regular_file', 'Only regular text files are supported.');
    if (stat.size > FILE_BYTES) fail('text_file_too_large', 'Text files are limited to 200 KiB.');
    const buffer = Buffer.alloc(FILE_BYTES + 1);
    let size = 0;
    while (size < buffer.length) {
      const result = await handle.read(buffer, size, buffer.length - size, size);
      if (!result.bytesRead) break;
      size += result.bytesRead;
    }
    if (size > FILE_BYTES) fail('text_file_too_large', 'Text files are limited to 200 KiB.');
    const bytes = buffer.subarray(0, size);
    let text;
    try { text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes); }
    catch { fail('not_text', 'Only valid UTF-8 text files are supported.'); }
    if (text.includes('\0')) fail('not_text', 'Binary files are unavailable through Direct files.');
    return { text, bytes, stat, sha256: hash(bytes) };
  } finally { await handle.close(); }
}

export const identityEqual = (a, b) => a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs;
function contentBytes(content) {
  const bytes = Buffer.from(content, 'utf8');
  if (content.includes('\0')) fail('not_text', 'Text content cannot contain NUL bytes.');
  if (bytes.length > FILE_BYTES) fail('text_file_too_large', 'Text files are limited to 200 KiB.');
  return bytes;
}

async function replaceText(project, relative, expected, makeContent) {
  if (relative.split('/').includes('.git')) fail('protected_path', 'Git metadata cannot be changed through Direct file writes.');
  const original = await resolveTarget(project, relative, { create: true });
  if (original.stat && !original.stat.isFile()) fail('not_regular_file', 'Only regular text files are supported.');
  if (expected === null && original.stat) fail('file_exists', 'The file exists; read its hash before overwriting it.');
  if (expected !== null && !original.stat) fail('hash_conflict', 'The file is missing; the expected hash cannot match.');
  const before = original.stat ? await readText(original.target) : null;
  if (before && before.sha256 !== expected) fail('hash_conflict', 'The file changed; read it again before editing.');
  const result = makeContent(before?.text ?? '');
  const bytes = contentBytes(result.content);
  const temp = path.join(path.dirname(original.target), `.direct-${randomUUID()}.tmp`);
  let handle;
  try {
    handle = await fs.open(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, before ? before.stat.mode & 0o777 : 0o600);
    if (before) await handle.chmod(before.stat.mode & 0o777);
    await handle.writeFile(bytes); await handle.sync(); await handle.close(); handle = null;
    const current = await resolveTarget(project, relative, { create: true });
    if (before) {
      if (!current.stat || !identityEqual(before.stat, current.stat)) fail('hash_conflict', 'The file changed while the edit was prepared.');
      const verified = await readText(current.target);
      if (!identityEqual(before.stat, verified.stat) || verified.sha256 !== expected) fail('hash_conflict', 'The file changed while the edit was prepared.');
      // This recheck reduces races; rename is atomic but is not a kernel CAS.
      await fs.rename(temp, original.target);
    } else {
      // Hard linking the prepared same-directory file gives exclusive creation.
      await fs.link(temp, original.target);
      await fs.unlink(temp);
    }
    return { ok: true, path: original.clean, sha256: hash(bytes), bytes: bytes.length, created: !before, ...result.extra };
  } catch (error) {
    if (error.code === 'EEXIST') fail('file_exists', 'Another writer created the file; read it before editing.');
    throw error;
  } finally {
    if (handle) await handle.close().catch(() => {});
    await fs.unlink(temp).catch(() => {});
  }
}

function clipUtf8(value, maxBytes) {
  const fits = text => Buffer.byteLength(JSON.stringify(text)) - 2 <= maxBytes;
  if (fits(value)) return { value, truncated: false };
  let low = 0, high = value.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (fits(value.slice(0, middle))) low = middle;
    else high = middle - 1;
  }
  if (low > 0 && /[\uD800-\uDBFF]/.test(value[low - 1])) low--;
  return { value: value.slice(0, low), truncated: true };
}

async function walk(project, start, visit) {
  const stats = { scannedEntries: 0, scannedFiles: 0, scannedBytes: 0, skipped: 0, truncated: false };
  const queue = [start];
  while (queue.length) {
    const directory = queue.shift();
    // Revalidate each directory immediately before walking it.
    await resolveTarget(project, path.relative(project, directory), { allowEmpty: true });
    const stream = await fs.opendir(directory);
    try {
      for await (const entry of stream) {
        if (++stats.scannedEntries > SCAN_FILES * 2) { stats.truncated = true; return stats; }
        if (SKIP_DIRS.has(entry.name) || entry.isSymbolicLink() || (!entry.isFile() && !entry.isDirectory())) { stats.skipped++; continue; }
        const target = path.join(directory, entry.name);
        if (entry.isDirectory()) { queue.push(target); continue; }
        if (++stats.scannedFiles > SCAN_FILES) { stats.truncated = true; return stats; }
        if (BINARY.test(entry.name)) { stats.skipped++; continue; }
        const keepGoing = await visit(target, stats);
        if (!keepGoing) { stats.truncated = true; return stats; }
      }
    } catch (error) { if (error.safeCode) throw error; stats.skipped++; }
  }
  return stats;
}

export async function runFileOperation(input) {
  try {
    const { operation, args, projectPath, roots } = input;
    const project = await resolveProject(projectPath, roots);
    if (operation === 'write_direct_file') return await replaceText(project, args.path, args.expectedSha256, () => ({ content: args.content }));
    if (operation === 'edit_direct_file') {
      if (!args.oldText) fail('invalid_edit', 'oldText must be nonempty.');
      return await replaceText(project, args.path, args.expectedSha256, text => {
        const count = text.split(args.oldText).length - 1;
        if (!count) fail('text_not_found', 'oldText does not occur in the file.');
        if (count !== 1 && !args.replaceAll) fail('ambiguous_edit', 'oldText occurs more than once; use replaceAll explicitly.');
        return { content: args.replaceAll ? text.replaceAll(args.oldText, () => args.newText) : text.replace(args.oldText, () => args.newText), extra: { replacements: args.replaceAll ? count : 1 } };
      });
    }
    const location = await resolveTarget(project, args.path ?? '', { allowEmpty: operation !== 'read_direct_file' });
    if (operation === 'read_direct_file') {
      if (!location.stat.isFile()) fail('not_regular_file', 'Only regular text files are supported.');
      const file = await readText(location.target);
      const lines = file.text === '' ? [] : file.text.split('\n');
      const offset = args.offset ?? 0, limit = args.limit ?? 200;
      const selected = lines.slice(offset, offset + limit);
      const clipped = clipUtf8(selected.join('\n'), OUTPUT_BYTES);
      return { ok: true, path: location.clean, content: clipped.value, sha256: file.sha256, bytes: file.bytes.length,
        offset, totalLines: lines.length, returnedLines: clipped.truncated ? clipped.value.split('\n').length : selected.length,
        truncated: clipped.truncated || offset + selected.length < lines.length };
    }
    if (!location.stat.isDirectory()) fail('not_directory', 'Listing and search require a directory path.');
    if (operation === 'list_direct_files') {
      const limit = args.limit ?? 200, entries = []; let skipped = 0, scannedEntries = 0, truncated = false, outputBytes = 0;
      const stream = await fs.opendir(location.target);
      for await (const entry of stream) {
        if (++scannedEntries > SCAN_FILES * 2) { truncated = true; break; }
        if (SKIP_DIRS.has(entry.name) || entry.isSymbolicLink() || (!entry.isFile() && !entry.isDirectory())) { skipped++; continue; }
        if (entries.length >= limit) { truncated = true; break; }
        const item = { path: path.relative(project, path.join(location.target, entry.name)), type: entry.isDirectory() ? 'directory' : 'file' };
        outputBytes += Buffer.byteLength(JSON.stringify(item));
        if (outputBytes > OUTPUT_BYTES) { truncated = true; break; }
        entries.push(item);
      }
      entries.sort((a, b) => a.path.localeCompare(b.path));
      return { ok: true, path: location.clean, entries, skipped, scannedEntries, truncated };
    }
    if (operation === 'search_direct_files') {
      const matches = [], limit = args.limit ?? 100; let outputBytes = 0;
      const stats = await walk(project, location.target, async (target, stats) => {
        let file;
        try {
          const relative = path.relative(project, target);
          await resolveTarget(project, relative);
          const stat = await fs.lstat(target);
          if (stat.size > FILE_BYTES) { stats.skipped++; return true; }
          if (stats.scannedBytes + stat.size > SCAN_BYTES) return false;
          stats.scannedBytes += stat.size;
          file = await readText(target);
          stats.scannedBytes += Math.max(0, file.bytes.length - stat.size);
          if (stats.scannedBytes > SCAN_BYTES) return false;
        } catch (error) { stats.skipped++; return true; }
        const lines = file.text.split('\n');
        for (let index = 0; index < lines.length; index++) {
          const column = lines[index].indexOf(args.query);
          if (column < 0) continue;
          if (matches.length >= limit) return false;
          const clip = clipUtf8(lines[index].slice(Math.max(0, column - 80), column + args.query.length + 160), 512);
          const item = { path: path.relative(project, target), line: index + 1, column: column + 1, text: clip.value, textTruncated: clip.truncated || column > 80 || lines[index].length > column + args.query.length + 160 };
          outputBytes += Buffer.byteLength(JSON.stringify(item));
          if (outputBytes > OUTPUT_BYTES) return false;
          matches.push(item);
        }
        return true;
      });
      return { ok: true, path: location.clean, matches, ...stats };
    }
    fail('unknown_tool', 'Unknown Direct file tool.');
  } catch (error) {
    const code = error.safeCode ?? ({ ENOENT: 'not_found', EACCES: 'permission_denied', EPERM: 'permission_denied', ELOOP: 'symlink_rejected', ENOTDIR: 'not_directory' }[error.code] ?? 'file_operation_failed');
    return { ok: false, error: { code, message: error.safeCode ? error.message : 'The file operation could not be completed on the selected host.' } };
  }
}
