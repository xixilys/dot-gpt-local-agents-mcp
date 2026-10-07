// SPDX-License-Identifier: Apache-2.0
// Binary counterpart to the project-scoped Direct file runtime. Path checks
// and the final optimistic overwrite check use the same text-file policy.
import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { posix as path } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { resolveProject, resolveTarget, identityEqual } from './direct-files-runtime.mjs';

export const MAX_BINARY_BYTES = 256 * 1024 * 1024;
export const BINARY_CHUNK_BYTES = 64 * 1024;
export const binaryError = (code, message = code) => Object.assign(new Error(message), { safeCode: code, code });
const fail = (code, message) => { throw binaryError(code, message); };
const checkSignal = signal => { if (signal?.aborted) throw signal.reason ?? binaryError('transfer_aborted'); };

export function validateBinaryInput(input, { importing = false } = {}) {
  const maxBytes = input.maxBytes ?? MAX_BINARY_BYTES;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_BINARY_BYTES) fail('invalid_limit', 'Binary transfers require a positive limit of at most 256 MiB.');
  if (importing) {
    if (input.expectedSha256 !== null && !/^[a-fA-F0-9]{64}$/.test(input.expectedSha256 ?? '')) fail('invalid_hash', 'An expected SHA-256 or null is required.');
    if (input.sha256 !== undefined && !/^[a-fA-F0-9]{64}$/.test(input.sha256)) fail('invalid_hash', 'The incoming SHA-256 is invalid.');
    if (input.bytes !== undefined && (!Number.isSafeInteger(input.bytes) || input.bytes < 0 || input.bytes > maxBytes)) fail('invalid_length', 'The incoming length exceeds the binary transfer limit.');
  }
  return maxBytes;
}

async function openSource(location) {
  const handle = await fs.open(location.target, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await handle.stat();
    if (!stat.isFile()) fail('not_regular_file', 'Binary transfers require a regular file.');
    if (!identityEqual(location.stat, stat)) fail('hash_conflict', 'The file changed before it was opened.');
    return { handle, stat };
  } catch (error) { await handle.close(); throw error; }
}

// One fixed-size allocation regardless of file size. Reads also check for a
// growing source by reading to EOF rather than trusting its initial length.
async function readSource(source, maxBytes, consume, signal) {
  const buffer = Buffer.allocUnsafe(BINARY_CHUNK_BYTES);
  const hash = createHash('sha256');
  let bytes = 0;
  while (true) {
    checkSignal(signal);
    const { bytesRead } = await source.handle.read(buffer, 0, buffer.length, bytes);
    if (!bytesRead) break;
    bytes += bytesRead;
    if (bytes > maxBytes) fail('binary_file_too_large', 'The file exceeds the binary transfer limit.');
    const chunk = buffer.subarray(0, bytesRead);
    hash.update(chunk);
    if (consume) await consume(chunk);
  }
  checkSignal(signal);
  if (bytes !== source.stat.size || !identityEqual(source.stat, await source.handle.stat())) fail('source_changed', 'The source changed during the transfer.');
  return { bytes, sha256: hash.digest('hex') };
}

async function hashCurrent(location, maxBytes, signal) {
  const source = await openSource(location);
  try { return { ...await readSource(source, maxBytes, null, signal), stat: source.stat }; }
  finally { await source.handle.close(); }
}

async function writeAll(handle, chunk, signal) {
  let offset = 0;
  while (offset < chunk.length) {
    checkSignal(signal);
    const { bytesWritten } = await handle.write(chunk, offset, chunk.length - offset);
    if (!bytesWritten) fail('write_failed', 'The destination stopped accepting bytes.');
    offset += bytesWritten;
  }
}

export async function exportBinary(input, consume, { signal } = {}) {
  const maxBytes = validateBinaryInput(input);
  checkSignal(signal);
  const project = await resolveProject(input.projectPath, input.roots);
  const location = await resolveTarget(project, input.path);
  if (!location.stat.isFile()) fail('not_regular_file', 'Binary transfers require a regular file.');
  const source = await openSource(location);
  try {
    if (source.stat.size > maxBytes) fail('binary_file_too_large', 'The file exceeds the binary transfer limit.');
    const result = await readSource(source, maxBytes, consume, signal);
    const current = await resolveTarget(project, input.path);
    if (!identityEqual(source.stat, current.stat)) fail('source_changed', 'The source path changed during the transfer.');
    return { ...result, fileName: path.basename(location.clean) };
  } finally { await source.handle.close(); }
}

export async function importBinary(input, readable, { signal } = {}) {
  const maxBytes = validateBinaryInput(input, { importing: true });
  checkSignal(signal);
  const project = await resolveProject(input.projectPath, input.roots);
  if (typeof input.path === 'string' && input.path.split('/').includes('.git')) fail('protected_path', 'Git metadata cannot be changed through Direct file writes.');
  const original = await resolveTarget(project, input.path, { create: true });
  if (original.stat && !original.stat.isFile()) fail('not_regular_file', 'Binary transfers require a regular file.');
  if (input.expectedSha256 === null && original.stat) fail('file_exists', 'The file exists; obtain its hash before overwriting it.');
  if (input.expectedSha256 !== null && !original.stat) fail('hash_conflict', 'The expected file is missing.');
  const expected = input.expectedSha256?.toLowerCase();
  const before = original.stat ? await hashCurrent(original, maxBytes, signal) : null;
  if (before && before.sha256 !== expected) fail('hash_conflict', 'The destination hash changed.');
  const temp = path.join(path.dirname(original.target), `.direct-binary-${randomUUID()}.tmp`);
  let handle;
  const onAbort = () => readable.destroy?.(signal.reason ?? binaryError('transfer_aborted'));
  signal?.addEventListener('abort', onAbort, { once: true });
  try {
    checkSignal(signal);
    handle = await fs.open(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
    const hash = createHash('sha256');
    let bytes = 0;
    for await (const value of readable) {
      checkSignal(signal);
      if (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) fail('invalid_stream', 'The incoming stream must contain bytes.');
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
      bytes += chunk.length;
      if (bytes > maxBytes || (input.bytes !== undefined && bytes > input.bytes)) fail('binary_file_too_large', 'The incoming stream exceeds its declared limit.');
      hash.update(chunk);
      await writeAll(handle, chunk, signal);
    }
    checkSignal(signal);
    if (input.bytes !== undefined && bytes !== input.bytes) fail('length_mismatch', 'The incoming stream ended before its declared length.');
    const sha256 = hash.digest('hex');
    if (input.sha256 !== undefined && sha256 !== input.sha256.toLowerCase()) fail('hash_mismatch', 'The incoming file does not match its SHA-256.');
    await handle.sync();
    const current = await resolveTarget(project, input.path, { create: true });
    if (before) {
      if (!current.stat || !identityEqual(before.stat, current.stat)) fail('hash_conflict', 'The destination changed while receiving the file.');
      const verified = await hashCurrent(current, maxBytes, signal);
      if (!identityEqual(before.stat, verified.stat) || verified.sha256 !== expected) fail('hash_conflict', 'The destination changed while receiving the file.');
      await handle.chmod(before.stat.mode & 0o777);
    } else if (current.stat) fail('file_exists', 'Another writer created the destination.');
    await handle.close(); handle = null;
    checkSignal(signal);
    // Same semantics as text writes: exclusive creation, or atomic rename after
    // optimistic identity/hash checks. Rename is not a kernel compare-and-swap.
    if (before) await fs.rename(temp, original.target);
    else await fs.link(temp, original.target);
    return { bytes, sha256, path: original.clean };
  } catch (error) {
    if (error.code === 'EEXIST') fail('file_exists', 'Another writer created the destination.');
    throw error;
  } finally {
    signal?.removeEventListener('abort', onAbort);
    if (handle) await handle.close().catch(() => {});
    await fs.unlink(temp).catch(() => {});
  }
}

export { writeAll };
