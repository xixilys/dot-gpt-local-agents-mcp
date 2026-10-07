import { spawn } from 'node:child_process';
import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import { validateSshTarget, sshArguments, validateNodeCommand } from './ssh-bridge.mjs';
import { exportBinary, importBinary, validateBinaryInput, writeAll, binaryError, MAX_BINARY_BYTES, BINARY_CHUNK_BYTES } from './binary-files-runtime.mjs';

const PROTOCOL_LIMIT = 64 * 1024;
const RESULT_PREFIX = 'DIRECT_BINARY_RESULT:';
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
const checkSignal = signal => { if (signal?.aborted) throw signal.reason ?? binaryError('transfer_aborted'); };
const byteStream = value => value?.getReader ? Readable.fromWeb(value) : value;

// Resolve only after the stream consumed this bounded chunk. This gives real
// backpressure on both SSH stdin and the remote stdout pipe.
async function writeChunk(stream, chunk, signal) {
  checkSignal(signal);
  await new Promise((resolve, reject) => {
    const onAbort = () => finish(signal.reason ?? binaryError('transfer_aborted'));
    const finish = error => { signal?.removeEventListener('abort', onAbort); error ? reject(error) : resolve(); };
    signal?.addEventListener('abort', onAbort, { once: true });
    stream.write(chunk, finish);
    if (signal?.aborted) onAbort();
  });
}

// A terminating zero frame is mandatory. EOF before it can never commit a
// partially delivered import, even when HTTP did not declare Content-Length.
async function* decodeFrames(iterator, initial = Buffer.alloc(0)) {
  let pending = initial;
  const take = async size => {
    const result = Buffer.allocUnsafe(size);
    let offset = 0;
    while (offset < size) {
      if (!pending.length) {
        const next = await iterator.next();
        if (next.done) throw binaryError('truncated_stream', 'The transfer ended before its completion frame.');
        pending = Buffer.from(next.value);
      }
      const count = Math.min(size - offset, pending.length);
      pending.copy(result, offset, 0, count); pending = pending.subarray(count); offset += count;
    }
    return result;
  };
  while (true) {
    const size = (await take(4)).readUInt32BE();
    if (!size) {
      if (pending.length || !(await iterator.next()).done) throw binaryError('invalid_framing', 'Bytes followed the completion frame.');
      return;
    }
    if (size > BINARY_CHUNK_BYTES) throw binaryError('invalid_framing', 'A binary chunk exceeded its limit.');
    yield await take(size);
  }
}

async function readHeader(stream) {
  const iterator = stream[Symbol.asyncIterator]();
  let pending = Buffer.alloc(0);
  while (true) {
    const next = await iterator.next();
    if (next.done) throw binaryError('invalid_framing');
    const chunk = Buffer.from(next.value), newline = chunk.indexOf(10);
    if (pending.length + (newline < 0 ? chunk.length : newline) > PROTOCOL_LIMIT) throw binaryError('metadata_limit');
    if (newline >= 0) return { input: JSON.parse(Buffer.concat([pending, chunk.subarray(0, newline)]).toString('utf8')), iterator, rest: chunk.subarray(newline + 1) };
    pending = Buffer.concat([pending, chunk]);
  }
}

let remoteProgram;
async function fixedRemoteProgram() {
  remoteProgram ??= (async () => {
    const directSource = await fs.readFile(new URL('./direct-files-runtime.mjs', import.meta.url), 'utf8');
    const directUrl = `data:text/javascript;base64,${Buffer.from(directSource).toString('base64')}`;
    const binarySource = (await fs.readFile(new URL('./binary-files-runtime.mjs', import.meta.url), 'utf8'))
      .replace("'./direct-files-runtime.mjs'", JSON.stringify(directUrl));
    // Only installed fixed source is encoded here. User paths and bytes are
    // exclusively in stdin, and no remote helper/service is installed.
    const binaryUrl = `data:text/javascript;base64,${Buffer.from(binarySource).toString('base64')}`;
    return `import { Readable } from 'node:stream';
import { exportBinary, importBinary, binaryError, BINARY_CHUNK_BYTES } from ${JSON.stringify(binaryUrl)};
const PROTOCOL_LIMIT = ${PROTOCOL_LIMIT};
const checkSignal = ${checkSignal.toString()};
const writeChunk = ${writeChunk.toString()};
const decodeFrames = ${decodeFrames.toString()};
const readHeader = ${readHeader.toString()};
const controller = new AbortController();
const cancel = () => { controller.abort(binaryError('transfer_aborted')); process.stdin.destroy(); };
process.on('SIGTERM', cancel); process.on('SIGINT', cancel); process.on('SIGHUP', cancel);
process.stdin.on('error', () => {}); process.stdout.on('error', cancel);
let timer, direction;
try {
  const { input, iterator, rest } = await readHeader(process.stdin);
  direction = input.direction;
  if (!Number.isSafeInteger(input.timeoutMs) || input.timeoutMs < 1 || input.timeoutMs > 3600000) throw binaryError('invalid_deadline');
  timer = setTimeout(cancel, input.timeoutMs);
  let result;
  if (input.direction === 'export') {
    if (rest.length || !(await iterator.next()).done) throw binaryError('invalid_framing');
    result = await exportBinary(input, chunk => writeChunk(process.stdout, chunk, controller.signal), { signal: controller.signal });
  } else if (input.direction === 'import') {
    result = await importBinary(input, Readable.from(decodeFrames(iterator, rest)), { signal: controller.signal });
  } else throw binaryError('invalid_operation');
  if (direction === 'import') process.stdout.write(JSON.stringify({ ok: true, ...result }));
  else process.stderr.write(${JSON.stringify(RESULT_PREFIX)} + JSON.stringify({ ok: true, ...result }) + '\\n');
} catch (error) {
  const result = JSON.stringify({ ok: false, error: { code: error.safeCode ?? 'remote_file_operation_failed', message: error.safeCode ? error.message : 'The binary transfer could not complete on the SSH host.' } });
  if (direction === 'import') process.stdout.write(result);
  else process.stderr.write(${JSON.stringify(RESULT_PREFIX)} + result + '\\n');
  process.exitCode = 1;
} finally { clearTimeout(timer); process.stdin.destroy(); }
`;
  })();
  return remoteProgram;
}

function readResult(output, prefixed = true) {
  const lines = prefixed ? output.toString('utf8').split('\n').filter(line => line.startsWith(RESULT_PREFIX)) : [output.toString('utf8')];
  if (lines.length !== 1) throw binaryError('remote_invalid_output');
  let result;
  try { result = JSON.parse(prefixed ? lines[0].slice(RESULT_PREFIX.length) : lines[0]); } catch { throw binaryError('remote_invalid_output'); }
  if (!result || typeof result.ok !== 'boolean') throw binaryError('remote_invalid_output');
  if (!result.ok) {
    const error = binaryError(result.error?.code ?? 'remote_file_operation_failed', result.error?.message);
    // The runtime's named validation/hash/framing errors occur before commit.
    // A generic filesystem failure cannot prove that a post-link cleanup or
    // commit did not already change the target.
    error.remoteRejected = error.code !== 'remote_file_operation_failed';
    throw error;
  }
  if (!Number.isSafeInteger(result.bytes) || result.bytes < 0 || !/^[a-f0-9]{64}$/.test(result.sha256 ?? '')) throw binaryError('remote_invalid_output');
  const { ok, ...metadata } = result;
  return metadata;
}

export function createBinaryFileTransport({ host, spawnImpl = spawn, timeoutMs = 120000 } = {}) {
  if (!host || !['local', 'ssh'].includes(host.transport)) throw new Error('Binary files require a configured local or SSH host');
  const target = host.transport === 'ssh' ? validateSshTarget(host.target) : null;
  const nodeCommand = validateNodeCommand(host.nodeCommand);
  const active = new Set();
  let closed = false;
  const scope = async (options, execute) => {
    if (closed) throw binaryError('binary_transport_closed');
    const deadline = options.timeoutMs ?? timeoutMs;
    if (!Number.isSafeInteger(deadline) || deadline < 1 || deadline > 3600000) throw binaryError('invalid_deadline');
    const controller = new AbortController();
    const onAbort = () => controller.abort(binaryError('transfer_aborted'));
    options.signal?.addEventListener('abort', onAbort, { once: true });
    if (options.signal?.aborted) onAbort();
    const timer = setTimeout(() => controller.abort(binaryError('transfer_timeout')), deadline);
    active.add(controller);
    try { checkSignal(controller.signal); return await execute(controller, deadline); }
    finally { clearTimeout(timer); active.delete(controller); options.signal?.removeEventListener('abort', onAbort); }
  };

  async function remote(input, readable, consume, controller, deadline) {
    const script = await fixedRemoteProgram();
    checkSignal(controller.signal);
    const header = Buffer.from(JSON.stringify({ ...input, timeoutMs: deadline }) + '\n');
    if (header.length > PROTOCOL_LIMIT) throw binaryError('metadata_limit');
    const child = spawnImpl('ssh', [...sshArguments(target), target.host, `${quote(nodeCommand)} --input-type=module -e ${quote(script)}`], { stdio: ['pipe', 'pipe', 'pipe'] });
    let stderr = Buffer.alloc(0), stdout = Buffer.alloc(0), stdoutBytes = 0, killTimer, completionAttempted = false, knownRemoteFailure;
    const stop = () => {
      readable?.destroy?.(controller.signal.reason);
      child.stdin.destroy(); child.stdout.destroy();
      try { child.kill('SIGTERM'); } catch {}
      killTimer ??= setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 1000);
      killTimer.unref?.();
    };
    const closedChild = new Promise((resolve, reject) => {
      child.once('error', () => { controller.abort(binaryError('remote_connection_failed')); reject(binaryError('remote_connection_failed')); });
      child.once('close', (code, signal) => {
        clearTimeout(killTimer);
        try {
          const result = readResult(consume ? stderr : stdout, Boolean(consume));
          if (controller.signal.aborted) throw controller.signal.reason;
          if (code !== 0 || signal) throw binaryError('remote_operation_failed');
          resolve(result);
        } catch (error) {
          if (error.remoteRejected) knownRemoteFailure = error;
          controller.abort(error); reject(error);
        }
      });
    });
    // A disconnect can arrive before the consumer starts awaiting completion.
    closedChild.catch(() => {});
    child.stderr.on('data', chunk => {
      if (stderr.length + chunk.length > PROTOCOL_LIMIT) controller.abort(binaryError('remote_output_limit'));
      else stderr = Buffer.concat([stderr, chunk]);
    });
    // A remote policy/hash rejection can close stdin while sending its useful
    // error result. Let close/readResult preserve that error instead of making
    // an ordinary conflict look like a broken SSH connection.
    child.stdin.on('error', () => {});
    child.stdout.on('error', () => {});
    readable?.on?.('error', () => {});
    controller.signal.addEventListener('abort', stop, { once: true });
    const receive = (async () => {
      for await (const chunk of child.stdout) {
        checkSignal(controller.signal);
        stdoutBytes += chunk.length;
        if (stdoutBytes > (consume ? input.maxBytes ?? MAX_BINARY_BYTES : PROTOCOL_LIMIT)) throw binaryError('remote_output_limit');
        if (consume) await consume(chunk);
        else stdout = Buffer.concat([stdout, chunk]);
      }
    })();
    const send = (async () => {
      await writeChunk(child.stdin, header, controller.signal);
      if (readable) {
        let bytes = 0;
        for await (const value of readable) {
          checkSignal(controller.signal);
          if (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) throw binaryError('invalid_stream');
          const chunk = Buffer.from(value.buffer, value.byteOffset, value.byteLength);
          bytes += chunk.length;
          if (bytes > (input.maxBytes ?? MAX_BINARY_BYTES) || (input.bytes !== undefined && bytes > input.bytes)) throw binaryError('binary_file_too_large');
          for (let offset = 0; offset < chunk.length; offset += BINARY_CHUNK_BYTES) {
            const part = chunk.subarray(offset, offset + BINARY_CHUNK_BYTES), size = Buffer.allocUnsafe(4); size.writeUInt32BE(part.length);
            await writeChunk(child.stdin, size, controller.signal);
            await writeChunk(child.stdin, part, controller.signal);
          }
        }
        checkSignal(controller.signal);
        if (input.bytes !== undefined && bytes !== input.bytes) throw binaryError('length_mismatch');
        // A failed write callback cannot tell whether all four terminator bytes
        // reached SSH. Once this write begins, missing acknowledgement means
        // the import may already have committed; it must never be replayed.
        completionAttempted = true;
        await writeChunk(child.stdin, Buffer.alloc(4), controller.signal);
      }
      child.stdin.end();
    })();
    try {
      const [result] = await Promise.all([closedChild, send, receive]);
      if (consume && result.bytes !== stdoutBytes) throw binaryError('length_mismatch');
      return result;
    } catch (error) {
      if (!controller.signal.aborted && ['EPIPE', 'ERR_STREAM_DESTROYED', 'ERR_STREAM_WRITE_AFTER_END'].includes(error.code)) {
        let graceTimer;
        try {
          await Promise.race([closedChild, new Promise(resolve => { graceTimer = setTimeout(resolve, 1000); })]);
        } catch (remoteError) { error = remoteError; }
        finally { clearTimeout(graceTimer); }
      }
      controller.abort(error); stop();
      await Promise.allSettled([closedChild, send, receive]);
      const failure = knownRemoteFailure ?? controller.signal.reason ?? error;
      if (input.direction === 'import' && completionAttempted && !knownRemoteFailure) failure.resultUnknown = true;
      throw failure;
    } finally { controller.signal.removeEventListener('abort', stop); }
  }

  return {
    async exportTo(options) {
      validateBinaryInput(options);
      return scope(options, async (controller, deadline) => {
        let handle, created = false;
        try {
          handle = await fs.open(options.destination, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600); created = true;
          const hash = createHash('sha256'); let bytes = 0;
          const consume = async chunk => { checkSignal(controller.signal); bytes += chunk.length; hash.update(chunk); await writeAll(handle, chunk, controller.signal); };
          const { destination, signal, timeoutMs: unused, ...input } = options;
          const result = target ? await remote({ ...input, direction: 'export' }, null, consume, controller, deadline)
            : await exportBinary(input, consume, { signal: controller.signal });
          if (bytes !== result.bytes || hash.digest('hex') !== result.sha256) throw binaryError('hash_mismatch');
          checkSignal(controller.signal);
          await handle.sync(); await handle.close(); handle = null;
          checkSignal(controller.signal);
          return result;
        } catch (error) {
          if (handle) { await handle.close().catch(() => {}); handle = null; }
          if (created) await fs.unlink(options.destination).catch(() => {});
          throw error;
        }
      });
    },
    async importFrom(input, source, options = {}) {
      const readable = byteStream(source);
      const onError = () => {};
      readable?.on?.('error', onError);
      try {
        validateBinaryInput(input, { importing: true });
        if (!readable?.[Symbol.asyncIterator]) throw binaryError('invalid_stream');
        return await scope(options, async (controller, deadline) => {
          return target ? await remote({ ...input, direction: 'import' }, readable, null, controller, deadline)
            : await importBinary(input, readable, { signal: controller.signal });
        });
      } catch (error) { readable?.destroy?.(); throw error; }
      finally { readable?.removeListener?.('error', onError); }
    },
    close() { closed = true; for (const controller of active) controller.abort(binaryError('binary_transport_closed')); },
  };
}
