import https from 'node:https';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { constants } from 'node:fs';
import * as fs from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { isPublicAddress } from './event-delivery.js';

export class FileDownloadError extends Error {
  constructor(code) { super(`File download could not complete (${code}).`); this.code = code; this.safeCode = code; }
}
const failure = code => new FileDownloadError(code);
export function fileDownloadURL(value) {
  let url;
  try { url = new URL(value); } catch { throw failure('invalid_download_url'); }
  if (typeof value !== 'string' || value.length > 8192 || url.protocol !== 'https:' || !url.hostname
    || url.username || url.password || value.includes('#') || url.hash) throw failure('invalid_download_url');
  return url;
}
function cancellable(promise, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    if (signal.aborted) { abort(); return; }
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(promise).then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

/** Streaming, outbound-only GET. No caller headers, redirects, decompression,
 * reused sockets, or second DNS lookup are permitted. Errors omit the URL. */
export class SafeFileDownloadClient {
  constructor({ resolve = lookup, request = https.request, timeoutMs = 120000 } = {}) {
    this.resolve = resolve; this.request = request; this.timeoutMs = timeoutMs;
  }
  async downloadTo(value, { destination, maxBytes, sha256, signal, timeoutMs = this.timeoutMs } = {}) {
    const url = fileDownloadURL(value);
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0 || maxBytes > 256 * 1024 * 1024
      || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120000
      || (sha256 !== undefined && !/^[a-fA-F0-9]{64}$/.test(sha256))) throw failure('invalid_download_limits');
    const controller = new AbortController();
    const abort = () => controller.abort(failure('transfer_aborted'));
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    const timer = setTimeout(() => controller.abort(failure('transfer_timeout')), timeoutMs);
    let req, response, handle, output, created = false;
    const stop = () => { req?.destroy(); response?.destroy(); output?.destroy(); };
    controller.signal.addEventListener('abort', stop, { once: true });
    const check = () => { if (controller.signal.aborted) throw controller.signal.reason; };
    try {
      check();
      const hostname = url.hostname.replace(/^\[|\]$/g, '');
      let records;
      try {
        records = await cancellable(isIP(hostname) ? [{ address: hostname, family: isIP(hostname) }]
          : Promise.resolve().then(() => this.resolve(hostname, { all: true, verbatim: true })), controller.signal);
      } catch { check(); throw failure('download_dns_error'); }
      check();
      if (!Array.isArray(records) || !records.length || records.some(record => !record
        || !isPublicAddress(record.address) || isIP(record.address) !== record.family)) throw failure('unsafe_download_destination');
      const pinned = records[0];
      const pinnedLookup = (_name, options, callback) => {
        if (typeof options === 'function') { callback = options; options = {}; }
        if (options?.all) callback(null, [pinned]); else callback(null, pinned.address, pinned.family);
      };
      response = await new Promise((resolve, reject) => {
        const onAbort = () => reject(controller.signal.reason);
        controller.signal.addEventListener('abort', onAbort, { once: true });
        try {
          req = this.request({ protocol: 'https:', hostname, port: url.port || 443, path: `${url.pathname}${url.search}`,
            method: 'GET', headers: { 'Accept-Encoding': 'identity', Accept: 'application/octet-stream', 'User-Agent': 'Local-Agents-MCP/0.5.0' },
            lookup: pinnedLookup, family: pinned.family, agent: false, servername: isIP(hostname) ? '' : hostname,
            rejectUnauthorized: true }, res => {
            controller.signal.removeEventListener('abort', onAbort);
            // The remote may disconnect while the exclusive spool fd is being
            // opened, before pipeline installs its stream error listeners.
            res.on('error', () => {});
            if (controller.signal.aborted) { res.destroy(); reject(controller.signal.reason); return; }
            resolve(res);
          });
          req.on('error', () => { controller.signal.removeEventListener('abort', onAbort); reject(failure('download_transport_error')); });
          req.end();
        } catch { controller.signal.removeEventListener('abort', onAbort); reject(failure('download_transport_error')); }
      });
      check();
      if (response.statusCode >= 300 && response.statusCode < 400) throw failure('download_redirect_rejected');
      if (response.statusCode !== 200) throw failure('download_http_error');
      const encoding = response.headers?.['content-encoding'];
      if (encoding && encoding.toLowerCase() !== 'identity') throw failure('download_encoding_rejected');
      const lengthHeader = response.headers?.['content-length'];
      if (lengthHeader !== undefined && !/^(0|[1-9][0-9]*)$/.test(lengthHeader)) throw failure('download_length_invalid');
      const length = lengthHeader === undefined ? null : Number(lengthHeader);
      if (length !== null && (!Number.isSafeInteger(length) || length > maxBytes)) throw failure('file_too_large');
      handle = await fs.open(destination, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      created = true; check();
      const hash = createHash('sha256'); let bytes = 0;
      output = new Writable({ write(chunk, _encoding, callback) {
        bytes += chunk.length;
        if (bytes > maxBytes) { callback(failure('file_too_large')); return; }
        hash.update(chunk);
        (async () => {
          let offset = 0;
          while (offset < chunk.length) {
            check();
            const result = await handle.write(chunk, offset, chunk.length - offset);
            if (!result.bytesWritten) throw failure('download_failed');
            offset += result.bytesWritten;
          }
        })().then(() => callback(), callback);
      } });
      await pipeline(response, output, { signal: controller.signal });
      check();
      if (length !== null && bytes !== length) throw failure('download_length_mismatch');
      const actualHash = hash.digest('hex');
      if (sha256 && actualHash !== sha256.toLowerCase()) throw failure('hash_mismatch');
      await handle.sync(); check();
      await handle.close(); handle = null;
      return { bytes, sha256: actualHash };
    } catch (error) {
      stop();
      if (handle) { await handle.close().catch(() => {}); handle = null; }
      if (created) await fs.unlink(destination).catch(() => {});
      if (controller.signal.aborted) throw controller.signal.reason;
      throw error instanceof FileDownloadError ? error : failure('download_failed');
    } finally {
      clearTimeout(timer); signal?.removeEventListener('abort', abort);
      controller.signal.removeEventListener('abort', stop);
      req?.destroy(); response?.destroy();
    }
  }
}
