#!/usr/bin/env node
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { StringDecoder } from 'node:string_decoder';
import { DEFAULT_AGENT_STATE_DIR, AGENT_ID_PATTERN, readAgentCapability, validateAgentMessage, MAX_MESSAGE_LENGTH } from '../src/agent-channel.js';

function parse(args) {
  const values = {};
  const allowed = new Set(['--request-id', '--kind', '--message-id', '--text']);
  for (let i = 0; i < args.length; i++) {
    const flag = args[i];
    if (!allowed.has(flag) || Object.hasOwn(values, flag) || i + 1 === args.length) throw new Error('invalid_arguments');
    values[flag] = args[++i];
  }
  return values;
}
async function readText(input) {
  let text = '';
  const decoder = new StringDecoder('utf8');
  for await (const chunk of input) {
    text += typeof chunk === 'string' ? chunk : decoder.write(chunk);
    if (text.length > MAX_MESSAGE_LENGTH) throw new Error('invalid_message');
  }
  return text + decoder.end();
}
function submit(socketPath, key, value, timeoutMs) {
  return new Promise((resolve, reject) => {
    let connected = false;
    let timer;
    const request = http.request({ socketPath, method: 'POST', path: '/message', headers: {
      authorization: `Bearer ${key}`, 'content-type': 'application/json',
    } }, response => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', chunk => { body += chunk; if (body.length > 2048) request.destroy(new Error('invalid_response')); });
      response.once('error', reject);
      response.once('aborted', () => reject(new Error('response_aborted')));
      response.once('end', () => {
        if (response.statusCode !== 200) { reject(Object.assign(new Error('rejected'), { rejected: true, statusCode: response.statusCode })); return; }
        try {
          const receipt = JSON.parse(body);
          if (receipt.messageId !== value.messageId || !['accepted', 'duplicate'].includes(receipt.status)) throw new Error('invalid_response');
          resolve({ messageId: receipt.messageId, status: receipt.status });
        } catch { reject(new Error('invalid_response')); }
      });
    });
    request.on('socket', socket => { if (!socket.connecting) connected = true; socket.once('connect', () => { connected = true; }); });
    request.once('error', error => { error.notReady = !connected && ['ENOENT', 'ECONNREFUSED'].includes(error.code); reject(error); });
    request.once('close', () => clearTimeout(timer));
    timer = setTimeout(() => request.destroy(new Error('network_timeout')), timeoutMs);
    request.end(JSON.stringify(value));
  });
}

// Explicit dependency injection is for tests/embedding only. The executable
// always uses its fixed local state directory and Paseo-provided agent identity.
export async function runDotMessage(args, { env = process.env, stateDir = DEFAULT_AGENT_STATE_DIR, stdin = process.stdin, stdout = process.stdout, stderr = process.stderr, readyTimeoutMs = 3000, retryDelayMs = 100, networkTimeoutMs = 5000 } = {}) {
  let messageId;
  try {
    const options = parse(args);
    messageId = options['--message-id'] ?? randomUUID();
    if (!AGENT_ID_PATTERN.test(env.PASEO_AGENT_ID ?? '')) throw new Error('missing_paseo_agent_id');
    const value = validateAgentMessage({ agentId: env.PASEO_AGENT_ID, requestId: options['--request-id'], messageId,
      kind: options['--kind'] ?? 'message', text: options['--text'] ?? await readText(stdin) });
    const deadline = Date.now() + Math.min(3000, Math.max(0, readyTimeoutMs));
    while (true) {
      try {
        const key = await readAgentCapability(stateDir, value.agentId);
        const receipt = await submit(join(stateDir, 'agent-channel.sock'), key, value, networkTimeoutMs);
        stdout.write(`${JSON.stringify(receipt)}\n`);
        return 0;
      } catch (error) {
        const notReady = error.code === 'ENOENT' || error.notReady;
        if (notReady && Date.now() < deadline) {
          await new Promise(resolve => setTimeout(resolve, Math.min(retryDelayMs, deadline - Date.now())));
          continue;
        }
        const permissionDenied = ['EPERM','EACCES'].includes(error.code);
        const status = permissionDenied ? 'permission_denied' : notReady ? 'channel_not_ready' : error.rejected && error.statusCode < 500 ? 'rejected' : 'outcome_unknown';
        const safeReasons = new Set(['EPERM','EACCES','ECONNREFUSED','ENOENT','network_timeout','response_aborted','invalid_response']);
        const reason = safeReasons.has(error.code ?? error.message) ? (error.code ?? error.message) : error.rejected ? `http_${error.statusCode}` : 'local_channel_error';
        stderr.write(`${JSON.stringify({ messageId, status, reason })}\n`);
        if (permissionDenied) stderr.write('The fixed local IPC command was denied. Request permission for this exact command; do not change global agent permissions.\n');
        if (status === 'outcome_unknown') stderr.write('Receipt unknown; retry with the same --message-id to recover safely.\n');
        return 1;
      }
    }
  } catch {
    stderr.write(`${JSON.stringify({ ...(AGENT_ID_PATTERN.test(messageId ?? '') ? { messageId } : {}), status: 'invalid_input' })}\n`);
    return 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) process.exitCode = await runDotMessage(process.argv.slice(2));
