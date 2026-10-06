import { randomUUID } from 'node:crypto';

export const UPSTREAM_URL = 'http://127.0.0.1:6767/mcp/agents';

export class PaseoUpstream {
  constructor(url = UPSTREAM_URL, { fetchImpl = fetch, timeoutMs = 30_000 } = {}) {
    if (url !== UPSTREAM_URL) throw new Error('The Paseo upstream must be the fixed local endpoint');
    this.fetch = fetchImpl;
    this.timeoutMs = timeoutMs;
  }

  async request(method, params, { timeoutMs = this.timeoutMs } = {}) {
    const id = randomUUID();
    const response = await this.fetch(UPSTREAM_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, ...(params ? { params } : {}) }),
      signal: AbortSignal.timeout(timeoutMs),
      redirect: 'error',
    });
    if (!response.ok) throw new Error('Paseo upstream HTTP failure');
    // Bound the response without buffering an unlimited upstream stream.
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let body = '', bytes = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (bytes > 4 * 1024 * 1024) throw new Error('Paseo upstream response exceeds limit');
        body += decoder.decode(value, { stream: true });
        if (response.headers.get('content-type')?.includes('text/event-stream')) {
          const result = decodeSse(body, id);
          if (result) return unwrap(result);
        }
      }
      body += decoder.decode();
      const message = response.headers.get('content-type')?.includes('text/event-stream')
        ? decodeSse(body, id) : JSON.parse(body);
      if (!message || message.id !== id) throw new Error('Paseo upstream response was incomplete');
      return unwrap(message);
    } finally {
      await reader.cancel().catch(() => {});
    }
  }

  async tools() { return (await this.request('tools/list')).tools; }
  async call(name, args, options) { return this.request('tools/call', { name, arguments: args }, options); }
}

function decodeSse(body, id) {
  const frames = body.replaceAll('\r\n', '\n').split('\n\n');
  frames.pop(); // Only complete events can be trusted.
  for (const frame of frames) {
    const data = frame.split('\n').filter(line => line.startsWith('data:'))
      .map(line => line.slice(5).trimStart()).join('\n');
    if (!data) continue;
    const message = JSON.parse(data);
    if (message.id === id) return message;
  }
}

function unwrap(message) {
  if (message.error || !Object.hasOwn(message, 'result')) throw new Error('Paseo upstream RPC failure');
  return message.result;
}
