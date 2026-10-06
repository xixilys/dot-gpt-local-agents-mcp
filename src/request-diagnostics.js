import { randomUUID } from 'node:crypto';

const METHODS = new Set(['server/discover', 'tools/list', 'tools/call', 'events/list', 'events/subscribe', 'events/unsubscribe', 'ping']);
const defaultWrite = record => process.stderr.write(`${JSON.stringify(record)}\n`);

// Operational metadata only: never copy headers, RPC IDs, arguments, URLs,
// error messages/stacks or arbitrary tool names into service logs.
export function createRequestDiagnostics(req, res, { gateway, write = defaultWrite } = {}) {
  const correlationId = randomUUID();
  const started = performance.now();
  let method = 'unknown', tool;
  try {
    if (METHODS.has(req.body?.method)) method = req.body.method;
    const name = req.body?.params?.name;
    if (method === 'tools/call' && typeof name === 'string' && /^[a-z][a-z_]{0,63}$/.test(name)
      && gateway?.catalog instanceof Map && gateway.catalog.has(name)) tool = name;
  } catch { /* Diagnostics cannot reject a request. */ }
  function report(stage, error) {
    try {
      const code = Number.isSafeInteger(error?.code) && error.code >= -32768 && error.code < 0 ? error.code : undefined;
      const errorClass = error instanceof TypeError ? 'TypeError' : error instanceof RangeError ? 'RangeError'
        : error instanceof SyntaxError ? 'SyntaxError' : error instanceof Error ? 'Error' : 'NonError';
      write({ event: stage === 'tool' ? 'mcp_tool_error' : 'mcp_request_error', at: new Date().toISOString(),
        pid: process.pid, correlationId, method, ...(tool ? { tool } : {}), stage,
        elapsedMs: Math.round(performance.now() - started), ...(error === undefined ? {} : { errorClass }),
        ...(code === undefined ? {} : { code }) });
    } catch { /* A logger failure must not change the wire result or dispatch. */ }
  }
  // An incomplete HTTP response is an observation, not proof about task state.
  let finished = false, incomplete = false;
  res.once('finish', () => { finished = true; });
  const transportIncomplete = stage => {
    if (!incomplete && !finished) { incomplete = true; report(stage); }
  };
  req.once('aborted', () => transportIncomplete('request_transport'));
  res.once('close', () => transportIncomplete('response_transport'));
  res.setHeader('x-local-agents-request-id', correlationId);
  return { correlationId, report };
}
