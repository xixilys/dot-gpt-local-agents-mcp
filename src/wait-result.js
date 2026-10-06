// The upstream MCP does not return a prompt/turn ID. A unique marker in the
// actual user message correlates new dispatches without storing private prompts.
export function requestMarker(requestId) {
  return `[Local Agents requestId: ${requestId}]\n`;
}

export async function waitForAgentResult(client, args, { now = Date.now } = {}) {
  const deadline = now() + args.timeoutMs;
  const remaining = () => Math.max(1, deadline - now());
  const base = {
    requestId: args.requestId, agentId: args.agentId,
    requestMatched: false, terminalDetected: null, turnOutcome: 'unknown',
    acceptancePassed: null, notification: 'mcp_wait_response',
    automaticWakeSupported: false,
  };
  let waited;
  try {
    // Reserve time for the actual result and a fresh snapshot. Native waiting
    // is read-only; a timeout or disconnect never resends or cancels the task.
    waited = await client.waitForFinish(args.agentId, Math.max(1, remaining() - 5000));
  } catch {
    return { ...base, status: 'error', reason: 'wait_disconnected', retrySafe: true };
  }
  const entries = [];
  const pages = [];
  let page;
  let epoch;
  let match;
  try {
    for (let i = 0; i < 3 && now() < deadline; i++) {
      page = await client.fetchAgentTimeline(args.agentId, {
        projection: 'projected', limit: args.limit,
        direction: i ? 'before' : 'tail',
        ...(i ? { cursor: page.startCursor } : {}),
        timeout: Math.min(2000, remaining()),
      });
      if (page.agentId !== args.agentId || !Array.isArray(page.entries) || page.error) throw new Error('Invalid timeline');
      if (page.agent && page.agent.id !== args.agentId) throw new Error('Invalid snapshot');
      if (page.gap || page.reset || page.staleCursor || (epoch && epoch !== page.epoch)) {
        return { ...base, status: 'timeout', reason: 'timeline_discontinuity',
          waitStatus: waited.status, startCursor: page.startCursor, endCursor: page.endCursor, retrySafe: true };
      }
      epoch = page.epoch;
      pages.push({ startCursor: page.startCursor, endCursor: page.endCursor, hasOlder: page.hasOlder, hasNewer: page.hasNewer });
      entries.unshift(...page.entries);
      const candidates = entries.filter(e => e.item?.type === 'user_message'
        && e.item.text?.startsWith(requestMarker(args.requestId))
        && Date.parse(e.timestamp) >= Date.parse(args.submittedAt));
      if (candidates.length > 1) return { ...base, status: 'timeout', reason: 'ambiguous_request_marker' };
      match = candidates[0];
      // A page beginning inside the target turn may hide a preceding user
      // message in the same turn. Read back to its boundary before deciding.
      if (match && (!page.hasOlder || page.entries[0]?.turnId !== match.turnId)) break;
      if (!page.hasOlder) break;
      if (!page.startCursor || !page.entries.length
        || (i && pages[i - 1].startCursor?.seq <= page.startCursor.seq)) {
        return { ...base, status: 'timeout', reason: 'cursor_did_not_advance', retrySafe: true };
      }
    }
    const fetched = await client.fetchAgent({ agentId: args.agentId, timeout: Math.min(2000, remaining()) });
    const snapshot = fetched.agent;
    if (fetched.error || !snapshot || snapshot.id !== args.agentId) throw new Error('Invalid final snapshot');
    const timeline = {
      agentId: args.agentId, agent: snapshot, projection: 'projected', epoch,
      entries: match ? entries.filter(e => e.seqEnd >= match.seqStart) : [], pages,
      startCursor: pages.at(-1)?.startCursor ?? null, endCursor: pages[0]?.endCursor ?? null,
      hasOlder: page?.hasOlder ?? false, hasNewer: pages[0]?.hasNewer ?? false,
      gap: false, staleCursor: false, reset: false,
    };
    const result = { ...base, waitStatus: waited.status, observedAgentState: snapshot.status,
      waitTimedOut: waited.status === 'timeout', timeline, retrySafe: true };
    if (!match) return { ...result, status: waited.status === 'error' ? 'error' : waited.status === 'permission' ? 'permission' : snapshot.status === 'running' ? 'running' : 'timeout',
      reason: waited.status === 'error' ? 'agent_error_unattributed' : page?.hasOlder ? 'history_window_exceeded' : 'request_marker_not_found' };
    if (!match.turnId || (args.turnId && match.turnId !== args.turnId)) {
      return { ...result, status: 'timeout', reason: 'turn_not_matched' };
    }
    const target = entries.filter(e => e.turnId === match.turnId && e.seqEnd >= match.seqStart);
    const later = entries.some(e => e.seqStart > match.seqStart && e.turnId !== match.turnId);
    const sameTurnUsers = entries.filter(e => e.turnId === match.turnId && e.item?.type === 'user_message');
    const incomplete = pages[0]?.hasNewer || (page?.hasOlder && page.entries[0]?.turnId === match.turnId);
    const matched = { ...result, requestMatched: true, turnId: match.turnId,
      userMessageId: match.item.messageId ?? null,
      messages: target.filter(e => e.item?.type === 'assistant_message'),
    };
    if (later || sameTurnUsers.length !== 1 || incomplete) return { ...matched, status: 'timeout',
      reason: later ? 'newer_turn_present' : sameTurnUsers.length !== 1 ? 'shared_turn' : 'history_window_exceeded' };
    if (snapshot.activeTurn && snapshot.activeTurn.turnId !== match.turnId) {
      return { ...matched, status: 'running', reason: 'different_active_turn' };
    }
    if (snapshot.pendingPermissions?.length || waited.status === 'permission') {
      return { ...matched, status: 'permission', terminalDetected: false,
        pendingPermissions: snapshot.pendingPermissions ?? [] };
    }
    if (snapshot.status === 'running' || snapshot.activeTurn) {
      return { ...matched, status: 'running', terminalDetected: false };
    }
    if (waited.status === 'error' || snapshot.status === 'error') {
      return { ...matched, status: 'error', reason: 'agent_error', error: waited.error ?? snapshot.lastError ?? null };
    }
    const messages = matched.messages.filter(e => e.seqStart > match.seqStart);
    // "idle" is an observed end state, not a turn_completed event or task
    // success. Cancellation/stopping can also leave an assistant message.
    if (messages.length && snapshot.status === 'idle' && snapshot.activeTurn === null) {
      // A follow-up can finish between pagination and the final snapshot. Do
      // not confirm the old window as latest just because the agent is idle
      // again. Recheck its tail without extending this call's deadline.
      const tail = await client.fetchAgentTimeline(args.agentId, {
        projection: 'projected', limit: 1, direction: 'tail', timeout: Math.min(2000, remaining()),
      });
      if (tail.agentId !== args.agentId || tail.error || tail.gap || tail.reset || tail.staleCursor
        || tail.hasNewer || tail.epoch !== epoch
        || tail.endCursor?.epoch !== pages[0]?.endCursor?.epoch
        || tail.endCursor?.seq !== pages[0]?.endCursor?.seq
        || (tail.agent && (tail.agent.id !== args.agentId || tail.agent.status !== 'idle' || tail.agent.activeTurn !== null))) {
        return { ...matched, status: 'timeout', reason: 'tail_changed_during_read' };
      }
      return { ...matched, status: waited.status === 'idle' ? 'idle' : 'result_available',
        terminalDetected: waited.status === 'idle' ? true : null,
        terminalKind: waited.status === 'idle' ? 'native_wait_idle' : null,
        lastMessage: messages.at(-1).item.text,
      };
    }
    return { ...matched, status: 'timeout', reason: 'no_matched_end_state' };
  } catch {
    return { ...base, status: 'error', reason: 'result_read_disconnected', waitStatus: waited.status, retrySafe: true };
  }
}
