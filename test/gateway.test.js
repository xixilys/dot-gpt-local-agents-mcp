import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, symlink, rm, readFile, realpath, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Gateway, PathPolicy } from '../src/gateway.js';
import { DispatchStore, dispatchFingerprint } from '../src/dispatch-store.js';

const tools = JSON.parse(await readFile(new URL('./fixtures/paseo-tools.json', import.meta.url), 'utf8'));
const id = '11111111-1111-4111-8111-111111111111';

async function setup(t, mutation) {
  const dir = await mkdtemp(join(tmpdir(), 'local-agents-test-'));
  const workspace = join(dir, 'workspace');
  await mkdir(workspace);
  let store = new DispatchStore(join(dir, 'state'));
  t.after(async () => { store.close(); await rm(dir, { recursive: true, force: true }); });
  const upstream = {
    async tools() { return tools; },
    async call(name, args) {
      if (name === 'list_workspaces') return { structuredContent: { workspaces: [{ workspaceId: 'wks_test', cwd: workspace }] } };
      if (name === 'get_agent_status') return { structuredContent: { snapshot: { id: args.agentId, cwd: workspace } } };
      return mutation(name, args);
    },
  };
  const config = { allowedRoots: [workspace] };
  const gateway = new Gateway({ upstream, store, config });
  await gateway.refreshTools();
  return { gateway, dir, workspace, store, upstream, config,
    reopen() { store.close(); store = new DispatchStore(join(dir, 'state')); return store; } };
}
const createArgs = { title: 'Acceptance', provider: 'codex/test-model', initialPrompt: 'private prompt', workspaceId: 'wks_test', requestId: 'one-create' };

test('simultaneous creates claim once, preserve labels, and recover exact receipt without resubmitting', async t => {
  let calls = 0;
  let release;
  const blocked = new Promise(resolve => { release = resolve; });
  const fixture = await setup(t, async (name, args) => {
    assert.equal(name, 'create_agent');
    calls++;
    assert.deepEqual(args.labels, { custom: 'keep', dotRequestId: 'one-create' });
    assert.equal(args.background, true);
    assert.equal(args.notifyOnFinish, false);
    assert.equal(args.requestId, undefined);
    await blocked;
    return { content: [{ type: 'text', text: 'created' }], structuredContent: { agentId: id } };
  });
  const args = { ...createArgs, labels: { custom: 'keep' } };
  const a = fixture.gateway.call('create_agent', args);
  const b = fixture.gateway.call('create_agent', args);
  // Both workspace lookups run concurrently, but only one dispatch can claim.
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(calls, 1);
  release();
  const results = await Promise.all([a, b]);
  assert.ok(results.some(r => r.structuredContent.state === 'submitted'));
  const replay = await fixture.gateway.call('create_agent', structuredClone(args));
  assert.equal(replay.structuredContent.agentId, id);
  assert.equal(replay.structuredContent.duplicate, true);
  assert.equal(calls, 1);
  const stored = await readFile(join(fixture.dir, 'state', 'dispatch.sqlite'));
  assert.equal(stored.includes(Buffer.from('private prompt')), false);
  await assert.rejects(fixture.gateway.call('create_agent', { ...args, initialPrompt: 'different' }), /different parameters/);
});

test('lost response is unknown across restart and cannot launch another prompt', async t => {
  let calls = 0;
  const fixture = await setup(t, async () => { calls++; throw new Error('transport timeout after acceptance'); });
  const args = { agentId: id, prompt: 'unique secret prompt', requestId: 'send-unknown' };
  const first = await fixture.gateway.call('send_agent_prompt', args);
  assert.equal(first.structuredContent.state, 'unknown');
  const store = fixture.reopen();
  const gateway = new Gateway({ store, upstream: fixture.upstream, config: fixture.config });
  await gateway.refreshTools();
  const replay = await gateway.call('send_agent_prompt', args);
  assert.equal(replay.structuredContent.state, 'unknown');
  assert.equal(replay.structuredContent.agentId, id);
  assert.equal(calls, 1);
  const lookup = await gateway.call('get_dispatch_request', { requestId: args.requestId });
  assert.equal(lookup.structuredContent.fingerprint, undefined);
  assert.equal(lookup.structuredContent.completionConfirmed, false);
});

test('interrupted durable claim becomes unknown, not retryable', async t => {
  const fixture = await setup(t, async () => { throw new Error('Must not call upstream'); });
  const payload = { agentId: id, prompt: 'before crash' };
  fixture.store.begin('crash', dispatchFingerprint('send_agent_prompt', payload), 'send_agent_prompt', id);
  const store = fixture.reopen();
  assert.equal(store.get('crash').state, 'unknown');
  const gateway = new Gateway({ store, upstream: fixture.upstream, config: fixture.config });
  await gateway.refreshTools();
  assert.equal((await gateway.call('send_agent_prompt', { ...payload, requestId: 'crash' })).structuredContent.state, 'unknown');
});

test('inputs cannot override endpoint/blocking or forge dispatch labels', async t => {
  const fixture = await setup(t, async () => { throw new Error('Unexpected dispatch'); });
  for (const patch of [{ background: false }, { notifyOnFinish: true }, { host: 'remote:1' }, { credentials: 'x' }, { requestId: '../bad' }]) {
    await assert.rejects(fixture.gateway.call('create_agent', { ...createArgs, ...patch }), /Invalid tool input/);
  }
  await assert.rejects(fixture.gateway.call('create_agent', { ...createArgs, workspaceId: undefined }), /Invalid tool input/);
  await assert.rejects(fixture.gateway.call('create_agent', { ...createArgs, labels: { dotRequestId: 'claimed' } }), /reserved/);
  await assert.rejects(fixture.gateway.call('archive_agent', { agentId: id }), /not exposed/);
  const catalog = await fixture.gateway.refreshTools();
  assert.deepEqual(catalog.find(t => t.name === 'get_agent_result').annotations,
    { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false });
  assert.deepEqual(catalog.find(t => t.name === 'create_agent').annotations,
    { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: true });
  for (const name of ['cancel_agent', 'update_agent', 'respond_to_permission']) {
    assert.equal(catalog.find(t => t.name === name).annotations.readOnlyHint, false);
  }
});

test('directory checks reject sibling prefixes and symlink escapes before mutation', async t => {
  const fixture = await setup(t, async () => { throw new Error('Unexpected dispatch'); });
  const outside = join(fixture.dir, 'workspace-escape');
  await mkdir(outside);
  await symlink(outside, join(fixture.workspace, 'escape'));
  const policy = new PathPolicy([fixture.workspace]);
  assert.equal(await policy.check(fixture.workspace), await realpath(fixture.workspace));
  await assert.rejects(policy.check(outside), /outside/);
  await assert.rejects(policy.check(join(fixture.workspace, 'escape')), /outside/);
  await assert.rejects(fixture.gateway.call('create_workspace', { isolation: 'local', path: outside }), /outside/);
  await assert.rejects(fixture.gateway.call('inspect_provider', { provider: 'codex', cwd: outside }), /outside/);
});

test('unavailable roots do not hide reachable workspaces or weaken path checks', async t => {
  const fixture = await setup(t, async () => { throw new Error('Unexpected dispatch'); });
  const missingRoot = join(fixture.dir, 'unavailable-mount');
  const rootFile = join(fixture.dir, 'not-a-directory');
  await writeFile(rootFile, 'file');
  const outside = join(fixture.dir, 'outside');
  await mkdir(outside);
  await symlink(outside, join(fixture.workspace, 'escape'));
  const roots = [missingRoot, join(rootFile, 'invalid-root'), fixture.workspace];
  const policy = new PathPolicy(roots);
  assert.equal(await policy.check(fixture.workspace), await realpath(fixture.workspace));
  await assert.rejects(policy.check(outside), /outside/);
  await assert.rejects(policy.check(join(fixture.workspace, 'escape')), /outside/);
  await assert.rejects(policy.check(join(fixture.workspace, 'missing-target')), { code: 'ENOENT' });
  fixture.gateway.paths = policy;
  const listed = await fixture.gateway.call('list_workspaces', {});
  assert.equal(listed.structuredContent.workspaces.length, 1);
  assert.equal(listed.structuredContent.workspaces[0].workspaceId, 'wks_test');
  assert.equal((await fixture.gateway.call('get_agent_status', { agentId: id })).structuredContent.snapshot.id, id);
});

test('all unavailable roots fail closed even for a valid target', async t => {
  const fixture = await setup(t, async () => { throw new Error('Unexpected dispatch'); });
  const policy = new PathPolicy([join(fixture.dir, 'missing-one'), join(fixture.dir, 'missing-two')]);
  await assert.rejects(policy.check(fixture.workspace), /outside allowedRoots/);
  fixture.gateway.paths = policy;
  await assert.rejects(fixture.gateway.call('get_agent_status', { agentId: id }), /outside allowedRoots/);
  assert.deepEqual((await fixture.gateway.call('list_workspaces', {})).structuredContent.workspaces, []);
});

test('out-of-root existing agents cannot be read or modified and listings exclude them', async t => {
  let mutated = false;
  const fixture = await setup(t, async () => { mutated = true; });
  const outside = join(fixture.dir, 'outside');
  await mkdir(outside);
  fixture.upstream.call = async (name, args) => {
    if (name === 'get_agent_status') return { structuredContent: { snapshot: { id: args.agentId, cwd: outside } } };
    if (name === 'list_agents') return { content: [{ type: 'text', text: 'outside agent must not leak' }], structuredContent: { agents: [{ id, cwd: outside }, { id: 'allowed', cwd: fixture.workspace }] } };
    if (name === 'list_workspaces') return { structuredContent: { workspaces: [{ workspaceId: 'outside', cwd: outside }] } };
    if (name === 'list_pending_permissions') return { structuredContent: { permissions: [{ agentId: id, requestId: 'permission' }] } };
    mutated = true;
  };
  for (const [name, args] of [
    ['get_agent_status', { agentId: id }], ['get_agent_activity', { agentId: id }],
    ['get_agent_result', { agentId: id }], ['cancel_agent', { agentId: id }],
    ['update_agent', { agentId: id, name: 'rename' }],
    ['send_agent_prompt', { agentId: id, requestId: 'outside-send', prompt: 'do work' }],
    ['respond_to_permission', { agentId: id, requestId: 'permission', response: { behavior: 'allow' } }],
  ]) await assert.rejects(fixture.gateway.call(name, args), /outside/);
  assert.equal(mutated, false);
  assert.equal(fixture.store.get('outside-send'), undefined);
  const listing = await fixture.gateway.call('list_agents', {});
  assert.equal(listing.structuredContent.agents.length, 1);
  assert.equal(listing.content[0].text.includes('outside agent'), false);
  assert.deepEqual((await fixture.gateway.call('list_workspaces', {})).structuredContent.workspaces, []);
  assert.deepEqual((await fixture.gateway.call('list_pending_permissions', {})).structuredContent.permissions, []);
});

test('timeline reads use fixed local reader, preserve full tool output/cursors, and reject argument injection', async t => {
  const fixture = await setup(t, async () => {});
  let observed;
  const response = { agentId: id, projection: 'projected', entries: [{ timestamp: '2026-10-05T06:22:09Z', item: { type: 'tool_call', detail: { output: 'complete output' } } }], hasOlder: true, hasNewer: false, startCursor: { epoch: 'e', seq: 2 }, endCursor: { epoch: 'e', seq: 3 }, error: null };
  fixture.gateway.runCli = async (...args) => { observed = args; return { stdout: JSON.stringify(response) }; };
  const result = (await fixture.gateway.call('get_agent_result', { agentId: id, direction: 'before', cursor: { epoch: 'e', seq: 4 }, limit: 2 })).structuredContent;
  assert.equal(result.entries[0].item.detail.output, 'complete output');
  assert.deepEqual(result.startCursor, { epoch: 'e', seq: 2 });
  assert.equal(result.gatewayTruncated, false);
  assert.equal(result.completionConfirmed, false);
  assert.equal(observed[0], '/Applications/Paseo.app/Contents/Frameworks/Paseo Helper.app/Contents/MacOS/Paseo Helper');
  assert.equal(JSON.parse(observed[1][1]).direction, 'before');
  assert.equal(observed[2].env.PASEO_HOST, undefined);
  for (const patch of [{ agentId: '--host=attacker' }, { limit: 0 }, { host: 'attacker' }, { cursor: { epoch: 'e', seq: -1 } }]) {
    await assert.rejects(fixture.gateway.call('get_agent_result', { agentId: id, ...patch }), /Invalid tool input/);
  }
  await assert.rejects(fixture.gateway.call('get_agent_result', { agentId: id, direction: 'before' }), /require a cursor/);
});

test('list_projects includes allowed projects without a workspace and filters real path escapes', async t => {
  const f = await setup(t, async () => {});
  const outside = join(f.dir, 'outside-project');
  await mkdir(outside); await symlink(outside, join(f.workspace, 'project-escape'));
  f.gateway.runCli = async (exe, argv, options) => {
    assert.equal(exe, '/Applications/Paseo.app/Contents/Frameworks/Paseo Helper.app/Contents/MacOS/Paseo Helper');
    assert.deepEqual(JSON.parse(argv[1]), { action: 'list_projects' });
    assert.ok(options.timeout < 30000);
    return { stdout: JSON.stringify({ projects: [
      { projectId: 'prj_allowed', name: 'sample-project', path: f.workspace, kind: 'git' },
      { projectId: 'prj_outside', path: outside }, { projectId: 'prj_symlink', path: join(f.workspace, 'project-escape') },
    ] }) };
  };
  const result = (await f.gateway.call('list_projects', {})).structuredContent;
  assert.deepEqual(result.projects.map(p => p.projectId), ['prj_allowed']);
  assert.equal(result.filteringApplied, true);
  const catalog = await f.gateway.refreshTools();
  for (const name of ['list_projects', 'wait_for_agent_result']) assert.equal(catalog.find(x => x.name === name).annotations.readOnlyHint, true);
});

test('new dispatch markers are unique and receipt replay preserves zero resends', async t => {
  const prompts = [];
  const f = await setup(t, async (name, args) => { prompts.push(args.prompt); return { structuredContent: { success: true } }; });
  for (const requestId of ['send-one', 'send-two']) {
    const args = { requestId, agentId: id, prompt: 'same prompt' };
    await f.gateway.call('send_agent_prompt', args); await f.gateway.call('send_agent_prompt', args);
  }
  assert.deepEqual(prompts, ['[Local Agents requestId: send-one]\nsame prompt', '[Local Agents requestId: send-two]\nsame prompt']);
});

test('wait verifies persisted receipt and agent, forwards a bounded read, and never mutates upstream', async t => {
  let writes = 0;
  const f = await setup(t, async () => { writes++; return { structuredContent: { agentId: id } }; });
  await f.gateway.call('create_agent', createArgs);
  f.gateway.runCli = async (exe, argv, options) => {
    const input = JSON.parse(argv[1]);
    assert.equal(input.action, 'wait'); assert.equal(input.requestId, createArgs.requestId); assert.equal(input.agentId, id);
    assert.ok(input.timeoutMs <= 2000); assert.ok(options.timeout <= 2000);
    return { stdout: JSON.stringify({ requestId: input.requestId, agentId: id, status: 'idle', requestMatched: true,
      terminalDetected: true, acceptancePassed: null, timeline: { agent: { id, cwd: f.workspace }, entries: [] } }) };
  };
  const args = { requestId: createArgs.requestId, agentId: id, timeoutMs: 2000 };
  assert.equal((await f.gateway.call('wait_for_agent_result', args)).structuredContent.status, 'idle');
  await assert.rejects(f.gateway.call('wait_for_agent_result', { ...args, agentId: '22222222-2222-4222-8222-222222222222' }), /different agent/);
  await assert.rejects(f.gateway.call('wait_for_agent_result', { ...args, requestId: 'missing' }), /Unknown dispatch/);
  await assert.rejects(f.gateway.call('wait_for_agent_result', { ...args, timeoutMs: 30000 }), /Invalid tool input/);
  f.gateway.runCli = async () => { throw Error('disconnect'); };
  assert.equal((await f.gateway.call('wait_for_agent_result', args)).structuredContent.retrySafe, true);
  assert.equal(writes, 1);
});

test('total wait budget includes a blocked identity check and does not start the reader afterward', async t => {
  const f = await setup(t, async () => ({ structuredContent: { agentId: id } }));
  await f.gateway.call('create_agent', createArgs);
  let release;
  let read = false;
  f.upstream.call = async () => new Promise(resolve => { release = () => resolve({ structuredContent: { snapshot: { id, cwd: f.workspace } } }); });
  f.gateway.runCli = async () => { read = true; throw Error('must not run'); };
  const started = Date.now();
  const r = await f.gateway.call('wait_for_agent_result', { requestId: createArgs.requestId, agentId: id, timeoutMs: 1000 });
  assert.equal(r.structuredContent.status, 'timeout'); assert.ok(Date.now() - started < 1600);
  release(); await new Promise(resolve => setTimeout(resolve, 20)); assert.equal(read, false);
});
