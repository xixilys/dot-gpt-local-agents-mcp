import { tmpdir } from 'node:os';
// SPDX-License-Identifier: Apache-2.0
// Independent tests for the Codex Bridge-inspired Direct command interface.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, symlink, rm, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { DirectCommands } from '../src/direct-commands.js';

const owner = 'alice';
async function fixture(t, options = {}) {
  const root = await mkdtemp(join(process.platform === 'darwin' ? '/private/tmp' : tmpdir(), 'direct-command-'));
  const project = join(root, 'project'), outside = join(root, 'outside'), stateDir = join(root, 'state');
  await mkdir(project); await mkdir(outside); await mkdir(join(project, 'child'));
  await symlink(outside, join(project, 'escape'));
  const host = { id: 'mac', transport: 'localMac', stateDir, allowedRoots: [project],
    direct: { projects: [{ id: 'p', name: 'test', path: project, read: false, write: false, commandMode: 'full' }] } };
  const commands = new DirectCommands({ host, ...options }), extras = [];
  t.after(async () => { await commands.close(); for (const c of extras) await c.close(); await rm(root, { recursive: true, force: true }); });
  return { root, project, outside, stateDir, host, commands, extras, call: (name, args, auth = owner) => commands.call(name, args, { owner: auth }) };
}
async function terminal(f, commandId, budget = 5000) {
  const deadline = Date.now() + budget;
  let receipt, cursor = 0, events = [], lostCount = 0;
  do {
    receipt = await f.call('read_direct_command', { commandId, cursor, waitMs: 50 });
    events.push(...receipt.events); lostCount += receipt.lostCount; cursor = receipt.nextCursor;
    if (!['pending', 'running'].includes(receipt.state)) return { ...receipt, events, lostCount };
  } while (Date.now() < deadline);
  assert.fail(`Command did not terminate: ${receipt.state}`);
}
const node = code => [process.execPath, '-e', code];
async function started(f, argv, requestId = 'one', extra = {}) {
  const result = await f.call('run_direct_command', { projectId: 'p', argv, requestId, ...extra });
  assert.match(result.commandId, /^[a-f0-9-]{36}$/); return result;
}

test('real argv execution streams split Unicode stdout/stderr, reads and preserves exit status', async t => {
  const f = await fixture(t);
  const result = await started(f, node(`process.stdout.write(Buffer.from([0xf0,0x9f]));setTimeout(()=>{process.stdout.write(Buffer.from([0x98,0x80]));process.stderr.write('error-text');process.exitCode=7},20)`));
  assert.equal(result.state, 'pending');
  const end = await terminal(f, result.commandId);
  assert.equal(end.state, 'exited'); assert.equal(end.exitCode, 7);
  assert.equal(end.events.filter(e => e.source === 'stdout').map(e => e.text).join(''), '😀');
  assert.equal(end.events.filter(e => e.source === 'stderr').map(e => e.text).join(''), 'error-text');
});

test('stdin inputId is at-most-once, EOF closes input and duplicate run executes only once', async t => {
  const f = await fixture(t), marker = join(f.project, 'runs');
  const argv = node(`require('fs').appendFileSync(${JSON.stringify(marker)},'x');process.stdin.on('data',c=>process.stdout.write(c));process.stdin.on('end',()=>process.stdout.write('EOF'));`);
  const result = await started(f, argv);
  const again = await started(f, argv); assert.equal(again.commandId, result.commandId); assert.equal(again.duplicate, true);
  const input = { commandId: result.commandId, inputId: 'input-one', text: '你好\n' };
  assert.equal((await f.call('write_direct_command_input', input)).state, 'accepted');
  assert.equal((await f.call('write_direct_command_input', input)).duplicate, true);
  await assert.rejects(f.call('write_direct_command_input', { ...input, text: 'changed' }), /inputId/);
  assert.equal((await f.call('write_direct_command_input', { commandId: result.commandId, inputId: 'eof', eof: true })).state, 'accepted');
  await assert.rejects(f.call('write_direct_command_input', { commandId: result.commandId, inputId: 'after', text: 'x' }), /stdin|running/);
  const end = await terminal(f, result.commandId);
  assert.equal(end.events.map(e => e.text).join(''), '你好\nEOF'); assert.equal(await readFile(marker, 'utf8'), 'x');
});

test('deadline stops owned process group, while bounded read waits leave the command running', async t => {
  const f = await fixture(t);
  const result = await started(f, node(`setInterval(()=>{},1000)`), 'deadline', { timeoutMs: 250 });
  const reading = await f.call('read_direct_command', { commandId: result.commandId, waitMs: 10 });
  assert.ok(['pending', 'running'].includes(reading.state));
  const end = await terminal(f, result.commandId);
  assert.equal(end.state, 'timed_out'); assert.equal(end.signal, 'SIGTERM');
});

async function waitFile(path, budget = 3000) {
  const deadline = Date.now() + budget;
  while (Date.now() < deadline) {
    try { return await readFile(path, 'utf8'); } catch {}
    await new Promise(r => setTimeout(r, 10));
  }
  assert.fail('Missing child pid file');
}
function running(pid) { try { process.kill(pid, 0); return true; } catch (e) { if (e.code === 'ESRCH') return false; throw e; } }
async function gone(pid) {
  for (let i = 0; i < 150; i++) { if (!running(pid)) return; await new Promise(r => setTimeout(r, 10)); }
  assert.fail(`Owned process ${pid} survived cleanup`);
}

test('explicit cancel escalates TERM-resistant children without touching unrelated processes', async t => {
  const f = await fixture(t), pidFile = join(f.project, 'child.pid');
  const unrelated = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
  t.after(() => unrelated.kill('SIGKILL'));
  const grandchild = `process.on('SIGTERM',()=>{});setInterval(()=>{},1000)`;
  const code = `const c=require('child_process').spawn(process.execPath,['-e',${JSON.stringify(grandchild)}],{stdio:'ignore'});require('fs').writeFileSync(${JSON.stringify(pidFile)},String(c.pid));process.on('SIGTERM',()=>{});setInterval(()=>{},1000)`;
  const result = await started(f, node(code), 'cancel');
  const pid = Number(await waitFile(pidFile)); await new Promise(r => setTimeout(r, 50));
  await f.call('cancel_direct_command', { commandId: result.commandId });
  const end = await terminal(f, result.commandId);
  assert.equal(end.state, 'cancelled'); assert.equal(end.signal, 'SIGKILL');
  await gone(pid); assert.equal(running(unrelated.pid), true);
  assert.equal((await f.call('cancel_direct_command', { commandId: result.commandId })).state, 'cancelled');
});

test('closing gateway cleans children and preserves stable receipts across restart', async t => {
  const f = await fixture(t), pidFile = join(f.project, 'close.pid');
  const argv = node(`require('fs').writeFileSync(${JSON.stringify(pidFile)},String(process.pid));setInterval(()=>{},1000)`);
  const result = await started(f, argv, 'close'); const pid = Number(await waitFile(pidFile));
  await f.commands.close(); await gone(pid);
  const reopened = new DirectCommands({ host: f.host }); f.extras.push(reopened);
  const read = await reopened.call('read_direct_command', { commandId: result.commandId }, { owner });
  assert.equal(read.state, 'interrupted'); assert.equal(read.outputAvailable, false);
  const duplicate = await reopened.call('run_direct_command', { projectId: 'p', argv, requestId: 'close' }, { owner });
  assert.equal(duplicate.commandId, result.commandId); assert.equal(duplicate.state, 'interrupted');
});

test('rejects owner/request/host collisions, unknown keys, authority escalation and relative escapes', async t => {
  const f = await fixture(t), argv = node(`process.stdout.write('ok')`), result = await started(f, argv);
  await terminal(f, result.commandId);
  for (const name of ['read_direct_command', 'cancel_direct_command', 'write_direct_command_input']) {
    const args = { commandId: result.commandId, ...(name === 'write_direct_command_input' ? { inputId: 'x', text: 'x' } : {}) };
    await assert.rejects(f.call(name, args, 'bob'), /owner/);
  }
  await assert.rejects(started(f, argv, 'one', { timeoutMs: 42 }), /requestId/);
  await assert.rejects(f.call('run_direct_command', { projectId: 'p', argv, requestId: 'one' }, 'bob'), /requestId/);
  for (const extra of [{ env: {} }, { executable: 'bash' }, { owner: 'bob' }, { target: 'ssh://evil' }]) await assert.rejects(started(f, argv, 'bad', extra), /arguments/);
  await assert.rejects(started(f, argv, 'abs', { cwd: '/tmp' }), /relative/);
  for (const cwd of ['../outside', 'escape']) {
    const r = await started(f, argv, `escape-${cwd.replaceAll(/[^a-z]/g, "-")}`, { cwd });
    assert.equal((await terminal(f, r.commandId)).state, 'failed');
  }
  const sub = await started(f, node('process.stdout.write(process.cwd())'), 'sub', { cwd: 'child' });
  assert.equal((await terminal(f, sub.commandId)).events.map(e => e.text).join(''), await realpath(join(f.project, 'child')));
  assert.throws(() => new DirectCommands({ host: f.host }), /already in use/);
  await f.commands.close();
  const foreign = new DirectCommands({ host: { ...f.host, id: 'wsl' } }); f.extras.push(foreign);
  await assert.rejects(foreign.call('read_direct_command', { commandId: result.commandId }, { owner }), /owner and host/);
  await assert.rejects(foreign.call('run_direct_command', { projectId: 'p', requestId: 'one', argv }, { owner }), /requestId/);
});

test('registered authorization matches whole argv and does not depend on file read/write', async t => {
  const f = await fixture(t), argv = node(`process.stdout.write('ok')`);
  f.commands.projects[0].commandMode = 'registered'; f.commands.projects[0].commands = [{ name: 'fixed', argv }];
  const r = await started(f, argv); assert.equal((await terminal(f, r.commandId)).state, 'exited');
  await assert.rejects(started(f, [...argv, 'extra'], 'extra'), /not registered/);
  await assert.rejects(started(f, node(`process.stdout.write('another')`), 'another'), /not registered/);
  f.commands.projects[0].commandMode = 'disabled'; await assert.rejects(started(f, argv, 'disabled'), /disabled/);
});

test('output cache and individual reads are bounded and overflow/Unicode loss remains visible', async t => {
  const f = await fixture(t, { maxOutputBytes: 32768 });
  const r = await started(f, node(`for(let i=0;i<400;i++) process.stdout.write('😀'.repeat(1000));process.stderr.write('tail')`));
  const end = await terminal(f, r.commandId);
  const read = await f.call('read_direct_command', { commandId: r.commandId, limit: 1000 });
  assert.ok(read.lostCount > 0); assert.equal(read.truncated, true);
  assert.ok(f.commands.outputBytes <= 32768);
  assert.ok(Buffer.byteLength(JSON.stringify(read)) <= 65536);
  assert.ok(read.events.every(e => !e.text.includes('�')));
  assert.equal(end.exitCode, 0);
});

test('lost worker response preserves original request and input receipts without resending', async t => {
  let count = 0;
  const f = await fixture(t, { spawnImpl: (...args) => { count++; return spawn(...args); } });
  const r = await started(f, node(`process.stdin.resume();setInterval(()=>{},1000)`));
  await f.commands.runtime.get(r.commandId).submission;
  const input = { commandId: r.commandId, inputId: 'lost-input', text: 'hello' };
  // Simulate a disconnect after sending to the worker and before ACK delivery.
  const original = f.commands.rpc.bind(f.commands);
  f.commands.rpc = async message => { const pending = original(message); if (message.op === 'input') f.commands.transportLost(); return pending; };
  assert.equal((await f.call('write_direct_command_input', input)).state, 'unknown');
  assert.equal((await f.call('write_direct_command_input', input)).state, 'unknown');
  assert.equal((await started(f, node(`process.stdin.resume();setInterval(()=>{},1000)`))).commandId, r.commandId);
  assert.equal(count, 1);
  assert.equal((await f.call('read_direct_command', { commandId: r.commandId })).state, 'unknown');
});

test('fixed SSH worker receives command only in stdin and verifies remote host paths', async t => {
  let executable, sshArgv, calls = 0;
  const f = await fixture(t, { spawnImpl: (name, argv, options) => {
    executable = name; sshArgv = argv; calls++;
    // Fake only SSH transport; run the actual fixed remote worker locally.
    return spawn(process.execPath, ['--input-type=module', '-e', f.commands.workerSource], options);
  } });
  f.commands.host.transport = 'sshWSL'; f.commands.host.target = 'ssh://owner@wsl:22'; f.commands.host.remoteStateDir = join(f.root, 'remote');
  const argv = node(`process.stdout.write('remote-value-unique')`);
  const r = await started(f, argv, 'remote');
  const end = await terminal(f, r.commandId);
  assert.equal(end.state, 'exited'); assert.equal(executable, 'ssh'); assert.equal(calls, 1);
  assert.equal(sshArgv.includes('owner@wsl'), true); assert.equal(sshArgv.join(' ').includes('remote-value-unique'), false);
  assert.equal(end.events.map(e => e.text).join(''), 'remote-value-unique');
  assert.equal(JSON.parse(await readFile(join(f.root, 'remote/direct/.direct-owner.json'), 'utf8')).hostId, 'mac');
});

test('restart reclassifies persisted pending/running and uncertain input, never spawns recovery', async t => {
  const f = await fixture(t); await f.commands.close();
  const db = new DatabaseSync(join(f.stateDir, 'direct.sqlite'));
  const commandId = '11111111-1111-4111-8111-111111111111';
  db.prepare(`INSERT INTO direct_commands(command_id,request_id,owner,host_id,project_id,fingerprint,state,created_at,updated_at) VALUES (?,?,?,?,?,?,'pending','then','then')`)
    .run(commandId, 'recovery', owner, 'mac', 'p', 'fingerprint');
  db.prepare("INSERT INTO direct_inputs(command_id,input_id,fingerprint,state,eof) VALUES (?,?,?,'sending',0)").run(commandId, 'sent', 'x'); db.close();
  let spawns = 0;
  const reopened = new DirectCommands({ host: f.host, spawnImpl: () => { spawns++; throw Error('must not spawn'); } }); f.extras.push(reopened);
  assert.equal((await reopened.call('read_direct_command', { commandId }, { owner })).state, 'unknown');
  assert.equal(reopened.db.prepare('SELECT state FROM direct_inputs').get().state, 'unknown');
  assert.equal(spawns, 0);
});

test('worker EOF after abrupt gateway death cleans only its children; stale lock permits receipt recovery', async t => {
  const f = await fixture(t); await f.commands.close();
  const pidFile = join(f.project, 'crash.pid');
  const argv = node(`require('fs').writeFileSync(${JSON.stringify(pidFile)},JSON.stringify({pid:process.pid,worker:process.ppid}));process.on('SIGTERM',()=>{});setInterval(()=>{},1000)`);
  const moduleUrl = new URL('../src/direct-commands.js', import.meta.url).href;
  const program = `import {DirectCommands} from ${JSON.stringify(moduleUrl)};const d=new DirectCommands({host:${JSON.stringify(f.host)}});d.call('run_direct_command',{projectId:'p',requestId:'crash',argv:${JSON.stringify(argv)}},{owner:'alice'}).then(r=>process.stdout.write(r.commandId+'\\n'));`;
  const gateway = spawn(process.execPath, ['--input-type=module', '-e', program], { stdio: ['ignore', 'pipe', 'ignore'] });
  t.after(() => gateway.kill('SIGKILL'));
  const commandId = await new Promise((resolve, reject) => { let out = ''; gateway.stdout.on('data', chunk => { out += chunk.toString(); if (out.includes('\n')) resolve(out.trim()); }); gateway.once('error', reject); });
  const pids = JSON.parse(await waitFile(pidFile));
  const dead = new Promise(resolve => gateway.once('close', resolve)); gateway.kill('SIGKILL'); await dead;
  await gone(pids.pid); await gone(pids.worker);
  const recovered = new DirectCommands({ host: f.host }); f.extras.push(recovered);
  const receipt = await recovered.call('read_direct_command', { commandId }, { owner });
  assert.ok(['interrupted', 'unknown'].includes(receipt.state));
  assert.equal((await recovered.call('run_direct_command', { projectId: 'p', requestId: 'crash', argv }, { owner })).commandId, commandId);
});

test('normal command exit cleans descendants instead of leaving a background process', async t => {
  const f = await fixture(t), pidFile = join(f.project, 'background.pid');
  const argv = node(`const c=require('child_process').spawn(process.execPath,['-e','process.on("SIGTERM",()=>{});setInterval(()=>{},1000)'],{stdio:'ignore'});require('fs').writeFileSync(${JSON.stringify(pidFile)},String(c.pid));setTimeout(()=>process.exit(0),100)`);
  const r = await started(f, argv, 'background');
  const pid = Number(await waitFile(pidFile));
  assert.equal((await terminal(f, r.commandId)).state, 'exited'); await gone(pid);
});

test('remote worker refuses symlink and unmarked private workerdirs before execution', async t => {
  for (const kind of ['symlink', 'unmarked']) {
    const sub = await fixture(t, { spawnImpl: (_name, _args, options) => spawn(process.execPath, ['--input-type=module', '-e', sub.commands.workerSource], options) });
    sub.commands.host.transport = 'sshWSL'; sub.commands.host.target = 'ssh://wsl'; sub.commands.host.remoteStateDir = join(sub.root, 'remote');
    await mkdir(sub.commands.host.remoteStateDir);
    const dir = join(sub.commands.host.remoteStateDir, 'direct');
    if (kind === 'symlink') await symlink(sub.outside, dir); else await mkdir(dir);
    const marker = join(sub.project, 'should-not-run');
    const r = await started(sub, node(`require('fs').writeFileSync(${JSON.stringify(marker)},'bad')`), kind);
    assert.equal((await terminal(sub, r.commandId)).state, 'failed');
    await assert.rejects(readFile(marker), { code: 'ENOENT' });
  }
});

test('fresh request recovers local and SSH worker only after old generation cleanup; old IDs and late events remain isolated', async t => {
  for (const transport of ['localMac', 'sshWSL']) {
    let spawns = 0, firstChild, oldPid;
    const f = await fixture(t, { spawnImpl: (executable, argv, options) => {
      spawns++;
      if (spawns === 2) {
        assert.ok(firstChild.exitCode !== null || firstChild.signalCode !== null, 'old worker must exit before reconnect');
        assert.equal(running(oldPid), false, 'old command must be cleaned before reconnect');
      }
      const child = transport === 'localMac' ? spawn(executable, argv, options)
        : spawn(process.execPath, ['--input-type=module', '-e', f.commands.workerSource], options);
      if (spawns === 1) firstChild = child;
      return child;
    } });
    if (transport === 'sshWSL') Object.assign(f.commands.host, { transport, target: 'ssh://wsl', remoteStateDir: join(f.root, 'remote') });
    const pidFile = join(f.project, 'recover.pid');
    const oldArgv = node(`require('fs').writeFileSync(${JSON.stringify(pidFile)},String(process.pid));process.on('SIGTERM',()=>{});process.stdin.resume();setInterval(()=>{},1000)`);
    const old = await started(f, oldArgv, 'old'); oldPid = Number(await waitFile(pidFile));
    await f.commands.runtime.get(old.commandId).submission;
    const generation = f.commands.generation;
    const original = f.commands.rpc.bind(f.commands);
    f.commands.rpc = (message, current) => {
      const pending = original(message, current);
      if (message.op === 'input') f.commands.transportLost(current);
      return pending;
    };
    const input = { commandId: old.commandId, inputId: 'unknown-input', text: 'never-resend' };
    assert.equal((await f.call('write_direct_command_input', input)).state, 'unknown');
    f.commands.rpc = original;
    // These are submitted while the previous worker is still terminating.
    const [fresh, parallel] = await Promise.all([
      started(f, node(`process.stdin.on('data',c=>process.stdout.write(c));process.stdin.on('end',()=>process.exit(0))`), 'fresh'),
      started(f, node(`process.stdout.write('parallel')`), 'parallel'),
    ]);
    await f.commands.runtime.get(fresh.commandId).submission;
    assert.equal(spawns, 2);
    // Delayed events from the retired transport cannot change the new worker.
    firstChild.stdin.emit('error', Error('late old transport error'));
    firstChild.stdout.emit('data', Buffer.from(`${JSON.stringify({ type: 'state', commandId: fresh.commandId, state: 'failed' })}\n`));
    f.commands.transportLost(generation);
    assert.equal((await f.call('read_direct_command', { commandId: fresh.commandId })).state, 'running');
    assert.equal((await started(f, oldArgv, 'old')).state, 'unknown');
    assert.equal((await f.call('write_direct_command_input', input)).state, 'unknown');
    assert.equal(spawns, 2);
    assert.equal((await f.call('write_direct_command_input', { commandId: fresh.commandId, inputId: 'new-input', text: 'recovered', eof: true })).state, 'accepted');
    const end = await terminal(f, fresh.commandId);
    assert.equal(end.state, 'exited'); assert.equal(end.events.map(e => e.text).join(''), 'recovered');
    assert.equal((await terminal(f, parallel.commandId)).events.map(e => e.text).join(''), 'parallel');
    assert.equal((await f.call('read_direct_command', { commandId: old.commandId })).state, 'unknown');
    await gone(oldPid);
  }
});

test('new request recovers after worker dies during initialization without retrying pending original request', async t => {
  let spawns = 0;
  const f = await fixture(t, { spawnImpl: (executable, argv, options) => {
    spawns++;
    return spawns === 1 ? spawn(process.execPath, ['-e', 'process.exit(17)'], options) : spawn(executable, argv, options);
  } });
  const argv = node(`process.stdout.write('fresh-after-init-loss')`);
  const old = await started(f, argv, 'initialization-loss');
  assert.equal((await terminal(f, old.commandId)).state, 'unknown');
  const fresh = await started(f, argv, 'fresh-after-init-loss');
  const end = await terminal(f, fresh.commandId);
  assert.equal(end.state, 'exited'); assert.equal(end.events.map(e => e.text).join(''), 'fresh-after-init-loss');
  assert.equal((await started(f, argv, 'initialization-loss')).commandId, old.commandId);
  assert.equal(spawns, 2);
});
