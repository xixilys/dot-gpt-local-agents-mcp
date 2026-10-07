import { mkdir, readFile, writeFile, chmod } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { directProjects } from './direct-policy.js';
import { validateSshTarget } from './ssh-bridge.mjs';

export const DEFAULT_HOST_ID = 'mac';
export const HOST_ID_PATTERN = '^[a-z][a-z0-9_-]{0,31}$';
const hostId = new RegExp(HOST_ID_PATTERN);

// The legacy, implicit Mac is deliberately not a configurable default. Adding
// a remote host must never redirect an old call, receipt or subscription.
export function configuredHosts(config) {
  const hosts = [{ id: DEFAULT_HOST_ID, name: 'Mac', transport: 'local',
    target: '127.0.0.1:6767', allowedRoots: config.allowedRoots, stateDir: config.stateDir, direct: config.direct }];
  if (config.hosts !== undefined && !Array.isArray(config.hosts)) throw new Error('hosts must be an array');
  for (const entry of config.hosts ?? []) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)
      || Object.keys(entry).some(key => !['id', 'name', 'transport', 'target', 'allowedRoots', 'remoteStateDir', 'nodeCommand', 'direct'].includes(key))) {
      throw new Error('Invalid configured host');
    }
    if (typeof entry.id !== 'string' || !hostId.test(entry.id) || hosts.some(host => host.id === entry.id)) {
      throw new Error('Host IDs must be unique lowercase identifiers; mac is reserved');
    }
    if (entry.transport !== 'ssh') throw new Error('Remote hosts require the SSH transport');
    validateSshTarget(entry.target);
    if (typeof entry.name !== 'string' || !entry.name.trim() || entry.name.length > 80) throw new Error('A short host name is required');
    if (!Array.isArray(entry.allowedRoots) || !entry.allowedRoots.length
      || entry.allowedRoots.some(root => typeof root !== 'string' || !root.startsWith('/') || root.includes('\0'))) {
      throw new Error('Remote allowedRoots must contain absolute POSIX directory paths');
    }
    if (typeof entry.remoteStateDir !== 'string' || !entry.remoteStateDir.startsWith('/') || entry.remoteStateDir.includes('\0')) {
      throw new Error('remoteStateDir must be an absolute POSIX directory path');
    }
    hosts.push({ ...entry, target: validateSshTarget(entry.target).uri, stateDir: join(config.stateDir, 'hosts', entry.id) });
  }
  for (const host of hosts) directProjects(host);
  return hosts;
}

export async function pinHost(host, serverId) {
  if (host.id === DEFAULT_HOST_ID) return;
  if (!isAbsolute(host.stateDir)) throw new Error('Host stateDir must be absolute');
  await mkdir(host.stateDir, { recursive: true, mode: 0o700 });
  await chmod(host.stateDir, 0o700);
  const filename = join(host.stateDir, 'host-binding.json');
  let existing;
  try { existing = JSON.parse(await readFile(filename, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw new Error('Cannot read the persisted host binding'); }
  if (existing && (existing.id !== host.id || existing.transport !== host.transport || existing.target !== host.target)) {
    throw new Error('This hostId is already bound to another daemon target; use a new hostId and preserve its old state');
  }
  if (existing?.serverId && serverId && existing.serverId !== serverId) {
    throw new Error('Remote daemon identity changed; old requests will not be sent to the replacement daemon');
  }
  if (!existing || serverId && !existing.serverId) {
    const binding = { id: host.id, transport: host.transport, target: host.target, ...(serverId ? { serverId } : {}) };
    // Only a verified identity may extend this gateway-owned binding. No receipt
    // or event database is copied, migrated or rewritten.
    await writeFile(filename, JSON.stringify(binding) + '\n', { mode: 0o600, flag: existing ? 'w' : 'wx' });
    await chmod(filename, 0o600);
  }
}
