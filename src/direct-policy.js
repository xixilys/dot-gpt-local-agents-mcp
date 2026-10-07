// SPDX-License-Identifier: Apache-2.0
// Direct interfaces draw on Codex Bridge's project-scoped Direct Workspace
// design. This Node implementation is modified for this gateway's host model.
// See NOTICE and third_party/codex-bridge-LICENSE.
import { posix } from 'node:path';

const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
export function validateArgv(argv) {
  if (!Array.isArray(argv) || !argv.length || argv.length > 64
    || argv.some(arg => typeof arg !== 'string' || arg.includes('\0'))
    || !argv[0] || argv[0].startsWith('-') || Buffer.byteLength(JSON.stringify(argv)) > 16384) {
    throw new Error('argv must be a bounded, nonempty array of literal command arguments');
  }
  return [...argv];
}

export function directProjects(host) {
  const direct = host.direct;
  if (direct === undefined) return [];
  if (!direct || typeof direct !== 'object' || Array.isArray(direct)
    || Object.keys(direct).some(key => !['projects'].includes(key)) || !Array.isArray(direct.projects)
    || direct.projects.length > 64) throw new Error('Direct configuration requires a bounded projects array');
  const result = [];
  for (const value of direct.projects) {
    if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).some(key => !['id', 'name', 'path', 'read', 'write', 'commandMode', 'commands'].includes(key))) {
      throw new Error('Invalid Direct project configuration');
    }
    if (typeof value.id !== 'string' || !ID.test(value.id) || result.some(project => project.id === value.id)
      || typeof value.path !== 'string' || !posix.isAbsolute(value.path) || value.path.includes('\0')
      || typeof (value.name ?? value.id) !== 'string' || (value.name ?? value.id).length > 100) {
      throw new Error('Direct projects need unique IDs and explicit absolute host-local paths');
    }
    for (const key of ['read', 'write']) if (value[key] !== undefined && typeof value[key] !== 'boolean') throw new Error('Invalid Direct file permission');
    const commandMode = value.commandMode ?? 'disabled';
    if (!['disabled', 'registered', 'full'].includes(commandMode)) throw new Error('Invalid Direct commandMode');
    const commands = value.commands ?? [];
    if (!Array.isArray(commands) || commands.length > 64) throw new Error('Direct commands must be a bounded array');
    const registered = commands.map(command => {
      if (!command || typeof command !== 'object' || Array.isArray(command)
        || Object.keys(command).some(key => !['name', 'argv'].includes(key))
        || typeof command.name !== 'string' || !ID.test(command.name)) throw new Error('Invalid registered Direct command');
      return { name: command.name, argv: validateArgv(command.argv) };
    });
    result.push({ id: value.id, name: value.name ?? value.id, path: value.path,
      read: value.read !== false, write: value.write === true, commandMode, commands: registered });
  }
  return result;
}

export function projectFor(projects, projectId, operation) {
  const project = projects.find(project => project.id === projectId);
  if (!project) throw new Error('Unknown Direct projectId; use list_direct_projects for the selected host');
  if (operation === 'read' && !project.read || operation === 'write' && !project.write
    || operation === 'command' && project.commandMode === 'disabled') throw new Error('This operation is disabled for the Direct project');
  return project;
}

export function commandAllowed(project, argv) {
  const normalized = validateArgv(argv);
  if (project.commandMode === 'full') return normalized;
  if (project.commandMode !== 'registered' || !project.commands.some(command =>
    command.argv.length === normalized.length && command.argv.every((arg, index) => arg === normalized[index]))) {
    throw new Error('Command is not registered with these exact arguments. The owner must configure it locally; the MCP client cannot grant itself permission');
  }
  return normalized;
}

export function assertOwner(owner) {
  if (typeof owner !== 'string' || !owner.length) throw new Error('Authenticated Direct owner is required');
}
