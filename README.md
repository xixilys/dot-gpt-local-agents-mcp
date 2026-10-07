<p align="center">
  <img src="docs/assets/banner.svg" alt="Dot GPT Local Agents MCP — one gateway, multiple Paseo hosts" width="100%">
</p>

<p align="center">
  <a href="README.zh-CN.md">简体中文</a> · English
</p>

<p align="center">
  <a href="NOTICE"><img alt="MIT and Apache-2.0 licenses" src="https://img.shields.io/badge/licenses-MIT%20%2B%20Apache--2.0-14b8a6.svg"></a>
  <a href="https://github.com/xixilys/dot-gpt-local-agents-mcp/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/xixilys/dot-gpt-local-agents-mcp/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Node.js 22.19–26" src="https://img.shields.io/badge/Node.js-22.19%E2%80%9326-2563eb.svg">
</p>

**One OAuth-protected MCP endpoint for Paseo agents and project-scoped Direct files and commands across your Mac and SSH-reachable Linux/WSL hosts.** Discover hosts, choose where work runs, and read its receipts and events from the same client connection.

This self-hosted gateway is for one trusted owner. Agents execute on their selected host with that host's Paseo installation and runtime permissions. The gateway does not provide model accounts, a hosted service, or an operating-system sandbox.

![A gateway routes authenticated MCP requests to independently managed Mac and SSH hosts.](docs/assets/host-topology.svg)

## What it does

- **One endpoint, explicit host selection.** `list_hosts` reports each configured `hostId`, name, and availability. Tools accept `hostId`; when omitted, calls keep their legacy Mac behavior. Include `hostId` for remote calls and later event or receipt reads.
- **Host-local work and history.** Projects, workspaces, agents, receipts, messages, and callbacks belong to one host. IDs are not guessed or looked up across machines.
- **Existing Paseo stays in charge.** The gateway connects to the Mac Paseo instance and configured SSH hosts. It does not inherit hosts from the Paseo desktop UI, install Paseo remotely, start its daemon, or change providers or credentials.
- **Remote messages return to the Mac gateway.** A fixed CLI on the remote host reaches that host's dedicated Mac-side channel through a reverse SSH Unix-socket forward. The Mac gateway persists messages centrally; no public message endpoint is added.

## Quick start

The example below is a shape guide for the 0.4.0 multi-host configuration. Replace every example path and host value with values you control. The Mac is implicit; list only additional remote hosts.

```sh
git clone https://github.com/xixilys/dot-gpt-local-agents-mcp.git
cd dot-gpt-local-agents-mcp
npm ci --ignore-scripts
cp config.example.json config.json
```

Configure one OAuth gateway and your hosts. Keep the existing Mac settings as top-level fields; add remote hosts in `hosts`:

```json
{
  "publicBaseUrl": "https://gateway.example.com",
  "port": 6768,
  "upstreamUrl": "http://127.0.0.1:6767/mcp/agents",
  "stateDir": "/Users/you/.local/share/local-agents-mcp",
  "ownerTokenFile": "/Users/you/.local/share/local-agents-mcp/owner.json",
  "paseoCommand": "/Applications/Paseo.app/Contents/Resources/bin/paseo",
  "allowedRoots": ["/Users/you/Projects"],
  "hosts": [
    {
      "id": "wsl",
      "name": "WSL",
      "transport": "ssh",
      "target": "ssh://linux-host",
      "allowedRoots": ["/home/you/project"],
      "remoteStateDir": "/home/you/.local-agents-mcp-bridge/gateway"
    }
  ]
}
```

Create the private owner-password file in the Mac state directory before starting the service. Keep the password and state outside source control. The gateway process and local Paseo instance must run as the same Mac user. Each remote host must already have Paseo installed and running, and the configured SSH identity must be able to reach it using your normal OpenSSH authentication setup. Remote `allowedRoots` are checked on the remote host after resolving canonical paths; this is a path policy, not an OS sandbox. Choose `remoteStateDir` and its parent with permissions that pass the bridge's private-directory checks; existing directories are not automatically made private.

On macOS, the local Paseo observer helpers use the app's Electron runtime and bundled client. The original supported setup is Paseo **0.10.3** at `/Applications/Paseo.app`; other versions or installation paths need verification. The gateway requires Node.js `>=22.19 <27`, npm, and `better-sqlite3` **13.0.3**, which ships Node-API prebuilt binaries. `npm ci --ignore-scripts` keeps dependency lifecycle scripts disabled; supported prebuilt platforms need no manual native rebuild. Unsupported platforms or an intentional source build may require a C/C++ toolchain, Python, and official Node headers.

```sh
npm run check
npm test
npm start
```

Run one gateway for the configured port and state. Put an HTTPS reverse proxy in front of the gateway at `127.0.0.1:6768`, preserve authorization and MCP headers, and connect a compatible client to `https://<your-origin>/mcp`. TLS, proxy limits, service management, and client registration are operator responsibilities. Keep Paseo's loopback endpoint private. The repository does not provision a tunnel or change firewall rules.

OAuth clients are dynamically registered. Authorization uses the `local-agents` scope, S256 PKCE, and the exact MCP resource URL (`https://<your-origin>/mcp`). Access tokens last one hour and refresh tokens 30 days. Redirects allow `chatgpt.com`, `chat.openai.com`, and loopback hosts (`localhost`, `127.0.0.1`, `::1`); other redirect hosts require a deliberate source change. Apply proxy rate limits to registration and consent endpoints before exposing the service.

<details>
<summary>Owner password file example</summary>

This creates a private file and refuses to overwrite an existing one:

```sh
node --input-type=module <<'NODE'
import { mkdir, chmod, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
const dir = join(homedir(), '.local', 'share', 'local-agents-mcp');
await mkdir(dir, { recursive: true, mode: 0o700 });
await chmod(dir, 0o700);
await writeFile(join(dir, 'owner.json'), JSON.stringify({
  ownerToken: randomBytes(32).toString('base64url')
}) + '\n', { mode: 0o600, flag: 'wx' });
NODE
```

</details>

## Choose a host and follow a task

Start with `list_hosts`. Use a returned `hostId` when selecting a remote host, and keep it with the host's `workspaceId`, `agentId`, `requestId`, and `routeId`. Mac calls may omit `hostId` for compatibility. Remote result reads, waits, message reads, and follow-ups should carry the same explicit `hostId` so the gateway never has to infer which machine owns an identifier.

The usual flow is to discover projects and provider capabilities on the chosen host, select or create a workspace there, then dispatch with a stable `requestId`. A receipt confirms submission, not task completion. If a result is `unknown` or still `submitting`, inspect the original request and that host's timeline; do not resend it under a new ID until you know whether it ran. Review the actual artifact before accepting the work.

`wait_for_agent_result` uses the original `requestId` and exact `agentId`, with a total budget of 1–20 seconds. `idle`, a terminal observation, readable text, and task success are separate; `acceptancePassed` remains `null`. Timeouts can be reread and do not cancel or redispatch work. Timeline pages are bounded to 1 MiB; follow returned cursors and check gap/stale/reset indicators.

Workspace roots only scope which project paths the gateway will target. They do not restrict what an agent process can execute. Authorized clients can still read and control agents in those roots, including cancellation, runtime settings, and specific permission responses. Grant OAuth access only to clients you trust with that authority.

## Direct Workspace

Direct Workspace adds project-scoped file and command tools to this existing Node.js gateway, alongside the tools backed by each host's Paseo daemon. Its design is adapted from Codex Bridge Direct; this is not a Swift runtime integration or a claim of complete protocol compatibility. It uses the same `hostId` routing as the other tools, with omitted `hostId` continuing to mean Mac.

The ten tools are `list_direct_projects`; `list_direct_files`, `search_direct_files`, `read_direct_file`, `write_direct_file`, and `edit_direct_file`; plus `run_direct_command`, `read_direct_command`, `write_direct_command_input`, and `cancel_direct_command`. Direct projects are configured locally per host. For Mac, place `direct.projects` at the top level; for an SSH host, put `direct.projects` inside that host's entry:

```json
{
  "direct": {
    "projects": [
      {
        "id": "example",
        "name": "Example project",
        "path": "/Users/you/Projects/example",
        "read": true,
        "write": false,
        "commandMode": "disabled"
      }
    ]
  }
}
```

Project defaults are read enabled, write disabled, and commands disabled. Enable writes per project when useful. File writes require `expectedSha256`: use `null` only to create a new file; updates must provide the current full-file hash, and a mismatch is rejected so you can reread before editing. `edit_direct_file` also checks the file hash before replacing the requested text.

Commands can stay disabled, use `commandMode: "registered"` with owner-configured exact `argv` entries (for example, `{ "name": "test", "argv": ["npm", "test"] }`), or use `commandMode: "full"` for literal argument arrays. Commands run with the selected host's normal OS permissions. The project working directory and allowed roots scope the project files and working directory; they do not sandbox a command or limit it to that directory.

Command requests are durable and keyed by `requestId`: rereading a request returns its recorded state and does not run it again. Interactive stdin writes use a deduplicated `inputId`; output reads are bounded and paginated by cursor. If the gateway restarts while a process is active, its record may become `interrupted` or `unknown`; the gateway does not promise to resume that process. Direct Workspace availability is independent of Paseo daemon health.

## Events and agent messages

The legacy tools transport and the `2026-07-28` MCP Events adapter share `/mcp`. For an Events-capable client, subscribe to `agent.attention` with explicit `hostId`, `workspaceId`, and a stable `routeId` unique to that chat, plus its HTTPS callback and signing secret. Confirm the route with `list_notification_routes`, then bind it to dispatches. Existing requests can be watched with their original request ID and an explicit route; this does not create an agent, send a prompt, or rebind another route. A subscription, route binding, `notifyOnFinish`, or successful callback delivery report does not prove that a client woke up or that its task succeeded. Without Events support, read or wait for results directly.

For remote tasks, the fixed message CLI runs on the remote host and reaches that host's dedicated channel on the Mac gateway through a reverse SSH Unix-socket forward. The Mac gateway centrally persists these messages; this does not create a public inbound message service. Keep each host's receipts, routes, and callback state distinct. When a host is unavailable, its data cannot be recovered by silently falling back to Mac; an uncertain request must be inspected on its original host rather than resubmitted elsewhere.

Callbacks require HTTPS, verified challenges, public DNS/IP destinations, and Standard Webhooks signatures. Delivery checks resolved addresses, rejects redirects and private/local destinations, and uses a durable outbox with up to five attempts for transient errors; delivery stops on HTTP 410. Subscriptions last one day by default (`ttlMs: null` selects seven days); refresh them before expiry. Cursor replay is not implemented (`cursor: null`); recover exact results through read tools. Unsubscribe when monitoring ends, preserving unrelated routes.

## Security and data

Read [SECURITY.md](SECURITY.md) before exposing a gateway. This is a single-owner authority domain, not a multi-tenant service. Protect the owner password, OAuth state, callback URLs/secrets, receipts, agent capabilities, messages, and task results. State is private local data, not encrypted storage; protect backups and access by other processes running as the same OS user.

The Mac and remote `allowedRoots` checks resolve canonical paths and reject out-of-scope targets and symlink escapes. They are gateway checks, not OS sandboxes. SSH access follows the operator's existing OpenSSH authentication; do not expose arbitrary URLs or SSH commands through public MCP tools. The gateway does not silently install or start remote services, configure providers, or transfer credentials. OAuth clients are dynamically registered; `local-agents` is the required scope, not a fixed client ID. The MCP resource URL is the configured public origin plus `/mcp`.

The public source snapshot excludes production configuration, runtime databases, captured sessions, private callback URLs, and operational history. Never use those items as public examples.

## Development and attribution

```sh
npm ci --ignore-scripts
npm run check
npm test
```

The scoped override pins `@waishnav/devspace`'s SQLite dependency to `better-sqlite3` 13.0.3 and its Node-API prebuilds, avoiding an unnecessary source rebuild on supported platforms; see the [official 13.0.0 release notes](https://github.com/WiseLibs/better-sqlite3/releases/tag/v13.0.0). CI runs syntax checks and the repository's behavior/security tests on Node.js 22.19 and 24. A green host availability result confirms that a configured Paseo daemon responds; it does not certify every task, message, or event flow. Verify the client and workflows you rely on in your deployment environment.

Original project code and documentation, except the Apache-marked Direct modules and tests, are licensed under [MIT](LICENSE), Copyright (c) 2026 xixilys. The modified Node modules in `src/direct-*` and Direct tests with Apache SPDX headers are adapted from Codex Bridge Direct at [revision `f881877`](https://github.com/Fanch-hui/codex-bridge/tree/f881877183bb1f5d355124caf6b86cb7f2323e53) and are licensed under Apache-2.0; see [NOTICE](NOTICE) and [third_party/codex-bridge-LICENSE](third_party/codex-bridge-LICENSE). The Paseo schema fixture is also Apache-2.0; its license is at [third_party/paseo-LICENSE](third_party/paseo-LICENSE). Dependencies and Paseo binaries are not bundled and retain their own licenses and terms.
