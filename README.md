# Dot GPT Local Agents MCP

Let **dot and GPT** dispatch and manage **local Paseo agents** through an OAuth-protected MCP gateway on your Mac. Compatible clients can discover workspaces, dispatch Codex, Claude Code or OpenCode tasks, recover exact request results, and subscribe to agent messages and attention events.

This is a self-hosted, single-user tool. It reuses your installed Paseo and its provider authentication. It does not provide model accounts, a hosted gateway, or an OS sandbox.

## Requirements and compatibility

- macOS with Paseo **0.10.3** installed at `/Applications/Paseo.app`. The reader/observer helpers use that app's Electron runtime and bundled client. Other versions or installation paths have not been verified; Linux/WSL and Windows are not supported gateway hosts.
- Paseo's existing daemon at `127.0.0.1:6767`, exposing `/mcp/agents`. This endpoint is deliberately fixed. Do not expose it to the internet.
- Node.js `>=22.19 <27` and npm. Gateway records use Node's SQLite API; the existing OAuth store also requires the `better-sqlite3` native module. There is no TypeScript compilation or application build step.
- Providers already configured in Paseo. Codex, Claude Code and OpenCode are supported through Paseo; discover actual provider/model/mode capabilities rather than assuming an account or model is available.
- An HTTPS origin that you control, with a reverse proxy to `127.0.0.1:6768`. TLS, tunnel provisioning, service management and client/plugin registration are operator responsibilities.

The legacy tools transport and the `2026-07-28` MCP Events adapter share `/mcp`. Clients without Events support must explicitly read or wait for results. A client integration is required to subscribe and handle callbacks; this repository does not install or publish a ChatGPT App or plugin.

## Install

```sh
git clone https://github.com/xixilys/dot-gpt-local-agents-mcp.git
cd dot-gpt-local-agents-mcp
npm ci --ignore-scripts
npm_config_build_from_source=true npm rebuild better-sqlite3
cp config.example.json config.json
```

Edit `config.json` before starting. All filesystem entries must be absolute; `~` is not expanded. Replace the example HTTPS origin, home path, installed Paseo command path and project root. The port must remain `6768`, and `upstreamUrl` must remain `http://127.0.0.1:6767/mcp/agents`.

Use **`<your home>/.local/share/local-agents-mcp` for `stateDir`**, with the gateway and Paseo agents running as the same OS user. The executable `dot-message.mjs` derives this default from the current user's home. Although the server accepts another state directory, the executable message channel does not follow a custom directory. Keep state outside the repository. Choose a narrow, existing project directory for `allowedRoots`, including any worktree roots you deliberately authorize. This only checks the target directory; it does not contain code executed by an agent.

Create a private owner-password file locally. The following command creates the default state directory and fails rather than overwriting an existing password file:

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

Set `ownerTokenFile` to the absolute path of that file. The owner password is only for your local consent workflow; never commit or send it in an agent message. Set `paseoCommand` to your installed CLI's absolute path; native timeline/project readers still use the fixed app installation above.

```sh
npm run check
npm test
npm start
```

Run one gateway for that port and state directory. Do not launch a second copy beside an existing gateway. Tests create temporary state and ephemeral local HTTP/Unix sockets with mocked upstream and callback clients; they do not start a production gateway or dispatch real tasks.

The rebuild compiles the locked SQLite module only; it needs a C/C++ toolchain (Xcode Command Line Tools on macOS), Python and access to official Node headers. Other dependency lifecycle scripts remain disabled.

Configure your HTTPS reverse proxy to the loopback gateway, forwarding authorization and MCP headers. Add `https://<your origin>/mcp` to a compatible client and complete owner consent there. OAuth requires `local-agents`, S256 PKCE and the exact resource URL; access tokens last one hour and refresh tokens 30 days. Authorization redirects accept `chatgpt.com`, `chat.openai.com` and loopback hosts (`localhost`, `127.0.0.1`, `::1`); other client redirect hosts require a deliberate source change. Protect registration/consent endpoints with proxy rate limits and monitor abuse before exposing this powerful service publicly. The repository does not provision a tunnel or change firewall rules.

## Task workflow

1. Discover the exact project with `list_projects`, then reuse or create an explicit `workspaceId`. Read `list_profiles` and discover provider capabilities before dispatching. Use an isolated worktree for independent writers.
2. Call `create_agent` with a provider/model, workspace, scoped prompt and stable unique `requestId`. `send_agent_prompt` continues an existing agent with a new request ID.
3. A persisted receipt confirms submission, not completion. Recover it with `get_dispatch_request`. If the outcome is `unknown` or `submitting`, inspect that original request and timeline; do not blindly resend under a new ID.
4. Read `get_request_result` for the original dispatch/turn, or `get_agent_result` for the projected timeline. Review the actual artifact before treating a task as accepted.

`wait_for_agent_result` takes the original `requestId` and exact `agentId`; its total budget is 1–20 seconds. `idle`, a terminal observation, readable text and task success are separate. `acceptancePassed` remains `null`; legacy requests without markers, shared turns, incomplete history or cursor discontinuities remain unconfirmed. A timeout is safe to reread and never redispatches or cancels an agent. Timeline pages are bounded to 1 MiB; use returned cursors and check gap/stale/reset indicators.

The gateway exposes a selected agent/workspace tool set. It does not expose general terminal management, workspace deletion or scheduling. Authorized clients can still read and control allowed agents, including cancellation, runtime settings and specific permission responses; grant access only to clients you trust with that authority.

## Events and agent messages

An Events-capable client subscribes to **`agent.attention`**, passing the real `workspaceId`, a stable `routeId` unique to that chat, and its own HTTPS callback/signing secret. Confirm with `list_notification_routes`, then bind dispatches with `notificationRouteId`. Different chats use different routes. Subscriptions belong to the authenticated OAuth client; callers cannot supply a different owner identity.

For an existing request, verify its receipt and workspace, subscribe in the intended chat, then call `watch_dispatch_request` with the original request ID and explicit route. This attaches notifications without creating an agent, sending a prompt or restarting work. It cannot rebind a request to another route. Attaching a watch does not inject messaging instructions into a running task; a later authorized follow-up receives those instructions.

A routed agent receives a command pointing to the installed `bin/dot-message.mjs`. It sends text through stdin with its request ID and runtime `PASEO_AGENT_ID`; a `needs_input` message should end the agent's current turn. The client reads the full message with `get_agent_message`, checks the original result, and replies to the same agent with `replyToMessageId` after `readyForReply` is true. One durable reply claim prevents duplicate callback deliveries from launching duplicate follow-ups. Agent text is untrusted task data, not new user authorization.

**Automatic wake requires an active matching subscription, working callback delivery, an online Mac/Paseo, and a client that actually handles the event.** A normal wait call, `notifyOnFinish`, route binding or HTTP 2xx does not prove that a chat woke or a task succeeded. Without Events support or a valid route, use explicit wait/read calls. No end-to-end wake latency guarantee is made.

Callbacks require HTTPS, verified challenge responses, public DNS/IP destinations and Standard Webhooks signatures. Connections pin a verified IP with TLS verification; redirects and private/local destinations are rejected. Delivery uses a durable outbox, at most five attempts for transient errors, and stops on 410. Default subscription lifetime is one day (`ttlMs: null` selects seven days); refresh it before expiry. Other positive finite lifetimes are accepted. Cursor replay is not implemented (`cursor: null`); exact results are recovered through read tools. Unsubscribe when monitoring ends, preserving unrelated routes.

## Security and data

Read [SECURITY.md](SECURITY.md) before exposing a gateway. This is a **single trusted user's authority domain**, not a multi-tenant service. OAuth-client ownership protects collaboration receipts, messages and routes; generic agent reads/controls primarily check `allowedRoots`. All authorized clients may affect agents in those roots.

OAuth state, callback URLs/secrets, agent capabilities, messages, task results and dispatch records are private local state. Treat state and backups as credentials and user data; filesystem permissions are not encryption. The Unix IPC protects against other OS users, not hostile processes running as the same user. Agent permissions and task authorization remain necessary.

This public source snapshot contains templates and synthetic tests. It excludes production configurations, runtime databases, private App/plugin bindings, captured sessions, research data, private callback URLs and operational history. Never use those items as public examples.

## Development and license

```sh
npm ci --ignore-scripts
npm_config_build_from_source=true npm rebuild better-sqlite3
npm run check
npm test
```

CI checks syntax and the existing behavior/security tests. It does not prove a production client subscription or real provider credentials work.

Original project code and documentation are licensed under [MIT](LICENSE), Copyright (c) 2026 xixilys. The Paseo schema fixture retains its Apache-2.0 license; see [NOTICE](NOTICE) and [third_party/paseo-LICENSE](third_party/paseo-LICENSE). Installed dependencies and provider software retain their own licenses and terms, including any proprietary terms. Dependencies and Paseo binaries are not bundled here. `private: true` prevents accidental npm publication; this release publishes source on GitHub.
