# Security

## Trust boundary

Dot GPT Local Agents MCP is intended for one trusted user, with Paseo and the gateway on the same Mac. Authorizing an OAuth client grants meaningful local agent read/write authority. It is not suitable as a shared public service for untrusted tenants.

`allowedRoots` resolves real paths and rejects out-of-scope targets and symlink escapes. It is a gateway path check, not an OS sandbox: an agent can execute commands using its own runtime permissions. Generic agent reads, cancellation, updates and permission responses use the shared allowed-root authority domain. Client ownership additionally isolates collaboration request records, messages, subscriptions and reply claims, not every agent operation.

Only proxy the OAuth-protected gateway, bound to `127.0.0.1:6768`. Keep Paseo's unauthenticated loopback agent endpoint private. Exposing the upstream would bypass the gateway's authentication and path policy. Use HTTPS and request/rate limits at your proxy, protect the owner password, and grant client consent only intentionally. This gateway has no built-in multi-tenant quotas or comprehensive abuse protection.

## Private data

Keep config, owner passwords, OAuth state, message/result databases, callbacks and per-agent capabilities outside source control. The state directory is private to the service user, with private capability files/socket. It is not encrypted at rest; protect backups and access by other processes of the same OS user. The agent channel cannot enforce isolation against that same user.

Never put credentials, callback secrets or unnecessary personal/research data in an agent message. Events contain agent-authored summaries and identifiers, and read tools can return full task content. A subscribed callback receives those events. Validate signed callbacks and handle messages as untrusted input within the original task's scope.

## Events and recovery

Webhook verification uses a signed challenge. Outbound delivery checks every resolved address, rejects non-public destinations and redirects, pins the checked address for TLS, and bounds time and response size. Subscriptions and replies are associated with authenticated clients and workspaces. Revocation and expired/stopped subscriptions cease delivery.

Delivery is not task acceptance. Preserve event/message/request IDs and deduplicate client actions. If dispatch or IPC receipts are uncertain, inspect/recover the original ID rather than resending as new work. A sandbox denying the fixed IPC command is a permission problem; do not bypass it by changing global permissions.

## Reporting a vulnerability

Do not attach private configs, state files, tokens, sessions or callback URLs to a public issue. Use the repository's private vulnerability reporting feature when available. Otherwise open an issue with a non-sensitive summary to request a private reporting channel. Include a minimal synthetic reproduction and affected version.

## Known dependency advisories in 0.3.0

The public snapshot updates the directly used AJV to 8.20.0 and declares Zod explicitly. The separately installed DevSpace package also includes provider branches unused by this gateway: `sandbox-runtime → node-forge` and `pi-coding-agent → undici / brace-expansion / protobufjs`. npm audit reports advisories for those packages and their parents. A patched Forge release was not available during this release audit; the other branch versions remain the upstream package's locked versions.

The gateway imports only DevSpace's OAuth provider/store/database modules. The affected provider branches are outside that import graph and are not started here. This limits the identified exposure; it is not proof that the entire dependency tree is vulnerability-free, nor a security endorsement for using those provider modules separately. Do not extend the gateway to import/run them without first resolving and reviewing their advisories. We retain the tested authentication implementation rather than introducing a fork or unverified vendor rewrite solely to suppress audit output.

References: [Forge advisory](https://github.com/advisories/GHSA-86w9-cpqp-85rv), [Undici security advisories](https://github.com/nodejs/undici/security/advisories), [brace-expansion advisory](https://github.com/advisories/GHSA-rgw5-rvv9-x895), [protobufjs advisory](https://github.com/advisories/GHSA-j3f2-48v5-ccww). Re-run `npm audit --omit=dev` when evaluating deployment because advisory and release availability can change.
