# Terraform LSP Multiplexing

## Goal

The fgDevDarwin OpenCode fork transparently shares its built-in `terraform-ls` between concurrent OpenCode processes while keeping Terraform workspace roots isolated. This removes the need for nixconf's global Terraform LSP disable workaround.

## Scope and selection

- Multiplexing is the default when the built-in `terraform` server is enabled without a `terraform` config entry. This includes `lsp: true` and an LSP config object that changes only other servers.
- `lsp: false` and `lsp.terraform.disabled: true` still disable Terraform.
- Any explicit `lsp.terraform` launch override, including command, environment, initialization, or extensions, uses the existing direct launch path and isn't pooled.
- Every non-Terraform server keeps its existing direct behavior.
- Pooling requires no project setting, opt-in, or experimental flag. Eligible roots join the broker's compatible multi-root session. If OpenCode can't establish that session or the server can't manage workspace folders dynamically, the affected client falls back to the existing direct built-in Terraform launch. Terraform and other servers remain unaffected.

## Broker contract

OpenCode uses one private, user-scoped local broker. It accepts workspace roots dynamically, keeps no fixed root list, and doesn't restart when roots are added or removed.

- The broker runs one compatible multi-root `terraform-ls` worker for each built-in session fingerprint. Unmodified built-in Terraform clients normally share a fingerprint, so their canonical roots share a worker.
- The broker initializes the worker as a multi-root server and manages leased canonical roots with `workspaceFolders` and `workspace/didChangeWorkspaceFolders`. It adds each root on its first lease and removes it after its final lease.
- Root canonicalization resolves relative components and symlinks before lookup. Each client session is bound to exactly one root.
- Concurrent OpenCode processes converge on the same user-scoped broker.
- The shared launch discovers or downloads the built-in `terraform-ls` exactly as a direct launch does, honors `OPENCODE_DISABLE_LSP_DOWNLOAD`, and preserves the built-in initialization options. It starts `terraform-ls serve` in a stable broker-owned context rather than a workspace root.
- The worker's initialize request advertises workspace-folder support and includes all currently leased roots without setting one as the global `rootUri`. Its initialization options and client capabilities remain compatible with direct built-in launch.
- The broker verifies compatible multi-root workspace-folder support before sharing a worker across roots. When a session fingerprint or server is incompatible, the affected client launches directly without changing an existing compatible session.

## Root and client isolation

- Each client connection is bound to one canonical root. File URIs in client traffic must resolve within that root. The broker rejects out-of-root paths and symlink escapes.
- Diagnostics are routed by canonical file path only to clients whose bound root contains that path. Related diagnostics and workspace results are filtered to prevent clients from receiving paths or diagnostics that belong only to another root.
- Each client's workspace-folder view exposes only its bound root, while the shared worker sees every leased root. Configuration requests preserve the built-in Terraform settings unchanged without exposing another root's state.

## Security

- The broker listens only on local IPC and never on a network interface. Its endpoint, discovery metadata, and random authentication secret live in a user-private location with owner-only permissions.
- Clients authenticate with that secret before naming a root or exchanging LSP traffic. The secret isn't exposed in logs or to `terraform-ls`.
- OpenCode launches directly when it can't establish private authenticated local IPC.

## Lifecycle and failures

- An authenticated connection explicitly acquires and releases its root lease. Normal OpenCode instance disposal closes its documents, releases the lease, and closes the broker connection.
- Multiple clients can lease the same canonical root. The broker adds the root to the worker on its first lease and removes it through `workspace/didChangeWorkspaceFolders` only after the final lease is released.
- Closing a client connection releases its leases. Releasing one root leaves the shared worker and other leased roots active. The worker shuts down when the final lease for its session fingerprint is released.
- After the final connection closes and its worker stops, the broker exits and removes its endpoint, secret, discovery metadata, and ownership state. Broker shutdown also terminates its worker.
- The affected client uses direct built-in Terraform launch when broker startup, connection, authentication, compatibility checking, workspace-folder attachment, or the shared worker is unavailable. Automatic worker replacement and document replay aren't required, nor is transparent generation recovery.

## Internal pool status for tests

A minimal, read-only internal status seam may report a Terraform client's pool mode and canonical root fingerprint, plus the pooled worker PID when applicable. It may also report when the client and worker are no longer active after final release.

- Tests use this seam only to observe default pooled selection, custom or disabled bypass, and final cleanup deterministically.
- The seam must not start or stop a broker or worker, acquire a lease, change selection or fallback behavior, or expose document or diagnostic content.
- This isn't public configuration or a supported broker protocol, API, CLI, UI, or user-visible status surface.

## Required tests

Tests use temporary directories and a controllable fake `terraform-ls`, so they don't require an installed Terraform server.

1. Concurrent OpenCode processes with the same compatible session fingerprint converge on one broker and share one multi-root worker, whether they use the same or different canonical roots.
2. Initialization includes the initial root. First and final leases add and remove later roots through the workspace-folder protocol without duplicate events. Releasing one root leaves another usable.
3. Out-of-root paths and symlink escapes are rejected. Diagnostics and related diagnostics can't cross client or root boundaries, and neither can workspace-result paths.
4. The internal status seam deterministically confirms that the default built-in Terraform configuration pools without an opt-in. It also confirms that disabled Terraform stays disabled, while custom Terraform overrides and non-Terraform servers continue to launch directly.
5. Direct built-in fallback applies when the broker is unavailable, private authentication fails, the session fingerprint is incompatible, or the server lacks compatible multi-root support.
6. Built-in discovery and download policy remain compatible with direct launch, as do `serve` arguments, initialization options, and client capabilities.
7. Releasing the final lease and connection stops the worker and broker, then removes private broker state. The internal status seam reports that the client and pooled worker are no longer active.

## Acceptance criteria

- Unmodified built-in Terraform configuration pools by default across concurrent OpenCode processes and compatible distinct canonical roots. No global or project opt-in is required.
- Client and root filesystem paths remain isolated, as do diagnostics.
- Disabled Terraform preserves its existing disabled behavior. Custom Terraform overrides and non-Terraform servers preserve their existing direct behavior.
- An unavailable or incompatible broker or session uses direct built-in Terraform fallback.
- Broker IPC and state are private to the local user. Final release removes broker state and stops its worker.
- Focused LSP and configuration tests pass, along with `packages/opencode` typechecking.

## Non-goals

- Pooling other language servers or building a general-purpose LSP broker.
- Adding a project-level pooling option, required feature flag, or public broker protocol.
- Exposing the internal pool-status test seam through public configuration, API, CLI, UI, or other supported user-facing behavior.
- Sharing workers across users or incompatible session fingerprints, including custom Terraform overrides.
- Automatic worker replacement and document replay, including transparent reconnect or exhaustive crash recovery.
- Changing root discovery, Terraform language IDs, formatting, public LSP APIs, or the custom LSP configuration shape.
- Editing nixconf in this repository. This behavior is required before removing its separate global disable workaround.

## Targeted validation

Run from `packages/opencode`:

```console
bun test --timeout 30000 test/lsp
bun test --timeout 30000 test/config/lsp.test.ts
bun typecheck
```
