# codex-app-server

`codex app-server` is the interface Codex uses to power rich interfaces such as the [Codex VS Code extension](https://marketplace.visualstudio.com/items?itemName=openai.chatgpt).

## Table of Contents

- [Protocol](#protocol)
- [Controlled execution and observer attachment](#controlled-execution-and-observer-attachment-experimental)
- [Message Schema](#message-schema)
- [Core Primitives](#core-primitives)
- [Lifecycle Overview](#lifecycle-overview)
- [Initialization](#initialization)
- [API Overview](#api-overview)
- [Events](#events)
- [Approvals](#approvals)
- [Skills](#skills)
- [Apps](#apps)
- [Auth endpoints](#auth-endpoints)
- [Experimental API Opt-in](#experimental-api-opt-in)

## Protocol

Similar to [MCP](https://modelcontextprotocol.io/), `codex app-server` supports bidirectional communication using JSON-RPC 2.0 messages (with the `"jsonrpc":"2.0"` header omitted on the wire).

Supported transports:

# Application network policy

App-server loads application network policy at startup and existing explicit
config/account reloads. Local requirements-file edits take effect on
the next explicit reload or restart. Installing a new policy cancels requests
that it no longer permits; a failed policy load blocks network traffic.

Embedded app-server installs the same policy-aware requirements loader for clients
it constructs. The embedding TUI and exec runtime install the same policy before
creating their telemetry providers, background HTTP clients, and executor
connections. TUI worktree cloud loaders retain that shared policy on reload.

# User verification cancellation (experimental)

When running with `--listen ws://IP:PORT`, the same listener also serves basic HTTP health probes:

- `GET /readyz` returns `200 OK` once the listener is accepting new connections.
- `GET /healthz` returns `200 OK` when no `Origin` header is present.
- Any request carrying an `Origin` header is rejected with `403 Forbidden`.

Websocket transport is currently experimental and unsupported. Do not rely on it for production workloads.

Pass `--code-mode-host URL` to connect this app-server process to a remote code-mode host instead of starting a local host. Use a root `http://` or `https://` URL without a path or query for gRPC. Remote hosts require the `code_mode_host` feature. This outbound connection is independent of `--listen` and is shared by the process's threads.

The unix socket transport is intended for local app-server control-plane clients. `codex app-server proxy`
opens exactly one raw stream connection to `$CODEX_HOME/app-server-control/app-server-control.sock`
by default, or to `--sock PATH` when provided, and proxies bytes between that socket and stdin/stdout.
The proxied stream carries the websocket HTTP Upgrade handshake followed by websocket frames.

On Windows, the socket directory is created with a protected current-user-only DACL. Existing
directories must already have that owner and DACL; startup rejects broader permissions rather
than attempting to repair previously exposed state. Custom sockets should use a new dedicated
subdirectory. The listener pins the validated directory until socket cleanup completes.

`codex app-server daemon` manages this local server on Unix and Windows using the standalone
installation. The TUI discovers an available local daemon; `codex agents` starts it when no explicit
remote endpoint is supplied. See [daemon lifecycle](../app-server-daemon/README.md) for commands and platform requirements.

Tracing/log output:

- `RUST_LOG` controls log filtering/verbosity.
- Set `LOG_FORMAT=json` to emit app-server tracing logs to `stderr` as JSON (one event per line).

Backpressure behavior:

- The server uses bounded queues between transport ingress, request processing, and outbound writes.
- When request ingress is saturated, new requests are rejected with a JSON-RPC error code `-32001` and message `"Server overloaded; retry later."`.
- Clients should treat this as retryable and use exponential backoff with jitter.

## Controlled execution and observer attachment (experimental)

Launch one service per managed execution environment, with a launcher-generated
fresh credential file for each process lifetime, containing 32–4096 bytes of random
secret text (surrounding whitespace
is trimmed). Keep that file available only to the launcher and controller:

```sh
codex app-server --listen unix:///absolute/path/actor.sock \
  --controller-token-file /absolute/path/controller-token
codex observe THREAD_UUID --remote unix:///absolute/path/actor.sock
```

The standalone `codex-app-server` binary accepts the same service flags. Explicit
`ws://IP:PORT` is also supported. Unix sockets carry a WebSocket handshake and
frames, not JSONL; the controller must use the native transport. The credential
establishes execution custody, not OS-user isolation or transport authentication.
Configure those separately in the launcher. Claiming an ordinary running service
is unsupported. Omitting the flag preserves ordinary service/client behavior.

Codex app-server advertises `openai/elicitation.userVerification` to the
host-owned plugin service for bundled, in-process TUI sessions (`codex-tui`) and
local stdio desktop sessions (`Codex Desktop`) on devices with supported biometric
hardware and the `experimentalApi` opt-in. This is an app-server decision,
independent of whether a key exists; TUI/Desktop/mobile do not advertise this MCP
capability. Mobile integration requires a separate rollout. Other clients and
network connections do not receive this mode, even with a recognized client name.
Before sending verification requests to desktop sessions, deploy a GUI that
handles the typed verification request, cancellation, and late proofs. The general
`experimentalApi` opt-in does not identify a compatible GUI version.

Native `openai/userVerification` elicitation requests preserve optional `_meta`
JSON through MCP transport and `mcpServer/elicitation/request`. Clients may use
this metadata for extension-specific presentation and must continue to accept
requests without it. Metadata does not change the challenge bytes or the proof
returned in the acceptance response.

Local UI clients use five methods. They require the existing
`experimentalApi` opt-in. The local provider reports
`unavailable/providerUnavailable` on unsupported platforms or without the required
ChatGPT account identity.

| Method | Params | Result |
| --- | --- | --- |
| `userVerification/status` | `{}` | `{credentialId, unavailableReason, unavailableMessage}` |
| `userVerification/enroll` | `{}` | `{credentialId, algorithm?, publicKey?}` |
| `userVerification/delete` | `{}` | `{}` |
| `userVerification/verify` | `{challenge, title, description}` | `{proof: {credentialId, signature}}` |
| `userVerification/cancel` | `{requestId}` | `{}` |

Status reads local readiness without prompting or contacting a backend. A null
`unavailableReason` means local checks passed, not that registration is valid.
Unsupported platforms and missing account identity are reported in the status
response's `unavailableReason` field.
Enrollment creates or reuses the local key and returns its public metadata. The
`publicKey` is unpadded base64url SPKI-DER; `algorithm` is `ecdsaP256Sha256X962`.
During the experimental rollout, `algorithm` and `publicKey` are optional for
compatibility with older app-servers. Current servers populate both fields;
callers must check that both are present and non-null before backend registration.
The trusted UI host owns backend registration: obtain an enrollment challenge,
sign it with `userVerification/verify`, check that the proof's `credentialId`
matches this response, and submit the public metadata and proof to the backend.
Local success is not server enrollment. The caller must preserve the authenticated
account across this flow and reconcile uncertain registration before retrying.
Deletion removes the local key; the caller owns backend revocation.
Enrollment and deletion coordinate credential lifecycle; callers do not issue
separate generate or rotate commands. Identity comes from the authenticated
account; this API exposes no caller-selected scope.

Verify signs 1–4096 decoded challenge bytes using P-256 ECDSA with SHA-256. The
challenge and DER signature use unpadded base64url. Title is 1–256 UTF-8 bytes;
description is at most 4096 bytes. The UI obtains approval for that display
context before calling. Verify does not require a pending elicitation; a UI with
its own authenticator can return proof directly in elicitation response content.
The calling flow owns pending-request checks and discards late proofs.
Native enroll, delete, and verify accept local stdio and in-process connections.
WebSocket and remote-control peers must use their own device authenticator;
status remains available for local readiness. Dropping an embedded RPC, disconnecting,
or changing authentication cancels its native operation. Responses recheck the
captured identity after waiting for outbound queue capacity.
Canceling or resolving an elicitation does not itself stop a separate
`userVerification/verify` RPC. The GUI must use `userVerification/cancel` to
cancel that RPC and discard late proofs when an approval is canceled or resolved.
See [User verification cancellation](#user-verification-cancellation-experimental)
for request ID and acknowledgment semantics.
Only one native worker runs per app-server. If an OS call remains active after
cancellation or timeout, subsequent local operations return `failed/providerError`
until that worker exits.

Failures use the normal JSON-RPC error envelope with closed `{type, reason}` data:
`invalidRequest`, `unavailable`, `cancelled`, or `failed`. UI clients branch on
these values rather than message text. Native diagnostic payloads stay private.

## Local rollout compression

The experimental `rollout/compress` method takes no parameters and immediately
returns `{}` after scheduling one best-effort background pass over the app-server's
local rollout storage. It does not change `features.local_thread_store_compression`
or require that startup flag to be enabled. Non-local thread stores do not support
this method.

The worker retains its existing cold-file checks, maintenance and writer locks,
concurrency limit, and cooldown. Acknowledgement does not imply completion or that
any files were compressed; failures are reported through existing logs and metrics.
There are no progress notifications or cancellation API. Clients sharing this
Codex home must support compressed rollout files, including shared histories.

## Managed model provider requirements

Existing threads retain their provider configuration. Input RPCs reject requests when managed
`model_provider` or `model_providers` requirements no longer match that configuration, or cannot
be loaded. This covers turn start/steer, review, compaction, manual queue start, and active goal
updates. Realtime connections use separate routing configuration and are not checked here.
Interrupt, realtime stop, and goal pause/clear remain available. User and project
configuration changes alone do not invalidate existing threads.

# Amazon Bedrock authentication

If `model_providers.amazon-bedrock.aws.credential_export` is configured, Bedrock setup and
Bedrock login return an error without changing configuration or saved credentials. Remove the
exporter configuration before selecting another credential source. `aws.credential_export` and
`aws.profile` cannot be configured together.

Application network restrictions apply to each AWS credential and region HTTP request and to
the Bedrock destination. Static access keys with an explicit region need no credential discovery.
AWS profile `credential_process` commands are run by the AWS SDK; their network traffic is outside
the application's HTTP policy. Configured credential exporters and AWS reauthentication commands
require unrestricted application policy; policy revocation cancels their active work.

## Stored thread attachments

- `thread/attachment/add` — add a durable resource reference to a stored thread without loading it. Repeated writes with the same attachment type and identity key return the existing attachment.
- `thread/attachment/list` — list attachments for one stored thread in a cursor-paginated request, including a thread that is not loaded.
- `thread/attachment/remove` — remove an attachment by its thread, attachment type, and identity key; returns `{}`.
- `thread/attachment/updated` — notification broadcast after an attachment is created or removed; contains the thread, attachment identity, attachment id, and operation.
### Example: Manage stored thread attachments

Attachments record the resources currently associated with a thread, independently of conversation history. Clients can add, remove, and list attachments for one stored thread at a time without resuming those threads. Adding or removing an attachment does not create or delete the underlying resource or rewrite history. An attachment is idempotently identified by its thread, `attachmentType`, and `identityKey`. For pull requests, clients should reuse the canonical application identity `JSON.stringify([canonicalHostname, lowercaseOwner, lowercaseRepository, pullRequestNumber])` so addition and removal agree across surfaces.

```json
{"id":1,"method":"initialize","params":{"clientInfo":{"name":"shoal","version":"1"},"capabilities":{"experimentalApi":true}}}
{"id":2,"method":"control/acquire","params":{"token":"LAUNCHER_CREDENTIAL"}}
{"id":2,"result":{"instanceId":"INSTANCE_UUID","state":"controlled","shutdown":"notStarted","reconciliationRequired":false}}
{"id":3,"method":"thread/start","params":{"dynamicTools":[{"type":"function","name":"effect","description":"Execute an actor effect","inputSchema":{"type":"object","properties":{}}}]}}
{"id":4,"method":"thread/ready","params":{"threadId":"THREAD_UUID"}}
{"id":5,"method":"turn/start","params":{"threadId":"THREAD_UUID","input":[{"type":"text","text":"Perform the assignment"}]}}
```

All roots created or resumed in controlled mode require readiness, including roots
without hosted tools. Register the destination host before acknowledging readiness.
The controller uses the existing input/steering, queue, interrupt and full-prefix
fork APIs. For an invocation-boundary fork, supply `throughCallId`,
`requireClientReadiness: true` and `expectedDynamicTools` matching the inherited
registration. Child-only protocol closures complete the inherited boundary without
executing its call or pretending to settle its source effect. The child needs its
own assignment and readiness acknowledgment; viewing it releases neither barrier.
See the invocation-boundary examples below.

Hosted calls, approvals, time and attestation requests are routed exclusively to the
controller. Reply on its original connection using the JSON-RPC request ID, for example:

```json
{"id":73,"method":"item/tool/call","params":{"contextCallId":null,"threadId":"THREAD_UUID","turnId":"TURN_UUID","callId":"CALL_ID","namespace":null,"tool":"effect","arguments":{}}}
{"id":73,"result":{"contentItems":[{"type":"inputText","text":"Recorded external result"}],"success":true}}
```

A second connection cannot claim custody, mutate execution or consume a callback
using a forged result/error. The server rejects observer mutations with error
`-32010` and `data.reason: "controlUnavailable"`. Custody is enforced by the server;
clients must not rely on the observer UI's disabled input as an authority boundary.

An observer calls `thread/observe` with an ID already loaded by the controller.
The response contains thread metadata and a listener-ordered active-turn snapshot;
subsequent notifications follow it. The connection may then read that thread's
history with `thread/read`, `thread/turns/list` and, for paginated history,
`thread/items/list`. It may unsubscribe. Observation never loads or resumes an
execution, acknowledges readiness, replays callbacks, or changes tools/configuration.
Reading inactive persisted history through observer attachment is deferred.
Controlled runtimes stay loaded independently of subscriber counts, including when
the controller unsubscribes; observer detachment never owns idle unloading.

The native `observe` command opens only this observation path. It shows a read-only
transcript; arrows scroll, PageUp browses persisted history from newest to oldest,
End returns to live output,
and q exits. Its display retains up to 1,000 recent live items and a separate older
page. It never answers server requests or reconnects automatically. A notification
gap is displayed explicitly; exit and reattach to refresh. Detachment closes only
the observer connection, leaving the service and executor children owned by the
controller. Standard interactive `codex --remote` is not the observer command.

### Controller disconnect and reconciliation

Controller disconnection irreversibly changes this service instance to `fenced`,
requests cancellation of inference/tool execution and blocks further execution
admission. No observer
becomes controller, and even the credential cannot acquire replacement custody in
the fenced instance. Already dispatched external effects may still finish.
`control/status/read` and `control/status/changed` expose:

- `state`: `awaitingController`, `controlled`, or `fenced`.
- `shutdown`: `notStarted`, `draining`, `sessionsStopped`, or `incomplete`.
- `reconciliationRequired`: true after fencing, even when native shutdown completed.
- `instanceId`: identifies this process lifetime, not an effect-deduplication key.

`sessionsStopped` means session-loop shutdown and controller request/startup draining
completed. Existing process managers request termination without confirming every
local/remote process exit; Windows restricted direct commands may not be cancellable.
This status does **not** establish process exit or external-effect outcomes. The
launcher must stop the service and its executor process tree before replacement,
including after `sessionsStopped`. `incomplete` additionally means session/request
cleanup timed out or failed; never assume safe draining.
The service remains available for status inspection and never automatically restarts,
replays callbacks, or resubmits input.

Call `control/pending/list` with optional `cursor` and `limit` (1–100). Each entry
contains a request ID, method and available thread/turn/call IDs, without arguments.
After fencing this is a frozen, process-local snapshot of at most 1,024 outstanding
requests; `truncated: true` means it is incomplete (also while capture is pending).
Live pagination can change as callbacks settle. This inventory is diagnostic, not a
persistent effect ledger: an accepted response may have left the callback map before
its result reached durable history. Absence from the inventory proves no outcome.

Preserve recorded results and reconcile unrecorded external-effect outcomes as
**uncertain**, never as failed or undone. A request response establishes only that
operation's documented acceptance; a successful socket write, callback ID or
`thread/ready` response does not prove presentation, effect completion or deduplication.
Use item/turn completion notifications and persisted history alongside the host's
own durable effect records. Stop the fenced service and its executor tree, confirm quiescence, and reconcile
outcomes before launching a replacement with a fresh credential. Then acquire custody,
resume the chosen thread, inspect
its durable queue/history, reconnect its host, then explicitly acknowledge readiness.
Readiness can release existing queued input: remove or reconcile uncertain input
before that acknowledgment. Do not blindly resubmit a request whose acknowledgment
was lost. No exactly-once guarantee is added by this protocol.

### Building and validating the controlled service

Build both native entry points from the pinned checkout:

```sh
cd codex-rs
cargo build --locked -p codex-cli --bin codex -p codex-app-server --bin codex-app-server
just test -p codex-app-server -p codex-app-server-protocol -p codex-core -p codex-tui -p codex-cli -E 'binary(observer) | test(observer::tests) | test(controlled_service) | test(execution_fence) | test(fenced_shutdown) | test(control::tests::disconnected_connections) | test(control::tests::only_lost)'
```

The focused acceptance selection passed all 13 tests on Linux with mock providers.
It exercises native WebSocket custody/callback routing, fork/readiness, fenced
recovery with a fresh credential, and actual native TUI attachment through a PTY.
Two observer rendering snapshots cover connected and fenced displays. Both stable
and experimental schema generation passed.

The broader five-crate run was not clean: 10,173 passed, 485 failed, one timed out,
and 18 were skipped. Failures included unavailable hardcoded system executables,
sandbox helper failures and unrelated existing terminal snapshots; the full failure
set has not been established as baseline-only. The complete workspace suite was
not run. Windows/macOS behavior and complete executor-process termination were not
verified by these Linux acceptance tests.

The consumer still owns mount setup, destination tool-host registration, durable
effect reconciliation, assignment/notification semantics and the mounted
controller/observer canary. Native tests and source review do not establish that
mounted integration's acceptance.

## Message Schema

Currently, you can dump a TypeScript version of the schema using `codex app-server generate-ts`, or a JSON Schema bundle via `codex app-server generate-json-schema`. Each output is specific to the version of Codex you used to run the command, so the generated artifacts are guaranteed to match that version.

```
codex app-server generate-ts --out DIR
codex app-server generate-json-schema --out DIR
```

## Core Primitives

The API exposes three top level primitives representing an interaction between a user and Codex:

- **Thread**: A conversation between a user and the Codex agent. Each thread contains multiple turns.
- **Turn**: One turn of the conversation, typically starting with a user message and finishing with an agent message. Each turn contains multiple items.
- **Item**: Represents user inputs and agent outputs as part of the turn, persisted and used as the context for future conversations. Example items include user message, agent reasoning, agent message, shell command, file edit, etc.

Use the thread APIs to create, list, or archive conversations. Drive a conversation with turn APIs and stream progress via turn notifications.

## Lifecycle Overview

- Initialize once per connection: Immediately after opening a transport connection, send an `initialize` request with your client metadata, then emit an `initialized` notification. Any other request on that connection before this handshake gets rejected.
- Start (or resume) a thread: Call `thread/start` to open a fresh conversation. The response returns the thread object and you’ll also get a `thread/started` notification. If you’re continuing an existing conversation, call `thread/resume` with its ID instead. If you want to branch from an existing conversation, call `thread/fork` to create a new thread id with copied history. Like `thread/start`, `thread/fork` also accepts `ephemeral: true` for an in-memory temporary thread.
  The returned `thread.ephemeral` flag tells you whether the session is intentionally in-memory only; when it is `true`, `thread.path` is `null`.
- Begin a turn: To send user input, call `turn/start` with the target `threadId` and the user's input. Optional fields let you override model, cwd, sandbox policy or experimental `permissions` profile selection, approval policy, approvals reviewer, etc. This immediately returns the new turn object. The app-server emits `turn/started` when that turn actually begins running.
- Stream events: After `turn/start`, keep reading JSON-RPC notifications on stdout. You’ll see `item/started`, `item/completed`, deltas like `item/agentMessage/delta`, tool progress, etc. These represent streaming model output plus any side effects (commands, tool calls, reasoning notes).
- Finish the turn: When the model is done (or the turn is interrupted via making the `turn/interrupt` call), the server sends `turn/completed` with the final turn state and token usage.

## Initialization

Clients must send a single `initialize` request per transport connection before invoking any other method on that connection, then acknowledge with an `initialized` notification. The server returns the user agent string it will present to upstream services, `codexHome` for the server's Codex home directory, and `platformFamily` and `platformOs` strings describing the app-server runtime target; subsequent requests issued before initialization receive a `"Not initialized"` error, and repeated `initialize` calls on the same connection receive an `"Already initialized"` error.

`initialize.params.capabilities` also supports per-connection notification opt-out via `optOutNotificationMethods`, which is a list of exact method names to suppress for that connection. Matching is exact (no wildcards/prefixes). Unknown method names are accepted and ignored.

Clients declare supported MCP extensions during initialization. For OpenAI
extended forms, clients must handle the request envelope, including a fallback
for unsupported field types. `mcpServerOpenaiFormElicitation: true` remains a
legacy alias for declaring the `openai/form` extension.

```json
{
  "capabilities": {
    "extensions": {
      "openai/form": {},
      "openai/elicitation": { "form": {} },
      "io.modelcontextprotocol/ui": {
        "mimeTypes": ["text/html;profile=mcp-app"]
      }
    }
  }
}
```

`openai/elicitation.form: {}` declares support for forms received as
`mode: "openaiForm"`. It does not follow from the legacy capability.
App-server retains only the `form` key under `openai/elicitation`, preserving
its value when present. Form requests require an object-valued `form`
declaration. A bare namespace does not imply form support. User verification
requests are not implemented.
Clients must only advertise features supported by both the client and the
connected app-server.

App-server keeps the complete value under `io.modelcontextprotocol/ui`, rather
than deriving a WebView boolean, so clients can advertise additional supported
MIME types and future extension settings. The MCP extension profile is fixed
when a Codex session is created by `thread/start`, `thread/resume`, or
`thread/fork`. Codex advertises that profile in the downstream MCP
`initialize` request; it is not repeated in individual tool-call metadata.
Every turn and direct MCP tool call in that loaded session therefore uses the
same initialized profile. A different app-server connection cannot change it
by starting a later turn. Subagent sessions inherit the same extension profile.

Applications building on top of `codex app-server` should identify themselves via the `clientInfo` parameter.

**Important**: `clientInfo.name` is used to identify the client for the OpenAI Compliance Logs Platform. If
you are developing a new Codex integration that is intended for enterprise use, please contact us to get it
added to a known clients list. For more context: https://chatgpt.com/admin/api-reference#tag/Logs:-Codex

Example (from OpenAI's official VSCode extension):

```json
{
  "method": "initialize",
  "id": 0,
  "params": {
    "clientInfo": {
      "name": "codex_vscode",
      "title": "Codex VS Code Extension",
      "version": "0.1.0"
    }
  }
}
```

Example with notification opt-out:

```json
{
  "method": "initialize",
  "id": 1,
  "params": {
    "clientInfo": {
      "name": "my_client",
      "title": "My Client",
      "version": "0.1.0"
    },
    "capabilities": {
      "experimentalApi": true,
      "optOutNotificationMethods": ["thread/started", "item/agentMessage/delta"]
    }
  }
}
```

## API Overview

- `server/diagnostics` — experimental; read process-local memory measurements and registered diagnostic gauges.
- `thread/start` — create a new thread; emits `thread/started` (including the current `thread.status`) and auto-subscribes you to turn/item events for that thread. Experimental `projectId` assigns a durable thread to an existing project; ephemeral threads expose the same project identity in live responses without creating a stored/listable assignment. Experimental `historyMode` selects the persisted history contract: when omitted, durable threads use `"paginated"` if the active thread store supports `thread/turns/list` and `thread/items/list`, while ephemeral threads and stores without that support use `"legacy"`. When the request includes a `cwd` and the resolved sandbox is `workspace-write` or full access, app-server also marks that project as trusted in the user `config.toml`. Pass `sessionStartSource: "clear"` when starting a replacement thread after clearing the current session so `SessionStart` hooks receive `source: "clear"` instead of the default `"startup"`. Experimental `allowProviderModelFallback` lets providers backed by an authoritative static model catalog replace an unavailable requested `model` with the catalog default; dynamic or cached catalogs preserve the requested model. Experimental `runtimeWorkspaceRoots` supplies the runtime workspace roots used when app-server creates default environment selections; paths must be absolute. For permissions, prefer experimental `permissions` profile selection by id; the legacy `sandbox` shorthand is still accepted but cannot be combined with `permissions`. Deprecated experimental `multiAgentMode` is ignored; use Ultra reasoning effort for proactive multi-agent behavior. Experimental `environments` selects the sticky execution environments for turns on the thread; omit it to use the server default, pass `[]` to disable environments, or pass explicit environment ids with per-environment `cwd` and optional environment-native `runtimeWorkspaceRoots`. Explicit environments ignore the top-level roots; omitted per-environment roots default to that environment's `cwd`, while an empty list explicitly selects no roots. Experimental `selectedCapabilityRoots` selects environment-owned plugin or standalone-skill roots using environment-native absolute paths. Skills found below those roots are listed and read through the owning environment. Stdio MCP servers declared by selected plugins are started in that environment, and HTTP MCP connections use that environment's HTTP client.
  Experimental `persistence: "immediate"` makes a new thread's rollout durably discoverable before the response and `thread/started` notification. It cannot be combined with `ephemeral: true`; omitted or `"lazy"` preserves first-turn materialization.
- `thread/resume` — reopen an existing thread by id so subsequent `turn/start` calls append to it. When loading a saved thread, an omitted `cwd` uses the cwd from the latest retained settings snapshot explicitly owned by that thread, or its startup cwd if none exists. Older snapshots without an owner ID do not override the startup cwd. Resume does not read older history solely to recover cwd. Successful compaction checkpoints the current thread settings so they remain available within that replay window. An explicit `cwd` overrides that default. Accepts the same permission override rules as `thread/start`.
- `thread/fork` — fork an existing thread into a new thread id by copying the stored history; pass an optional `lastTurnId` to copy history only through that turn, inclusive, and drop later turns from the fork. An in-progress `lastTurnId` boundary is rejected. Experimental `beforeTurnId` instead copies history strictly before the referenced turn, including when that turn is in progress, and cannot be combined with `lastTurnId`. If both boundaries are null while the source thread is mid-turn, the fork records the same interruption marker as `turn/interrupt` instead of inheriting an unmarked partial turn suffix. The returned `thread.forkedFromId` points at the source thread when known. Accepts `ephemeral: true` for an in-memory temporary fork, emits `thread/started` (including the current `thread.status`), and auto-subscribes you to turn/item events for the new thread. Clients can pass `excludeTurns: true` when they plan to page fork history via `thread/turns/list` instead of receiving the full turn array immediately. Experimental `deferGoalContinuation: true` carries the source thread's current goal into the fork and runs an explicit turn before automatic continuation resumes. Deferred goal continuation is persisted until that turn starts and cannot be combined with `ephemeral: true`. Accepts the same permission override rules as `thread/start`.
- `thread/start`, `thread/resume`, and `thread/fork` responses include the legacy `sandbox` compatibility projection. `instructionSources` lists loaded instruction files using each source environment's native absolute path syntax, including files loaded from remote environments. Experimental clients can read `runtimeWorkspaceRoots` for the thread-scoped runtime roots and `activePermissionProfile` for the named or implicit built-in profile identity/provenance when known. Their deprecated experimental `multiAgentMode` field, and the corresponding thread setting, always report `explicitRequestOnly`; Ultra reasoning effort is the source of proactive multi-agent behavior.
- `thread/list` — page through stored threads; supports cursor-based pagination and optional `modelProviders`, `sourceKinds`, `archived`, `sectionId`, `cwd`, and `searchTerm` filters. Experimental `projectId` filters one project, while `null` selects unassigned threads. Set `sortKey` to `"section_position"` when listing a section in its persisted manual order. Experimental clients can use `parentThreadId` for direct spawned children or `ancestorThreadId` for spawned descendants at any depth; the two filters are mutually exclusive. Review and Guardian threads are not included because they do not participate in that spawn-edge lifecycle. Each returned `thread` includes `status` (`ThreadStatus`), defaulting to `notLoaded` when the thread is not currently loaded. Subagent threads also include `parentThreadId` when the immediate parent is known.
- `project/list`, `project/read`, `project/create`, `project/import`, `project/update`, `project/move`, and `project/delete` — experimental SQLite-backed project APIs. Projects have canonical server-generated IDs, persisted manual positions, ordered absolute roots, and an opaque string metadata bag. `project/move` places a project before another project or appends it when `beforeProjectId` is `null`. Create and import require an opaque `idempotencyKey`; clients should generate a UUID for ordinary creates and may use a stable namespaced legacy ID for migration. Reusing a key returns the original project without emitting notifications or repeating thread assignments, and keys remain reserved after deletion. Import can atomically assign existing thread IDs. Delete clears assignments but never deletes threads, directories, or files.
- `project/changed` and `thread/project/updated` — experimental notifications emitted after committed project or assignment changes. Reconnect with `project/list` and `thread/list` to recover authoritative state.
- `threadSection/list` — page through independently persisted thread sections, including their display names and optional `appearance` (`icon` and `color`).
- `threadSection/create` — create a durable custom section with a server-generated UUID, nonempty display name, and optional `appearance`; returns its `section`.
- `threadSection/update` — rename an existing custom section and optionally replace its `appearance`; omit appearance to preserve it or pass `null` to clear it. The built-in pinned section cannot be updated.
- `threadSection/delete` — delete an existing custom section and atomically return its member threads to the unsectioned list; returns `{}`. The built-in pinned section cannot be deleted.
- `thread/loaded/list` — list the thread ids currently loaded in memory.
- `thread/read` — read a stored thread by id without resuming it; optionally include turns via `includeTurns`. The returned `thread` includes `status` (`ThreadStatus`), defaulting to `notLoaded` when the thread is not currently loaded. For loaded threads, experimental clients can use `canAcceptDirectInput` to determine whether `turn/start` and `turn/steer` are accepted (`false` for parent-owned Multi-Agent V2 subagents); unloaded stored threads report `null` when that capability is unavailable.
- `thread/turns/list` — page through a stored thread’s turn history without resuming it; supports cursor-based pagination with `sortDirection`, `itemsView`, `nextCursor`, and `backwardsCursor`.
- `thread/items/list` — page through persisted thread items without resuming the thread. Pass `turnId` to restrict results to one turn, or omit it to page items across the thread. The active thread store must support item pagination.
- `thread/searchOccurrences` — experimental; find literal, case-insensitive matches in visible user messages and summary-selected final assistant messages within one paginated thread.
- `thread/metadata/update` — patch stored thread metadata in sqlite; supports updating persisted `gitInfo` fields, experimental `projectId`, and experimental `daybreakEnabled`, then returns the refreshed `thread`. Omit `projectId` to preserve assignment and pass an empty string to clear it.
- `thread/section/move` — atomically move a thread into the section identified by `sectionId`, before another thread or at the end when `beforeThreadId` is `null`. Reordering within the same section preserves `sectionEnteredAt`; entering a different section resets it. Set `sectionId` to `null` to remove the thread from its section. Returns `{}` on success.
- `thread/settings/update` — experimental; queue a partial update to a loaded thread’s next-turn settings without starting a turn or adding transcript items. Omitted fields leave settings unchanged; `serviceTier: null` clears the tier; deprecated `multiAgentMode` is ignored, while Ultra reasoning effort enables proactive multi-agent behavior; `sandboxPolicy` and `permissions` cannot be combined. Parent-owned Multi-Agent V2 subagents reject direct settings updates. Returns `{}` when the update is accepted and emits `thread/settings/updated` with the full effective settings only if they actually change. `turn/start` settings overrides emit the same notification when they change the stored settings.
- `thread/memoryMode/set` — experimental; set a thread’s persisted memory eligibility to `"enabled"` or `"disabled"` for either a loaded thread or a stored rollout; returns `{}` on success.
- `memory/reset` — experimental; clear the current `CODEX_HOME/memories` directory and reset persisted memory stage data in sqlite while preserving existing thread memory modes; returns `{}` on success.
- `thread/goal/set` — create or update the single persisted goal for a materialized thread; returns the current goal and emits `thread/goal/updated`. Parent-owned Multi-Agent V2 subagents reject goal updates, including while unloaded.
- `thread/goal/get` — fetch the current persisted goal for a materialized thread; returns `goal: null` when no goal exists. Available even for parent-owned Multi-Agent V2 subagents.
- `thread/goal/clear` — clear the current persisted goal for a materialized thread; returns whether a goal was removed and emits `thread/goal/cleared` when state changes. Parent-owned Multi-Agent V2 subagents reject goal clearing, including while unloaded.
- `thread/goal/updated` — notification emitted whenever a thread goal changes; includes the full current goal.
- `thread/goal/cleared` — notification emitted whenever a thread goal is removed.
- `thread/queue/add` — experimental; persist a user turn for automatic FIFO submission when the thread next becomes idle.
- `thread/queue/list` — experimental; return one page of a thread's queued turns.
- `thread/queue/update` — experimental; edit a queued turn while preserving its stable submission ID, client message ID, and position.
- `thread/queue/delete` — experimental; remove a queued turn by submission ID.
- `thread/queue/reorder` — experimental; replace the order of a thread's queued turns.
- `thread/queue/start` — experimental; start the queue head or a selected queued submission when the thread is idle.
- `thread/queue/changed` — experimental notification emitted with the changed `threadId`.
- `thread/settings/updated` — experimental notification emitted to subscribed clients when a loaded thread’s effective next-turn settings change; includes `threadId` and the full `threadSettings`.
- `thread/status/changed` — notification emitted when a loaded thread’s status changes (`threadId` + new `status`).
- `thread/archive` — move a thread’s rollout file into the archived directory and attempt to move any spawned descendant thread rollout files; returns `{}` on success and emits `thread/archived` for each archived thread.
- `thread/delete` — hard-delete an active or archived thread and any spawned descendant threads; returns `{}` on success and emits `thread/deleted` for each deleted thread.
- `thread/unsubscribe` — unsubscribe this connection from thread turn/item events. If this was the last subscriber, the server keeps the thread loaded and unloads it only after it has had no subscribers and no thread activity for 60 seconds by default (configured by `thread_unload_delay_secs`), runs `SessionEnd` hooks, then emits `thread/closed`.
- `thread/name/set` — set or update a thread’s user-facing name for either a loaded thread or a persisted rollout; returns `{}` on success and emits `thread/name/updated` to initialized, opted-in clients. Thread names are not required to be unique; name lookups resolve to the most recently updated thread.
- `thread/unarchive` — move an archived rollout file back into the sessions directory; returns the restored `thread` on success and emits `thread/unarchived`.
- `thread/compact/start` — trigger conversation history compaction for a thread; returns `{}` immediately while progress streams through standard turn/item notifications. Parent-owned Multi-Agent V2 subagents reject direct compaction requests.
- `thread/shellCommand` — run a user-initiated `!` shell command against a thread; this runs unsandboxed with full access rather than inheriting the thread sandbox policy. Parent-owned Multi-Agent V2 subagents reject direct shell commands. Returns `{}` immediately while progress streams through standard turn/item notifications and any active turn receives the formatted output in its message stream.
- `thread/approveGuardianDeniedAction` — manually approve a previously denied Guardian action; parent-owned Multi-Agent V2 subagents reject direct approvals. Replies to pending server-issued approval requests are unaffected.
- `thread/backgroundTerminals/clean` — terminate all running background terminals for a thread (experimental; requires `capabilities.experimentalApi`); returns `{}` when the cleanup request is accepted.
- `thread/backgroundTerminals/list` — list running background terminals for a loaded thread (experimental; requires `capabilities.experimentalApi`); returns `data` with the running terminal ids.
- `thread/backgroundTerminals/terminate` — terminate one running background terminal by app-server `processId` (experimental; requires `capabilities.experimentalApi`); returns whether a process was terminated.
- `thread/rollback` — deprecated and will be removed soon. Drop the last N turns from the agent’s in-memory context and persist a rollback marker in the rollout so future resumes see the pruned history; returns the updated `thread` (with `turns` populated) on success. Paginated threads do not support rollback. Parent-owned Multi-Agent V2 subagents reject direct rollback requests.
- `thread/revert` — replace a loaded paginated thread's durable history with the prefix strictly before `beforeTurnId` while preserving its thread id. The operation interrupts an active turn if needed, leaves older rollout files immutable, reloads the thread, returns updated thread metadata with empty `turns` plus pagination cursors, and emits `thread/reverted`. It does not revert local file changes. Parent-owned Multi-Agent V2 subagents reject direct revert requests.
- `turn/start` — add user input or a named standalone function-call output to a thread and begin Codex generation; responds with the initial `turn` object and streams `turn/started`, `item/*`, and `turn/completed` notifications. For standalone outputs, provide `toolOutput` with an empty `input` array. Optional `turnTrigger` classifies who or what started a new turn and is sent as `turn_trigger` in Responses request metadata; it is ignored if the request steers an active turn. `clientUserMessageId` is optional; when supplied, the corresponding `userMessage` item echoes it as `clientId`. Experimental `runtimeWorkspaceRoots` supplies the default roots for newly resolved environment selections. Explicit `environments[].runtimeWorkspaceRoots` override that fallback with environment-native absolute paths. Prefer experimental `permissions` profile selection by id for permission overrides; the legacy `sandboxPolicy` field is still accepted but cannot be combined with `permissions`. For `collaborationMode`, `settings.developer_instructions: null` means "use built-in instructions for the selected mode". Deprecated experimental `multiAgentMode` is ignored; Ultra reasoning effort selects proactive behavior. Parent-owned Multi-Agent V2 subagents reject direct turns.
- `thread/inject_items` — append raw Responses API items to a loaded thread’s model-visible history without starting a turn; returns `{}` on success. Parent-owned Multi-Agent V2 subagents reject direct item injection.
- `turn/settings/update` — experimental; publish a reviewer or model-settings patch to the exact live task identified by `threadId` and `turnId`, regardless of task kind. Model-settings updates require `step_model_switching`; reviewer-only updates do not. Returns `status: "applied"` or `status: "targetUnavailable"`, or a request error if rejected. Future-thread settings and already captured steps are unchanged. Parent-owned Multi-Agent V2 subagents reject direct settings updates.
- `turn/steer` — add user input to an already in-flight regular turn without starting a new turn; returns the active `turnId` that accepted the input. `clientUserMessageId` is optional; when supplied, the corresponding `userMessage` item echoes it as `clientId`. Review and manual compaction turns reject `turn/steer`. Parent-owned Multi-Agent V2 subagents reject direct steering.
- `turn/interrupt` — request cancellation of an in-flight turn by `(thread_id, turn_id)`; success is an empty `{}` response and the turn finishes with `status: "interrupted"`. Also available for parent-owned Multi-Agent V2 subagents.
- `thread/realtime/start` — start a thread-scoped realtime session (experimental); pass `outputModality: "text"` or `outputModality: "audio"` to choose model output, optionally pass `model` and `version` to override configured realtime selection for this session only, pass `includeStartupContext: false` to omit Codex's generated startup context, and optionally pass `initialItems` to seed V3 with complete role-bearing text messages at session creation. Pass `realtimeStartInstructions` and `realtimeEndInstructions` to control the developer instructions given to the backing Codex model when this session starts and ends. Version `"v1"` uses legacy Bidi `conversation.handoff.*`, `"v2"` uses the Realtime Voice API, and `"v3"` preserves V1 Codex Voice behavior while using Frameless Bidi `delegation.*`. For V3 automatic Codex text, `codexResponseHandoffMode` accepts `"thinking"` (the default; all output uses channel-less thinking appends), `"commentary"` (all output uses the commentary channel), or `"bemTags"` (the raw BEM envelope selects the API channel: BEM `analysis` and `commentary` use `commentary`, while BEM `final` and unparsable output use `speakable`). The BEM envelope remains in the appended text for the frontend model to interpret. V1 and V2 ignore this setting. For V3, pass `delegationAckFiller: false` to suppress the Realtime API's delegation acknowledgement filler or `true` to restore it; omitting the field preserves the Realtime API's default. V1 and V2 ignore `delegationAckFiller`. V3 handoffs do not prepend the legacy `"Agent Final Message"` label. Pass `clientManagedHandoffs: true` to disable automatic Codex response delivery so only the client's explicit append calls produce handoffs. Pass `codexResponsesAsItems: true` to send automatic Codex responses as realtime conversation items instead, and optionally pass `codexResponseItemPrefix` to prepend experiment instructions to those items. Returns `{}` and streams `thread/realtime/*` notifications. Omit `transport` for the websocket transport, or pass `{ "type": "webrtc", "sdp": "..." }` to create a Bidi WebRTC session from a browser-generated SDP offer; the remote answer SDP is emitted as `thread/realtime/sdp`. Conversation `version: "v2"` requests remain unsupported for WebRTC. Parent-owned Multi-Agent V2 subagents reject this request.
- `thread/realtime/appendAudio` — append an input audio chunk to the active realtime session (experimental); returns `{}`. Parent-owned Multi-Agent V2 subagents reject this request.
- `thread/realtime/appendText` — append text input to the active realtime session with a required `role` of `user`, `developer`, or `assistant` (experimental); returns `{}`. Older clients that omit `role` default to `user`. Parent-owned Multi-Agent V2 subagents reject this request.
- `thread/realtime/appendSpeech` — append text that the realtime model should speak to the user (experimental); returns `{}`. Parent-owned Multi-Agent V2 subagents reject this request.
- `thread/realtime/stop` — stop the active realtime session for the thread (experimental); returns `{}`. Parent-owned Multi-Agent V2 subagents reject this request.
- `thread/timeline/list` — page ordinary turn items, durable realtime facts, and turn boundaries together in rollout order (experimental). Entries are tagged `item`, `realtime`, `turnStarted`, or `turnCompleted`. Turn boundaries carry lifecycle metadata without duplicating the turn's items; completed boundaries also cover interrupted and failed turns. Each response contains an opaque continuation cursor and `activeRealtimeSessionAtPageStart`, allowing clients to render any bounded page without loading earlier thread history. Entries at the same rollout position have stable ordering and can span pages. Existing `thread/items/list` remains unchanged.
- `review/start` — kick off Codex’s automated reviewer for a thread; responds like `turn/start`. Inline reviews emit `item/started`/`item/completed` notifications with `enteredReviewMode` and `exitedReviewMode` items, plus a final assistant `agentMessage` containing the review. Detached delivery is deprecated and emits `deprecationNotice`; supported detached reviews still stream ordinary turn items on the new review thread. Parent-owned Multi-Agent V2 subagents reject both inline and detached reviews.
- `command/exec` — run a single command under the server sandbox without starting a thread/turn (handy for utilities and validation).
- `command/exec/write` — write base64-decoded stdin bytes to a running `command/exec` session or close stdin; returns `{}`.
- `command/exec/resize` — resize a running PTY-backed `command/exec` session by `processId`; returns `{}`.
- `command/exec/terminate` — terminate a running `command/exec` session by `processId`; returns `{}`.
- `command/exec/outputDelta` — notification emitted for base64-encoded stdout/stderr chunks from a streaming `command/exec` session.
- `process/spawn` — experimental; spawn a standalone process without the Codex sandbox on the host where the app server is running; returns after the process starts and emits `process/outputDelta` and `process/exited` notifications.
- `process/writeStdin` — experimental; write base64-decoded stdin bytes to a running `process/spawn` session or close stdin; returns `{}`.
- `process/resizePty` — experimental; resize a running PTY-backed `process/spawn` session by `processHandle`; returns `{}`.
- `process/kill` — experimental; terminate a running `process/spawn` session by `processHandle`; returns `{}`.
- `process/outputDelta` — experimental; notification emitted for base64-encoded stdout/stderr chunks from a streaming `process/spawn` session.
- `process/exited` — experimental; notification emitted when a `process/spawn` session exits.
- `fs/readFile` — read an absolute file path and return `{ dataBase64 }`.
- `fs/writeFile` — write an absolute file path from base64-encoded `{ dataBase64 }`; returns `{}`.
- `fs/createDirectory` — create an absolute directory path; `recursive` defaults to `true`.
- `fs/getMetadata` — return metadata for an absolute path: `isDirectory`, `isFile`, `isSymlink`, `createdAtMs`, and `modifiedAtMs`.
- `fs/readDirectory` — list direct child entries for an absolute directory path; each entry contains `fileName`, `isDirectory`, and `isFile`, and `fileName` is just the child name, not a path.
- `fs/remove` — remove an absolute file or directory tree; `recursive` and `force` default to `true`.
- `fs/copy` — copy between absolute paths; directory copies require `recursive: true`.
- `fs/watch` — subscribe this connection to filesystem change notifications for an absolute file or directory path and caller-provided `watchId`; returns the canonicalized `path`.
- `fs/unwatch` — stop sending notifications for a prior `fs/watch`; returns `{}`.
- `fs/changed` — notification emitted when watched paths change, including the `watchId` and `changedPaths`.
- `model/list` — list available models (set `includeHidden: true` to include entries with `hidden: true`), with model-advertised string reasoning effort options in the catalog's intended progression order, optional `modelSpecialty`, nullable `multiAgentVersion` (`disabled`, `v1`, or `v2`), `additionalSpeedTiers`, `serviceTiers`, optional `defaultServiceTier`, optional legacy `upgrade` model ids, optional `upgradeInfo` metadata (`model`, `upgradeCopy`, `modelLink`, `migrationMarkdown`, nullable informational `retirementAt` Unix timestamp), and optional `availabilityNux` metadata. Clients should preserve the `supportedReasoningEfforts` array order rather than deriving order from the effort names.
- `modelProvider/capabilities/read` — read provider-level capabilities for the currently configured model provider.
- `experimentalFeature/list` — list feature flags with stage metadata (`beta`, `underDevelopment`, `stable`, etc.), enabled/default-enabled state, and cursor pagination. Pass `threadId` when showing feature state for an existing loaded thread so `enabled` is computed from that thread's refreshed config, including project-local config for the thread's cwd; if omitted, the server uses its default config resolution context. For non-beta flags, `displayName`/`description`/`announcement` are `null`.
- `permissionProfile/list` — beta; list available permission profile ids with optional display `description` text and an `allowed` flag reflecting effective requirements, using cursor pagination. Pass `cwd` when the caller needs project-local `[permissions.<id>]` entries to be included in the current catalog view.
- `experimentalFeature/enablement/set` — patch the in-memory process-wide runtime feature enablement for currently supported feature keys. For each feature, precedence is: cloud requirements > --enable <feature_name> > config.toml > experimentalFeature/enablement/set (new) > code default. Invalid keys will be ignored.
- `environment/add` — experimental; add or replace a named remote environment by `environmentId` and `execServerUrl` for later selection by `thread/start` or `turn/start`; optional `connectTimeoutMs` overrides the WebSocket connection timeout; returns `{}` and does not change the default environment.
- `environment/info` — experimental; connect to a configured environment by `environmentId` and return its detected `shell` plus its default `cwd` as a canonical environment-native `file:` URI. Connection failures are returned as request errors.
- `environment/status` — experimental; read the current status for one configured `environmentId`. Ready remote environments are probed over their existing exec-server connection without starting or reconnecting environments; the response reports `ready`, `pending`, `disconnected`, or `unknown`.
- `thread/environment/connected` and `thread/environment/disconnected` — experimental; report exec-server connection transitions observed after thread startup for selected environments. Current connection state is not replayed.
- `collaborationMode/list` — list available collaboration mode presets (experimental, no pagination). Built-in presets do not select a model; the Plan preset selects medium reasoning effort. This response omits built-in developer instructions; clients should either pass `settings.developer_instructions: null` when setting a mode to use Codex's built-in instructions, or provide their own instructions explicitly.
- `skills/list` — list skills for one or more `cwd` values (optional `forceReload`).
- `skills/extraRoots/set` — replace the app-server process runtime extra standalone skill roots. The roots are not persisted; missing directories are accepted and simply load no skills.
- `hooks/list` — list discovered hooks for one or more `cwd` values.
- `marketplace/add` — add a remote plugin marketplace from an HTTP(S) Git URL, SSH Git URL, or GitHub `owner/repo` shorthand, then persist it into the user marketplace config. Returns the installed root path plus whether the marketplace was already present.
- `marketplace/remove` — remove a configured marketplace by name from the user marketplace config, and delete its installed marketplace root when one exists.
- `marketplace/upgrade` — upgrade all configured Git plugin marketplaces, or one named marketplace when `marketplaceName` is provided. Returns selected marketplace names, upgraded roots, and per-marketplace errors.
- `plugin/list` — list discovered plugin marketplaces and plugin state, including effective marketplace install/auth policy metadata, nullable remote install-policy provenance in `installPolicySource` (`WORKSPACE_SETTING` or `IMPLICIT_CANONICAL_APP`), the remote marketplace `version` and locally materialized `localVersion` when available, plugin `availability` (`AVAILABLE` by default or `DISABLED_BY_ADMIN` for remote plugins blocked upstream), fail-open `marketplaceLoadErrors` entries for marketplace files that could not be parsed or loaded, and best-effort `featuredPluginIds` for the official curated marketplace. Every `PluginSummary` returned by plugin list, installed, read, and share-list methods includes nullable `disabledReason` and `eligiblePlanTypes`, preserving plugin-service availability metadata and raw plan identifiers for remote plugins while returning `null` for local plugins or older remote responses. The same summaries include `mustShowInstallationInterstitial`: remote service values preserve `true` or `false`, while local plugins and remote responses that omit the policy return `null`. Clients should fail closed when the value is `null`. Clients can explicitly request the remote `workspace-directory`, `shared-with-me`, or `created-by-me-remote` marketplace kinds. Set `forceRefetch: true` to bypass TTL-backed remote catalog caches for the requested marketplaces and wait for fresh data; cache entries are replaced only after a successful fetch. When local marketplaces are included, the request also waits for configured plugin caches to reconcile before marketplace summaries are returned. At app-server startup, existing cached catalogs remain available to `plugin/list` while they refresh in the background. `interface.category` uses the marketplace category when present; otherwise it falls back to the plugin manifest category (**under development; do not call from production clients yet**).
- `plugin/search` — search the remote plugin service directly and combine matching local marketplace plugins into the first result page. Accepts a `searchTerm`, optional `global`, `workspace`, or `personal` scope, optional `cwds` for discovering repo marketplaces, and optional `cursor` and `limit`; `personal` searches user-owned plugins. Local matching uses plugin names, display names, and keywords, with case- and punctuation-insensitive relevance ordering. Global searches include applicable built-in local plugins, personal searches include other local plugins, workspace searches remain remote-only, and an omitted scope includes all local plugins. When the remote global catalog is active, it is authoritative and replaces the local curated marketplace. Local results remain available with API-key authentication and when `remote_plugin` is disabled; in the latter case, omitted-scope and explicit workspace searches can still query the remote workspace catalog, while explicit global and personal searches do not query plugin-service. The first page includes at most 100 local matches and can exceed `limit`; subsequent pages contain remote results only, and the upstream pagination token is passed through unchanged as `nextCursor`. Local and remote copies are deduplicated by shared remote identity, with the remote summary retaining local installed state. Every result always explicitly returns `plugin.enabled: false`, including enabled local plugins, deduplicated plugins, and later remote-only pages; search reports discovery metadata rather than effective activation. Use `plugin/list` or `plugin/read` to determine whether a plugin is actually enabled. When `plugin_sharing` is disabled, shared/private workspace results are omitted after the remote page is fetched (**under development; do not call from production clients yet**).
- `plugin/installed` — list installed plugin rows plus any explicitly requested local install-suggestion plugin names, without fetching the broader remote catalog. Remote rows include nullable `installPolicySource` and `installedAt`, the backend installation timestamp in Unix seconds. `installedAt` is also returned by `plugin/list`, `plugin/read`, and `plugin/share/list`; it is `null` for local plugins, uninstalled plugins, plugins installed by default, and older backend responses that do not include an installation timestamp. Mention surfaces can use this narrower view when they need plugin mention payloads rather than plugin-page discovery data (**under development; do not call from production clients yet**).
- `plugin/reconcile` — sync installed remote plugin bundles to match the latest plugin-service state. Blocks until synchronization and required hook updates finish, then returns `changedPlugins` with `hasMcps`, `hasApps`, `hasHooks`, and `hasSkills` refresh hints, including removals. Callers refresh MCP and Apps runtimes; plugin skills are picked up automatically on subsequent turns.
- `plugin/read` — read one plugin by `marketplacePath` plus `pluginName`, returning marketplace info, a list-style `summary`, manifest descriptions/interface metadata, and bundled skills/hooks/apps/MCP server names. Remote plugin details can include scheduled task summaries from the catalog; `scheduledTasks: null` means the metadata is unavailable, while an empty array means the catalog found no scheduled tasks. Remote plugin details expose the canonical `shareUrl` supplied by the remote catalog when available; it is `null` for local plugins or when the catalog omits it. This field is separate from `summary.shareContext`, which continues to describe user and workspace sharing state. For owned workspace plugins, `summary.shareContext.canPublishToWorkspace` reports whether the current user may add the plugin to the workspace directory; `plugin/share/save` returns the same capability after creating or updating a share, and clients should fail closed when either value is `null`. Remote skill interfaces expose `iconSmallUrl` and `iconLargeUrl` when the catalog supplies icon URLs. Returned plugin skills include their current `enabled` state after local config filtering; bundled hooks are returned as lightweight declaration summaries keyed for correlation with `hooks/list`. Use `plugin/install`'s `appsNeedingAuth` to drive post-install authentication and `app/list`'s `isAccessible` to determine current connector accessibility (**under development; do not call from production clients yet**).
- `plugin/skill/read` — read remote plugin skill markdown on demand by `remoteMarketplaceName`, `remotePluginId`, and `skillName`. This lets clients preview uninstalled remote plugin skills without downloading the plugin bundle.
- `skills/changed` — notification emitted when watched local skill files change.
- `app/installed` — read installed connector runtime state from the last committed snapshot, optionally refreshing it first.
- `app/list` — list available apps.
- `remoteControl/enable` — experimental; enable remote control for the current app-server process and return the current remote-control status snapshot. By default, any missing enrollment is completed before the response and the preference is persisted for the current app-server client scope. Pass `ephemeral: true` to enable remote control only for the current process without changing the persisted preference.
- `remoteControl/disable` — experimental; disable remote control for the current app-server process and return the current remote-control status snapshot. By default, the disabled preference is persisted for the current app-server client scope. Pass `ephemeral: true` to disable only for the current process without changing the persisted preference. This does not revoke already enrolled controller devices.
- `remoteControl/status/read` — experimental; read the current remote-control status snapshot. `status` is one of `disabled`, `connecting`, `connected`, or `errored`; `serverName` is the local machine name used by this app-server process; `environmentId` is a string when the app-server has a current enrollment and `null` when that enrollment is cleared, invalidated, or remote control is disabled.
- `remoteControl/pairing/start` — experimental; start a short-lived remote-control pairing artifact for the current app-server process. Pass `manualCode: true` to also request a manual pairing code. Returns `pairingCode`, `manualPairingCode`, `environmentId`, and Unix-seconds `expiresAt`; app-server intentionally does not expose the backend `serverId`.
- `remoteControl/pairing/status` — experimental; poll whether a remote-control `pairingCode` or `manualPairingCode` has been claimed. Pass exactly one of the two fields. Returns `claimed`.
- `remoteControl/client/list` — experimental; list controller devices granted access to an environment. Pass `environmentId` and optional `cursor`, `limit`, and `order`; returns picker-oriented client metadata plus `nextCursor`. This signed-in account-management operation works while the local relay is disabled or unenrolled.
- `remoteControl/client/revoke` — experimental; revoke one controller device's grant for an environment. Pass `environmentId` and `clientId`; returns an empty object. This signed-in account-management operation works while the local relay is disabled or unenrolled.
- `remoteControl/status/changed` — notification emitted when the remote-control status or client-visible environment id changes. `status` is one of `disabled`, `connecting`, `connected`, or `errored`; `serverName` is the local machine name used by this app-server process; `environmentId` is a string when the app-server has a current enrollment and `null` when that enrollment is cleared, invalidated, or remote control is disabled. Newly initialized app-server clients always receive the current status snapshot.
- `skills/config/write` — write user-level skill config by name or absolute path.
- `plugin/install` — install a plugin from a discovered marketplace entry, rejecting marketplace entries marked unavailable for install, install MCPs if any, and return the effective plugin auth policy plus any apps that still need auth. Local marketplace installation also reloads user configuration for loaded threads before invalidating their MCP runtimes and returning; this applies pending user-config changes, including hook settings. Reload failures are logged without undoing installation, and MCP startup can finish afterward. For remote installs, clients may include an optional `installAttemptId`; app-server forwards it unchanged as `install_attempt_id` in the backend POST body, while omission preserves the legacy empty-body request (**under development; do not call from production clients yet**).
- `plugin/uninstall` — uninstall a local plugin by `pluginId` in `<plugin>@<marketplace>` form by removing its cached files and clearing its user-level config entry, or uninstall a remote ChatGPT plugin by backend `pluginId` by forwarding the uninstall to the ChatGPT plugin backend and removing any downloaded remote-plugin cache (**under development; do not call from production clients yet**).
- `mcpServer/oauth/login` — start an OAuth login for a configured MCP server; pass `threadId` to resolve servers from that thread's selected plugins and executor, optionally pass `clientRegistration` (`auto`, `cimd`, or `dcr`) to override client registration for this login only, and receive an `authorization_url` followed by `mcpServer/oauthLogin/completed` once the browser flow finishes. Omitting `clientRegistration` automatically discovers the authorization server's supported registration methods; the override is never persisted in server configuration.
- `tool/requestUserInput` — prompt the user with 1–3 short questions for a tool call and return their answers (experimental).
- `config/mcpServer/reload` — reload MCP server config from disk and queue a refresh for loaded threads (applied on each thread's next active turn); returns `{}`. Use this after editing `config.toml` without restarting the server.
- `mcpServerStatus/list` — enumerate configured MCP servers with their tools, auth status, server info, owning `pluginId` (`null` for servers not contributed by a plugin), and nullable `runtimeStatus` from the current thread’s published connections, plus resources/resource templates for `full` detail; supports optional `threadId` and cursor+limit pagination. If `threadId` is omitted, the server reads from the latest global config directly and `runtimeStatus` is `null`. Runtime status is also `null` when the latest server registration differs from the thread’s published configuration. Runtime status is observed without starting or reconnecting the thread’s servers; it can be `notStarted`, `starting`, `connected`, `authenticationRequired`, `failed`, `cancelled`, or `disabled`. Inventory may be cached or collected separately and does not prove that the thread is connected. Each server also includes nullable `toolsError`: a startup or tool-list discovery failure is reported when no catalog is returned. Returned catalogs, including cached or empty catalogs, have no error. Healthy servers are returned even when another server fails. Older servers omit `toolsError`; clients must preserve their existing behavior when it is absent. Older servers omit `runtimeStatus`; clients should treat that as unknown. If `detail` is omitted, the server defaults to `full`. An `unknown` auth status means OAuth support could not be determined; `unsupported` means OAuth is known not to be supported.
- `mcpServer/resource/read` — read a resource from a configured MCP server by optional `threadId`, `server`, and `uri`, returning text/blob resource `contents`. Pass `originCallId` with `threadId` to scope a Codex app widget to the app and account of the completed tool call that produced it; successful scoped reads return the same `originCallId`. Optional `connectorId` restricts other hosted app resources to their originating connector. If `threadId` is omitted, the server reads from the latest MCP config directly.
- `mcpServer/event/stream/start` (experimental) — subscribe to an MCP event by `threadId`, `server`, `subscriptionId`, event `name`, `arguments`, and optional `_meta`.
- `mcpServer/event/stream/stop` (experimental) — stop the caller's event subscription by `subscriptionId`.
- `mcpServer/tool/call` — call a tool on a thread's configured MCP server by `threadId`, `server`, `tool`, optional `arguments`, and optional `_meta`, returning the MCP tool result. Parent-owned Multi-Agent V2 subagents reject direct tool calls.
- `windowsSandbox/setupStart` — start Windows sandbox setup for the selected mode (`elevated` or `unelevated`); accepts an optional absolute `cwd` to target setup for a specific workspace, returns `{ started: true }` immediately, and later emits `windowsSandbox/setupCompleted`.
  The default-off `windows_sandbox_service` feature enables attempting service provisioning for elevated setup. Clients can set it through `experimentalFeature/enablement/set` before starting setup; when disabled, setup uses the existing elevated helper directly.
- `feedback/upload` — submit a feedback report (classification + optional reason/logs, conversation_id, and optional `extraLogFiles` attachments array); returns the tracking thread id. With logs enabled, includes bounded recent failed Guardian review actions, decisions, and reviewer history from the reported thread and its descendants, linked to the reviewed turn and target item where available. Rollout selection preserves the reported thread and prioritizes children with retained failed reviews before newer children, including each selected thread's available Guardian trunk rollout. `feedback-thread-index.json` lists selected filenames and bounded omission details; it describes selection, not successful delivery. Failed-review captures are process-local, so missing evidence does not establish that no denial occurred.
- `config/read` — fetch the runtime-effective config after resolving config layering and managed requirements, including opaque `desktop` values stored in `config.toml`. When configured, the `packagedDefaults` layer has the lowest precedence.
- `externalAgentConfig/detect` — detect migratable external-agent artifacts with `includeHome`, optional `cwds`, and an optional `migrationSource` selector. Omitted, `null`, or unrecognized migration-source values retain the default behavior. The deprecated optional `source` field remains accepted for compatibility but does not select the migration source. Each detected item includes `cwd` (`null` for home), and multi-item migrations may additionally include structured `details` with plugin ids, skill names, memory, session metadata, or other artifact names. The response also includes connector candidates inferred from detected source sessions, with a normalized display `name`, the number of detected sessions that used the connector, and the source metadata field used for detection.
- `externalAgentConfig/import` — apply selected external-agent migration items by passing explicit `migrationItems` with `cwd` (`null` for home) and any `details` returned by detect. Pass the same optional `migrationSource` used for detection so the server reads from the matching source; omitted, `null`, or unrecognized values retain the default behavior. The optional `source` identifies the product that initiated the import, while the optional opaque `providerId` attributes analytics to the provider selected by that product without affecting migration-source selection. The response acknowledges the synchronous import phase with an `importId`. Expected migration failures are reported as per-item failures rather than JSON-RPC errors, so the server still returns that `importId` and emits `externalAgentConfig/import/completed` with the same ID once all synchronous and background work finishes. The completion notification contains type-level `itemTypeResults` with successes and failures, including raw failure messages for the client to report separately.
- `externalAgentConfig/import/readHistories` — read completed import histories and connector candidates detected from successfully imported session histories. Successful session entries include the original imported title when one was available. Connector candidates include a normalized display `name`, the number of imported sessions that used the connector, and the source metadata field used for detection.
- `config/value/write` — write a single config key/value to the user's config.toml on disk; dotted paths such as `desktop.someKey` use the same generic write surface. Writes that overlap a managed requirement are rejected with `configRequirementReadonly`.
- `config/batchWrite` — apply multiple config edits atomically to the user's config.toml on disk, with optional `reloadUserConfig: true` to hot-reload loaded threads, including multiple `desktop.*` edits. Session-static model, reasoning-effort, Plan-mode reasoning-effort, service-tier, and personality defaults do not reload existing threads.
- `configRequirements/read` — fetch loaded requirements constraints from `requirements.toml` and/or MDM (or `null` if none are configured), including exact managed values (`cliAuthCredentialsStore`, `chatgptBaseUrl`, `sqliteHome`, `logDir`, `modelCatalogJson`, `checkForUpdateOnStartup`, `allowLoginShell`, `feedback.enabled`, and `windowsSandboxPrivateDesktop`), requirements-only developer instructions (`additionalDeveloperInstructions`, supplied independently of ordinary developer instructions), allow-lists (`allowedApprovalPolicies`, `allowedSandboxModes`, `allowedWebSearchModes`), the layered permission-profile allow map (`allowedPermissionProfiles`), the managed permission-profile default (`defaultPermissions`), lifecycle hook lockdown (`allowManagedHooksOnly`), remote-control policy (`allowRemoteControl`; `false` force-disables remote control while `true` or `null` preserves existing behavior), the Browser/Computer Use umbrella policy (`allowBrowserAndComputerUse`), computer use policy (`computerUse`, including persistent approval, default application access, and per-platform application rules), Browser Use policy (`browserUse`, including WebMCP enablement, history access, origin rules, auto-review, and approval controls), interactive browser import policy (`inAppBrowser.allowExternalBrowserSettingsImport`), pinned feature values (`featureRequirements`, including the default-allowed `in_app_updates` policy that administrators can set to `false`), managed lifecycle hooks (`hooks`, including command handlers with optional `additionalContextLimit` and `mcp_tool` handlers with `server`, `tool`, `input`, `timeoutSec`, and `statusMessage`), `enforceResidency`, managed automatic review (`autoReview.requiredOnModels` and `autoReview.ignoreRules`), model defaults (`models.newThread.model`, `models.newThread.modelReasoningEffort`, and `models.newThread.serviceTier`), and `network` constraints such as canonical domain/socket permissions plus `managedAllowedDomainsOnly` and `dangerFullAccessDenylistOnly`.
  - Managed `[browser_use].allow_webmcp` is returned as `browserUse.allowWebmcp`, preserving `true`, `false`, and omission as `null`. Desktop clients enable WebMCP when their WebMCP Statsig gate is on **or** this field is `true`. A `false` or omitted policy does not disable a gate-enabled feature. When policy is omitted, enterprise and consumer defaults come from Statsig. This is a requirements-only field; ordinary `config.toml` settings cannot enable it through this API.

`mcpServer/resource/read` and `mcpServer/tool/call` preserve MCP protocol errors
with their original `code`, `message`, and `data`, including authentication
metadata in `data._meta`. Other operation failures retain the existing
internal-error response. Tool results with `isError: true` remain results,
including their `_meta`.

### Application requirements

With experimental API support enabled, `configRequirements/read` returns
`application.network` from managed requirements, separately from agent-network
policy in `network`. This endpoint reports policy; it does not enforce it.

```toml
[application.network.domains]
"managed.example.com" = "allow"
```

Application rules use normal managed TOML precedence. A present network block
is enabled by default and denies unlisted domains; `enabled = false` disables it.

### Plugin configuration scope

Plugin activation and MCP settings use the existing merged configuration, including
system settings and trusted project overrides. `skills/list` resolves plugin skills
independently for each requested working directory.

Sites migration persists an account/backend-scoped list of excluded bundled plugin IDs, not
remote installed metadata. Once remote Sites is installed and locally loadable, the shared
marketplace and runtime loaders exclude `sites@openai-bundled`. The exclusion survives restarts;
normal remote refresh remains authoritative and clears it when remote Sites is unavailable.
The bundled files and preference remain available for account changes or a missing replacement.
Direct local reads and installs of excluded bundled Sites return the existing plugin-not-found error.
Plugin Service implicitly installs eligible Sites; migration does not call install, ensure, enable,
or disable. Remote enablement stays authoritative, including when bundled preferences differ.
A successful check that remote Sites is unavailable is throttled for 60 seconds. Catalog requests
then skip blocking bundle synchronization; normal background synchronization continues unchanged.

For local `plugin/list` and `plugin/installed` results, each requested cwd supplies
its effective plugin state and plugin feature flag. When a plugin appears in multiple
contexts, the first source wins and installed/enabled state is merged across contexts.
Invalid project configurations are reported in `marketplaceLoadErrors` without hiding
other projects or remote plugins. Omitted or empty `cwds` exclude project
configuration, including the app-server process's project. `forceRefetch` refreshes the selected local plugin
sources before returning; ordinary listing schedules the same work in the background.
Remote catalog settings and feature gating remain request-wide rather than being
selected from the requested repos. Search continues to report `enabled: false`.

Marketplace definitions can come from system configuration. Startup synchronization
and `marketplace/upgrade` download or update configured Git marketplaces using the
merged source, ref, and sparse-path settings. Snapshot metadata stays with the
downloaded files; configuration is not copied into the user layer. Pure catalog
listing does not wait for missing snapshots to download.
Activation reloads configuration with the operation's original load settings and
rolls back if the marketplace definition changed or the reload fails. User files
ignored at startup remain ignored during this check.

`marketplace/remove` rejects removal when the marketplace name is defined in another
enabled layer of the operation's loaded config stack. Otherwise it removes the
snapshot and any base-user entry; a base-user entry is not required for cleanup.

### Example: Start or resume a thread

The shared `Thread` object includes nullable `model` and `reasoningEffort` fields,
including in `thread/read`, `thread/list`, and `thread/started`. Loaded threads report
their current configured settings; unloaded threads report the latest persisted
values. Unavailable legacy or filesystem-only values remain `null`, and an unset
reasoning effort is also `null`. These fields are not per-turn execution telemetry.
Use `thread/read` or `thread/list` to inspect them without resuming a thread,
subscribing to it, or dispatching queued work or goal continuations.

Start a fresh thread when you need a new Codex conversation.

Experimental `Thread.environments` returns a loaded thread's current selection as `{ environmentId, cwd, runtimeWorkspaceRoots }` entries.
The first entry is the primary environment; paths use that environment's native syntax.
An empty list means no environments are selected; `null` means the thread is not loaded or the server does not expose its selection.
Start and resume responses report the resulting live selection, and read, list, and unarchive responses include it for loaded threads, even if the client missed `thread/environment/connected`.
The field is not persisted and does not change executor selection or resume behavior.
Reading an unloaded thread leaves it unloaded and returns `null`; use `environment/status` to check connection status separately.

```json
{ "method": "thread/start", "id": 10, "params": {
    // Optionally set config settings. If not specified, will use the user's
    // current config settings.
    "model": "gpt-5.1-codex",
    "cwd": "/Users/me/project",
    "approvalPolicy": "never",
    "sandbox": "workspaceWrite",
    // Prefer experimental profile selection:
    // "permissions": ":workspace"
    // Experimental runtime roots for :workspace_roots materialization:
    // "runtimeWorkspaceRoots": ["/Users/me/project", "/Users/me/openai"],
    // Experimental capability roots selected by the hosting platform:
    "selectedCapabilityRoots": [
        {
            "id": "github@openai",
            "location": {
                "type": "environment",
                "environmentId": "workspace",
                "path": "/opt/cca/plugins/github"
            }
        }
    ],
    // Do not send both "sandbox" and "permissions".
    "personality": "friendly",
    "serviceName": "my_app_server_client", // optional metrics tag (`service_name`)
    "sessionStartSource": "startup", // optional: "startup" (default) or "clear"
    // Experimental: requires opt-in
    "dynamicTools": [
        {
            "type": "namespace",
            "name": "tickets",
            "description": "Ticket management tools",
            "tools": [
                {
                    "type": "function",
                    "name": "lookup_ticket",
                    "description": "Fetch a ticket by id",
                    "deferLoading": true,
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" }
                        },
                        "required": ["id"]
                    }
                }
            ]
        }
    ],
} }
{ "id": 10, "result": {
    "thread": {
        "id": "thr_123",
        "preview": "",
        "modelProvider": "openai",
        "createdAt": 1730910000
    }
} }
{ "method": "thread/started", "params": { "thread": { … } } }
```

Valid `personality` values are `"friendly"`, `"pragmatic"`, and `"none"`. When `"none"` is selected, the personality placeholder is replaced with an empty string.

To continue a stored session, call `thread/resume` with the `thread.id` you previously recorded. The response shape matches `thread/start`. When the stored session includes persisted token usage, the server emits `thread/tokenUsage/updated` immediately after the response so clients can render restored usage before the next turn starts. You can also pass the same configuration overrides supported by `thread/start`, including `approvalsReviewer`. On cold resume, approval policy and the active permission-profile ID select a source in this order: request override, latest persisted thread setting, current configured default. The persisted profile ID is resolved through the same config and requirements path as a `permissions` override. Threads without an active profile ID use current config instead of restoring their concrete historical permissions.

Cold resume loads configuration without holding the global metadata permit, allowing unrelated thread metadata updates and MCP requests to proceed during configuration loading. Before startup, it rechecks the resolved thread and reloads its history under the permit; if persisted configuration inputs changed, it reloads configuration as well. Requests serialized on the same thread retain their existing order.

Parent-owned Multi-Agent V2 children are an exception: `thread/resume` ignores configuration overrides and reattaches to the existing child. An unloaded child is reloaded through its actual, currently loaded parent using parent-derived configuration. If that owner-controlled reload cannot be performed, the request returns JSON-RPC error `-32600`; resume the parent first, or use `thread/read` or `thread/turns/list` to inspect the child's stored history without loading it. This policy follows the child's multi-agent runtime, including leaf workers whose models cannot delegate further.

By default, `thread/resume` includes the reconstructed turn history in `thread.turns`. Full-history hydration is deprecated for paginated threads and emits `deprecationNotice`; clients should pass `excludeTurns: true` to return only thread metadata and live resume state, then page with `thread/turns/list` and `thread/items/list`. A cold paginated resume can still replay persisted `thread/tokenUsage/updated` when it can identify the corresponding stored turn; resuming an already-loaded thread waits for the next live update.

Paginated threads keep the same resume contract as legacy threads. A default resume materializes the full projected history into `thread.turns`; `excludeTurns: true` keeps that array empty and includes `turnsBackwardsCursor` and `itemsBackwardsCursor` for the durable history visible at the resume boundary. Pass each cursor directly to its matching list API with `sortDirection: "desc"`; the first page includes the row identified by the cursor, while newer records arrive through live notifications. Either cursor is `null` when there is no durable row yet.

Only one app-server process can hold a paginated thread open for writing at a time. If another process already owns the thread, `thread/resume`, `thread/archive`, and `thread/delete` fail with JSON-RPC error `-32600`. Archive and deletion also fail if another process owns any spawned descendant. Read-only requests remain available without resuming the thread.

Experimental clients that want the live resume subscription plus a turns page in one round trip can pass `initialTurnsPage`. It accepts the same `limit`, `sortDirection`, and `itemsView` controls as `thread/turns/list`; omitted controls use its defaults. The response includes `initialTurnsPage` with `nextCursor` and `backwardsCursor` for follow-up pagination.

By default, resume uses the latest persisted `model` and `reasoningEffort` values associated with the thread. Supplying any of `model`, `modelProvider`, `config.model`, or `config.model_reasoning_effort` disables that persisted fallback and uses the explicit overrides plus normal config resolution instead.

Example:

```json
{ "method": "thread/resume", "id": 11, "params": {
    "threadId": "thr_123",
    "personality": "friendly"
} }
{ "id": 11, "result": { "thread": { "id": "thr_123", … } } }

{ "method": "thread/resume", "id": 12, "params": {
    "threadId": "thr_123",
    "excludeTurns": true
} }
{ "id": 12, "result": {
    "thread": { "id": "thr_123", "turns": [], … },
    "turnsBackwardsCursor": "turn-backwards-cursor-or-null",
    "itemsBackwardsCursor": "item-backwards-cursor-or-null"
} }

{ "method": "thread/resume", "id": 13, "params": {
    "threadId": "thr_123",
    "excludeTurns": true,
    "initialTurnsPage": {
        "limit": 20,
        "sortDirection": "desc",
        "itemsView": "summary"
    }
} }
{ "id": 13, "result": {
    "thread": { "id": "thr_123", "turns": [], … },
    "initialTurnsPage": {
        "data": [ ... ],
        "nextCursor": "older-turns-cursor-or-null",
        "backwardsCursor": "newer-turns-cursor-or-null"
    }
} }
```

To branch from a stored session, call `thread/fork` with the `thread.id`. This creates a new thread id and emits a `thread/started` notification for it. The returned `thread.sessionId` identifies the current live session tree root. Root threads use their own `thread.id` as `thread.sessionId`; stored threads that are not loaded also report their own `thread.id`, because resuming one makes it the root of a new live session tree. When the source history includes persisted token usage, the server also emits `thread/tokenUsage/updated` for the new thread immediately after the response. If the source thread is actively running, the fork snapshots it as if the current turn had been interrupted first. Pass `ephemeral: true` when the fork should stay in-memory only:

```json
{ "method": "thread/fork", "id": 12, "params": { "threadId": "thr_123", "ephemeral": true } }
{ "id": 12, "result": { "thread": { "id": "thr_456", "sessionId": "thr_456", … } } }
{ "method": "thread/started", "params": { "thread": { … } } }
```

Like `thread/resume`, full-history hydration is deprecated for paginated `thread/fork` and emits `deprecationNotice`. Clients should pass `excludeTurns: true` to return only thread metadata in `thread.turns` and page history with `thread/turns/list` and `thread/items/list`. Metadata-only forks do not replay restored `thread/tokenUsage/updated`. Ephemeral forks of paginated threads require `excludeTurns: true`.

### Effort-only context forks

Use the existing `thread/fork` configuration override. The receiving server owns
the new child; it can read a persisted parent from shared Codex storage without
owning the live parent. Only the live owner can supply unpersisted settings:

For new interactive actors, an explicit `codex --remote unix://` connection to
`codex app-server --listen unix://` avoids implicit embedded-server fallback.
Connect the controller to that same socket using the initialization handshake
above. An embedded interactive server without an exposed control transport cannot
be targeted externally by this RPC; neither another app-server process nor
launch-time options on `codex queue` change that live owner.

```json
{"method":"thread/fork","id":42,"params":{"threadId":"PARENT_UUID","excludeTurns":true,"config":{"model_reasoning_effort":"low"}}}
```

Omitting the effort override inherits the parent's selected effort, including a
committed thread-settings update before its next turn. Loaded parents supply
their owner snapshot; unloaded parents supply persisted metadata. Model and
provider also default to the parent. Explicit model/provider changes retain their
ordinary configuration semantics and are not an effort-only prefix guarantee.
All existing effort values and non-empty model-defined strings are accepted by
the configuration parser; consult `model/list.supportedReasoningEfforts` for the
selected model's advertised values. Empty values and wrong JSON types produce
configuration errors; backend rejection is reported as a turn error.
Custom values longer than 128 UTF-8 bytes fail before Lite inference so trusted
history controls stay bounded; this does not restrict ordinary non-Lite settings.

For Responses Lite models, ordinary inference records trusted
`configuration_update` items. The original request-level reasoning baseline and
inherited items remain unchanged; a different effort appends an update before
the child's first sampling request. Repeated settings append nothing. Legacy and
paginated storage preserve these trusted updates through ID-based resume.
The stable request baseline requires a parent that has already inferred with
this implementation. Older histories without an authored configuration update
cannot establish the old request baseline from the item stream alone.
Non-Lite models retain the ordinary request-level effort behavior; they do not
have this effort-change cache-prefix guarantee. There is no runtime cache-support
probe in this API. Pin the implementing revision; schema presence alone does not
establish these semantics on older servers.

The response's `thread.id` is the exact child target and `reasoningEffort` is its
selected setting, **not** an acknowledgement of inference. Forking does not run
a turn unless existing goal-continuation behavior requests one; use
`deferGoalContinuation: true` (experimental clients) when applicable. Start ordinary
work with `turn/start` or queue it for the returned child ID. With raw events
enabled, `rawResponseItem/completed` includes `threadId`, `turnId`, and the staged
configuration item. It reports history recording, not provider acceptance; use
the corresponding turn's completion/error for the inference outcome.

Fork requests are not idempotent: retrying a fork may create another child. This
does not add a live effort-control endpoint or an applied-inference event.
Unless `throughCallId` or `afterCallId` is supplied as described below, existing active-turn
snapshot/interrupt-boundary and tool-result repair semantics still apply.
Use a completed parent turn and unchanged model, tools, and base
instructions for exact-prefix comparisons. Compaction can replace the active
context under its existing rules. Structural request-prefix preservation is not
proof of provider cache reuse; cached/uncached token measurements and lineage
must be recorded separately. Cache routing affinity is retained across the fork lineage. This is a routing
hint; it does not guarantee provider cache reuse.

### Destination-owned invocation forks (experimental)

Launch the interactive CLI inside an already-prepared destination execution
environment, sharing the source's underlying Codex storage:

```sh
codex fork PARENT_UUID --destination-local --through-call CALL_ID \
  --host-dynamic-tools-socket /absolute/child.sock -C /child/worktree \
  -c 'model_reasoning_effort="low"'
```

Omit `-c` to inherit effort. `--destination-local` explicitly bypasses daemon
reuse, configured remote execution environments, and exec-server environment
selection, regardless of effort. It rejects a remote app-server connection.
It does not create a namespace: the launcher must prepare the namespace,
workspace, native-tool policy, and hosted socket before starting Codex.

The reusable boundary is `(PARENT_UUID, CALL_ID)`, where `CALL_ID` is the recorded
function/custom invocation's call ID, not a JSON-RPC request ID. Hosted invocation
recording is flushed before dispatch to the host. Siblings and recursive forks
can capture through that invocation while the source awaits its result, without
interrupting or completing the source turn. Later source appends do not move the
boundary. The full invocation and arguments belong to the inherited prefix;
future results do not. Any necessary child-only protocol closures are appended
after the prefix and explicitly do not report source failure or returned values.
The child must receive its own assignment; it must not replay the inherited call.

The underlying experimental `thread/fork` fields are `throughCallId`,
`requireClientReadiness`, and `expectedDynamicTools`. Call IDs must contain
1–256 UTF-8 bytes; missing/ambiguous IDs and combinations with turn boundaries
are rejected. Source deletion or a revert removing the boundary prevents capture.
`expectedDynamicTools` must exactly equal the inherited declarations, including
order, descriptions, schemas, and grammars. The CLI supplies the destination
registration for this check; only the endpoint may differ. No live-child migration
or source-owner unload is required.

Destination-local forks require readiness and defer inherited goal continuation.
The hosted endpoint receives `POST /v1/dynamic-tools/session` with
`{"protocolVersion":3,"threadId":"CHILD_UUID"}`. Bind that UUID to the prepared
environment and respond HTTP `204` within the current five-second control timeout.
Readiness must not depend on inference. Queueing assignment for that UUID before
the acknowledgment is supported: the durable queue retains it behind the gate.
After successful host attachment, the CLI calls the experimental RPC internally:

```json
{"method":"thread/ready","id":43,"params":{"threadId":"CHILD_UUID"}}
{"id":43,"result":{"threadId":"CHILD_UUID","ready":true}}
```

The RPC targets the child's current owner. Unknown or unavailable threads fail
explicitly. Repeated acknowledgments are idempotent. `ready: true` means input
admission is open, not that a provider request was sent or accepted. Acknowledging
an idle thread creates no input or inference. Existing queued work is reconsidered
on the queue watcher (currently up to ten seconds). Direct inference requests
before readiness fail explicitly. The persisted readiness requirement rearms on
ID-based resume; RPC controllers must acknowledge the new runtime after setup.
Ordinary CLI resume does not yet automatically perform this readiness handshake.

Fork creation itself is not idempotent: retries create distinct children. Record
the UUID from session attachment; an attachment timeout can leave a persisted,
gated child. Queue RPC clients should retain `clientUserMessageId` for assignment
retry deduplication. Pin the implementing revision: older binaries may ignore
the persisted readiness policy. Experimental RPC clients must enable
`experimentalApi` during initialization; inspect the matching schema and CLI
`fork --help` for interface discovery, not as proof of provider cache support.

Verification includes legacy and paginated RPC tests for pending-invocation
siblings, recursive capture, parent independence, effort, injection authority,
and readiness/resume. On Linux, run the real CLI namespace smoke test with a
local mock provider:

```sh
python3 scripts/test-destination-fork.py /absolute/path/to/codex
```

It uses separate mount namespaces and checks first native-tool output against
each child's worktree sentinel, while ancestor hosted calls remain unresolved.
It reports lineage and a structural prefix digest; auxiliary title-generation
requests are counted separately. These are not real-provider cache measurements.

Hosted namespace declarations may set `modelOnly: true` to retain their native
model tool surface and exclude code-mode wrapping, regardless of model tool-mode
defaults. The default is false. This is persisted declaration metadata and must
match across destination forks, like the namespace's tool definitions. Per-tool
`deferLoading` still controls whether model-only tools are initially visible or
discovered.

Hosted call requests distinguish `callId` (individual execution/reply correlation)
from nullable `contextCallId` (the recorded model invocation owning that execution).
For direct calls these match; nested code-mode calls carry the original `exec`
call ID as context provenance, including after a cell yields. Use `threadId` and
`contextCallId` for destination-owned invocation forks. Never substitute `callId`
when context provenance is unavailable. Older persisted events may lack it.

Invocation forks retain a durable `cache_affinity` record containing a routing
session UUID and a prompt cache key. The routing UUID is sent consistently in
transport session headers and top-level request `client_metadata.session_id`. Thread/session identities and runtime ownership remain independent.
Recursive forks and resumed children reuse that affinity; legacy metadata falls back
to the source session's original cache selection. Cache affinity is a routing
hint, not proof of a provider cache hit: measure the first child inference.

For routing diagnostics, enable `RUST_LOG=warn,codex_core::cache_routing=info`.
Events distinguish the actual thread/session IDs from the provider routing UUID
and prompt cache key. `CODEX_ROLLOUT_TRACE_ROOT=/absolute/trace-directory`
additionally records full request/response evidence through the existing rollout
trace facility. Inspect the first child response's usage, not a later warm turn.

### Listing projects

`project/list` accepts optional `sortKey` (`position` or `recencyAt`) and
`sortDirection` (`asc` or `desc`), alongside `limit` and the opaque `cursor`.
Omitting `sortKey` preserves manual position order. A non-null `sortDirection`
requires an explicit key; it defaults to `asc` for `position` and `desc` for `recencyAt`.

```json
{ "sortKey": "recencyAt", "sortDirection": "desc", "limit": 50, "cursor": null }
```

Every project response includes `recencyAt`: the newest non-archived, explicitly
assigned thread's recency in Unix seconds, across all sources, or `null` when none
exist. Like `thread/list`, thread recency starts at creation and advances at turn
start, not for background output. Removing or archiving members can lower project
recency. Task activity does not change project `updatedAt` or emit `project/changed`.

`model/list` also checks gateway authentication before returning cached models.
If authentication fails after the provider configuration changes, it asks the client
to restart Codex so the retained catalog and gateway sign-in use the same provider.

## Application network policy

Application policy uses the same managed TOML merge as agent-network requirements:
higher-priority layers override conflicting values, including `enabled` and each
domain permission, while non-conflicting domain entries are retained. Omitted
values inherit from lower layers. After merging, a present network block defaults
to `enabled = true` and an empty domain map, meaning no external destinations are
allowed. An effective `enabled = false` disables application destination policy.
Domain keys are exact ASCII names, normalized to lowercase without a trailing dot
before merging; wildcards, URLs, ports, invalid permissions, and duplicate
normalized names are rejected.
App-server enforces these rules for its HTTP and WebSocket traffic before route
resolution or connection work, including redirects and reused clients. An allow
entry permits only HTTPS or WSS to that exact host. Agent-network requirements
remain separate in `network`.

App-server reloads effective requirements on explicit config or account reloads.
Local changes or read failures discovered on reload revoke active requests;
unchanged requirements preserve them. Failed policy loads block traffic until
requirements load successfully. Invalid request or project configuration does not
revoke unrelated traffic. Account changes revoke
outstanding requests and clients retaining the previous account's authorization.
Policy updates also stop active requests to newly denied destinations. Narrow
authentication and requirements-discovery clients use local requirements and
exact endpoint URLs while workspace policy is loading. API-key-only deployments
do not discover ChatGPT workspace requirements.

SDK transports without destination enforcement, including OTLP exporters and AWS
credential discovery/signing, are disabled while restrictions apply. Supported
HTTP, WebSocket, and code-mode gRPC requests use the shared destination checks.
User-directed Git, SSH, shell, and other subprocess traffic retain their existing
execution and sandbox policies.

### Completed-tool forks

Use experimental `thread/fork.afterCallId` (CLI `fork --after-call CALL_ID`)
to inherit the source through the real tool result and all other results in its
open tool batch. An incomplete or ambiguous call is rejected. The stored boundary
excludes later parent messages; sibling forks share that exact prefix without
synthetic tool outputs. `afterCallId` conflicts with `throughCallId` and turn-based
boundaries. The older `throughCallId` still means capture through the invocation.

Hosted protocol version 3 requests `experimentalRawEvents` on start, fork, and
resume. After a durable, protocol-closed result batch, the client posts
`{ "protocolVersion": 3, "threadId": "UUID", "contextCallId": "CALL_ID" }`
to `/v1/dynamic-tools/completed`. Hosts must acknowledge idempotently. The client
makes up to three callback attempts with a 60-second timeout per attempt. If
settlement fails, the TUI keeps the session alive and disables hosted tool access
for the rest of that client session. Subsequent hosted calls return an explicit
unavailable error; other Codex tools remain usable. Pending completion identities
are retained, and reconnect does not automatically re-enable hosted tools.
A timeout does not prove that the host operation or child startup failed.
Session attachment also allows 60 seconds because it settles pending effects;
registration retains its five-second timeout. Hosts settle any
unacknowledged forks when the client reattaches; they must not guess that an
unrecorded result completed. Registration and session binding otherwise retain
the existing contract.
