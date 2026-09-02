# Dynamic Custom Tools for Codex App Server

This guide is for external agents and host applications that want Codex models to emit raw source code or other free-form text into a host-owned tool.

Dynamic custom tools use the OpenAI Responses API `custom` tool mechanism while retaining the normal Codex runtime: threads, built-in tools, approvals, persistence, compaction, streaming, and turn continuation.

The feature is experimental. It is an App Server extension, not an MCP integration or a separate agent loop.

## Why use a custom tool?

A function dynamic tool requires the model to produce JSON arguments:

```json
{"program":"main = putStrLn \"hello\"\n"}
```

A custom dynamic tool lets the model produce the payload directly:

```haskell
main = putStrLn "hello"
```

The App Server's JSON-RPC transport still serializes that payload as a JSON string when calling the host, but the model does not generate a JSON object, wrapper property, or separately quoted JSON string.

## 1. Enable experimental APIs

Declare experimental API support when initializing the App Server connection:

```json
{
  "method": "initialize",
  "id": 1,
  "params": {
    "clientInfo": {
      "name": "example-host",
      "version": "1.0.0"
    },
    "capabilities": {
      "experimentalApi": true
    }
  }
}
```

## 2. Register a custom dynamic tool

Supply the tool in `dynamicTools` on `thread/start`:

```json
{
  "method": "thread/start",
  "id": 2,
  "params": {
    "dynamicTools": [
      {
        "type": "custom",
        "name": "tidepool",
        "description": "Evaluate a Tidepool Haskell program",
        "deferLoading": false
      }
    ]
  }
}
```

Custom tools do not have an `inputSchema`. When `format` is omitted, Codex registers an unrestricted, non-empty free-form text grammar.

The tool name must:

- Match `^[a-zA-Z0-9_-]+$`.
- Contain at most 128 characters.
- Not collide with built-in, MCP, or other registered tools.
- Not be `mcp` or begin with `mcp__`.

## 3. Let the model call the tool

Start a turn normally. The model sees `tidepool` as a Responses API custom tool and can emit ordinary source text as its input.

Conceptually, the model response item is:

```json
{
  "type": "custom_tool_call",
  "call_id": "call_456",
  "name": "tidepool",
  "input": "module Main where\n\nmain = putStrLn \"hello\"\n"
}
```

The value of `input` above represents the Responses wire format. The model-generated tool payload itself is the Haskell source, not JSON.

## 4. Handle the host callback

When Codex receives the custom tool call, the App Server sends the existing `item/tool/call` JSON-RPC request to the host:

```json
{
  "method": "item/tool/call",
  "id": 61,
  "params": {
    "threadId": "thr_123",
    "turnId": "turn_123",
    "callId": "call_456",
    "namespace": null,
    "tool": "tidepool",
    "arguments": "module Main where\n\nmain = putStrLn \"hello\"\n"
  }
}
```

For custom tools, `params.arguments` is a JSON string. After JSON-RPC decoding, its string value is the exact model payload, including:

- Newlines and indentation.
- Quotes and nested source strings.
- Backslashes.
- Unicode.
- Text that happens to look like JSON.

Do not parse this value as function arguments and do not expect an object such as `{ "input": ... }` or `{ "program": ... }`. Pass the decoded string directly to the execution environment.

For existing function dynamic tools, `params.arguments` remains a JSON object. Hosts supporting both variants should branch on the registered tool definition or on whether `arguments` is a string or object.

The App Server also emits `item/started` and `item/completed` lifecycle notifications for the dynamic tool call.

## 5. Return the execution result

Answer the JSON-RPC request using the request's JSON-RPC `id`:

```json
{
  "id": 61,
  "result": {
    "contentItems": [
      {
        "type": "inputText",
        "text": "Program completed successfully."
      }
    ],
    "success": true
  }
}
```

Supported result content items are:

- `inputText` with `text`.
- `inputImage` with an inline data URL in `imageUrl`.
- `inputAudio` with an inline data URL in `audioUrl`.

Codex pairs the result with the original `callId`, inserts it into the next model request as `custom_tool_call_output`, and continues the turn normally.

The JSON-RPC request `id` and the model tool `callId` serve different purposes:

- Respond to the host callback using the JSON-RPC `id` (`61` above).
- Codex internally preserves the model `callId` (`call_456` above) when constructing the custom-tool output.

## Optional grammar format

To constrain the free-form payload, supply a Responses-compatible custom-tool format:

```json
{
  "type": "custom",
  "name": "tidepool",
  "description": "Evaluate a Tidepool Haskell program",
  "deferLoading": false,
  "format": {
    "type": "grammar",
    "syntax": "lark",
    "definition": "start: SOURCE\nSOURCE: /[\\s\\S]+/"
  }
}
```

Omit `format` when unconstrained raw text is sufficient. A grammar constrains what the model may emit; it does not wrap the resulting payload in JSON.

## Namespaces and deferred loading

Custom and function tools can coexist in a dynamic namespace:

```json
{
  "type": "namespace",
  "name": "languages",
  "description": "Language execution environments",
  "tools": [
    {
      "type": "custom",
      "name": "haskell",
      "description": "Execute Haskell source",
      "deferLoading": false
    },
    {
      "type": "custom",
      "name": "idris",
      "description": "Execute Idris source",
      "deferLoading": true
    }
  ]
}
```

For a namespaced callback, `namespace` contains the namespace name and `tool` contains the child tool name.

Deferred tools must belong to a namespace. A deferred tool remains registered but is omitted from the ordinary model-facing tool list until discovered through tool search. Use deferred loading only when the selected model/runtime supports tool search.

Namespace names must match `^[a-zA-Z0-9_-]+$`, contain at most 64 characters, and avoid reserved Responses namespaces.

## Persistence and resume

Dynamic custom-tool definitions are stored in thread rollout metadata. A later `thread/resume` restores them automatically; the host should not send a replacement definition during resume.

The host must still be prepared to handle `item/tool/call` callbacks after reconnecting to a resumed thread.

## Host implementation checklist

1. Enable `experimentalApi` during initialization.
2. Register a `type: "custom"` dynamic tool on `thread/start`.
3. Retain the registered kind for each namespace/tool pair.
4. On `item/tool/call`, treat `arguments` as raw decoded text for custom tools.
5. Never add a JSON wrapper before passing the text to the execution environment.
6. Respond using the callback request's JSON-RPC `id`.
7. Keep handling callbacks after `thread/resume`.

## Experimental TUI host bridge

A local orchestrator can provide dynamic tools to the normal interactive Codex TUI without running
an MCP server. Start an HTTP/1.1 service on an actor-scoped Unix-domain socket, then launch Codex
with:

```text
codex --host-dynamic-tools-socket /absolute/private/actor/dynamic-tools.sock
```

This v1 bridge is available on Linux and macOS, is restricted to the bootstrap primary thread, and
uses HTTP only for framing over the Unix socket. Codex never creates or removes the socket. The host
must bind it before launch in an owner-only directory and retire the endpoint with the actor.

The service implements three routes:

```text
GET  /v1/dynamic-tools/registration
POST /v1/dynamic-tools/session
POST /v1/dynamic-tools/call
```

Registration returns the dynamic tool definitions before Codex starts its App Server:

```json
{
  "protocolVersion": 1,
  "dynamicTools": [
    {
      "type": "custom",
      "name": "tidepool",
      "description": "Evaluate a Tidepool Haskell program",
      "deferLoading": false
    }
  ],
  "scope": "primaryThread"
}
```

After `thread/start`, `thread/resume`, or the initial CLI `thread/fork`, Codex attaches the resulting
thread before submitting its first turn:

```json
{"protocolVersion":1,"threadId":"019..."}
```

The endpoint returns HTTP 204 only after validating that the thread belongs to the actor. The
operation must be idempotent for the same thread. Codex repeats registration validation and session
attachment after an App Server reconnect. Cold resume relies on the rollout's persisted dynamic-tool
definitions being compatible with the current registration.

Calls contain the existing callback fields plus the bridge version:

```json
{
  "protocolVersion": 1,
  "threadId": "019...",
  "turnId": "turn-123",
  "callId": "call-456",
  "namespace": null,
  "tool": "tidepool",
  "arguments": "module Main where\n\nmain = putStrLn \"hello\"\n"
}
```

For a custom tool, `arguments` is the exact decoded model payload. Return HTTP 200 with the same
`DynamicToolCallResponse` shape shown above. A compiler or evaluator rejection is a normal domain
result: return `success: true` with a receipt whose status is `rejected` and include its diagnostics.
Use `success: false` for infrastructure failures.

Codex never retries `/call`. If the socket closes after execution begins, the outcome is
indeterminate and the model must not assume that no effects occurred. Later foreground threads,
background agents, helper threads, and derived forks do not receive this authority in v1.
