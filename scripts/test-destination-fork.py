#!/usr/bin/env python3
"""Linux CLI fork smoke test with real mount namespaces and a local mock provider.

Run after building codex: python3 scripts/test-destination-fork.py /absolute/path/to/codex
No real provider traffic or cache-reuse measurement is performed.
"""

import argparse
import fcntl
import hashlib
import http.server
import json
import os
from pathlib import Path
import pty
import queue
import re
import select
import socketserver
import struct
import subprocess
import tempfile
import termios
import threading
import time


TOOLS = [
    {
        "type": "custom",
        "name": "hosted_eval",
        "description": "Resident host",
        "format": None,
    }
]


def wait(predicate, label, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(f"timed out: {label}")


class Rpc:
    def __init__(self, binary, env, cwd):
        self.process = subprocess.Popen(
            [binary, "app-server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            env=env,
            cwd=cwd,
        )
        self.messages = queue.Queue()
        self.serial = 0
        threading.Thread(target=self.read, daemon=True).start()
        self.call(
            "initialize",
            {
                "clientInfo": {"name": "destination-fork-test", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        self.send({"method": "initialized"})

    def read(self):
        for line in self.process.stdout:
            self.messages.put(json.loads(line))

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def call(self, method, params):
        self.serial += 1
        self.send({"id": self.serial, "method": method, "params": params})
        while True:
            message = self.messages.get(timeout=45)
            if message.get("id") == self.serial and "method" not in message:
                assert "error" not in message, message
                return message["result"]


class Provider(http.server.BaseHTTPRequestHandler):
    requests = []
    auxiliary_requests = 0
    code_mode = False
    by_actor = {}
    routing_headers = {}
    lock = threading.Lock()

    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        serialized = json.dumps(body["input"])
        actor = None
        for item in reversed(body["input"]):
            if item.get("role") == "user":
                texts = [part.get("text", "") for part in item.get("content", [])]
                assignments = [text for text in texts if text.startswith("ACTOR ")]
                if assignments:
                    actor = assignments[-1].split()[1]
                    break
        with self.lock:
            if actor is None:
                type(self).auxiliary_requests += 1
            else:
                self.requests.append(body)
                self.by_actor.setdefault(actor, []).append(body)
                self.routing_headers.setdefault(
                    actor,
                    {
                        "session-id": self.headers.get("session-id"),
                        "thread-id": self.headers.get("thread-id"),
                    },
                )
        if actor == "parent":
            item = {
                "type": "custom_tool_call",
                "call_id": "unfold_parent",
                "name": "hosted_eval",
                "input": "capture complete invocation arguments (a, b)",
            }
        elif actor is not None and f"native_{actor}" not in serialized:
            item = {
                "type": "function_call",
                "call_id": f"native_{actor}",
                "name": "exec_command",
                "arguments": json.dumps(
                    {
                        "cmd": "pwd; cat namespace-sentinel; readlink /proc/self/ns/mnt",
                        "max_output_tokens": 512,
                    }
                ),
            }
        elif actor == "low":
            item = {
                "type": "custom_tool_call",
                "call_id": "unfold_child",
                "name": "hosted_eval",
                "input": "recursive fork",
            }
        else:
            item = {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done"}],
            }
        if self.code_mode and item["type"] in ("custom_tool_call", "function_call"):
            if item["name"] == "hosted_eval":
                calls = [f"tools.hosted_eval({json.dumps(item['input'])})"]
                if actor == "parent":
                    calls.append('tools.hosted_eval("second independent execution")')
                code = "await Promise.all([" + ",".join(calls) + "]);"
            else:
                code = f"text(await tools.exec_command({item['arguments']}));"
            item = {
                "type": "custom_tool_call",
                "call_id": item["call_id"],
                "name": "exec",
                "input": '// @exec: {"yield_time_ms": 120000}\n' + code,
            }
        events = [
            {"type": "response.created", "response": {"id": f"response_{actor}"}},
            {"type": "response.output_item.done", "item": item},
            {
                "type": "response.completed",
                "response": {
                    "id": f"response_{actor}",
                    "usage": {
                        "input_tokens": 100,
                        "output_tokens": 10,
                        "total_tokens": 110,
                    },
                },
            },
        ]
        payload = "".join(f"data: {json.dumps(event)}\n\n" for event in events).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class Host(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def __init__(self, path):
        self.thread_id = None
        self.ready = threading.Event()
        self.pending = threading.Event()
        self.invocations = []
        super().__init__(str(path), HostHandler)
        threading.Thread(target=self.serve_forever, daemon=True).start()


class HostHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        payload = json.dumps(
            {"protocolVersion": 2, "dynamicTools": TOOLS, "scope": "primaryThread"}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path.endswith("/session"):
            self.server.thread_id = payload["threadId"]
            assert self.server.ready.wait(4), (
                "test must release readiness within the control timeout"
            )
            self.send_response(204)
            self.end_headers()
        else:
            self.server.invocations.append(payload)
            self.server.pending.set()
            # Keep the hosted invocation unresolved while descendants run.
            threading.Event().wait(120)


class Actor:
    def __init__(self, binary, env, root, workspace, name, source, call_id, effort):
        self.host = Host(root / f"{name}.sock")
        worktree = root / name
        worktree.mkdir()
        (worktree / "namespace-sentinel").write_text(name + "\n")
        master, slave = pty.openpty()
        self.master = master
        self.output = bytearray()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 140, 0, 0))
        command = [
            "unshare",
            "--user",
            "--map-root-user",
            "--mount",
            "--",
            "bash",
            "-c",
            'mount --bind "$1" "$2"; shift 2; exec "$@"',
            "fork-test",
            str(worktree),
            str(workspace),
            binary,
            "fork",
            source,
            "--destination-local",
            "--through-call",
            call_id,
            "--host-dynamic-tools-socket",
            str(root / f"{name}.sock"),
            "--no-alt-screen",
            "-C",
            str(workspace),
        ]
        if effort is not None:
            command += ["-c", f'model_reasoning_effort="{effort}"']
        self.process = subprocess.Popen(
            command, stdin=slave, stdout=slave, stderr=slave, env=env
        )
        os.close(slave)
        threading.Thread(target=self.drain, daemon=True).start()

    def drain(self):
        replies = {
            b"\x1b[6n": b"\x1b[1;1R",
            b"\x1b[?u": b"\x1b[?0u",
            b"\x1b]10;?": b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\",
            b"\x1b]11;?": b"\x1b]11;rgb:0000/0000/0000\x1b\\",
        }
        try:
            while self.process.poll() is None:
                if not select.select([self.master], [], [], 0.1)[0]:
                    continue
                chunk = os.read(self.master, 65536)
                self.output.extend(chunk)
                for query, reply in list(replies.items()):
                    if query in self.output:
                        os.write(self.master, reply)
                        del replies[query]
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("codex", type=lambda value: str(Path(value).resolve()))
    parser.add_argument("--code-mode", action="store_true")
    args = parser.parse_args()
    Provider.code_mode = args.code_mode
    subprocess.run(
        ["unshare", "--user", "--map-root-user", "--mount", "true"], check=True
    )
    actors = []
    source = None
    with tempfile.TemporaryDirectory(prefix="codex-destination-fork-") as temporary:
        root = Path(temporary)
        home = root / "home"
        workspace = root / "workspace"
        home.mkdir()
        workspace.mkdir()
        provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Provider)
        threading.Thread(target=provider.serve_forever, daemon=True).start()
        (home / "config.toml").write_text(
            'model="gpt-6-astra"\nmodel_provider="mock"\nmodel_reasoning_effort="high"\n'
            'approval_policy="never"\nsandbox_mode="danger-full-access"\n'
            + ("[features]\ncode_mode_only=true\n" if args.code_mode else "")
            + f'[model_providers.mock]\nname="Local test"\nbase_url="http://127.0.0.1:{provider.server_port}/v1"\n'
            'wire_api="responses"\nrequest_max_retries=0\nstream_max_retries=0\n'
            f'[projects.{json.dumps(str(workspace))}]\ntrust_level="trusted"\n'
        )
        env = dict(
            os.environ,
            CODEX_HOME=str(home),
            TERM="xterm-256color",
            OPENAI_API_KEY="local-test-only",
        )
        try:
            source = Rpc(args.codex, env, workspace)
            parent = source.call(
                "thread/start", {"dynamicTools": TOOLS, "cwd": str(workspace)}
            )["thread"]["id"]
            source.call(
                "turn/start",
                {
                    "threadId": parent,
                    "input": [{"type": "text", "text": "ACTOR parent"}],
                },
            )
            wait(lambda: Provider.requests, "parent inference")
            wait(
                lambda: any(
                    "unfold_parent" in path.read_text()
                    for path in home.glob("sessions/**/*.jsonl")
                ),
                "durable invocation",
            )
            pending_calls = []
            while len(pending_calls) < (2 if args.code_mode else 1):
                message = source.messages.get(timeout=45)
                if message.get("method") == "item/tool/call":
                    pending_calls.append(message["params"])
                    if args.code_mode and len(pending_calls) == 1:
                        # Hosted tools serialize; settle the first nested execution while
                        # the enclosing exec remains unresolved in its second invocation.
                        source.send(
                            {
                                "id": message["id"],
                                "result": {
                                    "contentItems": [
                                        {"type": "inputText", "text": "first settled"}
                                    ],
                                    "success": True,
                                },
                            }
                        )
            assert all(
                call["contextCallId"] == "unfold_parent" for call in pending_calls
            ), pending_calls
            assert len({call["callId"] for call in pending_calls}) == len(pending_calls)
            if args.code_mode:
                assert all(
                    call["callId"] != call["contextCallId"] for call in pending_calls
                )
            prefixes = []
            sentinels = []
            namespaces = []
            for name, effort, ancestor, call in [
                ("inherited", None, parent, "unfold_parent"),
                ("low", "low", parent, "unfold_parent"),
                ("recursive", "medium", None, "unfold_child"),
            ]:
                if ancestor is None:
                    wait(
                        lambda: actors[-1].host.pending.is_set(),
                        "child hosted call remains pending",
                    )
                    ancestor = actors[-1].host.thread_id
                    invocation = actors[-1].host.invocations[0]
                    assert invocation["contextCallId"] == call, invocation
                    if args.code_mode:
                        assert invocation["callId"] != call, invocation
                actor = Actor(
                    args.codex, env, root, workspace, name, ancestor, call, effort
                )
                actors.append(actor)
                thread_id = wait(
                    lambda: actor.host.thread_id, f"{name} session registration"
                )
                before = len(Provider.requests)
                source.call(
                    "thread/queue/add",
                    {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": f"ACTOR {name}"}],
                        "clientUserMessageId": name,
                    },
                )
                assert len(Provider.requests) == before, (
                    "queue bypassed hosted readiness"
                )
                actor.host.ready.set()
                wait(
                    lambda: len(Provider.by_actor.get(name, [])) >= 2,
                    f"{name} first native execution",
                )
                first, continuation = Provider.by_actor[name][:2]
                index = next(
                    i
                    for i, item in enumerate(first["input"])
                    if item.get("call_id") == call
                    and item["type"] == "custom_tool_call"
                )
                if name != "recursive":
                    parent_request = Provider.by_actor["parent"][0]
                    assert (
                        first["input"][: len(parent_request["input"])]
                        == parent_request["input"]
                    ), "child changed the sampled parent prefix"
                    assert first.get("tools") == parent_request.get("tools"), (
                        "child changed request-level tools"
                    )
                    assert first.get("instructions") == parent_request.get(
                        "instructions"
                    ), "child changed request-level instructions"
                    prefixes.append(first["input"][: index + 1])
                outputs = [
                    item
                    for item in continuation["input"]
                    if item.get("call_id") == f"native_{name}"
                    and item["type"]
                    == (
                        "custom_tool_call_output"
                        if args.code_mode
                        else "function_call_output"
                    )
                ]
                assert outputs, {
                    "keys": list(continuation),
                    "model": continuation.get("model"),
                    "input": [
                        (
                            item.get("type"),
                            item.get("role"),
                            item.get("call_id"),
                            str(item.get("content", item.get("output", "")))[-240:],
                        )
                        for item in continuation["input"]
                    ],
                }
                output = json.dumps(outputs)
                assert str(workspace) in output and name in output, output
                assert "mnt:[" in output, output
                namespaces.append(re.search(r"mnt:\[\d+\]", output).group())
                sentinels.append(output)
                updates = [
                    item
                    for item in first["input"]
                    if item["type"] == "configuration_update"
                ]
                assert updates[-1]["reasoning"]["effort"] == (effort or "high"), updates
            assert prefixes[0] == prefixes[1], (
                "siblings did not inherit an identical prefix"
            )
            cache_keys = {
                request[0].get("prompt_cache_key")
                for request in Provider.by_actor.values()
            }
            assert cache_keys == {Provider.by_actor["parent"][0]["prompt_cache_key"]}, (
                cache_keys
            )
            routing_id = Provider.routing_headers["parent"]["session-id"]
            for actor, requests in Provider.by_actor.items():
                assert Provider.routing_headers[actor]["session-id"] == routing_id
                metadata = requests[0]["client_metadata"]
                assert metadata["session_id"] == routing_id
                assert (
                    Provider.routing_headers[actor]["thread-id"]
                    == metadata["thread_id"]
                )
            assert (
                len(
                    {
                        headers["thread-id"]
                        for headers in Provider.routing_headers.values()
                    }
                )
                == 4
            )
            assert len(set(sentinels)) == len(sentinels)
            assert len(set(namespaces)) == len(namespaces), namespaces
            print(
                json.dumps(
                    {
                        "sourceThread": parent,
                        "throughCallId": "unfold_parent",
                        "prefixSha256": hashlib.sha256(
                            json.dumps(prefixes[0], sort_keys=True).encode()
                        ).hexdigest(),
                        "nativeOutputs": sentinels,
                        "auxiliaryRequests": Provider.auxiliary_requests,
                        "providerCacheReuse": "unverified: mock provider",
                        "promptCacheKeys": {
                            name: requests[0].get("prompt_cache_key")
                            for name, requests in Provider.by_actor.items()
                        },
                    },
                    indent=2,
                )
            )
        except Exception:
            with tempfile.NamedTemporaryFile(
                mode="w", prefix="codex-fork-failure-", suffix=".json", delete=False
            ) as report:
                json.dump(Provider.by_actor, report)
                print(f"Provider request evidence: {report.name}")
            raise
        finally:
            for actor in actors:
                if actor.process.poll() is None:
                    actor.process.terminate()
                actor.process.wait(timeout=10)
                os.close(actor.master)
                if not actor.host.thread_id:
                    print(actor.output.decode(errors="replace"))
                actor.host.server_close()
            if source is not None:
                source.process.terminate()
                source.process.wait(timeout=10)
            provider.shutdown()


if __name__ == "__main__":
    main()
