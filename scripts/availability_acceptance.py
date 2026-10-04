#!/usr/bin/env python3
"""Fixture-only Codex error/continuation acceptance. No real credentials or models.

Run with --codex-bin /absolute/path/to/codex. Temporary homes are removed.
The proxy refuses non-loopback traffic; this is protocol evidence, not a claim
that authenticated production quota helpers have zero network side effects.
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import socket
import time
import urllib.request
from contextlib import contextmanager


def sse(event: dict) -> bytes:
    return ("data: " + json.dumps(event) + "\n\n").encode()


class Upstream:
    def __init__(self, scenario: str):
        self.scenario = scenario
        self.requests: list[dict] = []
        self.recover = False
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 16 * 1024 * 1024:
                    self.send_error(413)
                    return
                body = json.loads(self.rfile.read(length))
                owner.requests.append(body)
                response = {"id": "resp_fixture", "object": "response", "model": "gpt-5",
                            "status": "completed", "output": [],
                            "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}}
                output = {"id": "msg_fixture", "type": "message", "role": "assistant",
                          "status": "completed", "content": [{"type": "output_text", "text": "EMP_OK"}]}
                status = 200
                content_type = "text/event-stream"
                if owner.recover:
                    response["output"] = [output]
                    payload = sse({"type": "response.output_item.done", "output_index": 0, "item": output})
                    payload += sse({"type": "response.completed", "response": response})
                elif owner.scenario == "tool_then_exhausted" and len(owner.requests) == 1:
                    tools = body.get("tools", [])
                    names = {t.get("name") for t in tools}
                    name = "exec_command" if "exec_command" in names else "shell"
                    args = ({"cmd": "printf EMP_TOOL_MARKER", "max_output_tokens": 20}
                            if name == "exec_command" else {"command": ["/usr/bin/printf", "EMP_TOOL_MARKER"], "timeout_ms": 1000})
                    item = {"type": "function_call", "id": "fc_fixture", "call_id": "call_fixture",
                            "name": name, "arguments": json.dumps(args), "status": "completed"}
                    response["output"] = [item]
                    payload = sse({"type": "response.output_item.done", "output_index": 0, "item": item})
                    payload += sse({"type": "response.completed", "response": response})
                elif owner.scenario == "interrupted":
                    payload = sse({"type": "response.created", "response": {**response, "status": "in_progress"}})
                    payload += sse({"type": "response.output_text.delta", "delta": "EMP_PARTIAL", "output_index": 0, "content_index": 0})
                else:
                    status = 429
                    content_type = "application/json"
                    payload = json.dumps({"error": {"type": "usage_limit_reached", "code": "usage_limit_reached",
                        "message": "Selected source quota is exhausted; wait for a quota recheck.", "resets_at": 2000000000}}).encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@contextmanager
def gateway(binary: Path, root: Path, upstream: Upstream, recovery: Upstream):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    home = root / "emp-home"
    home.mkdir()
    (home / ".codex").mkdir()
    (home / ".codex" / "auth.json").write_text(json.dumps({"tokens":{"access_token":"fixture-only","account_id":"fixture-caller"}}))
    config = root / "emp.json"
    config.write_text(json.dumps({"native_catalog_path": str(home / "models.json"),
        "providers": [{"id": name, "name": name, "base_url": f"http://127.0.0.1:{source.server.server_port}/v1",
                       "protocol": "responses", "auth_mode": "api_key", "api_key": "fixture-only"}
                      for name, source in [("fixture", upstream), ("recovery", recovery)]],
        "models": [{"id": f"{name}/model", "provider": name, "upstream_id": "gpt-5", "enabled": True}
                   for name in ["fixture", "recovery"]]}))
    env = {"PATH": os.defpath, "HOME": str(home), "CODEX_HOME": str(home / ".codex"),
           "HTTP_PROXY": "http://127.0.0.1:1", "HTTPS_PROXY": "http://127.0.0.1:1",
           "ALL_PROXY": "http://127.0.0.1:1", "NO_PROXY": "127.0.0.1,localhost"}
    process = subprocess.Popen([str(binary), "serve", "--config", str(config), "--port", str(port)],
                               env=env, cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        url = f"http://127.0.0.1:{port}/v1"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError("Fixture EMP exited before readiness")
            try:
                with opener.open(url + "/models", timeout=0.2) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Fixture EMP readiness timed out")
        yield url
    finally:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run(binary: Path, emp: Path, scenario: str) -> dict:
    upstream = Upstream(scenario)
    recovery = Upstream("recovery")
    recovery.recover = True
    try:
        with tempfile.TemporaryDirectory(prefix="emp-protocol-") as directory, gateway(emp, Path(directory), upstream, recovery) as endpoint:
            root = Path(directory).resolve()
            home, work = root / "home", root / "work"
            home.mkdir()
            work.mkdir()
            (home / "config.toml").write_text(
                'model = "fixture/model"\nmodel_provider = "fixture"\napproval_policy = "never"\nsandbox_mode = "read-only"\n'
                '[features]\nplugins = false\n'
                '[model_providers.fixture]\nname = "Fixture"\nwire_api = "responses"\nenv_key = "OPENAI_API_KEY"\n'
                f'base_url = "{endpoint}"\nsupports_websockets = false\n'
            )
            env = {"PATH": os.defpath, "HOME": str(home), "CODEX_HOME": str(home), "OPENAI_API_KEY": "fixture-only",
                   "HTTP_PROXY": "http://127.0.0.1:1", "HTTPS_PROXY": "http://127.0.0.1:1",
                   "ALL_PROXY": "http://127.0.0.1:1", "NO_PROXY": "127.0.0.1,localhost", "RUST_LOG": "off"}

            def call(resume: bool):
                args = [str(binary), "exec"]
                if resume:
                    args += ["resume", "--last", "-m", "recovery/model"]
                args += ["--skip-git-repo-check", "--json", "Continue the fixture task."]
                result = subprocess.run(args, cwd=work, env=env, capture_output=True, timeout=55)
                events = []
                for line in result.stdout.splitlines():
                    try:
                        events.append(json.loads(line))
                    except ValueError:
                        pass
                return result.returncode, events

            code, events = call(False)
            before = len(upstream.requests)
            resumed, after = call(True)
            last = json.dumps(recovery.requests[-1]) if recovery.requests else ""
            preserved = "EMP_TOOL_MARKER" in last and "function_call_output" in last
            repeated = sum(e.get("item", {}).get("type") == "command_execution" for e in after)
            accepted = code != 0 and resumed == 0 and 0 < before <= 6 and len(recovery.requests) == 1
            accepted = accepted and len(upstream.requests) == before and repeated == 0
            if scenario == "tool_then_exhausted":
                accepted = accepted and preserved and before == 2
            elif scenario == "exhausted":
                accepted = accepted and before == 1
            return {"scenario": scenario, "exit": code, "initial_requests": before,
                    "event_types": [e.get("type") for e in events], "resume_exit": resumed,
                    "explicit_source_switch": True, "resume_requests": len(recovery.requests),
                    "resume_errors": [e.get("message", e.get("error")) for e in after if e.get("type") in ("error", "turn.failed")],
                    "tool_output_preserved": preserved, "resume_tool_executions": repeated,
                    "acceptance": accepted}
    finally:
        upstream.close()
        recovery.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex-bin", type=Path, required=True)
    parser.add_argument("--emp-bin", type=Path, required=True)
    args = parser.parse_args()
    binary = args.codex_bin.resolve(strict=True)
    emp = args.emp_bin.resolve(strict=True)
    results = [run(binary, emp, scenario) for scenario in ("exhausted", "interrupted", "tool_then_exhausted")]
    print(json.dumps({"results": results}, indent=2))
    if not all(result["acceptance"] for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
