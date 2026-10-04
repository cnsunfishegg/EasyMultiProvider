#!/usr/bin/env python3
"""Exercise EMP's real quota process runner with fake credentials and a refusing proxy.

Build `cargo build -p emp-codex --example quota_probe` first. Proxy observations
cannot establish the authenticated helper's complete network behavior.
"""
import argparse
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--codex-bin", type=Path, required=True)
    args = parser.parse_args()
    counts = {"connections": 0, "forwarded_bytes": 0}

    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_CONNECT(self):
            counts["connections"] += 1
            self.send_error(503, "Fixture refuses external traffic")

        do_GET = do_CONNECT
        do_POST = do_CONNECT

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with tempfile.TemporaryDirectory(prefix="emp-quota-probe-") as directory:
            proxy = f"http://127.0.0.1:{server.server_port}"
            env = {"PATH": os.defpath, "HOME": directory, "TMPDIR": directory,
                   "HTTP_PROXY": proxy, "HTTPS_PROXY": proxy, "ALL_PROXY": proxy,
                   "http_proxy": proxy, "https_proxy": proxy, "all_proxy": proxy,
                   "NO_PROXY": "", "no_proxy": ""}
            completed = subprocess.run([str(args.runner.resolve(strict=True)), str(args.codex_bin.resolve(strict=True))],
                                       env=env, cwd=directory, capture_output=True, timeout=15, check=True)
            observed = json.loads(completed.stdout)
            observed["proxy"] = counts
            observed["temporary_credential_directories_remaining"] = len(list(Path(directory).glob("easy-mp-codex-account-*")))
            observed["authenticated_zero_download_acceptance"] = "UNKNOWN"
            print(json.dumps(observed, indent=2))
            assert observed["elapsed_ms"] < 12000
            assert observed["temporary_credential_directories_remaining"] == 0
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


if __name__ == "__main__":
    main()
