#!/usr/bin/env python3
"""Git Watch: a read-only local commit Channel."""

from collections import deque
from pathlib import Path
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import selectors
import signal
import subprocess
import tempfile
import time
import urllib.request


CHANNEL_ID = "dev.termite.git-watch.commits"
CONFIG_PATH = Path(__file__).with_name("config.json")
MAX_GIT_BYTES = 64 * 1024
MAX_HTTP_BYTES = 1024 * 1024
SAFE_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "GIT_TERMINAL_PROMPT": "0"}


def as_bool(value):
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def utf8_prefix(value, max_bytes):
    return str(value).encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def field(value, default, max_bytes):
    text = str(value) if value is not None else ""
    return utf8_prefix(text if text.strip() else default, max_bytes)


def iso_now(offset_seconds=0):
    value = datetime.now(timezone.utc) + timedelta(seconds=max(0, offset_seconds))
    return value.isoformat().replace("+00:00", "Z")


def load_config(path=CONFIG_PATH, environ=None):
    environ = os.environ if environ is None else environ
    config = {
        "enabled": False, "repository": "", "includeExistingCommits": False,
        "pollIntervalSeconds": 5, "maxCommitsPerPoll": 20,
    }
    if path.exists():
        if path.stat().st_size > 64 * 1024:
            raise ValueError("config.json exceeds 64 KiB")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("config.json must contain an object")
        config.update(value)
    mapping = {
        "enabled": "TERMITE_GIT_WATCH_ENABLED",
        "repository": "TERMITE_GIT_REPO",
        "includeExistingCommits": "TERMITE_GIT_INCLUDE_EXISTING",
        "pollIntervalSeconds": "TERMITE_GIT_INTERVAL",
        "maxCommitsPerPoll": "TERMITE_GIT_MAX_COMMITS",
    }
    for key, env_key in mapping.items():
        if environ.get(env_key) not in (None, ""):
            config[key] = environ[env_key]
    config["enabled"] = as_bool(config.get("enabled"))
    config["includeExistingCommits"] = as_bool(config.get("includeExistingCommits"))
    try:
        config["pollIntervalSeconds"] = min(300.0, max(1.0, float(config["pollIntervalSeconds"])))
        config["maxCommitsPerPoll"] = min(100, max(1, int(config["maxCommitsPerPoll"])))
    except (TypeError, ValueError) as exc:
        raise ValueError("pollIntervalSeconds/maxCommitsPerPoll are invalid") from exc
    if not config["enabled"]:
        raise ValueError("Git Watch is disabled; copy config.example.json and opt in")
    if not config.get("repository"):
        raise ValueError("repository must be explicitly configured")
    repository = Path(str(config["repository"])).expanduser()
    if not repository.is_absolute():
        repository = path.parent / repository
    config["repository"] = repository.resolve()
    if not config["repository"].is_dir():
        raise ValueError("repository must be an existing directory")
    return config


def run_bounded(argv, timeout=5, max_stdout=MAX_GIT_BYTES):
    with tempfile.TemporaryFile() as stdin_file:
        process = subprocess.Popen(
            argv, stdin=stdin_file, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=False, env=SAFE_ENV, close_fds=True, start_new_session=True,
        )
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        chunks = {"stdout": bytearray(), "stderr": bytearray()}
        deadline = time.monotonic() + timeout
        try:
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"{argv[0]} exceeded {timeout:g}s")
                for key, _ in selector.select(min(remaining, 0.1)):
                    data = os.read(key.fileobj.fileno(), 8192)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    chunks[key.data].extend(data)
                    limit = max_stdout if key.data == "stdout" else 4096
                    if len(chunks[key.data]) > limit:
                        raise ValueError(f"git {key.data} exceeds {limit} bytes")
            return_code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        except Exception:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        if return_code != 0:
            error = chunks["stderr"].decode("utf-8", "replace").strip()[:1024]
            raise RuntimeError(f"git exited {return_code}: {error}")
        return bytes(chunks["stdout"])


class GitSource:
    def __init__(self, repository, runner=run_bounded):
        self.repository = Path(repository)
        self.runner = runner

    def command(self, *arguments, max_stdout=MAX_GIT_BYTES):
        return self.runner(
            ["/usr/bin/git", "-C", str(self.repository), *arguments],
            timeout=5, max_stdout=max_stdout,
        )

    def canonical_root(self):
        return Path(self.command("rev-parse", "--show-toplevel", max_stdout=4096).decode().strip()).resolve()

    def recent_hashes(self, count):
        raw = self.command("log", f"--max-count={count}", "--format=%H")
        hashes = [line for line in raw.decode("ascii").splitlines() if line]
        if any(len(value) not in (40, 64) or not all(char in "0123456789abcdef" for char in value) for value in hashes):
            raise ValueError("git returned an invalid commit id")
        return hashes

    def commit(self, commit_hash):
        raw = self.command(
            "show", "-s", "--no-show-signature",
            "--format=%H%x00%an%x00%ae%x00%aI%x00%s%x00%b", commit_hash,
        )
        fields = raw.decode("utf-8", "replace").rstrip("\n").split("\0", 5)
        if len(fields) != 6 or fields[0] != commit_hash:
            raise ValueError("git returned malformed commit metadata")
        return {"hash": fields[0], "author": fields[1], "email": fields[2],
                "date": fields[3], "subject": fields[4], "body": fields[5]}


def commit_to_work_item(commit, repository):
    commit_hash = commit["hash"]
    repo_id = hashlib.sha256(str(Path(repository).resolve()).encode()).hexdigest()[:20]
    message = "\n".join(filter(None, [
        f"Repository: {Path(repository).name}",
        f"Commit: {commit_hash}",
        f"Author: {commit['author']} <{commit['email']}>",
        f"Date: {commit['date']}",
        "",
        commit["subject"],
        commit["body"].strip(),
    ]))
    encoded = message.encode("utf-8")
    if len(encoded) > MAX_GIT_BYTES:
        message = encoded[:MAX_GIT_BYTES - 64].decode("utf-8", "ignore") + "\n[message truncated]"
    return {
        "id": "commit-" + commit_hash,
        "deliveryID": "git:" + repo_id + ":" + commit_hash,
        "conversationID": "repository-" + repo_id,
        "senderID": utf8_prefix(commit["email"], 512),
        "senderName": field(commit["author"], "Git author", 256),
        "title": field(commit["subject"], commit_hash[:12], 512),
        "body": message,
        "createdAt": commit["date"],
        "projectHint": utf8_prefix(Path(repository), 4096),
    }


class TermiteAPI:
    def __init__(self, port, token, timeout=10):
        self.base = f"http://127.0.0.1:{int(port)}"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.timeout = timeout

    def request(self, path, body=None):
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        req = urllib.request.Request(self.base + path, data=data, headers=self.headers,
                                     method="GET" if body is None else "POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            raw = response.read(MAX_HTTP_BYTES + 1)
        if len(raw) > MAX_HTTP_BYTES:
            raise ValueError("Termite response exceeds 1 MiB")
        return json.loads(raw) if raw else {}

    def report_health(self, status, detail="", error="", retry_in=None):
        body = {"status": status, "detail": utf8_prefix(detail, 4096)}
        if status == "healthy":
            body["lastSuccessAt"] = iso_now()
        if error:
            body["error"] = utf8_prefix(error, 4096)
            body["lastErrorAt"] = iso_now()
        if retry_in is not None:
            body["nextRetryAt"] = iso_now(retry_in)
        return self.request(f"/v1/channels/{CHANNEL_ID}/health", body)


class Connector:
    def __init__(self, api, source, config):
        self.api, self.source, self.config = api, source, config
        self.known, self.order, self.initialized = set(), deque(), False

    def health(self, status, **fields):
        try:
            self.api.report_health(status, **fields)
        except Exception as exc:
            print(f"Git Watch health update failed: {exc}", flush=True)

    def remember(self, commit_hash):
        self.known.add(commit_hash)
        self.order.append(commit_hash)
        while len(self.order) > 1024:
            self.known.discard(self.order.popleft())

    def poll_once(self):
        hashes = self.source.recent_hashes(self.config["maxCommitsPerPoll"])
        if not self.initialized and not self.config["includeExistingCommits"]:
            for commit_hash in hashes:
                self.remember(commit_hash)
            self.initialized = True
            self.health("healthy", detail="Git commit baseline completed")
            return []
        submitted = []
        for commit_hash in reversed(hashes):
            if commit_hash in self.known:
                continue
            item = commit_to_work_item(self.source.commit(commit_hash), self.source.repository)
            self.api.request(f"/v1/channels/{CHANNEL_ID}/work-items", item)
            self.remember(commit_hash)
            submitted.append(item)
        self.initialized = True
        self.health("healthy", detail="Git commit poll completed")
        return submitted


def main():
    try:
        config = load_config()
        port, token = os.environ["TERMITE_PORT"], os.environ["TERMITE_TOKEN"]
        source = GitSource(config["repository"])
        source.repository = source.canonical_root()
    except (KeyError, ValueError, OSError, json.JSONDecodeError, RuntimeError) as exc:
        raise SystemExit(f"Git Watch not started: {exc}")
    api = TermiteAPI(port, token)
    api.request("/v1/channels", {
        "id": CHANNEL_ID,
        "name": "Git Watch",
        "service": "Git",
        "account": utf8_prefix(source.repository.name, 256),
        "description": utf8_prefix(f"Read-only commits from {source.repository}", 1024),
        "replyCapabilities": [],
    })
    connector = Connector(api, source, config)
    delay = config["pollIntervalSeconds"]
    while True:
        try:
            connector.poll_once()
            delay = config["pollIntervalSeconds"]
        except Exception as exc:
            print(f"Git Watch poll: {exc}; retrying in {delay:g}s", flush=True)
            connector.health("retrying", error=str(exc), retry_in=delay,
                             detail="Git commit poll failed")
            delay = min(delay * 2, 60.0)
        time.sleep(delay)


if __name__ == "__main__":
    main()
