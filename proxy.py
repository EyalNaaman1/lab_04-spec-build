#!/usr/bin/env python3
"""proxy.py: the naive agent-traffic proxy for Lab 4.

A reverse proxy that prints every request and response, forwards them
unchanged, and serves a live traffic dashboard. Standard library only; nothing
to install.

    python3 proxy.py                          # listens on http://localhost:8041
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    open http://localhost:8041/__dashboard    # live view (see SPEC.md)

Everything Claude Code sends to the API passes through this program and is
printed to the terminal. Forwarding changes nothing: headers go through untouched
(a subscription login needs its bearer token and its `anthropic-beta` header),
and the reply is streamed back byte for byte as it arrives. Secret headers are
redacted everywhere the proxy shows or stores them.

    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
"""

from __future__ import annotations

import argparse
import codecs
import functools
import http.client
import itertools
import json
import queue
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we print is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

# Secret headers: never printed, never stored, never sent to the dashboard.
REDACT = {"authorization", "x-api-key", "cookie", "set-cookie", "proxy-authorization"}

UPSTREAM = "https://api.anthropic.com"
DASHBOARD_PAGE = Path(__file__).with_name("dashboard.html")
DASHBOARD_HOSTS = {"localhost", "127.0.0.1"}  # DNS-rebinding guard for /__dashboard*
INSTANCE = uuid.uuid4().hex[:8]  # new per proxy start; lets the page drop state from an old run
_ids = itertools.count(1)


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def now_ms() -> int:
    return int(time.time() * 1000)


def redact(headers) -> list[list[str]]:
    return [[k, "[redacted]" if k.lower() in REDACT else v] for k, v in headers.items()]


def safe(fn):
    """Dashboard recording must never reach the harness: log the first failure and stop recording this exchange."""

    @functools.wraps(fn)
    def wrapper(self, *args):
        if self.broken:
            return None
        try:
            return fn(self, *args)
        except Exception as e:
            self.broken = True
            print(f"\n[{stamp()}] !!! dashboard recording failed in {fn.__name__}: {e!r}", flush=True)
            return None

    return wrapper


class Store:
    """In-memory dashboard state: the latest payload per (exchange, kind), plus open SSE subscribers."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.seq = 0
        self.latest: dict[tuple, tuple[int, str]] = {}  # (exchange id, kind) -> (seq, JSON text)
        self.subs: set[queue.Queue] = set()

    def put(self, key: tuple, payload: dict) -> None:
        with self.lock:
            self.seq += 1
            text = json.dumps(payload, ensure_ascii=False)
            self.latest[key] = (self.seq, text)
            for q in self.subs:
                q.put((self.seq, text))

    def subscribe(self, last: int) -> tuple[queue.Queue, list[tuple[int, str]]]:
        """Register a client; return what it missed after `last`, so no event is lost or doubled."""
        q: queue.Queue = queue.Queue()
        with self.lock:
            backlog = sorted(v for v in self.latest.values() if v[0] > last)
            self.subs.add(q)
        return q, backlog

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            self.subs.discard(q)


STORE = Store()


class SSE:
    """Incremental text/event-stream parser. feed() returns the complete (event, data) pairs so far."""

    def __init__(self) -> None:
        self.buf = ""
        self.dec = codecs.getincrementaldecoder("utf-8")("replace")

    def feed(self, data: bytes) -> list[tuple[str, str]]:
        self.buf = (self.buf + self.dec.decode(data)).replace("\r\n", "\n")
        out = []
        while "\n\n" in self.buf:
            block, self.buf = self.buf.split("\n\n", 1)
            name, lines = "message", []
            for line in block.split("\n"):
                if line.startswith("event:"):
                    name = line[6:].strip()
                elif line.startswith("data:"):
                    lines.append(line[6:] if line.startswith("data: ") else line[5:])
            if lines:
                out.append((name, "\n".join(lines)))
        return out


class Capture:
    """Records one exchange (SPEC.md §4). Every method is guarded; failures are printed, never raised."""

    def __init__(self, method: str, path: str, headers, body: bytes) -> None:
        self.id = next(_ids)
        self.broken = False
        self.stream = False
        self.saw_stop = False
        self.sse = SSE()
        self.blocks: dict[int, dict] = {}
        self.body_buf = bytearray()
        self.last_pub = 0.0
        self.resp = {"status": None, "state": "pending", "stop_reason": None, "usage": {}, "text": "",
                     "tool_calls": [], "error": None, "ended_at": None, "events": []}
        self.request(method, path, headers, body)

    @safe
    def request(self, method: str, path: str, headers, body: bytes) -> None:
        parsed, raw, bad = None, None, False
        if body:
            try:
                parsed = json.loads(body)
            except ValueError:
                raw, bad = body.decode("utf-8", "replace"), True
        STORE.put((self.id, "request"), {
            "type": "request", "id": self.id, "started_at": now_ms(), "method": method, "path": path,
            "headers": redact(headers), "body": parsed, "body_raw": raw, "body_error": bad,
        })

    @safe
    def head(self, status: int, reason: str, content_type: str | None) -> None:
        self.stream = "text/event-stream" in (content_type or "")
        self.resp["status"] = status
        self.resp["state"] = "streaming"
        if status >= 400:
            self.resp["error"] = {"type": "http_error", "status": status,
                                  "message": f"HTTP {status} {reason}".strip(), "body": ""}
        self.publish(force=True)

    @safe
    def chunk(self, data: bytes) -> None:
        if self.stream:
            for _, text in self.sse.feed(data):
                try:
                    obj = json.loads(text)
                except ValueError:
                    continue
                if isinstance(obj, dict):
                    self.resp["events"].append(obj)
                    self.on_event(obj, text)
            self.derive()
        else:
            self.body_buf += data
        self.publish()

    def on_event(self, obj: dict, text: str) -> None:
        t, r = obj.get("type"), self.resp
        if t == "message_start":
            r["usage"].update((obj.get("message") or {}).get("usage") or {})
        elif t == "content_block_start":
            cb = obj.get("content_block") or {}
            self.blocks[obj.get("index")] = {"type": cb.get("type"), "id": cb.get("id"), "name": cb.get("name"),
                                             "text": cb.get("text") or "", "input": cb.get("input") or None,
                                             "partial": ""}
        elif t == "content_block_delta":
            b, d = self.blocks.get(obj.get("index")), obj.get("delta") or {}
            if b is not None and d.get("type") == "text_delta":
                b["text"] += d.get("text") or ""
            elif b is not None and d.get("type") == "input_json_delta":
                b["partial"] += d.get("partial_json") or ""
        elif t == "content_block_stop":
            b = self.blocks.get(obj.get("index"))
            if b and b["type"] == "tool_use" and b["input"] is None:
                try:
                    b["input"] = json.loads(b["partial"] or "{}")
                except ValueError:
                    pass  # keep the raw partial text for display
        elif t == "message_delta":
            r["stop_reason"] = (obj.get("delta") or {}).get("stop_reason")
            r["usage"].update(obj.get("usage") or {})
        elif t == "message_stop":
            self.saw_stop = True
        elif t == "error":
            err = obj.get("error") or {}
            r["state"] = "aborted"
            r["error"] = {"type": "stream_error", "status": None,
                          "message": f"{err.get('type', 'error')}: {err.get('message', '')}", "body": text[:500]}

    def derive(self) -> None:
        blocks = [self.blocks[i] for i in sorted(self.blocks)]
        self.resp["text"] = "".join(b["text"] for b in blocks if b["type"] == "text")
        self.resp["tool_calls"] = [
            {"id": b["id"], "name": b["name"], "input": b["input"] if b["input"] is not None else b["partial"]}
            for b in blocks if b["type"] == "tool_use"
        ]

    def json_body(self) -> None:
        """A non-streamed reply is one JSON object: read its content blocks the same way as stream events."""
        try:
            obj = json.loads(self.body_buf)
        except ValueError:
            return
        if not isinstance(obj, dict):
            return
        self.resp["events"] = [obj]
        for i, cb in enumerate(obj.get("content") or []):
            if isinstance(cb, dict):
                inp = cb.get("input")
                self.blocks[i] = {"type": cb.get("type"), "id": cb.get("id"), "name": cb.get("name"),
                                  "text": cb.get("text") or "", "input": inp if isinstance(inp, dict) else None,
                                  "partial": ""}
        self.resp["stop_reason"] = obj.get("stop_reason")
        self.resp["usage"] = obj.get("usage") or {}
        self.derive()

    def abort(self, message: str) -> None:
        self.resp["state"] = "aborted"
        if self.resp["error"] is None:
            self.resp["error"] = {"type": "stream_aborted", "status": None, "message": message, "body": ""}

    @safe
    def finish(self, problem: str | None = None) -> None:
        r = self.resp
        if not self.stream and not problem:
            self.json_body()
        if r["state"] != "aborted":
            if problem:
                self.abort(problem)
            elif self.stream and not self.saw_stop:
                self.abort("stream closed before message_stop")
            else:
                r["state"] = "complete"
        err = r["error"]
        if err and err["type"] == "http_error" and not err["body"]:
            err["body"] = self.body_buf.decode("utf-8", "replace")[:500]
        r["ended_at"] = now_ms()
        self.publish(force=True)

    @safe
    def fail(self, message: str) -> None:
        self.resp.update(status=502, state="aborted", ended_at=now_ms(),
                         error={"type": "upstream_unreachable", "status": 502, "message": message, "body": ""})
        self.publish(force=True)

    def publish(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_pub < 0.1:  # cap streaming updates at ~10/s
            return
        self.last_pub = now
        view = {k: v for k, v in self.resp.items() if k != "events"}
        STORE.put((self.id, "reply"), {"type": "reply", "id": self.id, "response": view})


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def relay(self) -> None:
        if self.path.split("?", 1)[0].startswith("/__dashboard"):
            return self.dashboard()

        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""

        # 1. Print what the agent sent. Secret headers are redacted in full.
        print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
        for name, value in redact(self.headers):
            print(f"    {name}: {value}")
        if body:
            try:
                print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
            except ValueError:
                print(body.decode("utf-8", "replace"))
        cap = Capture(self.command, self.path, self.headers, body)

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        # Connect with a short timeout (a lost SYN must not stall the agent for
        # minutes); the long timeout is for reading a slow stream.
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            print(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
            cap.fail(f"cannot reach {UPSTREAM}: {e}")
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            return
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        try:
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except (OSError, http.client.HTTPException) as e:
            print(f"\n[{stamp()}] !!! upstream request failed: {e}", flush=True)
            cap.fail(f"upstream request failed: {e}")
            self.send_error(502, f"proxy: upstream request failed: {e}")
            self.close_connection = True
            return

        # 3. Stream the reply back as it arrives, printing and recording it on the way.
        print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        cap.head(reply.status, reply.reason, reply.getheader("content-type"))
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD":
            cap.finish()
            self.close_connection = True
            return
        problem = None
        try:
            while chunk := reply.read1(4096):  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except OSError:
                    problem = "client disconnected before the stream finished"
                    break
                cap.chunk(chunk)
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
        except (OSError, http.client.HTTPException) as e:
            problem = f"upstream read failed: {e}"
        cap.finish(problem)
        self.close_connection = True
        print(f"\n[{stamp()}] {'!!! ' + problem if problem else '--- done'}", flush=True)

    # ---- dashboard: answered here, never forwarded ----

    def dashboard(self) -> None:
        host = urlsplit("//" + (self.headers.get("Host") or "")).hostname
        path = self.path.split("?", 1)[0]
        if host not in DASHBOARD_HOSTS:
            return self.reply(403, "text/plain; charset=utf-8", b"dashboard is only served to localhost\n")
        if self.command == "GET" and path == "/__dashboard":
            csp = ("default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                   "connect-src 'self'; base-uri 'none'")
            return self.reply(200, "text/html; charset=utf-8", DASHBOARD_PAGE.read_bytes(),
                              [("Content-Security-Policy", csp)])
        if self.command == "GET" and path == "/__dashboard/events":
            return self.events()
        self.reply(404, "text/plain; charset=utf-8", b"not found\n")

    def reply(self, code: int, ctype: str, body: bytes, extra=()) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in extra:
            self.send_header(k, v)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self.close_connection = True

    def events(self) -> None:
        inst, _, n = (self.headers.get("Last-Event-ID") or "").partition(":")
        last = int(n) if inst == INSTANCE and n.isdigit() else 0  # ids from an old run: replay everything
        q, backlog = STORE.subscribe(last)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            hello = json.dumps({"type": "hello", "instance": INSTANCE})
            self.wfile.write(f"retry: 2000\ndata: {hello}\n\n".encode("utf-8"))
            for seq, text in backlog:
                self.sse(seq, text)
            while True:
                try:
                    seq, text = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")  # keeps the socket honest; a dead browser fails here
                    self.wfile.flush()
                    continue
                self.sse(seq, text)
        except OSError:
            pass  # the browser went away, or the proxy is stopping
        finally:
            STORE.unsubscribe(q)
            self.close_connection = True

    def sse(self, seq: int, text: str) -> None:
        self.wfile.write(f"id: {INSTANCE}:{seq}\ndata: {text}\n\n".encode("utf-8"))
        self.wfile.flush()


class Server(ThreadingHTTPServer):
    # On Windows, SO_REUSEADDR lets a second proxy bind the same port and split the traffic.
    allow_reuse_address = False


def main() -> None:
    global UPSTREAM
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    try:
        server = Server(("127.0.0.1", args.port), Tap)
    except OSError as e:
        raise SystemExit(f"port {args.port} is already in use (another proxy running?): {e}")
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    print(f"live dashboard:      http://localhost:{args.port}/__dashboard")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
