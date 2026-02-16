"""Configurable test HTTP server for xs3lerator integration tests.

Endpoints:
    GET /data/<size>              Serve <size> bytes with Content-Length and Accept-Ranges.
    GET /data/<size>?ranges_lie   Advertise Accept-Ranges but 403 on actual range requests.
    GET /data/<size>?no_ranges    Serve without Accept-Ranges header.
    GET /data/<size>?chunked      Serve with Transfer-Encoding: chunked (no Content-Length).
    GET /data/<size>?slow=<ms>    Add <ms> milliseconds delay per 64 KiB written.
    GET /echo-headers             Return request headers as JSON.
    GET /stats                    Return per-path request counts as JSON.
    GET /status/<code>            Return the given HTTP status code.
"""

import json
import threading
import time
import urllib.parse
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def generate_payload(size: int) -> bytes:
    """Deterministic payload: repeating 0x00..0xFF pattern."""
    pattern = bytes(range(256))
    full_repeats = size // 256
    remainder = size % 256
    return pattern * full_repeats + pattern[:remainder]


class _Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts: dict[str, int] = defaultdict(int)

    def record(self, path: str):
        with self.lock:
            self.counts[path] += 1

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return dict(self.counts)

    def reset(self):
        with self.lock:
            self.counts.clear()


_stats = _Stats()


class TestHandler(BaseHTTPRequestHandler):
    """Quiet handler — suppress access logs."""

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path = parsed.path

        _stats.record(path)

        if path.startswith("/data/"):
            try:
                size = int(path.split("/")[2])
            except (IndexError, ValueError):
                self.send_error(400, "bad size")
                return
            self._serve_data(size, params)
        elif path == "/echo-headers":
            self._echo_headers()
        elif path == "/stats":
            self._serve_stats()
        elif path == "/stats/reset":
            _stats.reset()
            self._json_response(200, {"ok": True})
        elif path.startswith("/status/"):
            try:
                code = int(path.split("/")[2])
            except (IndexError, ValueError):
                code = 500
            self.send_error(code)
        else:
            self.send_error(404)

    def do_HEAD(self):
        self.do_GET()

    def _serve_data(self, size: int, params: dict):
        data = generate_payload(size)
        chunked = "chunked" in params
        ranges_lie = "ranges_lie" in params
        no_ranges = "no_ranges" in params
        slow_ms = int(params.get("slow", [0])[0])

        range_header = self.headers.get("Range")

        # Server advertises ranges but rejects them
        if ranges_lie and range_header:
            self.send_error(403, "Forbidden")
            return

        # Serve a range request (only if ranges are supported)
        if range_header and not no_ranges and not chunked:
            return self._serve_range(data, size, range_header)

        # Chunked encoding: no Content-Length
        if chunked:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self._write_chunked(data, slow_ms)
            return

        # Normal full response
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size))
        if not no_ranges:
            self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", f'"test-{size}"')
        self.end_headers()
        self._write_with_delay(data, slow_ms)

    def _serve_range(self, data: bytes, total_size: int, range_header: str):
        """Parse Range header and serve 206 Partial Content."""
        if not range_header.startswith("bytes="):
            self.send_error(400, "bad range")
            return

        range_spec = range_header[6:]
        if "," in range_spec:
            self.send_error(416, "multi-range not supported")
            return

        start, end = self._parse_range(range_spec, total_size)
        if start is None:
            self.send_error(416, "invalid range")
            return

        slice_data = data[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(slice_data)))
        self.send_header("Content-Range", f"bytes {start}-{end}/{total_size}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(slice_data)

    @staticmethod
    def _parse_range(spec: str, total: int):
        """Parse a single byte range spec, return (start, end_inclusive) or (None, None)."""
        if spec.startswith("-"):
            suffix = int(spec[1:])
            if suffix <= 0 or suffix > total:
                return None, None
            return total - suffix, total - 1
        parts = spec.split("-", 1)
        start = int(parts[0])
        end = int(parts[1]) if parts[1] else total - 1
        end = min(end, total - 1)
        if start > end or start >= total:
            return None, None
        return start, end

    def _write_chunked(self, data: bytes, slow_ms: int):
        chunk_size = 64 * 1024
        offset = 0
        while offset < len(data):
            piece = data[offset : offset + chunk_size]
            self.wfile.write(f"{len(piece):x}\r\n".encode())
            self.wfile.write(piece)
            self.wfile.write(b"\r\n")
            offset += len(piece)
            if slow_ms:
                time.sleep(slow_ms / 1000.0)
        self.wfile.write(b"0\r\n\r\n")

    def _write_with_delay(self, data: bytes, slow_ms: int):
        if not slow_ms:
            self.wfile.write(data)
            return
        chunk_size = 64 * 1024
        offset = 0
        while offset < len(data):
            self.wfile.write(data[offset : offset + chunk_size])
            offset += chunk_size
            time.sleep(slow_ms / 1000.0)

    def _echo_headers(self):
        headers = {k: v for k, v in self.headers.items()}
        self._json_response(200, headers)

    def _serve_stats(self):
        self._json_response(200, _stats.snapshot())

    def _json_response(self, code: int, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class TestServer:
    """Wrapper to start/stop the test HTTP server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.server = ThreadingHTTPServer((host, port), TestHandler)
        self.host = host
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"
