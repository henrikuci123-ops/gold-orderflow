"""Gold order-flow history API (Henri, Oct 2026). Runs next to recorder.py on the server, behind Caddy at /api/.

GET /api/book?minutes=120      recorded order book of the last N minutes (1-360), for the app's heatmap:
    {"symbol": "XAUUSDT", "step": 0.25, "cols": [[t_sec, first_row, best_bid, best_ask, q, q, q, ...], ...]}
    first_row = price of the lowest row / step; the q's are the waiting ounces (bids + asks) of consecutive rows
    upward from there. One column per recorded snapshot (every 2 s).
GET /api/health                 "ok"
Only the Python standard library; reads the hourly files written by recorder.py (plain or gzipped).
"""
import gzip, json, os, threading, time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

SYMBOL = os.environ.get("GOF_SYMBOL", "XAUUSDT")
BASE = os.environ.get("GOF_DATA") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "orderflow_data")
PORT = int(os.environ.get("GOF_API_PORT", "8081"))
STEP = 0.25
MAX_MIN = 360

_cache, _lock = OrderedDict(), threading.Lock()      # finished (gzipped) hours, parsed once


def compact(rec):
    """One recorded snapshot -> [t_sec, first_row, bb, ba, q...]"""
    rows = {}
    for p, q in rec.get("b", []) + rec.get("a", []):
        r = round(p / STEP)
        rows[r] = rows.get(r, 0.0) + q
    if not rows:
        return None
    r0, r1 = min(rows), max(rows)
    qs = [0] * (r1 - r0 + 1)
    for r, q in rows.items():
        v = round(q, 1)
        qs[r - r0] = int(v) if v == int(v) else v
    return [rec["t"] // 1000, r0, rec.get("bb"), rec.get("ba")] + qs


def read_file(path):
    out = []
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    c = compact(json.loads(line))
                except (ValueError, KeyError, TypeError):
                    continue                     # e.g. a half-written last line
                if c:
                    out.append(c)
    except (OSError, EOFError):
        pass                                     # file being gzipped right now / truncated: use what we got
    return out


def hour_cols(day, hour):
    folder = os.path.join(BASE, SYMBOL, day)
    gz, plain = os.path.join(folder, f"book_{hour}.jsonl.gz"), os.path.join(folder, f"book_{hour}.jsonl")
    cols = []
    if os.path.exists(gz):
        st = os.stat(gz)
        key = (gz, st.st_mtime, st.st_size)
        with _lock:
            hit = _cache.get(key)
        if hit is None:
            hit = read_file(gz)
            if not os.path.exists(plain):        # finished hour: keep it in memory
                with _lock:
                    _cache[key] = hit
                    while len(_cache) > 8:
                        _cache.popitem(last=False)
        cols += hit
    if os.path.exists(plain):
        cols += read_file(plain)
    return cols


def book_history(minutes):
    now = datetime.now(timezone.utc)
    t0 = int(now.timestamp()) - minutes * 60
    h = now.replace(minute=0, second=0, microsecond=0) - timedelta(minutes=minutes)
    cols = []
    while h <= now:
        cols += hour_cols(h.strftime("%Y-%m-%d"), h.strftime("%H"))
        h += timedelta(hours=1)
    seen, out = set(), []
    for c in sorted(cols, key=lambda c: c[0]):
        if c[0] >= t0 and c[0] not in seen:
            seen.add(c[0])
            out.append(c)
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/api/health":
                return self.send(200, "ok", "text/plain")
            if u.path == "/api/book":
                minutes = max(1, min(MAX_MIN, int(float(q.get("minutes", ["120"])[0]))))
                t = time.time()
                cols = book_history(minutes)
                body = json.dumps({"symbol": SYMBOL, "step": STEP, "minutes": minutes, "cols": cols,
                                   "ms": round((time.time() - t) * 1000)}, separators=(",", ":"))
                return self.send(200, body)
            self.send(404, '{"error":"not found"}')
        except Exception as e:
            self.send(500, json.dumps({"error": f"{type(e).__name__}: {e}"}))


if __name__ == "__main__":
    print(f"history API on 127.0.0.1:{PORT}, data {os.path.abspath(BASE)}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
