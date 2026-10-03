"""Gold order-flow history API (Henri, Oct 2026). Runs next to recorder.py on the server, behind Caddy at /api/.

GET /api/book?minutes=120      recorded order book of the last N minutes (1-360), for the app's heatmap:
    {"symbol": "XAUUSDT", "step": 0.25, "cols": [[t_sec, first_row, best_bid, best_ask, q, q, q, ...], ...]}
    first_row = price of the lowest row / step; the q's are the waiting ounces (bids + asks) of consecutive rows
    upward from there. One column per recorded snapshot (every 2 s).
GET /api/fp?tf=5&step=0.5&from=<ms>&until=<ms>&sess=<ms>   footprint candles for the app (see fp_candles below)
GET /api/health                 "ok"
Only the Python standard library; reads the hourly files written by recorder.py (plain or gzipped).
"""
import gzip, json, os, threading, time
import fpcore as F
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


# ---------------------------------------------------------------- footprint candles
_fp_lock = threading.Lock()
_atoms_cache = OrderedDict()          # (day, hh) -> (mtime, atoms)          parsed atom files
_agg_cache = OrderedDict()            # (hour_ms, tf, stepc) -> (mtime, subcandles, profile_cells)


def hour_atoms(h, now_ms):
    """Minute atoms of the UTC hour starting at h. Returns (atoms, cache_token or None if not cacheable)."""
    day, hh = F.day_hour(h)
    p = F.atoms_path(day, hh)
    try:
        mt = os.stat(p).st_mtime
    except OSError:
        mt = None
    if mt is not None:
        hit = _atoms_cache.get((day, hh))
        if hit and hit[0] == mt:
            _atoms_cache.move_to_end((day, hh))
            return hit[1], mt
        r = F.load_atoms(day, hh)
        if r is not None:
            _atoms_cache[(day, hh)] = (mt, r[1])
            while len(_atoms_cache) > 96:
                _atoms_cache.popitem(last=False)
            return r[1], mt
    # no atom file yet (current hour, or the history service has not got to it): straight from the recording
    return F.build_atoms(F.recorded_trades(day, hh)), None


def aggregate(atoms, tf, stepc, t0, t1, sess):
    """atoms of one hour -> ({sub_t: [o, h, l, c, sell, buy, {row: [s, b]}]}, profile {row: [s, b]})"""
    subs, prof, tfm = {}, {}, tf * 60000
    for a in atoms:
        t = a[0]
        if t < t0 or t >= t1:
            continue
        k = t - t % tfm
        e = subs.get(k)
        if e is None:
            e = subs[k] = [a[1], a[2], a[3], a[4], 0.0, 0.0, {}]
        else:
            if a[2] > e[1]:
                e[1] = a[2]
            if a[3] < e[2]:
                e[2] = a[3]
            e[3] = a[4]
        r0, cells, inprof = a[5], e[6], t >= sess
        for i in range(6, len(a), 2):
            sv, bv = a[i], a[i + 1]
            if not sv and not bv:
                continue
            R = ((r0 + (i - 6) // 2) * F.BASEC) // stepc
            c = cells.get(R)
            if c is None:
                c = cells[R] = [0.0, 0.0]
            c[0] += sv
            c[1] += bv
            e[4] += sv
            e[5] += bv
            if inprof:
                pc = prof.get(R)
                if pc is None:
                    pc = prof[R] = [0.0, 0.0]
                pc[0] += sv
                pc[1] += bv
    return subs, prof


def flat_rows(cells):
    if not cells:
        return []
    r0, r1 = min(cells), max(cells)
    out = [r0] + [0] * (2 * (r1 - r0 + 1))
    for r, (sv, bv) in cells.items():
        out[1 + 2 * (r - r0)] = round(sv, 3)
        out[2 + 2 * (r - r0)] = round(bv, 3)
    return out


def fp_candles(tf, step, frm, until, sess):
    """Footprint candles of tf minutes (must divide 60) and $step rows for [frm, until), plus the volume profile of
    [sess, until). Candle: [t, o, h, l, c, sell, buy, first_row, sell, buy, sell, buy, ...]; profile:
    [first_row, sell, buy, ...]; rows are price/step (row r = r*step .. (r+1)*step)."""
    stepc = int(round(step * 100))
    if 60 % tf or stepc < F.BASEC or stepc % F.BASEC:
        raise ValueError("tf must divide 60 and step must be a multiple of $0.05")
    now_ms = int(time.time() * 1000)
    until = min(until, now_ms)
    frm = max(frm, until - 95 * 86400000)
    tfm = tf * 60000
    frm -= frm % tfm
    out, prof, first, pfrom = [], {}, None, None
    start = min(frm, sess)                     # earlier hours only for the session profile
    h = start - start % 3600000
    while h < until:
        he = h + 3600000
        if he <= frm and he <= sess:
            h = he
            continue
        full = he <= until and he <= now_ms - 120000
        hit = None
        atoms, token = (None, None)
        if full:
            day, hh = F.day_hour(h)
            try:
                mt = os.stat(F.atoms_path(day, hh)).st_mtime
            except OSError:
                mt = None
            if mt is not None:
                hit = _agg_cache.get((h, tf, stepc))
                if hit and hit[0] != mt:
                    hit = None
        if hit:
            _agg_cache.move_to_end((h, tf, stepc))
            subs, hprof = hit[1], hit[2]
        else:
            atoms, token = hour_atoms(h, now_ms)
            # full hours are cached with their whole profile (session start is always on a whole UTC hour)
            subs, hprof = aggregate(atoms, tf, stepc, h, until, 0 if full else sess)
            if full and token is not None:
                _agg_cache[(h, tf, stepc)] = (token, subs, hprof)
                while len(_agg_cache) > 20000:
                    _agg_cache.popitem(last=False)
        for k in sorted(subs):
            if k < frm:
                continue
            o, hi, lo, c, sv, bv, cells = subs[k]
            out.append([k, o, hi, lo, c, round(sv, 3), round(bv, 3)] + flat_rows(cells))
            if first is None:
                first = k
        if (h >= sess) if full else (he > sess):
            for R, (sv, bv) in hprof.items():
                pc = prof.get(R)
                if pc is None:
                    pc = prof[R] = [0.0, 0.0]
                pc[0] += sv
                pc[1] += bv
            if hprof and pfrom is None:
                pfrom = max(sess, h)
        h = he
    return {"tf": tf, "step": step, "from": frm, "until": until, "first": first, "pfrom": pfrom,
            "c": out, "p": flat_rows(prof)}


def warm_up():
    time.sleep(20)
    now = int(time.time() * 1000)
    for tf, step, days in ((60, 5, 90), (60, 2, 30), (60, 1, 10), (5, 0.5, 1), (15, 0.5, 3), (30, 0.5, 5)):
        try:
            with _fp_lock:
                fp_candles(tf, step, now - days * 86400000, now, now)
        except Exception as e:
            print("warm-up failed:", e, flush=True)


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
            if u.path == "/api/fp":
                g = lambda k, d: q.get(k, [d])[0]
                now = int(time.time() * 1000)
                tf, step = int(g("tf", "5")), float(g("step", "0.5"))
                until = int(float(g("until", now)))
                frm = int(float(g("from", until - 86400000)))
                sess = int(float(g("sess", until)))
                t = time.time()
                with _fp_lock:
                    d = fp_candles(tf, step, frm, until, sess)
                d["ms"] = round((time.time() - t) * 1000)
                return self.send(200, json.dumps(d, separators=(",", ":")))
            self.send(404, '{"error":"not found"}')
        except Exception as e:
            self.send(500, json.dumps({"error": f"{type(e).__name__}: {e}"}))


if __name__ == "__main__":
    print(f"history API on 127.0.0.1:{PORT}, data {os.path.abspath(BASE)}", flush=True)
    threading.Thread(target=warm_up, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
