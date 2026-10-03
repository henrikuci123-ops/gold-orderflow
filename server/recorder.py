"""Gold order-flow recorder (Henri, Oct 2026).

Records Binance XAUUSDT perpetual futures (real exchange data) to disk, so we can later test footprint / absorption /
order-book ideas on our own data:
  trades  : every aggregated trade  ->  <data>/XAUUSDT/YYYY-MM-DD/trades_HH.csv(.gz)
            columns: id, time_ms, price, qty_oz, seller_aggressive (1 = market SELL hit the bid, 0 = market BUY)
  book    : every 2 seconds, the order book grouped in $0.25 rows within +-$25 of the price
            ->  <data>/XAUUSDT/YYYY-MM-DD/book_HH.jsonl(.gz)   {"t": ms, "bb": best bid, "ba": best ask,
                                                                "b": [[price, oz], ...], "a": [[price, oz], ...]}
Hours are UTC. Each finished hour is gzipped automatically.
Missed trades are re-downloaded: after a dropped connection AND after a restart (it continues from the last saved
trade, up to about a week back). The order book cannot be re-downloaded, so book gaps stay gaps.
Runs on the DigitalOcean server (see setup.sh) or on a PC: start_recorder.bat / python recorder.py
(needs: pip install websockets). Light on CPU, no GPU.
Settings by environment variable: GOF_DATA (data folder), GOF_STATUS (folder for status.json), GOF_SYMBOL.
"""
import asyncio, collections, gzip, json, os, shutil, sys, threading, time, urllib.error, urllib.request
from datetime import datetime, timezone

SYMBOL = os.environ.get("GOF_SYMBOL", "XAUUSDT")
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("GOF_DATA") or os.path.join(HERE, "..", "orderflow_data")
STATUS_DIR = os.environ.get("GOF_STATUS") or BASE
VERSION_FILE = os.environ.get("GOF_VERSION_FILE", "")
API = "https://fapi.binance.com"
# Binance (since Apr 23 2026): trades only arrive on /market, the order book only on /public -> two connections
WS_TRADES = "wss://fstream.binance.com/market/stream?streams={s}@aggTrade"
WS_BOOK = "wss://fstream.binance.com/public/stream?streams={s}@depth@500ms"
BOOK_EVERY = 2.0          # seconds between book snapshots
BOOK_STEP = 0.25          # $ per book row
BOOK_RANGE = 25.0         # +- $ around the price
MAX_FILL_CALLS = 3000     # 1000 trades per call -> up to ~3M trades (about a week of gold)
FILL_PAUSE = 0.6          # seconds between download calls (aggTrades weight 20; limit 2400/min)
STATUS_EVERY = 30         # seconds

try:
    import websockets
except ImportError:
    print("Missing package: run   pip install websockets   (start_recorder.bat does this for you)")
    sys.exit(1)

LOG_TAIL = collections.deque(maxlen=40)


def log(*a):
    line = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a)
    print(line, flush=True)
    LOG_TAIL.append(line)
    try:
        with open(os.path.join(BASE, "recorder.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def get_json(path):
    try:
        with urllib.request.urlopen(API + path, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 451:
            log("BINANCE BLOCKS THIS SERVER'S LOCATION (HTTP 451) - the server must be in another country")
        raise


class HourFiles:
    """Plain text file per UTC hour; the previous hour is gzipped in the background. Thread-safe (the gap download
    runs in a helper thread while the main loop flushes)."""

    def __init__(self, kind, ext):
        self.kind, self.ext, self.key, self.f, self.path = kind, ext, None, None, None
        self.lock = threading.Lock()

    def write(self, t_ms, line):
        with self.lock:
            key = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d/%H")
            if key != self.key:
                self._close()
                day, hour = key.split("/")
                folder = os.path.join(BASE, SYMBOL, day)
                os.makedirs(folder, exist_ok=True)
                path = os.path.join(folder, f"{self.kind}_{hour}.{self.ext}")
                new = not os.path.exists(path) and not os.path.exists(path + ".gz")
                self.f = open(path, "a", encoding="utf-8", newline="")
                if new and self.kind == "trades":
                    self.f.write("id,time_ms,price,qty_oz,seller_aggressive\n")
                self.key, self.path = key, path
            self.f.write(line)

    def flush(self):
        with self.lock:
            if self.f:
                self.f.flush()

    def close(self):
        with self.lock:
            self._close()

    def _close(self):
        if self.f:
            self.f.close()
            threading.Thread(target=gzip_file, args=(self.path,), daemon=True).start()
        self.f = None


GZ_LOCK = threading.Lock()


def gzip_file(path):
    with GZ_LOCK:
        try:
            if not os.path.exists(path) or path.endswith(".gz"):
                return
            with open(path, "rb") as src, gzip.open(path + ".gz", "ab") as dst:
                shutil.copyfileobj(src, dst)
            os.remove(path)
        except OSError as e:
            log("gzip failed", path, e)


def compress_leftovers():
    """Gzip plain files from earlier runs (anything not from the current hour)."""
    now_key = datetime.now(timezone.utc).strftime("%H")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    root = os.path.join(BASE, SYMBOL)
    if not os.path.isdir(root):
        return
    for day in os.listdir(root):
        if not os.path.isdir(os.path.join(root, day)):
            continue
        for fn in os.listdir(os.path.join(root, day)):
            if fn.endswith((".csv", ".jsonl")) and not (day == today and fn.split("_")[1].split(".")[0] == now_key):
                gzip_file(os.path.join(root, day, fn))


def last_saved_trade():
    """(id, time_ms) of the newest trade on disk, or (0, 0)."""
    root = os.path.join(BASE, SYMBOL)
    if not os.path.isdir(root):
        return 0, 0
    for day in sorted(os.listdir(root), reverse=True):
        folder = os.path.join(root, day)
        if not os.path.isdir(folder):
            continue
        hours = sorted({fn.split("_")[1].split(".")[0] for fn in os.listdir(folder) if fn.startswith("trades_")},
                       reverse=True)
        for h in hours:
            best = (0, 0)
            for fn in (f"trades_{h}.csv", f"trades_{h}.csv.gz"):
                p = os.path.join(folder, fn)
                if not os.path.exists(p):
                    continue
                try:
                    opener = gzip.open if fn.endswith(".gz") else open
                    with opener(p, "rt", encoding="utf-8") as f:
                        for line in f:
                            parts = line.split(",")
                            if len(parts) == 5 and parts[0].isdigit():
                                best = max(best, (int(parts[0]), int(parts[1])))
                except (OSError, EOFError, ValueError) as e:
                    log("could not fully read", p, e)
            if best[0]:
                return best
    return 0, 0


class Recorder:
    def __init__(self):
        self.trades = HourFiles("trades", "csv")
        self.books = HourFiles("book", "jsonl")
        self.last_id, self.last_t, self.last_p = 0, 0, None
        self.bids, self.asks = {}, {}
        self.book_state, self.book_buf, self.book_last, self.book_pu = "wait", [], 0, 0
        self.count, self.count_prev, self.t0 = 0, None, time.time()
        self.filling, self.pending, self.fill_future = False, [], None
        self.fill_info = ""
        self.conn, self.connects = {"trades": False, "book": False}, 0
        self.snap, self.retry_at = None, 0
        self.version = ""
        try:
            if VERSION_FILE and os.path.exists(VERSION_FILE):
                self.version = open(VERSION_FILE, encoding="utf-8").read().strip()
        except OSError:
            pass

    # ---------------- trades
    def on_trade(self, d):
        if self.filling:
            self.pending.append(d)
            return
        a = d["a"]
        if a <= self.last_id:
            return
        if self.last_id and a > self.last_id + 1:          # missed trades: download them in the background
            self.pending.append(d)
            self.filling = True
            self.fill_future = asyncio.get_running_loop().run_in_executor(None, self.fill_gap, a)
            return
        self.write_trade(d)

    def after_fill(self):
        self.filling = False
        pend, self.pending = sorted(self.pending, key=lambda x: x["a"]), []
        if pend and pend[0]["a"] > self.last_id + 1:
            log(f"skipping {pend[0]['a'] - self.last_id - 1} trades that could not be downloaded")
            self.last_id = pend[0]["a"] - 1
        for d in pend:
            self.on_trade(d)

    def write_trade(self, d):
        self.trades.write(d["T"], f'{d["a"]},{d["T"]},{d["p"]},{d["q"]},{1 if d["m"] else 0}\n')
        self.last_id, self.last_t, self.last_p = d["a"], d["T"], d["p"]
        self.count += 1

    def fill_gap(self, upto):
        start, calls, got = self.last_id + 1, 0, 0
        log(f"gap: trades {start}..{upto - 1} missing ({upto - start}), downloading")
        while start < upto and calls < MAX_FILL_CALLS:
            try:
                arr = get_json(f"/fapi/v1/aggTrades?symbol={SYMBOL}&fromId={start}&limit=1000")
            except Exception as e:
                log("gap download failed:", e)
                time.sleep(5)
                calls += 1
                continue
            calls += 1
            if not arr:
                break
            for d in arr:
                if d["a"] >= upto:
                    break
                if d["a"] > self.last_id:
                    self.write_trade(d)
                    got += 1
            start = arr[-1]["a"] + 1
            self.fill_info = f"downloading missed trades: {got} of {upto - self.last_id - 1 + got}"
            if len(arr) < 1000:
                break
            time.sleep(FILL_PAUSE)
        self.fill_info = ""
        log(f"gap: downloaded {got} trades" + ("" if self.last_id >= upto - 1 else f", could not get the rest (up to {self.last_id})"))

    # ---------------- order book
    def apply(self, e):
        for p, q in e["b"]:
            k = round(float(p) * 100)
            if float(q) == 0:
                self.bids.pop(k, None)
            else:
                self.bids[k] = float(q)
        for p, q in e["a"]:
            k = round(float(p) * 100)
            if float(q) == 0:
                self.asks.pop(k, None)
            else:
                self.asks[k] = float(q)

    def on_depth(self, e):
        if self.book_state == "wait":
            self.book_buf.append(e)
            del self.book_buf[:-2000]
            return
        if self.book_state == "first":
            if e["u"] < self.book_last:
                return
            if e["U"] <= self.book_last <= e["u"]:
                self.apply(e)
                self.book_pu, self.book_state = e["u"], "live"
                return
            self.book_state = "resync"
            return
        if self.book_state != "live":
            return
        if e["pu"] != self.book_pu:
            self.book_state = "resync"
            return
        self.apply(e)
        self.book_pu = e["u"]

    def install_snapshot(self, d):
        self.bids = {round(float(p) * 100): float(q) for p, q in d["bids"]}
        self.asks = {round(float(p) * 100): float(q) for p, q in d["asks"]}
        self.book_last, self.book_state = d["lastUpdateId"], "first"
        buf, self.book_buf = self.book_buf, []
        for e in buf:
            self.on_depth(e)

    def write_book(self):
        if self.book_state not in ("live", "first") or not self.bids or not self.asks:
            return
        bb, ba = max(self.bids) / 100, min(self.asks) / 100
        mid = (bb + ba) / 2
        lo, hi = mid - BOOK_RANGE, mid + BOOK_RANGE
        rows_b, rows_a = {}, {}
        for k, q in self.bids.items():
            p = k / 100
            if lo <= p <= hi:
                r = round((p // BOOK_STEP) * BOOK_STEP, 2)
                rows_b[r] = rows_b.get(r, 0.0) + q
        for k, q in self.asks.items():
            p = k / 100
            if lo <= p <= hi:
                r = round((p // BOOK_STEP) * BOOK_STEP, 2)
                rows_a[r] = rows_a.get(r, 0.0) + q
        t = int(time.time() * 1000)
        rec = {"t": t, "bb": bb, "ba": ba,
               "b": [[p, round(q, 3)] for p, q in sorted(rows_b.items(), reverse=True)],
               "a": [[p, round(q, 3)] for p, q in sorted(rows_a.items())]}
        self.books.write(t, json.dumps(rec, separators=(",", ":")) + "\n")

    # ---------------- status page (status.json)
    def write_status(self):
        try:
            du = shutil.disk_usage(BASE)
            now = time.time()
            st = {
                "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": SYMBOL,
                "version": self.version,
                "running_for_min": round((now - self.t0) / 60, 1),
                "connected_to_binance": all(self.conn.values()),
                "streams_connected": dict(self.conn),
                "connections_since_start": self.connects,
                "last_trade_utc": datetime.fromtimestamp(self.last_t / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if self.last_t else None,
                "last_trade_age_sec": round(now - self.last_t / 1000) if self.last_t else None,
                "last_price": self.last_p,
                "trades_last_5min": self.count_prev,
                "trades_this_5min_so_far": self.count,
                "book": self.book_state,
                "book_levels": [len(self.bids), len(self.asks)],
                "best_bid_ask": [max(self.bids) / 100, min(self.asks) / 100] if self.bids and self.asks else None,
                "backfill": self.fill_info or ("waiting" if self.filling else "none"),
                "disk_free_gb": round(du.free / 1e9, 1),
                "disk_used_pct": round(100 * du.used / du.total, 1),
                "log": list(LOG_TAIL)[-25:],
            }
            os.makedirs(STATUS_DIR, exist_ok=True)
            tmp = os.path.join(STATUS_DIR, "status.json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(st, f, indent=1)
            os.replace(tmp, os.path.join(STATUS_DIR, "status.json"))
        except Exception as e:      # never let the status page stop the recording
            print("status write failed:", e, flush=True)

    # ---------------- main loop: two streams + housekeeping
    def depth_url(self):
        return f"/fapi/v1/depth?symbol={SYMBOL}&limit=1000"

    def book_opened(self):
        self.book_state, self.book_buf = "wait", []
        self.snap = asyncio.get_running_loop().run_in_executor(None, get_json, self.depth_url())

    async def stream(self, name, url, on_open=None):
        backoff = 1
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=60, max_size=2 ** 22) as ws:
                    self.conn[name], self.connects = True, self.connects + 1
                    log(f"connected ({name})")
                    backoff = 1
                    if on_open:
                        on_open()
                    async for raw in ws:
                        msg = json.loads(raw).get("data", {})
                        e = msg.get("e")
                        if e == "aggTrade":
                            self.on_trade(msg)
                        elif e == "depthUpdate":
                            self.on_depth(msg)
                    reason = "closed by Binance"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
            self.conn[name] = False
            if name == "book":
                self.book_state = "down"
            log(f"{name} connection lost ({reason}); retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def housekeeping(self):
        next_book, next_flush, next_status, next_stat = time.time() + BOOK_EVERY, time.time() + 10, 0, time.time() + 300
        while True:
            await asyncio.sleep(0.25)
            try:
                now = time.time()
                if self.filling and self.fill_future is not None and self.fill_future.done():
                    self.after_fill()
                if self.snap is not None and self.snap.done():
                    snap, self.snap = self.snap, None
                    try:
                        self.install_snapshot(snap.result())
                    except Exception as e:
                        log("book snapshot failed:", e)
                        self.book_state, self.retry_at = "resync", now + 5
                if self.book_state == "resync" and self.snap is None and now >= self.retry_at and self.conn["book"]:
                    self.book_opened()
                if now >= next_book:
                    self.write_book()
                    next_book = now + BOOK_EVERY
                if now >= next_flush:
                    self.trades.flush()
                    self.books.flush()
                    next_flush = now + 10
                if now >= next_status:
                    self.write_status()
                    next_status = now + STATUS_EVERY
                if now >= next_stat:
                    log(f"ok: {self.count} trades in the last 5 min, book {self.book_state}, "
                        f"{len(self.bids)}/{len(self.asks)} levels")
                    self.count_prev, self.count, next_stat = self.count, 0, now + 300
            except Exception as e:          # keep recording whatever happens here
                log(f"housekeeping error {type(e).__name__}: {e}")

    async def run(self):
        os.makedirs(BASE, exist_ok=True)
        compress_leftovers()
        self.last_id, self.last_t = last_saved_trade()
        log(f"recording {SYMBOL} to {os.path.abspath(os.path.join(BASE, SYMBOL))}" +
            (f" - continuing after trade {self.last_id}" if self.last_id else " - starting fresh"))
        self.write_status()
        s = SYMBOL.lower()
        await asyncio.gather(self.stream("trades", WS_TRADES.format(s=s)),
                             self.stream("book", WS_BOOK.format(s=s), self.book_opened),
                             self.housekeeping())


if __name__ == "__main__":
    try:
        asyncio.run(Recorder().run())
    except KeyboardInterrupt:
        log("stopped")
