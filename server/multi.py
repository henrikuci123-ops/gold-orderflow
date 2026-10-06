"""Other gold perpetuals for the footprint (Henri, Oct 2026). Runs on the server as gof-multi.service, 24/7.

Listens to every trade of the gold (XAU) perpetuals on Bybit, OKX, Bitget, Gate and MEXC - together they add about
70% to Binance's own gold volume - and records them, so the app's footprint, value areas and delta can show the
whole crypto-gold market instead of Binance alone:
  <data>/XAUUSDT/YYYY-MM-DD/xtrades_HH.csv(.gz)    venue,time_ms,price,qty_oz,seller_aggressive,raw_price
Prices are moved onto Binance's price: each exchange trades a little above or below Binance (cents to a dollar or two,
changing slowly); that gap is measured all the time against Binance's last trade and taken off, so a trade at
"Binance + gap" lands in the same footprint row as the matching Binance trades.
Status (every 15 s): <www>/multi.json
Needs: python3-websockets (installed by setup.sh). Light on CPU.
"""
import asyncio, gzip, json, os, shutil, threading, time
from datetime import datetime, timezone

import websockets

SYMBOL = "XAUUSDT"
HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("GOF_DATA") or os.path.join(HERE, "..", "orderflow_data")
STATUS_DIR = os.environ.get("GOF_STATUS") or BASE
BIN_WS = "wss://fstream.binance.com/market/stream?streams=xauusdt@aggTrade"
MAX_GAP = 25.0            # $: a gap / print further than this from Binance is a glitch -> ignored


def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a), flush=True)


# ---------------------------------------------------------------- the exchanges
def p_bybit(m):
    if not str(m.get("topic", "")).startswith("publicTrade"):
        return []
    return [(int(d["T"]), float(d["p"]), float(d["v"]), d["S"] == "Sell") for d in m.get("data") or []]


def p_okx(m):
    if (m.get("arg") or {}).get("channel") != "trades" or "data" not in m:
        return []
    return [(int(d["ts"]), float(d["px"]), float(d["sz"]) * 0.001, d["side"] == "sell") for d in m["data"]]


def p_bitget(m):
    if (m.get("arg") or {}).get("channel") != "trade" or m.get("action") != "update":
        return []                                   # the first message is a snapshot of old trades: skip it
    return [(int(d["ts"]), float(d["price"]), float(d["size"]), d["side"] == "sell") for d in m.get("data") or []]


def p_gate(m):
    if m.get("channel") != "futures.trades" or m.get("event") != "update":
        return []
    out = []
    for d in m.get("result") or []:
        t = d.get("create_time_ms") or (float(d.get("create_time", 0)) * 1000)
        sz = float(d["size"])
        out.append((int(float(t)), float(d["price"]), abs(sz) * 0.0001, sz < 0))
    return out


def p_mexc(m):
    if m.get("channel") != "push.deal":
        return []
    data = m.get("data")
    data = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    return [(int(d["t"]), float(d["p"]), float(d["v"]) * 0.001, int(d["T"]) == 2) for d in data]


VENUES = {
    "bybit": dict(url="wss://stream.bybit.com/v5/public/linear", parse=p_bybit, every=20,
                  sub=lambda: {"op": "subscribe", "args": ["publicTrade.XAUUSDT"]},
                  ping=lambda: json.dumps({"op": "ping"})),
    "okx": dict(url="wss://ws.okx.com:8443/ws/v5/public", parse=p_okx, every=25,
                sub=lambda: {"op": "subscribe", "args": [{"channel": "trades", "instId": "XAU-USDT-SWAP"}]},
                ping=lambda: "ping"),
    "bitget": dict(url="wss://ws.bitget.com/v2/ws/public", parse=p_bitget, every=25,
                   sub=lambda: {"op": "subscribe", "args": [{"instType": "USDT-FUTURES", "channel": "trade", "instId": "XAUUSDT"}]},
                   ping=lambda: "ping"),
    "gate": dict(url="wss://fx-ws.gateio.ws/v4/ws/usdt", parse=p_gate, every=20,
                 sub=lambda: {"time": int(time.time()), "channel": "futures.trades", "event": "subscribe", "payload": ["XAU_USDT"]},
                 ping=lambda: json.dumps({"time": int(time.time()), "channel": "futures.ping"})),
    "mexc": dict(url="wss://contract.mexc.com/edge", parse=p_mexc, every=15,
                 sub=lambda: {"method": "sub.deal", "param": {"symbol": "XAU_USDT"}},
                 ping=lambda: json.dumps({"method": "ping"})),
}


# ---------------------------------------------------------------- files
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


class HourFile:
    """xtrades_HH.csv of the current UTC hour (by arrival time); the finished hour is gzipped in the background."""

    def __init__(self):
        self.key, self.f, self.path = None, None, None

    def write(self, line):
        key = datetime.now(timezone.utc).strftime("%Y-%m-%d/%H")
        if key != self.key:
            self.close()
            day, hour = key.split("/")
            folder = os.path.join(BASE, SYMBOL, day)
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"xtrades_{hour}.csv")
            new = not os.path.exists(path)
            self.f = open(path, "a", encoding="utf-8", newline="")
            if new:
                self.f.write("venue,time_ms,price,qty_oz,seller_aggressive,raw_price\n")
            self.key, self.path = key, path
        self.f.write(line)

    def flush(self):
        if self.f:
            self.f.flush()

    def close(self):
        if self.f:
            self.f.close()
            threading.Thread(target=gzip_file, args=(self.path,), daemon=True).start()
        self.f = None


def compress_leftovers():
    now = datetime.now(timezone.utc)
    root = os.path.join(BASE, SYMBOL)
    if not os.path.isdir(root):
        return
    for day in os.listdir(root):
        folder = os.path.join(root, day)
        if not os.path.isdir(folder):
            continue
        for fn in os.listdir(folder):
            if fn.startswith("xtrades_") and fn.endswith(".csv") and not (
                    day == now.strftime("%Y-%m-%d") and fn == f"xtrades_{now.strftime('%H')}.csv"):
                gzip_file(os.path.join(folder, fn))


# ---------------------------------------------------------------- recorder
class Multi:
    def __init__(self):
        self.out = HourFile()
        self.bin_p, self.bin_t = None, 0.0          # Binance last trade price / arrival time (s)
        self.st = {v: {"connected": False, "connects": 0, "basis": None, "samples": [], "basis_t": 0.0,
                       "n": 0, "oz": 0.0, "n_prev": None, "oz_prev": None, "last_raw": None, "last_at": 0.0, "err": ""}
                   for v in VENUES}
        self.t0 = time.time()

    def on_binance(self, d):
        self.bin_p, self.bin_t = float(d["p"]), time.time()

    def on_trades(self, v, trades):
        s, now = self.st[v], time.time()
        for t, p, q, sell in trades:
            if q <= 0 or p <= 0:
                continue
            s["last_raw"], s["last_at"] = p, now
            fresh = self.bin_p is not None and now - self.bin_t < 3
            if fresh:
                gap = p - self.bin_p
                if abs(gap) < MAX_GAP:
                    if s["basis"] is None:
                        s["samples"].append(gap)
                        if len(s["samples"]) >= 20:
                            sm = sorted(s["samples"])
                            s["basis"], s["samples"] = sm[len(sm) // 2], []
                            log(f"{v}: gap to Binance {s['basis']:+.2f}")
                    elif now - s["basis_t"] >= 0.5:      # at most 2 updates a second: bursts do not dominate
                        s["basis"] += 0.02 * (gap - s["basis"])
                        s["basis_t"] = now
            if s["basis"] is None:
                continue
            pn = p - s["basis"]
            if self.bin_p is not None and abs(pn - self.bin_p) > MAX_GAP:
                continue
            self.out.write(f"{v},{t},{pn:.2f},{q:.4f},{1 if sell else 0},{p}\n")
            s["n"] += 1
            s["oz"] += q

    async def binance(self):
        backoff = 1
        while True:
            try:
                async with websockets.connect(BIN_WS, ping_interval=20, ping_timeout=60) as ws:
                    backoff = 1
                    log("connected (binance reference)")
                    async for raw in ws:
                        d = json.loads(raw).get("data", {})
                        if d.get("e") == "aggTrade":
                            self.on_binance(d)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"binance reference lost ({type(e).__name__}: {e})")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def venue(self, v):
        cfg, s, backoff = VENUES[v], self.st[v], 1
        while True:
            pinger = None
            try:
                async with websockets.connect(cfg["url"], ping_interval=None, max_size=2 ** 22, open_timeout=20) as ws:
                    await ws.send(json.dumps(cfg["sub"]()))
                    s["connected"], s["connects"], s["err"] = True, s["connects"] + 1, ""
                    backoff = 1
                    log(f"connected ({v})")

                    async def ping_loop():
                        while True:
                            await asyncio.sleep(cfg["every"])
                            await ws.send(cfg["ping"]())
                    pinger = asyncio.ensure_future(ping_loop())
                    last_msg = time.time()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=90)
                        except asyncio.TimeoutError:
                            raise RuntimeError("no data for 90 s")
                        last_msg = time.time()
                        if not isinstance(raw, str) or raw[:1] not in "{[":
                            continue                       # "pong" etc.
                        try:
                            m = json.loads(raw)
                        except ValueError:
                            continue
                        if isinstance(m, dict):
                            try:
                                tr = cfg["parse"](m)
                            except (KeyError, TypeError, ValueError) as e:
                                s["err"] = f"parse: {type(e).__name__}: {e}"[:120]
                                continue
                            if tr:
                                self.on_trades(v, tr)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                s["err"] = f"{type(e).__name__}: {e}"[:160]
                log(f"{v} lost ({s['err']}); retry in {backoff}s")
            finally:
                if pinger:
                    pinger.cancel()
            s["connected"] = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    def write_status(self):
        now = time.time()
        st = {"updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
              "running_for_min": round((now - self.t0) / 60, 1),
              "binance": {"last": self.bin_p, "age_s": round(now - self.bin_t, 1) if self.bin_t else None},
              "venues": {}}
        for v, s in self.st.items():
            st["venues"][v] = {"connected": s["connected"], "connects": s["connects"],
                               "gap_to_binance": round(s["basis"], 3) if s["basis"] is not None else None,
                               "trades_last_5min": s["n_prev"], "oz_last_5min": round(s["oz_prev"], 2) if s["oz_prev"] is not None else None,
                               "trades_this_5min": s["n"], "last_price": s["last_raw"],
                               "last_trade_age_s": round(now - s["last_at"], 1) if s["last_at"] else None, "error": s["err"]}
        try:
            os.makedirs(STATUS_DIR, exist_ok=True)
            tmp = os.path.join(STATUS_DIR, "multi.json.tmp")
            with open(tmp, "w") as f:
                json.dump(st, f, indent=1)
            os.replace(tmp, os.path.join(STATUS_DIR, "multi.json"))
        except OSError as e:
            log("status failed", e)

    async def housekeeping(self):
        next_stat = time.time() + 300
        while True:
            await asyncio.sleep(5)
            try:
                self.out.flush()
                if time.time() >= next_stat:
                    for s in self.st.values():
                        s["n_prev"], s["oz_prev"], s["n"], s["oz"] = s["n"], s["oz"], 0, 0.0
                    next_stat = time.time() + 300
                    log("5 min: " + ", ".join(f"{v} {s['n_prev']} trades {s['oz_prev']:.0f} oz gap {s['basis']}" for v, s in self.st.items()))
                self.write_status()
            except Exception as e:
                log(f"housekeeping error {type(e).__name__}: {e}")

    async def run(self):
        os.makedirs(BASE, exist_ok=True)
        compress_leftovers()
        log(f"recording {', '.join(VENUES)} to {os.path.abspath(os.path.join(BASE, SYMBOL))}")
        await asyncio.gather(self.binance(), self.housekeeping(), *(self.venue(v) for v in VENUES))


if __name__ == "__main__":
    try:
        asyncio.run(Multi().run())
    except KeyboardInterrupt:
        log("stopped")
