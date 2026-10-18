"""Burst strat - live paper trading (Henri, Oct 2026). Runs on the server as gof-burst.service, 24/7.

Same rules as the backtest on the recorded Binance ticks (Oct 1-16 2026), checked once per second:
  1. Burst: price moved >= $3 in the last 30 s, that 30 s traded >= 2x a normal 30 s (average of the 30 min before)
     and >= 60% of it was aggressors on the move's side. Only when the burst happens 09:00-22:00 Tirana, Monday-Friday.
  2. Wait for the volume to calm: from 10 s after the burst, the last 10 s trade at <= 30% of the burst's pace.
     Not calm within 2 minutes -> skip it.
  3. Fade it (sell an up-burst / buy a down-burst) 1 s later, 0.2 lot.
     2nd bet 0.2 lot if price goes $8 further against -> the stop goes $5 past the 2nd bet; when price is back at
     break even the stop locks there and trails $2 behind the best price.
     1st bet alone: trailing stop from +$5 profit, $2 behind. Max hold 30 minutes.
  P&L: Binance prices, minus $0.60/oz per bet (spread + slippage + ~$5 broker fee at 0.2 lot), $0.10 extra on stops.
  The MT5 quote (GoldPriceBridge) at entry and exit is stored too, to compare with the broker's price.

Version B (Oct 18 2026) runs side by side on the same trades: identical, except that when the volume has calmed it
only fades if the burst looks exhausted -
  * the order book leans against the burst: resting orders within $1.5 on the far side (asks for an up-burst)
    >= the near side (bids), from the recorder's 2-second book snapshot (<status>/book_now.json), and
  * price has already pulled back >= $0.50 from the burst's extreme.
  Otherwise B skips that burst. Version A records the same two numbers on its trades, for later analysis.

Writes  <status>/burst/YYYY-MM-DD.jsonl   version A: one line per finished trade or skipped burst (Tirana day)
        <status>/burst/B/YYYY-MM-DD.jsonl version B
        <status>/burst/state.json         what each version is doing now, today's trades, every day's totals
On start it replays the last ~80 minutes from the recorder's files (warms up the 'normal volume' and picks up a
trade that was open), without writing anything twice.
"""
import asyncio, glob, json, os, sys, time
from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import fpcore as F

WS = "wss://fstream.binance.com/market/stream?streams={s}@aggTrade"
WWW = os.environ.get("GOF_STATUS") or F.BASE
OUT = os.path.join(WWW, "burst")
MT5 = os.path.join(WWW, "context", "mt5.json")
BOOK_NOW = os.path.join(WWW, "book_now.json")
TZ = ZoneInfo("Europe/Tirane")

# ---- rules (dollars / seconds) ----
W, MOVE, SHARE, VOLX, NORM = 30, 3.0, 0.6, 2.0, 1800      # burst: $3 in 30 s, 60% one side, 2x normal 30 s
CALM_W, CALM_F, CALM_MIN, CALM_MAX = 10, 0.3, 10, 120     # calm: last 10 s <= 30% of burst pace, 10..120 s after
ADD, STOP_PAST, ACT, TRAIL, TRAIL2 = 8.0, 5.0, 5.0, 2.0, 2.0
MAX_HOLD = 1800
COST, STOP_SLIP, OZ = 0.60, 0.10, 20                      # per oz per bet; 0.2 lot = 20 oz per bet
H0, H1 = 9, 22                                            # Tirana hours for the burst
KEEP = NORM + W + CALM_MAX + 60
B_BOOK, B_PULL, BOOK_NEAR, BOOK_AGE = 1.0, 0.5, 1.5, 10   # version B: book ratio >= 1, pulled back >= $0.50


def book_sides(rec):
    """resting size within $1.5 of the best bid / ask (recorder rows are $0.25 buckets)"""
    bb, ba = rec["bb"], rec["ba"]
    return (sum(q for p, q in rec["b"] if p >= bb - BOOK_NEAR), sum(q for p, q in rec["a"] if p <= ba + BOOK_NEAR))


def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a), flush=True)


def tir(sec):
    return datetime.fromtimestamp(sec, TZ)


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


def mt5_quote():
    try:
        with open(MT5) as f:
            d = json.load(f)
        if time.time() * 1000 - d["recv"] < 15000 and d.get("age", 0) < 120:
            return {"bid": d["bid"], "ask": d["ask"]}
    except Exception:
        pass
    return None


class Engine:
    """Per-second state machine. Feed trades with add(); call tick() every second (also when there are no trades)."""

    def __init__(self, emit, variant="A", book_at=None):
        self.emit = emit                  # emit(rec) for finished trades / skipped bursts
        self.variant, self.book_at = variant, book_at
        self.buf = deque()                # closed seconds: (sec, cumulative buy, cumulative sell, price, high, low)
        self.hi_cur = self.lo_cur = None
        self.cb = self.cs = 0.0
        self.cur = None                   # open second
        self.cb_cur = self.cs_cur = 0.0
        self.px = 0.0
        self.mode, self.cool = "idle", 0
        self.burst = self.trade = None
        self.live = True                  # False while replaying recorded trades
        self.n_trades = 0

    # ---------------------------------------------------------------- input
    def add(self, t_ms, px, q, sell):
        s = t_ms // 1000
        if self.cur is None:
            self.cur = s
        if s > self.cur:
            self.advance(s)
        if sell:
            self.cs_cur += q
        else:
            self.cb_cur += q
        self.px = px
        if self.hi_cur is None or px > self.hi_cur:
            self.hi_cur = px
        if self.lo_cur is None or px < self.lo_cur:
            self.lo_cur = px
        self.n_trades += 1

    def tick(self, now_ms):
        if self.cur is not None:
            self.advance(now_ms // 1000)

    def advance(self, upto):
        """close every second before 'upto'"""
        if upto - self.cur > 3 * 86400:                  # very long gap: restart the buffer
            self.buf.clear()
            self.cur = upto - KEEP
        while self.cur < upto:
            self.cb += self.cb_cur
            self.cs += self.cs_cur
            self.buf.append((self.cur, self.cb, self.cs, self.px, self.hi_cur or self.px, self.lo_cur or self.px))
            self.cb_cur = self.cs_cur = 0.0
            self.hi_cur = self.lo_cur = None
            while len(self.buf) > KEEP:
                self.buf.popleft()
            if self.px > 0:
                self.process(self.cur)
            self.cur += 1

    # ---------------------------------------------------------------- helpers
    def at(self, sec):
        i = sec - self.buf[0][0]
        return self.buf[i] if 0 <= i < len(self.buf) else None

    def vol(self, a, b):                                 # volume of seconds (a, b]
        x, y = self.at(a), self.at(b)
        return (y[1] - x[1], y[2] - x[2])

    # ---------------------------------------------------------------- the rules
    def process(self, k):
        if self.mode == "trade":
            return self.step_trade(k)
        if self.mode == "enter":
            return self.enter(k)
        if self.mode == "wait":
            b = self.burst
            if k < b["k"] + CALM_MIN:
                return
            v = sum(self.vol(k - CALM_W, k))
            if v / CALM_W <= CALM_F * b["pace"]:
                b["calm"] = k
                self.exhaustion(b, k)
                if self.variant == "B" and not b["ok_b"]:
                    self.finish_skip(b, b["why_b"])
                    self.mode, self.burst, self.cool = "idle", None, k + 1
                    return
                self.mode = "enter"
                return
            if k < b["k"] + CALM_MAX:
                return
            self.finish_skip(b)
            self.mode, self.burst, self.cool = "idle", None, b["k"] + CALM_MAX
        if k < self.cool or self.at(k - NORM - W) is None:
            return
        tk = tir(k)
        if tk.weekday() >= 5 or tk.hour < H0 or tk.hour >= H1:   # Mon-Fri only (like the backtest; gold CFDs are shut at weekends)
            return
        p0, p1 = self.at(k - W)[3], self.at(k)[3]
        if not (p0 > 0 and p1 > 0) or abs(p1 - p0) < MOVE:
            return
        B, S = self.vol(k - W, k)
        tot = B + S
        avg = sum(self.vol(k - W - NORM, k - W)) / (NORM / W)
        d = 1 if p1 > p0 else -1
        share = (B if d > 0 else S) / tot if tot > 0 else 0
        if tot <= 0 or share < SHARE or tot < VOLX * avg:
            return
        self.burst = {"k": k, "move": round(p1 - p0, 2), "dir": d, "pace": tot / W, "volx": round(tot / avg, 1) if avg > 0 else None,
                      "share": round(share, 2), "from": p0, "to": p1}
        self.mode = "wait"

    def exhaustion(self, b, k):
        """version B's check at the moment the volume has calmed (second k)"""
        u = b["dir"]
        ext = None
        for i in range(b["k"] - W, k + 1):
            x = self.at(i)
            if x is None:
                continue
            v = x[4] if u > 0 else x[5]
            if ext is None or (v > ext if u > 0 else v < ext):
                ext = v
        b["ext"] = ext
        b["pull"] = round(u * (ext - self.at(k)[3]), 2)
        sides = self.book_at(k) if self.book_at else None
        b["book"] = None if sides is None or min(sides) <= 0 else round((sides[1] / sides[0]) if u > 0 else (sides[0] / sides[1]), 2)
        why = []
        if b["book"] is None:
            why.append("no order book")
        elif b["book"] < B_BOOK:
            why.append(f"book still with the burst ({b['book']:.2f})")
        if b["pull"] < B_PULL:
            why.append(f"no pullback yet (${b['pull']:.2f})")
        b["ok_b"], b["why_b"] = not why, "B: " + ", ".join(why)

    def enter(self, k):
        b = self.burst
        e = self.at(k)[3]
        d = -b["dir"]
        self.trade = {"b": b, "d": d, "k": k, "e": e, "p2": e - d * ADD, "nb": 1, "best": e, "on": False, "on2": False,
                      "best2": None, "stop": None, "tgt": None, "mfe": 0.0, "mae": 0.0, "t2": None,
                      "mt5_in": mt5_quote() if self.live else None}
        self.mode = "trade"

    def step_trade(self, j):
        T = self.trade
        d, e, p = T["d"], T["e"], self.at(j)[3]
        T["mfe"] = max(T["mfe"], d * (p - e))
        T["mae"] = max(T["mae"], -d * (p - e))
        if T["nb"] == 1 and d * (p - T["p2"]) <= 0:
            T["nb"], T["on"], T["on2"], T["best2"], T["t2"] = 2, False, False, p, j
            T["stop"] = T["p2"] - d * STOP_PAST
            T["tgt"] = (e + T["p2"]) / 2 + d * COST / 2
        if T["stop"] is not None and d * (p - T["stop"]) <= 0:
            why = "full stop" if (T["nb"] == 2 and not T["on2"]) else ("trail after 2nd bet" if T["nb"] == 2 else "trailing profit")
            return self.finish_trade(j, T["stop"] - d * STOP_SLIP, why)
        if T["nb"] == 2:
            if d * (p - T["best2"]) > 0:
                T["best2"] = p
            if not T["on2"] and d * (p - T["tgt"]) >= 0:
                T["on2"], T["stop"] = True, T["tgt"]
            if T["on2"]:
                ns = T["best2"] - d * TRAIL2
                if d * (ns - T["stop"]) > 0:
                    T["stop"] = ns
        else:
            if d * (p - T["best"]) > 0:
                T["best"] = p
            if d * (T["best"] - e) >= ACT:
                T["on"] = True
            if T["on"]:
                ns = T["best"] - d * TRAIL
                if T["stop"] is None or d * (ns - T["stop"]) > 0:
                    T["stop"] = ns
        if j >= T["k"] + MAX_HOLD:
            return self.finish_trade(j, p, "30 min max hold")

    def pnl(self, T, xp):
        d = T["d"]
        r = d * (xp - T["e"]) - COST
        if T["nb"] == 2:
            r += d * (xp - T["p2"]) - COST
        return r * OZ

    def finish_trade(self, j, xp, why):
        T, b = self.trade, self.trade["b"]
        usd = self.pnl(T, xp)
        rec = {"kind": "trade", "id": f"t{T['k']}", "day": tir(b["k"]).strftime("%Y-%m-%d"),
               "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "burst": b["move"], "volx": b["volx"], "share": b["share"],
               "wait_s": b["calm"] - b["k"], "side": "SELL" if T["d"] < 0 else "BUY",
               "in_t": tir(T["k"]).strftime("%H:%M:%S"), "entry": round(T["e"], 2), "add_px": round(T["p2"], 2),
               "bet2": T["nb"] == 2, "bet2_t": tir(T["t2"]).strftime("%H:%M:%S") if T["t2"] else None,
               "out_t": tir(j).strftime("%H:%M:%S"), "exit": round(xp, 2), "why": why, "usd": round(usd, 2),
               "hold_min": round((j - T["k"]) / 60, 1), "mfe": round(T["mfe"], 2), "mae": round(T["mae"], 2),
               "mt5_in": T["mt5_in"], "mt5_out": mt5_quote() if self.live else None, "t": T["k"] * 1000,
               "v": self.variant, "book": b.get("book"), "pull": b.get("pull")}
        self.mode, self.trade, self.burst, self.cool = "idle", None, None, j + 1
        self.emit(rec)

    def finish_skip(self, b, why="volume did not calm within 2 min"):
        self.emit({"kind": "skip", "id": f"s{b['k']}", "day": tir(b["k"]).strftime("%Y-%m-%d"),
                   "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "burst": b["move"], "volx": b["volx"], "share": b["share"],
                   "why": why, "t": b["k"] * 1000, "v": self.variant, "book": b.get("book"), "pull": b.get("pull")})

    # ---------------------------------------------------------------- for the app
    def now_state(self):
        if self.mode == "trade":
            T = self.trade
            p = self.px
            return {"mode": "trade", "side": "SELL" if T["d"] < 0 else "BUY", "in_t": tir(T["k"]).strftime("%H:%M:%S"),
                    "entry": round(T["e"], 2), "add_px": round(T["p2"], 2), "bet2": T["nb"] == 2, "price": round(p, 2),
                    "stop": round(T["stop"], 2) if T["stop"] is not None else None, "usd": round(self.pnl(T, p), 2),
                    "burst": T["b"]["move"], "min": round((self.cur - T["k"]) / 60, 1)}
        if self.mode in ("wait", "enter"):
            b = self.burst
            return {"mode": "wait", "burst": b["move"], "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "volx": b["volx"],
                    "secs": (self.cur or b["k"]) - b["k"]}
        t = tir(time.time())
        return {"mode": "idle", "hours": t.weekday() < 5 and H0 <= t.hour < H1}


class Book:
    """the recorder's 2-second order-book snapshots, reduced to (second, near bids, near asks)"""

    def __init__(self):
        self.snaps = deque(maxlen=6000)
        self.last_t = 0

    def add(self, rec):
        if rec.get("t", 0) <= self.last_t or not rec.get("b") or not rec.get("a"):
            return
        self.last_t = rec["t"]
        self.snaps.append((rec["t"] // 1000, *book_sides(rec)))

    def at(self, sec):
        for s_, b, a in reversed(self.snaps):
            if s_ <= sec:
                return (b, a) if sec - s_ <= BOOK_AGE else None
        return None

    def load_recorded(self, since_ms):
        import gzip
        now = int(time.time() * 1000)
        recs = []
        for h in range(3, -1, -1):
            day, hh = F.day_hour(now - h * 3600000)
            for fn in (f"book_{hh}.jsonl.gz", f"book_{hh}.jsonl"):
                path = os.path.join(F.rec_dir(day), fn)
                if not os.path.exists(path):
                    continue
                try:
                    with (gzip.open if fn.endswith(".gz") else open)(path, "rt") as f:
                        for line in f:
                            try:
                                r = json.loads(line)
                            except ValueError:
                                continue
                            if r.get("t", 0) >= since_ms:
                                recs.append(r)
                except (OSError, EOFError):
                    pass
        for r in sorted(recs, key=lambda r: r["t"]):
            self.add(r)
        return len(recs)

    def poll(self):
        try:
            with open(BOOK_NOW) as f:
                self.add(json.load(f))
        except (OSError, ValueError, KeyError, TypeError):
            pass


class Runner:
    def __init__(self):
        self.book = Book()
        self.out = {"A": OUT, "B": os.path.join(OUT, "B")}
        self.engines, self.seen, self.days = {}, {}, {}
        for v, d in self.out.items():
            os.makedirs(d, exist_ok=True)
            self.engines[v] = Engine(lambda rec, v=v: self.on_rec(v, rec), v, self.book.at)
            self.seen[v] = set()
            for p in sorted(glob.glob(os.path.join(d, "*.jsonl")))[-3:]:
                for r in self.read(p):
                    self.seen[v].add(r.get("id"))
            self.days[v] = {}
            self.rebuild_days(v)
        self.engine = self.engines["A"]
        self.last_id = 0
        self.connected = False

    @staticmethod
    def read(path):
        out = []
        try:
            with open(path) as f:
                for line in f:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
        except OSError:
            pass
        return out

    def on_rec(self, v, rec):
        if rec["id"] in self.seen[v]:
            return
        self.seen[v].add(rec["id"])
        with open(os.path.join(self.out[v], rec["day"] + ".jsonl"), "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        if rec["kind"] == "trade":
            log(f"{v} trade {rec['day']} {rec['in_t']} {rec['side']} {rec['entry']} -> {rec['exit']} ({rec['why']}) {rec['usd']:+.2f}")
        else:
            log(f"{v} skipped burst {rec['day']} {rec['burst_t']} {rec['burst']:+.2f}: {rec['why']}")
        self.rebuild_days(v, rec["day"])

    @staticmethod
    def summary(recs):
        tr = [r for r in recs if r["kind"] == "trade"]
        w = [r for r in tr if r["usd"] > 0]
        return {"n": len(tr), "wins": len(w), "losses": len(tr) - len(w), "usd": round(sum(r["usd"] for r in tr), 2),
                "bet2": sum(1 for r in tr if r["bet2"]), "full_stops": sum(1 for r in tr if r["why"] == "full stop"),
                "skips": sum(1 for r in recs if r["kind"] == "skip"),
                "best": max((r["usd"] for r in tr), default=None), "worst": min((r["usd"] for r in tr), default=None)}

    def rebuild_days(self, v, only=None):
        for p in sorted(glob.glob(os.path.join(self.out[v], "*.jsonl"))):
            day = os.path.basename(p)[:10]
            if only is None or day == only:
                self.days[v][day] = self.summary(self.read(p))

    def block(self, v, today):
        recs = self.read(os.path.join(self.out[v], today + ".jsonl"))
        days = [dict(day=k, **x) for k, x in sorted(self.days[v].items(), reverse=True)][:60]
        tr_days = [x for x in days if x["n"] > 0]
        allsum = {"days": len(tr_days), "n": sum(x["n"] for x in tr_days), "wins": sum(x["wins"] for x in tr_days),
                  "usd": round(sum(x["usd"] for x in tr_days), 2), "green_days": sum(1 for x in tr_days if x["usd"] > 0),
                  "full_stops": sum(x["full_stops"] for x in tr_days), "bet2": sum(x["bet2"] for x in tr_days)}
        return {"now": self.engines[v].now_state(), "today": {"day": today, **self.summary(recs), "list": recs[::-1]},
                "days": days, "all": allsum}

    def write_state(self):
        today = tir(time.time()).strftime("%Y-%m-%d")
        st = {"updated": int(time.time() * 1000), "connected": self.connected, "trades_seen": self.engine.n_trades,
              "book_age": round(time.time() - self.book.last_t / 1000, 1) if self.book.last_t else None,
              **self.block("A", today),
              "rules": "Burst: $3+ in 30 s on 2x normal volume, 60%+ one side (Mon-Fri 09-22 Tirana). Wait until the last 10 s "
                       "trade at <30% of the burst pace (max 2 min), then fade it: 0.2 lot; 2nd 0.2 lot at -$8, stop $5 past "
                       "it, trail $2 from break even; 1st bet trails $2 from +$5; max 30 min. Paper trades on Binance prices, "
                       "after ~$0.60/oz per bet (spread + ~$5 fee).",
              "B": {**self.block("B", today),
                    "rules": "Version B: same, but when the volume has calmed it only fades if the order book leans against "
                             "the burst (resting size within $1.5 on the far side >= the near side) and price has already "
                             "pulled back $0.50+ from the burst's extreme. Otherwise it skips the burst."}}
        write_json(os.path.join(OUT, "state.json"), st)

    def replay(self):
        now = int(time.time() * 1000)
        nb = self.book.load_recorded(now - 85 * 60000)
        trades = []
        for h in range(3, -1, -1):
            day, hh = F.day_hour(now - h * 3600000)
            trades += F.recorded_trades(day, hh)
        trades = sorted({t[0]: t for t in trades if t[1] >= now - 80 * 60000}.values(), key=lambda t: (t[1], t[0]))
        for e in self.engines.values():
            e.live = False
        for a, t, pc, q, sell in trades:
            for e in self.engines.values():
                e.add(t, pc / 100, q, sell)
            self.last_id = a
        for e in self.engines.values():
            e.tick(int(time.time() * 1000))
            e.live = True
        log(f"replay: {len(trades)} recorded trades, {nb} book snapshots from the last 80 min; "
            f"now A {self.engines['A'].mode}, B {self.engines['B'].mode}")

    def on_trade(self, d):
        a = d["a"]
        if a <= self.last_id:
            return
        self.last_id = a
        for e in self.engines.values():
            e.add(int(d["T"]), float(d["p"]), float(d["q"]), bool(d["m"]))

    async def stream(self):
        import websockets
        backoff = 1
        while True:
            try:
                async with websockets.connect(WS.format(s=F.SYMBOL.lower()), ping_interval=20, ping_timeout=60) as ws:
                    self.connected, backoff = True, 1
                    log("connected")
                    async for raw in ws:
                        d = json.loads(raw).get("data", {})
                        if d.get("e") == "aggTrade":
                            self.on_trade(d)
                    reason = "closed by Binance"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
            self.connected = False
            log(f"connection lost ({reason}); retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def clock(self):
        n = 0
        while True:
            await asyncio.sleep(1)
            self.book.poll()
            for e in self.engines.values():
                e.tick(int(time.time() * 1000))
            n += 1
            if n % 2 == 0:
                try:
                    self.write_state()
                except Exception as e:
                    log(f"state write failed: {e}")

    async def run(self):
        self.replay()
        self.write_state()
        await asyncio.gather(self.stream(), self.clock())


if __name__ == "__main__":
    log(f"burst strat paper trader: writing to {os.path.abspath(OUT)}")
    try:
        asyncio.run(Runner().run())
    except KeyboardInterrupt:
        sys.exit(0)
