"""Burst strat - live paper trading (Henri, Oct 2026). Runs on the server as gof-burst.service, 24/7.

Same rules as the backtest on the recorded Binance ticks (Oct 1-16 2026), checked once per second:
  1. Burst: price moved >= $3 in the last 30 s, that 30 s traded >= 2x a normal 30 s (average of the 30 min before)
     and >= 60% of it was aggressors on the move's side. Only when the burst happens 09:00-22:00 Tirana.
  2. Wait for the volume to calm: from 10 s after the burst, the last 10 s trade at <= 30% of the burst's pace.
     Not calm within 2 minutes -> skip it.
  3. Fade it (sell an up-burst / buy a down-burst) 1 s later, 0.2 lot.
     2nd bet 0.2 lot if price goes $8 further against -> the stop goes $5 past the 2nd bet; when price is back at
     break even the stop locks there and trails $2 behind the best price.
     1st bet alone: trailing stop from +$5 profit, $2 behind. Max hold 30 minutes.
  P&L: Binance prices, minus $0.60/oz per bet (spread + slippage + ~$5 broker fee at 0.2 lot), $0.10 extra on stops.
  The MT5 quote (GoldPriceBridge) at entry and exit is stored too, to compare with the broker's price.

Writes  <status>/burst/YYYY-MM-DD.jsonl   one line per finished trade or skipped burst (Tirana day of the burst)
        <status>/burst/state.json         what it is doing now, the open trade, today's trades, every day's totals
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
TZ = ZoneInfo("Europe/Tirane")

# ---- rules (dollars / seconds) ----
W, MOVE, SHARE, VOLX, NORM = 30, 3.0, 0.6, 2.0, 1800      # burst: $3 in 30 s, 60% one side, 2x normal 30 s
CALM_W, CALM_F, CALM_MIN, CALM_MAX = 10, 0.3, 10, 120     # calm: last 10 s <= 30% of burst pace, 10..120 s after
ADD, STOP_PAST, ACT, TRAIL, TRAIL2 = 8.0, 5.0, 5.0, 2.0, 2.0
MAX_HOLD = 1800
COST, STOP_SLIP, OZ = 0.60, 0.10, 20                      # per oz per bet; 0.2 lot = 20 oz per bet
H0, H1 = 9, 22                                            # Tirana hours for the burst
KEEP = NORM + W + CALM_MAX + 60


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

    def __init__(self, emit):
        self.emit = emit                  # emit(rec) for finished trades / skipped bursts
        self.buf = deque()                # closed seconds: (sec, cumulative buy, cumulative sell, price)
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
            self.buf.append((self.cur, self.cb, self.cs, self.px))
            self.cb_cur = self.cs_cur = 0.0
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
                self.mode = "enter"
                return
            if k < b["k"] + CALM_MAX:
                return
            self.finish_skip(b)
            self.mode, self.burst, self.cool = "idle", None, b["k"] + CALM_MAX
        if k < self.cool or self.at(k - NORM - W) is None:
            return
        h = tir(k).hour
        if h < H0 or h >= H1:
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
               "mt5_in": T["mt5_in"], "mt5_out": mt5_quote() if self.live else None, "t": T["k"] * 1000}
        self.mode, self.trade, self.burst, self.cool = "idle", None, None, j + 1
        self.emit(rec)

    def finish_skip(self, b):
        self.emit({"kind": "skip", "id": f"s{b['k']}", "day": tir(b["k"]).strftime("%Y-%m-%d"),
                   "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "burst": b["move"], "volx": b["volx"], "share": b["share"],
                   "why": "volume did not calm within 2 min", "t": b["k"] * 1000})

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
        h = tir(time.time()).hour
        return {"mode": "idle", "hours": H0 <= h < H1}


class Runner:
    def __init__(self):
        os.makedirs(OUT, exist_ok=True)
        self.engine = Engine(self.on_rec)
        self.last_id = 0
        self.connected = False
        self.seen = set()
        for p in sorted(glob.glob(os.path.join(OUT, "*.jsonl")))[-3:]:
            for r in self.read(p):
                self.seen.add(r.get("id"))
        self.days = {}
        self.rebuild_days()

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

    def on_rec(self, rec):
        if rec["id"] in self.seen:
            return
        self.seen.add(rec["id"])
        with open(os.path.join(OUT, rec["day"] + ".jsonl"), "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        if rec["kind"] == "trade":
            log(f"trade {rec['day']} {rec['in_t']} {rec['side']} {rec['entry']} -> {rec['exit']} ({rec['why']}) {rec['usd']:+.2f}")
        else:
            log(f"skipped burst {rec['day']} {rec['burst_t']} {rec['burst']:+.2f}")
        self.rebuild_days(rec["day"])

    @staticmethod
    def summary(recs):
        tr = [r for r in recs if r["kind"] == "trade"]
        w = [r for r in tr if r["usd"] > 0]
        return {"n": len(tr), "wins": len(w), "losses": len(tr) - len(w), "usd": round(sum(r["usd"] for r in tr), 2),
                "bet2": sum(1 for r in tr if r["bet2"]), "full_stops": sum(1 for r in tr if r["why"] == "full stop"),
                "skips": sum(1 for r in recs if r["kind"] == "skip"),
                "best": max((r["usd"] for r in tr), default=None), "worst": min((r["usd"] for r in tr), default=None)}

    def rebuild_days(self, only=None):
        files = sorted(glob.glob(os.path.join(OUT, "*.jsonl")))
        for p in files:
            day = os.path.basename(p)[:10]
            if only is None or day == only:
                self.days[day] = self.summary(self.read(p))

    def write_state(self):
        today = tir(time.time()).strftime("%Y-%m-%d")
        recs = self.read(os.path.join(OUT, today + ".jsonl"))
        days = [dict(day=k, **v) for k, v in sorted(self.days.items(), reverse=True)][:60]
        tr_days = [x for x in days if x["n"] > 0]
        allsum = {"days": len(tr_days), "n": sum(x["n"] for x in tr_days), "wins": sum(x["wins"] for x in tr_days),
                  "usd": round(sum(x["usd"] for x in tr_days), 2), "green_days": sum(1 for x in tr_days if x["usd"] > 0),
                  "full_stops": sum(x["full_stops"] for x in tr_days)}
        write_json(os.path.join(OUT, "state.json"), {
            "updated": int(time.time() * 1000), "connected": self.connected, "trades_seen": self.engine.n_trades,
            "now": self.engine.now_state(), "today": {"day": today, **self.summary(recs), "list": recs[::-1]},
            "days": days, "all": allsum,
            "rules": "Burst: $3+ in 30 s on 2x normal volume, 60%+ one side (09-22 Tirana). Wait until the last 10 s "
                     "trade at <30% of the burst pace (max 2 min), then fade it: 0.2 lot; 2nd 0.2 lot at -$8, stop $5 past "
                     "it, trail $2 from break even; 1st bet trails $2 from +$5; max 30 min. Paper trades on Binance prices, "
                     "after ~$0.60/oz per bet (spread + ~$5 fee)."})

    def replay(self):
        now = int(time.time() * 1000)
        trades = []
        for h in range(3, -1, -1):
            day, hh = F.day_hour(now - h * 3600000)
            trades += F.recorded_trades(day, hh)
        trades = sorted({t[0]: t for t in trades if t[1] >= now - 80 * 60000}.values(), key=lambda t: (t[1], t[0]))
        self.engine.live = False
        for a, t, pc, q, sell in trades:
            self.engine.add(t, pc / 100, q, sell)
            self.last_id = a
        self.engine.tick(int(time.time() * 1000))
        self.engine.live = True
        log(f"replay: {len(trades)} recorded trades from the last 80 min; now {self.engine.mode}")

    def on_trade(self, d):
        a = d["a"]
        if a <= self.last_id:
            return
        self.last_id = a
        self.engine.add(int(d["T"]), float(d["p"]), float(d["q"]), bool(d["m"]))

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
            self.engine.tick(int(time.time() * 1000))
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
