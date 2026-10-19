"""Burst strat - live paper trading (Henri, Oct 2026). Runs on the server as gof-burst.service, 24/7.

Same rules as the backtest on the recorded Binance ticks (Oct 1-16 2026), checked once per second:
  1. Burst: price moved >= $3 in the last 30 s, that 30 s traded >= 2x a normal 30 s (average of the 30 min before)
     and >= 60% of it was aggressors on the move's side. Only when the burst happens 09:00-22:00 Tirana, Monday-Friday.
  2. Wait for the volume to calm: from 10 s after the burst, the last 10 s trade at <= 30% of the burst's pace.
     Not calm within 2 minutes -> skip it.
  3. Fade it (sell an up-burst / buy a down-burst) 1 s later, 0.2 lot.
     2nd bet 0.2 lot if price goes $8 further against -> the stop goes $5 past the 2nd bet; when price is back at
     break even the stop locks there and trails $2 behind the best price.
     Up to 2 trades open at the same time (Oct 19 2026), each with its own 2nd bet / stop / trailing. The 2nd slot
     is for a NEW opportunity while a trade runs long, not for fading the same move again: a burst is only taken
     if every open trade has been running for at least 15 minutes (Henri's rule); otherwise it is skipped.
     No new trades from 45 min before until 30 min after a high- or medium-impact US news release (ForexFactory calendar,
     <status>/context/calendar.json), so no trade is stuck in the news (Oct 19 2026).
     B and C also need the burst's high (up-burst) / low (down-burst) to have held for 10 s before they fade it.
     1st bet alone: trailing stop from +$5 profit, $2 behind. No time limit (Henri, Oct 19 2026: removed the 30 min
     max hold - only the stop); a trade still open on Friday 22:55 Tirana is closed (broker shut at the weekend).
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
        <status>/burst/C/YYYY-MM-DD.jsonl version C (Oct 19 2026): B's entries, but the 1st bet's trailing stop
                                          starts at +$10 profit and stays $5 behind (A and B: +$5, $2 behind)
        <status>/burst/state.json         what each version is doing now, today's trades, every day's totals
On start it replays the recorder's files - the last ~80 minutes, or from just before the burst of a trade that is
still open (burst/open.json), so a trade of any length survives a restart - without writing anything twice.
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
TRAILS = {"A": (5.0, 2.0), "B": (5.0, 2.0), "C": (10.0, 5.0)}   # 1st bet: trail starts at +$X, $Y behind
MAX_POS = 2                                               # trades open at the same time, each managed on its own
MIN_GAP = 900                                             # 2nd trade only if the open one has run >= 15 min
NEWS_BEFORE, NEWS_AFTER = 45 * 60, 30 * 60                # no new trades around high-impact US news
NO_NEW = {"A": 0, "B": 10, "C": 10}                       # s the burst's extreme must hold before fading it
MAX_HOLD = None                                          # no time limit (was 1800 s until Oct 19)
COST, STOP_SLIP, OZ = 0.60, 0.10, 20                      # per oz per bet; 0.2 lot = 20 oz per bet
H0, H1 = 9, 22                                            # Tirana hours for the burst
KEEP = NORM + W + CALM_MAX + 60
RULES_TAG = "news-nonew-2"                                         # change it to replay today once after a rule change
B_BOOK, B_PULL, BOOK_NEAR, BOOK_AGE = 1.0, 0.5, 1.5, 10   # version B: book ratio >= 1, pulled back >= $0.50


class News:
    """high- and medium-impact US releases from the calendar the context service downloads; kept in burst/news.json too, so
    past events stay known for replays"""

    def __init__(self):
        self.events, self.loaded = {}, 0
        try:
            with open(os.path.join(OUT, "news.json")) as f:
                self.events = {int(k): v for k, v in json.load(f).items()}
        except (OSError, ValueError):
            pass

    def refresh(self):
        if time.time() - self.loaded < 60:
            return
        self.loaded = time.time()
        try:
            with open(os.path.join(WWW, "context", "calendar.json")) as f:
                cal = json.load(f)
        except (OSError, ValueError):
            return
        new = False
        for e in cal.get("events", []):
            if e.get("impact") in ("High", "Medium") and int(e["t"]) // 1000 not in self.events:
                self.events[int(e["t"]) // 1000] = e.get("title", "")
                new = True
        if new:
            try:
                write_json(os.path.join(OUT, "news.json"), {str(k): v for k, v in self.events.items()})
            except OSError:
                pass

    def block(self, sec):
        for t, title in self.events.items():
            if t - NEWS_BEFORE <= sec < t + NEWS_AFTER:
                return f"news: {title} at {tir(t):%H:%M}"
        return None


NEWS = News()


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

    def __init__(self, emit, variant="A", book_at=None, act=ACT, trail=TRAIL):
        self.emit = emit                  # emit(rec) for finished trades / skipped bursts
        self.variant, self.book_at, self.act, self.trail = variant, book_at, act, trail
        self.no_new = NO_NEW.get(variant, 0)
        self.buf = deque()                # closed seconds: (sec, cumulative buy, cumulative sell, price, high, low)
        self.hi_cur = self.lo_cur = None
        self.cb = self.cs = 0.0
        self.cur = None                   # open second
        self.cb_cur = self.cs_cur = 0.0
        self.px = 0.0
        self.mode, self.cool = "idle", 0
        self.burst = None
        self.trades = []                  # open trades (up to MAX_POS at the same time)
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
        n_open = list(self.trades)                       # open at the start of this second
        for T in list(self.trades):
            self.step_trade(T, k)
        if self.mode == "enter":
            self.enter(k, n_open)
        elif self.mode == "wait":
            b = self.burst
            x = self.at(k)
            if (x[4] >= b["ext"]) if b["dir"] > 0 else (x[5] <= b["ext"]):
                b["ext"], b["text"] = (x[4] if b["dir"] > 0 else x[5]), k
            if k < b["k"] + CALM_MIN:
                return
            v = sum(self.vol(k - CALM_W, k))
            if v / CALM_W <= CALM_F * b["pace"] and k - b["text"] >= self.no_new:
                b["calm"] = k
                self.exhaustion(b, k)
                self.cool = k + 1
                if self.variant in ("B", "C") and not b["ok_b"]:
                    self.finish_skip(b, b["why_b"])
                    self.mode, self.burst = "idle", None
                    return
                self.mode = "enter"
                return
            if k < b["k"] + CALM_MAX:
                return
            self.finish_skip(b)
            self.mode, self.burst, self.cool = "idle", None, b["k"] + CALM_MAX
        if self.mode != "idle" or k < self.cool or self.at(k - NORM - W) is None:
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
        ext, text = None, k                               # the burst's extreme so far and when it was made
        for i in range(k - W, k + 1):
            x = self.at(i)
            v = x[4] if d > 0 else x[5]
            if ext is None or (v >= ext if d > 0 else v <= ext):
                ext, text = v, i
        self.burst["ext"], self.burst["text"] = ext, text
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

    def enter(self, k, n_open):
        b = self.burst
        self.mode, self.burst = "idle", None
        nb = NEWS.block(k)
        if nb:
            return self.finish_skip(b, nb)
        if len(n_open) >= MAX_POS:
            return self.finish_skip(b, f"{MAX_POS} trades already open")
        if any(k - T["k"] < MIN_GAP for T in n_open):
            return self.finish_skip(b, "a trade opened < 15 min ago is still running (same move)")
        e = self.at(k)[3]
        d = -b["dir"]
        self.trades.append({"b": b, "d": d, "k": k, "e": e, "p2": e - d * ADD, "nb": 1, "best": e, "on": False, "on2": False,
                            "best2": None, "stop": None, "tgt": None, "mfe": 0.0, "mae": 0.0, "t2": None,
                            "mt5_in": mt5_quote() if self.live else None})

    def step_trade(self, T, j):
        d, e, p = T["d"], T["e"], self.at(j)[3]
        T["mfe"] = max(T["mfe"], d * (p - e))
        T["mae"] = max(T["mae"], -d * (p - e))
        if T["nb"] == 1 and d * (p - T["p2"]) <= 0:
            T["nb"], T["on"], T["on2"], T["best2"], T["t2"] = 2, False, False, p, j
            T["stop"] = T["p2"] - d * STOP_PAST
            T["tgt"] = (e + T["p2"]) / 2 + d * COST / 2
        if T["stop"] is not None and d * (p - T["stop"]) <= 0:
            why = "full stop" if (T["nb"] == 2 and not T["on2"]) else ("trail after 2nd bet" if T["nb"] == 2 else "trailing profit")
            return self.finish_trade(T, j, T["stop"] - d * STOP_SLIP, why)
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
            if d * (T["best"] - e) >= self.act:
                T["on"] = True
            if T["on"]:
                ns = T["best"] - d * self.trail
                if T["stop"] is None or d * (ns - T["stop"]) > 0:
                    T["stop"] = ns
        if MAX_HOLD and j >= T["k"] + MAX_HOLD:
            return self.finish_trade(T, j, p, "max hold")
        tj = tir(j)
        if tj.weekday() == 4 and (tj.hour, tj.minute) >= (22, 55):
            return self.finish_trade(T, j, p, "closed before the weekend")

    def pnl(self, T, xp):
        d = T["d"]
        r = d * (xp - T["e"]) - COST
        if T["nb"] == 2:
            r += d * (xp - T["p2"]) - COST
        return r * OZ

    def finish_trade(self, T, j, xp, why):
        b = T["b"]
        usd = self.pnl(T, xp)
        rec = {"kind": "trade", "id": f"t{T['k']}", "day": tir(b["k"]).strftime("%Y-%m-%d"),
               "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "burst": b["move"], "volx": b["volx"], "share": b["share"],
               "wait_s": b["calm"] - b["k"], "side": "SELL" if T["d"] < 0 else "BUY",
               "in_t": tir(T["k"]).strftime("%H:%M:%S"), "entry": round(T["e"], 2), "add_px": round(T["p2"], 2),
               "bet2": T["nb"] == 2, "bet2_t": tir(T["t2"]).strftime("%H:%M:%S") if T["t2"] else None,
               "out_t": tir(j).strftime("%H:%M:%S"), "exit": round(xp, 2), "why": why, "usd": round(usd, 2),
               "hold_min": round((j - T["k"]) / 60, 1), "mfe": round(T["mfe"], 2), "mae": round(T["mae"], 2),
               "mt5_in": T["mt5_in"], "mt5_out": mt5_quote() if self.live else None, "t": T["k"] * 1000,
               "v": self.variant, "book": b.get("book"), "pull": b.get("pull"), "open_with": len(self.trades) - 1}
        self.trades.remove(T)
        self.emit(rec)

    def finish_skip(self, b, why="volume did not calm within 2 min"):
        self.emit({"kind": "skip", "id": f"s{b['k']}", "day": tir(b["k"]).strftime("%Y-%m-%d"),
                   "burst_t": tir(b["k"]).strftime("%H:%M:%S"), "burst": b["move"], "volx": b["volx"], "share": b["share"],
                   "why": why, "t": b["k"] * 1000, "v": self.variant, "book": b.get("book"), "pull": b.get("pull")})

    # ---------------------------------------------------------------- for the app
    def now_state(self):
        p = self.px
        trades = [{"side": "SELL" if T["d"] < 0 else "BUY", "in_t": tir(T["k"]).strftime("%H:%M:%S"),
                   "entry": round(T["e"], 2), "add_px": round(T["p2"], 2), "bet2": T["nb"] == 2, "price": round(p, 2),
                   "stop": round(T["stop"], 2) if T["stop"] is not None else None, "usd": round(self.pnl(T, p), 2),
                   "burst": T["b"]["move"], "min": round((self.cur - T["k"]) / 60, 1)} for T in self.trades]
        st = {"trades": trades, "max_pos": MAX_POS}
        if trades:
            st.update(trades[0], mode="trade", usd=round(sum(x["usd"] for x in trades), 2))
        elif self.mode in ("wait", "enter"):
            b = self.burst
            st.update(mode="wait", burst=b["move"], burst_t=tir(b["k"]).strftime("%H:%M:%S"), volx=b["volx"],
                      secs=(self.cur or b["k"]) - b["k"])
        else:
            t = tir(time.time())
            st.update(mode="idle", hours=t.weekday() < 5 and H0 <= t.hour < H1)
        if trades and self.mode in ("wait", "enter"):
            st["waiting_burst"] = self.burst["move"]
        return st


class Book:
    """the recorder's 2-second order-book snapshots, reduced to (second, near bids, near asks)"""

    def __init__(self):
        self.snaps = deque(maxlen=60000)
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
        for h in range((now - since_ms) // 3600000 + 1, -1, -1):
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


def apply_fixes(out):
    """corrections to logged trades (server/burst_fixes.json, from the repo): set the given fields on a record"""
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "burst_fixes.json")) as f:
            fixes = json.load(f)
    except (OSError, ValueError):
        return
    for fx in fixes:
        for v in fx.get("versions", ["A"]):
            path = os.path.join(out[v], fx["day"] + ".jsonl")
            try:
                with open(path) as f:
                    lines = f.read().splitlines()
            except OSError:
                continue
            changed = False
            for i, line in enumerate(lines):
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("id") == fx["id"] and any(r.get(k) != x for k, x in fx["set"].items()):
                    r.update(fx["set"])
                    r["fixed"] = fx.get("why_fixed", "corrected")
                    lines[i] = json.dumps(r, separators=(",", ":"))
                    changed = True
            if changed:
                tmp = path + ".tmp"
                with open(tmp, "w") as f:
                    f.write("\n".join(lines) + "\n")
                os.replace(tmp, path)
                log(f"fixed {v} {fx['day']} {fx['id']}: {fx['set']}")


class Runner:
    def __init__(self):
        self.book = Book()
        self.out = {"A": OUT, "B": os.path.join(OUT, "B"), "C": os.path.join(OUT, "C")}
        self.engines, self.seen, self.days = {}, {}, {}
        for d in self.out.values():
            os.makedirs(d, exist_ok=True)
        self.reset_today_on_rule_change()
        apply_fixes(self.out)
        for v, d in self.out.items():
            os.makedirs(d, exist_ok=True)
            self.engines[v] = Engine(lambda rec, v=v: self.on_rec(v, rec), v, self.book.at, *TRAILS[v])
            self.seen[v] = set()
            for p in sorted(glob.glob(os.path.join(d, "*.jsonl")))[-3:]:
                for r in self.read(p):
                    self.seen[v].add(r.get("id"))
            self.days[v] = {}
            self.rebuild_days(v)
        self.engine = self.engines["A"]
        self.last_id = 0
        self.connected = False

    def reset_today_on_rule_change(self):
        """rules changed today: put today's logs aside so the whole day is replayed with the new rules"""
        t = tir(time.time())
        if t.weekday() >= 5 or t.hour < H0:
            return
        day = t.strftime("%Y-%m-%d")
        try:
            if open(os.path.join(OUT, "replayed.txt")).read().strip() == f"{day} {RULES_TAG}":
                return
        except OSError:
            pass
        for v, d in self.out.items():
            path = os.path.join(d, day + ".jsonl")
            if os.path.exists(path):
                os.replace(path, path + f".before-{RULES_TAG}")
                log(f"{v}: today's log set aside ({path}.before-{RULES_TAG}), replaying today with {RULES_TAG}")

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
        recs = sorted(self.read(os.path.join(self.out[v], today + ".jsonl")), key=lambda r: r.get("t", 0))
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
                       "it, trail $2 from break even; 1st bet trails $2 from +$5; no time limit (Friday 22:55 close); up to 2 trades open at once, each managed on its own, the 2nd only if the 1st has run 15+ min. Paper trades on Binance prices, "
                       "after ~$0.60/oz per bet (spread + ~$5 fee).",
              "B": {**self.block("B", today),
                    "rules": "Version B: same, but when the volume has calmed it only fades if the order book leans against "
                             "the burst (resting size within $1.5 on the far side >= the near side) and price has already "
                             "pulled back $0.50+ from the burst's extreme. Otherwise it skips the burst."},
              "C": {**self.block("C", today),
                    "rules": "Version C: B's entries (book + pullback check), but the 1st bet's trailing stop starts at "
                             "+$10 profit and stays $5 behind (A and B: from +$5, $2 behind). Everything else the same."}}
        write_json(os.path.join(OUT, "state.json"), st)
        write_json(os.path.join(OUT, "open.json"),       # bursts of trades still open, for a restart
                   {v: min(T["b"]["k"] for T in e.trades) for v, e in self.engines.items() if e.trades})

    def replay(self):
        NEWS.refresh()
        now = int(time.time() * 1000)
        start = now - 80 * 60000
        try:
            with open(os.path.join(OUT, "open.json")) as f:
                opened = {v: int(k) for v, k in json.load(f).items() if v in self.engines}
        except (OSError, ValueError, TypeError):
            opened = {}
        full = False
        t = tir(now / 1000)                               # a version with nothing logged today yet (just added):
        if t.weekday() < 5 and t.hour >= H0:              # replay today from 08:25 Tirana so it gets today's trades
            day = t.strftime("%Y-%m-%d")
            tag_path, tag = os.path.join(OUT, "replayed.txt"), f"{day} {RULES_TAG}"
            try:
                done = open(tag_path).read().strip() == tag
            except OSError:
                done = False
            if not done or any(not os.path.exists(os.path.join(d, day + ".jsonl")) for d in self.out.values()):
                start = min(start, int(t.replace(hour=H0 - 1, minute=25, second=0, microsecond=0).timestamp() * 1000))
                full = True
                with open(tag_path, "w") as f:               # rules changed today: replay the whole day once
                    f.write(tag)
        for v, k in opened.items():                       # replay that trade from its burst, no earlier entries
            if k * 1000 > now - 24 * 3600000:
                start = min(start, k * 1000 - 32 * 60000)
                if not full:                              # (a whole-day replay rebuilds the open trades itself)
                    self.engines[v].cool = k
        nb = self.book.load_recorded(start - 5 * 60000)
        trades = []
        for h in range((now - start) // 3600000 + 1, -1, -1):
            day, hh = F.day_hour(now - h * 3600000)
            trades += F.recorded_trades(day, hh)
        trades = sorted({t[0]: t for t in trades if t[1] >= start}.values(), key=lambda t: (t[1], t[0]))
        for e in self.engines.values():
            e.live = False
        for a, t, pc, q, sell in trades:
            for e in self.engines.values():
                e.add(t, pc / 100, q, sell)
            self.last_id = a
        for e in self.engines.values():
            e.tick(int(time.time() * 1000))
            e.live = True
        log(f"replay: {len(trades)} recorded trades, {nb} book snapshots since {datetime.fromtimestamp(start / 1000, TZ):%H:%M} (open before: {opened}); "
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
            NEWS.refresh()
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
