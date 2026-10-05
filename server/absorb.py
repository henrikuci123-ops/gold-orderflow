"""Absorption detector shared by signals.py (live, 24/7) and backtest.py (history) - same rules as the app's live
absorption bubble (index.html, liveAbsCheck):

  every second, over the last 60 s: the lowest (highest) price of that minute; aggressive selling (buying) within $0.40
  of it >= k x a normal minute's total volume (average of the last 30 min), at least 1.8 x the other side there, the
  extreme first traded 8+ s ago and price still within $0.80 of it  ->  absorption signal (low = long idea, high = short).
  App states afterwards: held = price moved $1 away, broken = traded $0.40 through the extreme (5 min without = held).

Scorecard: every signal is followed for 30 minutes from the price at the moment it fired: best / worst move in the
signal's direction, which of +-$1 / $2 / $3 came first, and the move after 5, 15 and 30 minutes.
Prices are Binance cents internally; results are in dollars and MT5 lots (100 oz)."""
from collections import deque
from datetime import datetime, timezone

Z, NEAR, HOLD_MS, WIN_S, NORM_S, FOLLOW_MS = 40, 80, 8000, 60, 1800, 1800000
KS = (0.8, 1.2, 1.8)                          # app setting: More / Normal / Only big
STEPS = (100, 200, 300)                       # $1, $2, $3
SNAPS = (5, 15, 30)                           # minutes


def session(t_ms):
    h = datetime.fromtimestamp(t_ms / 1000, timezone.utc).hour
    return "london" if 7 <= h < 13 else "newyork" if 13 <= h < 21 else "asia"


class Engine:
    def __init__(self, on_done, ks=KS):
        self.on_done = on_done
        self.ks = ks
        self.secs = deque()                   # closed seconds in the 60 s window: [sec, lo, hi, last, {cents: [s, b]}, s, b]
        self.vol = deque()                    # (sec, volume) for the normal-minute average
        self.vsum = 0.0
        self.win_s = self.win_b = 0.0
        self.cur = None
        self.first_sec = None
        self.last_ms = 0
        self.px = None
        self.checked = None                   # last second that was checked
        self.events = {k: [] for k in ks}     # app-style events per sensitivity
        self.track = []                       # signals being followed for 30 min
        self.n_trades = 0

    # ------------------------------------------------------------------ input
    def add(self, t, pc, q, sell):
        """one trade: time ms, price in cents, quantity oz, seller was the aggressor"""
        s = t // 1000
        if self.cur is None or s != self.cur[0]:
            if self.cur is not None:
                self._close()
            self.tick(s * 1000)                 # every earlier second is over now
            self.cur = [s, pc, pc, pc, {}, 0.0, 0.0]
            if self.first_sec is None:
                self.first_sec = s
        c = self.cur
        if pc < c[1]:
            c[1] = pc
        if pc > c[2]:
            c[2] = pc
        c[3] = pc
        lv = c[4].get(pc)
        if lv is None:
            lv = c[4][pc] = [0.0, 0.0]
        if sell:
            lv[0] += q
            c[5] += q
        else:
            lv[1] += q
            c[6] += q
        self.last_ms, self.px = t, pc
        self.n_trades += 1
        if self.track:
            self._follow(t, pc)

    def tick(self, now_ms):
        """run the once-a-second checks up to now (live: call every second, also without trades)"""
        upto = now_ms // 1000 - 1               # only whole seconds that are over
        if self.checked is None:
            self.checked = upto
            return
        if upto - self.checked > 120:          # long gap (night / weekend / restart): skip the empty seconds
            self.checked = upto - 120
        while self.checked < upto:
            self.checked += 1
            self._check(self.checked)
        if self.track:
            self._follow(now_ms, None)

    # ------------------------------------------------------------------ internals
    def _close(self):
        c = self.cur
        if self.cur is None:
            return
        self.secs.append(c)
        self.win_s += c[5]
        self.win_b += c[6]
        v = c[5] + c[6]
        self.vol.append((c[0], v))
        self.vsum += v
        self.cur = None

    def _check(self, sec):
        now = (sec + 1) * 1000
        if self.cur is not None and self.cur[0] <= sec:
            self._close()
        while self.secs and self.secs[0][0] <= sec - WIN_S:
            x = self.secs.popleft()
            self.win_s -= x[5]
            self.win_b -= x[6]
        while self.vol and self.vol[0][0] <= sec - NORM_S:
            self.vsum -= self.vol.popleft()[1]
        if not self.secs or now - self.last_ms > 30000 or self.first_sec is None:
            return
        span = min(30.0, (now - self.first_sec * 1000) / 60000)
        if span < 10:                          # not enough history for a 'normal minute' yet
            return
        norm = self.vsum / max(1.0, span)
        if norm <= 0:
            return
        L = min(x[1] for x in self.secs)
        H = max(x[2] for x in self.secs)
        px = self.px
        for evs in self.events.values():       # what happened to the live ones
            for e in evs:
                if e["state"] != "live":
                    continue
                if (L < e["ext"] - Z) if e["side"] == "low" else (H > e["ext"] + Z):
                    e["state"], e["tEnd"] = "broken", now
                elif (px >= e["ext"] + 100) if e["side"] == "low" else (px <= e["ext"] - 100):
                    e["state"], e["tEnd"] = "held", now
                elif now - e["t"] > 300000:
                    e["state"], e["tEnd"] = "held", now
        kmin = min(self.ks)
        for side in ("low", "high"):
            if (self.win_s if side == "low" else self.win_b) < kmin * norm:
                continue                       # not even all of that side's volume would be enough
            ext = L if side == "low" else H
            agg = opp = 0.0
            tfirst = None
            for x in self.secs:
                if side == "low":
                    if x[1] > ext + Z:
                        continue
                    if tfirst is None and x[1] <= ext + 5:
                        tfirst = x[0] * 1000
                    for pc, v in x[4].items():
                        if pc <= ext + Z:
                            agg += v[0]
                            opp += v[1]
                else:
                    if x[2] < ext - Z:
                        continue
                    if tfirst is None and x[2] >= ext - 5:
                        tfirst = x[0] * 1000
                    for pc, v in x[4].items():
                        if pc >= ext - Z:
                            agg += v[1]
                            opp += v[0]
            if tfirst is None or now - tfirst < HOLD_MS or agg < 1.8 * opp:
                continue
            if (px > ext + NEAR) if side == "low" else (px < ext - NEAR):
                continue
            for k in self.ks:
                if agg >= k * norm:
                    self._signal(k, side, ext, agg, opp, norm, now, px)

    def _signal(self, k, side, ext, agg, opp, norm, now, px):
        evs = self.events[k]
        prev = None
        for e in evs:
            if e["side"] == side and abs(e["ext"] - ext) <= Z and now - (e.get("tEnd") or e["t"]) < 180000:
                prev = e
        if prev is not None:
            if prev["state"] == "live":
                prev["t"] = now
                prev["vol"] = max(prev["vol"], agg)
                prev["ext"] = min(prev["ext"], ext) if side == "low" else max(prev["ext"], ext)
            return
        e = {"side": side, "ext": ext, "t0": now, "t": now, "vol": agg, "state": "live"}
        evs.append(e)
        if len(evs) > 200:
            del evs[:100]
        self.track.append({"k": k, "side": side, "dir": 1 if side == "low" else -1, "t": now, "entry": px, "ext": ext,
                           "agg": agg, "opp": opp, "norm": norm, "ev": e, "mfe": 0, "mae": 0, "hit": {}, "snap": {}, "last": px})

    def _follow(self, t, pc):
        keep = []
        for r in self.track:
            if pc is not None and t >= r["t"]:
                fav = (pc - r["entry"]) * r["dir"]
                if fav > r["mfe"]:
                    r["mfe"] = fav
                if -fav > r["mae"]:
                    r["mae"] = -fav
                for st in STEPS:
                    if fav >= st and ("+%d" % st) not in r["hit"]:
                        r["hit"]["+%d" % st] = t - r["t"]
                    if -fav >= st and ("-%d" % st) not in r["hit"]:
                        r["hit"]["-%d" % st] = t - r["t"]
                for m in SNAPS:
                    if m not in r["snap"] and t >= r["t"] + m * 60000:
                        r["snap"][m] = (r["last"] - r["entry"]) * r["dir"]     # price just before that moment
                r["last"] = pc
            elif pc is None:
                for m in SNAPS:
                    if m not in r["snap"] and t >= r["t"] + m * 60000:
                        r["snap"][m] = (r["last"] - r["entry"]) * r["dir"]
            if t >= r["t"] + FOLLOW_MS:
                self.on_done(self._result(r))
            else:
                keep.append(r)
        self.track = keep

    @staticmethod
    def _result(r):
        def first(st):
            a, b = r["hit"].get("+%d" % st), r["hit"].get("-%d" % st)
            if a is None and b is None:
                return None
            if b is None or (a is not None and a <= b):
                return "win"
            return "loss"
        e = r["ev"]
        out = {"k": r["k"], "side": r["side"], "dir": "long" if r["dir"] > 0 else "short", "t": r["t"],
               "time": datetime.fromtimestamp(r["t"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
               "session": session(r["t"]), "entry": r["entry"] / 100, "ext": r["ext"] / 100,
               "agg_lots": round(r["agg"] / 100, 2), "opp_lots": round(r["opp"] / 100, 2),
               "norm_lots": round(r["norm"] / 100, 2), "ratio": round(r["agg"] / r["norm"], 2),
               "state": e["state"], "state_s": round(((e.get("tEnd") or r["t"]) - r["t"]) / 1000),
               "mfe": round(r["mfe"] / 100, 2), "mae": round(r["mae"] / 100, 2),
               "hit_s": {h: round(v / 1000) for h, v in r["hit"].items()}}
        for st in STEPS:
            out["first%d" % (st // 100)] = first(st)
        for m in SNAPS:
            v = r["snap"].get(m, (r["last"] - r["entry"]) * r["dir"])
            out["m%d" % m] = round(v / 100, 2)
        return out


def summarize(recs):
    """win rates and average moves per sensitivity, overall and per session"""
    def block(rs):
        n = len(rs)
        if not n:
            return {"n": 0}
        out = {"n": n}
        for st in (1, 2, 3):
            dec = [r for r in rs if r.get("first%d" % st)]
            w = sum(1 for r in dec if r["first%d" % st] == "win")
            out["win%d" % st] = round(100 * w / len(dec), 1) if dec else None
            out["decided%d" % st] = len(dec)
        for m in SNAPS:
            out["avg_m%d" % m] = round(sum(r["m%d" % m] for r in rs) / n, 2)
        srt = sorted(r["mfe"] for r in rs)
        out["median_mfe"] = srt[n // 2]
        srt = sorted(r["mae"] for r in rs)
        out["median_mae"] = srt[n // 2]
        out["held_pct"] = round(100 * sum(1 for r in rs if r["state"] == "held") / n, 1)
        return out
    res = {}
    for k in KS:
        rs = [r for r in recs if r["k"] == k]
        name = {0.8: "more", 1.2: "normal", 1.8: "only_big"}[k]
        res[name] = {"all": block(rs), "long": block([r for r in rs if r["dir"] == "long"]),
                     "short": block([r for r in rs if r["dir"] == "short"])}
        for s in ("asia", "london", "newyork"):
            res[name][s] = block([r for r in rs if r["session"] == s])
    return res
