"""Live absorption scorecard (Henri, Oct 2026). Runs on the server as gof-signals.service, 24/7.

Listens to every Binance XAUUSDT trade, runs the same absorption rule as the app's pulsing bubble (absorb.py) for all
three app settings (More / Normal / Only big), follows each signal for 30 minutes and writes the result:
  <status>/signals/YYYY-MM-DD.jsonl   one line per finished signal (UTC day it fired)
  <status>/signals/summary.json       win rates / average moves: today, last 7 days, everything
  <status>/signals/open.json          signals still being followed (and what the app would show right now)
On start it reads the last 40 minutes from the recorder's files so the 'normal minute' is known immediately.
"""
import asyncio, glob, json, os, sys, time
from datetime import datetime, timezone, timedelta

import fpcore as F
from absorb import Engine, summarize

WS = "wss://fstream.binance.com/market/stream?streams={s}@aggTrade"
OUT = os.path.join(os.environ.get("GOF_STATUS") or F.BASE, "signals")


def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a), flush=True)


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


class Scorecard:
    def __init__(self):
        os.makedirs(OUT, exist_ok=True)
        self.engine = Engine(self.done)
        self.last_id = 0
        self.started = time.time()
        self.connected = False
        self.n_done = 0

    def done(self, rec):
        day = rec["time"][:10]
        with open(os.path.join(OUT, f"{day}.jsonl"), "a") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        self.n_done += 1
        log(f"signal finished: {rec['time']} {rec['dir']} k={rec['k']} entry {rec['entry']} -> 30 min {rec['m30']:+.2f}, first $2: {rec['first2']}")
        self.write_summary()

    def load_recs(self, days):
        recs = []
        for p in sorted(glob.glob(os.path.join(OUT, "*.jsonl")))[-days:]:
            with open(p) as f:
                recs += [json.loads(line) for line in f if line.strip()]
        return recs

    def write_summary(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        allr = self.load_recs(10000)
        write_json(os.path.join(OUT, "summary.json"), {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "since": allr[0]["time"] if allr else None,
            "today": summarize([r for r in allr if r["time"][:10] == today]),
            "last7d": summarize([r for r in allr if r["time"][:10] >= (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")]),
            "all": summarize(allr),
            "note": "win% = reached +$X before -$X (from the price when the signal fired, Binance prices, no spread); "
                    "avg_mN = average move in the signal's direction after N minutes; more/normal/only_big = app setting"})

    def write_open(self):
        e = self.engine
        live = [{"k": k, "side": ev["side"], "ext": ev["ext"] / 100, "state": ev["state"],
                 "since": datetime.fromtimestamp(ev["t0"] / 1000, timezone.utc).strftime("%H:%M:%S")}
                for k, evs in e.events.items() for ev in evs if time.time() * 1000 - ev["t"] < 600000]
        write_json(os.path.join(OUT, "open.json"), {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "connected": self.connected,
            "trades_seen": e.n_trades, "following": len(e.track), "finished_since_start": self.n_done, "recent": live})

    def warm_up(self):
        now = int(time.time() * 1000)
        trades = []
        for h in (now - now % 3600000 - 3600000, now - now % 3600000):
            day, hh = F.day_hour(h)
            trades += F.recorded_trades(day, hh)
        trades = sorted({t[0]: t for t in trades if t[1] >= now - 40 * 60000}.values(), key=lambda t: (t[1], t[0]))
        for a, t, pc, q, sell in trades:
            self.engine.add(t, pc, q, sell)
            self.last_id = a
        log(f"warm-up: {len(trades)} recorded trades from the last 40 min")

    def on_trade(self, d):
        a = d["a"]
        if a <= self.last_id:
            return
        self.last_id = a
        self.engine.add(d["T"], F.cents(d["p"]), float(d["q"]), bool(d["m"]))

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
            if n % 10 == 0:
                self.write_open()

    async def run(self):
        self.warm_up()
        self.write_summary()
        await asyncio.gather(self.stream(), self.clock())


if __name__ == "__main__":
    log(f"absorption scorecard: writing to {os.path.abspath(OUT)}")
    try:
        asyncio.run(Scorecard().run())
    except KeyboardInterrupt:
        sys.exit(0)
