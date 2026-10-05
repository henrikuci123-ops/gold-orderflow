"""Absorption backtest (Henri, Oct 2026): runs the app's live absorption rule (absorb.py) over every recorded trade of
the last DAYS days (Binance daily files + our own recording), exactly second by second as the app would have, and
follows every signal for 30 minutes.  Started once by update.sh when this file or absorb.py changes
(gof-backtest.service, low priority).  Output:
  <status>/backtest/absorption.json     summary (win rates, average moves per setting / direction / session)
  <status>/backtest/absorption.jsonl    every signal
  <status>/backtest/progress.json       while it runs
"""
import glob, json, os, sys, time
from datetime import datetime, timezone, timedelta

import fpcore as F
from absorb import Engine, summarize

DAYS = int(os.environ.get("GOF_BACKTEST_DAYS", "90"))
OUT = os.path.join(os.environ.get("GOF_STATUS") or F.BASE, "backtest")


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


def day_trades(day):
    """all trades of a UTC day, hour by hour: the official Binance file if we have it, else our recording"""
    zp = F.zip_path(day)
    if os.path.exists(zp):
        try:
            for _, trades in F.zip_hours(zp):
                yield trades
            return
        except Exception as e:
            print(f"{day}: zip unreadable ({e}), using the recording", flush=True)
    for hh in range(24):
        tr = F.recorded_trades(day, f"{hh:02d}")
        if tr:
            yield sorted({t[0]: t for t in tr}.values(), key=lambda t: (t[1], t[0]))


def main():
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=k)).isoformat() for k in range(DAYS, -1, -1)]
    recs = []
    eng = Engine(recs.append)
    last_id, done_days = 0, []
    for i, day in enumerate(days):
        n0 = eng.n_trades
        for trades in day_trades(day):
            for a, t, pc, q, sell in trades:
                if a <= last_id:
                    continue
                last_id = a
                eng.add(t, pc, q, sell)
        if eng.n_trades > n0:
            done_days.append(day)
        write_json(os.path.join(OUT, "progress.json"), {"day": day, "done": i + 1, "of": len(days), "signals": len(recs),
                                                         "trades": eng.n_trades, "seconds": round(time.time() - t0)})
        print(f"{day}: {eng.n_trades - n0} trades, {len(recs)} signals so far ({time.time() - t0:.0f}s)", flush=True)
    eng.tick(eng.last_ms + 40 * 60000)       # finish signals still being followed
    with open(os.path.join(OUT, "absorption.jsonl.tmp"), "w") as f:
        for r in recs:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    os.replace(os.path.join(OUT, "absorption.jsonl.tmp"), os.path.join(OUT, "absorption.jsonl"))
    write_json(os.path.join(OUT, "absorption.json"), {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "runtime_s": round(time.time() - t0),
        "days": len(done_days), "from": done_days[0] if done_days else None, "until": done_days[-1] if done_days else None,
        "trades": eng.n_trades, "signals": len(recs), "summary": summarize(recs),
        "note": "win% = reached +$X before -$X (from the price when the signal fired, Binance prices, no spread); "
                "avg_mN = average move in the signal's direction after N minutes; more/normal/only_big = app setting"})
    print(f"done: {len(recs)} signals from {eng.n_trades} trades over {len(done_days)} days in {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
