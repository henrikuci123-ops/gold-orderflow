"""Trade history for the footprint (Henri, Oct 2026). Runs on the server as gof-history.service.

Every 5 minutes:
 1. Downloads Binance's official daily trade files (every XAUUSDT trade of the day) for the last DAYS days that we do
    not have yet (a day's file appears a few hours after midnight UTC) and turns them into minute atoms (fpcore.py).
 2. Fills the start of a day that our recorder missed (e.g. the day the server was created) from the Binance REST API,
    as trades_HH_bf.csv.gz next to the recorder's files - only while that day's official file is not out yet.
 3. Builds atoms for every finished hour of our own recording that has none yet.
Progress: <status>/history.json (shown in the app's Server tab).
"""
import json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone, timedelta

import fpcore as F

DAYS = int(os.environ.get("GOF_HISTORY_DAYS", "30"))
API = "https://fapi.binance.com"
ZIP_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades/{s}/{s}-aggTrades-{d}.zip"
STATUS_DIR = os.environ.get("GOF_STATUS") or F.BASE
LOOP = 300


def log(*a):
    line = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a)
    print(line, flush=True)
    try:
        with open(os.path.join(F.BASE, "history.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def get_json(path):
    with urllib.request.urlopen(API + path, timeout=30) as r:
        return json.loads(r.read().decode())


STATE = {"days_wanted": DAYS, "zip_days": [], "missing_zip_days": [], "rec_hours_built": 0, "backfill": "", "updated": ""}


def save_state():
    STATE["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        tmp = os.path.join(STATUS_DIR, "history.json.tmp")
        with open(tmp, "w") as f:
            json.dump(STATE, f, indent=1)
        os.replace(tmp, os.path.join(STATUS_DIR, "history.json"))
    except OSError:
        pass


# ---------------------------------------------------------------- 1. official daily files
def do_zip_day(day):
    if os.path.exists(F.zip_marker(day)):
        return True
    zp = F.zip_path(day)
    if not os.path.exists(zp):
        url = ZIP_URL.format(s=F.SYMBOL, d=day)
        try:
            os.makedirs(os.path.dirname(zp), exist_ok=True)
            with urllib.request.urlopen(url, timeout=120) as r, open(zp + ".part", "wb") as f:
                while True:
                    b = r.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
            os.replace(zp + ".part", zp)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                log(f"zip {day}: HTTP {e.code}")
            return False
        except Exception as e:
            log(f"zip {day}: {type(e).__name__}: {e}")
            return False
    t0, n = time.time(), 0
    try:
        for h, trades in F.zip_hours(zp):
            d, hh = F.day_hour(h)
            if d != day:
                continue
            F.write_atoms(d, hh, "zip", F.build_atoms(trades))
            n += len(trades)
    except Exception as e:
        log(f"zip {day}: could not read ({type(e).__name__}: {e}) - will download again")
        try:
            os.remove(zp)
        except OSError:
            pass
        return False
    os.makedirs(os.path.dirname(F.zip_marker(day)), exist_ok=True)
    open(F.zip_marker(day), "w").close()
    log(f"zip {day}: {n} trades -> atoms in {time.time() - t0:.0f}s")
    return True


# ---------------------------------------------------------------- 2. start-of-day gap from the REST API
def first_recorded(day):
    """(id, t) of the first trade our recorder saved on that day (backfill files excluded), or None."""
    d = F.rec_dir(day)
    if not os.path.isdir(d):
        return None
    for hh in sorted({fn[7:9] for fn in os.listdir(d) if fn.startswith("trades_") and "_bf" not in fn}):
        best = None
        for fn in (f"trades_{hh}.csv.gz", f"trades_{hh}.csv"):
            for a, t, _, _, _ in F.read_trade_file(os.path.join(d, fn)):
                if best is None or a < best[0]:
                    best = (a, t)
        if best:
            return best
    return None


def rest_backfill(day):
    marker = os.path.join(F.rec_dir(day), ".bf_done")
    if os.path.exists(marker) or os.path.exists(F.zip_marker(day)):
        return
    fr = first_recorded(day)
    if not fr:
        return
    day_start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    first_id, first_t = fr
    if first_t - day_start < 120000:
        open(marker, "w").close()
        return
    STATE["backfill"] = f"{day}: downloading trades 00:00 -> {F.utc(first_t).strftime('%H:%M')} UTC"
    save_state()
    log(STATE["backfill"])
    start_id, t = None, day_start
    while t < first_t and start_id is None:            # first trade of the day (quiet hours may be empty)
        arr = get_json(f"/fapi/v1/aggTrades?symbol={F.SYMBOL}&startTime={t}&endTime={min(t + 3600000, first_t) - 1}&limit=1")
        if arr:
            start_id = arr[0]["a"]
        t += 3600000
        time.sleep(0.3)
    if start_id is None:
        open(marker, "w").close()
        return
    files, frm, n = {}, start_id, 0
    try:
        while frm < first_id:
            arr = get_json(f"/fapi/v1/aggTrades?symbol={F.SYMBOL}&fromId={frm}&limit=1000")
            if not arr:
                break
            for d in arr:
                if d["a"] >= first_id:
                    break
                dd, hh = F.day_hour(d["T"])
                if dd != day:
                    continue
                f = files.get(hh)
                if f is None:
                    f = files[hh] = open(os.path.join(F.rec_dir(day), f"trades_{hh}_bf.csv.part"), "w")
                    f.write("id,time_ms,price,qty_oz,seller_aggressive\n")
                f.write(f'{d["a"]},{d["T"]},{d["p"]},{d["q"]},{1 if d["m"] else 0}\n')
                n += 1
            frm = arr[-1]["a"] + 1
            if len(arr) < 1000:
                break
            time.sleep(0.6)
    finally:
        for f in files.values():
            f.close()
    import gzip, shutil
    for hh in files:
        part = os.path.join(F.rec_dir(day), f"trades_{hh}_bf.csv.part")
        with open(part, "rb") as src, gzip.open(os.path.join(F.rec_dir(day), f"trades_{hh}_bf.csv.gz"), "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.remove(part)
        try:
            os.remove(F.atoms_path(day, hh))           # rebuild that hour with the new trades
        except OSError:
            pass
    open(marker, "w").close()
    STATE["backfill"] = f"{day}: added {n} trades before the recorder started"
    log(STATE["backfill"])


# ---------------------------------------------------------------- 3. atoms from our own recording
def rec_atoms(now_ms):
    built = 0
    cur = now_ms - now_ms % 3600000                    # start of the current hour
    for k in range(1, DAYS * 24 + 25):
        hs = cur - k * 3600000
        if hs + 3600000 > now_ms - 180000:             # hour (plus gzip time) not finished yet
            continue
        day, hh = F.day_hour(hs)
        if os.path.exists(F.atoms_path(day, hh)) or os.path.exists(F.zip_marker(day)):
            continue
        trades = F.recorded_trades(day, hh)
        if trades:
            F.write_atoms(day, hh, "rec", F.build_atoms(trades))
            built += 1
    return built


def main():
    log(f"history service: keeping {DAYS} days of footprint history in {os.path.abspath(F.BASE)}")
    while True:
        try:
            today = datetime.now(timezone.utc).date()
            for day in ((today - timedelta(days=1)).isoformat(), today.isoformat()):
                try:
                    rest_backfill(day)
                except Exception as e:
                    log(f"backfill {day}: {type(e).__name__}: {e}")
            STATE["rec_hours_built"] += rec_atoms(int(time.time() * 1000))
            save_state()
            have, missing = [], []
            for k in range(1, DAYS + 1):                   # newest day first
                day = (today - timedelta(days=k)).isoformat()
                (have if do_zip_day(day) else missing).append(day)
                STATE["zip_days"], STATE["missing_zip_days"] = sorted(have), sorted(missing)
                save_state()
            STATE["rec_hours_built"] += rec_atoms(int(time.time() * 1000))
            save_state()
        except Exception as e:
            log(f"error {type(e).__name__}: {e}")
        time.sleep(LOOP)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
