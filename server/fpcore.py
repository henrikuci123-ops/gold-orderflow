"""Shared footprint code for history.py and api.py (Henri, Oct 2026).

"Atoms" = one entry per minute with the minute's open/high/low/close and the traded ounces per $0.05 price row,
split into aggressive sells and buys:  [t_ms, o, h, l, c, first_row, sell, buy, sell, buy, ...]
(rows consecutive upward from first_row; row r = prices r*0.05 .. r*0.05+0.0499). Every footprint candle size and
row size the app offers is built from these by adding them up.
Stored per UTC hour:  <data>/XAUUSDT/atoms/YYYY-MM-DD/HH.json.gz   {"src": "zip" | "rec", "m": [atoms...]}
  zip = from Binance's official daily trade file (complete), rec = from our own recording (+ REST backfill).
"""
import csv, gzip, io, json, os, zipfile
from datetime import datetime, timezone

SYMBOL = os.environ.get("GOF_SYMBOL", "XAUUSDT")
BASE = os.environ.get("GOF_DATA") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "orderflow_data")
BASEC = 5                       # atom row size in cents ($0.05)


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def day_hour(ms):
    d = utc(ms)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


def rec_dir(day):
    return os.path.join(BASE, SYMBOL, day)


def atoms_path(day, hh):
    return os.path.join(BASE, SYMBOL, "atoms", day, f"{hh}.json.gz")


def zip_path(day):
    return os.path.join(BASE, SYMBOL, "zips", f"{SYMBOL}-aggTrades-{day}.zip")


def zip_marker(day):
    return os.path.join(BASE, SYMBOL, "atoms", day, ".from_zip")


def cents(p):
    return int(round(float(p) * 100))


def build_atoms(trades):
    """trades: iterable of (id, t_ms, price_cents, qty, sell). Returns the minute atoms, sorted."""
    mins = {}
    seen = set()
    for a, t, pc, q, s in sorted(trades):
        if a in seen:
            continue
        seen.add(a)
        m = t - t % 60000
        e = mins.get(m)
        if e is None:
            e = mins[m] = [pc, pc, pc, pc, {}]
        else:
            if pc > e[1]:
                e[1] = pc
            if pc < e[2]:
                e[2] = pc
            e[3] = pc
        r = pc // BASEC
        cell = e[4].get(r)
        if cell is None:
            cell = e[4][r] = [0.0, 0.0]
        cell[0 if s else 1] += q
    out = []
    for m in sorted(mins):
        o, h, l, c, rows = mins[m]
        r0, r1 = min(rows), max(rows)
        flat = [0] * (2 * (r1 - r0 + 1))
        for r, (sv, bv) in rows.items():
            flat[2 * (r - r0)] = round(sv, 3)
            flat[2 * (r - r0) + 1] = round(bv, 3)
        out.append([m, o / 100, h / 100, l / 100, c / 100, r0] + flat)
    return out


def write_atoms(day, hh, src, atoms):
    p = atoms_path(day, hh)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump({"src": src, "m": atoms}, f, separators=(",", ":"))
    os.replace(tmp, p)


def load_atoms(day, hh):
    p = atoms_path(day, hh)
    try:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            d = json.load(f)
        return d.get("src"), d.get("m", [])
    except (OSError, EOFError, ValueError):
        return None


def recorded_trade_files(day, hh):
    """Our own recording for that hour: trades_HH.csv(.gz) and the REST backfill trades_HH_bf.csv(.gz)."""
    d = rec_dir(day)
    if not os.path.isdir(d):
        return []
    return [os.path.join(d, fn) for fn in sorted(os.listdir(d))
            if fn.startswith(f"trades_{hh}") and fn.endswith((".csv", ".csv.gz"))]


def read_trade_file(path):
    out = []
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split(",")
                if len(parts) != 5 or not parts[0].isdigit():
                    continue
                try:
                    out.append((int(parts[0]), int(parts[1]), cents(parts[2]), float(parts[3]), parts[4] == "1"))
                except ValueError:
                    continue
    except (OSError, EOFError):
        pass                     # being gzipped right now / truncated: use what we got
    return out


def recorded_trades(day, hh):
    tr = []
    for p in recorded_trade_files(day, hh):
        tr += read_trade_file(p)
    return tr


def zip_hours(path):
    """Binance daily aggTrades zip -> yields (hour_start_ms, [trades]) one hour at a time (low memory).
    CSV: agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker (header optional)."""
    cur_h, cur = None, []
    with zipfile.ZipFile(path) as z:
        name = [n for n in z.namelist() if n.endswith(".csv")][0]
        with z.open(name) as raw:
            for row in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
                if len(row) < 7 or not row[0].isdigit():
                    continue
                t = int(row[5])
                if t > 10 ** 14:                   # microseconds in some newer files
                    t //= 1000
                h = t - t % 3600000
                if h != cur_h:
                    if cur:
                        yield cur_h, cur
                    cur_h, cur = h, []
                cur.append((int(row[0]), t, cents(row[1]), float(row[2]), row[6].strip().lower() == "true"))
    if cur:
        yield cur_h, cur
