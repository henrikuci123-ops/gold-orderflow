"""Market context for the app (Henri, Oct 2026). Runs on the server as gof-context.service and writes small files to
<www>/context/ that the app reads:
  macro.json     US 10-year Treasury yield + dollar index (DXY), real time from CNBC's quote feed, every 30 s, plus the
                 last 36 hours minute by minute (for the little chart)
  calendar.json  this week's and next week's US economic events with medium or high impact (ForexFactory's feed),
                 refreshed every hour
  gex.json       gold options walls from GLD's option chain (Cboe, 15-min delayed): call wall, put wall, gamma flip and
                 the biggest gamma strikes, converted to Binance XAUUSDT prices (= the app's price scale). Every 15 min
                 while US stocks trade, otherwise every 2 hours.
Only the Python standard library.
"""
import gzip, json, math, os, re, time, urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import fpcore as F

WWW = os.environ.get("GOF_STATUS") or F.BASE
OUT = os.path.join(WWW, "context")
NY = ZoneInfo("America/New_York")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
      "Accept": "application/json,text/plain,*/*"}
CNBC = ("https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=US10Y%7C.DXY"
        "&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json")
FF = ["https://nfs.faireconomy.media/ff_calendar_thisweek.json", "https://nfs.faireconomy.media/ff_calendar_nextweek.json"]
CBOE = "https://cdn.cboe.com/api/global/delayed_quotes/options/GLD.json"


def log(*a):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC ") + " ".join(str(x) for x in a), flush=True)


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))


def save(name, obj):
    os.makedirs(OUT, exist_ok=True)
    tmp = os.path.join(OUT, name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, os.path.join(OUT, name))


def load(name):
    try:
        with open(os.path.join(OUT, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def now_ms():
    return int(time.time() * 1000)


def num(x):
    try:
        return float(str(x).replace("%", "").replace(",", "").strip())
    except ValueError:
        return None


# ---------------------------------------------------------------- yields + dollar
class Macro:
    def __init__(self):
        old = load("macro.json") or {}
        self.hist = [h for h in old.get("hist", []) if h[0] > now_ms() - 36 * 3600000]
        self.last = old

    def run_once(self):
        d = fetch(CNBC, 20)
        q = {x.get("symbol"): x for x in d["FormattedQuoteResult"]["FormattedQuote"]}
        y, x = q.get("US10Y", {}), q.get(".DXY", {})
        ty = datetime.fromisoformat(y["last_time"]).timestamp() * 1000 if y.get("last_time") else None
        tx = datetime.fromisoformat(x["last_time"]).timestamp() * 1000 if x.get("last_time") else None
        out = {"updated": now_ms(),
               "us10y": {"last": num(y.get("last")), "chg_bp": round(num(y.get("change")) * 100, 1) if num(y.get("change")) is not None else None,
                         "t": int(ty) if ty else None},
               "dxy": {"last": num(x.get("last")), "chg": num(x.get("change")), "pct": num(x.get("change_pct")),
                       "t": int(tx) if tx else None}}
        m = now_ms() // 60000 * 60000
        if out["us10y"]["last"] is not None and out["dxy"]["last"] is not None:
            if self.hist and self.hist[-1][0] == m:
                self.hist[-1] = [m, out["us10y"]["last"], out["dxy"]["last"]]
            else:
                self.hist.append([m, out["us10y"]["last"], out["dxy"]["last"]])
        self.hist = [h for h in self.hist if h[0] > now_ms() - 36 * 3600000]
        out["hist"] = self.hist
        save("macro.json", out)
        self.last = out


# ---------------------------------------------------------------- economic calendar
def calendar_once():
    events, ok = [], 0
    for url in FF:
        try:
            arr = fetch(url, 30)
            ok += 1
        except Exception as e:
            log("calendar", url.rsplit("/", 1)[-1], type(e).__name__, e)
            continue
        for e in arr:
            if e.get("country") != "USD" or e.get("impact") not in ("High", "Medium", "Holiday"):
                continue
            try:
                t = int(datetime.fromisoformat(e["date"]).timestamp() * 1000)
            except (KeyError, ValueError):
                continue
            events.append({"t": t, "title": e.get("title", ""), "impact": e.get("impact"),
                           "forecast": e.get("forecast") or "", "previous": e.get("previous") or ""})
    if not ok:
        return False
    seen, uniq = set(), []
    for e in sorted(events, key=lambda e: e["t"]):
        k = (e["t"], e["title"])
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    save("calendar.json", {"updated": now_ms(), "source": "ForexFactory", "events": uniq})
    return True


# ---------------------------------------------------------------- gold options walls (GEX)
OPT = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def bs_gamma(s, k, t, iv, r=0.04):
    if s <= 0 or k <= 0 or t <= 0 or iv <= 0:
        return 0.0
    v = iv * math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * iv * iv) * t) / v
    return math.exp(-0.5 * d1 * d1) / (2.5066282746310002 * s * v)


def binance_price_at(t_ms):
    """Binance XAUUSDT price at t (our recording): last trade at or before t, within 3 minutes."""
    best = None
    for h in (t_ms - t_ms % 3600000, t_ms - t_ms % 3600000 - 3600000):
        day, hh = F.day_hour(h)
        for a, t, pc, q, s in F.recorded_trades(day, hh):
            if t <= t_ms and (best is None or t > best[0]):
                best = (t, pc / 100)
        if best:
            break
    if best and t_ms - best[0] < 180000:
        return best[1]
    return None


def gex_once():
    d = fetch(CBOE, 60)["data"]
    S = float(d["current_price"])
    ltt = d.get("last_trade_time")
    t_ref = int(datetime.fromisoformat(ltt).replace(tzinfo=NY).timestamp() * 1000) if ltt else now_ms()
    gold = binance_price_at(t_ref)
    if gold is None:
        try:
            gold = float(json.load(open(os.path.join(WWW, "status.json")))["last_price"])
        except (OSError, ValueError, KeyError, TypeError):
            gold = None
    if not gold or S <= 0:
        raise RuntimeError("no reference price")
    ratio = gold / S
    nowt = time.time()
    opts = []
    for o in d["options"]:
        oi = o.get("open_interest") or 0
        m = OPT.match(o.get("option", ""))
        if not m or oi <= 0:
            continue
        exp = datetime.strptime(m.group(2), "%y%m%d").replace(hour=16, tzinfo=NY).timestamp()
        T = (exp - nowt) / (365 * 86400)
        if T <= 0 or T > 0.25:
            continue                         # expired / more than ~3 months out (little gamma)
        K = int(m.group(4)) / 1000
        if not (0.8 * S <= K <= 1.2 * S):
            continue
        sign = 1 if m.group(3) == "C" else -1
        opts.append((K, max(T, 1 / (365 * 24)), float(o.get("iv") or 0), oi, sign, float(o.get("gamma") or 0)))
    if not opts:
        raise RuntimeError("no options with open interest")
    call, put, net = {}, {}, {}
    for K, T, iv, oi, sign, g in opts:
        gex = g * oi * 100 * S * S * 0.01
        if sign > 0:
            call[K] = call.get(K, 0) + gex
        else:
            put[K] = put.get(K, 0) + gex
        net[K] = net.get(K, 0) + sign * gex
    # walls: biggest call gamma strike ABOVE the price (resistance) and biggest put gamma strike BELOW it (support).
    # (Without the split both often land on the same at-the-money strike, which says nothing.)
    ca = {k: v for k, v in call.items() if k > S} or call
    pb = {k: v for k, v in put.items() if k < S} or put
    cw = max(ca, key=ca.get)
    pw = max(pb, key=pb.get)
    total = lambda x: sum(sign * bs_gamma(x, K, T, iv) * oi * 100 * x * x * 0.01 for K, T, iv, oi, sign, g in opts if iv > 0)
    grid = [S * (0.9 + 0.0025 * i) for i in range(81)]
    vals = [total(x) for x in grid]
    flip = None
    for i in range(1, len(grid)):
        if (vals[i - 1] < 0) != (vals[i] < 0):
            x = grid[i - 1] + (grid[i] - grid[i - 1]) * (-vals[i - 1]) / (vals[i] - vals[i - 1])
            if flip is None or abs(x - S) < abs(flip - S):
                flip = x
    at_s = total(S)
    top = sorted(net, key=lambda k: -abs(net[k]))[:6]
    g2 = lambda k: round(k * ratio, 1)
    out = {"updated": now_ms(), "source": "GLD options (Cboe, 15-min delayed)", "gld": S, "gld_time": t_ref,
           "gold_ref": gold, "ratio": round(ratio, 5),
           "call_wall": {"strike": cw, "gold": g2(cw), "gex_musd": round(call[cw] / 1e6, 2)},
           "put_wall": {"strike": pw, "gold": g2(pw), "gex_musd": round(put[pw] / 1e6, 2)},
           "flip": {"strike": round(flip, 2), "gold": g2(flip)} if flip else None,
           "net_gex_musd": round(at_s / 1e6, 2), "regime": "positive" if at_s > 0 else "negative",
           "levels": [{"strike": k, "gold": g2(k), "net_musd": round(net[k] / 1e6, 2)} for k in sorted(top)],
           "options_used": len(opts)}
    save("gex.json", out)
    log(f"gex: GLD {S} ratio {ratio:.4f} call wall {cw} ({g2(cw)}) put wall {pw} ({g2(pw)}) flip {out['flip']} regime {out['regime']}")


def us_session(ts):
    t = datetime.fromtimestamp(ts, NY)
    return t.weekday() < 5 and (9, 30) <= (t.hour, t.minute) < (16, 15)


STATUS = {"started": now_ms(), "ok": {}, "errors": {}}


def note(name, err=None):
    if err is None:
        STATUS["ok"][name] = now_ms()
        STATUS["errors"].pop(name, None)
    else:
        STATUS["errors"][name] = {"t": now_ms(), "error": f"{type(err).__name__}: {err}"[:300]}
        log(name, "failed:", type(err).__name__, err)
    try:
        save("status.json", STATUS)
    except OSError:
        pass


def main():
    log(f"context service: writing to {os.path.abspath(OUT)}")
    macro = Macro()
    next_macro = next_cal = next_gex = 0.0
    while True:
        now = time.time()
        if now >= next_macro:
            try:
                macro.run_once()
                note("macro")
            except Exception as e:
                note("macro", e)
            next_macro = now + 30
        if now >= next_cal:
            ok = False
            try:
                ok = calendar_once()
                note("calendar") if ok else note("calendar", RuntimeError("both feeds failed"))
            except Exception as e:
                note("calendar", e)
            next_cal = now + (3600 if ok else 600)
        if now >= next_gex:
            try:
                gex_once()
                note("gex")
                next_gex = now + (900 if us_session(now) else 7200)
            except Exception as e:
                note("gex", e)
                next_gex = now + 600
        time.sleep(2)


if __name__ == "__main__":
    main()
