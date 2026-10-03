# Gold order-flow server

Records Binance XAUUSDT trades and the order book 24/7 on a DigitalOcean droplet (Frankfurt, Ubuntu 24.04).

- `recorder.py` – the recorder (same file also runs on a PC).
- `setup.sh` – installs everything; safe to re-run.
- `update.sh` – runs every 5 min: pulls this repo from GitHub, re-runs setup, restarts the recorder / API when their file changed.
- `api.py` – history API for the app (`/api/book?minutes=120`), behind Caddy at `/api/`.
- `history.py` – keeps 30 days of every trade (Binance daily files + gaps) as minute 'atoms' for the footprint.
- `fpcore.py` – shared footprint code; `api.py` also serves `/api/fp` (footprint candles).
- `status.html` – status page served at `https://<server-ip-with-dashes>.sslip.io/`.

## Create the server (DigitalOcean → Create Droplet)
Frankfurt · Ubuntu 24.04 LTS · Basic / Regular · $6/mo · Advanced options → "Add initialization scripts":

```bash
#!/bin/bash
command -v git >/dev/null || (apt-get update -q && apt-get install -y -q git)
git clone https://github.com/henrikuci123-ops/gold-orderflow /opt/gof/repo
bash /opt/gof/repo/server/setup.sh > /opt/gof/first-setup.log 2>&1
```

Changes: commit to this repo; the server applies them within 5 minutes (see `updates.log` on the status page).
