#!/bin/bash
# Gold order-flow server setup (Ubuntu 24.04 on DigitalOcean). Run as root.
# Safe to run again: update.sh runs it after every change pushed to GitHub.
#   /opt/gof/repo        this GitHub repo (pulled every 5 min)
#   /opt/gof/www         public web folder:  https://<server-ip-with-dashes>.sslip.io/
#       status.json      recorder health (updated every 30 s)
#       data/XAUUSDT/... recorded trades + order book (gzipped per hour)
#       updates.log      every code update the server applied
main() {
  set -u
  REPO=/opt/gof/repo
  WWW=/opt/gof/www
  DATA=$WWW/data
  export DEBIAN_FRONTEND=noninteractive

  # 1. packages (installed only when missing)
  local need=""
  for p in git curl python3 python3-websockets caddy; do
    dpkg -s "$p" >/dev/null 2>&1 || need="$need $p"
  done
  if [ -n "$need" ]; then
    echo "installing:$need"
    apt-get update -q && apt-get install -y -q $need
  fi

  # 2. user and folders (the recorder runs as the unprivileged user 'gof')
  id gof >/dev/null 2>&1 || useradd --system --home-dir /opt/gof --shell /usr/sbin/nologin gof
  mkdir -p "$DATA"
  chmod 755 /opt/gof "$WWW" "$DATA"
  chown gof:gof "$WWW" "$DATA"
  git -C "$REPO" rev-parse --short HEAD > /opt/gof/version.txt 2>/dev/null
  install -m 644 "$REPO/server/status.html" "$WWW/index.html"

  # 3. services
  write_if_changed /etc/systemd/system/gof-recorder.service <<EOF
[Unit]
Description=Gold order-flow recorder (Binance XAUUSDT trades + order book)
After=network-online.target
Wants=network-online.target

[Service]
User=gof
Environment=GOF_DATA=$DATA GOF_STATUS=$WWW GOF_VERSION_FILE=/opt/gof/version.txt
ExecStart=/usr/bin/python3 -u $REPO/server/recorder.py
Restart=always
RestartSec=5
Nice=5

[Install]
WantedBy=multi-user.target
EOF

  write_if_changed /etc/systemd/system/gof-api.service <<EOF
[Unit]
Description=Gold order-flow history API for the app (/api/ behind Caddy)
After=network-online.target

[Service]
User=gof
Environment=GOF_DATA=$DATA
ExecStart=/usr/bin/python3 -u $REPO/server/api.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  write_if_changed /etc/systemd/system/gof-history.service <<EOF
[Unit]
Description=Gold order-flow trade history (Binance daily files + gaps) for the footprint
After=network-online.target

[Service]
User=gof
Environment=GOF_DATA=$DATA GOF_STATUS=$WWW
ExecStart=/usr/bin/python3 -u $REPO/server/history.py
Restart=always
RestartSec=30
Nice=10

[Install]
WantedBy=multi-user.target
EOF

  write_if_changed /etc/systemd/system/gof-update.service <<EOF
[Unit]
Description=Pull gold-orderflow from GitHub and apply changes
After=network-online.target

[Service]
Type=oneshot
ExecStart=/bin/bash $REPO/server/update.sh
EOF

  write_if_changed /etc/systemd/system/gof-update.timer <<EOF
[Unit]
Description=Check GitHub for gold-orderflow updates every 5 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
EOF

  # 4. web server with automatic HTTPS on <ip>.sslip.io (free name that points to the server's own IP)
  local ip host
  ip=$(curl -s -4 --max-time 5 http://169.254.169.254/metadata/v1/interfaces/public/0/ipv4/address)
  case "$ip" in *.*.*.*) ;; *) ip=$(curl -s -4 --max-time 10 https://api.ipify.org) ;; esac
  host="$(echo "$ip" | tr . -).sslip.io"
  echo "https://$host/" > "$WWW/address.txt"
  write_if_changed /etc/caddy/Caddyfile <<EOF
$host {
	encode gzip
	handle /api/* {
		reverse_proxy 127.0.0.1:8081
	}
	handle {
		root * $WWW
		header Access-Control-Allow-Origin *
		header Cache-Control no-cache
		file_server browse
	}
}
EOF

  # 5. small swap file so a memory spike can never kill the recorder (1 GB RAM server)
  if [ ! -f /swapfile ]; then
    fallocate -l 1G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile &&
      echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi

  systemctl daemon-reload
  systemctl enable --now gof-recorder.service gof-api.service gof-history.service gof-update.timer caddy >/dev/null 2>&1
  if [ "${CHANGED_CADDY:-0}" = 1 ]; then systemctl reload caddy || systemctl restart caddy; fi
  if [ "${CHANGED_SERVICE:-0}" = 1 ]; then systemctl restart gof-recorder.service; fi
  if [ "${CHANGED_API:-0}" = 1 ]; then systemctl restart gof-api.service; fi
  if [ "${CHANGED_HIST:-0}" = 1 ]; then systemctl restart gof-history.service; fi
  echo "setup done $(date -u '+%F %T') version $(cat /opt/gof/version.txt 2>/dev/null) address https://$host/"
}

# write stdin to file $1 only if the content differs; remember what changed
write_if_changed() {
  local tmp
  tmp=$(mktemp)
  cat > "$tmp"
  if ! cmp -s "$tmp" "$1"; then
    mkdir -p "$(dirname "$1")"
    cp "$tmp" "$1"
    case "$1" in
      */Caddyfile) CHANGED_CADDY=1 ;;
      */gof-recorder.service) CHANGED_SERVICE=1 ;;
      */gof-api.service) CHANGED_API=1 ;;
      */gof-history.service) CHANGED_HIST=1 ;;
    esac
  fi
  rm -f "$tmp"
}

main "$@"
exit $?
