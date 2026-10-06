#!/bin/bash
# Runs every 5 minutes (gof-update.timer). If GitHub has new commits: take them, re-run setup.sh, and restart the
# recorder / API only when their own file changed. App-only changes (index.html etc.) touch neither.
# Everything is inside main() so bash has read the whole script before git replaces this file.
main() {
  local REPO=/opt/gof/repo LOG=/opt/gof/www/updates.log old new
  cd "$REPO" || return 1
  git fetch -q origin 2>>"$LOG" || return 0
  new=$(git rev-parse '@{u}') || return 0
  old=$(git rev-parse HEAD)
  [ "$new" = "$old" ] && return 0
  git reset -q --hard "$new" || return 1
  echo "$(date -u '+%F %T') update ${old:0:7} -> ${new:0:7}: $(git log -1 --format=%s "$new")" >> "$LOG"
  bash "$REPO/server/setup.sh" >> /opt/gof/www/setup.log 2>&1
  local changed
  changed=$(git diff --name-only "$old" "$new" -- server/)
  if echo "$changed" | grep -q '^server/recorder.py$'; then
    systemctl restart gof-recorder.service
    echo "$(date -u '+%F %T') recorder restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/(api|fpcore).py$'; then
    systemctl restart gof-api.service
    echo "$(date -u '+%F %T') api restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/(history|fpcore).py$'; then
    systemctl restart gof-history.service
    echo "$(date -u '+%F %T') history restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/(signals|absorb|fpcore).py$'; then
    systemctl restart gof-signals.service
    echo "$(date -u '+%F %T') signals restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/multi.py$'; then
    systemctl restart gof-multi.service
    echo "$(date -u '+%F %T') multi restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/(context|fpcore).py$'; then
    systemctl restart gof-context.service
    echo "$(date -u '+%F %T') context restarted" >> "$LOG"
  fi
  if echo "$changed" | grep -Eq '^server/(backtest|absorb).py$'; then
    systemctl start --no-block gof-backtest.service
    echo "$(date -u '+%F %T') backtest started" >> "$LOG"
  fi
  return 0
}
main "$@"
exit $?
