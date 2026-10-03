#!/bin/bash
# Runs every 5 minutes (gof-update.timer). If GitHub has new commits: take them, re-run setup.sh, and restart the
# recorder when anything in server/ changed. App-only changes (index.html etc.) do not touch the recorder.
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
  if [ -n "$(git diff --name-only "$old" "$new" -- server/)" ]; then
    systemctl restart gof-recorder.service
    echo "$(date -u '+%F %T') recorder restarted" >> "$LOG"
  fi
  return 0
}
main "$@"
exit $?
