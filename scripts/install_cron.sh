#!/usr/bin/env bash
# Install (or update / remove) the NHL-Data block in the user crontab without touching any
# other job. Dry run by default: prints the diff. See docs/scheduler.md.
#
#   scripts/install_cron.sh            # show what would change
#   scripts/install_cron.sh --apply    # back up the current crontab, then install
#   scripts/install_cron.sh --remove   # back up, then remove the NHL-Data block only
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BLOCK="$REPO/scripts/crontab.nhl"
BACKUPS="$HOME/crontab_backups"
MODE="${1:-}"

current="$(mktemp)"; proposed="$(mktemp)"
trap 'rm -f "$current" "$proposed"' EXIT
crontab -l > "$current" 2>/dev/null || true

# Everything except an existing NHL-Data block (between the BEGIN/END markers).
awk '/^# BEGIN NHL-Data/{skip=1} !skip{print} /^# END NHL-Data/{skip=0}' "$current" > "$proposed"
if [ "$MODE" != "--remove" ]; then
  # Exactly one blank line before the block.
  sed -i '' -e :a -e '/^\n*$/{$d;N;ba' -e '}' "$proposed" 2>/dev/null || true
  printf '\n' >> "$proposed"
  cat "$BLOCK" >> "$proposed"
fi

if diff -u "$current" "$proposed" > /dev/null; then
  echo "crontab already up to date"
  exit 0
fi
diff -u "$current" "$proposed" || true

case "$MODE" in
  --apply|--remove)
    mkdir -p "$BACKUPS" "$REPO/logs"
    backup="$BACKUPS/crontab.$(date +%Y%m%dT%H%M%S).txt"
    cp "$current" "$backup"
    crontab "$proposed"
    echo "installed; previous crontab saved to $backup"
    ;;
  *)
    echo
    echo "dry run: nothing changed. Re-run with --apply to install (a backup is saved first)."
    ;;
esac
