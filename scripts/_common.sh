#!/usr/bin/env bash
# Shared preamble for every cron wrapper: PATH, .env, venv, a lock, and a timeout.
#
# cron runs with a minimal PATH and no shell profile, so anything found by an interactive
# shell has to be arranged for explicitly here.
#
# macOS has neither `flock` nor `timeout`, and pretending otherwise is how a scheduler
# fails silently: `flock` reports "command not found", and if that call sits inside an
# `if !` it escapes `set -e`, takes the "lock is held" branch, and the wrapper exits 0
# having done nothing at all. Every run then reports success while collecting nothing.
# The lock below is therefore a *directory* — `mkdir` is atomic on every platform — and
# the timeout falls back to a forked watchdog.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# One header per run, so a reader of an append-only log can find where the last run
# began instead of guessing with a line count. OpsView scans from the last `^=== ` line
# (its registry's `run_start_pattern`); on a 50-line guess, a clean Silver run of 26
# lines left the previous day's traceback in the window and the job read as failing.
echo "=== $(basename "$0" .sh) $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

# shellcheck disable=SC1091
[ -f "$REPO/.env" ] && set -a && . "$REPO/.env" && set +a

if [ -f "$REPO/.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . "$REPO/.venv/bin/activate"
fi

LOCKDIR="${NHL_LOCKDIR:-/tmp/nhl_data_$(basename "$0" .sh).lock.d}"

acquire_lock() {
  if mkdir "$LOCKDIR" 2>/dev/null; then
    echo $$ > "$LOCKDIR/pid"
    trap 'rm -rf "$LOCKDIR"' EXIT
    return 0
  fi
  local holder
  holder="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  # A lock whose holder is gone is stale: clear it and take it once.
  if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
    echo "clearing stale lock held by pid $holder"
    rm -rf "$LOCKDIR"
    if mkdir "$LOCKDIR" 2>/dev/null; then
      echo $$ > "$LOCKDIR/pid"
      trap 'rm -rf "$LOCKDIR"' EXIT
      return 0
    fi
  fi
  echo "another run is in progress (pid ${holder:-unknown}) — exiting"
  return 1
}

# run_with_timeout SECONDS COMMAND...
run_with_timeout() {
  local seconds="$1"; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout -k 30 "$seconds" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout -k 30 "$seconds" "$@"
  else
    "$@" &
    local pid=$!
    # The watchdog's own fds go to /dev/null. It must NOT inherit ours: killing the
    # subshell below orphans its `sleep` child rather than reaping it, and an orphan
    # holding the caller's stdout keeps a pipe open for the full timeout. `... | tail`
    # then blocks long after the real work finished. In cron stdout is a log file, so
    # nothing blocks and the leak is invisible — which is exactly why it survived.
    ( sleep "$seconds"; kill -TERM "$pid" 2>/dev/null || true ) >/dev/null 2>&1 &
    local watchdog=$!
    # Drop the watchdog from the job table so killing it below does not print a
    # "Terminated" line into the cron log every single run.
    disown "$watchdog" 2>/dev/null || true
    local status=0
    wait "$pid" || status=$?
    # Children first, then the subshell. The other order reparents the `sleep` to init,
    # where it survives to its full duration — once every 30 minutes for the Pinnacle
    # poll, which is frequent enough to keep a handful alive at all times.
    pkill -P "$watchdog" 2>/dev/null || true
    kill "$watchdog" 2>/dev/null || true
    # Normalise "killed by SIGTERM" to the conventional timeout exit code.
    [ "$status" -eq 143 ] && status=124
    return "$status"
  fi
}
