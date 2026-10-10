#!/usr/bin/env bash
# Poll third-party sources once:  scripts/poll.sh WHAT [--window MINUTES] [--reprice]
#
# WHAT is a comma list of odds,props,props_lowvig,novig,goalies,lines,injuries,transactions,officials (or "all").
# --reprice reruns `nhl pregame` when a lineup source changed. Schedule: docs/scheduler.md.
# A snapshot not taken is
# not recoverable later, and only transitions are stored, so an extra poll is close to
# free and a missed one is permanent. The lock is per WHAT, so the every-15-minutes job
# and the every-5-minutes pregame-window job for the same source never overlap.
# Refusals (401/403/429) are skips, not failures: `nhl poll` exits 0 on them.
# shellcheck source=./_common.sh
WHAT="${1:?usage: poll.sh WHAT [--window MINUTES]}"; shift
NHL_LOCKDIR="/tmp/nhl_data_poll_${WHAT//,/_}.lock.d"
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
run_with_timeout "${POLL_TIMEOUT:-600}" nhl poll --what "$WHAT" "$@"
