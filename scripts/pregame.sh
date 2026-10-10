#!/usr/bin/env bash
# Price every date in the horizon from projected lineups and starters; write the pregame
# snapshots, then edges. No longer on cron (the nightly job prices the horizon since
# 2026-10-10); kept for manual runs. Repricing during the day is triggered by
# `poll.sh ... --reprice` and by odds polls that see a newly listed game. See docs/scheduler.md.
# shellcheck source=./_common.sh
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
run_with_timeout "${PREGAME_TIMEOUT:-900}" nhl pregame
# Edges against the latest odds; new flagged bets go to the paper ledger.
run_with_timeout 600 nhl edges || echo "edges failed (non-fatal)"
run_with_timeout 600 nhl props-edges || echo "prop edges failed (non-fatal)"
