#!/usr/bin/env bash
# Nightly (04:19 ET): ingest last night's games and rebuild everything downstream (events,
# xG, game state, a rating snapshot dated today), then refresh transactions and injuries so
# the morning pregame run starts from current rosters, then grade yesterday's bets. See docs/scheduler.md.
# shellcheck source=./_common.sh
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
run_with_timeout "${NIGHTLY_TIMEOUT:-5400}" nhl update
# Off-day news (IR moves, recalls) isn't polled during the day when no game is near.
run_with_timeout 600 nhl poll --what transactions,injuries || echo "post-update polls failed (non-fatal)"
# Grade finished bets (paper and real): CLV against our captured close, result, units.
run_with_timeout 600 nhl grade-bets || echo "grading failed (non-fatal)"
