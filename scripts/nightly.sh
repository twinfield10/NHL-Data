#!/usr/bin/env bash
# Nightly (04:19 ET): ingest last night's games and rebuild everything downstream (events,
# xG, game state, a rating snapshot dated today), then refresh transactions and injuries so
# the morning pregame run starts from current rosters, then grade yesterday's bets. See docs/scheduler.md.
# shellcheck source=./_common.sh
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
# A failed update shouldn't block grading (games are final once the catalog step ran), but the
# wrapper still exits non-zero at the end so the failure is visible.
update_rc=0
run_with_timeout "${NIGHTLY_TIMEOUT:-5400}" nhl update || update_rc=$?
# Off-day news (IR moves, recalls) isn't polled during the day when no game is near.
run_with_timeout 600 nhl poll --what transactions,injuries || echo "post-update polls failed (non-fatal)"
# Site ratings boards from the new snapshot (pregame runs rebuild them too; this covers off days).
run_with_timeout 600 nhl site-tables || echo "site tables failed (non-fatal)"
# Grade finished bets (paper and real): CLV against our captured close, result, units.
run_with_timeout 600 nhl grade-bets || echo "grading failed (non-fatal)"
# Final scores and graded bets: rebuild yesterday's and today's prebuilt site views in full.
run_with_timeout 600 nhl site-views --force --date "$(TZ=America/New_York date -v-1d +%F)" || echo "site views (yesterday) failed (non-fatal)"
run_with_timeout 600 nhl site-views --force || echo "site views failed (non-fatal)"
exit "$update_rc"
