#!/usr/bin/env bash
# Price today's games from projected lineups and starters; write the pregame snapshots.
# Repricing during the day is triggered by `poll.sh ... --reprice`; this is the morning run
# that always happens, even when no source changed overnight. See docs/scheduler.md.
# shellcheck source=./_common.sh
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
run_with_timeout "${PREGAME_TIMEOUT:-900}" nhl pregame
