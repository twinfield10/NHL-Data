#!/usr/bin/env bash
# Nightly: refresh the game catalog, ingest yesterday's finished games, rebuild the current
# season's events and shot features, and score them with the production xG model.
# shellcheck source=./_common.sh
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

acquire_lock || exit 0
run_with_timeout "${NIGHTLY_TIMEOUT:-5400}" nhl update
