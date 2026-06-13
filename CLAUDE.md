# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Read-only Polymarket smart-wallet analysis and **paper**-follow tool for narrow
esports and sports winner markets. There is **no** live CLOB execution, private
keys, balances, approvals, or real orders — every "trade" is a simulated paper
leg recorded for research. The project intentionally uses **only the Python
standard library** (no third-party runtime deps); the dashboardV2 UI ships
vendored JS under `poly_fight/dashboardV2/vendor/`.

`AGENTS.md` is the authoritative operational policy doc (market scope, scoring
rules, collection limits, follow eligibility, dashboard mutation allowlist, VPS
ops). Read it before changing scope, scoring, or follow behavior — the rules
encode hard-won decisions and are easy to regress. This file is the short map.

## Commands

```bash
# Tests (stdlib unittest; no pytest config required)
python3 -m unittest discover -s tests -v
python3 -m unittest tests.test_core -v                 # single module
python3 -m unittest tests.test_core.TestName.test_x    # single test
node --test tests/dashboardv2_strategy_mapping.test.js  # JS strategy-mapping test

# Collect wallets + build leaderboard (writes to data/ — mutates real data)
python3 collect.py                                      # convenience wrapper
python3 -m poly_fight.cli collect
python3 -m poly_fight.cli collect --category sports
python3 -m poly_fight.cli build-leaderboard

# Analyze a current event
python3 -m poly_fight.cli analyze-event
python3 -m poly_fight.cli analyze-event --event-slug <slug>
python3 -m poly_fight.cli analyze-event --condition-id 0x...

# Paper follow
python3 -m poly_fight.cli follow --stake-usdc 1 --stake-ratio-percent 10   # one tick
python3 -m poly_fight.cli run --stake-usdc 1 --stake-ratio-percent 10      # loop (preferred)

# Dashboard (read-only API + dashboardV2 UI)
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

Global options (`--data-dir`, rate limits) come **before** the subcommand:
`python3 -m poly_fight.cli --data-dir data_smoke build-leaderboard`.

`run` is the preferred loop entrypoint; it refreshes the leaderboard first
unless `--skip-initial-build` is passed. `follow` is a single tick.

### Verifying dashboardV2 without live data

`serve` defaults its static dir to `poly_fight/dashboardV2/`. Append `?mock=1`
to the URL to render the UI from `dashboardV2/mock.js` fixtures (no `follow.db`
needed). Use `--static-dir` to point at an alternate build.

## Architecture (big picture)

The pipeline has two stages plus a dashboard, all driven through one CLI:

```
Stage 1 — discovery/scoring (collect / build-leaderboard):
  closed in-scope markets -> discovery slate -> trades -> candidate wallets
  -> scoped wallet history -> leaderboard.db (per category)

Stage 2 — paper follow (follow / run):
  leaderboard wallets -> upcoming watched markets -> poll trades?user=
  -> paper legs -> CLV / settlement / performance, persisted in follow.db

serve: read-only dashboard reading ONLY from SQLite (follow.db + per-category
       leaderboard.db; no raw-JSON parsing), with a small mutation allowlist
       for refresh/runner/balance controls.
```

Module map (all under `poly_fight/`):

| File | Role |
|---|---|
| `cli.py` | Argument parsing + every subcommand orchestration (large; the spine) |
| `core.py` | Pure logic: classification, wallet scoring/profiling, `to_float`/`to_int` helpers |
| `api.py` | Read-only rate-limited HTTP client for Gamma + Data API |
| `follow.py` | Paper-follow tick logic: cursors, legs, quarantine, CLV, settlement |
| `follow_strategy.py` | Configurable follow strategy schema (stake sizing, prefilters); `default_follow_strategy()`, `ACTIVE_FOLLOW_STRATEGY_ID` |
| `storage.py` | `FollowStore` — SQLite source of truth (`data/follow/follow.db`) + dashboard query persistence |
| `dashboard.py` | Read-only HTTP dashboard/API server; mutation allowlist; SSE stream |
| `control.py` | Runner/refresh/pause control-file helpers |
| `dashboardV2/` | Current UI: `app.jsx` (vendored React via Babel), `adapt.js`, `api.js`, `mock.js`, `ds/` design system |

### Data and state

- `data/follow/follow.db` — **source of truth**: wallet cursors, open/closed
  paper signals, legs, behavior events, results, quarantine, CLV/contested
  fields, performance, manual balance cap, follow strategy, active/closed market
  caches, and run ticks (the old `follow_run_log.jsonl`).
- `data/{esports,sports}/leaderboard.db` — dashboard source for the leaderboard
  and collection-run summaries (the old `smart_wallet_leaderboard.json` /
  `build_summary.json` are no longer written).
- `data/follow/follow_state.json` — thin metadata-only compatibility file.
- `data/{esports,sports}/*.json` — **collector intermediates only** (not read by
  the dashboard): `discovery_slate`, `candidate_wallets`, `wallet_profiles`, raw
  trade caches. `data/`, `logs/`, `review/`, `sample/`, `secret/` are git-ignored.
- Dashboard reads **only** from SQLite — no JSON fallbacks on any read path.
  SQLite access is **read-only** (`mode=ro`, `PRAGMA query_only=1`). Never call
  `FollowStore.init_db()` or write methods from a request path.

## Conventions that bite if missed

- Normalize wallet addresses to **lowercase** everywhere.
- Filter closed positions by scoped `conditionId`s, **never** by title keywords;
  classify markets with semantic `category`/`league`/`market_type`, not raw
  title substring blacklists.
- Use the real match start time for timing
  (`eventStartTime -> startTime -> gameStartTime -> end_date fallback`); never
  trust Polymarket page display times (stale / timezone-ambiguous).
- `realizedPnl == 0` positions are neutral — excluded from win rate, ROI, and
  sample count.
- New-signal creation is intentionally **esports-only** right now; sports
  scoring/leaderboard/display may exist but sports wallets must not open new
  paper signals unless that policy is explicitly changed.

## Git / workflow preferences

When asked to submit or push, commit locally and push directly to the configured
GitHub branch — do **not** open a PR unless explicitly asked. Do not run
`collect` / `build-leaderboard` (they overwrite real `data/`) without
confirming. Keep VPS IPs, hostnames, credentials, and secret paths out of the
repo (`secret/` is local-only).
