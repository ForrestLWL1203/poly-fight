# AGENTS.md

Read-only Polymarket smart-wallet analysis and paper-follow tool for narrow
esports and sports winner markets. No live CLOB execution, private keys,
balances, approvals, or real orders.

## Commands

```bash
python3 -m unittest discover -s tests -v
python3 collect.py

python3 -m poly_fight.cli collect
python3 -m poly_fight.cli collect --category sports
python3 -m poly_fight.cli build-leaderboard
python3 -m poly_fight.cli build-leaderboard --category sports

python3 -m poly_fight.cli analyze-event
python3 -m poly_fight.cli analyze-event --event-slug <slug>
python3 -m poly_fight.cli analyze-event --condition-id <condition_id>

python3 -m poly_fight.cli follow --stake-usdc 1 --stake-ratio-percent 10
python3 -m poly_fight.cli run --stake-usdc 1 --stake-ratio-percent 10
```

`follow` runs one paper tick. `run` is the preferred loop entrypoint; it starts
with a leaderboard refresh unless `--skip-initial-build` is passed.

Global CLI options come before the subcommand:

```bash
python3 -m poly_fight.cli --data-dir data_smoke build-leaderboard
```

Batched discovery shares cache/profile state across batches:

```bash
python3 -m poly_fight.cli collect --classification-lookback-days 15 --market-batch-size 50 --market-batch-index 0
python3 -m poly_fight.cli collect --classification-lookback-days 15 --market-batch-size 50 --market-batch-index 1
```

## Current Workflow

```text
collect-v2 / build-leaderboard (periodic full rebuild):
  closed in-scope markets -> dual-side discovery -> trades -> candidate wallets
  -> scoped wallet history -> score -> A-only leaderboard.db (per category)

observe-v2 (sidecar, ~2h): newly-SETTLED markets -> top-PnL dual-side holders
  -> score on history -> merge into leaderboard; also M5 quarantine recovery +
  resolution/score freshness refresh. Covers matches not seen live.

observe-live (sidecar, ~10min): ACTIVE (unsettled) watched markets over a volume
  gate -> dual-side current holders -> score on history -> grade-A promoted into
  leaderboard EARLY (seed_source=observe_live). Lets follow act before settlement.
  (observe-v2 + observe-live share leaderboard.db; their publish critical sections
  serialize via acquire_build_lock.)

follow / run:
  leaderboard wallets -> upcoming/in-progress watched markets -> on-chain WS fill
  detection (data-api fallback) -> paper legs -> CLV / settlement / performance.
  Incremental backfill synthesizes each newly-eligible wallet's pre-existing
  positions into legs (startup + mid-run live-seed promotions).

serve:
  read-only dashboard (all data from SQLite) + wallet refresh / runner controls
```

## Market Scope

Keep `classification_set` and `discovery_slate` separate. Classification is
historical category membership; discovery is the recent high-quality subset used
to find active candidates.

Allowed esports:

```text
LOL / CS2 / Dota2
main_match: full match winner
game_winner: Dota2/LOL Game N Winner
map_winner: CS2 Map N Winner
```

Allowed sports:

```text
category = sports
league = nba or ufc
market_type = main_match / moneyline winner only
```

Sports and esports are top-level categories. NBA and UFC are leagues inside
`sports`, like LOL/Dota2/CS2 inside esports.

Exclude Valorant, MLB, NFL, props, spreads, totals, handicaps, futures, correct
scores, kills, first blood, towers, Roshan, barracks, and similar derived plays.
Use semantic `category`, `league`, and `market_type`; do not rely on raw title
substring blacklists.

Use true match start time for timing:

```text
market.eventStartTime -> event.startTime -> market.gameStartTime -> end_date fallback only
```

Polymarket sports listing pages can show stale games or UTC-looking display
times. Watched-market logic must compare parsed timestamps to current time, not
trust page UI.

## Wallet Scoring

Discovery recall:

```text
high_participation OR large_size
```

Frequency is a cheap recall filter, not a wallet quality signal.

Default discovery source is `trades?market=<conditionId>&takerOnly=false`.
Closed-market holders are experimental only; after resolution holder balances can
be biased.

Rating signals (copy-edge axis; Wilson lower-bound and dollar gates removed —
entry/leaderboard axes are unified on θ̂):

```text
θ̂  = recency-weighted point win-rate (half-life ~21d) on followable (<=0.85) subset
n_eff floor (>= ~10, per-game scope-calibrated)
copy-edge (θ̂ vs entry price; only +EV-to-copy wallets qualify)
edge-type: directional vs technical (hold-to-settle pnl vs swing) — see classify_edge_type
win/loss count excluding pnl=0 neutral markets; recency; capital size
bot-like / two-sided / tail-entry behavior (hard exclusions)
```

`realizedPnl == 0` positions are neutral and excluded from win rate, ROI, and
sample count; track as `neutral_market_count`.

Win rate alone is weak. ROI can be distorted by a few large wins. For sports,
prefer genuinely high flat win-rate wallets over “small losses, big wins”
gamblers because follow sizing is average/proportional, not max-bet-copy.

The exported leaderboard (persisted to `leaderboard.db`) is stricter than raw
scoring:

```text
grade == A (B stays in pool, not on the board)
recent category activity (idle hard cut, default 72h)
meaningful discovery participation / cash
same-condition two-sided behavior excluded
max exported wallets = 200 safety cap (quality gate is the real limiter)
```

Do not re-apply old raw win-rate, median-entry, or zero-tolerance late-entry
filters at leaderboard export time. Put scoring rules in scoring/profiling.

## Collection Rules

Keep collection fast and scoped:

```text
classification cache ttl = 24h unless --refresh-classification
raw_market_trades cache ttl = 7d unless --refresh-market-trades
max_workers = 8
max_requests_per_second = 10
request_burst = 5
classification_lookback_days = 60 by default
esports discovery buckets:
  main_match = LOL 100 / CS2 100 / Dota2 100
  game_winner = LOL 50 / Dota2 50
  map_winner = CS2 50
market_batch_size = 50
market_batch_count = 2
max_pages_per_market = 3
closed_position_market_chunk_size = 50
check_current_positions = false by default
```

Sports defaults:

```text
sports-nba-target-markets = 80
sports-ufc-target-markets = 80
sports-nba-min-market-volume = 250000
sports-nba-fallback-min-market-volume = 100000
sports-ufc-min-market-volume = 25000
sports-ufc-fallback-min-market-volume = 10000
```

Wallet history must be scoped to target conditionIds. Do not deep scan unrelated
categories by default.

Before deep profiling, use cheap discovery filters:

```text
participated_market_count >= threshold
avg_market_cash >= threshold
two_sided_market_count == 0
tail_entry_market_count == 0
```

Two-sided and tail-entry wallets should not consume profiling budget by default.
Churn by trade count is only an observation; do not exclude wallets merely for
splitting one position into many buys.

If Polymarket returns 429/503, lower `--max-requests-per-second` first.
`--max-requests-per-second 0` is debugging only.

## Paper Follow

Per tick:

```text
read category leaderboards (reloaded every tick; live-seed promotions picked up next tick)
filter by follow recency and wallet_quarantine
build watched markets: upcoming within observe window OR started-but-unresolved
detect fills via on-chain WS (drain ~5s when healthy); data-api polling fallback
incremental backfill: each NEWLY-eligible wallet's pre-existing positions -> legs once
new BUY trades create paper legs (open or add)
sub-min BUY fills accumulate per (wallet,cond,outcome) until >= min order, then follow (small-buy accumulator)
SELL mirrors exit proportionally (cumulative wallet_sold_frac; >= $1 min, hold/accumulate else, dust full-clear)
material SELL or opposite-side BUY writes wallet_quarantine
post-start snapshot records CLV once
same conditionId with both outcomes open marks contested
settled markets move open signals to results; M5 demotion re-scores followed wallets every 10 settle events
```

Do not rely on `/trades` time-range params. Use local cursor `{timestamp, id}`
and recent pages (`--user-trades-limit` default 100,
`--user-trades-max-pages` default 3). First sight of a wallet is baseline only.

Stake sizing:

The active runner sizes by a configurable follow strategy persisted in
`follow.db` (Kelly-on-edge): `edge = θ̂×0.95 − price`; stake ≈ ¼-Kelly ×
edge/(1−price) × bankroll, bounded by per-signal cap (5%), per-match cap (10%),
and a `min_stake` floor. `θ̂` is the followed bucket's recency-weighted point
win-rate. Each BUY leg (incl. adds) is sized independently.

```text
# legacy / no-strategy fallback only:
wallet_trade_cash = wallet BUY size * wallet BUY price
stake = max(--stake-usdc, wallet_trade_cash * --stake-ratio-percent / 100)
```

`--stake-usdc` (dashboard default 1) is the minimum paper stake;
`--stake-ratio-percent` (default 10) the replication ratio. If `--bankroll-usdc`
cannot cover the desired stake but can cover the minimum, cap to available
balance and mark the leg capped; if it cannot cover the minimum, skip.

Follow eligibility:

```text
esports wallets: eligible market_type set only
sports wallets: category=sports + market_type=main_match + own league
```

Current paper-follow new-signal creation is intentionally esports-only. Sports
collection, scoring, leaderboard, and dashboard display may exist, but sports
wallets should not open new paper follow signals unless this policy is
explicitly changed. Keep the sports eligibility rule above as the intended scope
if sports follow is later enabled.

NBA wallets follow NBA only. UFC wallets follow UFC only. Wallets no longer
eligible can only affect already-open signal markets.

Live price gate: the **sole** funded-follow price gate is the edge gate —
current price must be `< θ̂×0.95` (`THETA_FOLLOW_DISCOUNT`), else blocked
`no_live_edge`. `--max-entry-price` (default 0.85, or strategy
`max_follow_entry_price`) is a hard ceiling on our observed buy price;
`--min-wallet-entry-price` floors the target's fill price. The old
`slippage_over_entry` and `cost_ratio_cap` (cost×1.15) gates were removed so the
entry axis matches the leaderboard axis (θ̂); `--max-slippage-over-entry` remains
as an accepted but non-blocking flag. Contested signals are recorded but not
live-followable.

Failure policy:

```text
per-wallet failure -> empty for that wallet, tick continues
run error -> log run_iteration_error and sleep
--error-retry-seconds default 180
--max-consecutive-error-seconds default 600
```

Resolution lookup uses separate short cache:
`--resolution-cache-ttl-seconds` default 60 and `--resolution-gamma-pages`
default 2.

Follow files:

```text
data/follow/follow.db              source of truth (active/closed market cache,
                                   run ticks, signals, legs, results, strategy)
data/follow/follow_state.json      thin metadata compatibility
data/follow/follow_control.json    runner/refresh/pause control
logs/follow/*.out and dashboard-runner-*.out
```

The active/closed market caches and run ticks (formerly
`active_market_cache.json` / `follow_run_log.jsonl`) now live inside `follow.db`.
Any remaining JSON of those names is a legacy import source only.

## Dashboard

Run:

```bash
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

For VPS + TLS use `--host 0.0.0.0 --cookie-secure`; local HTTP should leave
`--cookie-secure` off. Do not change dashboard password unless explicitly asked.

The dashboard serves every response from SQLite (`follow.db` plus per-category
`leaderboard.db`) and must never parse raw JSON outputs — there are no JSON
fallbacks on read paths. SQLite access must be read-only: do not call
`FollowStore.init_db()` or write-capable load methods from dashboard request
paths; use read-only SQLite (`mode=ro`, `PRAGMA query_only=1`). Missing
`follow.db` should return empty/waiting data.

Mirror this on the writer side: the collector persists the leaderboard and
collection summary to `leaderboard.db` (no `smart_wallet_leaderboard.json` /
`build_summary.json`), and the follow loop persists run ticks to `follow.db` (no
`follow_run_log.jsonl`). Collector intermediates (`candidate_wallets.json`,
`wallet_profiles.json`, `discovery_slate.json`, raw trade caches) stay as JSON.

Allowed dashboard mutations only:

```text
POST /api/wallet-refresh
POST /api/wallet-favorites
POST /api/account-balance
POST /api/runner/start
POST /api/runner/stop
POST /api/reset-data
```

`wallet-refresh` and runner controls write process/control state only, not
follow signal state. `account-balance` writes the manual paper usable-funds cap
in `follow.db`; it must be locked while the runner is running. `reset-data` is an
explicit personal-use destructive operation for clearing generated
category/follow/log state; it must stay behind authenticated dashboard access
and must not be called implicitly. The only live external Data API request from
dashboard is `/api/wallets/{addr}/trades`; validate wallet addresses first.

`GET /api/stream` is same-origin, cookie-authenticated SSE. It sends an
immediate frame, heartbeats, caps clients, and releases the count in `finally`.
Do not switch to WebSocket unless the zero-dependency design changes.

Dashboard runner input is a percentage (`--stake-ratio-percent`), not dollars.
Minimum stake remains server config `--runner-stake-usdc` (default 1).

## UI Notes

This is a personal dashboard, not a commercial product. Prefer existing icons.
Acceptable sources: `https://www.flaticon.com/`, then
`https://www.iconfont.cn/`; otherwise use existing assets or simple text badges.

## Follow-Signal Principle

Single-wallet signals are weak. Strong future live candidates should require:

```text
>= 2 strict wallets
same conditionId
same outcome side
early / not tail-water
no two-sided contamination
```

Do not implement heavy-stake live execution in the collector.

## Important Pitfalls

- Normalize wallet keys to lowercase.
- Filter closed positions by scoped conditionIds, never title keywords.
- Compare holder conviction in USD:

```text
holder_usd_value = holder_amount * outcome_current_price
```

- Same-condition two-sided holdings are suspicious.
- `holders` is useful for active snapshots, not default offline discovery.
- Offline discovery uses `trades?market=<conditionId>&takerOnly=false`.
- Sports follow must be league-scoped.
- Unknown holders should not block `analyze-event`.
- Negative/unqualified profiles need TTL-based negative caching.

## Current Implementation

```text
poly_fight/core.py       classification, scoring, pure logic
poly_fight/api.py        read-only HTTP client
poly_fight/cli.py        collect/follow/run/serve
poly_fight/follow.py     paper follow logic
poly_fight/dashboard.py  read-only dashboard/API
poly_fight/storage.py    SQLite follow-state persistence
tests/test_core.py       unittest coverage
```

The project intentionally uses only the Python standard library.

## Generated Data

Category collection output lives under `data/esports/` and `data/sports/`;
older root-level files may exist for compatibility.

Typical generated files:

```text
leaderboard.db                  dashboard source: leaderboard + collection runs
esports_classification_set.json
discovery_slate.json            collector intermediates (kept as JSON, not read
candidate_wallets.json            by the dashboard)
wallet_profiles.json
leaderboard_wallet_overlap.json
raw_market_trades/
raw_user_trades/
```

The leaderboard and collection summary are persisted to `leaderboard.db`; the
collector no longer writes `smart_wallet_leaderboard.json` / `build_summary.json`.
Follow business data lives in `data/follow/follow.db`. Avoid committing large
live data dumps unless explicitly asked.

## VPS Operations Notes

Do not write VPS IPs, hostnames, usernames, passwords, secret-file paths, or
SSH commands into this repository. Keep private deployment details in local
private notes outside git.

Known paper runner shape:

```bash
python3 -u -m poly_fight.cli --data-dir <data-dir> run \
  --stake-usdc 1 --stake-ratio-percent 10 \
  --max-profiles-per-run 1000 --skip-initial-build
```

Operational notes:

- `data/follow/follow.db` is the long-running paper follow source of truth,
  including run ticks (query with the `run_ticks` table, not a JSONL file).
- `logs/follow/*.out` runner stdout is diagnostic. Keep logs out of git.
- Raw market/user-trade caches are the largest data files; add cleanup if VPS
  disk pressure becomes a problem.
- Do not edit live code directly on the VPS unless explicitly asked. Prefer
  local commit/push, then pull/deploy on the VPS.

VPS deploy/restart (launcher-driven):

Deployment is driven by the local launcher (`launcher/launcher.py`), not by
hand-editing live processes. The dashboard runs as a `poly-fight-dashboard`
systemd unit bound to `127.0.0.1`, fronted by Caddy for HTTPS; the **paper runner
and observe processes are spawned by the dashboard panel**, not as a separate
systemd unit or manual argv.

```text
1. Read local private ops notes outside git for the host + login method.
2. Commit and push local changes to GitHub first.
3. Launcher → 远程 VPS → 环境准备: pairs SSH key (first run), installs
   git/python3/caddy, opens ufw 80/443, git-resets the live repo to origin/main
   (aborts if the worktree is dirty), writes secrets, installs/enables the
   poly-fight-dashboard systemd unit + Caddy block.
4. Launcher → 启动: systemctl restart poly-fight-dashboard.
5. Start the runner / realtime refresh from the dashboard panel, not by argv.
6. Verify: VPS repo HEAD matches local, `systemctl is-active
   poly-fight-dashboard caddy`, the HTTPS domain returns 200, and the latest
   `run_ticks` row in `follow.db` looks current.
```

When debugging follower latency on the VPS, check the latest run log for
`tick_runtime_seconds`, `stage_seconds`, `wallet_trade_fetch_seconds`,
`observed_trade_delay_seconds`, and `index_lag_lower_bound_seconds`. These
separate local runner time from Polymarket data/index lag.

## Git Workflow Preference

When the user asks to submit or push code, commit locally and push directly to
the configured GitHub branch. Do not create a GitHub PR unless explicitly asked.

## Known Limitations

- Realized PnL has survivorship / unrealized-position bias.
- ROI is not time-normalized.
- `holders?limit=10` can miss medium-sized wallets ranked 11-30.
- `trades?market&takerOnly=false` may have maker/taker coverage bias.
- Sports pages/API can contain stale events or timezone display inconsistencies.
- Signals are observational paper research, not auto-trade recommendations.
