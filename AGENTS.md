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
python3 -m poly_fight.cli collect --discovery-lookback-days 15 --market-batch-size 50 --market-batch-index 0
python3 -m poly_fight.cli collect --discovery-lookback-days 15 --market-batch-size 50 --market-batch-index 1
```

## Current Workflow

```text
collect / build-leaderboard:
  closed in-scope markets -> discovery slate -> trades -> candidate wallets
  -> scoped wallet history -> smart_wallet_leaderboard

follow / run:
  leaderboard wallets -> upcoming watched markets -> wallet trade polling
  -> paper legs -> CLV / settlement / performance

serve:
  read-only dashboard + wallet refresh / runner controls
```

## Market Scope

Keep `classification_set` and `discovery_slate` separate. Classification is
historical category membership; discovery is the recent high-quality subset used
to find active candidates.

Allowed esports:

```text
LOL / CS2 / Dota2 only
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

Rating signals:

```text
realized PnL
win/loss count excluding pnl=0 neutral markets
Wilson lower bound at 80% confidence (z=1.28)
edge against breakeven entry price
entry price
capital size
positive market rate / flat win rate
sample count
recency
bot-like / two-sided / tail-entry behavior
```

`realizedPnl == 0` positions are neutral and excluded from win rate, ROI, and
sample count; track as `neutral_market_count`.

Win rate alone is weak. ROI can be distorted by a few large wins. For sports,
prefer genuinely high flat win-rate wallets over “small losses, big wins”
gamblers because follow sizing is average/proportional, not max-bet-copy.

Exported `smart_wallet_leaderboard.json` is stricter than raw scoring:

```text
grade == A
recent category activity
meaningful discovery participation / cash
same-condition two-sided behavior excluded
max exported wallets = 30 by default
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
classification_lookback_days = 14 by default
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
read category leaderboards
filter by follow recency and wallet_quarantine
build watched markets from start_time within observe window
poll trades?user= for follow wallets and wallets with open signals
cold-start wallets set cursor only
optional bootstrap current positions once
new BUY trades create paper legs
SELL mirrors exit wallet-market-outcome legs
material SELL or opposite-side BUY writes wallet_quarantine
post-start snapshot records CLV once
same conditionId with both outcomes open marks contested
settled markets move open signals to results
```

Do not rely on `/trades` time-range params. Use local cursor `{timestamp, id}`
and recent pages (`--user-trades-limit` default 100,
`--user-trades-max-pages` default 3). First sight of a wallet is baseline only.

Stake sizing:

```text
wallet_trade_cash = wallet BUY size * wallet BUY price
stake = max(--stake-usdc, wallet_trade_cash * --stake-ratio-percent / 100)
```

`--stake-usdc` is the minimum paper stake. Dashboard default is 1.  
`--stake-ratio-percent` is the target-wallet replication ratio. Dashboard
default is 10. Each BUY leg, including later adds, is sized independently.

If `--bankroll-usdc` cannot cover desired proportional stake but can cover the
minimum, cap to available balance and mark the leg capped. If it cannot cover
the minimum, skip.

Follow eligibility:

```text
esports wallets: eligible market_type set only
sports wallets: category=sports + market_type=main_match + own league
```

NBA wallets follow NBA only. UFC wallets follow UFC only. Wallets no longer
eligible can only affect already-open signal markets.

`--max-slippage-over-entry` sets `would_follow`; paper signals are still
recorded for learning. Contested signals are recorded but not live-followable.

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
data/follow/follow.db              source of truth
data/follow/follow_state.json      thin metadata compatibility
data/follow/active_market_cache.json
data/follow/follow_control.json    runner/refresh/pause control
logs/follow/*.jsonl and dashboard-runner-*.out
```

## Dashboard

Run:

```bash
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

For VPS + TLS use `--host 0.0.0.0 --cookie-secure`; local HTTP should leave
`--cookie-secure` off. Do not change dashboard password unless explicitly asked.

Dashboard SQLite access must be read-only. Do not call `FollowStore.init_db()`
or write-capable load methods from dashboard request paths. Use read-only
SQLite (`mode=ro`, `PRAGMA query_only=1`). Missing `follow.db` should return
empty/waiting data.

Allowed dashboard mutations only:

```text
POST /api/wallet-refresh
POST /api/runner/start
POST /api/runner/stop
```

These write process/control state only, not follow signal state. The only live
external Data API request from dashboard is `/api/wallets/{addr}/trades`;
validate wallet addresses first.

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
esports_classification_set.json
discovery_slate.json
candidate_wallets.json
wallet_profiles.json
smart_wallet_leaderboard.json
leaderboard_wallet_overlap.json
build_summary.json
raw_market_trades/
raw_user_trades/
```

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

- `data/follow/follow.db` is the long-running paper follow source of truth.
- `logs/follow/follow_run_log.jsonl` is diagnostic. Keep logs out of git.
- Raw market/user-trade caches are the largest data files; add cleanup if VPS
  disk pressure becomes a problem.
- Do not edit live code directly on the VPS unless explicitly asked. Prefer
  local commit/push, then pull/deploy on the VPS.

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
