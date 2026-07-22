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
python3 -m poly_fight.cli ai-backtest --limit 50

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

collect-v2 --loop-hours N / runner pool refresh: periodic full rebuild from
  newly available settled history, including score freshness and re-discovery.

observe-live (sidecar, ~10min): ACTIVE (unsettled) watched markets over a volume
  gate -> dual-side current holders -> score on history -> grade-A promoted into
  leaderboard EARLY (seed_source=observe_live). Lets follow act before settlement.
  (collect-v2 + observe-live share leaderboard.db; their publish critical sections
  serialize via acquire_build_lock.)

follow / run:
  leaderboard wallets -> upcoming/in-progress watched markets -> on-chain
  eth_getLogs cursor-polling fill detection (data-api fallback) -> paper legs
  -> CLV / settlement / performance.
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
recent category activity (idle hard cut, default 5d / 120h — see V2_MAX_LEADERBOARD_IDLE_HOURS)
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
max_profile_wallets = 3000 (manual and 12h runner refresh share this budget)
max exported leaderboard wallets = 200 (presentation safety cap only)
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
detect fills via on-chain eth_getLogs cursor polling (~15-30s/round, drain when healthy); data-api polling fallback
incremental backfill: each NEWLY-eligible wallet's pre-existing positions -> legs once
new BUY trades create paper legs (open or add)
sub-min BUY fills accumulate per (wallet,cond,outcome) until >= min order, then follow (small-buy accumulator)
SELL mirrors exit proportionally (cumulative wallet_sold_frac; >= $1 min, hold/accumulate else, dust full-clear)
post-start snapshot records CLV once
same conditionId with both outcomes open marks contested
  settled markets move open signals to results; a fast observed-performance breaker deletes non-favorite wallets after >=2 actual followed results when win-rate <=50% and stake-weighted realized ROI <=-25%; independently, M5 re-scores followed wallets every N settle events (--rescore-settled-threshold, default 15) and DELETES any that fail uncapped grade-A quality/activity/hard gates: leaderboard row + scoring profile + raw trade cache dropped (no quarantine middle state). Ranking quotas and the 200-wallet display safety cap never count as demotion. Favorites are spared; follow.db research records are kept and open positions settle out. Re-discovery by the observer is the only way back onto the board, and prior results before the last demotion do not retrigger the breaker.
```

Do not rely on `/trades` time-range params. Use local cursor `{timestamp, id}`
and recent pages (`--user-trades-limit` default 100,
`--user-trades-max-pages` default 3). First sight of a wallet is baseline only.

Stake sizing:

The active runner sizes by a configurable follow strategy persisted in
`follow.db`: **stake = floor(current available balance × per-signal percent)**,
clamped to `[min_stake floor, per-match remaining]`.
- Target-wallet order cash is only a minimum-order filter. It does not scale the
  stake and is not treated as an absolute conviction score.
- The per-match percentage is the cumulative ceiling for one conditionId across
  all followed wallets and outcomes. Main and submarket caps are configurable
  independently.
- The per-match order-count limit is also cumulative per conditionId across all
  followed wallets and outcomes; it is not reset for each wallet.
- Example: with $5,000 available and a 2% per-signal setting, the next leg starts
  at $100, unless the remaining match budget or cash balance is lower.
- The live edge check (`θ̂×0.95 > price`) remains an entry gate, but it does not
  scale the stake. Wallet quality and copy edge are also enforced when building
  the grade-A leaderboard.

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

Live price gate: current price must be `< θ̂×0.95`, otherwise the candidate is
blocked as `no_live_edge`. `--max-entry-price` (default 0.85, or strategy
`max_follow_entry_price`) is an additional hard ceiling on our observed buy price;
`--min-wallet-entry-price` floors the target's fill price. The old
`slippage_over_entry` and `cost_ratio_cap` (cost×1.15) gates were removed so the
entry axis matches the leaderboard axis; `--max-slippage-over-entry` remains as
an accepted but non-blocking flag. Contested signals are recorded but not live-followable.

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
POST /api/wallet-quarantine
POST /api/account-balance
POST /api/follow-strategy
POST /api/follow-strategies      (+ /api/follow-strategies/<id>/{activate,delete})
POST /api/runner/start
POST /api/runner/stop
POST /api/reset-data
POST /api/ai-risk/credential
POST /api/ai-risk/credential/test
POST /api/ai-risk/credential/delete
POST /api/ai-risk/data-credential
POST /api/ai-risk/data-credential/test
POST /api/ai-risk/data-credential/delete
POST /api/ai-risk/settings
```

`wallet-refresh` and runner controls write process/control state only, not
follow signal state. `wallet-favorites` / `wallet-quarantine` are the **manual**
pin / quarantine buttons (manual quarantine is the only quarantine entry point —
there is no automatic quarantine). `follow-strategy` saves the active follow
strategy and `follow-strategies` manages the saved-strategy library (runner start
requires a saved strategy); both mutate strategy config in `follow.db`, never
follow signal/leg state. `account-balance` writes the manual paper usable-funds cap
in `follow.db`; it must be locked while the runner is running. `reset-data` is an
explicit personal-use destructive operation for clearing generated
category/follow/log state; it must stay behind authenticated dashboard access
and must not be called implicitly. The only live external Data API request from
dashboard is `/api/wallets/{addr}/trades`; validate wallet addresses first.
AI credential mutations store only the browser-created AES-GCM/RSA-OAEP
envelope under `data/.secrets/`; the plaintext key must never enter SQLite,
logs, SSE, follow state, or control files. `reset-data` deliberately preserves
this separate credential store; deleting it requires the explicit credential
delete endpoint.

AI risk provider rules:

```text
data/model = parallel multi-source EvidenceRouter -> Gemini / gemini-3.6-flash
  Dota2 = PandaScore + OpenDota
  LoL = PandaScore + Leaguepedia Cargo
  CS2 = PandaScore + Liquipedia MediaWiki action API
trigger = an otherwise eligible LOL/CS2/Dota2 main_match BUY intent only
history = 30/60/120d summaries; extend to 180d when sparse; at most 30
          full-match series/team with 12 detailed series in the prompt
prompt compaction = <=~12K tokens; evidence IDs, coverage gaps and conflicts
                    included; raw provider JSON, URLs and streams excluded
cache = provider + game + team normalized evidence with 6h freshness;
        one immutable evidence snapshot/assessment per condition
provider input excludes wallet, intended side, price, stake and condition_id
prompt = full-match winner only, 50:50 baseline, historical strength + form +
         opponent quality + H2H + roster/system + event tier + BO format
evidence score = local 0-100 score; model cannot change it
local block rule = evidence >=70, opponent >=65%, confidence >=75%
provider/schema/timeout/quota failure = wallet fail open + audited unavailable
settlement = retain immutable evidence snapshot; prune unused team cache by TTL
blocked audit = original-wallet shadow + same-stake AI-side hold-to-settlement shadow
self-run shadow = independent 5000 USDC ledger; evidence >=80, winner >=65%,
                  confidence >=75%, positive EV and read-only CLOB depth gates;
                  unformed books probe only at T-3h/T-2h/T-1h, then cold-skip;
                  one main-match position per condition, hold to settlement
```

Gemini receives no search/tools and returns strict JSON. Local code validates
scores, confidence, evidence references and winner/score consistency; only local
code decides whether to block a wallet signal or enter the self-run shadow.

`GET /api/stream` is same-origin, cookie-authenticated SSE. It sends an
immediate frame, heartbeats, caps clients, and releases the count in `finally`.
Do not switch to WebSocket without a concrete need and corresponding tests.

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
poly_fight/ai_risk.py    Gemini assessment, encrypted BYOK config, local gates
poly_fight/evidence.py   parallel evidence router, merge, scoring and snapshots
poly_fight/opendota.py   cutoff-safe Dota2 history and limited match metrics
poly_fight/leaguepedia.py Cargo whole-series LoL evidence with circuit breaker
poly_fight/liquipedia.py compliant MediaWiki API CS2 evidence with rate limits
poly_fight/pandascore.py bounded multi-game team-history cache adapter
poly_fight/orderbook.py  read-only CLOB depth/VWAP filters for self-run shadow
poly_fight/dashboard.py  read-only dashboard/API
poly_fight/storage.py    SQLite follow-state persistence
tests/test_core.py       unittest coverage
```

Dependencies are allowed when they materially improve security or correctness.
Keep runtime dependencies minimal, version-locked in `requirements.txt`, covered
by tests, and installed into the project virtual environment.  Never add a
dependency merely for convenience when a small existing implementation is safer.

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

VPS deploy/restart (direct SSH for routine releases):

For an already-provisioned VPS with working SSH access, routine deployments must
connect directly to the VPS: update the live checkout to `origin/main`, install
locked dependencies when needed, and restart the `poly-fight-dashboard` systemd
unit. Do not launch `launcher/launcher.py` for a normal code release. The launcher
is reserved for first-time/bootstrap work such as SSH key pairing, installing the
base packages, creating the systemd unit, or configuring Caddy/firewall.

The dashboard runs as a `poly-fight-dashboard` systemd unit bound to `127.0.0.1`,
fronted by Caddy for HTTPS; the **paper runner and observe processes are spawned
by the dashboard panel**, not as a separate systemd unit or manual argv. A
dashboard-only release preserves child processes. A release that changes runner,
observer, AI, evidence or storage code must stop and restart them through the
authenticated dashboard control path so they load the new code; never spawn them
with a manual argv.

```text
1. Read local private ops notes outside git for the host + login method.
2. Commit and push local changes to GitHub first.
3. Connect over SSH and abort if the VPS worktree is dirty; otherwise fetch,
   check out `main`, and reset the live checkout to `origin/main`.
4. Install `requirements.txt` into the existing project virtual environment if
   dependencies may have changed. If runtime code changed, stop runner/observer
   through dashboard controls before switching code. Restart the
   `poly-fight-dashboard` unit directly, then start runner/observer again through
   dashboard controls. Never spawn either with a manual argv.
5. Use the launcher only when the VPS has not yet been provisioned or its
   SSH/systemd/Caddy/firewall setup needs bootstrap repair.
6. Start a stopped runner / realtime refresh from the dashboard panel, not by
   manual argv.
7. Verify: VPS repo HEAD matches local, `systemctl is-active
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
