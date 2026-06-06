# AGENTS.md

This repo implements a read-only Polymarket esports smart-wallet analysis tool.

## Project Goal

Build an on-demand / scheduled batch analysis CLI, not a daemon.

The v1 workflow:

```text
build-leaderboard:
  closed esports markets -> discovery slate -> market trades -> candidate wallets
  -> wallet closed positions -> smart_wallet_leaderboard

analyze-event:
  active esports event -> top holders -> leaderboard join
  -> USD-normalized side comparison -> ignore/watch/candidate signal
```

No WebSocket, no automatic trading, no private CLOB execution.

## Commands

Run tests:

```bash
python3 -m unittest discover -s tests -v
```

User-friendly one-shot collection:

```bash
python3 collect.py
```

Batched deeper discovery:

```bash
python3 -m poly_fight.cli collect --discovery-lookback-days 15 --market-batch-size 50 --market-batch-index 0
python3 -m poly_fight.cli collect --discovery-lookback-days 15 --market-batch-size 50 --market-batch-index 1
python3 -m poly_fight.cli collect --discovery-lookback-days 15 --market-batch-size 50 --market-batch-index 2
```

Markets are volume-sorted descending before batching. Adjacent batches share the
same wallet profile cache and leaderboard output, so later batches enrich the
same smart-wallet database rather than creating isolated results.

Build leaderboard through the internal CLI:

```bash
python3 -m poly_fight.cli build-leaderboard
python3 -m poly_fight.cli collect
```

Analyze current esports event:

```bash
python3 -m poly_fight.cli analyze-event
python3 -m poly_fight.cli analyze-event --event-slug <slug>
python3 -m poly_fight.cli analyze-event --condition-id <condition_id>
```

Run one paper follow tick:

```bash
python3 -m poly_fight.cli follow --stake-usdc 1
```

Run the stage-two paper follow loop:

```bash
python3 -m poly_fight.cli run --stake-usdc 1
```

`run` is the preferred follow entrypoint. It starts with a leaderboard build
unless `--skip-initial-build` is passed, then runs follow ticks with adaptive
sleep. `follow` remains a one-tick debugging command.

Global CLI options such as `--data-dir` must come before the subcommand:

```bash
python3 -m poly_fight.cli --data-dir data_smoke build-leaderboard
```

## Architecture Notes

Keep two market sets separate:

```text
esports_classification_set:
  Historical esports main-match conditionIds.
  Used only to decide whether a closed position belongs to esports.

discovery_slate:
  Recent high-quality subset of classification_set.
  Used to find currently active candidate wallets.
```

Do not collapse these into one table. A short discovery window cannot support
the A/B wallet sample thresholds.

Candidate discovery uses frequency OR size:

```text
high_participation OR large_size
```

Frequency is only a cheap recall filter. It must not be used as a smart-wallet
rating signal.

Default discovery source is `trades?market=<conditionId>&takerOnly=false`.
Closed-market holders are an explicit experimental mode only; after resolution,
`outcomePrices` become 0/1 and holders can be biased toward unresolved/redeemed
balances.

Wallet rating must primarily use:

```text
realized PnL
win/loss count after excluding pnl=0 neutral markets
Wilson win-rate lower bound
entry edge = wilson_win_rate_lower_bound - median_entry_price
entry price
capital size
median market ROI
positive market rate
sample count
recency
bot-like score
```

Win rate alone is weak because favorites can have high win rate with poor edge.
ROI is no longer a hard grading gate once entry price is constrained; with low
entry prices and hold-to-settlement behavior, ROI mostly follows price. Treat
ROI as display context, not the primary A/B criterion.

`realizedPnl == 0` closed positions are treated as abnormal/neutral settlement
and excluded from win rate, ROI, and sample count. Track them separately as
`neutral_market_count`.

A/B grading uses Wilson win-rate lower bound at 80% confidence (`z=1.28`) so
small perfect samples do not beat larger strong samples without making 2-3
losses nearly fatal. It also compares Wilson lower bound against entry price,
because breakeven win rate equals buy price in binary markets. A-grade wallets
need enough filtered samples, enough capital at risk, low entry price, positive
entry edge, and non-bot/non-stale behavior. Losses lower Wilson and are recorded
in reasons, but a large strong sample can still qualify. Very small capital
wallets are downgraded as experimental/noise.

The exported `smart_wallet_leaderboard.json` is stricter than raw A/B grading.
It is a core A-wallet list for follow-signal use; B wallets remain useful for
research but are not exported by default:

```text
grade == A
last esports activity <= 90 days
candidate.participated_market_count >= 3 by default
candidate.avg_market_cash >= 1500 by default
candidate.two_sided_market_count == 0
max exported leaderboard wallets = 30 by default
```

Do not re-apply old raw win-rate / median-entry / late-entry zero-tolerance
filters at leaderboard time. Those belong in scoring or discovery profiling.
The leaderboard should mostly trust the A grade, while keeping recency,
meaningful recent size, and same-condition two-sided behavior as hard guards.

## Collection Efficiency Rules

Default collection should be fast and focused:

```text
reuse esports_classification_set cache for 24h unless --refresh-classification
reuse raw_market_trades cache for 7d unless --refresh-market-trades
max_workers = 8
max_requests_per_second = 10
request_burst = 5
classification_lookback_days = 14
target_markets = 20
discovery_lookback_days = 14
market_batch_size = 50
market_batch_count = 2
default market coverage = top100 by volume
max_pages_per_market = 3
max_esports_closed_positions_per_wallet = 50
closed_position_market_chunk_size = 50
check_current_positions = false by default
```

Wallet profiling fetches closed positions with a target-market scope:
`closed-positions?user=<wallet>&market=<csv conditionIds>`. ConditionIds come
from the recent esports classification scope (`--classification-lookback-days`,
default 14) and are sent in chunks (`--closed-position-market-chunk-size`,
default 50) to avoid oversized URLs. It then merges, dedupes, sorts by
timestamp, and scores at most the latest 50 closed positions from that scoped
market type. The old raw all-category cap is kept only as a backward-compatible
CLI knob; the default path should not deep scan unrelated categories.

Network collection is concurrent but rate-limited. Treat rps as the true
throughput knob and workers as waiting slots. If Polymarket returns more 429/503
responses, lower `--max-requests-per-second` first. `--max-requests-per-second
0` disables the limiter only for debugging; pair it with lower workers.

Profile fetching uses a deterministic budget before dispatch:

```text
70% new profile candidates
30% existing-profile schema/scoring migration
unused budget spills to the other side
candidate wallet wins when duplicated across both sets
```

For broader discovery, prefer batching over a single huge run:

```text
--discovery-lookback-days 15
--market-batch-size 50
--market-batch-index 0,1,2...
```

Before deep wallet profiling, apply the cheap discovery-time filter:

```text
participated_market_count >= 3
avg_market_cash >= 1500
two_sided_market_count == 0
tail_entry_market_count == 0
```

This intentionally focuses deep profiling on wallets with both repeated
participation and meaningful capital. Slate participation is only a discovery
signal, not a final quality gate; historical Wilson + entry edge decide quality.
Two-sided wallets are hard-excluded. Tail-entry is also hard-excluded because it
means high-price chasing. Plain late-entry is an observation field,
not a hard exclusion: a wallet may add after match start if it already built a
position, keeps one direction, and maintains a low average entry price. Tail
entry means late timing plus high average entry price. Churn by trade count is
an observation field only; do not exclude
wallets merely because they split a position across many buys. Dirty,
two-sided, or tiny-size wallets can remain in
`candidate_wallets.json` for inspection, but should not consume closed-position
requests by default.

Closed-market trades are immutable enough for v1 discovery. Cache them by
conditionId plus fetch parameters under `data/raw_market_trades/`; repeated
same-day collection should normally hit cache for all discovery markets.

`leaderboard_wallet_overlap.json` records strict-wallet historical market union
and intersections for research only. Collection must not generate heavy-stake
signals. Heavy-stake decisions belong in the later follow script, which should
poll upcoming events before match start and watch whether multiple strict
wallets buy the same side in real time.

## Paper Follow

The `follow` subcommand is read-only shadow mode. It never touches CLOB, private
keys, balances, approvals, or live orders.

Per tick:

```text
read A-wallet leaderboard
filter follow-eligible wallets with 30-day recency and wallet_quarantine exclusion
build watched esports market set from start_time within the 24h observe window
if watched markets or open signals exist, poll latest trades?user= for follow wallets
plus wallets with still-open paper signals
cold-start wallets set last_trade_cursor only
new BUY trades in watched pre-start markets create paper legs
wallets no longer eligible can only affect their already-open signal markets;
they cannot create new paper signals
new trades for already-open signal markets remain tracked even after start
new SELL trades mirror-exit all open legs for that wallet-market-outcome
material same-market SELL or opposite-side BUY writes wallet_quarantine
post-start market snapshot records closing-line value (CLV) once
same conditionId with open signals on both outcomes marks all sides contested
settled markets move from open signals to compact results
performance is aggregated by wallet, overall, and clean/contested groups
```

Polymarket Data API `/trades` documents `timestamp` in the response, but does
not document a time-range query parameter. Do not rely on `since`/date filters.
Use a per-wallet local cursor `{timestamp, id}` and fetch recent pages
(`--user-trades-limit`, default 100; `--user-trades-max-pages`, default 3),
reading until the cursor. First sight of a wallet is baseline only and must not
follow old trades.

Cold-start exception: also check current positions once. If an A wallet already
holds a watched future-start esports market and the current price is still no
more than `--max-slippage-over-entry` above the wallet average entry, create one
bootstrap paper leg. This catches "script started after smart wallet already
built early" cases. It is controlled by `--bootstrap-current-positions`
(default on) / `--no-bootstrap-current-positions`.

REST API failure policy:

```text
per-wallet trade/position failure -> return empty for that wallet, tick continues
broader build/follow failure -> run logs run_iteration_error and sleeps
error retry interval -> --error-retry-seconds, default 180
continuous outage stop -> --max-consecutive-error-seconds, default 600
```

This strategy avoids stopping on one bad wallet/API response, but exits with
status 2 if Polymarket/network appears unavailable for about 10 minutes.

Resolution lookup uses a separate short cache from active event discovery:
`--resolution-cache-ttl-seconds` defaults to 60 and
`--resolution-gamma-pages` defaults to 2. Do not reuse the 15-minute active event
cache TTL or broader discovery page budget for settlement checks.

Follow output lives under `data/follow/`:

```text
follow.db
follow_state.json
active_market_cache.json
follow_run_log.jsonl
follow_control.json
```

`follow.db` is the source of truth for wallet cursors, open/closed paper
signals, legs, behavior events, results, and performance. `follow_state.json` is
only a thin compatibility/metadata file. `active_market_cache.json` is separate
from state and refreshes on the event cache TTL; do not put the large active
market list back into `follow_state.json`. `follow_performance.json` is legacy
migration input or optional export only, not a per-tick synchronized state file.
`follow_control.json` stores dashboard-visible operational status such as wallet
refresh, runner state, and follow pause flags; it is not signal state.

Paper signals record both the smart wallet entry basis and our detected current
price basis. `would_follow` is only a paper flag based on
`--max-slippage-over-entry`; signals are still retained for threshold analysis.
The first tick should normally create no signals because it only establishes the
baseline of already-open positions.

Follow v4 quality controls:

```text
contested market:
  same conditionId has open follow signals on both outcomes
  all signals remain recorded but get contested=True and would_follow=False

closing-line value:
  first post-start active market price snapshot
  wallet_clv = closing_line_price - wallet_entry_price
  our_clv = closing_line_price - our_entry_price

wallet_quarantine:
  pre/live safety net between leaderboard rebuilds
  material SELL threshold uses --quarantine-sell-frac, default 0.2
  cumulative split sells count toward the threshold
  opposite-side BUY on the same conditionId quarantines as two_sided_switch
  quarantined wallets are excluded from future follow-eligible polling
```

`--consensus-block-opposite` defaults on. `--consensus-min-same-side` exists for
live policy tuning, but paper v4 still records single-wallet signals for
learning. Contested signals should not be treated as live-followable.

## Dashboard API

`serve` runs a read-only dashboard/API process:

```bash
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli serve --data-dir data --host 127.0.0.1 --port 8787
```

It follows the poly-monitor style: username/password login, signed HttpOnly
cookie session, and stdlib `ThreadingHTTPServer`. For VPS + TLS deployment use
`--host 0.0.0.0 --cookie-secure`; local HTTP debugging should leave
`--cookie-secure` off.

Dashboard SQLite access must be strictly read-only. Do not call
`FollowStore.init_db()` or any existing write-capable `load_*` method from
dashboard request paths. Use the dedicated read-only snapshot path, which opens
SQLite with `mode=ro` and `PRAGMA query_only=1`; if `follow.db` does not exist
yet, endpoints should return empty/waiting data instead of creating a DB or
crashing.

The dashboard must not mutate watchlists, signals, cursors, performance, open
positions, or results. It may perform the limited operational controls exposed
by the current UI:

```text
POST /api/wallet-refresh   start a one-shot collector refresh
POST /api/runner/start     start the paper run loop
POST /api/runner/stop      stop the paper run loop
```

These controls write `follow_control.json` / process state only; they must not
edit follow signal state. The only live external Data API request from the
dashboard is `/api/wallets/{addr}/trades`; validate the wallet address before
calling the Data API and reuse the normal rate-limited `PolymarketClient`.

Dashboard overview should expose v4 learning fields: contested count, clean
count, average wallet CLV, clean/contested performance groups, and wallet
quarantine status. These are read-only facts from `follow.db`, not controls.

`GET /api/stream` is a lightweight Server-Sent Events endpoint for the static
dashboard. It must stay same-origin/cookie-authenticated, send an immediate
header frame, heartbeat with `: ping`, cap active clients, and release the count
in `finally`. Use `follow_snapshot_updated_at` from SQLite meta plus run log,
control file, and leaderboard mtimes for dirty flags. Do not switch this to
WebSocket unless the project explicitly changes the zero-dependency design.

## Follow-Signal Principle

For future copy-trading logic, single-wallet signals are weak by default.

Strong signals should require multiple strict smart wallets converging on the
same side before or early in the event window:

```text
strong follow candidate:
  >= 2 core/supplement smart wallets
  same event conditionId
  same outcome side
  entry is early, not tail-water
  no two-sided holder contamination
```

This matters because one wallet can be idiosyncratic, hedged, or wrong. Two or
more independent strict wallets buying the same side early is much closer to the
kind of predictive signal the project is trying to capture.

Do not implement this heavy-stake signal in the collection script. The collector
only builds and refreshes wallet intelligence. A separate follow script should
use that wallet set, poll upcoming esports markets before start time, detect
same-side buys from multiple strict wallets, and then decide stake sizing.

## Important Pitfalls

- Normalize all wallet keys to lowercase before joining, caching, or deduping.
- Closed positions are filtered by `conditionId in esports_classification_set`,
  never by title keyword matching.
- Compare holder conviction in USD, not token amount:

```text
holder_usd_value = holder_amount * outcome_current_price
```

- Same-condition two-sided holdings are suspicious. Different conditionIds are
  not automatically suspicious.
- `holders` is reliable for active event snapshots, not default offline
  discovery.
- Offline discovery uses `trades?market=<conditionId>&takerOnly=false`; keep
  `max_pages_per_market` capped.
- Use real match start time for entry timing. Prefer `market.eventStartTime`,
  then `event.startTime`, then `market.gameStartTime`. Do not infer early/late
  entry from `end_date` unless no start field exists.
- Unknown holders should not block `analyze-event`; profile them later.
- Negative/unqualified profiles need TTL-based negative caching to avoid
  repeatedly profiling the same weak wallets.

## Current Implementation

Core modules:

```text
poly_fight/core.py  pure logic and scoring
poly_fight/api.py   Polymarket read-only HTTP client
poly_fight/cli.py   build-leaderboard and analyze-event commands
poly_fight/dashboard.py read-only dashboard/API server
poly_fight/storage.py SQLite follow-state persistence
tests/test_core.py  standard-library unittest coverage
```

The project intentionally uses only the Python standard library.

## Generated Data

Runtime output goes under `data/` by default:

```text
esports_classification_set.json
discovery_slate.json
candidate_wallets.json
wallet_profiles.json
smart_wallet_leaderboard.json
leaderboard_wallet_overlap.json
build_summary.json
last_event_analysis.json
raw_market_trades/
```

These files are generated artifacts. Avoid committing large live data dumps
unless the user explicitly asks.

Follow long-running business data lives in `data/follow/follow.db`; collection
caches and smart-wallet leaderboard outputs remain JSON and are refreshed by the
collector.

## VPS Operations Notes

Sweden VPS information was originally recorded in
`/Users/forrestliao/workspace/poly-monitor/AGENTS.md`.

Connection:

```bash
SSHPASS="$(cat /Users/forrestliao/workspace/new-poly/docs/sweden-vps-secret.txt)" \
  sshpass -e ssh root@70.34.207.45
```

Do not write the actual password into this repository. Keep using the local
secret file above.

Current known poly-fight paths on the VPS:

```text
/opt/poly-fight/repo
/opt/poly-fight/data
/opt/poly-fight/data/follow
```

Known paper runner shape:

```bash
python3 -u -m poly_fight.cli --data-dir /opt/poly-fight/data run --stake-usdc 1 --max-profiles-per-run 1000 --skip-initial-build
```

Operational notes:

- `data/follow/follow.db` is the long-running paper follow source of truth.
- `data/follow/follow_run_log.jsonl` is diagnostic and should stay small after
  log trimming.
- `data/raw_market_trades/` is the largest collection cache. Same-market cache
  files are reused, but new market conditionIds create new files; add cleanup if
  VPS disk pressure becomes a problem.
- Do not edit live code directly on the VPS unless explicitly asked. Prefer
  local commit/push, then pull/deploy on the VPS.

## Git Workflow Preference

When the user asks to submit or push code for this repository, commit locally and
push directly to the configured GitHub branch. Do not try to create a GitHub PR
unless the user explicitly asks for a PR.

## Known v1 Limitations

- Realized PnL creates survivorship / unrealized-position bias.
- ROI is not time-normalized.
- `holders?limit=10` can miss medium-sized smart wallets ranked 11-30.
- `trades?market&takerOnly=false` may still have maker/taker coverage bias.
- The signal is observational only and should not be treated as an auto-trade
  recommendation.
