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

A/B grading uses Wilson win-rate lower bound so small perfect samples do not
beat larger strong samples. It also compares Wilson lower bound against entry
price, because breakeven win rate equals buy price in binary markets. A-grade
wallets need enough filtered samples, enough capital at risk, low entry price,
positive entry edge, and non-bot/non-stale behavior. Losses lower Wilson and are
recorded in reasons, but a large strong sample can still qualify. Very small
capital wallets are downgraded as experimental/noise.

The exported `smart_wallet_leaderboard.json` is stricter than raw A/B grading.
It is a core A-wallet list for follow-signal use; B wallets remain useful for
research but are not exported by default:

```text
grade == A
last esports activity <= 90 days
candidate.participated_market_count >= 3 by default
candidate.avg_market_cash >= 1500 by default
candidate.two_sided_market_count == 0
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
target_markets = 20
discovery_lookback_days = 14
market_batch_size = 50
market_batch_count = 2
default market coverage = top100 by volume
max_pages_per_market = 3
max_closed_positions_per_wallet = 500
max_esports_closed_positions_per_wallet = 50
check_current_positions = false by default
```

Wallet profiling fetches recent closed positions sorted by timestamp, filters
them by `conditionId in esports_classification_set`, and scores at most the
latest 50 esports closed positions per wallet. The raw closed-position cap
remains higher so wallets with other categories mixed into their history can
still surface enough esports records.

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

## Known v1 Limitations

- Realized PnL creates survivorship / unrealized-position bias.
- ROI is not time-normalized.
- `holders?limit=10` can miss medium-sized smart wallets ranked 11-30.
- `trades?market&takerOnly=false` may still have maker/taker coverage bias.
- The signal is observational only and should not be treated as an auto-trade
  recommendation.
