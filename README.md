# Poly Fight

Polymarket esports smart-wallet analysis, implemented as a read-only batch CLI.

## Easiest Use

Collect wallets and build the leaderboard:

```bash
python3 collect.py
```

This uses historical closed-market trades by default to discover candidate
wallets. Historical holders can be tried explicitly, but they are biased after
markets resolve.

## Advanced Commands

Build the leaderboard:

```bash
python3 -m poly_fight.cli build-leaderboard
python3 -m poly_fight.cli collect
python3 -m poly_fight.cli collect --discovery-source holders
```

Collection uses bounded network concurrency by default:

```bash
python3 -m poly_fight.cli collect --max-workers 8 --max-requests-per-second 10 --request-burst 5
```

`--max-requests-per-second` is the real throughput ceiling. If 429/503 errors
increase, lower rps first, for example `--max-requests-per-second 4`.
`--max-requests-per-second 0` disables rate limiting for debugging; use that
carefully and lower workers at the same time.

Analyze a current esports event:

```bash
python3 -m poly_fight.cli analyze-event
python3 -m poly_fight.cli analyze-event --event-slug dota2-aur1-ty-2026-06-04
python3 -m poly_fight.cli analyze-event --condition-id 0x...
```

Run one paper follow tick:

```bash
python3 -m poly_fight.cli follow --stake-usdc 1
python3 -m poly_fight.cli follow --stake-usdc 1 --quarantine-sell-frac 0.2 --consensus-block-opposite
```

Run the paper follow loop:

```bash
python3 -m poly_fight.cli run --stake-usdc 1
```

`run` is the recommended stage-two entrypoint. It builds/refeshes the smart
wallet leaderboard, then keeps running follow ticks with adaptive sleep. The
default observation window starts 24 hours before match start and continues
until settlement.

Follow detection is trade-centric. Each tick pulls recent
`trades?user=<wallet>` pages for A wallets, advances a local per-wallet cursor,
and ignores historical trades on cold start. The Data API documents `timestamp`
as a response field but does not document a time-range query parameter for
`/trades`, so incremental behavior is implemented client-side with that cursor.
After cold start, the client fetches up to `--user-trades-max-pages` pages
(default 3) until it reaches the previous cursor. BUY trades in watched esports
markets create paper legs; SELL trades mirror-exit the open paper position.

The follow runner keeps tracking markets that already have open paper signals,
even after match start. If strict wallets appear on opposite outcomes of the
same conditionId, both sides are marked `contested=True` and
`would_follow=False` while still being retained for learning. The first
post-start price snapshot is stored as closing-line value (`wallet_clv` /
`our_clv`). Material same-market SELLs, including split sells whose cumulative
size exceeds `--quarantine-sell-frac` (default 0.2), and opposite-side BUYs
quarantine the wallet in SQLite so it is excluded from later follow ticks until
the next clean leaderboard cycle.

On wallet cold start, the follow loop also checks current positions once. If an
A wallet already holds a watched future-start esports market and the current
price is still within `--max-slippage-over-entry` of that wallet's average
entry, the paper loop opens one bootstrap leg. Disable this with
`--no-bootstrap-current-positions`.

REST API failures are isolated. Per-wallet trade/position failures do not stop
the tick. Broader event/build/write failures are caught by `run`, logged as
`run_iteration_error`, retried after `--error-retry-seconds` (default 180), and
only stop the process after `--max-consecutive-error-seconds` (default 600) of
continuous failures.

Run the read-only dashboard API:

```bash
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli serve --data-dir data --host 127.0.0.1 --port 8787
```

Open `http://127.0.0.1:8787`, then login with `admin` and the dashboard
password. For VPS domain deployment, put the service behind nginx + TLS and run
with `--host 0.0.0.0 --cookie-secure`. The dashboard is read-only: it reads
`follow.db`, `smart_wallet_leaderboard.json`, `active_market_cache.json`, and
`follow_run_log.jsonl`; only the wallet-trades endpoint proxies live Data API
requests with the same rate-limited client. Overview and wallet APIs expose v4
quality fields such as contested count, clean-vs-contested performance, average
CLV, and quarantine status.

Outputs are written under `data/`:

```text
esports_classification_set.json
discovery_slate.json
candidate_wallets.json
wallet_profiles.json
smart_wallet_leaderboard.json
build_summary.json
last_event_analysis.json
follow/follow_state.json
follow/follow.db
follow/active_market_cache.json
follow/follow_run_log.jsonl
```

`follow.db` is the primary long-running follow state. It stores wallet cursors,
open/closed paper signals, legs, behavior events, results, wallet quarantine,
CLV/contested fields, and performance.
`follow_state.json` is now a small compatibility file with DB/cache metadata.
`active_market_cache.json` is refreshed on the event-cache TTL instead of being
rewritten inside state every tick. `follow_performance.json` is treated as a
legacy migration input or optional export, not a per-tick synchronized state
file. Collection caches and
`smart_wallet_leaderboard.json` remain JSON files.

`smart_wallet_leaderboard.json` is intentionally strict. By default it exports
only core A-grade wallets for follow-signal research, capped at the top 30
wallets by default. A wallet must be recently active, participate in at least 3
discovery-window markets, have meaningful average market size, have no
same-condition two-sided market flags, and have no tail-entry flags.

Wallet history is capped for scoring: each profile uses at most the latest 50
closed esports main markets found in that wallet's recent closed-position
history. Non-esports markets are filtered out by conditionId before scoring.

Tail entry means late timing plus high average entry price. Plain late entry is
only an observation field: a wallet may keep buying after match start if it keeps
one direction and maintains a low average entry price. Entry timing uses the real
match start time when Gamma provides it, not market end time.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The project uses only the Python standard library.
