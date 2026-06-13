# Poly Fight

Read-only Polymarket **smart-wallet analysis** and **paper-follow** research
tool for narrow esports and sports winner markets. It discovers high-quality
wallets from historical trades, scores them into a leaderboard, then simulates
following their entries as paper legs — **no live orders, private keys,
balances, or approvals are ever used.**

The project runs on the **Python standard library only** (no third-party
runtime dependencies). The dashboard UI ships vendored JS assets.

---

## Requirements

- Python 3.10+
- Network access to Polymarket's public Gamma + Data APIs
- (Optional) Node.js, only to run the JS dashboard unit test

---

## Quick start

### 1. Collect wallets and build the leaderboard

```bash
python3 collect.py
```

This discovers candidate wallets from historical closed-market trades and writes
a strict A-grade `smart_wallet_leaderboard.json` under `data/`. The equivalent
explicit form is `python3 -m poly_fight.cli collect`.

> Collection writes into `data/` and makes live API calls. Lower throughput with
> `--max-requests-per-second 4` if you hit 429/503 errors.

### 2. Run the paper-follow loop

```bash
python3 -m poly_fight.cli run --stake-usdc 1 --stake-ratio-percent 10 --bankroll-usdc 1000
```

`run` refreshes the leaderboard, then keeps polling leaderboard wallets and
opening simulated paper legs when they buy into watched upcoming markets. Use
`--skip-initial-build` to skip the opening leaderboard refresh.

### 3. Open the dashboard

```bash
export POLY_FIGHT_DASH_PASSWORD='change-me'
export POLY_FIGHT_DASH_COOKIE_SECRET='change-me-too'
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

Visit `http://127.0.0.1:8787` and log in with user `admin` and your dashboard
password.

---

## Commands

All commands run through `python3 -m poly_fight.cli <subcommand>`. Global options
(`--data-dir`, rate limits) go **before** the subcommand:

```bash
python3 -m poly_fight.cli --data-dir data_smoke build-leaderboard
```

### Collect & build the leaderboard

```bash
python3 -m poly_fight.cli build-leaderboard
python3 -m poly_fight.cli collect
python3 -m poly_fight.cli collect --category sports
python3 -m poly_fight.cli collect --discovery-source holders   # experimental; biased after resolution
```

Collection uses bounded network concurrency by default:

```bash
python3 -m poly_fight.cli collect --max-workers 8 --max-requests-per-second 10 --request-burst 5
```

`--max-requests-per-second` is the real throughput ceiling. If 429/503 errors
increase, lower it first (e.g. `--max-requests-per-second 4`).
`--max-requests-per-second 0` disables rate limiting for debugging only — use
carefully and lower workers at the same time.

Batched discovery shares cache/profile state across batches:

```bash
python3 -m poly_fight.cli collect --classification-lookback-days 15 --market-batch-size 50 --market-batch-index 0
python3 -m poly_fight.cli collect --classification-lookback-days 15 --market-batch-size 50 --market-batch-index 1
```

### Analyze an event

```bash
python3 -m poly_fight.cli analyze-event
python3 -m poly_fight.cli analyze-event --event-slug dota2-aur1-ty-2026-06-04
python3 -m poly_fight.cli analyze-event --condition-id 0x...
```

### Paper follow

```bash
# One tick
python3 -m poly_fight.cli follow --stake-usdc 1 --stake-ratio-percent 10 --bankroll-usdc 1000

# Continuous loop (recommended stage-two entrypoint)
python3 -m poly_fight.cli run --stake-usdc 1 --stake-ratio-percent 10 --bankroll-usdc 1000
```

Stake sizing:

- `--stake-usdc` — minimum paper stake per BUY leg.
- `--stake-ratio-percent` — target-wallet cash replication ratio. The leg size is
  `max(--stake-usdc, wallet_trade_cash * ratio_percent / 100)`.
- `--bankroll-usdc` — caps total open paper exposure. If the bankroll cannot
  cover the proportional stake it is capped to the minimum (flagged), or skipped.

Each tick pulls recent `trades?user=<wallet>` pages, advances a local per-wallet
cursor (the Data API has no documented time-range parameter for `/trades`), and
ignores historical trades on cold start. BUY trades in watched markets create
paper legs; SELL trades mirror-exit the open position. Material same-market
SELLs (cumulative size over `--quarantine-sell-frac`, default 0.2) and
opposite-side BUYs quarantine a wallet until the next clean leaderboard cycle.

The loop keeps tracking markets that already have open signals. Strict wallets
appearing on opposite outcomes of the same `conditionId` mark both sides
`contested=True`. The first post-start price snapshot is stored as closing-line
value (`wallet_clv` / `our_clv`). REST failures are isolated: per-wallet errors
don't stop the tick; broader errors are retried after `--error-retry-seconds`
(default 180) and only halt the process after `--max-consecutive-error-seconds`
(default 600) of continuous failure.

### Dashboard server

```bash
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

The dashboard is **read-only** over `follow.db` and the JSON outputs. It never
places trades. The only live external request it makes is the wallet-trades
proxy. Authenticated users can trigger a background smart-wallet refresh, start/
stop the runner, set a manual paper balance cap, manage favorites, and reset
generated data — nothing that writes follow-signal state directly.

- **VPS / TLS:** put the service behind nginx + TLS and run with
  `--host 0.0.0.0 --cookie-secure`. Leave `--cookie-secure` off for local HTTP.
- **Static assets:** the UI defaults to `poly_fight/dashboardV2/`. Override with
  `--static-dir <dir>`.
- **Mock mode:** append `?mock=1` to the dashboard URL to render the UI from
  built-in fixtures without a populated `follow.db` — useful for UI verification.

---

## Outputs and state

Generated data is written under `data/` (git-ignored):

```text
data/{esports,sports}/
  discovery_slate.json
  candidate_wallets.json
  wallet_profiles.json
  smart_wallet_leaderboard.json
  build_summary.json
  raw_market_trades/  raw_user_trades/
data/follow/
  follow.db                 # source of truth (see below)
  follow_state.json         # thin metadata/compatibility file
  active_market_cache.json
  follow_control.json       # runner/refresh/pause control
```

`follow.db` is the long-running source of truth: wallet cursors, open/closed
paper signals, legs, behavior events, results, quarantine, CLV/contested fields,
performance, manual balance cap, and the configurable follow strategy.

Diagnostic logs live outside `data/` under `logs/follow/` (e.g.
`follow_run_log.jsonl`). Runtime-downloaded team logos are cached locally. Both
are git-ignored — they persist locally but are never committed.

### Leaderboard strictness

`smart_wallet_leaderboard.json` is intentionally strict: by default it exports
only the top ~30 A-grade wallets that are recently active, participate in enough
discovery-window markets, carry meaningful average market size, and have no
same-condition two-sided or tail-entry flags. Scoring uses a Wilson lower bound
at 80% confidence (`z=1.28`) so strong wallets with a few historical losses are
not treated as noise. Entry timing uses the real match start time when Gamma
provides it, not market end time.

---

## Market scope

| Category | Allowed |
|---|---|
| esports | LOL / CS2 / Dota2 — full-match winner, Dota2/LOL Game N Winner, CS2 Map N Winner |
| sports | leagues NBA and UFC only — moneyline / main-match winner |

Excluded: Valorant, MLB, NFL, props, spreads, totals, handicaps, futures,
correct scores, kills, first blood, towers, Roshan, and similar derived plays.
New paper signals are currently **esports-only**; sports data may be collected
and displayed but sports wallets do not open new paper signals.

---

## Tests

```bash
python3 -m unittest discover -s tests -v        # all Python tests
python3 -m unittest tests.test_core -v          # one module
node --test tests/dashboardv2_strategy_mapping.test.js   # JS strategy mapping
```

---

## More

`AGENTS.md` documents the full operational policy — collection limits, wallet
scoring signals, follow eligibility rules, the dashboard mutation allowlist,
known limitations, and VPS deployment checklist. Read it before changing scope,
scoring, or follow behavior.
