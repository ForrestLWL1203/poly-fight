# Poly Fight

Read-only Polymarket **smart-wallet analysis** and **paper-follow** research
tool for narrow esports and sports winner markets. It discovers high-quality
wallets from historical trades, scores them into a leaderboard, then simulates
following their entries as paper legs — **no live orders, private keys,
balances, or approvals are ever used.**

The project keeps dependencies deliberately small. `cryptography` is the only
third-party runtime dependency and protects the encrypted DeepSeek BYOK
credential envelope; the dashboard UI ships vendored JS assets.

---

## Requirements

- Python 3.10+
- `python3 -m pip install -r requirements.txt` (prefer a project `.venv`)
- Network access to Polymarket's public Gamma + Data APIs
- (Optional) Node.js, only to run the JS dashboard unit test

---

## Quick start

### 1. Collect wallets and build the leaderboard

```bash
python3 collect.py
```

This discovers candidate wallets from historical closed-market trades and writes
a strict A-grade leaderboard into `data/{category}/leaderboard.db`. The
equivalent explicit form is `python3 -m poly_fight.cli collect`.

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
such as `--data-dir` go **before** the subcommand; command-specific rate limits
go after it:

```bash
python3 -m poly_fight.cli --data-dir data_smoke build-leaderboard
```

### Collect & build the leaderboard

```bash
python3 -m poly_fight.cli build-leaderboard
python3 -m poly_fight.cli collect
python3 -m poly_fight.cli collect --category sports
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

Continuous refresh:

```bash
# periodic full rebuild from settled in-scope markets
python3 -m poly_fight.cli collect-v2 --loop-hours 2 --follow-dir data/follow

# ~10min: discover from ACTIVE (unsettled) watched markets over a volume gate and
# promote grade-A wallets EARLY so the follow loop can act before settlement
python3 -m poly_fight.cli observe-live --loop-minutes 10 --follow-dir data/follow
```

Both publish into the same per-category `leaderboard.db`; their publish critical
sections serialize via a build lock. The dashboard's realtime-refresh toggle
starts `observe-live`; periodic full rebuilds are also driven by the runner's
pool-refresh cycle.

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

Stake sizing: the runner sizes by a follow strategy stored in `follow.db`:
`stake = floor(current available balance × per-signal percent)`, capped by the
remaining per-match budget. The target wallet's order size is only a minimum
filter and never scales the stake. For example, $5,000 available at 2% produces
a $100 next leg, unless the match budget or remaining cash is lower. The flags
below are the legacy fallback when no strategy is configured:

- `--stake-usdc` — minimum paper stake per BUY leg.
- `--stake-ratio-percent` — target-wallet cash replication ratio:
  `max(--stake-usdc, wallet_trade_cash * ratio_percent / 100)`.
- `--bankroll-usdc` — caps total open paper exposure. If the bankroll cannot
  cover the proportional stake it is capped to the minimum (flagged), or skipped.

Fills are detected from on-chain logs when an RPC is configured, falling back to
`trades?user=<wallet>` polling otherwise. BUY trades in watched
markets create paper legs; sub-minimum BUY fills accumulate per
`(wallet, condition, outcome)` until they clear the minimum order, then follow
(small-buy accumulator). Funded entries must pass both `θ̂×0.95 > price` and the
strategy's configured observed-price floor/ceiling; neither gate scales the
stake, and there is no target-order conviction multiplier. Each newly-eligible wallet's
pre-existing positions are backfilled into legs once (startup and mid-run, when a
wallet is promoted onto the leaderboard live). SELL trades mirror-exit
proportionally. Quarantine is manual-only; score-driven demotion removes the
wallet from the leaderboard and scoring cache, while preserving paper records.

The loop keeps tracking markets that already have open signals. Strict wallets
appearing on opposite outcomes of the same `conditionId` mark the condition
contested and block funding the second direction. The first post-start price snapshot is stored as closing-line
value (`wallet_clv` / `our_clv`). REST failures are isolated: per-wallet errors
don't stop the tick; broader errors are retried after `--error-retry-seconds`
(default 180) and only halt the process after `--max-consecutive-error-seconds`
(default 600) of continuous failure.

### Dashboard server

```bash
python3 -m poly_fight.cli --data-dir data serve --host 127.0.0.1 --port 8787
```

The dashboard serves **every** response from SQLite (`follow.db` plus the
per-category `leaderboard.db`) — it never parses raw JSON outputs. Access is
**read-only** (`mode=ro`, `PRAGMA query_only=1`); it never places trades. The
live external requests are the wallet-trades proxy and explicit authenticated
DeepSeek/PandaScore credential tests. Authenticated users can trigger a
background smart-wallet refresh, start/stop the runner, set
a manual paper balance cap, manage favorites, and reset generated data — nothing
that writes follow-signal state directly.

- **VPS / TLS:** use the launcher only for first-time provisioning (repository,
  systemd, Caddy and firewall). Routine releases commit/push locally, update the
  VPS checkout over direct SSH, install locked requirements, and restart only the
  `poly-fight-dashboard` unit. The dashboard binds to `127.0.0.1` behind Caddy.
  See `launcher/README.md`.
- **Static assets:** the UI defaults to `poly_fight/dashboardV2/`. Override with
  `--static-dir <dir>`.
- **Mock mode:** append `?mock=1` to the dashboard URL to render the UI from
  built-in fixtures without a populated `follow.db` — useful for UI verification.

---

## Outputs and state

Generated data is written under `data/` (git-ignored):

```text
data/{esports,sports}/
  leaderboard.db            # dashboard source: leaderboard + collection runs
  discovery_slate.json      # collector intermediate products (not read by the
  candidate_wallets.json    #   dashboard) — kept as JSON for inspection
  wallet_profiles.json
  raw_market_trades/  raw_user_trades/
data/follow/
  follow.db                 # source of truth (see below)
  follow_state.json         # thin metadata/compatibility file
  follow_control.json       # runner/refresh/pause control
data/.secrets/
  ai_config.db              # AI settings + encrypted provider envelopes only
  credential_wrap_private.pem  # local RSA key, chmod 0600
```

The dashboard's "product" data lives only in SQLite now: the leaderboard and
collection summaries in each `leaderboard.db`; active/closed market caches, run
ticks (the old `follow_run_log.jsonl`), and all follow-signal state in
`follow.db`. The collector no longer writes `smart_wallet_leaderboard.json` /
`build_summary.json`, and the follow loop no longer writes
`follow_run_log.jsonl` — those products are persisted to SQLite. The remaining
JSON files above are collector intermediates or thin compatibility stubs.

`follow.db` is the long-running source of truth: wallet cursors, open/closed
paper signals, legs, behavior events, results, quarantine, CLV/contested fields,
performance, manual balance cap, run ticks, the configurable follow strategy,
and AI assessment/intent/shadow-position audit records.

### PandaScore-grounded DeepSeek main-match risk gate

The optional AI risk radar evaluates only LOL/CS2/Dota2 full-match winner BUYs.
Only after a BUY passes the deterministic strategy gates does the runner query
PandaScore. Team history uses a 120-day primary window; if a team has fewer than
8 matches it extends to 180 days, capped at 20 matches per team. Before calling
DeepSeek, raw responses are compacted: the most recent 10 matches retain bounded
date/opponent/result/BO/event details, while matches 11-20 contribute only to
aggregate record, recent-5/10 and BO splits. URLs, streams and unrelated provider
fields never enter the prompt.

The versioned prompt scores the full-match winner from a 50:50 baseline using
only this pre-match evidence. Wallet identity, intended side, price and stake
never leave the process. Strong opponent predictions (>=65% win
probability and >=75 confidence) block the paper leg; insufficient/stale data
or provider failures fail open. The feature is off by default and can be
configured, tested, enabled, or disabled from the dashboard's **AI 风控** page.
Each blocked intent maintains both the original-wallet shadow and a same-stake
AI-side hold-to-settlement shadow. Referenced team caches are removed after the
match settles; idle/expired rows are pruned automatically.

Historical settled main matches can be replayed without changing follow state:

```bash
python3 -m poly_fight.cli --data-dir data ai-backtest --limit 50
```

Historical opposite-side prices were not retained, so that command labels its
AI-side PnL as an estimate using the binary complement of the original entry.

Runtime-downloaded team logos are cached locally under `logs/`-adjacent dirs and
are git-ignored — they persist locally but are never committed.

### Leaderboard strictness

The exported leaderboard (persisted to `leaderboard.db`) is intentionally
strict: it exports only A-grade wallets that are recently
active, participate in enough
discovery-window markets, carry meaningful average market size, and have no
same-condition two-sided or tail-entry flags, with a 200-wallet safety cap.
Scoring uses recency-weighted point win rate, effective sample size and copy edge;
entry timing uses the real match start time when Gamma provides it, with market
end time only as the final fallback.

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
