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
python3 -m poly_fight.cli follow --stake-usdc 25
```

The first tick cold-starts wallet position baselines and should not emit old
in-flight positions as signals. Later ticks detect new pre-match esports
positions opened by A wallets, write paper signals, and settle them after
resolution. Schedule this command externally, for example every 3 minutes.

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
follow/follow_signals_open.json
follow/follow_results.jsonl
follow/follow_performance.json
follow/follow_run_log.jsonl
```

`smart_wallet_leaderboard.json` is intentionally strict. By default it exports
only core A-grade wallets for follow-signal research. A wallet must be recently
active, participate in at least 3 discovery-window markets, have meaningful
average market size, have no same-condition two-sided market flags, and have no
tail-entry flags.

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
