/* Poly Sniper dashboard V2 — mock fixtures (window.PSMock).
   API-shaped responses for verifying the UI when local data is sparse.
   Activate by loading the dashboard with ?mock=1 — api.js returns these
   instead of hitting the backend, so the real adapters/components run.    */
(function () {
  "use strict";
  const now = Date.now();
  const iso = (offsetH) => new Date(now + offsetH * 3600 * 1000).toISOString();
  const ago = (s) => Math.floor(now / 1000) - s;

  /* ---- shared market fixtures ---- */
  const M = {
    cs2_main: {
      condition_id: "0xmockcs2parivisionmonte0001",
      title: "Counter-Strike: PARIVISION vs Monte (BO3) - IEM Cologne Major",
      match_parts: { game: "Counter-Strike", teamA: "PARIVISION", teamB: "Monte", meta: "(BO3) IEM Cologne Major" },
      team_logos: { teamA: "", teamB: "" },
      market_type: "main_match", market_type_label: "主盘",
      match_start_time: iso(-1.5), end_date: iso(2.5),
      outcomes: ["PARIVISION", "Monte"], outcome_prices: [0.555, 0.445],
    },
    dota_main: {
      condition_id: "0xmockdotaspiritfalcons0002",
      title: "Dota 2: Team Spirit vs Falcons (BO3) - ESL One",
      match_parts: { game: "Dota 2", teamA: "Team Spirit", teamB: "Falcons", meta: "(BO3) ESL One 半决赛" },
      team_logos: { teamA: "", teamB: "" },
      market_type: "main_match", market_type_label: "主盘",
      match_start_time: iso(3), end_date: iso(7),
      outcomes: ["Team Spirit", "Falcons"], outcome_prices: [0.62, 0.38],
    },
    lol_main: {
      condition_id: "0xmocklolt1geng0003",
      title: "League of Legends: T1 vs Gen.G (BO5) - LCK Playoffs",
      match_parts: { game: "League of Legends", teamA: "T1", teamB: "Gen.G", meta: "(BO5) LCK 决赛" },
      team_logos: { teamA: "", teamB: "" },
      market_type: "main_match", market_type_label: "主盘",
      match_start_time: iso(-20), end_date: iso(-16),
      outcomes: ["T1", "Gen.G"], outcome_prices: [0.48, 0.52],
    },
  };

  /* ---- overview ---- */
  const overview = {
    db_ready: true,
    open_signal_count: 7, result_count: 52, settled_count: 42, exited_count: 10,
    win_rate: 0.73, our_realized_pnl: 2481.5, hypothetical_pnl: 2600.0, wallet_basis_realized_pnl: 2500.0,
    total_stake: 20100.0, resolved_stake: 16920.0, realized_roi: 0.1234, wallet_basis_realized_roi: 0.1477,
    open_exposure: 3180.0, account_total_equity_usdc: 8642.18,
    account_balance: { configured: true, balance_usdc: 5462.18, source: "manual", updated_at: ago(3600) },
    clean_signal_count: 14, two_sided_signal_count: 2, disagreement_signal_count: 2,
    win_rates_by_game: [
      { game: "dota2", game_label: "Dota 2", wins: 18, losses: 7, settled_count: 25, win_rate: 0.72 },
      { game: "lol", game_label: "LoL", wins: 9, losses: 5, settled_count: 14, win_rate: 0.64 },
      { game: "cs2", game_label: "CS2", wins: 11, losses: 2, settled_count: 13, win_rate: 0.85 },
    ],
    follow_type_distribution: {
      total: 62, total_stake: 20100,
      by_game: [
        { game: "dota2", game_label: "Dota 2", total: 21, total_stake: 7050, types: [
          { type: "main_match", label: "主盘", count: 14, stake: 5200 }, { type: "sub_game", label: "Sub Game", count: 7, stake: 1850 }] },
        { game: "cs2", game_label: "CS2", total: 21, total_stake: 7000, types: [
          { type: "main_match", label: "主盘", count: 12, stake: 4400 }, { type: "sub_game", label: "Sub Game", count: 9, stake: 2600 }] },
        { game: "lol", game_label: "LoL", total: 20, total_stake: 6050, types: [
          { type: "main_match", label: "主盘", count: 11, stake: 3650 }, { type: "sub_game", label: "Sub Game", count: 9, stake: 2400 }] },
      ],
    },
    open_by_game: [
      { game: "dota2", game_label: "Dota 2", count: 3 },
      { game: "cs2", game_label: "CS2", count: 2 },
      { game: "lol", game_label: "LoL", count: 2 },
    ],
    // settled results over time (each = one settlement's realized pnl).
    // windows: 24h=+30, 7d=+150, 30d=+230, 至今=+2481.50 (== realizedPnl)
    equity_points: [
      { timestamp: ago(86400 * 90), pnl: 451.5 },
      { timestamp: ago(86400 * 70), pnl: 700 },
      { timestamp: ago(86400 * 55), pnl: 900 },
      { timestamp: ago(86400 * 40), pnl: 200 },
      { timestamp: ago(86400 * 10), pnl: 80 },
      { timestamp: ago(86400 * 3), pnl: 120 },
      { timestamp: ago(3600 * 10), pnl: -20 },
      { timestamp: ago(3600), pnl: 50 },
    ],
  };

  /* ---- wallets ---- */
  const mkWallet = (over) => Object.assign({
    wallet: "0x0000000000000000000000000000000000000000", category: "esports",
    grade: "A", rank: 1, best_bucket_score: 90, esports_roi: 0.2, overall_esports_roi: 0.12,
    avg_market_cash: 8000, recent_bucket_roi: 0.15, last_esports_trade_at: ago(900),
    eligible_buckets: ["cs2:main_match"], observed_buckets: ["cs2:main_match"],
    observed: { signals: 0, wins: 0, losses: 0, exits: 0, open: 0, our_pnl: 0, wallet_pnl: 0, win_rate: null, has_loss: false },
    favorite: false, quarantined: false, quarantine_reason: null, quarantined_at: null,
  }, over);
  const wallets = {
    wallets: [
      mkWallet({ wallet: "0x8f3c2a1b9d4e5f60718293a4b5c6d7e8f9012345", rank: 1, grade: "A", best_bucket_score: 96, esports_roi: 0.284, overall_esports_roi: 0.142, avg_market_cash: 12480, recent_bucket_roi: 0.221, eligible_buckets: ["dota2:main_match", "cs2:main_match"], favorite: true, observed: { signals: 7, wins: 6, losses: 1, exits: 0, open: 2, our_pnl: 412.8, wallet_pnl: 430, win_rate: 0.857, has_loss: true } }),
      mkWallet({ wallet: "0x2b71e9c4a8d3f50612839a4b5c6d7e8f90126789", rank: 2, grade: "A", best_bucket_score: 93, esports_roi: 0.247, overall_esports_roi: 0.118, avg_market_cash: 8240, recent_bucket_roi: 0.186, eligible_buckets: ["cs2:map_winner"], observed_at: ago(3600), observed: { signals: 12, wins: 9, losses: 3, exits: 0, open: 1, our_pnl: 286.4, wallet_pnl: 300, win_rate: 0.75, has_loss: true } }),
      mkWallet({ wallet: "0x5d92f0a6b1c8e7430219384a5b6c7d8e9f013579", rank: 3, grade: "B", best_bucket_score: 81, esports_roi: 0.212, overall_esports_roi: 0.094, avg_market_cash: 19600, recent_bucket_roi: -0.042, eligible_buckets: ["lol:main_match"], observed: { signals: 5, wins: 4, losses: 1, exits: 0, open: 1, our_pnl: 198.2, wallet_pnl: 210, win_rate: 0.8, has_loss: true } }),
      mkWallet({ wallet: "0x1a40c7e2b9d6f8530412938a4b5c6d7e8f024680", rank: 4, grade: "B", best_bucket_score: 74, esports_roi: 0.121, overall_esports_roi: 0.066, avg_market_cash: 4980, recent_bucket_roi: null, eligible_buckets: ["dota2:main_match"], observed: { signals: 0, wins: 0, losses: 0, exits: 0, open: 0, our_pnl: 0, wallet_pnl: 0, win_rate: null, has_loss: false } }),
      mkWallet({ wallet: "0xaa31f7c2e9b4d6850219384a5b6c7d8e9f079135", rank: null, grade: "C", best_bucket_score: 64, esports_roi: -0.084, overall_esports_roi: -0.05, avg_market_cash: 3200, recent_bucket_roi: -0.12, eligible_buckets: ["cs2:main_match"], quarantined: true, quarantine_reason: "opposite_wallet_buy", quarantined_at: ago(7200), last_esports_trade_at: ago(7200) }),
    ],
    active_count: 4, favorite_count: 1, quarantined_count: 1,
    leaderboard_updated_at: ago(1800), scoring_version: 15, db_ready: true,
  };

  /* ---- events ---- */
  const mkEvent = (m, over) => Object.assign({
    condition_id: m.condition_id, category: "esports",
    match_parts: m.match_parts, team_logos: m.team_logos,
    match_start_time: m.match_start_time, end_date: m.end_date,
    outcomes: m.outcomes, outcome_prices: m.outcome_prices,
    market_type: m.market_type, market_type_label: m.market_type_label,
    open_signal_count: 0, signal_count: 0, result_count: 0, contested: false, side_counts: {},
    event_url: "https://polymarket.com/event/" + (m.condition_id || "demo"),
  }, over);
  const events = {
    events: [
      mkEvent(M.cs2_main, { open_signal_count: 5, signal_count: 5, side_counts: { PARIVISION: 4, Monte: 1 } }),
      mkEvent(M.dota_main, { open_signal_count: 2, signal_count: 2, side_counts: { "Team Spirit": 2 } }),
      mkEvent({ ...M.lol_main, match_start_time: iso(6), end_date: iso(10) }, { side_counts: {} }),
      // 延期盘:原定结束已过、仍未结算(Polymarket 改期未更新档期)→ 应显示「延期中 / 原定截止」
      mkEvent({ ...M.dota_main, condition_id: "0xmockdelayed0001", match_start_time: iso(-30), end_date: iso(-6) }, { open_signal_count: 1, signal_count: 1, side_counts: { "Team Spirit": 1 } }),
    ],
    archived_events: [
      { condition_id: M.lol_main.condition_id, match_parts: M.lol_main.match_parts, team_logos: M.lol_main.team_logos, match_start_time: iso(-30), end_date: iso(-26), market_type: "main_match", market_type_label: "主盘", our_realized_pnl: 502.7, wallet_basis_realized_pnl: 480, last_activity_at: ago(90000), status: null },
      { condition_id: "0xmockarchcs2faze0009", match_parts: { game: "Counter-Strike", teamA: "FaZe", teamB: "G2", meta: "(BO3) 八强" }, team_logos: { teamA: "", teamB: "" }, match_start_time: iso(-50), end_date: iso(-47), market_type: "main_match", market_type_label: "主盘", our_realized_pnl: -86.2, wallet_basis_realized_pnl: -80, last_activity_at: ago(170000), status: null },
    ],
    count: 3, archived_count: 2, cache_updated_at: ago(120), cache_stale: false,
  };

  /* ---- follows (list) ---- */
  const mkFollow = (m, over) => Object.assign({
    condition_id: m.condition_id, title: m.title, match_parts: m.match_parts, team_logos: m.team_logos,
    match_start_time: m.match_start_time, end_date: m.end_date,
    market_type: m.market_type, market_type_label: m.market_type_label, category: "esports",
    wallet_count: 1, leg_count: 1, stake: 100, our_realized_pnl: 0,
    status: "open", settlement_type: "", display_pnl: 0, display_pnl_kind: "unrealized",
    quality_label: "one_way", quality_two_sided: false, quality_disagreement: false,
    current_price: m.outcome_prices[0], sides: [],
  }, over);
  const follows = {
    page: 1, size: 25, total: 5, status: "", category: "esports", db_ready: true,
    follows: [
      mkFollow(M.cs2_main, { wallet_count: 5, leg_count: 11, stake: 880, display_pnl: 142.6, display_pnl_kind: "unrealized", status: "open", quality_label: "one_way", sides: [{ outcome: "PARIVISION", outcome_index: 0, leg_count: 11 }] }),
      mkFollow(M.dota_main, { wallet_count: 4, leg_count: 8, stake: 640, display_pnl: -38.2, display_pnl_kind: "unrealized", status: "open", quality_two_sided: true, quality_label: "two_sided", market_type: "map_winner", market_type_label: "地图", sides: [{ outcome: "Team Spirit", outcome_index: 0, leg_count: 5 }, { outcome: "Falcons", outcome_index: 1, leg_count: 3 }] }),
      mkFollow(M.lol_main, { wallet_count: 6, leg_count: 14, stake: 1200, our_realized_pnl: 502.7, display_pnl: 502.7, display_pnl_kind: "realized", status: "settled", sides: [{ outcome: "Gen.G", outcome_index: 1, leg_count: 14 }] }),
      mkFollow({ ...M.cs2_main, condition_id: "0xmockarchcs2faze0009", title: "Counter-Strike: FaZe vs G2 (BO3) - 八强", match_parts: { game: "Counter-Strike", teamA: "FaZe", teamB: "G2", meta: "(BO3) 八强" } }, { wallet_count: 3, leg_count: 6, stake: 480, our_realized_pnl: -86.2, display_pnl: -86.2, display_pnl_kind: "realized", status: "settled", quality_disagreement: true, quality_label: "disagreement", sides: [{ outcome: "FaZe", outcome_index: 0, leg_count: 4 }, { outcome: "G2", outcome_index: 1, leg_count: 2 }] }),
      mkFollow({ ...M.cs2_main, condition_id: "0xmockchaosexit0042", title: "Counter-Strike: CHAOS vs Alpha Dominion Nation (BO3) - United21 Group C", match_parts: { game: "Counter-Strike", teamA: "CHAOS", teamB: "Alpha Dominion Nation", meta: "(BO3) United21 Group C" } }, { wallet_count: 1, leg_count: 4, stake: 200, our_realized_pnl: 0, display_pnl: 0, display_pnl_kind: "realized", status: "settled", settlement_type: "manual_exit", follow_exit_price: 0.62, quality_label: "one_way", sides: [{ outcome: "CHAOS", outcome_index: 0, leg_count: 4 }] }),
    ],
  };

  /* ---- follow detail ---- */
  const mkLeg = (over) => Object.assign({
    wallet_trade_at: ago(3600), wallet_fill_price: 0.55, wallet_trade_cash: 250,
    observed_delay_seconds: 42, funded_stake: 25, stake: 25, our_entry_price: 0.555,
    slippage_over_wallet_entry: 0.005, would_follow: true, funding_status: "funded",
    trade_id: "0xleg" + Math.floor(now % 1e6),
  }, over);
  function followDetail(cid) {
    const m = M.cs2_main;
    return {
      condition_id: cid, category: "esports", title: m.title, question: m.title,
      event_slug: "cs2-prv-mnte-2026-06-12", event_url: "https://polymarket.com/event/cs2-prv-mnte-2026-06-12",
      match_start_time: m.match_start_time, end_date: m.end_date,
      market_type: m.market_type, market_type_label: m.market_type_label,
      match_parts: m.match_parts, team_logos: m.team_logos,
      outcomes: m.outcomes, outcome_prices: m.outcome_prices, signal_count: 2, db_ready: true,
      wallets: [
        {
          wallet: "0x8f3c2a1b9d4e5f60718293a4b5c6d7e8f9012345", short_addr: "0x8f3...345",
          leaderboard_rank: 1, leg_count: 7, follow_total_stake: 175, followed_outcome_count: 1,
          follow_avg_entry_price: 0.5565, follow_realized_pnl: 2.75,
          follow_exit_price: 0.56, follow_exit_stake: 90,
          signals: [{ signal_id: "sig-1", outcome: "PARIVISION", outcome_index: 0, legs: [
            ...Array.from({ length: 7 }, (_, i) => mkLeg({ wallet_trade_at: ago(6000 - i * 600), wallet_fill_price: 0.55 + (i % 3) * 0.003, wallet_trade_cash: 320 - i * 25, our_entry_price: 0.555 + (i % 3) * 0.002, slippage_over_wallet_entry: i % 2 ? -0.002 : 0.005, observed_delay_seconds: 25 + i * 7 })),
            mkLeg({ wallet_trade_at: ago(900), wallet_fill_price: 0.552, our_entry_price: 0.557, slippage_over_wallet_entry: 0.005, observed_delay_seconds: 120, would_follow: false, funding_status: "unfunded", funded_stake: 0 }),
          ] }],
        },
        {
          wallet: "0x2b71e9c4a8d3f50612839a4b5c6d7e8f90126789", short_addr: "0x2b7...789",
          leaderboard_rank: 2, leg_count: 2, follow_total_stake: 50, followed_outcome_count: 2,
          follow_avg_entry_price: 0.561, follow_realized_pnl: 12.4,
          signals: [
            { signal_id: "sig-2", outcome: "PARIVISION", outcome_index: 0, legs: [
              mkLeg({ wallet_trade_at: ago(6000), wallet_fill_price: 0.56, wallet_trade_cash: 420, our_entry_price: 0.562, slippage_over_wallet_entry: 0.002, observed_delay_seconds: 22 }),
            ] },
            { signal_id: "sig-3", outcome: "Monte", outcome_index: 1, legs: [
              mkLeg({ wallet_trade_at: ago(2400), wallet_fill_price: 0.449, wallet_trade_cash: 130, our_entry_price: 0.447, slippage_over_wallet_entry: -0.002, observed_delay_seconds: 75 }),
            ] },
          ],
        },
      ],
    };
  }

  /* ---- wallet follows (modal) ---- */
  function walletFollows(wallet) {
    return {
      wallet, page: 1, size: 20, total: 3,
      signals: [
        { signal_id: "wf-1", event_title: "CS2: PARIVISION vs Monte", match_start_time: iso(-1.5), outcome: "PARIVISION", status: "open", follow_avg_entry_price: 0.553, settlement_price: null },
        { signal_id: "wf-2", event_title: "Dota 2: Team Spirit vs Falcons", match_start_time: iso(-26), outcome: "Team Spirit", status: "settled", follow_avg_entry_price: 0.61, settlement_price: 1.0 },
        { signal_id: "wf-3", event_title: "LoL: T1 vs Gen.G", match_start_time: iso(-50), outcome: "Gen.G", status: "exited", follow_avg_entry_price: 0.49, settlement_price: 0.0 },
      ],
    };
  }

  /* ---- strategy / runner / health ---- */
  let strategy = {
    configured: true, schema_version: 1, updated_at: ago(600),
    stake_sizing: { mode: "fixed", ratio_percent: 10, per_order_cap_enabled: false, per_order_cap_usdc: 0, fixed_usdc: 1, balance_percent: 1 },
    prefilters: { min_target_wallet_order_cash_usdc: 10, max_follow_entry_price: 0.68 },
    condition_limits: { order_count_mode: "condition", max_orders: 10, stake_cap_mode: "fixed", stake_cap_usdc: 200, stake_cap_balance_percent: 5 },
    balance: { required: true, usable_balance_usdc: 5000 },
  };
  /* ---- named strategy library (stateful: create/update/activate/delete) ---- */
  const mkStrat = (over) => Object.assign({
    configured: true, schema_version: 1, updated_at: ago(600),
    stake_sizing: { mode: "fixed", ratio_percent: 10, per_order_cap_enabled: false, per_order_cap_usdc: 0, fixed_usdc: 1, balance_percent: 1 },
    prefilters: { min_target_wallet_order_cash_usdc: 10, max_follow_entry_price: 0.68 },
    condition_limits: { order_count_mode: "condition", max_orders: 10, stake_cap_mode: "fixed", stake_cap_usdc: 200, stake_cap_balance_percent: 5 },
    balance: { required: true, usable_balance_usdc: 5000 },
  }, over || {});
  let _slugSeq = 1;
  const newSlug = () => "s_mock_" + (_slugSeq++);
  let library = [
    { slug: "s_mock_steady", name: "稳健跟单", active: true, updated_at: ago(600), strategy: mkStrat({ realtime_refresh: true }) },
    { slug: "s_mock_aggro", name: "激进满仓", active: false, updated_at: ago(3600), strategy: mkStrat({
      stake_sizing: { mode: "balance_percent", ratio_percent: 10, per_order_cap_enabled: false, per_order_cap_usdc: 0, fixed_usdc: 50, balance_percent: 8 },
      condition_limits: { order_count_mode: "none", max_orders: 0, stake_cap_mode: "balance_percent", stake_cap_usdc: 0, stake_cap_balance_percent: 20 },
      balance: { required: true, usable_balance_usdc: 5000 },
    }) },
  ];
  const listStrategies = () => ({ strategies: library.map((e) => Object.assign({}, e)), active_slug: (library.find((e) => e.active) || {}).slug || null });
  function createStrategy(name, strat) {
    if (library.some((e) => e.name.toLowerCase() === String(name).trim().toLowerCase())) throw { error: "duplicate_name" };
    const active = library.length === 0;
    if (active) library.forEach((e) => (e.active = false));
    const entry = { slug: newSlug(), name: String(name).trim(), active, updated_at: Math.floor(Date.now() / 1000), strategy: Object.assign({ configured: true }, strat) };
    library.push(entry);
    if (active) strategy = entry.strategy;
    return entry;
  }
  function updateStrategy(slug, name, strat) {
    const e = library.find((x) => x.slug === slug);
    if (!e) throw { error: "strategy_not_found" };
    if (library.some((x) => x.slug !== slug && x.name.toLowerCase() === String(name).trim().toLowerCase())) throw { error: "duplicate_name" };
    e.name = String(name).trim(); e.updated_at = Math.floor(Date.now() / 1000);
    e.strategy = Object.assign({ configured: true }, strat);
    if (e.active) strategy = e.strategy;
    return e;
  }
  function activateStrategy(slug) {
    const e = library.find((x) => x.slug === slug);
    if (!e) throw { error: "strategy_not_found" };
    library.forEach((x) => (x.active = x.slug === slug));
    strategy = e.strategy;
    return listStrategies();
  }
  function deleteStrategy(slug) {
    const e = library.find((x) => x.slug === slug);
    if (!e) throw { error: "strategy_not_found" };
    const wasActive = e.active;
    library = library.filter((x) => x.slug !== slug);
    if (wasActive) {
      if (library.length === 1) { library[0].active = true; strategy = library[0].strategy; }
      else { strategy = Object.assign({}, mkStrat(), { configured: false }); }
    }
    return listStrategies();
  }

  const runnerBase = {
    pid: 12345, source: "dashboard", started_at: ago(7445),
    stake_usdc: 1, stake_ratio_percent: 10,
    realtime_refresh: true, observe_running: true, observe_pid: 12346,
    strategy_configured: true, strategy_updated_at: ago(600), strategy_summary: "比例 10%（封顶 $100）· 门槛 $10",
  };
  // stateful mock so the progress masks (sample / stop) animate end-to-end
  let runnerStatus = "running";
  let stopAt = 0;
  let sampleAt = 0;
  function runner() {
    let s = runnerStatus;
    if (s === "stopping" && Date.now() - stopAt > 3000) { s = "stopped"; runnerStatus = "stopped"; }
    return Object.assign({}, runnerBase, { status: s });
  }
  function runnerStart() { runnerStatus = "running"; return { status: "running" }; }
  function runnerStop() { stopAt = Date.now(); runnerStatus = "stopping"; return { status: "stopping" }; }
  function walletRefresh() { sampleAt = Date.now(); return { status: "running" }; }
  function walletRefreshStatusFn() {
    let st = "idle";
    if (sampleAt) st = (Date.now() - sampleAt < 4500) ? "running" : "succeeded";
    return { status: { esports: { status: st, started_at: Math.floor(sampleAt / 1000) }, sports: { status: "idle" } } };
  }
  const health = {
    db_ready: true, status: "healthy", healthy: true, last_tick_at: ago(20), gate_open: true,
    watched_market_count: 18, open_signal_count: 7, leaderboard_updated_at: ago(1800),
    scoring_version: 15, recent_error_count: 0, last_error: null, uptime_seconds: 7445,
    detection_source: "onchain", onchain_configured: true, onchain_healthy: true, follow_wallet_count: 41,
  };

  window.PSMock = {
    overview: () => overview,
    wallets: () => wallets,
    events: () => events,
    follows: () => follows,
    followDetail,
    walletFollows,
    followStrategy: () => strategy,
    strategies: listStrategies,
    createStrategy,
    updateStrategy,
    activateStrategy,
    deleteStrategy,
    runner,
    runnerStart,
    runnerStop,
    walletRefresh,
    health: () => health,
    walletRefreshStatus: walletRefreshStatusFn,
    marketPrices: (cid) => ({ condition_id: cid, outcomes: M.cs2_main.outcomes, outcome_prices: [0.57, 0.43], updated_at: ago(1) }),
  };
})();
