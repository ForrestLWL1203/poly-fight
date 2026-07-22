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
    open_signal_count: 7, result_count: 64, settled_count: 54, exited_count: 10,
    win_rate: 0.71875, our_realized_pnl: 2481.5, hypothetical_pnl: 2600.0, wallet_basis_realized_pnl: 2500.0,
    total_stake: 21900.0, resolved_stake: 18720.0, realized_roi: 0.1326, wallet_basis_realized_roi: 0.1335,
    open_exposure: 3180.0, account_total_equity_usdc: 8642.18,
    account_balance: { configured: true, balance_usdc: 5462.18, source: "manual", updated_at: ago(3600) },
    clean_signal_count: 14, two_sided_signal_count: 2, disagreement_signal_count: 2,
    ai_risk: {
      enabled: true, credential_configured: true, credential_status: "valid",
      blocked_count: 9, agree_count: 24, insufficient_count: 4, unavailable_count: 1,
      resolved_blocked_count: 7, avoided_loss_usdc: 1084, missed_profit_usdc: 214,
      net_effect_usdc: 870, proprietary_pnl_usdc: 284.61, proprietary_open_count: 2,
    },
    win_rates_by_game: [
      { game: "dota2", game_label: "Dota 2", wins: 18, losses: 7, settled_count: 25, win_rate: 0.72 },
      { game: "cs2", game_label: "CS2", wins: 11, losses: 2, settled_count: 13, win_rate: 0.85 },
      { game: "lol", game_label: "LoL", wins: 9, losses: 5, settled_count: 14, win_rate: 0.64 },
      { game: "valorant", game_label: "Valorant", wins: 8, losses: 4, settled_count: 12, win_rate: 0.67 },
    ],
    follow_type_distribution: {
      total: 70, total_stake: 21900,
      by_game: [
        { game: "dota2", game_label: "Dota 2", total: 21, total_stake: 7050, types: [
          { type: "main_match", label: "主盘", count: 14, stake: 5200 }, { type: "sub_game", label: "Sub Game", count: 7, stake: 1850 }] },
        { game: "cs2", game_label: "CS2", total: 21, total_stake: 7000, types: [
          { type: "main_match", label: "主盘", count: 12, stake: 4400 }, { type: "sub_game", label: "Sub Game", count: 9, stake: 2600 }] },
        { game: "lol", game_label: "LoL", total: 20, total_stake: 6050, types: [
          { type: "main_match", label: "主盘", count: 11, stake: 3650 }, { type: "sub_game", label: "Sub Game", count: 9, stake: 2400 }] },
        { game: "valorant", game_label: "Valorant", total: 8, total_stake: 1800, types: [
          { type: "main_match", label: "主盘", count: 8, stake: 1800 }, { type: "sub_game", label: "Sub Game", count: 0, stake: 0 }] },
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
    price_bands: [
      { band: "0.40-0.55", win_rate: 0.55, n: 9, hold_pnl: -30, followable: "full" },
      { band: "0.55-0.70", win_rate: 0.74, n: 22, hold_pnl: 410, followable: "partial" },
      { band: ">=0.70", win_rate: 0.88, n: 14, hold_pnl: -60, followable: "none" },
    ],
  }, over);
  const wallets = {
    wallets: [
      mkWallet({ wallet: "0x8f3c2a1b9d4e5f60718293a4b5c6d7e8f9012345", rank: 1, grade: "A", best_bucket_score: 96, esports_roi: 0.284, overall_esports_roi: 0.142, avg_market_cash: 12480, recent_bucket_roi: 0.221, eligible_buckets: ["dota2:main_match", "cs2:main_match"], favorite: true, observed: { signals: 7, wins: 6, losses: 1, exits: 0, open: 2, our_pnl: 412.8, wallet_pnl: 430, win_rate: 0.857, has_loss: true } }),
      mkWallet({ wallet: "0x2b71e9c4a8d3f50612839a4b5c6d7e8f90126789", rank: 2, grade: "A", best_bucket_score: 93, esports_roi: 0.247, overall_esports_roi: 0.118, avg_market_cash: 8240, recent_bucket_roi: 0.186, eligible_buckets: ["cs2:map_winner"], observed_at: ago(3600), observed: { signals: 12, wins: 9, losses: 3, exits: 0, open: 1, our_pnl: 286.4, wallet_pnl: 300, win_rate: 0.75, has_loss: true } }),
      mkWallet({ wallet: "0x5d92f0a6b1c8e7430219384a5b6c7d8e9f013579", rank: 3, grade: "B", best_bucket_score: 81, esports_roi: 0.212, overall_esports_roi: 0.094, avg_market_cash: 19600, recent_bucket_roi: -0.042, eligible_buckets: ["lol:game_winner"], price_bands: [
        { band: "<0.40", win_rate: 0.33, n: 6, hold_pnl: -283, followable: "full" },
        { band: "0.40-0.55", win_rate: 0.46, n: 13, hold_pnl: -49, followable: "full" },
        { band: "0.55-0.70", win_rate: 0.78, n: 45, hold_pnl: 2078, followable: "partial" },
        { band: ">=0.70", win_rate: 0.85, n: 33, hold_pnl: -100, followable: "none" },
      ], observed: { signals: 5, wins: 4, losses: 1, exits: 0, open: 1, our_pnl: 198.2, wallet_pnl: 210, win_rate: 0.8, has_loss: true } }),
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
    page: 1, size: 25, total: 7, status: "", category: "esports", db_ready: true,
    follows: [
      mkFollow(M.cs2_main, { wallet_count: 5, leg_count: 11, stake: 880, display_pnl: 142.6, display_pnl_kind: "unrealized", status: "open", quality_label: "one_way", sides: [{ outcome: "PARIVISION", outcome_index: 0, leg_count: 11 }], ai_action: "agree", ai_risk: { status: "ok", verdict: "team_a", team_a: "PARIVISION", team_b: "Monte", team_a_win_probability: 69, team_b_win_probability: 31, confidence: 82, reason_zh: "近期状态与系列赛稳定性更强" } }),
      mkFollow(M.dota_main, { wallet_count: 4, leg_count: 8, stake: 640, display_pnl: -38.2, display_pnl_kind: "unrealized", status: "open", quality_two_sided: true, quality_label: "two_sided", market_type: "map_winner", market_type_label: "地图", sides: [{ outcome: "Team Spirit", outcome_index: 0, leg_count: 5 }, { outcome: "Falcons", outcome_index: 1, leg_count: 3 }] }),
      mkFollow(M.lol_main, { wallet_count: 6, leg_count: 14, stake: 1200, our_realized_pnl: 502.7, display_pnl: 502.7, display_pnl_kind: "realized", status: "settled", settled_by_price: true, sides: [{ outcome: "Gen.G", outcome_index: 1, leg_count: 14 }], ai_action: "insufficient", ai_risk: { status: "insufficient", verdict: "insufficient", team_a: "T1", team_b: "Gen.G", team_a_win_probability: 50, team_b_win_probability: 50, confidence: 30, reason_zh: "赛前阵容与近期状态证据不足" } }),
      mkFollow({ ...M.cs2_main, condition_id: "0xmockarchcs2faze0009", title: "Counter-Strike: FaZe vs G2 (BO3) - 八强", match_parts: { game: "Counter-Strike", teamA: "FaZe", teamB: "G2", meta: "(BO3) 八强" } }, { wallet_count: 3, leg_count: 6, stake: 480, our_realized_pnl: -86.2, display_pnl: -86.2, display_pnl_kind: "realized", status: "settled", quality_disagreement: true, quality_label: "disagreement", sides: [{ outcome: "FaZe", outcome_index: 0, leg_count: 4 }, { outcome: "G2", outcome_index: 1, leg_count: 2 }] }),
      mkFollow({ ...M.cs2_main, condition_id: "0xmockchaosexit0042", title: "Counter-Strike: CHAOS vs Alpha Dominion Nation (BO3) - United21 Group C", match_parts: { game: "Counter-Strike", teamA: "CHAOS", teamB: "Alpha Dominion Nation", meta: "(BO3) United21 Group C" } }, { wallet_count: 1, leg_count: 4, stake: 200, our_realized_pnl: 0, display_pnl: 0, display_pnl_kind: "realized", status: "settled", settlement_type: "manual_exit", follow_exit_price: 0.62, quality_label: "one_way", sides: [{ outcome: "CHAOS", outcome_index: 0, leg_count: 4 }] }),
      mkFollow({ ...M.dota_main, condition_id: "0xmockstoploss0077", title: "Dota 2: Tundra vs BetBoom (BO3) - 主盘止损样例", match_parts: { game: "Dota 2", teamA: "Tundra", teamB: "BetBoom", meta: "(BO3) 主盘" } }, { wallet_count: 1, leg_count: 3, stake: 150, our_realized_pnl: -82.5, display_pnl: -82.5, display_pnl_kind: "realized", status: "settled", settlement_type: "stop_loss", follow_exit_price: 0.27, quality_label: "one_way", sides: [{ outcome: "Tundra", outcome_index: 0, leg_count: 3 }] }),
      mkFollow({ ...M.lol_main, condition_id: "0xmockaiblockedt1drx", title: "League of Legends: T1 vs Kiwoom DRX (BO1) - KeSPA Cup", match_parts: { game: "League of Legends", teamA: "T1", teamB: "Kiwoom DRX", meta: "(BO1) KeSPA Cup" } }, { wallet_count: 3, leg_count: 0, stake: 0, display_pnl: 0, status: "ai_blocked", sides: [{ outcome: "Kiwoom DRX", outcome_index: 1, leg_count: 0 }], ai_action: "blocked", ai_intent_count: 3, ai_blocked_intended_stake: 682, ai_net_effect: 682, ai_risk: { status: "ok", verdict: "team_a", team_a: "T1", team_b: "Kiwoom DRX", team_a_win_probability: 82, team_b_win_probability: 18, confidence: 91, reason_zh: "T1长期实力、交手与大赛经验明显占优" } }),
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
    if (cid === "0xmockaiblockedt1drx") return {
      condition_id: cid, category: "esports", title: "League of Legends: T1 vs Kiwoom DRX (BO1) - KeSPA Cup", question: "T1 vs Kiwoom DRX",
      match_start_time: iso(-4), end_date: iso(2), market_type: "main_match", market_type_label: "主盘",
      match_parts: { game: "League of Legends", teamA: "T1", teamB: "Kiwoom DRX", meta: "(BO1) KeSPA Cup" }, team_logos: {},
      outcomes: ["T1", "Kiwoom DRX"], outcome_prices: [1, .001], signal_count: 0, db_ready: true, wallets: [],
      ai_risk: {
        assessment: { status: "ok", verdict: "team_a", team_a: "T1", team_b: "Kiwoom DRX", team_a_win_probability: 82, team_b_win_probability: 18, confidence: 91, knowledge_recency: "recent", reason_zh: "优势：T1历史实力与交手；风险：BO1波动", model: "gemini-3.6-flash", prompt_version: "esports-main-rag-v2" },
        action_counts: { blocked: 3 }, intent_count: 3, blocked_intent_count: 3, blocked_intended_stake: 682, net_effect: 682,
        blocked_wallets: [
          { wallet: "0xcb7286ed5e91a6db709876543210abcdef126532", outcome: "Kiwoom DRX", outcome_index: 1, intended_stake: 126, entry_price: .585, shadow_status: "settled", baseline_pnl: -126, ai_net_effect: 126 },
          { wallet: "0xe405b789a4f01234567890abcdef12345678c67e", outcome: "Kiwoom DRX", outcome_index: 1, intended_stake: 243, entry_price: .605, shadow_status: "settled", baseline_pnl: -243, ai_net_effect: 243 },
          { wallet: "0xe548c67890abcdef1234567890abcdef1234a9fe", outcome: "Kiwoom DRX", outcome_index: 1, intended_stake: 313, entry_price: .605, shadow_status: "settled", baseline_pnl: -313, ai_net_effect: 313 },
        ], counterfactual_label: "被拦截意图级反事实；不包含释放资金后续用途",
      },
    };
    const m = M.cs2_main;
    return {
      condition_id: cid, category: "esports", title: m.title, question: m.title,
      event_slug: "cs2-prv-mnte-2026-06-12", event_url: "https://polymarket.com/event/cs2-prv-mnte-2026-06-12",
      match_start_time: m.match_start_time, end_date: m.end_date,
      market_type: m.market_type, market_type_label: m.market_type_label,
      match_parts: m.match_parts, team_logos: m.team_logos,
      outcomes: m.outcomes, outcome_prices: m.outcome_prices, signal_count: 2, db_ready: true,
      ai_risk: {
        assessment: { status: "ok", verdict: "team_a", team_a: "PARIVISION", team_b: "Monte", team_a_win_probability: 69, team_b_win_probability: 31, confidence: 82, knowledge_recency: "recent", reason_zh: "优势：近期状态更稳；风险：阵容信息有限", model: "gemini-3.6-flash", prompt_version: "esports-main-rag-v2" },
        action_counts: { agree: 2 }, intent_count: 2, blocked_intent_count: 0, blocked_intended_stake: 0, blocked_wallets: [], net_effect: 0,
        counterfactual_label: "被拦截意图级反事实；不包含释放资金后续用途",
      },
      wallets: [
        {
          wallet: "0x8f3c2a1b9d4e5f60718293a4b5c6d7e8f9012345", short_addr: "0x8f3...345",
          leaderboard_rank: 1, leg_count: 7, follow_total_stake: 175, followed_outcome_count: 1,
          follow_avg_entry_price: 0.5565, follow_realized_pnl: 2.75,
          follow_exit_price: 0.56, follow_exit_stake: 90,
          signals: [{ signal_id: "sig-1", outcome: "PARIVISION", outcome_index: 0,
            legs: [
              ...Array.from({ length: 7 }, (_, i) => mkLeg({ wallet_trade_at: ago(6000 - i * 600), wallet_fill_price: 0.55 + (i % 3) * 0.003, wallet_trade_cash: 320 - i * 25, our_entry_price: 0.555 + (i % 3) * 0.002, slippage_over_wallet_entry: i % 2 ? -0.002 : 0.005, observed_delay_seconds: 25 + i * 7 })),
              mkLeg({ wallet_trade_at: ago(900), wallet_fill_price: 0.552, our_entry_price: 0.557, slippage_over_wallet_entry: 0.005, observed_delay_seconds: 120, would_follow: false, funding_status: "unfunded", funded_stake: 0 }),
            ],
            partial_exits: [{ timestamp: ago(540), price: 0.554, sold_stake: 90, sold_fraction_delta: 0.5, cumulative_sold_fraction: 0.5 }],
            behavior_events: [{ kind: "exit", price: 0.553, size: 5000, timestamp: ago(600) }],
          }],
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
    configured: true, schema_version: 2, updated_at: ago(600),
    sizing: { per_signal_percent: 1, per_match_percent: 1, min_stake_usdc: 1 },
    prefilters: { min_target_wallet_order_cash_usdc: 10, max_follow_entry_price: 0.85 },
    balance: { required: true, usable_balance_usdc: 5000 },
  };
  /* ---- named strategy library (stateful: create/update/activate/delete) ---- */
  const mkStrat = (over) => Object.assign({
    configured: true, schema_version: 2, updated_at: ago(600),
    sizing: { per_signal_percent: 1, per_match_percent: 1, min_stake_usdc: 1 },
    prefilters: { min_target_wallet_order_cash_usdc: 10, max_follow_entry_price: 0.85 },
    balance: { required: true, usable_balance_usdc: 5000 },
  }, over || {});
  let _slugSeq = 1;
  const newSlug = () => "s_mock_" + (_slugSeq++);
  let library = [
    { slug: "s_mock_steady", name: "稳健跟单", active: true, updated_at: ago(600), strategy: mkStrat({ realtime_refresh: true }) },
    { slug: "s_mock_aggro", name: "激进满仓", active: false, updated_at: ago(3600), strategy: mkStrat({
      sizing: { per_signal_percent: 2, per_match_percent: 4, min_stake_usdc: 1 },
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
    realtime_refresh: true, observe_live_running: true, observe_live_pid: 12346,
    strategy_configured: true, strategy_updated_at: ago(600), strategy_summary: "单笔 余额1%（每场累计预算 余额1%）· 现价上限 0.85 · 目标单≥$10",
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

  function aiRisk() {
    return {
      settings: { enabled: true, model: "gemini-3.6-flash", win_probability_threshold: 65, confidence_threshold: 75, updated_at: ago(300) },
      credential: { configured: true, status: "valid", updated_at: ago(600), last_validated_at: ago(120) },
      data_credential: { configured: true, status: "valid", updated_at: ago(600), last_validated_at: ago(120) },
      summary: { assessment_count: 18, intent_count: 31, blocked_count: 5, agree_count: 20, insufficient_count: 4, unavailable_count: 2, resolved_blocked_count: 3, avoided_loss_usdc: 682, missed_profit_usdc: 126, net_effect_usdc: 556, ai_inverse_resolved_count: 3, ai_inverse_win_count: 2, ai_inverse_pnl_usdc: 412.35, ai_vs_wallet_usdc: 1094.35, proprietary_screened_count: 12, proprietary_open_count: 2, proprietary_settled_count: 7, proprietary_win_count: 5, proprietary_pnl_usdc: 284.61, proprietary_roi: .081, proprietary_bankroll_usdc: 5284.61, proprietary_brier_score: .164 },
      source_health: [
        { provider: "pandascore", status: "ok", coverage: 18, last_success_at: ago(80) },
        { provider: "opendota", status: "ok", coverage: 14, last_success_at: ago(220) },
        { provider: "leaguepedia", status: "limited", coverage: 9, error: "leaguepedia_ratelimited", last_success_at: ago(1800) },
        { provider: "liquipedia", status: "ok", coverage: 12, last_success_at: ago(110) },
      ],
      proprietary_records: [
        { condition_id: "self-1", game_family: "cs2", outcomes: ["Astralis", "HEROIC"], outcome_index: 0, status: "open", decision: "entered", evidence_score: 91, ai_probability: 72, confidence: 84, stake_usdc: 52, entry_price: .61 },
        { condition_id: "self-2", game_family: "lol", outcomes: ["Top Esports", "Team WE"], outcome_index: 0, status: "settled", decision: "settled", evidence_score: 87, ai_probability: 68, confidence: 79, stake_usdc: 50, entry_price: .58, realized_pnl: 36.21, prediction_correct: true },
        { condition_id: "self-3", game_family: "dota2", outcomes: ["Falcons", "Team Spirit"], outcome_index: -1, status: "watching", decision: "volume_insufficient", evidence_score: 0, required_volume_usdc: 1000, volume_usdc: 640 },
      ],
      recent_assessments: [
        { condition_id: "0xmockaiblockedt1drx", game: "lol", team_a: "T1", team_b: "Kiwoom DRX", best_of: "BO1", verdict: "team_a", team_a_win_probability: 82, team_b_win_probability: 18, confidence: 91, reason_zh: "T1长期实力、交手与大赛经验明显占优" },
        { condition_id: M.cs2_main.condition_id, game: "cs2", team_a: "PARIVISION", team_b: "Monte", best_of: "BO3", verdict: "team_a", team_a_win_probability: 69, team_b_win_probability: 31, confidence: 82, reason_zh: "近期状态与系列赛稳定性更强" },
        { condition_id: M.dota_main.condition_id, game: "dota2", team_a: "Team Spirit", team_b: "Falcons", best_of: "BO3", verdict: "insufficient", team_a_win_probability: 52, team_b_win_probability: 48, confidence: 58, reason_zh: "近期阵容信息不足" },
      ],
      recent_intents: [
        { condition_id: "0xmockaiblockedt1drx", action: "blocked" },
        { condition_id: M.cs2_main.condition_id, action: "agree" },
        { condition_id: M.dota_main.condition_id, action: "insufficient" },
      ],
    };
  }

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
    aiRisk,
    health: () => health,
    walletRefreshStatus: walletRefreshStatusFn,
    marketPrices: (cid) => ({ condition_id: cid, outcomes: M.cs2_main.outcomes, outcome_prices: [0.57, 0.43], updated_at: ago(1) }),
  };
})();
