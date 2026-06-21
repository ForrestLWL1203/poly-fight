/* Poly Sniper dashboard — API → UI-kit shape adapters + shared helpers.
   Plain JS, attaches to window.PSAdapt. The Python backend returns rich
   objects; the React kit expects flat shapes (see the original data.js).   */
(function () {
  "use strict";

  /* ---------- shared maps / helpers ---------- */
  const GAME_LABELS = { dota2: "Dota 2", cs2: "CS2", lol: "LoL", valorant: "Valorant", multi: "跨游戏" };
  const GAME_ORDER = { dota2: 0, lol: 1, cs2: 2, valorant: 3, multi: 9 };
  const MARKET_TYPE_LABELS = { main_match: "主盘", game_winner: "单局", map_winner: "地图" };
  const QUARANTINE_REASONS = {
    manual_dashboard_quarantine: "手动隔离",
    manual_quarantine: "手动隔离",
    large_sell: "尾盘大额卖出",
    opposite_wallet_buy: "同盘双边下注",
    two_sided: "同盘双边下注",
    revalidation_required: "需重新评估",
    rescore_below_grade_a: "重评跌出A级",
  };
  // [main_match, sub_game] accent colors per game, for the distribution donut.
  const GAME_COLORS = {
    dota2: ["#f0512f", "#ff9a72"],
    cs2: ["#d98a1e", "#f0c074"],
    lol: ["#1f7a73", "#6cc0b8"],
    valorant: ["#7d5cff", "#b9a6ff"],
    multi: ["#6b7280", "#a8b1c0"],
  };

  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  const pct = (frac) => Math.round(num(frac) * 1000) / 10; // fraction -> percent, 1dp
  const gameLabel = (g) => GAME_LABELS[g] || (g ? String(g).toUpperCase() : "");

  function normalizeGame(value) {
    const compact = String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "");
    if (["cs2", "counterstrike2", "counterstrike"].includes(compact)) return "cs2";
    if (["dota2", "dota"].includes(compact)) return "dota2";
    if (["lol", "leagueoflegends", "league"].includes(compact)) return "lol";
    if (["valorant", "valo"].includes(compact)) return "valorant";
    if (compact === "multi") return "multi";  // 跨游戏盘口专家(per-type 合格)
    return "";
  }

  /* relative time, Chinese, from a unix-seconds timestamp */
  function timeAgo(tsSeconds, nowMs) {
    if (!tsSeconds) return "";
    const now = nowMs || Date.now();
    const diff = Math.floor(now / 1000 - num(tsSeconds));
    if (diff < 0) return "刚刚";
    if (diff < 60) return diff + "秒前";
    if (diff < 3600) return Math.floor(diff / 60) + "分钟前";
    if (diff < 86400) return Math.floor(diff / 3600) + "小时前";
    if (diff < 86400 * 30) return Math.floor(diff / 86400) + "天前";
    return Math.floor(diff / (86400 * 30)) + "个月前";
  }

  /* parse an ISO/epoch into "MM-DD HH:mm" or "今天/明天 HH:mm".
     钉死 UTC+8(北京):赛事时间统一按北京墙钟显示,不随查看设备的时区漂移。
     做法 = 把绝对时刻 +8h 后用 getUTC* 读取(中国无夏令时,固定偏移即正确)。 */
  function fmtClock(value, nowMs) {
    const d = toDate(value);
    if (!d) return "";
    const pad = (n) => String(n).padStart(2, "0");
    const OFFSET = 8 * 3600 * 1000;             // UTC+8 固定偏移
    const ds = new Date(d.getTime() + OFFSET);
    const ns = new Date((nowMs || Date.now()) + OFFSET);
    const hm = pad(ds.getUTCHours()) + ":" + pad(ds.getUTCMinutes());
    const dayKey = (x) => x.getUTCFullYear() * 372 + x.getUTCMonth() * 31 + x.getUTCDate();
    const delta = dayKey(ds) - dayKey(ns);
    if (delta === 0) return "今天 " + hm;
    if (delta === 1) return "明天 " + hm;
    if (delta === 2) return "后天 " + hm;
    return pad(ds.getUTCMonth() + 1) + "-" + pad(ds.getUTCDate()) + " " + hm;
  }

  function toDate(value) {
    if (value == null || value === "") return null;
    if (typeof value === "number") return new Date(value < 1e12 ? value * 1000 : value);
    const n = Number(value);
    if (Number.isFinite(n) && String(value).trim() !== "" && /^\d+$/.test(String(value).trim()))
      return new Date(n < 1e12 ? n * 1000 : n);
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d;
  }

  function countdown(value, status, nowMs) {
    if (status === "live") return "进行中";
    const d = toDate(value);
    if (!d) return "";
    let diff = Math.floor((d.getTime() - (nowMs || Date.now())) / 1000);
    if (diff <= 0) return "进行中";
    if (diff < 3600) return Math.floor(diff / 60) + "分钟后";
    if (diff < 86400) return Math.floor(diff / 3600) + "小时后";
    return Math.floor(diff / 86400) + "天后";
  }

  /* derive teamA/teamB/meta/game from an API row carrying match_parts/title */
  function matchInfo(row) {
    const mp = row.match_parts && typeof row.match_parts === "object" ? row.match_parts : {};
    const game = normalizeGame(mp.game || row.game || row.game_family || row.league);
    const teamA = mp.teamA || mp.team_a || "";
    const teamB = mp.teamB || mp.team_b || "";
    const meta = [mp.meta, mp.stage, mp.format].filter(Boolean).join(" · ") ||
      row.market_type_label || row.market_type || "";
    return { game, teamA, teamB, meta };
  }

  /* ---------- Overview ---------- */
  function overview(api, health) {
    const o = api || {};
    const wr = o.win_rates_by_game || [];
    const wins = wr.reduce((a, g) => a + num(g.wins), 0);
    const losses = wr.reduce((a, g) => a + num(g.losses), 0);
    const bal = o.account_balance || {};
    const watched = health && health.watched_market_count != null
      ? num(health.watched_market_count) : null;
    return {
      realizedPnl: num(o.our_realized_pnl),
      realizedRoi: pct(o.realized_roi),
      totalStake: num(o.total_stake),
      settledCount: num(o.settled_count),
      openExposure: num(o.open_exposure),
      walletBalance: bal.configured ? num(bal.balance_usdc) : 0,
      walletConfigured: !!bal.configured,
      watchedEvents: watched != null ? watched : num(o.open_signal_count),
      openFollows: num(o.open_signal_count),
      openByGame: (o.open_by_game || [])
        .map((g) => ({ game: normalizeGame(g.game), name: g.game_label || gameLabel(g.game), count: num(g.count) }))
        .filter((g) => g.game),
      cleanCount: num(o.clean_signal_count),
      twoSidedCount: num(o.two_sided_signal_count),
      disagreementCount: num(o.disagreement_signal_count),
      winRate: { wins, losses },
    };
  }

  function equityPoints(api) {
    const pts = (api && api.equity_points) || [];
    const vals = pts.map((p) => num(p.cumulative_pnl));
    // EquityArea needs >= 2 points; pad with a leading 0 baseline.
    if (vals.length === 0) return [0, 0];
    if (vals.length === 1) return [0, vals[0]];
    return vals;
  }

  /* rolling realized P&L over 24h / 7d / 30d, summed from equity_points
     (each point = a settled/exited result with {timestamp, pnl}). */
  function rollingPnl(api, nowMs) {
    const now = nowMs || Date.now();
    const pts = (api && api.equity_points) || [];
    const sum = (winMs) => pts.reduce((a, p) => {
      const d = toDate(p.timestamp);
      if (!d) return a;
      return (now - d.getTime()) <= winMs ? a + num(p.pnl) : a;
    }, 0);
    const DAY = 86400000;
    return { h24: sum(DAY), d7: sum(7 * DAY), d30: sum(30 * DAY) };
  }

  /* equity line for a window: chronological, rebased from 0, accumulating each
     in-window settlement's pnl. Returns [0, p1, p1+p2, …] so the last value
     equals the window's realized P&L (keeps the chart's endpoint == the big
     number, and the up/down trend consistent). winMs null => all points. */
  function equitySeries(api, winMs, nowMs) {
    const pts = (api && api.equity_points) || [];
    const now = nowMs || Date.now();
    const within = (winMs ? pts.filter((p) => { const d = toDate(p.timestamp); return d && (now - d.getTime()) <= winMs; }) : pts.slice());
    within.sort((a, b) => { const da = toDate(a.timestamp), db = toDate(b.timestamp); return (da ? da.getTime() : 0) - (db ? db.getTime() : 0); });
    const out = [0];
    let acc = 0;
    within.forEach((p) => { acc += num(p.pnl); out.push(acc); });
    return out;
  }

  function winRates(api) {
    return ((api && api.win_rates_by_game) || [])
      .map((g) => ({ game: normalizeGame(g.game), name: g.game_label || gameLabel(g.game), wins: num(g.wins), losses: num(g.losses) }))
      .filter((g) => g.game);
  }

  /* flatten follow_type_distribution.by_game into kit CategoryDonut segments */
  function followTypes(api) {
    const dist = (api && api.follow_type_distribution) || {};
    const segments = [];
    (dist.by_game || []).forEach((gr) => {
      const game = normalizeGame(gr.game);
      const colors = GAME_COLORS[game] || ["#8a7f6c", "#bcae97"];
      (gr.types || []).forEach((t) => {
        const idx = t.type === "main_match" ? 0 : 1;
        if (!num(t.count)) return;
        segments.push({
          group: gr.game_label || gameLabel(game),
          gameId: game,
          label: t.label || (t.type === "main_match" ? "主盘" : "Sub Game"),
          value: num(t.count),
          stake: num(t.stake),
          color: colors[idx],
        });
      });
    });
    return {
      total: num(dist.total),
      totalStake: num(dist.total_stake),
      segments,
    };
  }

  /* ---------- Leaderboard ---------- */
  function scopeList(row) {
    const buckets = row.eligible_buckets && row.eligible_buckets.length
      ? row.eligible_buckets : (row.observed_buckets || []);
    const seen = new Set();
    const out = [];
    buckets.forEach((b) => {
      const [g, mt] = String(b).split(":");
      const game = normalizeGame(g);
      if (!game) return;
      const market = MARKET_TYPE_LABELS[mt] || mt || "主盘";
      const key = game + ":" + market;
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ game, market });
    });
    return out;
  }

  function wallet(row, nowMs) {
    nowMs = nowMs || Date.now();   // 调用方未传 → 用当前时间(否则 isNew 里 nowMs=undefined→NaN→恒 false)
    const obs = row.observed || {};
    const settled = num(obs.wins) + num(obs.losses);
    return {
      rank: row.quarantined ? null : (row.rank != null ? num(row.rank) : null),
      addr: row.wallet,
      grade: row.grade || "",
      category: row.category || "esports",
      game: normalizeGame(row.primary_game || row.best_game_family),
      // M4 动态观测发现并入榜 < 2h → 显示 "NEW";超过 2h 不再显示
      // observe-v2 是 2h 一轮,用 4h 窗口(覆盖两轮)让"刚入榜"的 NEW 标稳定可见、不会一过 2h 就闪没。
      isNew: row.observed_at ? (nowMs - num(row.observed_at) * 1000 < 4 * 3600 * 1000) : false,
      score: Math.round(num(row.best_bucket_score)),
      roi: Math.round(pct(row.esports_roi) * 10) / 10,
      overallRoi: row.overall_esports_roi != null ? Math.round(pct(row.overall_esports_roi) * 10) / 10 : null,
      // 后端已算好"会跟桶 θ̂"(1 桶=该桶,多桶=平均);回退整体 positive_market_rate 仅防旧后端。
      winRate: row.followed_win_rate != null ? Math.round(pct(row.followed_win_rate))
        : (row.positive_market_rate != null ? Math.round(pct(row.positive_market_rate)) : null),
      closedCount: num(row.esports_closed_count),
      avgCash: num(row.avg_market_cash),  // 后端已从 candidate 解析好顶层 avg_market_cash
      recent: row.recent_bucket_roi != null ? Math.round(pct(row.recent_bucket_roi) * 10) / 10 : null,
      // 后端已展开好 scope(跨游戏桶 → 真实游戏 + 盘口);旧后端无此字段时回退 scopeList。
      scope: (row.scope && row.scope.length)
        ? row.scope.map((s) => ({ game: normalizeGame(s.game) || s.game, market: MARKET_TYPE_LABELS[s.market_type] || s.market_type || "主盘" }))
        : scopeList(row),
      settled,
      open: num(obs.open),
      followRec: settled > 0 ? `${num(obs.wins)}-${num(obs.losses)}` : "—",
      followPnl: num(obs.our_pnl),
      lastTrade: timeAgo(row.last_esports_trade_at, nowMs),
      fav: !!row.favorite,
      quarantined: !!row.quarantined,
      reason: QUARANTINE_REASONS[row.quarantine_reason] || row.quarantine_reason || "已隔离",
      reasonTime: timeAgo(row.quarantined_at, nowMs),
    };
  }

  function wallets(api, nowMs) {
    const d = api || {};
    const rows = (d.wallets || []).map((r) => wallet(r, nowMs));
    return {
      rows,
      activeCount: d.active_count != null ? num(d.active_count) : rows.filter((w) => !w.quarantined).length,
      favoriteCount: d.favorite_count != null ? num(d.favorite_count) : rows.filter((w) => w.fav && !w.quarantined).length,
      quarantinedCount: d.quarantined_count != null ? num(d.quarantined_count) : rows.filter((w) => w.quarantined).length,
      updatedAt: d.leaderboard_updated_at || null,
      scoringVersion: d.scoring_version || null,
    };
  }

  /* ---------- Events ---------- */
  function teamLogoMap(row, info) {
    const tl = row.team_logos || {};
    const map = {};
    if (info.teamA && tl.teamA) map[info.teamA] = tl.teamA;
    if (info.teamB && tl.teamB) map[info.teamB] = tl.teamB;
    return map;
  }

  function event(row, nowMs) {
    const info = matchInfo(row);
    const now = nowMs || Date.now();
    const start = toDate(row.match_start_time);
    const end = toDate(row.end_date);
    const started = start ? start.getTime() <= now : false;
    // 原定结束时间已过、却仍在活跃(未结算)列表 → 延期/超时,而非真"进行中"。
    // Polymarket 改期常不更新 gameStartTime/endDate,旧档期会把延期盘错判成已开赛。
    const delayed = end ? end.getTime() <= now : false;
    const status = delayed ? "delayed" : started ? "live" : "upcoming";
    const outs = row.outcomes || [];
    const sc = row.side_counts || {};
    return {
      cid: row.condition_id,
      game: info.game, teamA: info.teamA, teamB: info.teamB, meta: info.meta,
      teamLogos: teamLogoMap(row, info),
      marketType: row.market_type_label || row.market_type || "",
      start: fmtClock(row.match_start_time, nowMs),
      end: fmtClock(row.end_date, nowMs),
      status,
      delayed,
      countdown: status === "delayed" ? "延期中" : countdown(row.match_start_time, status, nowMs),
      followA: num(sc[outs[0]]),
      followB: num(sc[outs[1]]),
      openSignals: num(row.open_signal_count),
      contested: !!row.contested,
      eventUrl: row.event_url || "",
    };
  }

  function archivedEvent(row, nowMs) {
    const info = matchInfo(row);
    return {
      cid: row.condition_id,
      game: info.game, teamA: info.teamA, teamB: info.teamB, meta: info.meta,
      teamLogos: teamLogoMap(row, info),
      marketType: row.market_type_label || row.market_type || "",
      start: fmtClock(row.match_start_time, nowMs),
      end: fmtClock(row.end_date, nowMs),
      status: "settled",
      pnl: num(row.our_realized_pnl),
      eventUrl: row.event_url || "",
    };
  }

  function events(api, nowMs) {
    const d = api || {};
    return {
      events: (d.events || []).map((e) => event(e, nowMs)),
      archive: (d.archived_events || []).map((e) => archivedEvent(e, nowMs)),
    };
  }

  /* ---------- Follows ---------- */
  function followQuality(row) {
    if (row.quality_two_sided) return "two-sided";
    if (row.quality_disagreement) return "contested";
    return "clean";
  }
  function follow(row, nowMs) {
    const info = matchInfo(row);
    const pnl = num(row.display_pnl);
    const open = row.status === "open";
    const settlement = open ? "未结算" : (pnl > 0 ? "盈利" : pnl < 0 ? "亏损" : "未结算");
    // BO 系列的单局盘共用系列标题,靠 market_question 里的 "Game N" 区分(否则两局看起来一样)。
    const mtLabel = row.market_type_label || row.market_type || "";
    const gm = /game\s*(\d+)/i.exec(String(row.question || row.market_question || ""));
    const marketType = gm ? (mtLabel ? `${mtLabel} · 第${gm[1]}局` : `第${gm[1]}局`) : mtLabel;
    return {
      cid: row.condition_id,
      game: info.game, teamA: info.teamA, teamB: info.teamB, meta: info.meta,
      teamLogos: teamLogoMap(row, info),
      marketType,
      // 我们买入哪一边(可能两边:对手盘 / 自对冲)。
      sides: (row.sides || []).map((s) => ({ outcome: String(s.outcome || ""), index: num(s.outcome_index), legs: num(s.leg_count) })),
      status: open ? "open" : "settled",
      // 已平仓的细分:manual_exit=目标清仓/对账兜底,我们提前镜像平仓;
      // auto_settlement=等到市场结算;auto_and_manual=多信号混合。用于区分"提前卖出 vs 自动结算"。
      settlementType: open ? "" : String(row.settlement_type || ""),
      settlement,
      exitPrice: row.follow_exit_price != null ? num(row.follow_exit_price) : null,
      wallets: num(row.wallet_count),
      legs: num(row.leg_count),
      stake: num(row.stake),
      pnl,
      pnlKind: row.display_pnl_kind === "unrealized" ? "unrealized" : "realized",
      quality: followQuality(row),
      sourceOffLeaderboard: !!row.source_off_leaderboard,
      start: fmtClock(row.match_start_time, nowMs),
      end: fmtClock(row.end_date, nowMs),
    };
  }
  function follows(api, nowMs) {
    const d = api || {};
    return {
      rows: (d.follows || []).map((f) => follow(f, nowMs)),
      total: num(d.total),
    };
  }

  /* ---------- Strategy (API <-> kit flat state) — 单一模型 v2 ---------- */
  function strategyToKit(api, walletBalance) {
    const s = api || {};
    // 兼容:优先读 v2 "sizing",回退旧 "stake_sizing"。
    const sizing = s.sizing || s.stake_sizing || {};
    const pre = s.prefilters || {};
    const bal = s.balance || {};
    const usable = num(bal.usable_balance_usdc);
    const minSignal = num(pre.min_target_wallet_order_cash_usdc);
    const str = (v, d) => (v === 0 || v ? String(v) : d);
    const perSignal = sizing.per_signal_percent != null ? sizing.per_signal_percent : sizing.per_signal_cap_percent;
    const perMatch = sizing.per_match_percent != null ? sizing.per_match_percent : sizing.per_match_cap_percent;
    // 子盘(map/game winner)每场预算:缺失 → 回退主盘值(行为不变)。
    const perMatchSub = sizing.per_match_percent_sub != null ? sizing.per_match_percent_sub : perMatch;
    // 现价上限:字段缺失(老策略)→ 默认开 0.68(评分价区);显式 0 → 关。
    const maxEntryRaw = pre.max_follow_entry_price;
    const maxEntryVal = num(maxEntryRaw);
    return {
      usableMode: usable > 0 ? "cap" : "all",
      usableCap: str(usable || "", String(num(walletBalance) || 5000)),
      minSignalOn: minSignal > 0,
      minSignal: str(minSignal || 10, "10"),
      maxEntryOn: maxEntryRaw === undefined || maxEntryRaw === null ? true : maxEntryVal > 0 && maxEntryVal < 1,
      maxEntry: str(maxEntryVal > 0 ? maxEntryVal : 0.68, "0.68"),
      perSignalPct: str(num(perSignal) || 1, "1"),
      perMatchPct: str(num(perMatch) || 1, "1"),
      perMatchSubPct: str(num(perMatchSub) || num(perMatch) || 1, "1"),
      maxOrdersPerMatch: str(num(sizing.max_follow_orders_per_match) || 0, "0"),
      minStake: str(num(sizing.min_stake_usdc) || 1, "1"),
      realtimeRefresh: !!s.realtime_refresh,
    };
  }

  function strategyFromKit(k, walletBalance) {
    const usable = k.usableMode === "cap" ? num(k.usableCap) : num(walletBalance);
    return {
      configured: true,
      schema_version: 2,
      sizing: {
        per_signal_percent: num(k.perSignalPct),
        per_match_percent: num(k.perMatchPct),
        per_match_percent_sub: num(k.perMatchSubPct) || num(k.perMatchPct),  // 空 → 回退主盘(防存成0=非法)
        max_follow_orders_per_match: num(k.maxOrdersPerMatch) || 0,
        min_stake_usdc: num(k.minStake),
      },
      prefilters: {
        min_target_wallet_order_cash_usdc: k.minSignalOn ? num(k.minSignal) : 0,
        max_follow_entry_price: k.maxEntryOn ? num(k.maxEntry) : 0,
      },
      balance: { required: true, usable_balance_usdc: usable },
      realtime_refresh: !!k.realtimeRefresh,
    };
  }

  /* one saved-strategy library entry {slug,name,active,updated_at,strategy} → kit shape. */
  function strategyEntry(row, walletBalance) {
    const r = row || {};
    return {
      slug: String(r.slug || ""),
      name: String(r.name || ""),
      active: !!r.active,
      updatedAt: num(r.updated_at),
      kit: strategyToKit(r.strategy || {}, walletBalance),
    };
  }
  function strategyEntries(api, walletBalance) {
    const rows = (api && api.strategies) || [];
    return {
      activeSlug: api && api.active_slug ? String(api.active_slug) : null,
      list: rows.map((r) => strategyEntry(r, walletBalance)),
    };
  }

  /* usable balance the strategy may deploy (kit semantics): full wallet, or a cap. */
  function strategyAvail(s, walletBalance) {
    const w = num(walletBalance);
    return s.usableMode === "cap" ? Math.min(num(s.usableCap), w || num(s.usableCap)) : w;
  }

  const _usdInt = (v) => "$" + Math.floor(Math.max(0, num(v))).toLocaleString();

  /* "示例推演": for a target wallet buy-in `sample`, what would we actually buy.
     Returns {ignored:true} when below the (enabled) signal threshold, else
     {amount, basis}. amount floors to integer (matches the backend evaluator). */
  function strategyExample(s, sample, walletBalance) {
    const t = num(sample);
    const threshold = s.minSignalOn ? num(s.minSignal) : 0;
    if (threshold > 0 && t < threshold) return { ignored: true };
    // 单一模型:单笔 = 可用余额 × per_signal%(flat,与目标下单额无关),夹到 [min_stake, 每场预算]。
    const avail = strategyAvail(s, walletBalance);
    const budget = avail * num(s.perMatchPct) / 100;
    let raw = avail * num(s.perSignalPct) / 100;
    if (budget > 0) raw = Math.min(raw, budget);
    const minStake = num(s.minStake) || 1;
    if (raw > 0 && raw < minStake) raw = minStake;
    const basis = `余额${s.perSignalPct || 0}% × 可用 ${_usdInt(avail)}(每钱包每场≤余额${s.perMatchPct || 0}%）`;
    return { amount: Math.floor(Math.max(0, raw)), basis };
  }

  /* required-field gaps that must be cleared before the runner may start.
     Empty array => ready. Order/labels match the StrategyPage 待完善 list. */
  function strategyIssues(s, walletBalance) {
    const issues = [];
    if (!(num(s.perSignalPct) > 0)) issues.push("单笔基数%");
    if (!(num(s.perMatchPct) > 0)) issues.push("单场预算%");
    else if (num(s.perMatchPct) + 1e-9 < num(s.perSignalPct)) issues.push("单场预算不能小于单笔基数");
    if (!(num(s.minStake) > 0)) issues.push("单笔下限");
    if (s.usableMode === "cap" && !(num(s.usableCap) > 0)) issues.push("可动用上限");
    else if (!(strategyAvail(s, walletBalance) > 0)) issues.push("可用余额");
    if (s.minSignalOn && !(num(s.minSignal) > 0)) issues.push("最小信号金额");
    return issues;
  }

  window.PSAdapt = {
    GAME_LABELS, GAME_ORDER, GAME_COLORS, MARKET_TYPE_LABELS, QUARANTINE_REASONS,
    num, pct, gameLabel, normalizeGame, timeAgo, fmtClock, toDate, countdown, matchInfo, scopeList,
    overview, equityPoints, equitySeries, rollingPnl, winRates, followTypes, wallet, wallets, event, archivedEvent, events,
    follow, follows, followQuality, strategyToKit, strategyFromKit, strategyEntry, strategyEntries, strategyAvail, strategyExample, strategyIssues,
  };
})();
