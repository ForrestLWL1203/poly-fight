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

  /* parse an ISO/epoch into "MM-DD HH:mm" or "今天/明天 HH:mm" */
  function fmtClock(value, nowMs) {
    const d = toDate(value);
    if (!d) return "";
    const pad = (n) => String(n).padStart(2, "0");
    const hm = pad(d.getHours()) + ":" + pad(d.getMinutes());
    const now = new Date(nowMs || Date.now());
    const dayKey = (x) => x.getFullYear() * 372 + x.getMonth() * 31 + x.getDate();
    const delta = dayKey(d) - dayKey(now);
    if (delta === 0) return "今天 " + hm;
    if (delta === 1) return "明天 " + hm;
    if (delta === 2) return "后天 " + hm;
    return pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + " " + hm;
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
    const obs = row.observed || {};
    const settled = num(obs.wins) + num(obs.losses);
    return {
      rank: row.quarantined ? null : (row.rank != null ? num(row.rank) : null),
      addr: row.wallet,
      grade: row.grade || "",
      category: row.category || "esports",
      game: normalizeGame(row.primary_game || row.best_game_family),
      // M4 动态观测发现并入榜 < 2h → 显示 "NEW";超过 2h 不再显示
      isNew: row.observed_at ? (nowMs - num(row.observed_at) * 1000 < 2 * 3600 * 1000) : false,
      score: Math.round(num(row.best_bucket_score)),
      roi: Math.round(pct(row.esports_roi) * 10) / 10,
      overallRoi: row.overall_esports_roi != null ? Math.round(pct(row.overall_esports_roi) * 10) / 10 : null,
      // 显示"我们会跟的专精桶"胜率 θ̂:1 个桶=该桶胜率,多个桶=各桶平均(与胜率门同口径)。
      // 不显示整体 positive_market_rate(口径不对)。老 row 无桶明细时回退 best_bucket_win_rate。
      winRate: (() => {
        const wrs = (row.eligible_bucket_details || []).map((d) => d.win_rate).filter((x) => x != null);
        if (wrs.length) return Math.round(pct(wrs.reduce((a, b) => a + b, 0) / wrs.length));
        if (row.best_bucket_win_rate != null) return Math.round(pct(row.best_bucket_win_rate));
        return null;
      })(),
      closedCount: num(row.esports_closed_count),
      // 场均交易额在顶层或嵌套 candidate 里(与后端 _v2_candidate_metric 同口径);老/瘦身 row 只在 candidate。
      avgCash: num(row.avg_market_cash != null ? row.avg_market_cash : (row.candidate && row.candidate.avg_market_cash)),
      recent: row.recent_bucket_roi != null ? Math.round(pct(row.recent_bucket_roi) * 10) / 10 : null,
      scope: scopeList(row),
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
    const start = toDate(row.match_start_time);
    const live = start ? start.getTime() <= (nowMs || Date.now()) : false;
    const status = live ? "live" : "upcoming";
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
      countdown: countdown(row.match_start_time, status, nowMs),
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
    return {
      cid: row.condition_id,
      game: info.game, teamA: info.teamA, teamB: info.teamB, meta: info.meta,
      teamLogos: teamLogoMap(row, info),
      marketType: row.market_type_label || row.market_type || "",
      // 我们买入哪一边(可能两边:对手盘 / 自对冲)。
      sides: (row.sides || []).map((s) => ({ outcome: String(s.outcome || ""), index: num(s.outcome_index), legs: num(s.leg_count) })),
      status: open ? "open" : "settled",
      settlement,
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

  /* ---------- Strategy (API <-> kit flat state) ---------- */
  function strategyToKit(api, walletBalance) {
    const s = api || {};
    const sizing = s.stake_sizing || {};
    const pre = s.prefilters || {};
    const lim = s.condition_limits || {};
    const bal = s.balance || {};
    const mode = sizing.mode === "kelly" ? "kelly" : sizing.mode === "fixed" ? "fixed" : sizing.mode === "balance_percent" ? "balancePct" : "ratio";
    const usable = num(bal.usable_balance_usdc);
    const minSignal = num(pre.min_target_wallet_order_cash_usdc);
    const str = (v, d) => (v === 0 || v ? String(v) : d);
    // 现价上限:字段缺失(老策略)→ 默认开 0.85(全系统唯一分水岭);显式 0 → 关。
    const maxEntryRaw = pre.max_follow_entry_price;
    const maxEntryVal = num(maxEntryRaw);
    return {
      usableMode: usable > 0 ? "cap" : "all",
      usableCap: str(usable || "", String(num(walletBalance) || 5000)),
      minSignalOn: minSignal > 0,
      minSignal: str(minSignal || 10, "10"),
      maxEntryOn: maxEntryRaw === undefined || maxEntryRaw === null ? true : maxEntryVal > 0 && maxEntryVal < 1,
      maxEntry: str(maxEntryVal > 0 ? maxEntryVal : 0.85, "0.85"),
      sizing: mode,
      // Kelly 智能引擎参数
      kellyFraction: str(num(sizing.kelly_fraction) || 0.25, "0.25"),
      perSignalPct: str(num(sizing.per_signal_cap_percent) || 5, "5"),
      perMatchPct: str(num(sizing.per_match_cap_percent) || 10, "10"),
      minStake: str(num(sizing.min_stake_usdc) || 1, "1"),
      ratio: str(num(sizing.ratio_percent) || 10, "10"),
      ratioCapOn: !!sizing.per_order_cap_enabled,
      ratioCap: str(num(sizing.per_order_cap_usdc) || 100, "100"),
      fixed: str(num(sizing.fixed_usdc) || 50, "50"),
      balancePct: str(num(sizing.balance_percent) || 1, "1"),
      countOn: (lim.order_count_mode || "none") !== "none",
      countMode: lim.order_count_mode === "wallet" ? "wallet" : "event",
      count: str(num(lim.max_orders) || 10, "10"),
      spendOn: (lim.stake_cap_mode || "none") !== "none",
      spendMode: lim.stake_cap_mode === "balance_percent" ? "balancePct" : "fixed",
      spendFixed: str(num(lim.stake_cap_usdc) || 200, "200"),
      spendPct: str(num(lim.stake_cap_balance_percent) || 5, "5"),
      realtimeRefresh: !!s.realtime_refresh,
    };
  }

  function strategyFromKit(k, walletBalance) {
    const mode = k.sizing === "kelly" ? "kelly" : k.sizing === "fixed" ? "fixed" : k.sizing === "balancePct" ? "balance_percent" : "proportional";
    const usable = k.usableMode === "cap" ? num(k.usableCap) : num(walletBalance);
    const balanceRequired = k.sizing === "kelly" || k.sizing === "balancePct" || (k.spendOn && k.spendMode === "balancePct") || k.usableMode === "cap";
    return {
      configured: true,
      schema_version: 1,
      stake_sizing: {
        mode,
        kelly_fraction: num(k.kellyFraction),
        per_signal_cap_percent: num(k.perSignalPct),
        per_match_cap_percent: num(k.perMatchPct),
        min_stake_usdc: num(k.minStake),
        ratio_percent: num(k.ratio),
        per_order_cap_enabled: k.sizing === "ratio" && !!k.ratioCapOn,
        per_order_cap_usdc: num(k.ratioCap),
        fixed_usdc: num(k.fixed),
        balance_percent: num(k.balancePct),
      },
      prefilters: {
        min_target_wallet_order_cash_usdc: k.minSignalOn ? num(k.minSignal) : 0,
        max_follow_entry_price: k.maxEntryOn ? num(k.maxEntry) : 0,
      },
      condition_limits: {
        order_count_mode: k.countOn ? (k.countMode === "wallet" ? "wallet" : "condition") : "none",
        max_orders: num(k.count),
        stake_cap_mode: k.spendOn ? (k.spendMode === "balancePct" ? "balance_percent" : "fixed") : "none",
        stake_cap_usdc: num(k.spendFixed),
        stake_cap_balance_percent: num(k.spendPct),
      },
      balance: { required: balanceRequired, usable_balance_usdc: usable },
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
    const avail = strategyAvail(s, walletBalance);
    let raw, basis;
    if (s.sizing === "kelly") {
      // Kelly 按实时 edge 定额,与目标下单额无关 → 用代表性场景演示:胜率72% @ 现价62¢(edge 10¢)。
      const wlb = 0.72, p = 0.62, kf = num(s.kellyFraction) || 0.25;
      raw = kf * ((wlb - p) / (1 - p)) * avail;
      const signalCap = avail * num(s.perSignalPct) / 100;
      if (signalCap > 0) raw = Math.min(raw, signalCap);
      const minStake = num(s.minStake) || 1;
      if (raw > 0 && raw < minStake) raw = minStake;
      basis = `Kelly×${kf}:示例 胜率72%@现价0.62(edge 10¢),单笔≤${s.perSignalPct || 0}%`;
    } else if (s.sizing === "ratio") {
      raw = t * num(s.ratio) / 100; basis = `${s.ratio || 0}% × ${_usdInt(t)}`;
      if (s.ratioCapOn && raw > num(s.ratioCap)) { raw = num(s.ratioCap); basis = `命中封顶 ${_usdInt(num(s.ratioCap))}`; }
    } else if (s.sizing === "fixed") { raw = num(s.fixed); basis = "固定金额"; }
    else { raw = avail * num(s.balancePct) / 100; basis = `${s.balancePct || 0}% × 可用 ${_usdInt(avail)}`; }
    return { amount: Math.floor(Math.max(0, raw)), basis };
  }

  /* required-field gaps that must be cleared before the runner may start.
     Empty array => ready. Order/labels match the StrategyPage 待完善 list. */
  function strategyIssues(s, walletBalance) {
    const avail = strategyAvail(s, walletBalance);
    const issues = [];
    if (s.sizing === "kelly") {
      if (!(num(s.kellyFraction) > 0)) issues.push("Kelly 激进度");
      if (!(num(s.perSignalPct) > 0)) issues.push("单笔上限%");
      if (!(num(s.perMatchPct) > 0)) issues.push("单场上限%");
      if (!(num(s.minStake) > 0)) issues.push("单笔下限");
      if (!(avail > 0)) issues.push("可用余额");
    } else {
      const sizingPrimary = s.sizing === "ratio" ? s.ratio : s.sizing === "fixed" ? s.fixed : s.balancePct;
      if (!(num(sizingPrimary) > 0)) issues.push("单笔金额");
      if (s.sizing === "ratio" && s.ratioCapOn && !(num(s.ratioCap) > 0)) issues.push("单笔封顶金额");
      if (s.sizing === "balancePct" && !(avail > 0)) issues.push("可用余额");
    }
    if (s.usableMode === "cap" && !(num(s.usableCap) > 0)) issues.push("可动用上限");
    if (s.minSignalOn && !(num(s.minSignal) > 0)) issues.push("最小信号金额");
    if (s.countOn && !(num(s.count) > 0)) issues.push("单场笔数");
    if (s.spendOn && !(num(s.spendMode === "fixed" ? s.spendFixed : s.spendPct) > 0)) issues.push("单场投入上限");
    return issues;
  }

  window.PSAdapt = {
    GAME_LABELS, GAME_ORDER, GAME_COLORS, MARKET_TYPE_LABELS, QUARANTINE_REASONS,
    num, pct, gameLabel, normalizeGame, timeAgo, fmtClock, toDate, countdown, matchInfo, scopeList,
    overview, equityPoints, equitySeries, rollingPnl, winRates, followTypes, wallet, wallets, event, archivedEvent, events,
    follow, follows, followQuality, strategyToKit, strategyFromKit, strategyEntry, strategyEntries, strategyAvail, strategyExample, strategyIssues,
  };
})();
