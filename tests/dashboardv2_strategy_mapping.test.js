/* Node unit test for the dashboardV2 strategy mappers (adapt.js) — 单一模型 v3:
   strategyFromKit / strategyToKit round-trip + backend-validity contract.
   Run directly (`node tests/dashboardv2_strategy_mapping.test.js`) or via the
   Python wrapper. Exits non-zero on fail. */
"use strict";
const fs = require("fs");
const path = require("path");

const adaptPath = path.join(__dirname, "..", "poly_fight", "dashboardV2", "adapt.js");
const window = {};
// eslint-disable-next-line no-eval
eval(fs.readFileSync(adaptPath, "utf8")); // attaches window.PSAdapt
const A = window.PSAdapt;

let fail = 0;
const eq = (name, got, exp) => {
  const ok = JSON.stringify(got) === JSON.stringify(exp);
  if (!ok) { fail++; console.log("FAIL", name, "\n  got", JSON.stringify(got), "\n  exp", JSON.stringify(exp)); }
};

// Mirror of poly_fight/follow_strategy.py validate_follow_strategy (v3 单一模型).
function backendErrors(s) {
  const e = [];
  const fp = (v) => Number.isFinite(+v) && +v > 0;
  const fnn = (v) => Number.isFinite(+v) && +v >= 0;
  const sz = s.sizing || {}, pre = s.prefilters || {};
  if (sz.per_signal_cap_enabled && !fp(sz.per_signal_percent)) e.push("sizing.per_signal_percent");
  if (!fp(sz.per_match_percent)) e.push("sizing.per_match_percent");
  else if (sz.per_signal_cap_enabled && +sz.per_match_percent + 1e-9 < +sz.per_signal_percent) e.push("sizing.per_match_percent");
  if (!fp(sz.per_match_percent_sub)) e.push("sizing.per_match_percent_sub");
  if (!fp(sz.fill_line_x_cap)) e.push("sizing.fill_line_x_cap");
  if (!fp(sz.edge_ref)) e.push("sizing.edge_ref");
  if (!fp(sz.min_stake_usdc)) e.push("sizing.min_stake_usdc");
  if (!fnn(pre.min_target_wallet_order_cash_usdc)) e.push("prefilters.min_target_wallet_order_cash_usdc");
  return e;
}

// 0) round-trip + backend-valid
const k0 = { usableMode: "cap", usableCap: "2000", minSignalOn: true, minSignal: "10", maxEntryOn: true, maxEntry: "0.68", perSignalCapOn: true, perSignalPct: "1", perMatchPct: "2", fillLineXCap: "10", edgeRef: "0.2", minStake: "1", realtimeRefresh: false };
const s0 = A.strategyFromKit(k0, 2000);
eq("s0.schema", s0.schema_version, 3);
eq("s0.per_signal", s0.sizing.per_signal_percent, 1);
eq("s0.per_match", s0.sizing.per_match_percent, 2);
eq("s0.min_stake", s0.sizing.min_stake_usdc, 1);
eq("s0.max_entry", s0.prefilters.max_follow_entry_price, 0.68);
eq("s0.min_target", s0.prefilters.min_target_wallet_order_cash_usdc, 10);
eq("s0.balance_required", s0.balance.required, true);
eq("s0.usable", s0.balance.usable_balance_usdc, 2000);
eq("s0.no_legacy_keys", !!(s0.stake_sizing || s0.condition_limits), false);
eq("s0.backend_valid", backendErrors(s0), []);
const rt0 = A.strategyToKit(s0, 2000);
["usableMode", "minSignalOn", "minSignal", "maxEntryOn", "maxEntry", "perSignalCapOn", "perSignalPct", "perMatchPct", "fillLineXCap", "edgeRef", "minStake"].forEach((f) => eq("s0.rt." + f, rt0[f], k0[f]));

// 主盘/子盘预算解耦 + 每场最大笔数(2026-06-21)
eq("s0.sub_falls_back_to_main", s0.sizing.per_match_percent_sub, 2); // kit 无子盘字段 → 回退主盘(非 0)
eq("s0.max_orders_default", s0.sizing.max_follow_orders_per_match, 0);
const kSub = { usableMode: "cap", usableCap: "2000", minSignalOn: false, minSignal: "10", maxEntryOn: false, maxEntry: "0.68", perSignalPct: "1", perMatchPct: "2", perMatchSubPct: "0.5", maxOrdersPerMatch: "3", minStake: "1" };
const sSub = A.strategyFromKit(kSub, 2000);
eq("sub.per_match_sub", sSub.sizing.per_match_percent_sub, 0.5);
eq("sub.max_orders", sSub.sizing.max_follow_orders_per_match, 3);
const rtSub = A.strategyToKit(sSub, 2000);
eq("sub.rt.perMatchSubPct", rtSub.perMatchSubPct, "0.5");
eq("sub.rt.maxOrdersPerMatch", rtSub.maxOrdersPerMatch, "3");

// 1) "all balance" + signal threshold off + entry ceiling off
const k1 = { usableMode: "all", usableCap: "", minSignalOn: false, minSignal: "10", maxEntryOn: false, maxEntry: "0.68", perSignalPct: "1", perMatchPct: "1", minStake: "1", realtimeRefresh: true };
const s1 = A.strategyFromKit(k1, 1200);
eq("k1.usable_is_wallet", s1.balance.usable_balance_usdc, 1200);
eq("k1.min_off", s1.prefilters.min_target_wallet_order_cash_usdc, 0);
eq("k1.maxentry_off", s1.prefilters.max_follow_entry_price, 0);
eq("k1.realtime", s1.realtime_refresh, true);
eq("k1.backend_valid", backendErrors(s1), []);

// 2) toKit from a configured v2 backend strategy
const api = { configured: true, sizing: { per_signal_percent: 2, per_match_percent: 4, min_stake_usdc: 1 }, prefilters: { min_target_wallet_order_cash_usdc: 15, max_follow_entry_price: 0.6 }, balance: { required: true, usable_balance_usdc: 4000 } };
const k = A.strategyToKit(api, 4000);
eq("tk.perSignal", k.perSignalPct, "2");
eq("tk.perMatch", k.perMatchPct, "4");
eq("tk.minSignal", k.minSignal, "15");
eq("tk.maxEntry", k.maxEntry, "0.6");
eq("tk.usableMode", k.usableMode, "cap");

// 2b) toKit tolerates a legacy v1 strategy (migration: stake_sizing → sizing)
const legacyApi = { configured: true, stake_sizing: { mode: "kelly", per_signal_cap_percent: 3, per_match_cap_percent: 10 }, prefilters: { min_target_wallet_order_cash_usdc: 20 }, balance: { usable_balance_usdc: 5000 } };
const lk = A.strategyToKit(legacyApi, 5000);
eq("legacy.perSignal", lk.perSignalPct, "3");
eq("legacy.perMatch", lk.perMatchPct, "10");

// ---- strategyExample (示例推演:cap × conviction²，skill 示例固定为 1) ----
const base = { usableMode: "all", usableCap: "", minSignalOn: false, minSignal: "100", maxEntryOn: true, maxEntry: "0.68", perSignalCapOn: false, perSignalPct: "10", perMatchPct: "2", fillLineXCap: "10", edgeRef: "0.2", minStake: "1" };
const ex = (over, sample, wallet) => A.strategyExample(Object.assign({}, base, over), sample, wallet);
eq("ex.full_conviction", ex({}, "1200", 5000).amount, 100);
eq("ex.full_conviction_caps", ex({}, "99999", 5000).amount, 100);
eq("ex.optional_signal_cap", ex({ perSignalCapOn: true, perSignalPct: "1" }, "1200", 5000).amount, 50);
eq("ex.min_floor", ex({ minStake: "5" }, "50", 5000).amount, 5);
eq("ex.ignored_below_threshold", ex({ minSignalOn: true, minSignal: "100" }, "50", 5000), { ignored: true });
eq("ex.threshold_disabled", ex({ minSignalOn: false }, "50", 5000).amount, 1);

// ---- strategyIssues (必填校验 / 启动门槛) ----
const valid = { usableMode: "cap", usableCap: "5000", minSignalOn: true, minSignal: "10", maxEntryOn: true, maxEntry: "0.68", perSignalCapOn: false, perSignalPct: "10", perMatchPct: "10", fillLineXCap: "10", edgeRef: "0.2", minStake: "1" };
const iss = (over, wallet) => A.strategyIssues(Object.assign({}, valid, over), wallet === undefined ? 8000 : wallet);
eq("iss.valid_empty", iss({}), []);
eq("iss.missing_perSignal", iss({ perSignalCapOn: true, perSignalPct: "" }), ["单笔硬上限%"]);
eq("iss.missing_perMatch", iss({ perMatchPct: "" }), ["单场预算%"]);
eq("iss.budget_lt_signal", iss({ perSignalCapOn: true, perSignalPct: "5", perMatchPct: "2" }), ["单场预算不能小于单笔硬上限"]);
eq("iss.missing_fill_line", iss({ fillLineXCap: "" }), ["满 conviction 倍数"]);
eq("iss.missing_edge_ref", iss({ edgeRef: "" }), ["skill 参考 edge"]);
eq("iss.missing_minStake", iss({ minStake: "" }), ["单笔下限"]);
eq("iss.missing_cap", iss({ usableCap: "" }), ["可动用上限"]);
eq("iss.missing_minSignal", iss({ minSignal: "" }), ["最小信号金额"]);
eq("iss.no_avail", iss({ usableMode: "all" }, 0), ["可用余额"]);

// ---- strategyEntries (named library: backend list → kit shapes) ----
const libApi = {
  active_slug: "s2",
  strategies: [
    { slug: "s1", name: "稳健", active: false, updated_at: 100, strategy: { configured: true, sizing: { per_signal_percent: 1, per_match_percent: 1, min_stake_usdc: 1 }, prefilters: { min_target_wallet_order_cash_usdc: 10 }, balance: {} } },
    { slug: "s2", name: "激进", active: true, updated_at: 200, strategy: { configured: true, sizing: { per_signal_percent: 2, per_match_percent: 4, min_stake_usdc: 1 }, prefilters: {}, balance: {} } },
  ],
};
const lib = A.strategyEntries(libApi, 8000);
eq("lib.activeSlug", lib.activeSlug, "s2");
eq("lib.len", lib.list.length, 2);
eq("lib.0.name", lib.list[0].name, "稳健");
eq("lib.0.active", lib.list[0].active, false);
eq("lib.0.kit.perSignal", lib.list[0].kit.perSignalPct, "1");
eq("lib.1.active", lib.list[1].active, true);
eq("lib.1.kit.perSignal", lib.list[1].kit.perSignalPct, "2");
eq("lib.1.updatedAt", lib.list[1].updatedAt, 200);
eq("lib.empty.activeSlug", A.strategyEntries({}, 8000).activeSlug, null);
eq("lib.empty.list", A.strategyEntries({}, 8000).list, []);

if (fail) { console.log("\n" + fail + " assertion(s) FAILED"); process.exit(1); }
console.log("ALL PASS (strategy mappers v3 + example + issues + library)");
process.exit(0);
