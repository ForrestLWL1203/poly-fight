/* Node unit test for the dashboardV2 strategy mappers (adapt.js):
   strategyFromKit / strategyToKit round-trip + backend-validity contract.
   Run directly (`node tests/dashboardv2_strategy_mapping.test.js`) or via the
   Python wrapper test_dashboardv2_strategy_mapping.py. Exits non-zero on fail. */
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

// Mirror of poly_fight/follow_strategy.py validate_follow_strategy predicates,
// so we assert strategyFromKit output is actually acceptable to the backend.
function backendErrors(s) {
  const e = [];
  const fp = (v) => Number.isFinite(+v) && +v > 0;
  const fnn = (v) => Number.isFinite(+v) && +v >= 0;
  const sz = s.stake_sizing, lim = s.condition_limits, pre = s.prefilters, bal = s.balance;
  if (!["kelly", "proportional", "fixed", "balance_percent"].includes(sz.mode)) e.push("stake_sizing.mode");
  if (sz.mode === "kelly") {
    if (!(fp(sz.kelly_fraction) && +sz.kelly_fraction <= 1)) e.push("stake_sizing.kelly_fraction");
    if (!fp(sz.per_signal_cap_percent)) e.push("stake_sizing.per_signal_cap_percent");
    if (!fp(sz.per_match_cap_percent)) e.push("stake_sizing.per_match_cap_percent");
    if (!fp(sz.min_stake_usdc)) e.push("stake_sizing.min_stake_usdc");
  }
  if (sz.mode === "proportional" && !fp(sz.ratio_percent)) e.push("stake_sizing.ratio_percent");
  if (sz.per_order_cap_enabled && !fp(sz.per_order_cap_usdc)) e.push("stake_sizing.per_order_cap_usdc");
  if (sz.mode === "fixed" && !fp(sz.fixed_usdc)) e.push("stake_sizing.fixed_usdc");
  if (sz.mode === "balance_percent" && !fp(sz.balance_percent)) e.push("stake_sizing.balance_percent");
  if (!fnn(pre.min_target_wallet_order_cash_usdc)) e.push("prefilters.min_target_wallet_order_cash_usdc");
  if (!["none", "condition", "wallet"].includes(lim.order_count_mode)) e.push("condition_limits.order_count_mode");
  if (lim.order_count_mode !== "none" && !(+lim.max_orders >= 1)) e.push("condition_limits.max_orders");
  if (!["none", "fixed", "balance_percent"].includes(lim.stake_cap_mode)) e.push("condition_limits.stake_cap_mode");
  if (lim.stake_cap_mode === "fixed" && !fp(lim.stake_cap_usdc)) e.push("condition_limits.stake_cap_usdc");
  if (lim.stake_cap_mode === "balance_percent" && !fp(lim.stake_cap_balance_percent)) e.push("condition_limits.stake_cap_balance_percent");
  const br = bal.required || sz.mode === "balance_percent" || sz.mode === "kelly" || lim.stake_cap_mode === "balance_percent";
  if (br && !fp(bal.usable_balance_usdc)) e.push("balance.usable_balance_usdc");
  return e;
}

// 0) kelly(默认引擎)round-trip + backend-valid
const k0 = { usableMode: "cap", usableCap: "2000", minSignalOn: true, minSignal: "10", sizing: "kelly", kellyFraction: "0.25", perSignalPct: "1", perMatchPct: "10", minStake: "1", ratio: "10", ratioCapOn: false, ratioCap: "100", fixed: "50", balancePct: "1", countOn: false, countMode: "event", count: "10", spendOn: false, spendMode: "fixed", spendFixed: "200", spendPct: "5" };
const s0 = A.strategyFromKit(k0, 2000);
eq("k0.mode", s0.stake_sizing.mode, "kelly");
eq("k0.per_signal", s0.stake_sizing.per_signal_cap_percent, 1);  // 单笔基数(单位)%
eq("k0.per_match", s0.stake_sizing.per_match_cap_percent, 10);
eq("k0.min_stake", s0.stake_sizing.min_stake_usdc, 1);
eq("k0.balance_required", s0.balance.required, true);
eq("k0.backend_valid", backendErrors(s0), []);
const rtk0 = A.strategyToKit(s0, 2000);
["sizing", "perSignalPct", "perMatchPct", "minStake"].forEach((f) => eq("k0.rt." + f, rtk0[f], k0[f]));

// 1) proportional + per-order cap + condition count cap + fixed spend cap
const k1 = { usableMode: "cap", usableCap: "5000", minSignalOn: true, minSignal: "10", sizing: "ratio", ratio: "10", ratioCapOn: true, ratioCap: "100", fixed: "50", balancePct: "1", countOn: true, countMode: "event", count: "10", spendOn: true, spendMode: "fixed", spendFixed: "200", spendPct: "5" };
const s1 = A.strategyFromKit(k1, 8000);
eq("p1.mode", s1.stake_sizing.mode, "proportional");
eq("p1.ratio", s1.stake_sizing.ratio_percent, 10);
eq("p1.cap_enabled", s1.stake_sizing.per_order_cap_enabled, true);
eq("p1.cap", s1.stake_sizing.per_order_cap_usdc, 100);
eq("p1.min", s1.prefilters.min_target_wallet_order_cash_usdc, 10);
eq("p1.order_count_mode", s1.condition_limits.order_count_mode, "condition");
eq("p1.max_orders", s1.condition_limits.max_orders, 10);
eq("p1.stake_cap_mode", s1.condition_limits.stake_cap_mode, "fixed");
eq("p1.stake_cap_usdc", s1.condition_limits.stake_cap_usdc, 200);
eq("p1.usable", s1.balance.usable_balance_usdc, 5000);
eq("p1.backend_valid", backendErrors(s1), []);

// 2) fixed mode, signal threshold off, "all balance"
const k2 = { usableMode: "all", usableCap: "", minSignalOn: false, minSignal: "10", sizing: "fixed", ratio: "10", ratioCapOn: false, ratioCap: "100", fixed: "50", balancePct: "1", countOn: false, countMode: "event", count: "10", spendOn: false, spendMode: "fixed", spendFixed: "200", spendPct: "5" };
const s2 = A.strategyFromKit(k2, 1200);
eq("f2.mode", s2.stake_sizing.mode, "fixed");
eq("f2.fixed", s2.stake_sizing.fixed_usdc, 50);
eq("f2.min_off", s2.prefilters.min_target_wallet_order_cash_usdc, 0);
eq("f2.order_count_none", s2.condition_limits.order_count_mode, "none");
eq("f2.stake_cap_none", s2.condition_limits.stake_cap_mode, "none");
eq("f2.usable_is_wallet", s2.balance.usable_balance_usdc, 1200);
eq("f2.backend_valid", backendErrors(s2), []);

// 3) balance_percent + per-wallet count cap + balance% spend cap
const k3 = { usableMode: "cap", usableCap: "3000", minSignalOn: true, minSignal: "5", sizing: "balancePct", ratio: "10", ratioCapOn: false, ratioCap: "100", fixed: "50", balancePct: "2", countOn: true, countMode: "wallet", count: "3", spendOn: true, spendMode: "balancePct", spendFixed: "200", spendPct: "8" };
const s3 = A.strategyFromKit(k3, 9999);
eq("b3.mode", s3.stake_sizing.mode, "balance_percent");
eq("b3.balance_percent", s3.stake_sizing.balance_percent, 2);
eq("b3.order_count_wallet", s3.condition_limits.order_count_mode, "wallet");
eq("b3.stake_cap_balpct", s3.condition_limits.stake_cap_mode, "balance_percent");
eq("b3.stake_cap_balance_percent", s3.condition_limits.stake_cap_balance_percent, 8);
eq("b3.required", s3.balance.required, true);
eq("b3.backend_valid", backendErrors(s3), []);

// 4) round-trip toKit(fromKit(k1)) preserves user-facing fields
const rt = A.strategyToKit(A.strategyFromKit(k1, 8000), 8000);
["usableMode", "minSignalOn", "sizing", "ratio", "ratioCapOn", "ratioCap", "countOn", "countMode", "count", "spendOn", "spendMode", "spendFixed"].forEach((f) => eq("roundtrip." + f, rt[f], k1[f]));

// 4b) realtime_refresh round-trips through fromKit/toKit (off by default, on when set)
eq("rt.realtime_default", A.strategyFromKit(k1, 8000).realtime_refresh, false);
eq("rt.realtime_on", A.strategyFromKit(Object.assign({}, k1, { realtimeRefresh: true }), 8000).realtime_refresh, true);
eq("rt.realtime_toKit", A.strategyToKit({ realtime_refresh: true }, 8000).realtimeRefresh, true);

// 5) toKit from a configured backend strategy
const api = { configured: true, stake_sizing: { mode: "proportional", ratio_percent: 12, per_order_cap_enabled: true, per_order_cap_usdc: 80, fixed_usdc: 0, balance_percent: 0 }, prefilters: { min_target_wallet_order_cash_usdc: 15 }, condition_limits: { order_count_mode: "condition", max_orders: 6, stake_cap_mode: "fixed", stake_cap_usdc: 150, stake_cap_balance_percent: 0 }, balance: { required: true, usable_balance_usdc: 4000 } };
const k = A.strategyToKit(api, 4000);
eq("tk.sizing", k.sizing, "ratio");
eq("tk.ratio", k.ratio, "12");
eq("tk.ratioCapOn", k.ratioCapOn, true);
eq("tk.ratioCap", k.ratioCap, "80");
eq("tk.minSignal", k.minSignal, "15");
eq("tk.countOn", k.countOn, true);
eq("tk.count", k.count, "6");
eq("tk.spendOn", k.spendOn, true);
eq("tk.spendFixed", k.spendFixed, "150");
eq("tk.usableMode", k.usableMode, "cap");

// ---- strategyExample (示例推演) ----
const base = { usableMode: "all", usableCap: "", minSignalOn: false, minSignal: "100", sizing: "ratio", ratio: "10", ratioCapOn: false, ratioCap: "100", fixed: "50", balancePct: "2", countOn: false, countMode: "event", count: "10", spendOn: false, spendMode: "fixed", spendFixed: "200", spendPct: "5" };
const ex = (over, sample, wallet) => A.strategyExample(Object.assign({}, base, over), sample, wallet);

eq("ex.ratio_amount", ex({ sizing: "ratio", ratio: "10" }, "1200", 8000).amount, 120);
eq("ex.ratio_basis", ex({ sizing: "ratio", ratio: "10" }, "1200", 8000).basis, "10% × $1,200");
eq("ex.ratio_cap_hit", ex({ sizing: "ratio", ratio: "10", ratioCapOn: true, ratioCap: "100" }, "1200", 8000).amount, 100);
eq("ex.ratio_cap_basis", ex({ sizing: "ratio", ratio: "10", ratioCapOn: true, ratioCap: "100" }, "1200", 8000).basis, "命中封顶 $100");
eq("ex.ratio_floor", ex({ sizing: "ratio", ratio: "10" }, "125", 8000).amount, 12); // 12.5 -> 12
eq("ex.fixed", ex({ sizing: "fixed", fixed: "50" }, "1200", 8000).amount, 50);
eq("ex.balancePct_all", ex({ sizing: "balancePct", balancePct: "2", usableMode: "all" }, "1200", 5000).amount, 100); // 2% of 5000
eq("ex.balancePct_cap", ex({ sizing: "balancePct", balancePct: "2", usableMode: "cap", usableCap: "3000" }, "1200", 9999).amount, 60); // 2% of min(3000,9999)
eq("ex.ignored_below_threshold", ex({ minSignalOn: true, minSignal: "100" }, "50", 8000), { ignored: true });
eq("ex.threshold_disabled", ex({ minSignalOn: false, minSignal: "100", sizing: "fixed", fixed: "50" }, "50", 8000).amount, 50); // not ignored when off

// ---- strategyIssues (必填校验 / 启动门槛) ----
const valid = { usableMode: "cap", usableCap: "5000", minSignalOn: true, minSignal: "10", sizing: "ratio", ratio: "10", ratioCapOn: true, ratioCap: "100", fixed: "50", balancePct: "2", countOn: true, countMode: "event", count: "10", spendOn: true, spendMode: "fixed", spendFixed: "200", spendPct: "5" };
const iss = (over, wallet) => A.strategyIssues(Object.assign({}, valid, over), wallet === undefined ? 8000 : wallet);

eq("iss.valid_empty", iss({}), []);
eq("iss.missing_ratio", iss({ ratio: "" }), ["单笔金额"]);
eq("iss.missing_cap", iss({ usableCap: "" }), ["可动用上限"]);
eq("iss.missing_minSignal", iss({ minSignal: "" }), ["最小信号金额"]);
eq("iss.missing_ratioCap", iss({ ratioCap: "" }), ["单笔封顶金额"]);
eq("iss.missing_count", iss({ count: "" }), ["单场笔数"]);
eq("iss.missing_spendFixed", iss({ spendFixed: "" }), ["单场投入上限"]);
eq("iss.balancePct_no_avail", iss({ sizing: "balancePct", balancePct: "2", usableMode: "all", ratioCapOn: false }, 0), ["可用余额"]);

// ---- strategyEntries (named library: backend list → kit shapes) ----
const libApi = {
  active_slug: "s2",
  strategies: [
    { slug: "s1", name: "稳健", active: false, updated_at: 100, strategy: { configured: true, stake_sizing: { mode: "proportional", ratio_percent: 10 }, prefilters: { min_target_wallet_order_cash_usdc: 10 }, condition_limits: {}, balance: {} } },
    { slug: "s2", name: "激进", active: true, updated_at: 200, strategy: { configured: true, stake_sizing: { mode: "fixed", fixed_usdc: 50 }, prefilters: { min_target_wallet_order_cash_usdc: 0 }, condition_limits: {}, balance: {} } },
  ],
};
const lib = A.strategyEntries(libApi, 8000);
eq("lib.activeSlug", lib.activeSlug, "s2");
eq("lib.len", lib.list.length, 2);
eq("lib.0.name", lib.list[0].name, "稳健");
eq("lib.0.active", lib.list[0].active, false);
eq("lib.0.kit.sizing", lib.list[0].kit.sizing, "ratio");
eq("lib.1.active", lib.list[1].active, true);
eq("lib.1.kit.sizing", lib.list[1].kit.sizing, "fixed");
eq("lib.1.updatedAt", lib.list[1].updatedAt, 200);

// empty / missing library
eq("lib.empty.activeSlug", A.strategyEntries({}, 8000).activeSlug, null);
eq("lib.empty.list", A.strategyEntries({}, 8000).list, []);

if (fail) { console.log("\n" + fail + " assertion(s) FAILED"); process.exit(1); }
console.log("ALL PASS (strategy mappers + example + issues + library)");
process.exit(0);
