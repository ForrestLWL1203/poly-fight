# Plan: 扩展 Esports 钱包采集到胜负子盘（细化拆解）

## Context
现采集对每个 esports event 只取**一个主盘**（`core.py:156` `choose_main_market`），导致像 `0xe16…e30` 这类专打 `Game/Map Winner` 子盘的强钱包，其子盘历史完全不进评分，钱包被低估或漏采。目标：把采集与评分从「每 event 单主盘」扩展为「每 event 多个胜负盘口」，覆盖单局/地图胜负，同时控制数据量与请求量。v1 只纳入**胜负类**盘口（主盘整场胜负 + 单局/地图胜负），排除击杀/一血/Roshan/让分/大小分等 props。

## 锁定决策
1. **完全砍掉 valorant**：主盘+子盘全部排除，只保留 `cs2 / dota2 / lol`。接受现有 valorant 主盘钱包掉榜——回归测试断言需相应更新。
2. **子盘独立阈值＋独立配额**：discovery 按 `market_type` 分组，主盘与子盘各自一套 `min_volume` 和 `max_markets_per_run`，互不挤占。
3. **子盘单独设较低样本门槛**：`classify_wallet` 主盘维持 `count>=8`，子盘用较低 `min_sample`（默认 5），按 type 配置。

## 允许的 market_type
- `main_match`：`BO1/BO3/BO5` 整场胜负主盘。
- `game_winner`：Dota2/LOL 的 `Game N Winner`。
- `map_winner`：CS2 的 `Map N Winner`。
- `game_family` 仅 `{cs2, dota2, lol}`。

---

## Phase 1 — 市场识别：单主盘 → 多胜负盘口

**核心难点：market_type 判定必须靠语义，不能靠子串黑名单。** `Game 1 Winner`（收）与 `Game 1 - Total Kills` / `Game 1 Handicap`（弃）都含 `"game 1"`。

`poly_fight/core.py`：
- 新增 `classify_market_type(event, market) -> str | None`：
  - 返回 `main_match` / `game_winner` / `map_winner` / `None`（None=排除）。
  - 正向：`question` 含 `game N` + `winner` 语义 → `game_winner`（仅 dota2/lol）；`map N` + `winner` → `map_winner`（仅 cs2）；整场 `vs` + BO 标记 → `main_match`。
  - 负向：含 `kill`/`first blood`/`roshan`/`handicap`/`spread`/`correct score`/`total`/`over`/`under` 一律 None。复用并改造现有 `core.py:135` `is_non_main_market` 的负向词表，但改成「先判 winner 语义，再排 props」。
- 改造 `game_family_from_event`（`core.py:88`）：删除 valorant 分支，valorant → `other`（被 family 白名单挡掉）。
- 改造 `is_main_match_title`（`core.py:113`）：移除 `valorant:` 前缀。
- **新增 `event_to_market_records(event) -> list[dict]`（复数）** 取代单数 `core.py:170` `event_to_market_record` 的内部逻辑：
  - 遍历 event 全部 binary market，对每个调 `classify_market_type`，非 None 的产出一条记录。
  - 每条记录在现有字段基础上新增 `market_type`，并确保带齐：`game_family / event_slug / question / condition_id / match_start_time / end_date / outcomes / outcome_prices / volume / liquidity`（多数已在现有 record 里，补 `market_type`）。
  - family 不在白名单 → 返回 `[]`。
- 保留单数 `event_to_market_record` 作为薄封装（返回第一条 `main_match`），供 `cli.py:1064` `find_active_market`「选单个市场」语义继续使用，**避免破坏 analyze 类单市场命令**。

`build_classification_set`（`core.py:197`）：
- 改为对每个 event 调 `event_to_market_records`（复数）展开。
- 准入闸从 `is_main_match_title`（`core.py:214`，会挡死所有子盘）换成「`market_type` 非空」。
- 仍按 `condition_id` 去重、按 `end_date` 倒序、保留 `is_settled_binary_prices` 与 lookback 过滤。

## Phase 2 — Discovery：按 market_type 分组配额

`build_discovery_slate`（`core.py:224`）：
- 新签名增加按类型配置（建议用 dict）：
  - `main_match`: `min_volume≈25k`, `max_markets_per_run≈50`（沿用现值）。
  - `game_winner`/`map_winner`: 较低 `min_volume`（如 5k–8k）、各自独立 `max_markets_per_run`。
- 选取逻辑按 `market_type` 分组各跑一次 `select(...)`，合并结果；meta 里分类型报告 `total_selected_market_count` 与 `market_count`，便于 dry-run 观察分布。
- CLI 暴露对应参数（`poly_fight/cli.py` 的 build-leaderboard arg 区，参照现有 `--leaderboard-*`）。

## Phase 3 — 评分按 market_type 分桶

**数据通路（原版缺这条）**：positions 不带 market_type，需把 `condition_id -> market_type` 映射一路传下去。

`poly_fight/cli.py`（约 `cli.py:959`）：
- 现有 `condition_ids = {row["condition_id"] for row in discovery_slate}` 旁新增
  `condition_type = {row["condition_id"]: row["market_type"] for row in discovery_slate}`。
- 透传给 `profile_candidate_wallet`（`cli.py:970`）。

`poly_fight/core.py`：
- `summarize_closed_positions`（`core.py:525`）：接收 `condition_type` 映射，按 `market_type` 把 rows 分桶，对**每个桶**分别算 ROI / Wilson / entry edge / sample / capital，返回结构如 `per_type: {main_match: {...}, game_winner: {...}}`，并保留整体汇总向后兼容。
- `classify_wallet`（`core.py:657`）：
  - 改为对每个桶各跑一次评级（抽出内部 `_grade_bucket(summary, *, min_sample)`，主盘 `min_sample=8`、子盘 `min_sample=5`）。
  - 聚合产出 `per_type_grades: {type: grade}` 与 `eligible_market_types = [type for type, g in per_type_grades if g == "A"]`。
  - 顶层 `grade` 取各类型最佳（保持 leaderboard 排序兼容）。
- `profile_candidate_wallet`（`core.py:793`）：把 `condition_type` 透传进 summarize/classify。

`build_leaderboard_from_profiles`（`cli.py:541`）：
- grade 过滤（现 `grade == "A"`，`cli.py:558`）改为 `eligible_market_types` 非空。
- 导出 row 带上 `eligible_market_types` 与 `per_type_grades` → `smart_wallet_leaderboard.json`。

## Phase 4 — Follow 按类型授权

`poly_fight/cli.py` run 流程：
- active 市场构建（`cli.py:1073`）改用 `event_to_market_records`（复数），让 active watch 纳入子盘；`watched_markets`（`cli.py:319`）逻辑不变（仍按 start_time 窗口），但市场记录现带 `market_type`。
- `eligible_follow_wallets`（`follow.py:10`）：从硬编码 `grade == "A"` 改为「`eligible_market_types` 非空」，并把每个钱包的 `eligible_market_types` 带进返回行。

`process_follow_trades`（`follow.py:342`）：
- **新增 gating 分支**（不是纯数据）：拿到 trade 的 `market.market_type`，若不在该 wallet 的 `eligible_market_types` → 跳过开 signal（计入 ignored 原因 `market_type_not_eligible`）。
- 其余开/平仓逻辑复用。

## Phase 5 — Dashboard 文案

`poly_fight/dashboard.py`：
- `build_wallets`（`dashboard.py:592`）：row 增加 `eligible_market_types`（映射为「主盘/单局/地图」标签）。
- `build_follow_detail`（`dashboard.py:651`）：返回 `market_type`（从 market 记录取）。

`poly_fight/dashboard/static/`：
- `index.html` leaderboard 表（`index.html:160` grade badge 处）增加「可跟类型」列；follows 表（`index.html:208`）增加 market type 列，避免把 `Game 1 Winner` 误当 BO3 主盘。
- `app.js` 加 `marketTypeLabel()`（main_match→主盘 / game_winner→单局 / map_winner→地图）。

---

## Test Plan（`tests/test_core.py` 为主）

单元测试：
- `classify_market_type`：Dota2 event 同时产出 BO3 `main_match` + `Game 1/2 Winner`（`game_winner`），排除 `Roshan/Kills/Handicap`。
- CS2 event：BO3 `main_match` + `Map 1/2/3 Winner`（`map_winner`）。
- `game N` + 非 winner（Total Kills/Handicap）判为 None；防子串误收。
- MLBB / Valorant event → `event_to_market_records` 返回 `[]`（family 白名单 + valorant 排除）。
- `summarize_closed_positions` 分桶：构造一个主盘高胜率 + 子盘低胜率的钱包，断言 `per_type` 两桶 ROI/胜率互不污染。
- `classify_wallet`：子盘以 5 个样本达标 A、主盘 5 个样本不达标，验证 per-type `min_sample`。
- `process_follow_trades`：wallet `eligible_market_types=["main_match"]`，对 `game_winner` 市场的 trade 不开 signal、计 ignored。

回归（需更新）：
- **删除/改写**「valorant 主盘钱包仍在榜」类断言（决策 1：valorant 全砍）。
- 现有 cs2/dota2/lol 主盘 A 钱包仍在 leaderboard。
- 现有主盘 follow 行为不变。
- dashboard/API 测试继续通过。

## Verification（dry-run，遵守「不擅自重建真实 data/」）
- **先确认**：重建 classification / leaderboard 会覆盖真实 `data/`，执行前向用户确认。
- 14 天窗口重建 classification，记录：总市场数、按 market_type 分布、API 请求量（对比改造前的乘数倍率）。
- 确认 `0xe16…e30` 的 `game_winner` 历史进入评分，且其 `per_type_grades.game_winner == "A"`、出现在 `eligible_market_types`。
- `run --max-run-ticks 1` smoke：确认 `Game 1 Winner` 类新交易不再计入 `ignored_trade_count`（除非该 wallet 该类型未授权）。
- dashboard 起本地：leaderboard 显示可跟类型，follows 显示 market type。

## Assumptions
- v1 仅胜负类子盘，不含 props（kills/一血/Roshan/让分/大小分）。
- 历史窗口默认 14 天，不扩大，先观察请求量与候选质量。
- 钱包评分按类型隔离，follow 按类型授权。
- 不恢复 VPS paper/dashboard，等本轮验证通过再议。
