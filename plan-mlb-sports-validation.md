# Plan: sports(MLB) 泛化 —— 验证 + 双类目生产化集成

> **分阶段，按风险递增**：Phase 1（MLB 验证）✅；Phase 1.5（评分 v13 + data 分区）✅；
> Phase 2 三步：**2A 双 tab 双采样 + 统一控制面（低风险，先做）→ 2B follow 融合后端（高风险，含 live DB 迁移）→ 2C dashboard 融合 UI**。
> 架构定调：**采集分类别独立（`data/esports`、`data/sports`），follow 统一（`data/follow`），dashboard 以 `data` 为 root 聚合两类**。sports 为正式 Sports 类目，不标实验。

## 分类层级（贯穿全文）
```
category(类目):   esports              sports
                 /  |   \             /  |  \
game/league:  cs2 lol dota2        mlb nba nfl   ← v1 只 mlb
```
- **category 决定市场分类器**（esports: main/game/map winner 排 roshan/kills；sports: moneyline 排 spread/total/inning）。
- **league 决定拉哪些 tag_slug**（esports: cs2/lol/dota2；sports v1: mlb）。
- MLB moneyline = esports `main_match`：event 标题 `Team A vs. Team B`，`outcomes=[队A,队B]`、结算 `["0","1"]`；让分/大小分/props/期货排除。

---

# ✅ Phase 1 — MLB 验证（已完成）
`event_category`/`event_league`/`classify_market_type` 按 category 分派；`--category {esports,sports}` 选 tag；MLB 跑通独立目录。实测 MLB 与 esports 分布相近、有正 capital_weighted_edge 强钱包 → 绿灯。

# ✅ Phase 1.5 — 评分优化 + data 分区（已完成）
- **capital_weighted_edge 为技能轴**：删 `sold_before_resolution`/`low_historical_roi` 硬闸；`capEdge<=0` 才排除；roi/swing 转软 reason；新增 `pre_match_entry_rate`（+ 可选 `--min-pre-match-entry-rate`）。`SCORING_VERSION=13`。
- **data 分区**：`resolve_data_dir` → esports=`data/esports`、sports=`data/sports`；旧 `data/` 顶层已迁入 `data/esports/`。

---

# Phase 2 — 双类目集成

## 路径模型（先钉死，全 Phase 2 共用）
| 用途 | 解析方式 | 默认 |
|---|---|---|
| collect/build 的 **category 数据目录** | `resolve_data_dir(args)`（按 `--data-dir`/`--category`） | esports=`data/esports`、sports=`data/sports` |
| **dashboard root**（聚合两类） | 固定 root，**不复用 resolve_data_dir** | `data`（`serve` 无 `--data-dir` 时 root=`data`，**不是** `data/esports`）|
| **follow 状态目录** | 新接口 `--follow-dir` | `data/follow` |

- `category_data_dirs(root)` **用固定白名单**，不 glob（避免纳入备份/测试/临时目录）：
  ```
  esports -> root/esports
  sports  -> root/sports
  ```
  某目录不存在 → 该类目返回空数据，不报错。
- `--data-dir` 仅对 collect/build 表示 category 数据目录；**runner 不再把 `--data-dir` 当 follow 状态目录**。dashboard 读写 follow 状态只用 `follow_dir`。

## 前置（Phase 2.0）：两份 v13 榜先重建 + review
1. 启 dashboard（root=`data`）→ esports tab 点「立即采样」重建 esports v13（榜 6→~11，确认 0xe16/0xd3b0 回归）。
2. sports tab 点「立即采样」重建 `data/sports`（2A 做出 sports 按钮后；或先 CLI `collect --category sports`）。
3. **人工 review 两份 v13 榜**通过，才进 2B。

---

## Phase 2A — Dashboard 双榜 + 双 tab 双采样 + 统一控制面（低风险，先做）
**目标**：dashboard 以 `data` 为 root 同时看两类、分别一键重建。**不碰 follow 跟单逻辑、不迁移 follow.db、零 live 风险。**

**落地步骤：**
1. **dashboard root**：`command_serve` 的 `DashboardConfig` 改为 root（默认 `data`），经 `category_data_dirs(root)` 固定映射读 `data/esports`、`data/sports`。**不要把无 `--data-dir` 解析成 `data/esports`**（否则只看得到 esports）。
2. **wallet 聚合以目录 category 为准**：读 `data/esports/smart_wallet_leaderboard.json` → 行**强制 `category="esports"`**；读 `data/sports/...` → 强制 `category="sports"`。行内已有 category 仅参考、不可信（兼容 v12/v13 过渡数据）。`build_wallets` 合并两类。
3. **聪明钱包 UI**（`index.html`/`app.js`）：加 `eSports / Sports` tab（复用现有 `tabs`/`table-filter` 样式），按 `wallet.category` 过滤；每 tab 显示对应榜更新时间/`scoring_version`/隔离数。
4. **双采样按钮**：每 tab 一个「立即采样」+ 各自 busy 态。
   - `/api/wallet-refresh?category=esports|sports`（无效值→400）；`start_wallet_refresh` 按 category 选目录与 `--category`：esports→`collect --category esports --data-dir data/esports`、sports→`collect --category sports --data-dir data/sports`（采样**仍只写各自 category 数据目录**）。
5. **统一控制面**（2A 就引入，别分散）：dashboard 控制状态统一写 `data/follow/follow_control.json`：
   ```
   wallet_refresh: { esports: {...}, sports: {...} }
   runner: {...}
   pause_follow: {...}
   ```
   采样 busy 态读 `wallet_refresh.<category>`，只遮罩对应 tab。

**测试/验证**：`serve` 默认 root=`data` 且能同时看到两类；`/api/wallet-refresh?category=esports` 只 spawn esports collect、`sports` 只 spawn sports collect；refresh 状态写入 `data/follow/follow_control.json`；无效 category→400；两 tab 行数/计数对得上。起 dashboard 分别点两按钮 → 各自重建 `data/esports`、`data/sports`。

---

## Phase 2B — Follow 融合后端（高风险：live DB 迁移 + 路径接口）
**目标**：单 runner 同时跟两类，统一写 `data/follow`。**做完先验证 sports 后端可跟性（验收标准见下），再上 2C。**

**落地步骤：**

1. **`--follow-dir` 作为明确接口**：`run`/`follow`/`serve` 支持 `--follow-dir`（默认 `data/follow`）。follow 状态（`follow.db`/`follow_control.json`/`active_market_cache.json`/`logs/follow`）只走 `follow_dir`。榜单来源 = `category_data_dirs(data)`。

2. **live follow.db 迁移（最高风险，单独脚本、备份、幂等）**：
   - 迁 `data/esports/follow/follow.db` → `data/follow/follow.db`，旧 signals/results 补 `category="esports"`。
   - 若 `data/sports/follow/follow.db` 存在 → 补 `category="sports"` 并入；没有则跳过。
   - **迁移前备份源与目标 DB**；**幂等**：按 `signal_id` / result key 去重。
   - 迁后旧 category-local follow 目录降为只读 legacy（不再更新）。

3. **新数据模型**：新增/更新 signal 必写 `category`/`league`/`market_type`/`market_type_label`（进 `raw_json`，无需加列）；open signal、settlement result 都保留 category。

4. **runner 双榜 eligible scope** — `eligible_follow_wallets` 接收两类榜（每钱包带来源 `category`，以目录为准）；同一钱包两类都合格 → 两条 category-scoped 资格。

5. **active 双拉** — 抓 esports tags + mlb tag，分类器自动给 `category`/`league`，统一写 `data/follow/active_market_cache.json`；每 watched market 必带 `category`/`league`/`market_type`。

6. **门控扩成 (category, market_type)** — `process_follow_trades` 新开 signal 时：市场 `category` 在该钱包 eligible 类目内 **且** `market_type` 达标。

7. **Quarantine 改 category-scoped**：`quarantine key = (category, wallet)`。sports 触发只挡该钱包 sports 新信号、esports 同理；旧 esports quarantine 迁为 `(esports, wallet)`；dashboard 按当前 category 查。

8. **performance 分类聚合**（`storage.py`）：total + `by_category.esports`/`by_category.sports`；wallet performance 仍按 wallet 汇总，signal/result 行可按 category 过滤。

9. **rescore 双榜**：run 循环 `maybe_rescore` 分别刷 `data/esports`、`data/sports`。

**⚠️ Sports 可跟性验收（2B 完成后、做 2C 前必须过）**：
- `sports watched market count > 0`、`sports eligible wallet count > 0`、`sports trade request count > 0`、`sports wallet cursor 能建立`。
- 首轮不强求出 signal；但**连续多个 sports 比赛窗口都没有 pre-match signal** → 重新评估：sports 是否继续 `require_pre_match=True` / 引入 sports 专属 `min_pre_match_entry_rate` / 允许 sports 特定 post-start grace 策略（很多 sports 钱包盘中入场，如 0x6b90）。

**测试**：runner 读两榜并按 category 匹配 watched；esports/sports 钱包各只跟本类目盘；双合格钱包两条独立 scope；signal/settlement 保留 category；performance total + by_category；quarantine 按 (category,wallet)；迁移幂等不重复；缺 sports 榜/缺 follow.db → 空/waiting 不报错、不从 dashboard 路径建 DB。

---

## Phase 2C — Dashboard 融合 UI（前提：2B 后端跑通 + sports 可跟性达标）
**API 读三源**：leaderboard `data/esports`+`data/sports`；follow `data/follow`；control `data/follow/follow_control.json`。
- `/api/follows`（统一 DB，按 category+status 过滤分页）；`/api/events`（统一 active cache 按 category）；`/api/overview`（total + by_category）；`/api/health`（统一 runner + watched/open by category）；`/api/runner/start|stop`（唯一 runner）。
- 详情接口带 category：`/api/follows/{condition_id}?category=`、`/api/wallet-follows?wallet=&status=&category=`、`/api/markets/{condition_id}/prices?category=`。

**UI**（复用现有样式）：
- 聪明钱包/跟单记录/监控赛事 各 `eSports/Sports` tab（保留 status / active-archive / 分页；「游戏」改「项目」：esports=CS2/LOL/Dota2，sports=MLB）。
- 顶部总览：监控赛事/进行中跟单/已结算 PnL 显总数 + 下方 `eSports N / Sports M`；clean/contested 保留总数、tooltip 分类。
- Runner：单状态 + 单启停按钮；任一类目榜空仍可启动，runner 只跟有合格钱包的类目，UI 标 active/waiting。
- 文案 `eSports`/`Sports`，不标实验。

---

## 落地顺序
1. **2.0**：dashboard 点按钮重建 esports v13 + sports v13 → review 两榜。
2. **2A**：dashboard root=`data` + 双 tab + 双采样 + 统一控制面（`data/follow/follow_control.json`）。
3. **2B**：`--follow-dir` 接口 + **迁移 live follow.db（备份+幂等）** + (category,market_type) 门控 + category-scoped quarantine + **sports 可跟性验收**。
4. **2C**：dashboard 融合 UI。

## Test Plan（汇总）
- `serve` 默认 root=`data`，同时看到 `data/esports` + `data/sports`。
- `/api/wallet-refresh?category=esports|sports` 各跑各的 collect；refresh 状态统一写 `data/follow/follow_control.json`；无效 category→400。
- `run/follow` 默认 `--follow-dir data/follow`；runner 读两榜按 category 匹配 watched；new signal/result 写 category。
- quarantine 按 `(category, wallet)` 生效；migration 幂等、不重复 signal/result。
- overview 返回 total + by_category；UI tab/分页/过滤/详情按 category 正确。
- `python3 -m unittest discover -s tests -v` 全绿。

## Assumptions
- sports v1 = MLB moneyline only。
- `data/follow` 是统一 paper follow 唯一真相源；category 数据目录只负责采集与 leaderboard。
- dashboard root=`data`，固定白名单映射两类；不复用 resolve_data_dir、不 glob。
- quarantine = `(category, wallet)`。
- 单 runner 是最终目标，不做双 runner 过渡。
