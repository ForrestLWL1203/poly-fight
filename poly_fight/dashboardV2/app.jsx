/* Poly Sniper dashboard V2 — single-scope React app (Babel-in-browser).
   Composes the design-system bundle (window.PolySniperDesignSystem_8d05e5),
   the data layer (window.PSApi) and the API→kit adapters (window.PSAdapt).   */
const {
  SidebarNav, Tabs, Card, Button, IconButton, Switch, SegmentedControl,
  StatTile, TrendValue, RankBadge, WalletAddress, GameIcon, Badge, StatusPill, WinRateRing, CategoryDonut, Input,
} = window.PolySniperDesignSystem_8d05e5;
const Api = window.PSApi;
const Adapt = window.PSAdapt;
const ASSET_BASE = "/ds/assets";
const RECENT_FOLLOWS = 5; // Overview「最近跟单」shows the most recent N follows

/* ---------- formatters & helpers ---------- */
const money = (n) => (n < 0 ? "-$" : "$") + Math.abs(Number(n) || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const signedMoney = (n) => { n = Number(n) || 0; return (n > 0 ? "+$" : n < 0 ? "-$" : "$") + Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); };
const compactMoney = (n) => { n = Number(n) || 0; return Math.abs(n) >= 1000 ? "$" + (n / 1000).toFixed(1) + "K" : "$" + n.toFixed(0); };
const usdInt = (n) => "$" + Math.floor(Math.max(0, Number(n) || 0)).toLocaleString();
const pnlClass = (n) => (n > 0 ? "pnl-up" : n < 0 ? "pnl-down" : "pnl-flat");
const hms = (sec) => {
  sec = Math.max(0, Math.floor(Number(sec) || 0));
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = sec % 60;
  const p = (x) => String(x).padStart(2, "0");
  return p(h) + ":" + p(m) + ":" + p(s);
};

/* ---------- Pager ---------- */
function pageList(cur, total) {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
  const s = new Set([1, total, cur, cur - 1, cur + 1]);
  if (cur <= 3) [2, 3, 4].forEach((n) => s.add(n));
  if (cur >= total - 2) [total - 1, total - 2, total - 3].forEach((n) => s.add(n));
  const arr = [...s].filter((n) => n >= 1 && n <= total).sort((a, b) => a - b);
  const out = [];
  arr.forEach((n, i) => { if (i > 0 && n - arr[i - 1] > 1) out.push("…"); out.push(n); });
  return out;
}
const Chevron = ({ dir }) => (
  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    {dir === "left" ? <path d="M15 18l-6-6 6-6" /> : <path d="M9 18l6-6-6-6" />}
  </svg>
);
function Pager({ total, pageSize, page, onChange, unit = "条" }) {
  const pages = Math.max(1, Math.ceil(total / pageSize));
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = Math.min(total, page * pageSize);
  return (
    <div className="pager">
      <span className="pager-range">{from}–{to}<em> / 共 {total} {unit}</em></span>
      <div className="pager-nav">
        <button className="pg-btn pg-arrow" disabled={page <= 1} onClick={() => onChange(page - 1)} aria-label="上一页"><Chevron dir="left" /></button>
        {pageList(page, pages).map((n, i) => n === "…"
          ? <span key={"e" + i} className="pg-ellipsis">…</span>
          : <button key={n} className={"pg-btn" + (n === page ? " is-active" : "")} onClick={() => onChange(n)} aria-current={n === page ? "page" : undefined}>{n}</button>)}
        <button className="pg-btn pg-arrow" disabled={page >= pages} onClick={() => onChange(page + 1)} aria-label="下一页"><Chevron dir="right" /></button>
      </div>
    </div>
  );
}

/* ---------- match cell / teams ---------- */
const initials = (name) => {
  const p = String(name || "").split(/\s+/).filter(Boolean);
  if (!p.length) return "?";
  return (p.length === 1 ? p[0].slice(0, 2) : p[0][0] + p[1][0]).toUpperCase();
};
// Two fixed, theme-aware side colors (same A/B convention as the SplitBar).
const TEAM_SIDE_COLOR = { a: "var(--side-a)", b: "var(--side-b)" };
function TeamMonogram({ name, logo, side = "a", size = 26 }) {
  if (logo) return <img className="team-logo" src={logo} alt="" style={{ width: size, height: size, borderRadius: "50%", objectFit: "cover", flex: "none" }} />;
  return <span className="team-mono" style={{ width: size, height: size, "--team": TEAM_SIDE_COLOR[side] || TEAM_SIDE_COLOR.a }}>{initials(name)}</span>;
}
function TeamLine({ ev, size = 26 }) {
  const logos = ev.teamLogos || {};
  return (
    <div className="team-line">
      <span className="team"><TeamMonogram name={ev.teamA} logo={logos[ev.teamA]} side="a" size={size} /><span className="team-name">{ev.teamA || "—"}</span></span>
      <span className="vs">vs</span>
      <span className="team"><TeamMonogram name={ev.teamB} logo={logos[ev.teamB]} side="b" size={size} /><span className="team-name">{ev.teamB || "—"}</span></span>
    </div>
  );
}
function MatchCell({ ev }) {
  return (
    <div className="match-cell">
      <div className="match-game">{ev.game ? <GameIcon game={ev.game} base={ASSET_BASE} chip /> : null}<span className="match-meta">{ev.meta}</span></div>
      <TeamLine ev={ev} />
      {(ev.start || ev.end) && <div className="match-times"><span>开始 {ev.start || "—"}</span><span className="dot-sep">·</span><span>截止 {ev.end || "—"}</span></div>}
    </div>
  );
}
function EquityArea({ points, width = 520, height = 76 }) {
  const pts = points && points.length >= 2 ? points : (points && points.length === 1 ? [points[0], points[0]] : [0, 0]);
  const min = Math.min(...pts), max = Math.max(...pts), span = max - min || 1;
  const stepX = width / (pts.length - 1);
  const xy = pts.map((p, i) => [i * stepX, height - ((p - min) / span) * (height - 10) - 5]);
  const line = xy.map((p, i) => (i === 0 ? "M" : "L") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  const area = line + ` L ${width} ${height} L 0 ${height} Z`;
  const last = xy[xy.length - 1];
  const up = pts[pts.length - 1] >= pts[0];
  const col = up ? "var(--pnl-up)" : "var(--pnl-down)";
  return (
    <svg className="equity-svg" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" width="100%" height="100%">
      <defs>
        <linearGradient id="eqFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={up ? "rgba(31,157,87,0.22)" : "rgba(229,72,77,0.18)"} />
          <stop offset="100%" stopColor="rgba(0,0,0,0)" />
        </linearGradient>
      </defs>
      <path d={area} fill="url(#eqFill)" />
      <path d={line} fill="none" stroke={col} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={last[0]} cy={last[1]} r="3" fill={col} />
    </svg>
  );
}
function SplitBar({ a, b }) {
  const total = a + b || 1;
  return <div className="split-bar" title={`${a} : ${b}`}><span className="split-a" style={{ width: (a / total) * 100 + "%" }}></span><span className="split-b" style={{ width: (b / total) * 100 + "%" }}></span></div>;
}
const qualityBadge = (q) =>
  q === "clean" ? <Badge tone="up">单向</Badge> : q === "contested" ? <Badge tone="warn">分歧</Badge> : <Badge tone="warn" outline>双边</Badge>;

function Spinner({ sm }) { return <span className={"ps-spinner" + (sm ? " sm" : "")} aria-label="加载中" />; }
function CenterLoad() { return <div style={{ padding: "var(--sp-10)", display: "flex", justifyContent: "center" }}><Spinner /></div>; }
function PagePlaceholder({ title }) {
  return (
    <div className="page-inner">
      <Card pad="lg"><div style={{ padding: "var(--sp-8)", textAlign: "center", color: "var(--text-tertiary)" }}>{title} · 建设中</div></Card>
    </div>
  );
}

/* ============================================================
   Overview
   ============================================================ */
function OverviewPage({ data, onNav, onOpenFollow }) {
  const raw = data.overview;
  const [distMetric, setDistMetric] = React.useState("count");
  const [pnlWin, setPnlWin] = React.useState("h24");
  if (!raw) return <CenterLoad />;
  const o = Adapt.overview(raw, data.health);
  // Prefer the grouped event count from /api/events over raw watched-market count.
  if (data.events && Array.isArray(data.events.events)) o.watchedEvents = data.events.events.length;
  const ft = Adapt.followTypes(raw);
  const winRates = Adapt.winRates(raw);
  const winCaption = { h24: "过去 24 小时", d7: "过去 7 日", d30: "过去 30 日", all: "累计至今" };
  const DAY = 86400000;
  const winMs = { h24: DAY, d7: 7 * DAY, d30: 30 * DAY, all: null };
  const equity = Adapt.equitySeries(raw, winMs[pnlWin]);
  const winPnl = equity.length ? equity[equity.length - 1] : 0;
  const nav = onNav || (() => {});
  const runnerLive = data.runner && data.runner.status === "running";

  const distSegments = ft.segments.map((s) => ({ ...s, value: distMetric === "stake" ? s.stake : s.value }));
  const distCenter = distMetric === "stake" ? compactMoney(ft.totalStake) : ft.total;
  const distLabel = distMetric === "stake" ? "总投入" : "跟单笔数";
  const distTotal = distSegments.reduce((a, s) => a + (s.value || 0), 0) || 1;
  const distMarkets = [...new Set(distSegments.map((s) => s.label))];
  const distMap = {};
  distSegments.forEach((s) => { distMap[s.group + "|" + s.label] = s; });
  const distGames = [...new Set(distSegments.map((s) => s.group))].map((group) => ({
    group, gameId: (distSegments.find((s) => s.group === group) || {}).gameId,
  }));
  const hasDist = distSegments.length > 0;

  return (
    <div className="page-inner">
      <div className="ov-grid">
        <Card glow pad="lg" className="ov-herocard">
          <div className="ov-hero">
            <div className="ov-hero-top">
              <StatTile size="lg" tone="up" label="已结算盈亏" value={signedMoney(o.realizedPnl)} delta={<TrendValue value={o.realizedRoi} percent chip />} sub={`累计投入 ${money(o.totalStake)}`} />
              <StatusPill status={runnerLive ? "live" : "idle"} label={runnerLive ? "运行中" : "已停止"} extra={runnerLive ? hms(data.health && data.health.uptime_seconds) : undefined} />
            </div>
            <div className="ov-metricbar">
              <div className="m"><span>已结算 ROI</span><b className={pnlClass(o.realizedPnl)}>{o.realizedRoi > 0 ? "+" : ""}{o.realizedRoi}%</b></div>
              <div className="m"><span>结算场次</span><b>{o.settledCount}</b></div>
              <div className="m"><span>当前持仓</span><b>{money(o.openExposure)}</b></div>
              <div className="m"><span>钱包余额</span><b>{o.walletConfigured ? money(o.walletBalance) : "—"}</b></div>
            </div>
            {hasDist && (
              <div className="ov-herodist">
                <div className="ov-herodist-head">
                  <div>
                    <div className="ps-card-eyebrow">盘口结构</div>
                    <h3 className="ov-herodist-title">历史跟单类型分布</h3>
                  </div>
                  <SegmentedControl value={distMetric} onChange={setDistMetric} options={[
                    { value: "count", label: "按笔数" },
                    { value: "stake", label: "按金额" },
                  ]} />
                </div>
                <div className="ov-herodist-body">
                  <CategoryDonut size={112} thickness={18} centerValue={distCenter} centerLabel={distLabel} segments={distSegments} />
                  <div className="ov-distmatrix">
                    <div className="ov-dm-row ov-dm-head">
                      <span className="ov-dm-game"></span>
                      {distMarkets.map((m) => <span key={m} className="ov-dm-cell">{m}</span>)}
                    </div>
                    {distGames.map((g) => (
                      <div key={g.group} className="ov-dm-row">
                        <span className="ov-dm-game">{g.gameId ? <GameIcon game={g.gameId} size="sm" base={ASSET_BASE} /> : null} {g.group}</span>
                        {distMarkets.map((m) => {
                          const s = distMap[g.group + "|" + m];
                          return (
                            <span key={m} className="ov-dm-cell">
                              <i className="ov-dm-sw" style={{ background: s ? s.color : "transparent" }}></i>
                              {s ? Math.round((s.value / distTotal) * 100) : 0}%
                            </span>
                          );
                        })}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </Card>
        <Card className="ov-rightcard">
          <div className="ov-twostat">
            <StatTile label="监控赛事" value={<button type="button" className="ov-link-num" onClick={() => nav("events")}>{o.watchedEvents}<i data-lucide="arrow-up-right"></i></button>} sub="esports" />
            <div className="ov-twostat-div"></div>
            <StatTile label="进行中跟单" tone="gradient" value={<button type="button" className="ov-link-num" onClick={() => nav("follows")}>{o.openFollows}<i data-lucide="arrow-up-right"></i></button>} sub={`${o.openByGame.length} 个项目`} />
          </div>
          <div className="ov-openlist">
            {o.openByGame.length === 0 && <div className="ov-openrow" style={{ cursor: "default", justifyContent: "center", color: "var(--text-tertiary)" }}>暂无进行中跟单</div>}
            {o.openByGame.map((g) => (
              <button type="button" key={g.game} className="ov-openrow" onClick={() => nav("follows")}>
                <span className="ov-openrow-game"><GameIcon game={g.game} size="sm" base={ASSET_BASE} /> {g.name}</span>
                <span className="ov-openrow-count">{g.count} 场</span>
              </button>
            ))}
          </div>
          <div className="ov-qualityblock">
            <div className="ov-qualityblock-head">
              <div className="ps-card-eyebrow">盘口结构</div>
              <h3 className="ov-qtitle">跟单质量</h3>
            </div>
            <div className="ov-quality">
              <div className="ov-qcell"><span className="qv pnl-flat">{o.cleanCount}</span><span className="ql">单向盘</span><small>无双边 / 分歧</small></div>
              <div className="ov-qcell"><span className="qv" style={{ color: "var(--status-warn)" }}>{o.twoSidedCount + o.disagreementCount}</span><span className="ql">双边 / 分歧盘</span><small>双边 {o.twoSidedCount} · 分歧 {o.disagreementCount}</small></div>
            </div>
          </div>
        </Card>
      </div>
      <Card className="ov-winrate-card" eyebrow="跟单胜率" title="历史综合胜率" action={
        <SegmentedControl value={pnlWin} onChange={setPnlWin} options={[
          { value: "h24", label: "24小时" },
          { value: "d7", label: "7日" },
          { value: "d30", label: "30日" },
          { value: "all", label: "至今" },
        ]} />
      }>
        <div className="ov-winrate">
          <div className="ov-winrate-hero">
            <div className="ov-wr-big">
              <WinRateRing size="lg" wins={o.winRate.wins} losses={o.winRate.losses} label="综合胜率" legend />
            </div>
            <div className="ov-wr-games-col">
              {winRates.length === 0 && <span className="ov-wr-empty">暂无已结算项目</span>}
              {winRates.slice(0, 3).map((g) => (
                <div className="ov-wr-grow" key={g.game}>
                  <WinRateRing size="sm" wins={g.wins} losses={g.losses} label="" />
                  <div className="ov-wr-gname">
                    <span className="g-name"><GameIcon game={g.game} size="sm" base={ASSET_BASE} /> {g.name}</span>
                    <span className="g-rec">胜 {g.wins} · 负 {g.losses}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="ov-winrate-divider"></div>
          <div className="ov-winrate-right">
            <div className="ov-pnlwin">
              <span className="ps-card-eyebrow">区间盈亏</span>
              <div className="ov-pnlwin-val">
                <b className={pnlClass(winPnl)}>{signedMoney(winPnl)}</b>
                <span className="ov-pnlwin-cap">{winCaption[pnlWin]}</span>
              </div>
              <div className="ov-pnlwin-chart">
                {equity.length >= 2
                  ? <EquityArea points={equity} />
                  : <div className="ov-pnlwin-empty">该区间暂无结算数据</div>}
              </div>
            </div>
          </div>
        </div>
      </Card>
      <Card pad="flush">
        <div style={{ padding: "var(--sp-6) var(--sp-6) var(--sp-4)" }}>
          <div className="sec-head" style={{ marginBottom: 0 }}><h2 style={{ fontSize: "var(--fs-h4)" }}>最近跟单</h2><Badge tone="accent">最近 {RECENT_FOLLOWS} 笔</Badge></div>
        </div>
        <div className="tbl-wrap">
          <table className="ps-table">
            <thead><tr><th>赛事</th><th>状态</th><th>钱包</th><th>投入</th><th>盈亏</th><th>质量</th></tr></thead>
            <tbody>
              {(data.follows && data.follows.follows ? data.follows.follows : []).slice(0, RECENT_FOLLOWS).map((row) => {
                const f = Adapt.follow ? Adapt.follow(row) : null;
                if (!f) return null;
                return (
                  <tr key={f.cid} className="clickable" onClick={() => onOpenFollow && onOpenFollow(f.cid)}>
                    <td><MatchCell ev={f} /></td>
                    <td>{f.status === "open" ? <StatusPill status="live" label="进行中" /> : <Badge tone="neutral">已结算</Badge>}</td>
                    <td className="strong">{f.wallets}</td>
                    <td className="num">{money(f.stake)}</td>
                    <td className={pnlClass(f.pnl)}><div className="cell-stack"><span className="strong">{signedMoney(f.pnl)}</span>{f.pnlKind === "unrealized" && <span className="muted">未实现</span>}</div></td>
                    <td>{qualityBadge(f.quality)}</td>
                  </tr>
                );
              })}
              {(!data.follows || !(data.follows.follows || []).length) && <tr><td colSpan="6" className="empty-cell">暂无跟单记录</td></tr>}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

/* ============================================================
   Leaderboard
   ============================================================ */
function LeaderboardPage({ data, merge, toast, onOpenWallet, onSample }) {
  const [view, setView] = React.useState("active");
  const [gameFilter, setGameFilter] = React.useState("all");
  const [pg, setPg] = React.useState(1);
  const [busy, setBusy] = React.useState({});
  const [samplePanel, setSamplePanel] = React.useState(false);
  const [wrInput, setWrInput] = React.useState("75");
  const [entryInput, setEntryInput] = React.useState("0.65");
  React.useEffect(() => { setPg(1); }, [view, gameFilter]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });

  if (!data.wallets) return <CenterLoad />;
  const lb = Adapt.wallets(data.wallets);
  const all = lb.rows;
  const activeWallets = all.filter((w) => !w.quarantined);
  const favRows = activeWallets.filter((w) => w.fav);
  const quarantinedRows = all.filter((w) => w.quarantined);
  const viewRows = view === "quarantined" ? quarantinedRows : view === "favorite" ? favRows : activeWallets;
  const rows = gameFilter === "all" ? viewRows : viewRows.filter((w) => w.game === gameFilter);
  const q = view === "quarantined";
  const PAGE = 20;
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);

  const refresh = () => Api.wallets().then((w) => merge({ wallets: w })).catch(() => {});
  const setW = (addr, v) => setBusy((p) => ({ ...p, [addr]: v }));

  const toggleFav = async (w) => {
    setW(w.addr, true);
    try { await Api.setFavorite(w.addr, w.category, !w.fav); await refresh(); }
    catch (e) { toast(e && e.error === "wallet_quarantined" ? "已隔离的钱包无法收藏" : "操作失败", "error"); }
    finally { setW(w.addr, false); }
  };
  const isolate = async (w) => {
    setW(w.addr, true);
    try { await Api.setQuarantine(w.addr, w.category, true); await refresh(); toast("已隔离该钱包", "success"); }
    catch (e) { toast("隔离失败", "error"); }
    finally { setW(w.addr, false); }
  };
  const restore = async (w) => {
    setW(w.addr, true);
    try { await Api.setQuarantine(w.addr, w.category, false); await refresh(); toast("已恢复该钱包", "success"); }
    catch (e) { toast("恢复失败", "error"); }
    finally { setW(w.addr, false); }
  };
  const refreshing = JSON.stringify(data.refresh || {}).includes('"running"');
  const updatedLabel = lb.updatedAt ? Adapt.timeAgo(lb.updatedAt) : "—";
  const startSample = () => {
    const pr = Math.max(50, Math.min(99, parseFloat(wrInput) || 75)) / 100;
    const me = Math.max(0.30, Math.min(0.95, parseFloat(entryInput) || 0.65));
    setSamplePanel(false);
    onSample && onSample("esports", { min_positive_rate: pr, max_median_entry: me });
  };

  return (
    <div className="page-inner">
      <Card pad="flush">
        <div style={{ padding: "var(--sp-6) var(--sp-6) var(--sp-5)" }}>
          <div className="panel-toolbar" style={{ marginBottom: 0 }}>
            <SegmentedControl value={view} onChange={setView} options={[
              { value: "active", label: "活跃", count: lb.activeCount },
              { value: "favorite", label: "收藏", count: favRows.length },
              { value: "quarantined", label: "隔离", count: lb.quarantinedCount },
            ]} />
            <div className="sec-actions">
              <select className="ps-select" value={gameFilter} onChange={(e) => setGameFilter(e.target.value)} aria-label="按游戏过滤">
                <option value="all">全部游戏</option>
                <option value="dota2">Dota 2</option>
                <option value="cs2">CS2</option>
                <option value="lol">LoL</option>
              </select>
              <span className="lb-updated" style={{ marginRight: 4 }}>最后更新 {updatedLabel} · {lb.activeCount} 个活跃</span>
              <div className="sample-trigger">
                <Button variant="primary" disabled={refreshing} iconLeft={refreshing ? <Spinner sm /> : <i data-lucide="radar" style={{ width: 16, height: 16 }} />} onClick={() => setSamplePanel((v) => !v)}>采样钱包</Button>
                {samplePanel && <>
                  <div className="sample-backdrop" onClick={() => setSamplePanel(false)} />
                  <div className="sample-pop" role="dialog" aria-label="采集门槛设置">
                    <div className="sample-pop-title">采集门槛</div>
                    <Input label="胜率门槛" type="number" min="50" max="99" step="1" suffix="%" value={wrInput} onChange={(e) => setWrInput(e.target.value)} block />
                    <Input label="买入价上限" type="number" min="0.3" max="0.95" step="0.01" value={entryInput} onChange={(e) => setEntryInput(e.target.value)} block />
                    <div className="sample-pop-hint">仅采集专精盘口胜率 ≥ 门槛、买入价 ≤ 上限的钱包</div>
                    <Button variant="primary" disabled={refreshing} iconLeft={<i data-lucide="radar" style={{ width: 15, height: 15 }} />} onClick={startSample}>开始采样</Button>
                  </div>
                </>}
              </div>
            </div>
          </div>
        </div>
        <div className="tbl-wrap">
          <table className="ps-table">
            <thead><tr>
              {!q && <th></th>}<th>Rank</th><th>钱包</th>{q && <th>隔离原因</th>}<th>专精 ROI</th><th>胜率</th><th>场均交易额</th><th>专精</th>{!q && <th>跟单胜负</th>}{!q && <th>跟单 PnL</th>}<th>最后交易</th><th></th>
            </tr></thead>
            <tbody key={view + cur} className="tbl-fade">
              {pageRows.map((w) => (
                <tr key={w.addr} className="clickable" onClick={() => onOpenWallet && onOpenWallet(w.addr)}>
                  {!q && <td><button className={"fav-btn" + (w.fav ? " on" : "")} disabled={busy[w.addr]} onClick={(e) => { e.stopPropagation(); toggleFav(w); }} aria-label="收藏">{w.fav ? "★" : "☆"}</button></td>}
                  <td>{w.rank != null ? <RankBadge rank={w.rank} /> : <span className="muted">—</span>}</td>
                  <td><WalletAddress address={w.addr} copyable /></td>
                  {q && <td><div className="cell-stack"><span className="strong" style={{ color: "var(--status-warn)" }}>{w.reason}</span><span className="muted">{w.reasonTime}</span></div></td>}
                  <td><div className="cell-stack"><span className={pnlClass(w.roi) + " strong"}>{w.roi > 0 ? "+" : ""}{w.roi}%</span>{w.overallRoi != null && <span className="muted">全部 {w.overallRoi > 0 ? "+" : ""}{w.overallRoi}%</span>}</div></td>
                  <td><div className="cell-stack"><span className="strong">{w.winRate != null ? w.winRate + "%" : "—"}</span>{w.closedCount > 0 && <span className="muted">{w.closedCount} 场</span>}</div></td>
                  <td className="num" title={money(w.avgCash)}>{compactMoney(w.avgCash)}</td>
                  <td><div className="scope-list">{w.scope.map((s, j) => <span key={j} className="scope-item"><GameIcon game={s.game} base={ASSET_BASE} size="sm" /><span>{s.market}</span></span>)}{!w.scope.length && <span className="muted">–</span>}</div></td>
                  {!q && <td className="strong">{w.followRec}</td>}
                  {!q && <td className={pnlClass(w.followPnl) + " num strong"}>{w.settled ? signedMoney(w.followPnl) : "—"}</td>}
                  <td className="muted">{w.lastTrade || "—"}</td>
                  <td className="row-action">
                    {!q && <Button variant="danger" size="sm" className="tbl-action danger" disabled={busy[w.addr]} iconLeft={<i data-lucide="circle-minus" style={{ width: 12, height: 12 }} />} onClick={(e) => { e.stopPropagation(); isolate(w); }}>隔离</Button>}
                    {q && <Button variant="secondary" size="sm" className="tbl-action restore" disabled={busy[w.addr]} iconLeft={<i data-lucide="rotate-ccw" style={{ width: 12, height: 12 }} />} onClick={(e) => { e.stopPropagation(); restore(w); }}>恢复</Button>}
                  </td>
                </tr>
              ))}
              {!rows.length && <tr><td colSpan="11" className="empty-cell">暂无{view === "favorite" ? "收藏" : view === "quarantined" ? "隔离" : "活跃"}钱包</td></tr>}
            </tbody>
          </table>
        </div>
        <div style={{ padding: "0 var(--sp-6) var(--sp-5)" }}>
          <Pager total={rows.length} pageSize={PAGE} page={cur} onChange={setPg} unit="钱包" />
        </div>
      </Card>
    </div>
  );
}

/* ============================================================
   Events
   ============================================================ */
function EventsPage({ data }) {
  const [tab, setTab] = React.useState("active");
  const [game, setGame] = React.useState("all");
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => { setPg(1); }, [tab, game]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });

  if (!data.events) return <CenterLoad />;
  const ev = Adapt.events(data.events);
  const base = tab === "archive" ? ev.archive : ev.events;
  const rows = game === "all" ? base : base.filter((e) => e.game === game);
  const archive = tab === "archive";
  const PAGE = 10;
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);

  return (
    <div className="page-inner">
      <Card pad="flush">
        <div style={{ padding: "var(--sp-6) var(--sp-6) var(--sp-5)" }}>
          <div className="panel-toolbar" style={{ marginBottom: 0 }}>
            <Tabs value={tab} onChange={setTab} tabs={[{ id: "active", label: "进行中 / 即将开始", count: ev.events.length }, { id: "archive", label: "已结算", count: ev.archive.length }]} />
            <div className="filter-group">
              <label htmlFor="game-f">项目</label>
              <select id="game-f" className="ps-select" value={game} onChange={(e) => setGame(e.target.value)}>
                <option value="all">全部</option><option value="dota2">Dota 2</option><option value="cs2">CS2</option><option value="lol">LoL</option><option value="valorant">Valorant</option>
              </select>
            </div>
          </div>
        </div>
        <div className="tbl-wrap">
          <table className="ps-table">
            <thead><tr><th>赛事</th><th>状态</th><th>{archive ? "结算 PNL" : "跟单情况 (A : B)"}</th></tr></thead>
            <tbody key={tab + game + cur} className="tbl-fade">
              {pageRows.map((e) => (
                <tr key={e.cid}>
                  <td>{e.eventUrl ? <a className="evt-link" href={e.eventUrl} target="_blank" rel="noopener noreferrer" title="在 Polymarket 打开该赛事"><MatchCell ev={e} /></a> : <MatchCell ev={e} />}</td>
                  <td><div className="evt-status">
                    {e.status === "live" && <StatusPill status="live" label="进行中" />}
                    {e.status === "upcoming" && <Badge tone="accent" dot>即将开始</Badge>}
                    {e.status === "settled" && <Badge tone="neutral">已结算</Badge>}
                    {e.countdown && !archive && e.status !== "live" && <span className="evt-count">{e.countdown}</span>}
                  </div></td>
                  <td>{archive
                    ? <span className={pnlClass(e.pnl) + " strong num"} style={{ fontSize: "var(--fs-h4)" }}>{signedMoney(e.pnl)}</span>
                    : ((e.followA + e.followB) > 0
                      ? <div className="follow-count-line"><SplitBar a={e.followA} b={e.followB} /><span><b className="ca">{e.followA}</b> : <b className="cb">{e.followB}</b></span></div>
                      : <span className="muted">暂无跟单</span>)}
                  </td>
                </tr>
              ))}
              {!rows.length && <tr><td colSpan="3" className="empty-cell">{archive ? "暂无已结算赛事" : "当前窗口暂无监控赛事"}</td></tr>}
            </tbody>
          </table>
        </div>
        <div style={{ padding: "0 var(--sp-6) var(--sp-5)" }}>
          <Pager total={rows.length} pageSize={PAGE} page={cur} onChange={setPg} unit="赛事" />
        </div>
      </Card>
    </div>
  );
}

/* ============================================================
   Follows
   ============================================================ */
function FollowsPage({ data, goStrategy, onOpenFollow }) {
  const [status, setStatus] = React.useState("all");
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => { setPg(1); }, [status]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });

  if (!data.follows) return <CenterLoad />;
  const all = Adapt.follows(data.follows).rows;
  const rows = status === "all" ? all : all.filter((f) => f.status === status);
  const PAGE = 10;
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);

  return (
    <div className="page-inner">
      <Card pad="flush">
        <div style={{ padding: "var(--sp-6) var(--sp-6) var(--sp-5)" }}>
          <div className="panel-toolbar" style={{ marginBottom: 0 }}>
            <div className="filter-group">
              <label htmlFor="st-f">状态</label>
              <select id="st-f" className="ps-select" value={status} onChange={(e) => setStatus(e.target.value)}>
                <option value="all">全部</option><option value="open">进行中</option><option value="settled">已结算</option>
              </select>
            </div>
            <Button size="sm" variant="ghost" iconLeft={<i data-lucide="sliders-horizontal"></i>} onClick={goStrategy}>调整策略</Button>
          </div>
        </div>
        <div className="tbl-wrap">
          <table className="ps-table">
            <thead><tr><th>赛事</th><th>状态</th><th>结算</th><th>钱包数</th><th>单数</th><th>投入</th><th>盈亏</th><th>质量</th></tr></thead>
            <tbody key={status + cur} className="tbl-fade">
              {pageRows.map((f) => (
                <tr key={f.cid} className="clickable" onClick={() => onOpenFollow(f.cid)}>
                  <td><MatchCell ev={f} /></td>
                  <td><div className="evt-status">{f.status === "open" ? <StatusPill status="live" label="进行中" /> : <Badge tone="neutral">已结算</Badge>}{f.sourceOffLeaderboard && <Badge tone="warn" title="源钱包已不在最新榜单 — 此跟单继续跟至结算，但不再新开仓">源已脱榜</Badge>}</div></td>
                  <td>{f.settlement === "盈利" ? <span className="pnl-up strong">盈利</span> : f.settlement === "亏损" ? <span className="pnl-down strong">亏损</span> : <span className="muted">未结算</span>}</td>
                  <td className="strong">{f.wallets}</td>
                  <td className="num">{f.legs}</td>
                  <td className="num">{money(f.stake)}</td>
                  <td className={pnlClass(f.pnl)}><div className="cell-stack"><span className="strong num">{signedMoney(f.pnl)}</span>{f.pnlKind === "unrealized" && <span className="muted">未实现</span>}</div></td>
                  <td>{qualityBadge(f.quality)}</td>
                </tr>
              ))}
              {!rows.length && <tr><td colSpan="8" className="empty-cell">暂无跟单记录</td></tr>}
            </tbody>
          </table>
        </div>
        <div style={{ padding: "0 var(--sp-6) var(--sp-5)" }}>
          <Pager total={rows.length} pageSize={PAGE} page={cur} onChange={setPg} unit="笔" />
        </div>
      </Card>
    </div>
  );
}

/* ---------- Follow detail modal ---------- */
const fmtDelay = (s) => { s = Math.round(Number(s) || 0); return s < 60 ? s + "s" : Math.floor(s / 60) + "m" + (s % 60) + "s"; };
const priceStr = (p) => (p == null ? "—" : Number(p).toFixed(3));
// Drop the leading "<Game full name>: " prefix from a Polymarket event title
// (the game is shown as a logo chip instead). Only strips a short leading prefix.
const stripGamePrefix = (title) => { const t = String(title || ""); const i = t.indexOf(": "); return i > 0 && i <= 24 ? t.slice(i + 2) : t; };

const WALLET_LEGS_PER_PAGE = 5;
const isFundedLeg = (leg) => leg.would_follow !== false && leg.funding_status !== "unfunded";
function WalletLegBlock({ w, prices }) {
  const [pg, setPg] = React.useState(1);
  const legs = (w.signals || []).flatMap((s) => (s.legs || [])
    .filter(isFundedLeg)
    .map((leg, i) => ({ leg, key: s.signal_id + ":" + i })));

  // P&L: settled → realized; open → unrealized from current orderbook
  // (Σ funded_stake × (current_price − our_entry_price) / our_entry_price).
  const px = prices || [];
  let unrealized = 0, priced = false;
  (w.signals || []).forEach((s) => {
    const cp = Number(px[Number(s.outcome_index || 0)]);
    if (!Number.isFinite(cp)) return;
    (s.legs || []).filter(isFundedLeg).forEach((leg) => {
      const entry = Number(leg.our_entry_price);
      const stake = Number(leg.funded_stake != null ? leg.funded_stake : leg.stake);
      if (entry > 0 && stake > 0) { unrealized += stake * (cp - entry) / entry; priced = true; }
    });
  });
  const realized = w.follow_realized_pnl != null;
  const pnlValue = realized ? Number(w.follow_realized_pnl) : (priced ? unrealized : null);
  const pages = Math.max(1, Math.ceil(legs.length / WALLET_LEGS_PER_PAGE));
  const cur = Math.min(pg, pages);
  const pageLegs = legs.slice((cur - 1) * WALLET_LEGS_PER_PAGE, cur * WALLET_LEGS_PER_PAGE);
  return (
    <div className="wallet-block">
      <div className="wallet-block-head">
        <div style={{ display: "flex", alignItems: "center", gap: "var(--sp-3)" }}>
          {w.leaderboard_rank != null && <RankBadge rank={w.leaderboard_rank} />}
          <WalletAddress address={w.wallet} copyable />
        </div>
        <div className="wallet-block-meta">
          <span>投入 <b>{money(w.follow_total_stake)}</b></span>
          <span>均价 <b>{priceStr(w.follow_avg_entry_price)}</b></span>
          <span>盈亏 <b className={pnlClass(pnlValue || 0)}>{pnlValue != null ? signedMoney(pnlValue) : "—"}</b></span>
        </div>
      </div>
      <div className="tbl-wrap">
        <table className="ps-table">
          <thead><tr><th>钱包时间</th><th>钱包价</th><th>钱包额</th><th>延迟</th><th>投入</th><th>我方价</th><th>滑点</th></tr></thead>
          <tbody key={cur} className="tbl-fade">
            {pageLegs.map(({ leg, key }) => (
              <tr key={key}>
                <td className="muted">{Adapt.fmtClock(leg.wallet_trade_at)}</td>
                <td className="num">{priceStr(leg.wallet_fill_price)}</td>
                <td className="num">{money(leg.wallet_trade_cash)}</td>
                <td className="muted">{fmtDelay(leg.observed_delay_seconds)}</td>
                <td className="num strong">{money(leg.funded_stake != null ? leg.funded_stake : leg.stake)}</td>
                <td className="num">{priceStr(leg.our_entry_price)}</td>
                <td className={"num " + pnlClass(-(Number(leg.slippage_over_wallet_entry) || 0))}>{leg.slippage_over_wallet_entry != null ? (Number(leg.slippage_over_wallet_entry) > 0 ? "+" : "") + Number(leg.slippage_over_wallet_entry).toFixed(3) : "—"}</td>
              </tr>
            ))}
            {!legs.length && <tr><td colSpan="7" className="empty-cell">暂无已跟记录</td></tr>}
          </tbody>
        </table>
      </div>
      {legs.length > WALLET_LEGS_PER_PAGE && <Pager total={legs.length} pageSize={WALLET_LEGS_PER_PAGE} page={cur} onChange={setPg} unit="笔" />}
    </div>
  );
}

function FollowDetailModal({ cid, onClose, toast }) {
  const [detail, setDetail] = React.useState(null);
  const [err, setErr] = React.useState(false);
  const [prices, setPrices] = React.useState(null);
  const [refreshing, setRefreshing] = React.useState(false);
  React.useEffect(() => {
    let alive = true;
    Api.followDetail(cid).then((d) => alive && setDetail(d)).catch(() => alive && setErr(true));
    return () => { alive = false; };
  }, [cid]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });

  const refreshPrices = async () => {
    setRefreshing(true);
    try { const p = await Api.marketPrices(cid); setPrices(p.outcome_prices || null); toast && toast("已刷新盘口价", "success"); }
    catch (e) { toast && toast("刷新失败", "error"); }
    finally { setRefreshing(false); }
  };

  const stop = (e) => e.stopPropagation();
  const mp = (detail && detail.match_parts) || {};
  const titleGame = detail ? Adapt.normalizeGame(mp.game) : "";
  const titleText = detail ? (stripGamePrefix(detail.title || "") || (mp.teamA ? `${mp.teamA} vs ${mp.teamB}` : "跟单详情")) : "跟单详情";
  const titleInner = detail
    ? <>{titleGame ? <GameIcon game={titleGame} base={ASSET_BASE} chip /> : null}<span>{titleText}</span></>
    : "跟单详情";
  let body;
  if (err) body = <div className="empty-cell">加载失败</div>;
  else if (!detail) body = <CenterLoad />;
  else {
    const ev = { game: Adapt.normalizeGame((detail.match_parts || {}).game), teamA: (detail.match_parts || {}).teamA, teamB: (detail.match_parts || {}).teamB, meta: (detail.match_parts || {}).meta, teamLogos: (() => { const tl = detail.team_logos || {}; const m = {}; const mp = detail.match_parts || {}; if (mp.teamA && tl.teamA) m[mp.teamA] = tl.teamA; if (mp.teamB && tl.teamB) m[mp.teamB] = tl.teamB; return m; })(), start: Adapt.fmtClock(detail.match_start_time), end: Adapt.fmtClock(detail.end_date) };
    const outs = detail.outcomes || [];
    const px = prices || detail.outcome_prices || [];
    body = (
      <div className="modal-body">
        <div className="modal-hero">
          <div className="mh-match">
            <Badge tone="accent">{detail.market_type_label || detail.market_type}</Badge>
            <div style={{ marginTop: "var(--sp-3)" }}>
              <TeamLine ev={ev} />
              {(ev.start || ev.end) && <div className="match-times" style={{ marginTop: "6px" }}><span>开始 {ev.start || "—"}</span><span className="dot-sep">·</span><span>截止 {ev.end || "—"}</span></div>}
            </div>
          </div>
          <div className="mh-prices">
            <span className="mh-prices-label">实时盘口</span>
            {outs.map((o, i) => <div className="price-cell" key={o}><span>{o}</span><b>{priceStr(px[i])}</b></div>)}
            <Button size="sm" variant="secondary" disabled={refreshing} iconLeft={refreshing ? <Spinner sm /> : <i data-lucide="refresh-cw" style={{ width: 14, height: 14 }} />} onClick={refreshPrices}>刷新价</Button>
          </div>
        </div>
        {(detail.wallets || []).map((w) => <WalletLegBlock w={w} prices={px} key={w.wallet} />)}
      </div>
    );
  }
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={stop}>
        <div className="modal-head">
          <h2 className="modal-title">
            {detail && detail.event_url
              ? <a className="modal-title-link" href={detail.event_url} target="_blank" rel="noopener noreferrer" title="在 Polymarket 打开该赛事">{titleInner}<i data-lucide="external-link"></i></a>
              : <span className="modal-title-link" style={{ cursor: "default" }}>{titleInner}</span>}
          </h2>
          <button className="modal-close" onClick={onClose} aria-label="关闭"><i data-lucide="x"></i></button>
        </div>
        {body}
      </div>
    </div>
  );
}

/* ---------- Wallet follows modal ---------- */
function WalletFollowsModal({ wallet, onClose }) {
  const [res, setRes] = React.useState(null);
  const [err, setErr] = React.useState(false);
  const [page, setPage] = React.useState(1);
  const SIZE = 20;
  React.useEffect(() => {
    let alive = true;
    Api.walletFollows(wallet, { page, size: SIZE }).then((d) => alive && setRes(d)).catch(() => alive && setErr(true));
    return () => { alive = false; };
  }, [wallet, page]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });
  const stop = (e) => e.stopPropagation();
  const sigs = res ? (res.signals || res.follows || []) : [];
  const total = res ? Adapt.num(res.total) : 0;
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={stop}>
        <div className="modal-head">
          <h2 className="modal-title"><WalletAddress address={wallet} copyable /></h2>
          <button className="modal-close" onClick={onClose} aria-label="关闭"><i data-lucide="x"></i></button>
        </div>
        <div className="modal-body">
          {err ? <div className="empty-cell">加载失败</div> : !res ? <CenterLoad /> : (
            <>
              <div className="tbl-wrap">
                <table className="ps-table">
                  <thead><tr><th>赛事</th><th>方向</th><th>状态</th><th>均价</th><th>结算</th></tr></thead>
                  <tbody>
                    {sigs.map((s, i) => (
                      <tr key={(s.signal_id || i)}>
                        <td><div className="cell-stack"><span className="strong">{s.event_title || s.market_question || Adapt.matchInfo(s).teamA + " vs " + Adapt.matchInfo(s).teamB}</span><span className="muted">{Adapt.fmtClock(s.match_start_time)}</span></div></td>
                        <td>{s.outcome || "—"}</td>
                        <td>{s.status === "open" ? <StatusPill status="live" label="进行中" /> : <Badge tone="neutral">{s.status === "settled" ? "已结算" : s.status === "exited" ? "已退出" : s.status}</Badge>}</td>
                        <td className="num">{priceStr(s.follow_avg_entry_price)}</td>
                        <td className="num">{s.settlement_price != null ? priceStr(s.settlement_price) : "—"}</td>
                      </tr>
                    ))}
                    {!sigs.length && <tr><td colSpan="5" className="empty-cell">暂无跟单记录</td></tr>}
                  </tbody>
                </table>
              </div>
              {total > SIZE && <Pager total={total} pageSize={SIZE} page={page} onChange={setPage} unit="条" />}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/* ============================================================
   Strategy
   ============================================================ */
const STRATEGY_DEFAULTS = {
  usableMode: "all", usableCap: "5000",
  minSignalOn: true, minSignal: "10",
  sizing: "ratio", ratio: "10", ratioCapOn: false, ratioCap: "100",
  fixed: "50", balancePct: "1",
  countOn: false, countMode: "event", count: "10",
  spendOn: false, spendMode: "fixed", spendFixed: "200", spendPct: "5",
};
const clampNum = (v) => String(v).replace(/[^\d.]/g, "");

function strategyDigest(s) {
  const n = (v) => Number(v) || 0;
  const sizing = s.sizing === "ratio"
    ? `比例 ${s.ratio || 0}%${s.ratioCapOn ? `（封顶 ${usdInt(n(s.ratioCap))}）` : ""}`
    : s.sizing === "fixed" ? `固定 ${usdInt(n(s.fixed))}` : `余额 ${s.balancePct || 0}%`;
  const filter = `门槛 ${usdInt(n(s.minSignal))}`;
  const count = s.countOn ? (s.countMode === "event" ? `单场 ${s.count} 笔` : `每钱包 ${s.count} 笔`) : null;
  const spend = s.spendOn ? (s.spendMode === "fixed" ? `单场 ≤ ${usdInt(n(s.spendFixed))}` : `单场 ≤ 余额 ${s.spendPct}%`) : null;
  return { sizing, chips: [filter, count, spend].filter(Boolean) };
}

function NumField({ value, onChange, unit, width = 76, lead, disabled }) {
  return (
    <span className={"num-field" + (disabled ? " is-disabled" : "")}>
      {lead ? <span className="nf-lead">{lead}</span> : null}
      <input value={value} onChange={(e) => onChange(clampNum(e.target.value))} style={{ width }} inputMode="decimal" disabled={disabled} />
      {unit ? <span className="nf-unit">{unit}</span> : null}
    </span>
  );
}
function SizingOption({ id, active, onSelect, title, desc, children, disabled }) {
  return (
    <div className={"opt-card" + (active ? " is-active" : "") + (disabled ? " is-disabled" : "")} onClick={() => !disabled && onSelect(id)} role="radio" aria-checked={active}>
      <div className="opt-head">
        <span className="opt-radio" aria-hidden="true"></span>
        <div className="opt-titles"><span className="opt-title">{title}</span><span className="opt-desc">{desc}</span></div>
      </div>
      {active && children ? <div className="opt-body" onClick={(e) => e.stopPropagation()}>{children}</div> : null}
    </div>
  );
}

function ConfirmModal({ title, body, danger, confirmLabel, onConfirm, onClose, busy }) {
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" style={{ maxWidth: 440 }} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head"><h2 className="modal-title">{title}</h2><button className="modal-close" onClick={onClose}><i data-lucide="x"></i></button></div>
        <div className="modal-body">
          <p style={{ margin: 0, color: "var(--text-secondary)", fontSize: "var(--fs-callout)" }}>{body}</p>
          <div style={{ display: "flex", justifyContent: "flex-end", gap: "var(--sp-3)" }}>
            <Button variant="ghost" onClick={onClose}>取消</Button>
            <Button variant={danger ? "danger" : "primary"} disabled={busy} onClick={onConfirm}>{busy ? <Spinner sm /> : (confirmLabel || "确认")}</Button>
          </div>
        </div>
      </div>
    </div>
  );
}

/* React-owned inline SVG icons for the strategy subtree. lucide.createIcons()
   mutates <i data-lucide> placeholders into <svg>, which corrupts React's DOM
   bookkeeping in this frequently-reordering list (insertBefore crash → white
   screen). Inline SVG keeps every node React-owned. */
function Ico({ n, className }) {
  const p = { width: 16, height: 16, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 2, strokeLinecap: "round", strokeLinejoin: "round", className: className, "aria-hidden": true };
  switch (n) {
    case "chevron-down": return <svg {...p}><path d="m6 9 6 6 6-6" /></svg>;
    case "plus": return <svg {...p}><path d="M5 12h14" /><path d="M12 5v14" /></svg>;
    case "trash-2": return <svg {...p}><path d="M3 6h18" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" /><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /><line x1="10" x2="10" y1="11" y2="17" /><line x1="14" x2="14" y1="11" y2="17" /></svg>;
    case "sparkles": return <svg {...p}><path d="m12 3 1.9 5.8a2 2 0 0 0 1.3 1.3L21 12l-5.8 1.9a2 2 0 0 0-1.3 1.3L12 21l-1.9-5.8a2 2 0 0 0-1.3-1.3L3 12l5.8-1.9a2 2 0 0 0 1.3-1.3z" /></svg>;
    case "circle-alert": return <svg {...p}><circle cx="12" cy="12" r="10" /><line x1="12" x2="12" y1="8" y2="12" /><line x1="12" x2="12.01" y1="16" y2="16" /></svg>;
    case "lock": return <svg {...p}><rect width="18" height="11" x="3" y="11" rx="2" ry="2" /><path d="M7 11V7a5 5 0 0 1 10 0v4" /></svg>;
    case "crosshair": return <svg {...p}><circle cx="12" cy="12" r="10" /><line x1="22" x2="18" y1="12" y2="12" /><line x1="6" x2="2" y1="12" y2="12" /><line x1="12" x2="12" y1="6" y2="2" /><line x1="12" x2="12" y1="22" y2="18" /></svg>;
    case "wallet": return <svg {...p}><path d="M19 7V4a1 1 0 0 0-1-1H5a2 2 0 0 0 0 4h15a1 1 0 0 1 1 1v4h-3a2 2 0 0 0 0 4h3a1 1 0 0 0 1-1v-2a1 1 0 0 0-1-1" /><path d="M3 5v14a2 2 0 0 0 2 2h15a1 1 0 0 0 1-1v-4" /></svg>;
    case "filter": return <svg {...p}><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" /></svg>;
    default: return null;
  }
}

/* compact "overview pipe" nodes for one strategy (kit shape) */
function strategyNodes(s) {
  const n = (v) => Number(v) || 0;
  const dg = strategyDigest(s);
  return [
    { k: "资金池", v: s.usableMode === "all" ? "全部余额" : `上限 ${usdInt(n(s.usableCap))}` },
    { k: "信号门槛", v: s.minSignalOn ? `忽略 < ${usdInt(n(s.minSignal))}` : "不限" },
    { k: "单笔金额", v: dg.sizing, key: true },
    { k: "单场笔数", v: s.countOn ? (s.countMode === "event" ? `整场 ${s.count} 笔` : `每钱包 ${s.count} 笔`) : "不限" },
    { k: "单场投入", v: s.spendOn ? (s.spendMode === "fixed" ? `≤ ${usdInt(n(s.spendFixed))}` : `≤ 余额 ${s.spendPct}%`) : "不限" },
  ];
}
function StrategyPipe({ kit }) {
  return (
    <ol className="so-pipe srow-pipe">
      {strategyNodes(kit).map((nd) => (
        <li key={nd.k} className={"so-node" + (nd.key ? " is-key" : "")}>
          <span className="son-k">{nd.k}</span><span className="son-v">{nd.v}</span>
        </li>
      ))}
    </ol>
  );
}

/* the two-column config form (shared by create + edit) */
function StrategyEditor({ s, up, wallet, locked }) {
  return (
    <div className="cfg-split" style={locked ? { opacity: 0.55, pointerEvents: "none" } : null}>
      <div className="cfg-col">
        <div className="cfg-mini">
          <div className="cm-head"><Ico n="wallet" /><span>可动用资金</span></div>
          <div className="cm-body">
            <SegmentedControl value={s.usableMode} onChange={up("usableMode")} options={[{ value: "all", label: "全部余额" }, { value: "cap", label: "指定上限" }]} />
            {s.usableMode === "cap" ? <NumField value={s.usableCap} onChange={up("usableCap")} unit="USDC" width={84} /> : <span className="mc-note">钱包 {wallet > 0 ? money(wallet) : "未设置"}</span>}
          </div>
        </div>
        <div className="cfg-head"><h3>单笔跟单金额</h3><Badge tone="up" outline>必填</Badge></div>
        <p className="cfg-sub">每个有效信号买入多少 · 三选一，金额向下取整规避下单异常</p>
        <div className="opt-list">
          <SizingOption id="ratio" active={s.sizing === "ratio"} onSelect={up("sizing")} title="按目标比例" desc="跟随目标钱包买入额的固定比例">
            <div className="ratio-rows">
              <span className="rr-lead"><span className="rr-check-slot"></span>跟单比例</span>
              <NumField value={s.ratio} onChange={up("ratio")} unit="%" width={56} />
              <label className="rr-lead is-check" onClick={(e) => e.stopPropagation()}>
                <input type="checkbox" checked={s.ratioCapOn} onChange={(e) => up("ratioCapOn")(e.target.checked)} />单笔封顶
              </label>
              <NumField value={s.ratioCap} onChange={up("ratioCap")} unit="USDC" width={56} disabled={!s.ratioCapOn} />
            </div>
          </SizingOption>
          <SizingOption id="fixed" active={s.sizing === "fixed"} onSelect={up("sizing")} title="固定金额" desc="每笔买入固定金额，与目标下单额无关">
            <div className="ctrl-row"><NumField value={s.fixed} onChange={up("fixed")} unit="USDC" lead="每笔买入" /></div>
          </SizingOption>
          <SizingOption id="balancePct" active={s.sizing === "balancePct"} onSelect={up("sizing")} title="按本金百分比" desc="按当前可动用余额的百分比动态买入">
            <div className="ctrl-row"><NumField value={s.balancePct} onChange={up("balancePct")} unit="%" lead="每笔占用" /></div>
          </SizingOption>
        </div>
      </div>

      <div className="cfg-col">
        <div className="cfg-mini">
          <div className="cm-head"><Ico n="filter" /><span>信号门槛</span></div>
          <div className="cm-body">
            <label className="check-row" onClick={(e) => e.stopPropagation()}>
              <input type="checkbox" checked={s.minSignalOn} onChange={(e) => up("minSignalOn")(e.target.checked)} /><span className="cr-label">启用</span>
            </label>
            <NumField value={s.minSignal} onChange={up("minSignal")} unit="USDC" lead="忽略目标买入 <" width={64} disabled={!s.minSignalOn} />
          </div>
        </div>
        <div className="cfg-head"><h3>单场风控上限</h3></div>
        <p className="cfg-sub">对单场赛事的累计跟单设防 · 两项可独立开启</p>
        <div className="sub-block">
          <div className="switch-row">
            <div className="sr-text"><span className="sr-title">单场笔数上限</span><span className="sr-desc">限制一场赛事累计可跟的笔数</span></div>
            <Switch checked={s.countOn} onChange={(v) => up("countOn")(v)} accent />
          </div>
          {s.countOn ? <div className="sub-controls">
            <div className="ctrl-row"><SegmentedControl value={s.countMode} onChange={up("countMode")} options={[{ value: "event", label: "按赛事合计" }, { value: "wallet", label: "按每个钱包" }]} /><NumField value={s.count} onChange={up("count")} unit="笔" width={58} /></div>
          </div> : null}
        </div>
        <div className="sub-block">
          <div className="switch-row">
            <div className="sr-text"><span className="sr-title">单场投入上限</span><span className="sr-desc">限制一场赛事的累计买入金额</span></div>
            <Switch checked={s.spendOn} onChange={(v) => up("spendOn")(v)} accent />
          </div>
          {s.spendOn ? <div className="sub-controls">
            <div className="ctrl-row"><SegmentedControl value={s.spendMode} onChange={up("spendMode")} options={[{ value: "fixed", label: "固定金额" }, { value: "balancePct", label: "余额百分比" }]} />{s.spendMode === "fixed" ? <NumField value={s.spendFixed} onChange={up("spendFixed")} unit="USDC" width={88} /> : <NumField value={s.spendPct} onChange={up("spendPct")} unit="%" width={58} />}</div>
          </div> : null}
        </div>
      </div>
    </div>
  );
}

/* name + config + save/cancel — used both inline (edit) and for new strategy */
function StrategyRowEditor({ initName, initKit, wallet, saveLocked, saving, takenNames, onSave, onCancel }) {
  const [name, setName] = React.useState(initName || "");
  const [s, setS] = React.useState(() => ({ ...initKit }));
  const up = (k) => (v) => setS((p) => ({ ...p, [k]: v }));
  const issues = Adapt.strategyIssues(s, wallet);
  const nameTrim = name.trim();
  const dup = takenNames.indexOf(nameTrim.toLowerCase()) >= 0;
  const nameErr = !nameTrim ? "请输入策略名称" : dup ? "该名称已存在" : "";
  const ready = !nameErr && issues.length === 0 && !saveLocked;
  return (
    <div className="strat-editor">
      <div className="se-name">
        <label className="se-name-label">策略名称</label>
        <input className="se-name-input" value={name} maxLength={24} placeholder="给这个策略起个名字，例如：稳健跟单"
          onChange={(e) => setName(e.target.value)} />
        {nameErr ? <span className="se-name-err"><Ico n="circle-alert" />{nameErr}</span> : null}
      </div>
      <StrategyEditor s={s} up={up} wallet={wallet} locked={saveLocked} />
      {issues.length ? <div className="so-todo"><Ico n="circle-alert" /> 待完善必填项：{issues.join("、")}</div> : null}
      {saveLocked ? <div className="so-todo"><Ico n="lock" /> 跟单运行中，无法修改生效策略，请先停止跟单</div> : null}
      <div className="se-actions">
        <Button variant="ghost" onClick={onCancel} disabled={saving}>取消</Button>
        <Button variant="primary" disabled={!ready || saving} onClick={() => onSave(nameTrim, s)}>{saving ? <Spinner sm /> : "保存"}</Button>
      </div>
    </div>
  );
}

const MAX_STRATEGIES = 5;
function strategyErrText(e) {
  if (!e) return "操作失败";
  if (e.error === "duplicate_name") return "策略名称已存在";
  if (e.error === "follow_strategy_locked") return "跟单运行中，无法修改生效策略，请先停止";
  if (e.error === "invalid_follow_strategy") return "策略参数不完整，请检查必填项";
  if (e.error === "name_required") return "请输入策略名称";
  if (e.error === "strategy_not_found") return "策略不存在，请刷新后重试";
  return "操作失败";
}

function StrategyPage({ data, merge, toast }) {
  const wallet = data.overview && data.overview.account_balance && data.overview.account_balance.configured
    ? Adapt.num(data.overview.account_balance.balance_usdc) : 0;
  const lib = Adapt.strategyEntries(data.strategies || {}, wallet);
  const list = lib.list;
  const running = data.runner && data.runner.status === "running";
  const stopping = data.runner && data.runner.status === "stopping";
  const locked = running || stopping;

  const [expanded, setExpanded] = React.useState(null); // slug | "__new__" | null
  const [saving, setSaving] = React.useState(false);
  const [busySlug, setBusySlug] = React.useState(null);
  const [delTarget, setDelTarget] = React.useState(null);
  const [deleting, setDeleting] = React.useState(false);

  const refetch = async () => {
    const [ls, st, rn, ov] = await Promise.all([
      Api.strategies(), Api.followStrategy().catch(() => null), Api.runner().catch(() => null), Api.overview().catch(() => null),
    ]);
    const patch = { strategies: ls };
    if (st) patch.strategy = st;
    if (rn) patch.runner = rn;
    if (ov) patch.overview = ov;
    merge(patch);
  };
  const otherNames = (slug) => list.filter((e) => e.slug !== slug).map((e) => e.name.toLowerCase());

  const create = async (name, kit) => {
    setSaving(true);
    try { await Api.createStrategy(name, Adapt.strategyFromKit(kit, wallet)); await refetch(); setExpanded(null); toast("策略已创建", "success"); }
    catch (e) { toast(strategyErrText(e), "error"); }
    finally { setSaving(false); }
  };
  const update = async (slug, name, kit) => {
    setSaving(true);
    try { await Api.updateStrategy(slug, name, Adapt.strategyFromKit(kit, wallet)); await refetch(); setExpanded(null); toast("策略已保存", "success"); }
    catch (e) { toast(strategyErrText(e), "error"); }
    finally { setSaving(false); }
  };
  const activate = async (slug) => {
    setBusySlug(slug);
    try { await Api.activateStrategy(slug); await refetch(); toast("已切换生效策略", "success"); }
    catch (e) { toast(strategyErrText(e), "error"); }
    finally { setBusySlug(null); }
  };
  const remove = async () => {
    if (!delTarget) return;
    setDeleting(true);
    try { await Api.deleteStrategy(delTarget.slug); await refetch(); if (expanded === delTarget.slug) setExpanded(null); setDelTarget(null); toast("策略已删除", "success"); }
    catch (e) { toast(strategyErrText(e), "error"); }
    finally { setDeleting(false); }
  };

  const atMax = list.length >= MAX_STRATEGIES;
  const startNew = () => setExpanded("__new__");
  const toggle = (slug) => setExpanded((cur) => (cur === slug ? null : slug));

  return (
    <div className="page-inner strat-page">
      <div className="strat-toolbar">
        <div className="strat-toolbar-info">
          <span className="stt-count">已保存 {list.length} / {MAX_STRATEGIES}</span>
          {lib.activeSlug ? null : list.length ? <span className="stt-warn"><Ico n="circle-alert" /> 未选定生效策略</span> : null}
        </div>
        <span className={"tb-tip" + (atMax ? " disabled" : "")} data-tip={atMax ? `最多保存 ${MAX_STRATEGIES} 个策略` : undefined}>
          <Button variant="primary" size="sm" iconLeft={<Ico n="plus" />} disabled={atMax || expanded === "__new__"} onClick={startNew}>新增策略</Button>
        </span>
      </div>

      {expanded === "__new__" ? (
        <div className="strat-row is-new is-open">
          <div className="srow-newhead"><Ico n="sparkles" /><span>新建策略</span></div>
          <div className="srow-expand open"><div className="srow-expand-inner">
            <StrategyRowEditor initName="" initKit={{ ...STRATEGY_DEFAULTS }} wallet={wallet} saveLocked={false} saving={saving}
              takenNames={list.map((e) => e.name.toLowerCase())} onSave={create} onCancel={() => setExpanded(null)} />
          </div></div>
        </div>
      ) : null}

      {list.length === 0 && expanded !== "__new__" ? (
        <div className="strat-empty">
          <div className="se-illo"><Ico n="crosshair" /></div>
          <h3 className="se-title">还没有跟单策略</h3>
          <p className="se-desc">创建一个策略来定义单笔金额、信号门槛与单场风控上限。<br />配置完成后即可选定为生效策略并启动跟单。</p>
          <Button variant="primary" iconLeft={<Ico n="plus" />} onClick={startNew}>创建第一个策略</Button>
        </div>
      ) : null}

      {list.length ? (
        <div className="strat-list" role="radiogroup" aria-label="生效策略">
          {list.map((entry) => {
            const open = expanded === entry.slug;
            const rowBusy = busySlug === entry.slug;
            const editLocked = locked && entry.active;
            return (
              <div key={entry.slug} className={"strat-row" + (entry.active ? " is-active" : "") + (open ? " is-open" : "")}>
                <div className="srow-head">
                  <label className="srow-radio" title={locked ? "运行中无法切换生效策略" : "设为生效策略"} onClick={(e) => e.stopPropagation()}>
                    <input type="radio" name="active-strategy" checked={entry.active} disabled={locked || rowBusy}
                      onChange={() => activate(entry.slug)} />
                    <span className="srow-radio-dot" aria-hidden="true"></span>
                  </label>
                  <button className="srow-main" onClick={() => toggle(entry.slug)} aria-expanded={open}>
                    <div className="srow-title">
                      <span className="srow-name">{entry.name}</span>
                      {entry.active ? <Badge tone="up">生效中</Badge> : null}
                      <Ico n="chevron-down" className={"srow-chev" + (open ? " up" : "")} />
                    </div>
                    <StrategyPipe kit={entry.kit} />
                  </button>
                  <span className="srow-sep" aria-hidden="true"></span>
                  <button className="srow-del" disabled={editLocked || rowBusy} title={editLocked ? "运行中无法删除生效策略" : "删除策略"}
                    onClick={() => setDelTarget(entry)}><Ico n="trash-2" /></button>
                </div>
                <div className={"srow-expand" + (open ? " open" : "")}>
                  <div className="srow-expand-inner">
                    {open ? (
                      <StrategyRowEditor key={entry.slug + ":" + entry.updatedAt} initName={entry.name} initKit={entry.kit}
                        wallet={wallet} saveLocked={editLocked} saving={saving} takenNames={otherNames(entry.slug)}
                        onSave={(n, k) => update(entry.slug, n, k)} onCancel={() => setExpanded(null)} />
                    ) : null}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      ) : null}

      {delTarget && <ConfirmModal title="删除策略" danger confirmLabel="确认删除" busy={deleting}
        body={`确定删除策略「${delTarget.name}」吗？此操作不可撤销。`}
        onConfirm={remove} onClose={() => !deleting && setDelTarget(null)} />}
    </div>
  );
}

/* ---------- Progress mask (response-driven; never time-estimated) ----------
   Phase 1 (in-flight, unknown duration): bar creeps asymptotically toward 90%.
   Phase 2 (real completion response): smooth 3s fill to 100%, then dismiss.   */
function ProgressMask({ kind, done, error, label, hint, onClose }) {
  const [pct, setPct] = React.useState(6);
  const [filling, setFilling] = React.useState(false);
  React.useEffect(() => {
    if (done) return undefined;
    const id = setInterval(() => setPct((p) => (p < 90 ? p + (90 - p) * 0.07 : p)), 200);
    return () => clearInterval(id);
  }, [done]);
  React.useEffect(() => {
    if (!done) return undefined;
    const raf = requestAnimationFrame(() => { setFilling(true); setPct(100); });
    const t = setTimeout(() => onClose && onClose(), 3000);
    return () => { cancelAnimationFrame(raf); clearTimeout(t); };
  }, [done, onClose]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });
  const icon = error ? "alert-triangle" : kind === "stop" ? "square" : "radar";
  return (
    <div className="mask-overlay">
      <div className="mask-card">
        <div className={"mask-icon" + (done && !error ? " done" : "") + (error ? " err" : "")}>
          {!done && !error ? <Spinner /> : <i data-lucide={done && !error ? "check" : icon}></i>}
        </div>
        <div className="mask-title">{label}</div>
        <div className="mask-hint">{hint}</div>
        <div className="mask-bar"><span className={"mask-bar-fill" + (filling ? " filling" : "") + (error ? " err" : "")} style={{ width: pct + "%" }}></span></div>
        <div className="mask-pct">{Math.round(pct)}%</div>
      </div>
    </div>
  );
}

/* ============================================================
   Shell + data orchestration
   ============================================================ */
const PAGES = {
  overview: { title: "概览", icon: "layout-dashboard" },
  strategy: { title: "跟单策略", icon: "crosshair" },
  leaderboard: { title: "Leaderboard", icon: "trophy" },
  events: { title: "关注赛事", icon: "swords" },
  follows: { title: "跟单列表", icon: "list-checks" },
};

function useToasts() {
  const [toasts, setToasts] = React.useState([]);
  const idRef = React.useRef(1);
  const push = React.useCallback((msg, kind = "info") => {
    const id = idRef.current++;
    setToasts((t) => [...t, { id, msg, kind }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4200);
  }, []);
  return { toasts, push };
}

function Dashboard({ onLogout, toast }) {
  const [page, setPage] = React.useState("overview");
  const [light, setLight] = React.useState(true);
  const [modal, setModal] = React.useState(null); // {type:'follow', cid} | {type:'wallet', addr}
  const openFollow = React.useCallback((cid) => setModal({ type: "follow", cid }), []);
  const openWallet = React.useCallback((addr) => setModal({ type: "wallet", addr }), []);
  const closeModal = React.useCallback(() => setModal(null), []);

  const [data, setData] = React.useState({
    overview: null, health: null, runner: null, strategy: null, strategies: null,
    wallets: null, events: null, follows: null, refresh: null,
  });
  const merge = React.useCallback((patch) => setData((d) => ({ ...d, ...patch })), []);

  /* ---- response-driven progress masks (sampling / stopping) ---- */
  const [mask, setMask] = React.useState(null); // {kind, done, error, label, hint}
  const maskCancel = React.useRef(false);
  const closeMask = React.useCallback(() => { maskCancel.current = true; setMask(null); }, []);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  const runSample = React.useCallback(async (category, thresholds) => {
    category = category || "esports";
    maskCancel.current = false;
    setMask({ kind: "sample", done: false, error: false, label: "正在采集聪明钱包", hint: "分析链上交易并重建榜单，请稍候…" });
    const catOf = (st) => (st && st.status && st.status[category]) || {};
    let prevStarted = 0;
    try { prevStarted = Adapt.num(catOf(await Api.walletRefreshStatus()).started_at); } catch (e) {}
    try { await Api.walletRefresh(category, thresholds); }
    catch (e) { if (!(e && e.error === "wallet_refresh_running")) { setMask({ kind: "sample", done: true, error: true, label: "采样启动失败", hint: "请稍后重试" }); return; } }
    const start = Date.now();
    while (!maskCancel.current) {
      await sleep(1500);
      let cat; try { cat = catOf(await Api.walletRefreshStatus()); } catch (e) { continue; }
      const fresh = Adapt.num(cat.started_at) >= prevStarted;
      if (fresh && cat.status && cat.status !== "running") {
        const ok = cat.status === "succeeded";
        Promise.all([Api.wallets().catch(() => null), Api.overview().catch(() => null), Api.events().catch(() => null)])
          .then(([w, ov, ev]) => { if (!maskCancel.current) merge({ ...(w ? { wallets: w } : {}), ...(ov ? { overview: ov } : {}), ...(ev ? { events: ev } : {}) }); });
        setMask((m) => (m ? { ...m, done: true, error: !ok, label: ok ? "采样完成" : "采样失败", hint: ok ? "聪明钱包榜单已更新" : "collect 进程返回非零退出码" } : null));
        return;
      }
      if (Date.now() - start > 20 * 60 * 1000) { setMask((m) => (m ? { ...m, done: true, error: true, label: "采样超时", hint: "请检查 collect 日志" } : null)); return; }
    }
  }, [merge]);

  const runStop = React.useCallback(async () => {
    maskCancel.current = false;
    setMask({ kind: "stop", done: false, error: false, label: "正在停止跟单", hint: "等待跟单进程安全退出…" });
    try { await Api.runnerStop(); } catch (e) {}
    const start = Date.now();
    while (!maskCancel.current) {
      await sleep(1200);
      let r; try { r = await Api.runner(); } catch (e) { continue; }
      if (r) merge({ runner: r });
      if (r && r.status === "stopped") {
        setMask((m) => (m ? { ...m, done: true, label: "已停止跟单", hint: "跟单进程已退出" } : null));
        return;
      }
      if (Date.now() - start > 120 * 1000) { setMask((m) => (m ? { ...m, done: true, error: true, label: "停止超时", hint: "进程可能仍在退出" } : null)); return; }
    }
  }, [merge]);

  const runStart = React.useCallback(async () => {
    try {
      await Api.runnerStart();
      const [rn, hh] = await Promise.all([Api.runner().catch(() => null), Api.health().catch(() => null)]);
      merge({ ...(rn ? { runner: rn } : {}), ...(hh ? { health: hh } : {}) });
      toast("跟单已启动", "success");
    } catch (e) {
      toast(e && e.error === "runner_already_running" ? "已在运行中" : (e && e.detail) || "启动失败，请先完成并保存跟单策略", "error");
    }
  }, [merge, toast]);

  /* initial load */
  React.useEffect(() => {
    let alive = true;
    (async () => {
      const safe = (p) => p.catch((e) => { if (e instanceof Api.AuthError) onLogout(); return null; });
      const [overview, health, runner, strategy, strategies, wallets, events, follows, refresh] = await Promise.all([
        safe(Api.overview()), safe(Api.health()), safe(Api.runner()), safe(Api.followStrategy()), safe(Api.strategies()),
        safe(Api.wallets()), safe(Api.events()), safe(Api.follows({ page: 1, size: 25 })),
        safe(Api.walletRefreshStatus()),
      ]);
      if (alive) merge({ overview, health, runner, strategy, strategies, wallets, events, follows, refresh });
    })();
    return () => { alive = false; };
  }, [merge, onLogout]);

  /* live stream + polling fallback */
  React.useEffect(() => {
    let alive = true;
    const refetch = {
      wallets: () => Api.wallets().then((w) => alive && merge({ wallets: w })).catch(() => {}),
      events: () => Api.events().then((e) => alive && merge({ events: e })).catch(() => {}),
      follows: () => Api.follows({ page: 1, size: 25 }).then((f) => alive && merge({ follows: f })).catch(() => {}),
    };
    const onFrame = (frame) => {
      if (!alive || !frame) return;
      const patch = {};
      if (frame.overview) patch.overview = frame.overview;
      if (frame.health) patch.health = frame.health;
      if (frame.runner) patch.runner = frame.runner;
      if (frame.refresh) patch.refresh = frame.refresh;
      if (Object.keys(patch).length) merge(patch);
      if (frame.wallets_dirty) refetch.wallets();
      if (frame.events_dirty) refetch.events();
      if (frame.follows_dirty) refetch.follows();
    };
    let pollTimer = null;
    const startPolling = () => {
      if (pollTimer) return;
      pollTimer = setInterval(() => {
        Api.overview().then((o) => alive && merge({ overview: o })).catch(() => {});
        Api.runner().then((r) => alive && merge({ runner: r })).catch(() => {});
      }, 15000);
    };
    const stream = Api.openStream(onFrame, (status) => { if (status === "error") startPolling(); });
    return () => { alive = false; stream.close(); if (pollTimer) clearInterval(pollTimer); };
  }, [merge]);

  /* theme */
  React.useEffect(() => {
    document.documentElement.setAttribute("data-theme", light ? "light" : "dark");
  }, [light]);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });

  const counts = {
    leaderboard: data.wallets ? (data.wallets.wallets || []).length : undefined,
    events: data.events ? (data.events.events || []).length : undefined,
    follows: data.overview ? (Adapt.num(data.overview.open_signal_count) || undefined) : undefined,
  };
  const runnerStatus = (data.runner && data.runner.status) || "stopped";
  const runnerLive = runnerStatus === "running";
  // start requires an *active* strategy (the configured "active" row the runner reads)
  const strategyReady = !!(data.strategy && data.strategy.configured);
  const stratCount = (data.strategies && data.strategies.strategies || []).length;
  const startTip = strategyReady ? "" : (stratCount ? "无生效策略，请指定" : "请先创建并选定跟单策略");

  const ico = (n) => <i data-lucide={n}></i>;
  let Body;
  if (page === "overview") Body = <OverviewPage data={data} onNav={setPage} onOpenFollow={openFollow} />;
  else if (page === "strategy") Body = <StrategyPage data={data} merge={merge} toast={toast} />;
  else if (page === "leaderboard") Body = <LeaderboardPage data={data} merge={merge} toast={toast} onOpenWallet={openWallet} onSample={runSample} />;
  else if (page === "events") Body = <EventsPage data={data} />;
  else if (page === "follows") Body = <FollowsPage data={data} goStrategy={() => setPage("strategy")} onOpenFollow={openFollow} />;

  return (
    <div className="app-shell" data-theme={light ? "light" : "dark"}>
      <SidebarNav value={page} onChange={setPage} groupLabel="工作台"
        items={[
          { id: "overview", label: "概览", icon: ico("layout-dashboard") },
          { id: "strategy", label: "跟单策略", icon: ico("crosshair") },
          { id: "leaderboard", label: "Leaderboard", icon: ico("trophy"), count: counts.leaderboard },
          { id: "events", label: "关注赛事", icon: ico("swords"), count: counts.events },
          { id: "follows", label: "跟单列表", icon: ico("list-checks"), count: counts.follows },
        ]}
        footer={<div className="theme-toggle"><span>{ico(light ? "sun" : "moon")} {light ? "浅色" : "深色"}</span><Switch checked={!light} onChange={() => setLight((v) => !v)} accent /></div>}
      />
      <div className="app-main">
        <header className="topbar">
          <h1 className="topbar-title">{PAGES[page].title}</h1>
          <div className="topbar-spacer"></div>
          <div className="topbar-actions">
            {runnerLive || runnerStatus === "stopping"
              ? <Button variant="danger" size="sm" className="tb-runbtn" disabled={runnerStatus === "stopping"} iconLeft={<svg viewBox="0 0 24 24" width="13" height="13" fill="currentColor" aria-hidden="true"><rect x="6" y="6" width="12" height="12" rx="2.5" /></svg>} onClick={() => runStop()}>停止跟单</Button>
              : <span className={"tb-tip" + (strategyReady ? "" : " disabled")} data-tip={startTip}>
                  <Button variant="primary" size="sm" className="tb-runbtn" disabled={!strategyReady} iconLeft={<svg viewBox="0 0 24 24" width="13" height="13" fill="currentColor" aria-hidden="true"><path d="M7 4.5v15a1 1 0 0 0 1.5.87l13-7.5a1 1 0 0 0 0-1.74l-13-7.5A1 1 0 0 0 7 4.5z" /></svg>} onClick={() => runStart()}>启动跟单</Button>
                </span>}
            <StatusPill status={runnerLive ? "live" : "idle"} label={runnerLive ? "运行中" : runnerStatus === "stopping" ? "停止中" : "已停止"} extra={runnerLive ? hms(data.health && data.health.uptime_seconds) : undefined} />
            <Button variant="ghost" size="sm" iconLeft={ico("log-out")} onClick={onLogout}>退出</Button>
          </div>
        </header>
        <div className="page-scroll">{Body}</div>
      </div>
      {modal && modal.type === "follow" && <FollowDetailModal cid={modal.cid} onClose={closeModal} toast={toast} />}
      {modal && modal.type === "wallet" && <WalletFollowsModal wallet={modal.addr} onClose={closeModal} />}
      {mask && <ProgressMask kind={mask.kind} done={mask.done} error={mask.error} label={mask.label} hint={mask.hint} onClose={closeMask} />}
    </div>
  );
}

/* ============================================================
   Auth gate
   ============================================================ */
function LoginPanel({ onSuccess, toast }) {
  const [u, setU] = React.useState("");
  const [p, setP] = React.useState("");
  const [err, setErr] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  React.useEffect(() => { window.lucide && window.lucide.createIcons(); });
  const submit = async (e) => {
    e.preventDefault();
    setBusy(true); setErr("");
    try { await Api.login(u, p); onSuccess(); }
    catch (ex) { setErr(ex && ex.error === "invalid_login" ? "用户名或密码错误" : "登录失败，请重试"); }
    finally { setBusy(false); }
  };
  return (
    <main className="login-shell">
      <section className="login-panel">
        <div className="login-brand">
          <span className="lb-mark"><i data-lucide="crosshair"></i></span>
          <h1>Polymarket Sniper</h1>
          <p>聪明钱跟单控制台</p>
        </div>
        <form className="login-form" onSubmit={submit}>
          <label className="login-field">
            <span className="login-label">用户名</span>
            <input className="login-input" value={u} onChange={(e) => setU(e.target.value)} autoComplete="username" required />
          </label>
          <label className="login-field">
            <span className="login-label">密码</span>
            <input className="login-input" type="password" value={p} onChange={(e) => setP(e.target.value)} autoComplete="current-password" required />
          </label>
          {err && <p className="login-error" role="alert">{err}</p>}
          <Button variant="primary" size="lg" type="submit" disabled={busy}>{busy ? <Spinner sm /> : "登录"}</Button>
        </form>
      </section>
    </main>
  );
}

function App() {
  const [auth, setAuth] = React.useState("checking"); // checking | out | in
  const { toasts, push } = useToasts();
  React.useEffect(() => {
    let alive = true;
    Api.health()
      .then(() => { if (alive) setAuth("in"); })
      .catch((e) => { if (alive) setAuth(e instanceof Api.AuthError ? "out" : "in"); });
    return () => { alive = false; };
  }, []);
  const logout = React.useCallback(() => { Api.logout().catch(() => {}); setAuth("out"); }, []);

  return (
    <>
      {auth === "checking" && <main className="boot-shell"><Spinner /></main>}
      {auth === "out" && <LoginPanel onSuccess={() => setAuth("in")} toast={push} />}
      {auth === "in" && <Dashboard onLogout={logout} toast={push} />}
      <div className="toast-stack">
        {toasts.map((t) => (
          <div key={t.id} className={"toast " + (t.kind === "error" ? "err" : t.kind === "success" ? "ok" : "")}>
            <span className="t-dot" /><span>{t.msg}</span>
          </div>
        ))}
      </div>
    </>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
setTimeout(() => window.lucide && window.lucide.createIcons(), 80);
