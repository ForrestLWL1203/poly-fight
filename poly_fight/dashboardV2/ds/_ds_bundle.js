/* @ds-bundle: {"format":3,"namespace":"PolySniperDesignSystem_8d05e5","components":[{"name":"Badge","sourcePath":"components/core/Badge.jsx"},{"name":"Button","sourcePath":"components/core/Button.jsx"},{"name":"Card","sourcePath":"components/core/Card.jsx"},{"name":"IconButton","sourcePath":"components/core/IconButton.jsx"},{"name":"CategoryDonut","sourcePath":"components/data/CategoryDonut.jsx"},{"name":"GameIcon","sourcePath":"components/data/GameIcon.jsx"},{"name":"RankBadge","sourcePath":"components/data/RankBadge.jsx"},{"name":"StatTile","sourcePath":"components/data/StatTile.jsx"},{"name":"TrendValue","sourcePath":"components/data/TrendValue.jsx"},{"name":"WalletAddress","sourcePath":"components/data/WalletAddress.jsx"},{"name":"WinRateRing","sourcePath":"components/data/WinRateRing.jsx"},{"name":"StatusPill","sourcePath":"components/feedback/StatusPill.jsx"},{"name":"Input","sourcePath":"components/forms/Input.jsx"},{"name":"SegmentedControl","sourcePath":"components/forms/SegmentedControl.jsx"},{"name":"Switch","sourcePath":"components/forms/Switch.jsx"},{"name":"SidebarNav","sourcePath":"components/navigation/SidebarNav.jsx"},{"name":"Tabs","sourcePath":"components/navigation/Tabs.jsx"}],"sourceHashes":{"app.jsx":"ed13913d3c18","components/core/Badge.jsx":"171d96df0ac5","components/core/Button.jsx":"74b5f492f1d5","components/core/Card.jsx":"cc28091483ef","components/core/IconButton.jsx":"4338d86db286","components/data/CategoryDonut.jsx":"6f6672ec61e1","components/data/GameIcon.jsx":"5d25d7a29a9c","components/data/RankBadge.jsx":"594596ec1ed3","components/data/StatTile.jsx":"8003fa487ab2","components/data/TrendValue.jsx":"aab73a1f950e","components/data/WalletAddress.jsx":"5b004cad2968","components/data/WinRateRing.jsx":"6e2af54d6f6e","components/feedback/StatusPill.jsx":"d219676456f1","components/forms/Input.jsx":"0d6e9a5c88a2","components/forms/SegmentedControl.jsx":"e039e5cce923","components/forms/Switch.jsx":"491cac6ff192","components/navigation/SidebarNav.jsx":"0cb54f631d92","components/navigation/Tabs.jsx":"5d1bb575ba70","ui_kits/dashboard/app.jsx":"d100b9773bea","ui_kits/dashboard/data.js":"9745fb48b6d1"},"inlinedExternals":[],"unexposedExports":[]} */

(() => {

const __ds_ns = (window.PolySniperDesignSystem_8d05e5 = window.PolySniperDesignSystem_8d05e5 || {});

const __ds_scope = {};

(__ds_ns.__errors = __ds_ns.__errors || []);

// app.jsx
try { (() => {
/* Poly Sniper dashboard — single-scope app (avoids cross-file babel scope collisions). */
const {
  SidebarNav,
  Tabs,
  Card,
  Button,
  IconButton,
  Switch,
  SegmentedControl,
  StatTile,
  TrendValue,
  RankBadge,
  WalletAddress,
  GameIcon,
  Badge,
  StatusPill,
  WinRateRing,
  CategoryDonut
} = window.PolySniperDesignSystem_8d05e5;
const D = window.PS_DATA;
const C = D.teamColors;
const ASSET_BASE = "../../assets"; // game logos live at project-root assets/

/* ---------- formatters & helpers ---------- */
const money = n => (n < 0 ? "-$" : "$") + Math.abs(n).toLocaleString(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const signedMoney = n => (n > 0 ? "+$" : n < 0 ? "-$" : "$") + Math.abs(n).toLocaleString(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const compactMoney = n => Math.abs(n) >= 1000 ? "$" + (n / 1000).toFixed(1) + "K" : "$" + n.toFixed(0);
const usdInt = n => "$" + Math.floor(Math.max(0, Number(n) || 0)).toLocaleString();
const pnlClass = n => n > 0 ? "pnl-up" : n < 0 ? "pnl-down" : "pnl-flat";

/* ---------- Pager (Apple-minimal, functional) ---------- */
function pageList(cur, total) {
  if (total <= 7) return Array.from({
    length: total
  }, (_, i) => i + 1);
  const s = new Set([1, total, cur, cur - 1, cur + 1]);
  if (cur <= 3) [2, 3, 4].forEach(n => s.add(n));
  if (cur >= total - 2) [total - 1, total - 2, total - 3].forEach(n => s.add(n));
  const arr = [...s].filter(n => n >= 1 && n <= total).sort((a, b) => a - b);
  const out = [];
  arr.forEach((n, i) => {
    if (i > 0 && n - arr[i - 1] > 1) out.push("…");
    out.push(n);
  });
  return out;
}
const Chevron = ({
  dir
}) => /*#__PURE__*/React.createElement("svg", {
  viewBox: "0 0 24 24",
  width: "16",
  height: "16",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: "2.2",
  strokeLinecap: "round",
  strokeLinejoin: "round",
  "aria-hidden": "true"
}, dir === "left" ? /*#__PURE__*/React.createElement("path", {
  d: "M15 18l-6-6 6-6"
}) : /*#__PURE__*/React.createElement("path", {
  d: "M9 18l6-6-6-6"
}));
function Pager({
  total,
  pageSize,
  page,
  onChange,
  unit = "条"
}) {
  const pages = Math.max(1, Math.ceil(total / pageSize));
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = Math.min(total, page * pageSize);
  return /*#__PURE__*/React.createElement("div", {
    className: "pager"
  }, /*#__PURE__*/React.createElement("span", {
    className: "pager-range"
  }, from, "\u2013", to, /*#__PURE__*/React.createElement("em", null, " / \u5171 ", total, " ", unit)), /*#__PURE__*/React.createElement("div", {
    className: "pager-nav"
  }, /*#__PURE__*/React.createElement("button", {
    className: "pg-btn pg-arrow",
    disabled: page <= 1,
    onClick: () => onChange(page - 1),
    "aria-label": "\u4E0A\u4E00\u9875"
  }, /*#__PURE__*/React.createElement(Chevron, {
    dir: "left"
  })), pageList(page, pages).map((n, i) => n === "…" ? /*#__PURE__*/React.createElement("span", {
    key: "e" + i,
    className: "pg-ellipsis"
  }, "\u2026") : /*#__PURE__*/React.createElement("button", {
    key: n,
    className: "pg-btn" + (n === page ? " is-active" : ""),
    onClick: () => onChange(n),
    "aria-current": n === page ? "page" : undefined
  }, n)), /*#__PURE__*/React.createElement("button", {
    className: "pg-btn pg-arrow",
    disabled: page >= pages,
    onClick: () => onChange(page + 1),
    "aria-label": "\u4E0B\u4E00\u9875"
  }, /*#__PURE__*/React.createElement(Chevron, {
    dir: "right"
  }))));
}
const initials = name => {
  const p = name.split(/\s+/).filter(Boolean);
  return (p.length === 1 ? p[0].slice(0, 2) : p[0][0] + p[1][0]).toUpperCase();
};
function TeamMonogram({
  name,
  size = 26
}) {
  return /*#__PURE__*/React.createElement("span", {
    className: "team-mono",
    style: {
      width: size,
      height: size,
      "--team": C[name] || "var(--accent)"
    }
  }, initials(name));
}
function TeamLine({
  ev,
  size = 26
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "team-line"
  }, /*#__PURE__*/React.createElement("span", {
    className: "team"
  }, /*#__PURE__*/React.createElement(TeamMonogram, {
    name: ev.teamA,
    size: size
  }), /*#__PURE__*/React.createElement("span", {
    className: "team-name"
  }, ev.teamA)), /*#__PURE__*/React.createElement("span", {
    className: "vs"
  }, "vs"), /*#__PURE__*/React.createElement("span", {
    className: "team"
  }, /*#__PURE__*/React.createElement(TeamMonogram, {
    name: ev.teamB,
    size: size
  }), /*#__PURE__*/React.createElement("span", {
    className: "team-name"
  }, ev.teamB)));
}
function MatchCell({
  ev
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "match-cell"
  }, /*#__PURE__*/React.createElement("div", {
    className: "match-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: ev.game,
    base: ASSET_BASE,
    chip: true
  }), /*#__PURE__*/React.createElement("span", {
    className: "match-meta"
  }, ev.meta)), /*#__PURE__*/React.createElement(TeamLine, {
    ev: ev
  }), /*#__PURE__*/React.createElement("div", {
    className: "match-times"
  }, /*#__PURE__*/React.createElement("span", null, "\u5F00\u59CB ", ev.start), /*#__PURE__*/React.createElement("span", {
    className: "dot-sep"
  }, "\xB7"), /*#__PURE__*/React.createElement("span", null, "\u622A\u6B62 ", ev.end)));
}
function EquityArea({
  points,
  width = 560,
  height = 120
}) {
  const min = Math.min(...points),
    max = Math.max(...points),
    span = max - min || 1;
  const stepX = width / (points.length - 1);
  const xy = points.map((p, i) => [i * stepX, height - (p - min) / span * (height - 12) - 6]);
  const line = xy.map((p, i) => (i === 0 ? "M" : "L") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  const area = line + ` L ${width} ${height} L 0 ${height} Z`;
  const last = xy[xy.length - 1];
  return /*#__PURE__*/React.createElement("svg", {
    className: "equity-svg",
    viewBox: `0 0 ${width} ${height}`,
    preserveAspectRatio: "none",
    width: "100%",
    height: height
  }, /*#__PURE__*/React.createElement("defs", null, /*#__PURE__*/React.createElement("linearGradient", {
    id: "eqFill",
    x1: "0",
    y1: "0",
    x2: "0",
    y2: "1"
  }, /*#__PURE__*/React.createElement("stop", {
    offset: "0%",
    stopColor: "rgba(31,157,87,0.28)"
  }), /*#__PURE__*/React.createElement("stop", {
    offset: "100%",
    stopColor: "rgba(31,157,87,0)"
  })), /*#__PURE__*/React.createElement("linearGradient", {
    id: "eqLine",
    x1: "0",
    y1: "0",
    x2: "1",
    y2: "0"
  }, /*#__PURE__*/React.createElement("stop", {
    offset: "0%",
    stopColor: "#ff8a5f"
  }), /*#__PURE__*/React.createElement("stop", {
    offset: "100%",
    stopColor: "#1f9d57"
  }))), /*#__PURE__*/React.createElement("path", {
    d: area,
    fill: "url(#eqFill)"
  }), /*#__PURE__*/React.createElement("path", {
    d: line,
    fill: "none",
    stroke: "url(#eqLine)",
    strokeWidth: "2.5",
    strokeLinecap: "round",
    strokeLinejoin: "round"
  }), /*#__PURE__*/React.createElement("circle", {
    cx: last[0],
    cy: last[1],
    r: "4",
    fill: "#1f9d57"
  }), /*#__PURE__*/React.createElement("circle", {
    cx: last[0],
    cy: last[1],
    r: "8",
    fill: "rgba(31,157,87,0.22)"
  }));
}
function SplitBar({
  a,
  b
}) {
  const total = a + b || 1;
  return /*#__PURE__*/React.createElement("div", {
    className: "split-bar",
    title: `${a} : ${b}`
  }, /*#__PURE__*/React.createElement("span", {
    className: "split-a",
    style: {
      width: a / total * 100 + "%"
    }
  }), /*#__PURE__*/React.createElement("span", {
    className: "split-b",
    style: {
      width: b / total * 100 + "%"
    }
  }));
}
const qualityBadge = q => q === "clean" ? /*#__PURE__*/React.createElement(Badge, {
  tone: "up"
}, "\u5355\u5411") : q === "contested" ? /*#__PURE__*/React.createElement(Badge, {
  tone: "warn"
}, "\u5206\u6B67") : /*#__PURE__*/React.createElement(Badge, {
  tone: "warn",
  outline: true
}, "\u53CC\u8FB9");

/* ---------- Overview ---------- */
function OverviewPage({
  onNav
}) {
  const o = D.overview;
  const nav = onNav || (() => {});
  const [distMetric, setDistMetric] = React.useState("count");
  const ft = D.followTypes;
  const distSegments = ft.segments.map(s => ({
    ...s,
    value: distMetric === "stake" ? s.stake : s.value
  }));
  const distCenter = distMetric === "stake" ? compactMoney(ft.totalStake) : ft.total;
  const distLabel = distMetric === "stake" ? "总投入" : "跟单笔数";
  const distTotal = distSegments.reduce((a, s) => a + (s.value || 0), 0) || 1;
  const distMarkets = [...new Set(distSegments.map(s => s.label))];
  const distMap = {};
  distSegments.forEach(s => {
    distMap[s.group + "|" + s.label] = s;
  });
  const distGames = [...new Set(distSegments.map(s => s.group))].map(group => ({
    group,
    gameId: (distSegments.find(s => s.group === group) || {}).gameId
  }));
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-grid"
  }, /*#__PURE__*/React.createElement(Card, {
    glow: true,
    pad: "lg",
    className: "ov-herocard"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-hero"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-hero-top"
  }, /*#__PURE__*/React.createElement(StatTile, {
    size: "lg",
    tone: "up",
    label: "\u5DF2\u7ED3\u7B97\u76C8\u4E8F",
    value: signedMoney(o.realizedPnl),
    delta: /*#__PURE__*/React.createElement(TrendValue, {
      value: o.realizedRoi,
      percent: true,
      chip: true
    }),
    sub: `累计投入 ${money(o.totalStake)}`
  }), /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    extra: "02:14:08"
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-metricbar"
  }, /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5DF2\u7ED3\u7B97 ROI"), /*#__PURE__*/React.createElement("b", {
    className: "pnl-up"
  }, "+", o.realizedRoi, "%")), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u7ED3\u7B97\u573A\u6B21"), /*#__PURE__*/React.createElement("b", null, o.settledCount)), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5F53\u524D\u6301\u4ED3"), /*#__PURE__*/React.createElement("b", null, money(o.openExposure))), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u94B1\u5305\u4F59\u989D"), /*#__PURE__*/React.createElement("b", null, money(o.walletBalance)))), /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist-head"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u76D8\u53E3\u7ED3\u6784"), /*#__PURE__*/React.createElement("h3", {
    className: "ov-herodist-title"
  }, "\u5386\u53F2\u8DDF\u5355\u7C7B\u578B\u5206\u5E03")), /*#__PURE__*/React.createElement(SegmentedControl, {
    value: distMetric,
    onChange: setDistMetric,
    options: [{
      value: "count",
      label: "按笔数"
    }, {
      value: "stake",
      label: "按金额"
    }]
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist-body"
  }, /*#__PURE__*/React.createElement(CategoryDonut, {
    size: 112,
    thickness: 18,
    centerValue: distCenter,
    centerLabel: distLabel,
    segments: distSegments
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-distmatrix"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-dm-row ov-dm-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-dm-game"
  }), distMarkets.map(m => /*#__PURE__*/React.createElement("span", {
    key: m,
    className: "ov-dm-cell"
  }, m))), distGames.map(g => /*#__PURE__*/React.createElement("div", {
    key: g.group,
    className: "ov-dm-row"
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-dm-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: g.gameId,
    size: "sm",
    base: ASSET_BASE
  }), " ", g.group), distMarkets.map(m => {
    const s = distMap[g.group + "|" + m];
    return /*#__PURE__*/React.createElement("span", {
      key: m,
      className: "ov-dm-cell"
    }, /*#__PURE__*/React.createElement("i", {
      className: "ov-dm-sw",
      style: {
        background: s ? s.color : "transparent"
      }
    }), s ? Math.round(s.value / distTotal * 100) : 0, "%");
  })))))))), /*#__PURE__*/React.createElement(Card, {
    className: "ov-rightcard"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-twostat"
  }, /*#__PURE__*/React.createElement(StatTile, {
    label: "\u76D1\u63A7\u8D5B\u4E8B",
    value: /*#__PURE__*/React.createElement("button", {
      type: "button",
      className: "ov-link-num",
      onClick: () => nav("events")
    }, o.watchedEvents, /*#__PURE__*/React.createElement("i", {
      "data-lucide": "arrow-up-right"
    })),
    sub: "esports"
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-twostat-div"
  }), /*#__PURE__*/React.createElement(StatTile, {
    label: "\u8FDB\u884C\u4E2D\u8DDF\u5355",
    tone: "gradient",
    value: /*#__PURE__*/React.createElement("button", {
      type: "button",
      className: "ov-link-num",
      onClick: () => nav("follows")
    }, o.openFollows, /*#__PURE__*/React.createElement("i", {
      "data-lucide": "arrow-up-right"
    })),
    sub: `${o.openByGame.length} 个项目`
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-openlist"
  }, o.openByGame.map(g => /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: g.game,
    className: "ov-openrow",
    onClick: () => nav("follows")
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-openrow-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: g.game,
    size: "sm",
    base: ASSET_BASE
  }), " ", g.name), /*#__PURE__*/React.createElement("span", {
    className: "ov-openrow-count"
  }, g.count, " \u573A")))), /*#__PURE__*/React.createElement("div", {
    className: "ov-qualityblock"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-qualityblock-head"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u76D8\u53E3\u7ED3\u6784"), /*#__PURE__*/React.createElement("h3", {
    className: "ov-qtitle"
  }, "\u8DDF\u5355\u8D28\u91CF")), /*#__PURE__*/React.createElement("div", {
    className: "ov-quality"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-qcell"
  }, /*#__PURE__*/React.createElement("span", {
    className: "qv pnl-flat"
  }, o.cleanCount), /*#__PURE__*/React.createElement("span", {
    className: "ql"
  }, "\u5355\u5411\u76D8"), /*#__PURE__*/React.createElement("small", null, "\u65E0\u53CC\u8FB9 / \u5206\u6B67")), /*#__PURE__*/React.createElement("div", {
    className: "ov-qcell"
  }, /*#__PURE__*/React.createElement("span", {
    className: "qv",
    style: {
      color: "var(--status-warn)"
    }
  }, o.twoSidedCount + o.disagreementCount), /*#__PURE__*/React.createElement("span", {
    className: "ql"
  }, "\u53CC\u8FB9 / \u5206\u6B67\u76D8"), /*#__PURE__*/React.createElement("small", null, "\u53CC\u8FB9 ", o.twoSidedCount, " \xB7 \u5206\u6B67 ", o.disagreementCount)))))), /*#__PURE__*/React.createElement(Card, {
    eyebrow: "\u8DDF\u5355\u80DC\u7387",
    title: "\u6574\u4F53\u4E0E\u5206\u9879\u76EE\u8868\u73B0"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-hero"
  }, /*#__PURE__*/React.createElement(WinRateRing, {
    size: "lg",
    wins: o.winRate.wins,
    losses: o.winRate.losses,
    label: "\u80DC\u7387",
    legend: true
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-hero-meta"
  }, /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u603B\u573A\u6B21"), /*#__PURE__*/React.createElement("b", null, o.winRate.wins + o.winRate.losses)), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5DF2\u7ED3\u7B97\u76C8\u4E8F"), /*#__PURE__*/React.createElement("b", {
    className: "pnl-up"
  }, signedMoney(o.realizedPnl))))), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-divider"
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-games"
  }, D.winRates.map(g => /*#__PURE__*/React.createElement(WinRateRing, {
    key: g.game,
    size: "sm",
    wins: g.wins,
    losses: g.losses,
    label: "\u80DC\u7387",
    caption: /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(GameIcon, {
      game: g.game,
      size: "sm",
      base: ASSET_BASE
    }), " ", g.name)
  }))))), /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-4)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "sec-head",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("h2", {
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, "\u6700\u8FD1\u8DDF\u5355"), /*#__PURE__*/React.createElement(Badge, {
    tone: "accent"
  }, D.follows.length, " \u6761"))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305"), /*#__PURE__*/React.createElement("th", null, "\u6295\u5165"), /*#__PURE__*/React.createElement("th", null, "\u76C8\u4E8F"), /*#__PURE__*/React.createElement("th", null, "\u8D28\u91CF"))), /*#__PURE__*/React.createElement("tbody", null, D.follows.slice(0, 5).map(f => /*#__PURE__*/React.createElement("tr", {
    key: f.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: f
  })), /*#__PURE__*/React.createElement("td", null, f.status === "open" ? /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }) : /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, f.wallets), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, money(f.stake)), /*#__PURE__*/React.createElement("td", {
    className: pnlClass(f.pnl)
  }, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong"
  }, signedMoney(f.pnl)), f.pnlKind === "unrealized" && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u5B9E\u73B0"))), /*#__PURE__*/React.createElement("td", null, qualityBadge(f.quality)))))))));
}

/* ---------- Leaderboard ---------- */
function LeaderboardPage() {
  const [view, setView] = React.useState("active");
  const [favs, setFavs] = React.useState(() => {
    const s = {};
    D.wallets.forEach(w => s[w.addr] = w.fav);
    return s;
  });
  const toggleFav = addr => setFavs(p => ({
    ...p,
    [addr]: !p[addr]
  }));
  const [isolated, setIsolated] = React.useState({});
  const isolate = w => setIsolated(p => ({
    ...p,
    [w.addr]: {
      reason: "手动隔离",
      reasonTime: "刚刚"
    }
  }));
  const restore = addr => setIsolated(p => {
    const n = {
      ...p
    };
    delete n[addr];
    return n;
  });
  const activeWallets = D.wallets.filter(w => !isolated[w.addr]);
  const manualQ = D.wallets.filter(w => isolated[w.addr]).map(w => ({
    ...w,
    manual: true,
    reason: isolated[w.addr].reason,
    reasonTime: isolated[w.addr].reasonTime
  }));
  const quarantinedRows = [...manualQ, ...D.quarantined];
  const favRows = activeWallets.filter(w => favs[w.addr]);
  const rows = view === "quarantined" ? quarantinedRows : view === "favorite" ? favRows : activeWallets;
  const q = view === "quarantined";
  const PAGE = 5;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [view]);
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: view,
    onChange: setView,
    options: [{
      value: "active",
      label: "活跃",
      count: activeWallets.length
    }, {
      value: "favorite",
      label: "收藏",
      count: favRows.length
    }, {
      value: "quarantined",
      label: "隔离",
      count: quarantinedRows.length
    }]
  }), /*#__PURE__*/React.createElement("div", {
    className: "sec-actions"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sec-sub",
    style: {
      marginRight: 4
    }
  }, "\u66F4\u65B0 14:08 \xB7 30 \u4E2A\u6838\u5FC3 A \u7EA7"), /*#__PURE__*/React.createElement(Button, {
    variant: "primary",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "radar",
      style: {
        width: 16,
        height: 16
      }
    })
  }, "\u91C7\u6837\u94B1\u5305")))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, !q && /*#__PURE__*/React.createElement("th", null), /*#__PURE__*/React.createElement("th", null, "Rank"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305"), q && /*#__PURE__*/React.createElement("th", null, "\u9694\u79BB\u539F\u56E0"), /*#__PURE__*/React.createElement("th", null, "\u8BC4\u5206"), /*#__PURE__*/React.createElement("th", null, "\u4E13\u7CBE ROI"), /*#__PURE__*/React.createElement("th", null, "\u573A\u5747\u4EA4\u6613\u989D"), /*#__PURE__*/React.createElement("th", null, "\u8FD1\u671F"), /*#__PURE__*/React.createElement("th", null, "\u4E13\u7CBE"), !q && /*#__PURE__*/React.createElement("th", null, "\u8DDF\u5355\u80DC\u8D1F"), !q && /*#__PURE__*/React.createElement("th", null, "\u8DDF\u5355 PnL"), /*#__PURE__*/React.createElement("th", null, "\u6700\u540E\u4EA4\u6613"), /*#__PURE__*/React.createElement("th", null))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map((w, i) => /*#__PURE__*/React.createElement("tr", {
    key: w.addr,
    className: "clickable"
  }, !q && /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("button", {
    className: "fav-btn" + (favs[w.addr] ? " on" : ""),
    onClick: e => {
      e.stopPropagation();
      toggleFav(w.addr);
    },
    "aria-label": "\u6536\u85CF"
  }, favs[w.addr] ? "★" : "☆")), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(RankBadge, {
    rank: w.rank || (cur - 1) * PAGE + i + 1
  })), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(WalletAddress, {
    address: w.addr,
    copyable: false
  })), q && /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong",
    style: {
      color: "var(--status-warn)"
    }
  }, w.reason), /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, w.reasonTime))), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, w.score), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: pnlClass(w.roi) + " strong"
  }, w.roi > 0 ? "+" : "", w.roi, "%"), w.overallRoi != null && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u5168\u90E8 +", w.overallRoi, "%"))), /*#__PURE__*/React.createElement("td", {
    className: "num",
    title: money(w.avgCash)
  }, compactMoney(w.avgCash)), /*#__PURE__*/React.createElement("td", null, w.recent != null ? /*#__PURE__*/React.createElement(TrendValue, {
    value: w.recent,
    percent: true
  }) : /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u2013")), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "scope-list"
  }, w.scope.map((s, j) => /*#__PURE__*/React.createElement("span", {
    key: j,
    className: "scope-item"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: s.game,
    base: ASSET_BASE,
    size: "sm"
  }), /*#__PURE__*/React.createElement("span", null, s.market))))), !q && /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, w.followRec), !q && /*#__PURE__*/React.createElement("td", {
    className: pnlClass(w.followPnl) + " num strong"
  }, signedMoney(w.followPnl)), /*#__PURE__*/React.createElement("td", {
    className: "muted"
  }, w.lastTrade), /*#__PURE__*/React.createElement("td", {
    className: "row-action"
  }, !q && /*#__PURE__*/React.createElement(Button, {
    variant: "danger",
    size: "sm",
    className: "tbl-action danger",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "circle-minus",
      style: {
        width: 13,
        height: 13
      }
    }),
    onClick: e => {
      e.stopPropagation();
      isolate(w);
    }
  }, "\u9694\u79BB"), q && w.manual && /*#__PURE__*/React.createElement(Button, {
    variant: "secondary",
    size: "sm",
    className: "tbl-action restore",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "rotate-ccw",
      style: {
        width: 13,
        height: 13
      },
      "data-comment-anchor": "1a70becd41-i-311-118"
    }),
    onClick: e => {
      e.stopPropagation();
      restore(w.addr);
    }
  }, "\u6062\u590D")))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "12",
    className: "empty-cell"
  }, "\u6682\u65E0", view === "favorite" ? "收藏" : view === "quarantined" ? "隔离" : "活跃", "\u94B1\u5305"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u94B1\u5305"
  }))));
}

/* ---------- Events ---------- */
function EventsPage() {
  const [tab, setTab] = React.useState("active");
  const [game, setGame] = React.useState("all");
  const base = tab === "archive" ? D.archive : D.events;
  const rows = game === "all" ? base : base.filter(e => e.game === game);
  const archive = tab === "archive";
  const PAGE = 4;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [tab, game]);
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement(Tabs, {
    value: tab,
    onChange: setTab,
    tabs: [{
      id: "active",
      label: "进行中 / 即将开始",
      count: D.events.length
    }, {
      id: "archive",
      label: "已结算",
      count: D.archive.length
    }]
  }), /*#__PURE__*/React.createElement("div", {
    className: "filter-group"
  }, /*#__PURE__*/React.createElement("label", {
    htmlFor: "game-f"
  }, "\u9879\u76EE"), /*#__PURE__*/React.createElement("select", {
    id: "game-f",
    className: "ps-select",
    value: game,
    onChange: e => setGame(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: "all"
  }, "\u5168\u90E8"), /*#__PURE__*/React.createElement("option", {
    value: "dota2"
  }, "Dota 2"), /*#__PURE__*/React.createElement("option", {
    value: "cs2"
  }, "CS2"), /*#__PURE__*/React.createElement("option", {
    value: "lol"
  }, "LoL"), /*#__PURE__*/React.createElement("option", {
    value: "valorant"
  }, "Valorant"))))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, archive ? "结算 PNL" : "跟单情况 (A : B)"))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map(e => /*#__PURE__*/React.createElement("tr", {
    key: e.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: e
  })), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "evt-status"
  }, e.status === "live" && /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }), e.status === "upcoming" && /*#__PURE__*/React.createElement(Badge, {
    tone: "accent",
    dot: true
  }, "\u5373\u5C06\u5F00\u59CB"), e.status === "settled" && /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97"), e.countdown && !archive && /*#__PURE__*/React.createElement("span", {
    className: "evt-count"
  }, e.countdown))), /*#__PURE__*/React.createElement("td", null, archive ? /*#__PURE__*/React.createElement("span", {
    className: pnlClass(e.pnl) + " strong num",
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, signedMoney(e.pnl)) : /*#__PURE__*/React.createElement("div", {
    className: "follow-count-line"
  }, /*#__PURE__*/React.createElement(SplitBar, {
    a: e.followA,
    b: e.followB
  }), /*#__PURE__*/React.createElement("span", null, /*#__PURE__*/React.createElement("b", {
    className: "ca"
  }, e.followA), " : ", /*#__PURE__*/React.createElement("b", {
    className: "cb"
  }, e.followB)))))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "3",
    className: "empty-cell"
  }, "\u5F53\u524D\u7A97\u53E3\u6682\u65E0\u76D1\u63A7\u8D5B\u4E8B"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u8D5B\u4E8B"
  }))));
}

/* ---------- Follows ---------- */
function FollowsPage({
  strategy: s,
  goStrategy
}) {
  const [status, setStatus] = React.useState("all");
  const rows = status === "all" ? D.follows : D.follows.filter(f => f.status === status);
  const dg = s ? strategyDigest(s) : null;
  const PAGE = 4;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [status]);
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, dg ? /*#__PURE__*/React.createElement("div", {
    className: "strat-banner"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sb-left"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sb-icon"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "crosshair"
  })), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "sb-k"
  }, "\u751F\u6548\u7B56\u7565"), /*#__PURE__*/React.createElement("span", {
    className: "sb-sizing"
  }, dg.sizing))), /*#__PURE__*/React.createElement("div", {
    className: "sb-chips"
  }, dg.chips.map(c => /*#__PURE__*/React.createElement("span", {
    className: "sb-chip",
    key: c
  }, c))), /*#__PURE__*/React.createElement(Button, {
    size: "sm",
    variant: "ghost",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "sliders-horizontal"
    }),
    onClick: goStrategy
  }, "\u8C03\u6574\u7B56\u7565")) : null, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "sec-head",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("h2", {
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, "\u8DDF\u5355\u5217\u8868"), /*#__PURE__*/React.createElement("div", {
    className: "sec-sub"
  }, "\u6309\u76EE\u6807\u94B1\u5305\u4E70\u5165\u6BD4\u4F8B\u955C\u50CF\u5EFA\u4ED3"))), /*#__PURE__*/React.createElement("div", {
    className: "filter-group"
  }, /*#__PURE__*/React.createElement("label", {
    htmlFor: "st-f"
  }, "\u72B6\u6001"), /*#__PURE__*/React.createElement("select", {
    id: "st-f",
    className: "ps-select",
    value: status,
    onChange: e => setStatus(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: "all"
  }, "\u5168\u90E8"), /*#__PURE__*/React.createElement("option", {
    value: "open"
  }, "\u8FDB\u884C\u4E2D"), /*#__PURE__*/React.createElement("option", {
    value: "settled"
  }, "\u5DF2\u7ED3\u7B97"))))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, "\u7ED3\u7B97"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305\u6570"), /*#__PURE__*/React.createElement("th", null, "\u5355\u6570"), /*#__PURE__*/React.createElement("th", null, "\u6295\u5165"), /*#__PURE__*/React.createElement("th", null, "\u76C8\u4E8F"), /*#__PURE__*/React.createElement("th", null, "\u8D28\u91CF"))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map(f => /*#__PURE__*/React.createElement("tr", {
    key: f.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: f
  })), /*#__PURE__*/React.createElement("td", null, f.status === "open" ? /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }) : /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", null, f.settlement === "盈利" ? /*#__PURE__*/React.createElement("span", {
    className: "pnl-up strong"
  }, "\u76C8\u5229") : f.settlement === "亏损" ? /*#__PURE__*/React.createElement("span", {
    className: "pnl-down strong"
  }, "\u4E8F\u635F") : /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, f.wallets), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, f.legs), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, money(f.stake)), /*#__PURE__*/React.createElement("td", {
    className: pnlClass(f.pnl)
  }, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong num"
  }, signedMoney(f.pnl)), f.pnlKind === "unrealized" && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u5B9E\u73B0"))), /*#__PURE__*/React.createElement("td", null, qualityBadge(f.quality)))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "8",
    className: "empty-cell"
  }, "\u6682\u65E0\u8DDF\u5355\u8BB0\u5F55"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u7B14"
  }))));
}

/* ---------- Shell ---------- */
const PAGES = {
  overview: {
    title: "概览",
    comp: OverviewPage
  },
  strategy: {
    title: "跟单策略",
    comp: StrategyPage
  },
  leaderboard: {
    title: "Leaderboard",
    comp: LeaderboardPage
  },
  events: {
    title: "关注赛事",
    comp: EventsPage
  },
  follows: {
    title: "跟单列表",
    comp: FollowsPage
  }
};
const STRATEGY_DEFAULTS = {
  usableMode: "all",
  usableCap: "5000",
  minSignalOn: true,
  minSignal: "10",
  sizing: "ratio",
  ratio: "10",
  ratioCapOn: false,
  ratioCap: "100",
  fixed: "50",
  balancePct: "1",
  countOn: false,
  countMode: "event",
  count: "10",
  spendOn: false,
  spendMode: "fixed",
  spendFixed: "200",
  spendPct: "5"
};
const clampNum = v => v.replace(/[^\d.]/g, "");
function strategyDigest(s) {
  const n = v => Number(v) || 0;
  const sizing = s.sizing === "ratio" ? `比例 ${s.ratio || 0}%${s.ratioCapOn ? `（封顶 ${usdInt(n(s.ratioCap))}）` : ""}` : s.sizing === "fixed" ? `固定 ${usdInt(n(s.fixed))}` : `余额 ${s.balancePct || 0}%`;
  const filter = `门槛 ${usdInt(n(s.minSignal))}`;
  const count = s.countOn ? s.countMode === "event" ? `单场 ${s.count} 笔` : `每钱包 ${s.count} 笔` : null;
  const spend = s.spendOn ? s.spendMode === "fixed" ? `单场 ≤ ${usdInt(n(s.spendFixed))}` : `单场 ≤ 余额 ${s.spendPct}%` : null;
  return {
    sizing,
    chips: [filter, count, spend].filter(Boolean)
  };
}
function NumField({
  value,
  onChange,
  unit,
  width = 76,
  lead,
  disabled
}) {
  return /*#__PURE__*/React.createElement("span", {
    className: "num-field" + (disabled ? " is-disabled" : "")
  }, lead ? /*#__PURE__*/React.createElement("span", {
    className: "nf-lead"
  }, lead) : null, /*#__PURE__*/React.createElement("input", {
    value: value,
    onChange: e => onChange(clampNum(e.target.value)),
    style: {
      width
    },
    inputMode: "decimal",
    disabled: disabled
  }), unit ? /*#__PURE__*/React.createElement("span", {
    className: "nf-unit"
  }, unit) : null);
}
function StageCard({
  no,
  title,
  sub,
  badge,
  children
}) {
  return /*#__PURE__*/React.createElement(Card, {
    pad: "lg"
  }, /*#__PURE__*/React.createElement("div", {
    className: "stage-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "stage-no"
  }, no), /*#__PURE__*/React.createElement("div", {
    className: "stage-titles"
  }, /*#__PURE__*/React.createElement("div", {
    className: "stage-title-row"
  }, /*#__PURE__*/React.createElement("h3", {
    className: "stage-title"
  }, title), badge), /*#__PURE__*/React.createElement("p", {
    className: "stage-sub"
  }, sub))), /*#__PURE__*/React.createElement("div", {
    className: "stage-body"
  }, children));
}
function SizingOption({
  id,
  active,
  onSelect,
  title,
  desc,
  children
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "opt-card" + (active ? " is-active" : ""),
    onClick: () => onSelect(id),
    role: "radio",
    "aria-checked": active
  }, /*#__PURE__*/React.createElement("div", {
    className: "opt-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "opt-radio",
    "aria-hidden": "true"
  }), /*#__PURE__*/React.createElement("div", {
    className: "opt-titles"
  }, /*#__PURE__*/React.createElement("span", {
    className: "opt-title"
  }, title), /*#__PURE__*/React.createElement("span", {
    className: "opt-desc"
  }, desc))), active && children ? /*#__PURE__*/React.createElement("div", {
    className: "opt-body",
    onClick: e => e.stopPropagation()
  }, children) : null);
}
function StrategyPage({
  strategy: s,
  setStrategy,
  running,
  setRunning
}) {
  const up = k => v => setStrategy(p => ({
    ...p,
    [k]: v
  }));
  const n = v => Number(v) || 0;
  const wallet = D.overview.walletBalance;
  const avail = s.usableMode === "cap" ? Math.min(n(s.usableCap), wallet) : wallet;
  const [sample, setSample] = React.useState("1200");
  const t = n(sample);
  let ex;
  if (t < n(s.minSignal)) {
    ex = {
      ignored: true,
      reason: `目标买入低于门槛 ${usdInt(n(s.minSignal))}`
    };
  } else {
    let raw, basis;
    if (s.sizing === "ratio") {
      raw = t * n(s.ratio) / 100;
      basis = `${s.ratio || 0}% × ${usdInt(t)}`;
      if (s.ratioCapOn && raw > n(s.ratioCap)) {
        raw = n(s.ratioCap);
        basis = `命中封顶 ${usdInt(n(s.ratioCap))}`;
      }
    } else if (s.sizing === "fixed") {
      raw = n(s.fixed);
      basis = "固定金额";
    } else {
      raw = avail * n(s.balancePct) / 100;
      basis = `${s.balancePct || 0}% × 可用 ${usdInt(avail)}`;
    }
    ex = {
      amount: Math.floor(Math.max(0, raw)),
      basis
    };
  }
  const sizingPrimary = s.sizing === "ratio" ? s.ratio : s.sizing === "fixed" ? s.fixed : s.balancePct;
  const issues = [];
  if (!(n(sizingPrimary) > 0)) issues.push("单笔金额");
  if (s.usableMode === "cap" && !(n(s.usableCap) > 0)) issues.push("可动用上限");
  if (!(n(s.minSignal) > 0)) issues.push("最小信号金额");
  if (s.sizing === "ratio" && s.ratioCapOn && !(n(s.ratioCap) > 0)) issues.push("单笔封顶金额");
  if (s.countOn && !(n(s.count) > 0)) issues.push("单场笔数");
  if (s.spendOn && !(n(s.spendMode === "fixed" ? s.spendFixed : s.spendPct) > 0)) issues.push("单场投入上限");
  const ready = issues.length === 0;
  const dg = strategyDigest(s);
  const ico = nm => /*#__PURE__*/React.createElement("i", {
    "data-lucide": nm
  });
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner strat-page"
  }, /*#__PURE__*/React.createElement("section", {
    className: "strat-overview"
  }, /*#__PURE__*/React.createElement("div", {
    className: "so-bar"
  }, /*#__PURE__*/React.createElement("div", {
    className: "so-title"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u7B56\u7565\u603B\u89C8"), /*#__PURE__*/React.createElement("h2", {
    className: "so-h"
  }, running ? "跟单引擎运行中" : "策略待启动")), /*#__PURE__*/React.createElement("div", {
    className: "so-example"
  }, /*#__PURE__*/React.createElement("span", {
    className: "soe-label"
  }, "\u793A\u4F8B\u63A8\u6F14"), /*#__PURE__*/React.createElement("span", {
    className: "soe-row"
  }, "\u76EE\u6807\u4E70\u5165", /*#__PURE__*/React.createElement(NumField, {
    value: sample,
    onChange: setSample,
    unit: "USDC",
    width: 58
  }), /*#__PURE__*/React.createElement("i", {
    "data-lucide": "arrow-right",
    className: "soe-arrow"
  }), ex.ignored ? /*#__PURE__*/React.createElement("span", {
    className: "soe-out ignored"
  }, "\u4FE1\u53F7\u5FFD\u7565") : /*#__PURE__*/React.createElement("span", {
    className: "soe-out"
  }, "\u5B9E\u9645\u8DDF\u5355 ", /*#__PURE__*/React.createElement("b", null, usdInt(ex.amount))))), /*#__PURE__*/React.createElement("div", {
    className: "so-right"
  }, /*#__PURE__*/React.createElement(StatusPill, {
    status: running ? "live" : "idle",
    label: running ? "运行中" : "已停止",
    extra: running ? "02:14:08" : undefined
  }), /*#__PURE__*/React.createElement(Button, {
    variant: "ghost",
    size: "sm",
    iconLeft: ico("rotate-ccw"),
    onClick: () => setStrategy({
      ...STRATEGY_DEFAULTS
    })
  }, "\u91CD\u7F6E"), running ? /*#__PURE__*/React.createElement(Button, {
    variant: "danger",
    iconLeft: /*#__PURE__*/React.createElement("svg", {
      viewBox: "0 0 24 24",
      width: "15",
      height: "15",
      fill: "currentColor",
      "aria-hidden": "true"
    }, /*#__PURE__*/React.createElement("rect", {
      x: "6",
      y: "6",
      width: "12",
      height: "12",
      rx: "2.5"
    })),
    onClick: () => setRunning(false)
  }, "\u505C\u6B62\u8DDF\u5355") : /*#__PURE__*/React.createElement(Button, {
    variant: "primary",
    iconLeft: /*#__PURE__*/React.createElement("svg", {
      viewBox: "0 0 24 24",
      width: "15",
      height: "15",
      fill: "currentColor",
      "aria-hidden": "true"
    }, /*#__PURE__*/React.createElement("path", {
      d: "M7 4.5v15a1 1 0 0 0 1.5.87l13-7.5a1 1 0 0 0 0-1.74l-13-7.5A1 1 0 0 0 7 4.5z"
    })),
    disabled: !ready,
    onClick: () => setRunning(true)
  }, ready ? "启动跟单" : "待完善"))), /*#__PURE__*/React.createElement("ol", {
    className: "so-pipe"
  }, [{
    k: "资金池",
    v: s.usableMode === "all" ? "全部余额" : `上限 ${usdInt(n(s.usableCap))}`
  }, {
    k: "信号门槛",
    v: `忽略 < ${usdInt(n(s.minSignal))}`
  }, {
    k: "单笔金额",
    v: dg.sizing,
    key: true
  }, {
    k: "单场笔数",
    v: s.countOn ? s.countMode === "event" ? `整场 ${s.count} 笔` : `每钱包 ${s.count} 笔` : "不限"
  }, {
    k: "单场投入",
    v: s.spendOn ? s.spendMode === "fixed" ? `≤ ${usdInt(n(s.spendFixed))}` : `≤ 余额 ${s.spendPct}%` : "不限"
  }].map(nd => /*#__PURE__*/React.createElement("li", {
    key: nd.k,
    className: "so-node" + (nd.key ? " is-key" : "")
  }, /*#__PURE__*/React.createElement("span", {
    className: "son-k"
  }, nd.k), /*#__PURE__*/React.createElement("span", {
    className: "son-v"
  }, nd.v)))), !ready && !running ? /*#__PURE__*/React.createElement("div", {
    className: "so-todo"
  }, ico("circle-alert"), " \u5F85\u5B8C\u5584\u5FC5\u586B\u9879\uFF1A", issues.join("、")) : null), /*#__PURE__*/React.createElement(Card, {
    pad: "lg"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-split"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-col"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-mini"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cm-head"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "wallet"
  }), /*#__PURE__*/React.createElement("span", null, "\u53EF\u52A8\u7528\u8D44\u91D1")), /*#__PURE__*/React.createElement("div", {
    className: "cm-body"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.usableMode,
    onChange: up("usableMode"),
    options: [{
      value: "all",
      label: "全部余额"
    }, {
      value: "cap",
      label: "指定上限"
    }]
  }), s.usableMode === "cap" ? /*#__PURE__*/React.createElement(NumField, {
    value: s.usableCap,
    onChange: up("usableCap"),
    unit: "USDC",
    width: 84
  }) : /*#__PURE__*/React.createElement("span", {
    className: "mc-note"
  }, "\u94B1\u5305 ", money(wallet)))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-head"
  }, /*#__PURE__*/React.createElement("h3", null, "\u5355\u7B14\u8DDF\u5355\u91D1\u989D"), /*#__PURE__*/React.createElement(Badge, {
    tone: "up",
    outline: true
  }, "\u5FC5\u586B")), /*#__PURE__*/React.createElement("p", {
    className: "cfg-sub"
  }, "\u6BCF\u4E2A\u6709\u6548\u4FE1\u53F7\u4E70\u5165\u591A\u5C11 \xB7 \u4E09\u9009\u4E00\uFF0C\u91D1\u989D\u5411\u4E0B\u53D6\u6574\u89C4\u907F\u4E0B\u5355\u5F02\u5E38"), /*#__PURE__*/React.createElement("div", {
    className: "opt-list"
  }, /*#__PURE__*/React.createElement(SizingOption, {
    id: "ratio",
    active: s.sizing === "ratio",
    onSelect: up("sizing"),
    title: "\u6309\u76EE\u6807\u6BD4\u4F8B",
    desc: "\u8DDF\u968F\u76EE\u6807\u94B1\u5305\u4E70\u5165\u989D\u7684\u56FA\u5B9A\u6BD4\u4F8B"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ratio-rows"
  }, /*#__PURE__*/React.createElement("span", {
    className: "rr-lead"
  }, /*#__PURE__*/React.createElement("span", {
    className: "rr-check-slot"
  }), "\u8DDF\u5355\u6BD4\u4F8B"), /*#__PURE__*/React.createElement(NumField, {
    value: s.ratio,
    onChange: up("ratio"),
    unit: "%",
    width: 56
  }), /*#__PURE__*/React.createElement("label", {
    className: "rr-lead is-check",
    onClick: e => e.stopPropagation()
  }, /*#__PURE__*/React.createElement("input", {
    type: "checkbox",
    checked: s.ratioCapOn,
    onChange: e => up("ratioCapOn")(e.target.checked)
  }), "\u5355\u7B14\u5C01\u9876"), /*#__PURE__*/React.createElement(NumField, {
    value: s.ratioCap,
    onChange: up("ratioCap"),
    unit: "USDC",
    width: 56,
    disabled: !s.ratioCapOn
  }))), /*#__PURE__*/React.createElement(SizingOption, {
    id: "fixed",
    active: s.sizing === "fixed",
    onSelect: up("sizing"),
    title: "\u56FA\u5B9A\u91D1\u989D",
    desc: "\u6BCF\u7B14\u4E70\u5165\u56FA\u5B9A\u91D1\u989D\uFF0C\u4E0E\u76EE\u6807\u4E0B\u5355\u989D\u65E0\u5173"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.fixed,
    onChange: up("fixed"),
    unit: "USDC",
    lead: "\u6BCF\u7B14\u4E70\u5165"
  }))), /*#__PURE__*/React.createElement(SizingOption, {
    id: "balancePct",
    active: s.sizing === "balancePct",
    onSelect: up("sizing"),
    title: "\u6309\u672C\u91D1\u767E\u5206\u6BD4",
    desc: "\u6309\u5F53\u524D\u53EF\u52A8\u7528\u4F59\u989D\u7684\u767E\u5206\u6BD4\u52A8\u6001\u4E70\u5165"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.balancePct,
    onChange: up("balancePct"),
    unit: "%",
    lead: "\u6BCF\u7B14\u5360\u7528"
  }))))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-col"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-mini"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cm-head"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "filter"
  }), /*#__PURE__*/React.createElement("span", null, "\u4FE1\u53F7\u95E8\u69DB"), /*#__PURE__*/React.createElement(Badge, {
    tone: "up",
    outline: true
  }, "\u5FC5\u586B")), /*#__PURE__*/React.createElement("div", {
    className: "cm-body"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.minSignal,
    onChange: up("minSignal"),
    unit: "USDC",
    lead: "\u5FFD\u7565\u76EE\u6807\u4E70\u5165 <",
    width: 64
  }))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-head"
  }, /*#__PURE__*/React.createElement("h3", null, "\u5355\u573A\u98CE\u63A7\u4E0A\u9650")), /*#__PURE__*/React.createElement("p", {
    className: "cfg-sub"
  }, "\u5BF9\u5355\u573A\u8D5B\u4E8B\u7684\u7D2F\u8BA1\u8DDF\u5355\u8BBE\u9632 \xB7 \u4E24\u9879\u53EF\u72EC\u7ACB\u5F00\u542F"), /*#__PURE__*/React.createElement("div", {
    className: "sub-block"
  }, /*#__PURE__*/React.createElement("div", {
    className: "switch-row"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sr-text"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sr-title"
  }, "\u5355\u573A\u7B14\u6570\u4E0A\u9650"), /*#__PURE__*/React.createElement("span", {
    className: "sr-desc"
  }, "\u9650\u5236\u4E00\u573A\u8D5B\u4E8B\u7D2F\u8BA1\u53EF\u8DDF\u7684\u7B14\u6570")), /*#__PURE__*/React.createElement(Switch, {
    checked: s.countOn,
    onChange: v => up("countOn")(v),
    accent: true
  })), s.countOn ? /*#__PURE__*/React.createElement("div", {
    className: "sub-controls"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.countMode,
    onChange: up("countMode"),
    options: [{
      value: "event",
      label: "按赛事合计"
    }, {
      value: "wallet",
      label: "按每个钱包"
    }]
  }), /*#__PURE__*/React.createElement(NumField, {
    value: s.count,
    onChange: up("count"),
    unit: "\u7B14",
    width: 58
  }))) : null), /*#__PURE__*/React.createElement("div", {
    className: "sub-block"
  }, /*#__PURE__*/React.createElement("div", {
    className: "switch-row"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sr-text"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sr-title"
  }, "\u5355\u573A\u6295\u5165\u4E0A\u9650"), /*#__PURE__*/React.createElement("span", {
    className: "sr-desc"
  }, "\u9650\u5236\u4E00\u573A\u8D5B\u4E8B\u7684\u7D2F\u8BA1\u4E70\u5165\u91D1\u989D")), /*#__PURE__*/React.createElement(Switch, {
    checked: s.spendOn,
    onChange: v => up("spendOn")(v),
    accent: true
  })), s.spendOn ? /*#__PURE__*/React.createElement("div", {
    className: "sub-controls"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.spendMode,
    onChange: up("spendMode"),
    options: [{
      value: "fixed",
      label: "固定金额"
    }, {
      value: "balancePct",
      label: "余额百分比"
    }]
  }), s.spendMode === "fixed" ? /*#__PURE__*/React.createElement(NumField, {
    value: s.spendFixed,
    onChange: up("spendFixed"),
    unit: "USDC",
    width: 88
  }) : /*#__PURE__*/React.createElement(NumField, {
    value: s.spendPct,
    onChange: up("spendPct"),
    unit: "%",
    width: 58
  }))) : null)))));
}
function App() {
  const [page, setPage] = React.useState("overview");
  const [running, setRunning] = React.useState(true);
  const [light, setLight] = React.useState(true);
  const [strategy, setStrategy] = React.useState({
    ...STRATEGY_DEFAULTS
  });
  React.useEffect(() => {
    window.lucide && window.lucide.createIcons();
  });
  React.useEffect(() => {
    document.documentElement.setAttribute("data-theme", light ? "light" : "dark");
  }, [light]);
  const Page = PAGES[page].comp;
  const ico = n => /*#__PURE__*/React.createElement("i", {
    "data-lucide": n
  });
  return /*#__PURE__*/React.createElement("div", {
    className: "app-shell",
    "data-theme": light ? "light" : "dark"
  }, /*#__PURE__*/React.createElement(SidebarNav, {
    value: page,
    onChange: setPage,
    groupLabel: "\u5DE5\u4F5C\u53F0",
    items: [{
      id: "overview",
      label: "概览",
      icon: ico("layout-dashboard")
    }, {
      id: "strategy",
      label: "跟单策略",
      icon: ico("crosshair")
    }, {
      id: "leaderboard",
      label: "Leaderboard",
      icon: ico("trophy"),
      count: 30
    }, {
      id: "events",
      label: "关注赛事",
      icon: ico("swords"),
      count: 18
    }, {
      id: "follows",
      label: "跟单列表",
      icon: ico("list-checks"),
      count: 7
    }],
    footer: /*#__PURE__*/React.createElement("div", {
      className: "theme-toggle"
    }, /*#__PURE__*/React.createElement("span", null, ico(light ? "sun" : "moon"), " ", light ? "浅色" : "深色"), /*#__PURE__*/React.createElement(Switch, {
      checked: !light,
      onChange: () => setLight(v => !v),
      accent: true
    }))
  }), /*#__PURE__*/React.createElement("div", {
    className: "app-main"
  }, /*#__PURE__*/React.createElement("header", {
    className: "topbar"
  }, /*#__PURE__*/React.createElement("h1", {
    className: "topbar-title"
  }, PAGES[page].title), /*#__PURE__*/React.createElement("div", {
    className: "topbar-spacer"
  }), /*#__PURE__*/React.createElement("div", {
    className: "topbar-actions"
  }, /*#__PURE__*/React.createElement(StatusPill, {
    status: running ? "live" : "idle",
    label: running ? "运行中" : "已停止",
    extra: running ? "02:14:08" : undefined
  }))), /*#__PURE__*/React.createElement("div", {
    className: "page-scroll"
  }, /*#__PURE__*/React.createElement(Page, {
    strategy: strategy,
    setStrategy: setStrategy,
    running: running,
    setRunning: setRunning,
    goStrategy: () => setPage("strategy"),
    onNav: setPage
  }))));
}
ReactDOM.createRoot(document.getElementById("root")).render(/*#__PURE__*/React.createElement(App, null));
setTimeout(() => window.lucide && window.lucide.createIcons(), 80);
})(); } catch (e) { __ds_ns.__errors.push({ path: "app.jsx", error: String((e && e.message) || e) }); }

// components/core/Badge.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-badge-css")) {
  const s = document.createElement("style");
  s.id = "ps-badge-css";
  s.textContent = `
  .ps-badge {
    --_bg: var(--surface-inset); --_fg: var(--text-secondary);
    display: inline-flex; align-items: center; gap: var(--sp-1);
    height: 22px; padding: 0 var(--sp-3);
    border-radius: var(--r-pill);
    font: var(--fw-semibold) var(--fs-caption)/1 var(--font-sans);
    letter-spacing: 0.02em; white-space: nowrap;
    background: var(--_bg); color: var(--_fg);
    border: 1px solid transparent;
  }
  .ps-badge.outline { background: transparent; border-color: currentColor; }
  .ps-badge.dot::before {
    content: ""; width: 6px; height: 6px; border-radius: 50%;
    background: currentColor; opacity: 0.9;
  }
  .ps-badge.t-neutral { --_bg: var(--surface-inset); --_fg: var(--text-secondary); }
  .ps-badge.t-accent  { --_bg: var(--accent-soft); --_fg: var(--accent); }
  .ps-badge.t-up      { --_bg: var(--pnl-up-soft); --_fg: var(--pnl-up); }
  .ps-badge.t-down    { --_bg: var(--pnl-down-soft); --_fg: var(--pnl-down); }
  .ps-badge.t-warn    { --_bg: var(--status-warn-soft); --_fg: var(--status-warn); }
  `;
  document.head.appendChild(s);
}
function Badge({
  tone = "neutral",
  outline = false,
  dot = false,
  className = "",
  children,
  ...rest
}) {
  const cls = ["ps-badge", `t-${tone}`, outline ? "outline" : "", dot ? "dot" : "", className].filter(Boolean).join(" ");
  return React.createElement("span", {
    className: cls,
    ...rest
  }, children);
}
Object.assign(__ds_scope, { Badge });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/core/Badge.jsx", error: String((e && e.message) || e) }); }

// components/core/Button.jsx
try { (() => {
const {
  forwardRef
} = React;
if (typeof document !== "undefined" && !document.getElementById("ps-button-css")) {
  const s = document.createElement("style");
  s.id = "ps-button-css";
  s.textContent = `
  .ps-btn {
    --_bg: var(--surface-inset);
    --_fg: var(--text-primary);
    --_bd: var(--border-hairline);
    display: inline-flex; align-items: center; justify-content: center;
    gap: var(--sp-2); white-space: nowrap;
    font-family: var(--font-sans); font-weight: var(--fw-semibold);
    letter-spacing: var(--ls-body);
    border-radius: var(--r-pill);
    border: 1px solid var(--_bd);
    background: var(--_bg); color: var(--_fg);
    cursor: pointer; user-select: none;
    transition: var(--t-hover), var(--t-press);
    -webkit-backdrop-filter: var(--blur-thin); backdrop-filter: var(--blur-thin);
  }
  .ps-btn:active { transform: scale(0.97); }
  .ps-btn:disabled { opacity: 0.4; pointer-events: none; }
  .ps-btn.sz-sm { height: var(--control-sm); padding: 0 var(--sp-4); font-size: var(--fs-subhead); }
  .ps-btn.sz-md { height: var(--control-md); padding: 0 var(--sp-5); font-size: var(--fs-callout); }
  .ps-btn.sz-lg { height: var(--control-lg); padding: 0 var(--sp-7); font-size: var(--fs-h4); }
  .ps-btn.v-primary {
    --_bg: var(--gradient-brand); --_fg: var(--text-on-accent); --_bd: transparent;
    background-clip: border-box;
    box-shadow: var(--shadow-accent), inset 0 1px 0 rgba(255, 255, 255, 0.22);
  }
  .ps-btn.v-primary:hover { filter: brightness(1.05); }
  .ps-btn.v-secondary { --_bg: var(--surface-card); --_bd: var(--border-strong); }
  .ps-btn.v-secondary:hover { --_bg: var(--surface-inset); border-color: var(--accent-ring); }
  .ps-btn.v-ghost { --_bg: transparent; --_bd: transparent; -webkit-backdrop-filter: none; backdrop-filter: none; }
  .ps-btn.v-ghost:hover { --_bg: var(--surface-inset); }
  .ps-btn.v-danger { --_bg: var(--pnl-down-soft); --_fg: var(--pnl-down); --_bd: transparent; }
  .ps-btn.v-danger:hover { --_bg: var(--pnl-down); --_fg: #fff; }
  .ps-btn.block { width: 100%; }
  .ps-btn .ps-btn-icon { width: 1.1em; height: 1.1em; object-fit: contain; display: block; }
  `;
  document.head.appendChild(s);
}
const Button = forwardRef(function Button({
  variant = "secondary",
  size = "md",
  block = false,
  iconLeft,
  iconRight,
  className = "",
  children,
  ...rest
}, ref) {
  const cls = ["ps-btn", `v-${variant}`, `sz-${size}`, block ? "block" : "", className].filter(Boolean).join(" ");
  return React.createElement("button", {
    ref,
    className: cls,
    ...rest
  }, iconLeft, children != null ? React.createElement("span", null, children) : null, iconRight);
});
Object.assign(__ds_scope, { Button });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/core/Button.jsx", error: String((e && e.message) || e) }); }

// components/core/Card.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-card-css")) {
  const s = document.createElement("style");
  s.id = "ps-card-css";
  s.textContent = `
  .ps-card {
    position: relative;
    border-radius: var(--r-lg);
    background: var(--surface-card);
    border: 1px solid var(--border-hairline);
    box-shadow: var(--shadow-md), var(--inner-highlight);
    -webkit-backdrop-filter: var(--blur-regular); backdrop-filter: var(--blur-regular);
    padding: var(--sp-6);
    transition: var(--t-hover);
  }
  @supports not ((backdrop-filter: blur(1px)) or (-webkit-backdrop-filter: blur(1px))) {
    .ps-card { background: var(--surface-solid); }
  }
  .ps-card.pad-sm { padding: var(--sp-4); }
  .ps-card.pad-lg { padding: var(--sp-8); }
  .ps-card.flush { padding: 0; overflow: hidden; }
  .ps-card.interactive { cursor: pointer; }
  .ps-card.interactive:hover { border-color: var(--border-strong); box-shadow: var(--shadow-lg), var(--inner-highlight-strong); transform: translateY(-2px); }
  .ps-card.interactive:active { transform: translateY(0); }
  .ps-card.glow::after {
    content: ""; position: absolute; inset: 0; border-radius: inherit;
    padding: 1px; background: var(--gradient-brand);
    -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
    -webkit-mask-composite: xor; mask-composite: exclude;
    opacity: 0.5; pointer-events: none;
  }
  .ps-card-head { display: flex; align-items: baseline; justify-content: space-between; gap: var(--sp-3); margin-bottom: var(--sp-4); }
  .ps-card-title { font: var(--text-headline); letter-spacing: var(--ls-body); margin: 0; }
  .ps-card-eyebrow { font: var(--text-label); text-transform: uppercase; letter-spacing: var(--ls-label); color: var(--text-tertiary); }
  `;
  document.head.appendChild(s);
}
function Card({
  pad,
  interactive = false,
  glow = false,
  eyebrow,
  title,
  action,
  className = "",
  children,
  ...rest
}) {
  const cls = ["ps-card", pad ? `pad-${pad}` : "", interactive ? "interactive" : "", glow ? "glow" : "", className].filter(Boolean).join(" ");
  const hasHead = eyebrow || title || action;
  return React.createElement("div", {
    className: cls,
    ...rest
  }, hasHead ? React.createElement("div", {
    className: "ps-card-head"
  }, React.createElement("div", null, eyebrow ? React.createElement("div", {
    className: "ps-card-eyebrow"
  }, eyebrow) : null, title ? React.createElement("h3", {
    className: "ps-card-title"
  }, title) : null), action || null) : null, children);
}
Object.assign(__ds_scope, { Card });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/core/Card.jsx", error: String((e && e.message) || e) }); }

// components/core/IconButton.jsx
try { (() => {
const {
  forwardRef
} = React;
if (typeof document !== "undefined" && !document.getElementById("ps-iconbutton-css")) {
  const s = document.createElement("style");
  s.id = "ps-iconbutton-css";
  s.textContent = `
  .ps-iconbtn {
    display: inline-flex; align-items: center; justify-content: center;
    width: var(--icon-btn); height: var(--icon-btn);
    border-radius: var(--r-pill);
    border: 1px solid var(--border-hairline);
    background: var(--surface-inset); color: var(--text-secondary);
    cursor: pointer; transition: var(--t-hover), var(--t-press);
    -webkit-backdrop-filter: var(--blur-thin); backdrop-filter: var(--blur-thin);
  }
  .ps-iconbtn:hover { color: var(--text-primary); border-color: var(--accent-ring); background: var(--surface-card); }
  .ps-iconbtn:active { transform: scale(0.92); }
  .ps-iconbtn:disabled { opacity: 0.4; pointer-events: none; }
  .ps-iconbtn.sz-sm { width: var(--control-sm); height: var(--control-sm); }
  .ps-iconbtn.sz-lg { width: var(--control-lg); height: var(--control-lg); }
  .ps-iconbtn.active { color: var(--text-on-accent); background: var(--gradient-brand); border-color: transparent; box-shadow: var(--shadow-accent); }
  .ps-iconbtn img { width: 50%; height: 50%; object-fit: contain; display: block; }
  .ps-iconbtn .ps-iconbtn-glyph { font-size: 1.05rem; line-height: 1; }
  `;
  document.head.appendChild(s);
}
const IconButton = forwardRef(function IconButton({
  size = "md",
  active = false,
  label,
  className = "",
  children,
  ...rest
}, ref) {
  const cls = ["ps-iconbtn", `sz-${size}`, active ? "active" : "", className].filter(Boolean).join(" ");
  return React.createElement("button", {
    ref,
    className: cls,
    "aria-label": label,
    title: label,
    ...rest
  }, children);
});
Object.assign(__ds_scope, { IconButton });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/core/IconButton.jsx", error: String((e && e.message) || e) }); }

// components/data/CategoryDonut.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-categorydonut-css")) {
  const s = document.createElement("style");
  s.id = "ps-categorydonut-css";
  s.textContent = `
  .ps-cdonut { display: inline-flex; align-items: center; gap: var(--sp-8); flex-wrap: wrap; }
  .ps-cdonut-ring { position: relative; display: grid; place-items: center; flex: none; }
  .ps-cdonut-ring svg { display: block; transform: rotate(-90deg); }
  .ps-cdonut-track { fill: none; stroke: var(--surface-inset); }
  .ps-cdonut-seg { fill: none; stroke-linecap: butt; transition: stroke-dasharray var(--dur-slow) var(--ease-out); }
  .ps-cdonut-center {
    position: absolute; inset: 0; display: flex; flex-direction: column;
    align-items: center; justify-content: center; gap: 2px; pointer-events: none; text-align: center;
  }
  .ps-cdonut-value { font-family: var(--font-sans); font-weight: var(--fw-bold); color: var(--text-primary); line-height: 1; letter-spacing: var(--ls-title); font-variant-numeric: tabular-nums; }
  .ps-cdonut-label { font-weight: var(--fw-semibold); color: var(--text-tertiary); text-transform: uppercase; letter-spacing: var(--ls-label); line-height: 1.2; }
  .ps-cdonut-legend { display: grid; gap: var(--sp-2) var(--sp-6); }
  .ps-cdonut-item { display: flex; align-items: center; gap: var(--sp-3); min-width: 0; }
  .ps-cdonut-sw { width: 12px; height: 12px; border-radius: 5px; flex: none; box-shadow: var(--inner-highlight); }
  .ps-cdonut-name { font-size: var(--fs-callout); color: var(--text-secondary); white-space: nowrap; }
  .ps-cdonut-name b { color: var(--text-primary); font-weight: var(--fw-semibold); }
  .ps-cdonut-pct { margin-left: auto; padding-left: var(--sp-3); font-size: var(--fs-callout); font-weight: var(--fw-semibold); color: var(--text-primary); font-variant-numeric: tabular-nums; }
  `;
  document.head.appendChild(s);
}
function CategoryDonut({
  segments = [],
  size = 200,
  thickness = 28,
  gap = 3,
  centerLabel,
  centerValue,
  legend = false,
  legendCols = 2,
  className = "",
  ...rest
}) {
  const total = segments.reduce((a, s) => a + (s.value || 0), 0) || 1;
  const r = (size - thickness) / 2 - 1;
  const c = 2 * Math.PI * r;
  const cx = size / 2;
  let cursor = 0;
  const arcs = segments.map((s, i) => {
    const arc = (s.value || 0) / total * c;
    const drawLen = Math.max(0.5, arc - gap);
    const node = React.createElement("circle", {
      key: i,
      className: "ps-cdonut-seg",
      cx,
      cy: cx,
      r,
      stroke: s.color,
      strokeWidth: thickness,
      strokeDasharray: `${drawLen} ${c - drawLen}`,
      strokeDashoffset: -(cursor + gap / 2)
    });
    cursor += arc;
    return node;
  });
  const ring = React.createElement("div", {
    className: "ps-cdonut-ring",
    style: {
      width: size,
      height: size
    }
  }, React.createElement("svg", {
    width: size,
    height: size,
    viewBox: `0 0 ${size} ${size}`
  }, React.createElement("circle", {
    className: "ps-cdonut-track",
    cx,
    cy: cx,
    r,
    strokeWidth: thickness
  }), arcs), centerValue || centerLabel ? React.createElement("div", {
    className: "ps-cdonut-center"
  }, centerValue ? React.createElement("span", {
    className: "ps-cdonut-value",
    style: {
      fontSize: size * 0.17
    }
  }, centerValue) : null, centerLabel ? React.createElement("span", {
    className: "ps-cdonut-label",
    style: {
      fontSize: Math.max(10, size * 0.058)
    }
  }, centerLabel) : null) : null);
  const legendEl = legend ? React.createElement("div", {
    className: "ps-cdonut-legend",
    style: {
      gridTemplateColumns: `repeat(${legendCols}, auto)`
    }
  }, segments.map((s, i) => React.createElement("div", {
    key: i,
    className: "ps-cdonut-item"
  }, React.createElement("span", {
    className: "ps-cdonut-sw",
    style: {
      background: s.color
    }
  }), React.createElement("span", {
    className: "ps-cdonut-name"
  }, s.group ? React.createElement("b", null, s.group + " ") : null, s.label), React.createElement("span", {
    className: "ps-cdonut-pct"
  }, Math.round((s.value || 0) / total * 100) + "%")))) : null;
  return React.createElement("div", {
    className: ["ps-cdonut", className].filter(Boolean).join(" "),
    ...rest
  }, legendEl, ring);
}
Object.assign(__ds_scope, { CategoryDonut });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/CategoryDonut.jsx", error: String((e && e.message) || e) }); }

// components/data/GameIcon.jsx
try { (() => {
const GAME_LABELS = {
  cs2: "CS2",
  dota2: "Dota 2",
  lol: "LoL",
  valorant: "Valorant",
  multi: "跨游戏"
};
// 无对应 logo 的 game(如 multi / 未来新游戏)→ 破图兜底:把坏 img 换成短文字,不显示裂图。
function __gameIconOnError(e) {
  const el = e && e.currentTarget;
  if (!el || el.dataset.fallback) return;
  el.dataset.fallback = "1";
  const txt = (el.getAttribute("alt") || "").slice(0, 2) || "?";
  const span = document.createElement("span");
  span.textContent = txt;
  span.style.cssText = "font:600 10px/1 var(--font-sans);color:var(--text-secondary)";
  el.replaceWith(span);
}
if (typeof document !== "undefined" && !document.getElementById("ps-gameicon-css")) {
  const s = document.createElement("style");
  s.id = "ps-gameicon-css";
  s.textContent = `
  .ps-gameicon {
    display: inline-flex; align-items: center; justify-content: center;
    border-radius: var(--r-md);
    background: var(--surface-inset);
    border: 1px solid var(--border-hairline);
    overflow: hidden; flex: none;
  }
  .ps-gameicon.sz-sm { width: 22px; height: 22px; border-radius: var(--r-xs); }
  .ps-gameicon.sz-md { width: 32px; height: 32px; }
  .ps-gameicon.sz-lg { width: 44px; height: 44px; border-radius: var(--r-lg); }
  .ps-gameicon img { width: 64%; height: 64%; object-fit: contain; display: block; }
  .ps-gamechip {
    display: inline-flex; align-items: center; gap: var(--sp-2);
    height: 26px; padding: 0 var(--sp-3) 0 4px;
    border-radius: var(--r-pill);
    background: var(--surface-inset); border: 1px solid var(--border-hairline);
    font: var(--fw-semibold) var(--fs-subhead)/1 var(--font-sans); color: var(--text-secondary);
  }
  .ps-gamechip img { width: 18px; height: 18px; object-fit: contain; }
  `;
  document.head.appendChild(s);
}
function GameIcon({
  game,
  size = "md",
  base = "assets",
  chip = false,
  label,
  className = "",
  ...rest
}) {
  const src = `${base}/games/${game}.png`;
  const text = label || GAME_LABELS[game] || game;
  if (chip) {
    return React.createElement("span", {
      className: ["ps-gamechip", className].filter(Boolean).join(" "),
      ...rest
    }, React.createElement("img", {
      src,
      alt: text,
      onError: __gameIconOnError
    }), text);
  }
  return React.createElement("span", {
    className: ["ps-gameicon", `sz-${size}`, className].filter(Boolean).join(" "),
    title: text,
    ...rest
  }, React.createElement("img", {
    src,
    alt: text,
    onError: __gameIconOnError
  }));
}
Object.assign(__ds_scope, { GameIcon });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/GameIcon.jsx", error: String((e && e.message) || e) }); }

// components/data/RankBadge.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-rankbadge-css")) {
  const s = document.createElement("style");
  s.id = "ps-rankbadge-css";
  s.textContent = `
  .ps-rank {
    display: inline-flex; align-items: center; justify-content: center;
    width: 30px; height: 30px; border-radius: var(--r-pill);
    font: var(--fw-bold) var(--fs-subhead)/1 var(--font-sans);
    font-variant-numeric: tabular-nums;
    color: var(--text-secondary); background: var(--surface-inset);
    border: 1px solid var(--border-hairline); flex: none;
  }
  .ps-rank.medal { color: #1a1205; border: 0; background-clip: border-box; box-shadow: var(--shadow-sm), var(--inner-highlight-strong); }
  .ps-rank.gold   { background: linear-gradient(150deg, #ffe79a 0%, #f5c451 45%, #c8932a 100%); }
  .ps-rank.silver { background: linear-gradient(150deg, #f4f6fb 0%, #cfd6e2 45%, #9aa3b4 100%); }
  .ps-rank.bronze { background: linear-gradient(150deg, #f0c39a 0%, #d59760 45%, #a36a37 100%); }
  `;
  document.head.appendChild(s);
}
function RankBadge({
  rank,
  className = "",
  ...rest
}) {
  const n = Number(rank);
  const medal = n === 1 ? "gold" : n === 2 ? "silver" : n === 3 ? "bronze" : null;
  const cls = ["ps-rank", medal ? "medal " + medal : "", className].filter(Boolean).join(" ");
  return React.createElement("span", {
    className: cls,
    ...rest
  }, Number.isFinite(n) ? n : rank || "–");
}
Object.assign(__ds_scope, { RankBadge });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/RankBadge.jsx", error: String((e && e.message) || e) }); }

// components/data/StatTile.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-stattile-css")) {
  const s = document.createElement("style");
  s.id = "ps-stattile-css";
  s.textContent = `
  .ps-stat { display: flex; flex-direction: column; gap: var(--sp-2); min-width: 0; }
  .ps-stat-label {
    font: var(--text-label); text-transform: uppercase; letter-spacing: var(--ls-label);
    color: var(--text-tertiary); display: inline-flex; align-items: center; gap: var(--sp-2);
  }
  .ps-stat-value {
    font-family: var(--font-sans); font-weight: var(--fw-bold);
    letter-spacing: var(--ls-title); line-height: var(--lh-tight);
    color: var(--text-primary); font-variant-numeric: tabular-nums;
  }
  .ps-stat.sz-sm .ps-stat-value { font-size: var(--fs-h3); }
  .ps-stat.sz-md .ps-stat-value { font-size: var(--fs-h1); }
  .ps-stat.sz-lg .ps-stat-value { font-size: var(--fs-display); letter-spacing: var(--ls-display); }
  .ps-stat-value.up { color: var(--pnl-up); }
  .ps-stat-value.down { color: var(--pnl-down); }
  .ps-stat-value.gradient {
    background: var(--gradient-brand); -webkit-background-clip: text;
    background-clip: text; color: transparent;
  }
  .ps-stat-foot { display: inline-flex; align-items: center; gap: var(--sp-2); font: var(--text-body); font-size: var(--fs-subhead); color: var(--text-secondary); }
  .ps-stat-sub { font-size: var(--fs-subhead); color: var(--text-tertiary); }
  `;
  document.head.appendChild(s);
}
function StatTile({
  label,
  value,
  size = "md",
  tone = "default",
  delta,
  sub,
  icon,
  className = "",
  ...rest
}) {
  const valueCls = ["ps-stat-value", tone === "up" ? "up" : tone === "down" ? "down" : tone === "gradient" ? "gradient" : ""].filter(Boolean).join(" ");
  return React.createElement("div", {
    className: ["ps-stat", `sz-${size}`, className].filter(Boolean).join(" "),
    ...rest
  }, label ? React.createElement("span", {
    className: "ps-stat-label"
  }, icon || null, label) : null, React.createElement("span", {
    className: valueCls
  }, value), delta || sub ? React.createElement("span", {
    className: "ps-stat-foot"
  }, delta || null, sub ? React.createElement("span", {
    className: "ps-stat-sub"
  }, sub) : null) : null);
}
Object.assign(__ds_scope, { StatTile });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/StatTile.jsx", error: String((e && e.message) || e) }); }

// components/data/TrendValue.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-trendvalue-css")) {
  const s = document.createElement("style");
  s.id = "ps-trendvalue-css";
  s.textContent = `
  .ps-trend {
    display: inline-flex; align-items: center; gap: 3px;
    font: var(--fw-semibold) var(--fs-subhead)/1 var(--font-sans);
    font-variant-numeric: tabular-nums;
  }
  .ps-trend.up { color: var(--pnl-up); }
  .ps-trend.down { color: var(--pnl-down); }
  .ps-trend.flat { color: var(--text-tertiary); }
  .ps-trend.chip {
    height: 22px; padding: 0 var(--sp-2) 0 var(--sp-1); border-radius: var(--r-pill);
  }
  .ps-trend.chip.up { background: var(--pnl-up-soft); }
  .ps-trend.chip.down { background: var(--pnl-down-soft); }
  .ps-trend-arrow { font-size: 0.85em; line-height: 1; }
  `;
  document.head.appendChild(s);
}
function TrendValue({
  value,
  percent = false,
  prefix = "",
  chip = false,
  showSign = true,
  className = "",
  ...rest
}) {
  const n = Number(value);
  const dir = n > 0 ? "up" : n < 0 ? "down" : "flat";
  const arrow = n > 0 ? "▲" : n < 0 ? "▼" : "·";
  const abs = Math.abs(n);
  const num = percent ? abs.toFixed(1) + "%" : abs.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  const sign = !showSign ? "" : n > 0 ? "+" : n < 0 ? "−" : "";
  const cls = ["ps-trend", dir, chip ? "chip" : "", className].filter(Boolean).join(" ");
  return React.createElement("span", {
    className: cls,
    ...rest
  }, React.createElement("span", {
    className: "ps-trend-arrow"
  }, arrow), React.createElement("span", null, sign + prefix + num));
}
Object.assign(__ds_scope, { TrendValue });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/TrendValue.jsx", error: String((e && e.message) || e) }); }

// components/data/WalletAddress.jsx
try { (() => {
const {
  useState
} = React;
if (typeof document !== "undefined" && !document.getElementById("ps-wallet-css")) {
  const s = document.createElement("style");
  s.id = "ps-wallet-css";
  s.textContent = `
  .ps-wallet {
    display: inline-flex; align-items: center; gap: var(--sp-2);
    font-family: var(--font-mono); font-size: var(--fs-subhead);
    color: var(--text-primary); letter-spacing: 0;
  }
  .ps-wallet-addr {
    background: none; border: 0; padding: 0; cursor: pointer;
    color: inherit; font: inherit; text-decoration: none;
  }
  .ps-wallet-addr:hover { color: var(--accent); }
  .ps-wallet-copy {
    display: inline-flex; align-items: center; justify-content: center;
    width: 22px; height: 22px; border-radius: var(--r-xs);
    border: 0; background: transparent; color: var(--text-tertiary);
    cursor: pointer; transition: var(--t-hover);
  }
  .ps-wallet-copy:hover { background: var(--surface-inset); color: var(--text-primary); }
  .ps-wallet-copy.copied { color: var(--pnl-up); }
  `;
  document.head.appendChild(s);
}
function shorten(addr) {
  if (!addr) return "";
  return addr.length > 12 ? `${addr.slice(0, 6)}…${addr.slice(-4)}` : addr;
}
function WalletAddress({
  address,
  href,
  copyable = true,
  className = "",
  ...rest
}) {
  const [copied, setCopied] = useState(false);
  const text = shorten(address);
  const onCopy = () => {
    try {
      navigator.clipboard && navigator.clipboard.writeText(address);
    } catch (e) {}
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  };
  return React.createElement("span", {
    className: ["ps-wallet", className].filter(Boolean).join(" "),
    ...rest
  }, React.createElement(href ? "a" : "button", href ? {
    className: "ps-wallet-addr",
    href,
    target: "_blank",
    rel: "noreferrer"
  } : {
    className: "ps-wallet-addr",
    type: "button"
  }, text), copyable ? React.createElement("button", {
    className: "ps-wallet-copy" + (copied ? " copied" : ""),
    type: "button",
    "aria-label": "复制地址",
    title: "复制",
    onClick: onCopy
  }, copied ? "✓" : "⧉") : null);
}
Object.assign(__ds_scope, { WalletAddress });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/WalletAddress.jsx", error: String((e && e.message) || e) }); }

// components/data/WinRateRing.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-winratering-css")) {
  const s = document.createElement("style");
  s.id = "ps-winratering-css";
  s.textContent = `
  .ps-wrring { display: inline-flex; flex-direction: column; align-items: center; gap: var(--sp-3); }
  .ps-wrring-ring { position: relative; display: grid; place-items: center; }
  .ps-wrring-ring svg { display: block; transform: rotate(-90deg); }
  .ps-wrring-track { fill: none; stroke: var(--surface-inset); }
  .ps-wrring-win { fill: none; stroke: var(--pnl-up); stroke-linecap: round; transition: stroke-dasharray var(--dur-slow) var(--ease-out); }
  .ps-wrring-loss { fill: none; stroke: var(--pnl-down); stroke-linecap: round; transition: stroke-dasharray var(--dur-slow) var(--ease-out); }
  .ps-wrring-center {
    position: absolute; inset: 0; display: flex; flex-direction: column;
    align-items: center; justify-content: center; gap: 1px; pointer-events: none;
  }
  .ps-wrring-pct {
    font-family: var(--font-sans); font-weight: var(--fw-bold);
    color: var(--text-primary); line-height: 1;
    letter-spacing: var(--ls-title); font-variant-numeric: tabular-nums;
    display: inline-flex; align-items: baseline;
  }
  .ps-wrring-pct i { font-style: normal; font-weight: var(--fw-semibold); color: var(--text-tertiary); margin-left: 1px; }
  .ps-wrring-label {
    font-weight: var(--fw-semibold); color: var(--text-tertiary);
    text-transform: uppercase; letter-spacing: var(--ls-label); line-height: 1;
  }
  .ps-wrring-caption { display: inline-flex; align-items: center; gap: var(--sp-2); font: var(--text-body); font-size: var(--fs-subhead); color: var(--text-secondary); }
  .ps-wrring-legend { display: inline-flex; align-items: center; gap: var(--sp-4); font-size: var(--fs-footnote); color: var(--text-tertiary); font-variant-numeric: tabular-nums; }
  .ps-wrring-legend b { color: var(--text-secondary); font-weight: var(--fw-semibold); }
  .ps-wrring-legend .dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; margin-right: 5px; vertical-align: middle; }
  .ps-wrring-legend .dot.win { background: var(--pnl-up); }
  .ps-wrring-legend .dot.loss { background: var(--pnl-down); }
  `;
  document.head.appendChild(s);
}
const PS_WRRING_SIZES = {
  sm: {
    box: 92,
    stroke: 9,
    fs: 22,
    sub: 9,
    gap: 9
  },
  md: {
    box: 128,
    stroke: 11,
    fs: 30,
    sub: 11,
    gap: 10
  },
  lg: {
    box: 176,
    stroke: 14,
    fs: 44,
    sub: 13,
    gap: 12
  }
};
function WinRateRing({
  value,
  wins,
  losses,
  size = "md",
  label = "胜率",
  caption,
  legend = false,
  decimals = 0,
  className = "",
  ...rest
}) {
  let v = value;
  if (v == null && (wins != null || losses != null)) {
    const w = wins || 0,
      l = losses || 0;
    v = w + l ? w / (w + l) * 100 : 0;
  }
  v = Math.max(0, Math.min(100, v || 0));
  const S = PS_WRRING_SIZES[size] || PS_WRRING_SIZES.md;
  const r = (S.box - S.stroke) / 2 - 1;
  const c = 2 * Math.PI * r;
  const frac = v / 100;
  const full = v >= 99.5;
  const empty = v <= 0.5;
  const gap = full || empty ? 0 : S.gap;
  const avail = c - 2 * gap;
  const winLen = avail * frac;
  const lossLen = avail * (1 - frac);
  const cx = S.box / 2;
  const display = decimals > 0 ? v.toFixed(decimals) : Math.round(v);
  return React.createElement("div", {
    className: ["ps-wrring", className].filter(Boolean).join(" "),
    ...rest
  }, React.createElement("div", {
    className: "ps-wrring-ring",
    style: {
      width: S.box,
      height: S.box
    }
  }, React.createElement("svg", {
    width: S.box,
    height: S.box,
    viewBox: `0 0 ${S.box} ${S.box}`
  }, React.createElement("circle", {
    className: "ps-wrring-track",
    cx,
    cy: cx,
    r,
    strokeWidth: S.stroke
  }), !empty ? React.createElement("circle", {
    className: "ps-wrring-win",
    cx,
    cy: cx,
    r,
    strokeWidth: S.stroke,
    strokeDasharray: `${winLen} ${c - winLen}`,
    strokeDashoffset: 0
  }) : null, !full ? React.createElement("circle", {
    className: "ps-wrring-loss",
    cx,
    cy: cx,
    r,
    strokeWidth: S.stroke,
    strokeDasharray: `${lossLen} ${c - lossLen}`,
    strokeDashoffset: -(winLen + gap)
  }) : null), React.createElement("div", {
    className: "ps-wrring-center"
  }, React.createElement("span", {
    className: "ps-wrring-pct",
    style: {
      fontSize: S.fs
    }
  }, display, React.createElement("i", {
    style: {
      fontSize: S.fs * 0.45
    }
  }, "%")), label ? React.createElement("span", {
    className: "ps-wrring-label",
    style: {
      fontSize: S.sub
    }
  }, label) : null)), caption ? React.createElement("div", {
    className: "ps-wrring-caption"
  }, caption) : null, legend && (wins != null || losses != null) ? React.createElement("div", {
    className: "ps-wrring-legend"
  }, React.createElement("span", null, React.createElement("span", {
    className: "dot win"
  }), "胜 ", React.createElement("b", null, wins || 0)), React.createElement("span", null, React.createElement("span", {
    className: "dot loss"
  }), "负 ", React.createElement("b", null, losses || 0))) : null);
}
Object.assign(__ds_scope, { WinRateRing });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/data/WinRateRing.jsx", error: String((e && e.message) || e) }); }

// components/feedback/StatusPill.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-statuspill-css")) {
  const s = document.createElement("style");
  s.id = "ps-statuspill-css";
  s.textContent = `
  .ps-statuspill {
    display: inline-flex; align-items: center; gap: var(--sp-2);
    height: 28px; padding: 0 var(--sp-3) 0 var(--sp-3);
    border-radius: var(--r-pill); white-space: nowrap;
    background: var(--surface-inset); border: 1px solid var(--border-hairline);
    font: var(--fw-semibold) var(--fs-subhead)/1 var(--font-sans); color: var(--text-secondary);
  }
  .ps-statuspill .ps-status-dot {
    width: 8px; height: 8px; border-radius: 50%; background: var(--status-idle); flex: none;
  }
  .ps-statuspill .ps-status-extra { color: var(--text-tertiary); font-weight: var(--fw-medium); }
  .ps-statuspill.live { color: var(--pnl-up); background: var(--pnl-up-soft); border-color: transparent; }
  .ps-statuspill.live .ps-status-dot { background: var(--status-live); box-shadow: 0 0 0 0 rgba(31,157,87,0.6); animation: ps-pulse 1.8s var(--ease-out) infinite; }
  .ps-statuspill.warn { color: var(--status-warn); background: var(--status-warn-soft); border-color: transparent; }
  .ps-statuspill.warn .ps-status-dot { background: var(--status-warn); }
  .ps-statuspill.paused { color: var(--text-secondary); }
  .ps-statuspill.paused .ps-status-dot { background: var(--status-warn); }
  @keyframes ps-pulse {
    0% { box-shadow: 0 0 0 0 rgba(31,157,87,0.5); }
    70% { box-shadow: 0 0 0 7px rgba(31,157,87,0); }
    100% { box-shadow: 0 0 0 0 rgba(31,157,87,0); }
  }
  @media (prefers-reduced-motion: reduce) {
    .ps-statuspill.live .ps-status-dot { animation: none; }
  }
  `;
  document.head.appendChild(s);
}
const STATUS_TEXT = {
  live: "运行中",
  idle: "未运行",
  warn: "需处理",
  paused: "已暂停"
};
function StatusPill({
  status = "idle",
  label,
  extra,
  className = "",
  ...rest
}) {
  const cls = ["ps-statuspill", status, className].filter(Boolean).join(" ");
  return React.createElement("span", {
    className: cls,
    ...rest
  }, React.createElement("span", {
    className: "ps-status-dot"
  }), React.createElement("span", null, label || STATUS_TEXT[status] || status), extra ? React.createElement("span", {
    className: "ps-status-extra"
  }, extra) : null);
}
Object.assign(__ds_scope, { StatusPill });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/feedback/StatusPill.jsx", error: String((e && e.message) || e) }); }

// components/forms/Input.jsx
try { (() => {
const {
  forwardRef
} = React;
if (typeof document !== "undefined" && !document.getElementById("ps-input-css")) {
  const s = document.createElement("style");
  s.id = "ps-input-css";
  s.textContent = `
  .ps-field { display: inline-flex; flex-direction: column; gap: var(--sp-2); }
  .ps-field.block { display: flex; width: 100%; }
  .ps-field-label { font: var(--text-label); text-transform: uppercase; letter-spacing: var(--ls-label); color: var(--text-tertiary); }
  .ps-input-wrap {
    display: flex; align-items: center; gap: var(--sp-2);
    height: var(--control-md); padding: 0 var(--sp-4);
    border-radius: var(--r-md);
    background: var(--surface-inset);
    border: 1px solid var(--border-hairline);
    transition: var(--t-hover);
  }
  .ps-input-wrap:focus-within { border-color: var(--accent); box-shadow: 0 0 0 4px var(--accent-soft); }
  .ps-input-wrap .ps-input-affix { color: var(--text-tertiary); display: inline-flex; font-size: var(--fs-callout); }
  .ps-input {
    flex: 1; min-width: 0; border: 0; background: transparent; outline: none;
    color: var(--text-primary); font: var(--text-body); font-size: var(--fs-callout);
    font-variant-numeric: tabular-nums;
  }
  .ps-input::placeholder { color: var(--text-quaternary); }
  .ps-field.invalid .ps-input-wrap { border-color: var(--pnl-down); box-shadow: 0 0 0 4px var(--pnl-down-soft); }
  `;
  document.head.appendChild(s);
}
const Input = forwardRef(function Input({
  label,
  prefix,
  suffix,
  invalid = false,
  block = false,
  className = "",
  ...rest
}, ref) {
  const wrapCls = ["ps-field", block ? "block" : "", invalid ? "invalid" : "", className].filter(Boolean).join(" ");
  return React.createElement("label", {
    className: wrapCls
  }, label ? React.createElement("span", {
    className: "ps-field-label"
  }, label) : null, React.createElement("div", {
    className: "ps-input-wrap"
  }, prefix ? React.createElement("span", {
    className: "ps-input-affix"
  }, prefix) : null, React.createElement("input", {
    ref,
    className: "ps-input",
    ...rest
  }), suffix ? React.createElement("span", {
    className: "ps-input-affix"
  }, suffix) : null));
});
Object.assign(__ds_scope, { Input });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Input.jsx", error: String((e && e.message) || e) }); }

// components/forms/SegmentedControl.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-segmented-css")) {
  const s = document.createElement("style");
  s.id = "ps-segmented-css";
  s.textContent = `
  .ps-segmented {
    display: inline-flex; align-items: center; gap: 2px;
    padding: 3px; border-radius: var(--r-pill);
    background: var(--surface-inset);
    border: 1px solid var(--border-hairline);
  }
  .ps-segmented-opt {
    appearance: none; border: 0; cursor: pointer;
    display: inline-flex; align-items: center; gap: var(--sp-2);
    height: 30px; padding: 0 var(--sp-4);
    border-radius: var(--r-pill);
    background: transparent; color: var(--text-secondary);
    font: var(--fw-semibold) var(--fs-subhead)/1 var(--font-sans);
    transition: var(--t-hover);
    white-space: nowrap;
  }
  .ps-segmented-opt:hover { color: var(--text-primary); }
  .ps-segmented-opt.active {
    background: var(--surface-card); color: var(--text-primary);
    box-shadow: var(--shadow-sm), var(--inner-highlight);
  }
  .ps-segmented-opt .ps-seg-count {
    font-variant-numeric: tabular-nums; color: var(--text-tertiary);
    font-weight: var(--fw-semibold);
  }
  .ps-segmented-opt.active .ps-seg-count { color: var(--accent); }
  `;
  document.head.appendChild(s);
}
function SegmentedControl({
  options = [],
  value,
  onChange,
  className = "",
  ...rest
}) {
  const cls = ["ps-segmented", className].filter(Boolean).join(" ");
  return React.createElement("div", {
    className: cls,
    role: "tablist",
    ...rest
  }, options.map(opt => {
    const o = typeof opt === "string" ? {
      value: opt,
      label: opt
    } : opt;
    const active = o.value === value;
    return React.createElement("button", {
      key: o.value,
      type: "button",
      role: "tab",
      "aria-selected": active,
      className: "ps-segmented-opt" + (active ? " active" : ""),
      onClick: e => onChange && onChange(o.value, e)
    }, o.label, o.count != null ? React.createElement("span", {
      className: "ps-seg-count"
    }, o.count) : null);
  }));
}
Object.assign(__ds_scope, { SegmentedControl });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/SegmentedControl.jsx", error: String((e && e.message) || e) }); }

// components/forms/Switch.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-switch-css")) {
  const s = document.createElement("style");
  s.id = "ps-switch-css";
  s.textContent = `
  .ps-switch { display: inline-flex; align-items: center; gap: var(--sp-3); cursor: pointer; user-select: none; appearance: none; -webkit-appearance: none; background: none; border: 0; padding: 0; margin: 0; font: inherit; color: inherit; }
  .ps-switch.disabled { opacity: 0.4; pointer-events: none; }
  .ps-switch-track {
    position: relative; box-sizing: border-box;
    width: 50px; height: 30px; border-radius: var(--r-pill);
    background: var(--surface-inset);
    box-shadow: inset 0 1px 3px rgba(0,0,0,0.28);
    transition: background var(--dur-base) var(--ease-out), box-shadow var(--dur-base) var(--ease-out);
    flex: none;
  }
  .ps-switch-thumb {
    position: absolute; top: 4px; left: 4px; width: 22px; height: 22px;
    border-radius: 50%; background: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,0.45), 0 1px 1px rgba(0,0,0,0.25);
    transition: transform var(--dur-base) var(--ease-spring);
  }
  .ps-switch.on .ps-switch-track { background: var(--pnl-up); box-shadow: 0 2px 10px var(--pnl-up-soft); }
  .ps-switch.on.accent .ps-switch-track { background: var(--gradient-brand); box-shadow: var(--shadow-accent); }
  .ps-switch.on .ps-switch-thumb { transform: translateX(20px); }
  .ps-switch-label { font: var(--text-body); font-size: var(--fs-callout); color: var(--text-primary); }
  `;
  document.head.appendChild(s);
}
function Switch({
  checked = false,
  onChange,
  disabled = false,
  accent = false,
  label,
  className = "",
  ...rest
}) {
  const cls = ["ps-switch", checked ? "on" : "", accent ? "accent" : "", disabled ? "disabled" : "", className].filter(Boolean).join(" ");
  return React.createElement("button", {
    type: "button",
    role: "switch",
    "aria-checked": checked,
    className: cls,
    onClick: e => onChange && onChange(!checked, e),
    ...rest
  }, React.createElement("span", {
    className: "ps-switch-track"
  }, React.createElement("span", {
    className: "ps-switch-thumb"
  })), label ? React.createElement("span", {
    className: "ps-switch-label"
  }, label) : null);
}
Object.assign(__ds_scope, { Switch });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Switch.jsx", error: String((e && e.message) || e) }); }

// components/navigation/SidebarNav.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-sidebar-css")) {
  const s = document.createElement("style");
  s.id = "ps-sidebar-css";
  s.textContent = `
  .ps-sidebar {
    display: flex; flex-direction: column;
    width: var(--sidebar-w); flex: none; height: 100%;
    padding: var(--sp-6) var(--sp-4);
    background: var(--surface-sidebar);
    border-right: 1px solid var(--border-hairline);
    -webkit-backdrop-filter: var(--blur-thick); backdrop-filter: var(--blur-thick);
  }
  .ps-brand { display: flex; align-items: center; gap: var(--sp-3); padding: var(--sp-2) var(--sp-3) var(--sp-6); }
  .ps-brand-mark {
    width: 38px; height: 38px; border-radius: var(--r-sm); flex: none;
    position: relative; background: var(--gradient-brand);
    box-shadow: var(--shadow-accent), var(--inner-highlight-strong);
  }
  .ps-brand-mark::before {
    content: ""; position: absolute; inset: 0; margin: auto; width: 62%; height: 62%;
    background: rgba(255,255,255,0.96);
    -webkit-mask: url("data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2024%2024'%3E%3Cpath%20fill-rule='evenodd'%20d='M12%202c2.7%202.4%204%205.8%204%209.4V15H8v-3.6C8%207.8%209.3%204.4%2012%202Zm0%204.4a1.9%201.9%200%201%200%200%203.8%201.9%201.9%200%200%200%200-3.8Z'/%3E%3Cpath%20d='M8%2012.6%205.2%2017l2.8-1.1Z'/%3E%3Cpath%20d='M16%2012.6%2018.8%2017l-2.8-1.1Z'/%3E%3Cpath%20d='M10.3%2015.8%2012%2020l1.7-4.2a4%204%200%200%201-3.4%200Z'/%3E%3C/svg%3E") center/contain no-repeat;
    mask: url("data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2024%2024'%3E%3Cpath%20fill-rule='evenodd'%20d='M12%202c2.7%202.4%204%205.8%204%209.4V15H8v-3.6C8%207.8%209.3%204.4%2012%202Zm0%204.4a1.9%201.9%200%201%200%200%203.8%201.9%201.9%200%200%200%200-3.8Z'/%3E%3Cpath%20d='M8%2012.6%205.2%2017l2.8-1.1Z'/%3E%3Cpath%20d='M16%2012.6%2018.8%2017l-2.8-1.1Z'/%3E%3Cpath%20d='M10.3%2015.8%2012%2020l1.7-4.2a4%204%200%200%201-3.4%200Z'/%3E%3C/svg%3E") center/contain no-repeat;
  }
  .ps-brand-ring { display: none; }
  .ps-brand-name { display: flex; flex-direction: column; line-height: 1.1; }
  .ps-brand-name b { font: var(--fw-bold) var(--fs-h4)/1.1 var(--font-sans); letter-spacing: var(--ls-body); color: var(--text-primary); }
  .ps-brand-name span { font: var(--fw-semibold) var(--fs-caption)/1.2 var(--font-sans); letter-spacing: var(--ls-label); text-transform: uppercase; color: var(--text-tertiary); }

  .ps-nav-group-label { font: var(--text-label); text-transform: uppercase; letter-spacing: var(--ls-label); color: var(--text-quaternary); padding: var(--sp-4) var(--sp-3) var(--sp-2); }
  .ps-nav { display: flex; flex-direction: column; gap: 2px; }
  .ps-nav-item {
    display: flex; align-items: center; gap: var(--sp-3);
    padding: 0 var(--sp-3); height: 44px; border-radius: var(--r-md);
    border: 0; background: transparent; cursor: pointer; width: 100%;
    color: var(--text-secondary); text-align: left;
    font: var(--fw-medium) var(--fs-callout)/1 var(--font-sans);
    transition: var(--t-hover);
  }
  .ps-nav-item:hover { background: var(--surface-inset); color: var(--text-primary); }
  .ps-nav-item .ps-nav-ico { width: 20px; height: 20px; flex: none; display: inline-flex; align-items: center; justify-content: center; opacity: 0.85; }
  .ps-nav-item .ps-nav-ico svg { width: 20px; height: 20px; }
  .ps-nav-item .ps-nav-label { flex: 1; min-width: 0; }
  .ps-nav-item .ps-nav-count {
    font-variant-numeric: tabular-nums; font-size: var(--fs-footnote);
    color: var(--text-tertiary); font-weight: var(--fw-semibold);
  }
  .ps-nav-item.active {
    background: var(--gradient-brand-soft); color: var(--text-primary);
    box-shadow: var(--inner-highlight);
  }
  .ps-nav-item.active .ps-nav-ico { opacity: 1; color: var(--accent); }
  .ps-nav-item.active .ps-nav-count { color: var(--accent); }
  .ps-nav-item.active::before {
    content: ""; width: 3px; height: 20px; border-radius: var(--r-pill);
    background: var(--gradient-brand); margin-left: -4px; margin-right: 1px;
  }
  .ps-sidebar-foot { margin-top: auto; padding: var(--sp-4) var(--sp-3) 0; }
  `;
  document.head.appendChild(s);
}
function SidebarNav({
  items = [],
  value,
  onChange,
  groupLabel,
  brand = true,
  brandName = "Poly Sniper",
  brandSub = "ESPORTS COPY-TRADING",
  footer,
  className = "",
  ...rest
}) {
  return React.createElement("aside", {
    className: ["ps-sidebar", className].filter(Boolean).join(" "),
    ...rest
  }, brand ? React.createElement("div", {
    className: "ps-brand"
  }, React.createElement("div", {
    className: "ps-brand-mark"
  }, React.createElement("span", {
    className: "ps-brand-ring"
  })), React.createElement("div", {
    className: "ps-brand-name"
  }, React.createElement("b", null, brandName), React.createElement("span", null, brandSub))) : null, groupLabel ? React.createElement("div", {
    className: "ps-nav-group-label"
  }, groupLabel) : null, React.createElement("nav", {
    className: "ps-nav"
  }, items.map(it => React.createElement("button", {
    key: it.id,
    type: "button",
    className: "ps-nav-item" + (it.id === value ? " active" : ""),
    onClick: e => onChange && onChange(it.id, e)
  }, it.icon ? React.createElement("span", {
    className: "ps-nav-ico"
  }, it.icon) : null, React.createElement("span", {
    className: "ps-nav-label"
  }, it.label), it.count != null ? React.createElement("span", {
    className: "ps-nav-count"
  }, it.count) : null))), footer ? React.createElement("div", {
    className: "ps-sidebar-foot"
  }, footer) : null);
}
Object.assign(__ds_scope, { SidebarNav });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/navigation/SidebarNav.jsx", error: String((e && e.message) || e) }); }

// components/navigation/Tabs.jsx
try { (() => {
if (typeof document !== "undefined" && !document.getElementById("ps-tabs-css")) {
  const s = document.createElement("style");
  s.id = "ps-tabs-css";
  s.textContent = `
  .ps-tabs { display: inline-flex; align-items: center; gap: var(--sp-5); border-bottom: 1px solid var(--border-hairline); }
  .ps-tab {
    position: relative; appearance: none; border: 0; background: transparent; cursor: pointer;
    padding: var(--sp-3) 2px; color: var(--text-secondary);
    font: var(--fw-semibold) var(--fs-callout)/1 var(--font-sans);
    display: inline-flex; align-items: center; gap: var(--sp-2);
  }
  .ps-tab:hover { color: var(--text-primary); }
  .ps-tab .ps-tab-count { font-variant-numeric: tabular-nums; font-size: var(--fs-footnote); color: var(--text-tertiary); }
  .ps-tab.active { color: var(--text-primary); }
  .ps-tab.active .ps-tab-count { color: var(--accent); }
  .ps-tab.active::after {
    content: ""; position: absolute; left: 0; right: 0; bottom: -1px; height: 2px;
    border-radius: var(--r-pill); background: var(--gradient-brand);
  }
  `;
  document.head.appendChild(s);
}
function Tabs({
  tabs = [],
  value,
  onChange,
  className = "",
  ...rest
}) {
  return React.createElement("div", {
    className: ["ps-tabs", className].filter(Boolean).join(" "),
    role: "tablist",
    ...rest
  }, tabs.map(t => {
    const tab = typeof t === "string" ? {
      id: t,
      label: t
    } : t;
    const active = tab.id === value;
    return React.createElement("button", {
      key: tab.id,
      type: "button",
      role: "tab",
      "aria-selected": active,
      className: "ps-tab" + (active ? " active" : ""),
      onClick: e => onChange && onChange(tab.id, e)
    }, tab.label, tab.count != null ? React.createElement("span", {
      className: "ps-tab-count"
    }, tab.count) : null);
  }));
}
Object.assign(__ds_scope, { Tabs });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/navigation/Tabs.jsx", error: String((e && e.message) || e) }); }

// ui_kits/dashboard/app.jsx
try { (() => {
/* Poly Sniper dashboard — single-scope app (avoids cross-file babel scope collisions). */
const {
  SidebarNav,
  Tabs,
  Card,
  Button,
  IconButton,
  Switch,
  SegmentedControl,
  StatTile,
  TrendValue,
  RankBadge,
  WalletAddress,
  GameIcon,
  Badge,
  StatusPill,
  WinRateRing,
  CategoryDonut
} = window.PolySniperDesignSystem_8d05e5;
const D = window.PS_DATA;
const C = D.teamColors;
const ASSET_BASE = "../../assets"; // game logos live at project-root assets/

/* ---------- formatters & helpers ---------- */
const money = n => (n < 0 ? "-$" : "$") + Math.abs(n).toLocaleString(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const signedMoney = n => (n > 0 ? "+$" : n < 0 ? "-$" : "$") + Math.abs(n).toLocaleString(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const compactMoney = n => Math.abs(n) >= 1000 ? "$" + (n / 1000).toFixed(1) + "K" : "$" + n.toFixed(0);
const usdInt = n => "$" + Math.floor(Math.max(0, Number(n) || 0)).toLocaleString();
const pnlClass = n => n > 0 ? "pnl-up" : n < 0 ? "pnl-down" : "pnl-flat";

/* ---------- Pager (Apple-minimal, functional) ---------- */
function pageList(cur, total) {
  if (total <= 7) return Array.from({
    length: total
  }, (_, i) => i + 1);
  const s = new Set([1, total, cur, cur - 1, cur + 1]);
  if (cur <= 3) [2, 3, 4].forEach(n => s.add(n));
  if (cur >= total - 2) [total - 1, total - 2, total - 3].forEach(n => s.add(n));
  const arr = [...s].filter(n => n >= 1 && n <= total).sort((a, b) => a - b);
  const out = [];
  arr.forEach((n, i) => {
    if (i > 0 && n - arr[i - 1] > 1) out.push("…");
    out.push(n);
  });
  return out;
}
const Chevron = ({
  dir
}) => /*#__PURE__*/React.createElement("svg", {
  viewBox: "0 0 24 24",
  width: "16",
  height: "16",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: "2.2",
  strokeLinecap: "round",
  strokeLinejoin: "round",
  "aria-hidden": "true"
}, dir === "left" ? /*#__PURE__*/React.createElement("path", {
  d: "M15 18l-6-6 6-6"
}) : /*#__PURE__*/React.createElement("path", {
  d: "M9 18l6-6-6-6"
}));
function Pager({
  total,
  pageSize,
  page,
  onChange,
  unit = "条"
}) {
  const pages = Math.max(1, Math.ceil(total / pageSize));
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = Math.min(total, page * pageSize);
  return /*#__PURE__*/React.createElement("div", {
    className: "pager"
  }, /*#__PURE__*/React.createElement("span", {
    className: "pager-range"
  }, from, "\u2013", to, /*#__PURE__*/React.createElement("em", null, " / \u5171 ", total, " ", unit)), /*#__PURE__*/React.createElement("div", {
    className: "pager-nav"
  }, /*#__PURE__*/React.createElement("button", {
    className: "pg-btn pg-arrow",
    disabled: page <= 1,
    onClick: () => onChange(page - 1),
    "aria-label": "\u4E0A\u4E00\u9875"
  }, /*#__PURE__*/React.createElement(Chevron, {
    dir: "left"
  })), pageList(page, pages).map((n, i) => n === "…" ? /*#__PURE__*/React.createElement("span", {
    key: "e" + i,
    className: "pg-ellipsis"
  }, "\u2026") : /*#__PURE__*/React.createElement("button", {
    key: n,
    className: "pg-btn" + (n === page ? " is-active" : ""),
    onClick: () => onChange(n),
    "aria-current": n === page ? "page" : undefined
  }, n)), /*#__PURE__*/React.createElement("button", {
    className: "pg-btn pg-arrow",
    disabled: page >= pages,
    onClick: () => onChange(page + 1),
    "aria-label": "\u4E0B\u4E00\u9875"
  }, /*#__PURE__*/React.createElement(Chevron, {
    dir: "right"
  }))));
}
const initials = name => {
  const p = name.split(/\s+/).filter(Boolean);
  return (p.length === 1 ? p[0].slice(0, 2) : p[0][0] + p[1][0]).toUpperCase();
};
function TeamMonogram({
  name,
  size = 26
}) {
  return /*#__PURE__*/React.createElement("span", {
    className: "team-mono",
    style: {
      width: size,
      height: size,
      "--team": C[name] || "var(--accent)"
    }
  }, initials(name));
}
function TeamLine({
  ev,
  size = 26
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "team-line"
  }, /*#__PURE__*/React.createElement("span", {
    className: "team"
  }, /*#__PURE__*/React.createElement(TeamMonogram, {
    name: ev.teamA,
    size: size
  }), /*#__PURE__*/React.createElement("span", {
    className: "team-name"
  }, ev.teamA)), /*#__PURE__*/React.createElement("span", {
    className: "vs"
  }, "vs"), /*#__PURE__*/React.createElement("span", {
    className: "team"
  }, /*#__PURE__*/React.createElement(TeamMonogram, {
    name: ev.teamB,
    size: size
  }), /*#__PURE__*/React.createElement("span", {
    className: "team-name"
  }, ev.teamB)));
}
function MatchCell({
  ev
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "match-cell"
  }, /*#__PURE__*/React.createElement("div", {
    className: "match-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: ev.game,
    base: ASSET_BASE,
    chip: true
  }), /*#__PURE__*/React.createElement("span", {
    className: "match-meta"
  }, ev.meta)), /*#__PURE__*/React.createElement(TeamLine, {
    ev: ev
  }), /*#__PURE__*/React.createElement("div", {
    className: "match-times"
  }, /*#__PURE__*/React.createElement("span", null, "\u5F00\u59CB ", ev.start), /*#__PURE__*/React.createElement("span", {
    className: "dot-sep"
  }, "\xB7"), /*#__PURE__*/React.createElement("span", null, "\u622A\u6B62 ", ev.end)));
}
function EquityArea({
  points,
  width = 560,
  height = 120
}) {
  const min = Math.min(...points),
    max = Math.max(...points),
    span = max - min || 1;
  const stepX = width / (points.length - 1);
  const xy = points.map((p, i) => [i * stepX, height - (p - min) / span * (height - 12) - 6]);
  const line = xy.map((p, i) => (i === 0 ? "M" : "L") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  const area = line + ` L ${width} ${height} L 0 ${height} Z`;
  const last = xy[xy.length - 1];
  return /*#__PURE__*/React.createElement("svg", {
    className: "equity-svg",
    viewBox: `0 0 ${width} ${height}`,
    preserveAspectRatio: "none",
    width: "100%",
    height: height
  }, /*#__PURE__*/React.createElement("defs", null, /*#__PURE__*/React.createElement("linearGradient", {
    id: "eqFill",
    x1: "0",
    y1: "0",
    x2: "0",
    y2: "1"
  }, /*#__PURE__*/React.createElement("stop", {
    offset: "0%",
    stopColor: "rgba(31,157,87,0.28)"
  }), /*#__PURE__*/React.createElement("stop", {
    offset: "100%",
    stopColor: "rgba(31,157,87,0)"
  })), /*#__PURE__*/React.createElement("linearGradient", {
    id: "eqLine",
    x1: "0",
    y1: "0",
    x2: "1",
    y2: "0"
  }, /*#__PURE__*/React.createElement("stop", {
    offset: "0%",
    stopColor: "#ff8a5f"
  }), /*#__PURE__*/React.createElement("stop", {
    offset: "100%",
    stopColor: "#1f9d57"
  }))), /*#__PURE__*/React.createElement("path", {
    d: area,
    fill: "url(#eqFill)"
  }), /*#__PURE__*/React.createElement("path", {
    d: line,
    fill: "none",
    stroke: "url(#eqLine)",
    strokeWidth: "2.5",
    strokeLinecap: "round",
    strokeLinejoin: "round"
  }), /*#__PURE__*/React.createElement("circle", {
    cx: last[0],
    cy: last[1],
    r: "4",
    fill: "#1f9d57"
  }), /*#__PURE__*/React.createElement("circle", {
    cx: last[0],
    cy: last[1],
    r: "8",
    fill: "rgba(31,157,87,0.22)"
  }));
}
function SplitBar({
  a,
  b
}) {
  const total = a + b || 1;
  return /*#__PURE__*/React.createElement("div", {
    className: "split-bar",
    title: `${a} : ${b}`
  }, /*#__PURE__*/React.createElement("span", {
    className: "split-a",
    style: {
      width: a / total * 100 + "%"
    }
  }), /*#__PURE__*/React.createElement("span", {
    className: "split-b",
    style: {
      width: b / total * 100 + "%"
    }
  }));
}
const qualityBadge = q => q === "clean" ? /*#__PURE__*/React.createElement(Badge, {
  tone: "up"
}, "\u5355\u5411") : q === "contested" ? /*#__PURE__*/React.createElement(Badge, {
  tone: "warn"
}, "\u5206\u6B67") : /*#__PURE__*/React.createElement(Badge, {
  tone: "warn",
  outline: true
}, "\u53CC\u8FB9");

/* ---------- Overview ---------- */
function OverviewPage({
  onNav
}) {
  const o = D.overview;
  const nav = onNav || (() => {});
  const [distMetric, setDistMetric] = React.useState("count");
  const ft = D.followTypes;
  const distSegments = ft.segments.map(s => ({
    ...s,
    value: distMetric === "stake" ? s.stake : s.value
  }));
  const distCenter = distMetric === "stake" ? compactMoney(ft.totalStake) : ft.total;
  const distLabel = distMetric === "stake" ? "总投入" : "跟单笔数";
  const distTotal = distSegments.reduce((a, s) => a + (s.value || 0), 0) || 1;
  const distMarkets = [...new Set(distSegments.map(s => s.label))];
  const distMap = {};
  distSegments.forEach(s => {
    distMap[s.group + "|" + s.label] = s;
  });
  const distGames = [...new Set(distSegments.map(s => s.group))].map(group => ({
    group,
    gameId: (distSegments.find(s => s.group === group) || {}).gameId
  }));
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-grid"
  }, /*#__PURE__*/React.createElement(Card, {
    glow: true,
    pad: "lg",
    className: "ov-herocard"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-hero"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-hero-top"
  }, /*#__PURE__*/React.createElement(StatTile, {
    size: "lg",
    tone: "up",
    label: "\u5DF2\u7ED3\u7B97\u76C8\u4E8F",
    value: signedMoney(o.realizedPnl),
    delta: /*#__PURE__*/React.createElement(TrendValue, {
      value: o.realizedRoi,
      percent: true,
      chip: true
    }),
    sub: `累计投入 ${money(o.totalStake)}`
  }), /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    extra: "02:14:08"
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-metricbar"
  }, /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5DF2\u7ED3\u7B97 ROI"), /*#__PURE__*/React.createElement("b", {
    className: "pnl-up"
  }, "+", o.realizedRoi, "%")), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u7ED3\u7B97\u573A\u6B21"), /*#__PURE__*/React.createElement("b", null, o.settledCount)), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5F53\u524D\u6301\u4ED3"), /*#__PURE__*/React.createElement("b", null, money(o.openExposure))), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u94B1\u5305\u4F59\u989D"), /*#__PURE__*/React.createElement("b", null, money(o.walletBalance)))), /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist-head"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u76D8\u53E3\u7ED3\u6784"), /*#__PURE__*/React.createElement("h3", {
    className: "ov-herodist-title"
  }, "\u5386\u53F2\u8DDF\u5355\u7C7B\u578B\u5206\u5E03")), /*#__PURE__*/React.createElement(SegmentedControl, {
    value: distMetric,
    onChange: setDistMetric,
    options: [{
      value: "count",
      label: "按笔数"
    }, {
      value: "stake",
      label: "按金额"
    }]
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-herodist-body"
  }, /*#__PURE__*/React.createElement(CategoryDonut, {
    size: 112,
    thickness: 18,
    centerValue: distCenter,
    centerLabel: distLabel,
    segments: distSegments
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-distmatrix"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-dm-row ov-dm-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-dm-game"
  }), distMarkets.map(m => /*#__PURE__*/React.createElement("span", {
    key: m,
    className: "ov-dm-cell"
  }, m))), distGames.map(g => /*#__PURE__*/React.createElement("div", {
    key: g.group,
    className: "ov-dm-row"
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-dm-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: g.gameId,
    size: "sm",
    base: ASSET_BASE
  }), " ", g.group), distMarkets.map(m => {
    const s = distMap[g.group + "|" + m];
    return /*#__PURE__*/React.createElement("span", {
      key: m,
      className: "ov-dm-cell"
    }, /*#__PURE__*/React.createElement("i", {
      className: "ov-dm-sw",
      style: {
        background: s ? s.color : "transparent"
      }
    }), s ? Math.round(s.value / distTotal * 100) : 0, "%");
  })))))))), /*#__PURE__*/React.createElement(Card, {
    className: "ov-rightcard"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-twostat"
  }, /*#__PURE__*/React.createElement(StatTile, {
    label: "\u76D1\u63A7\u8D5B\u4E8B",
    value: /*#__PURE__*/React.createElement("button", {
      type: "button",
      className: "ov-link-num",
      onClick: () => nav("events")
    }, o.watchedEvents, /*#__PURE__*/React.createElement("i", {
      "data-lucide": "arrow-up-right"
    })),
    sub: "esports"
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-twostat-div"
  }), /*#__PURE__*/React.createElement(StatTile, {
    label: "\u8FDB\u884C\u4E2D\u8DDF\u5355",
    tone: "gradient",
    value: /*#__PURE__*/React.createElement("button", {
      type: "button",
      className: "ov-link-num",
      onClick: () => nav("follows")
    }, o.openFollows, /*#__PURE__*/React.createElement("i", {
      "data-lucide": "arrow-up-right"
    })),
    sub: `${o.openByGame.length} 个项目`
  })), /*#__PURE__*/React.createElement("div", {
    className: "ov-openlist"
  }, o.openByGame.map(g => /*#__PURE__*/React.createElement("button", {
    type: "button",
    key: g.game,
    className: "ov-openrow",
    onClick: () => nav("follows")
  }, /*#__PURE__*/React.createElement("span", {
    className: "ov-openrow-game"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: g.game,
    size: "sm",
    base: ASSET_BASE
  }), " ", g.name), /*#__PURE__*/React.createElement("span", {
    className: "ov-openrow-count"
  }, g.count, " \u573A")))), /*#__PURE__*/React.createElement("div", {
    className: "ov-qualityblock"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-qualityblock-head"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u76D8\u53E3\u7ED3\u6784"), /*#__PURE__*/React.createElement("h3", {
    className: "ov-qtitle"
  }, "\u8DDF\u5355\u8D28\u91CF")), /*#__PURE__*/React.createElement("div", {
    className: "ov-quality"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-qcell"
  }, /*#__PURE__*/React.createElement("span", {
    className: "qv pnl-flat"
  }, o.cleanCount), /*#__PURE__*/React.createElement("span", {
    className: "ql"
  }, "\u5355\u5411\u76D8"), /*#__PURE__*/React.createElement("small", null, "\u65E0\u53CC\u8FB9 / \u5206\u6B67")), /*#__PURE__*/React.createElement("div", {
    className: "ov-qcell"
  }, /*#__PURE__*/React.createElement("span", {
    className: "qv",
    style: {
      color: "var(--status-warn)"
    }
  }, o.twoSidedCount + o.disagreementCount), /*#__PURE__*/React.createElement("span", {
    className: "ql"
  }, "\u53CC\u8FB9 / \u5206\u6B67\u76D8"), /*#__PURE__*/React.createElement("small", null, "\u53CC\u8FB9 ", o.twoSidedCount, " \xB7 \u5206\u6B67 ", o.disagreementCount)))))), /*#__PURE__*/React.createElement(Card, {
    eyebrow: "\u8DDF\u5355\u80DC\u7387",
    title: "\u6574\u4F53\u4E0E\u5206\u9879\u76EE\u8868\u73B0"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-hero"
  }, /*#__PURE__*/React.createElement(WinRateRing, {
    size: "lg",
    wins: o.winRate.wins,
    losses: o.winRate.losses,
    label: "\u80DC\u7387",
    legend: true
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-hero-meta"
  }, /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u603B\u573A\u6B21"), /*#__PURE__*/React.createElement("b", null, o.winRate.wins + o.winRate.losses)), /*#__PURE__*/React.createElement("div", {
    className: "m"
  }, /*#__PURE__*/React.createElement("span", null, "\u5DF2\u7ED3\u7B97\u76C8\u4E8F"), /*#__PURE__*/React.createElement("b", {
    className: "pnl-up"
  }, signedMoney(o.realizedPnl))))), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-divider"
  }), /*#__PURE__*/React.createElement("div", {
    className: "ov-winrate-games"
  }, D.winRates.map(g => /*#__PURE__*/React.createElement(WinRateRing, {
    key: g.game,
    size: "sm",
    wins: g.wins,
    losses: g.losses,
    label: "\u80DC\u7387",
    caption: /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(GameIcon, {
      game: g.game,
      size: "sm",
      base: ASSET_BASE
    }), " ", g.name)
  }))))), /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-4)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "sec-head",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("h2", {
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, "\u6700\u8FD1\u8DDF\u5355"), /*#__PURE__*/React.createElement(Badge, {
    tone: "accent"
  }, D.follows.length, " \u6761"))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305"), /*#__PURE__*/React.createElement("th", null, "\u6295\u5165"), /*#__PURE__*/React.createElement("th", null, "\u76C8\u4E8F"), /*#__PURE__*/React.createElement("th", null, "\u8D28\u91CF"))), /*#__PURE__*/React.createElement("tbody", null, D.follows.slice(0, 5).map(f => /*#__PURE__*/React.createElement("tr", {
    key: f.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: f
  })), /*#__PURE__*/React.createElement("td", null, f.status === "open" ? /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }) : /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, f.wallets), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, money(f.stake)), /*#__PURE__*/React.createElement("td", {
    className: pnlClass(f.pnl)
  }, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong"
  }, signedMoney(f.pnl)), f.pnlKind === "unrealized" && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u5B9E\u73B0"))), /*#__PURE__*/React.createElement("td", null, qualityBadge(f.quality)))))))));
}

/* ---------- Leaderboard ---------- */
function LeaderboardPage() {
  const [view, setView] = React.useState("active");
  const [favs, setFavs] = React.useState(() => {
    const s = {};
    D.wallets.forEach(w => s[w.addr] = w.fav);
    return s;
  });
  const toggleFav = addr => setFavs(p => ({
    ...p,
    [addr]: !p[addr]
  }));
  const [isolated, setIsolated] = React.useState({});
  const isolate = w => setIsolated(p => ({
    ...p,
    [w.addr]: {
      reason: "手动隔离",
      reasonTime: "刚刚"
    }
  }));
  const restore = addr => setIsolated(p => {
    const n = {
      ...p
    };
    delete n[addr];
    return n;
  });
  const activeWallets = D.wallets.filter(w => !isolated[w.addr]);
  const manualQ = D.wallets.filter(w => isolated[w.addr]).map(w => ({
    ...w,
    manual: true,
    reason: isolated[w.addr].reason,
    reasonTime: isolated[w.addr].reasonTime
  }));
  const quarantinedRows = [...manualQ, ...D.quarantined];
  const favRows = activeWallets.filter(w => favs[w.addr]);
  const rows = view === "quarantined" ? quarantinedRows : view === "favorite" ? favRows : activeWallets;
  const q = view === "quarantined";
  const PAGE = 5;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [view]);
  React.useEffect(() => {
    window.lucide && window.lucide.createIcons();
  });
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: view,
    onChange: setView,
    options: [{
      value: "active",
      label: "活跃",
      count: activeWallets.length
    }, {
      value: "favorite",
      label: "收藏",
      count: favRows.length
    }, {
      value: "quarantined",
      label: "隔离",
      count: quarantinedRows.length
    }]
  }), /*#__PURE__*/React.createElement("div", {
    className: "sec-actions"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sec-sub",
    style: {
      marginRight: 4
    }
  }, "\u66F4\u65B0 14:08 \xB7 30 \u4E2A\u6838\u5FC3 A \u7EA7"), /*#__PURE__*/React.createElement(Button, {
    variant: "primary",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "radar",
      style: {
        width: 16,
        height: 16
      }
    })
  }, "\u91C7\u6837\u94B1\u5305")))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, !q && /*#__PURE__*/React.createElement("th", null), /*#__PURE__*/React.createElement("th", null, "Rank"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305"), q && /*#__PURE__*/React.createElement("th", null, "\u9694\u79BB\u539F\u56E0"), /*#__PURE__*/React.createElement("th", null, "\u8BC4\u5206"), /*#__PURE__*/React.createElement("th", null, "\u4E13\u7CBE ROI"), /*#__PURE__*/React.createElement("th", null, "\u573A\u5747\u4EA4\u6613\u989D"), /*#__PURE__*/React.createElement("th", null, "\u8FD1\u671F"), /*#__PURE__*/React.createElement("th", null, "\u4E13\u7CBE"), !q && /*#__PURE__*/React.createElement("th", null, "\u8DDF\u5355\u80DC\u8D1F"), !q && /*#__PURE__*/React.createElement("th", null, "\u8DDF\u5355 PnL"), /*#__PURE__*/React.createElement("th", null, "\u6700\u540E\u4EA4\u6613"), /*#__PURE__*/React.createElement("th", null))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map((w, i) => /*#__PURE__*/React.createElement("tr", {
    key: w.addr,
    className: "clickable"
  }, !q && /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("button", {
    className: "fav-btn" + (favs[w.addr] ? " on" : ""),
    onClick: e => {
      e.stopPropagation();
      toggleFav(w.addr);
    },
    "aria-label": "\u6536\u85CF"
  }, favs[w.addr] ? "★" : "☆")), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(RankBadge, {
    rank: w.rank || (cur - 1) * PAGE + i + 1
  })), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(WalletAddress, {
    address: w.addr,
    copyable: false
  })), q && /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong",
    style: {
      color: "var(--status-warn)"
    }
  }, w.reason), /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, w.reasonTime))), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, w.score), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: pnlClass(w.roi) + " strong"
  }, w.roi > 0 ? "+" : "", w.roi, "%"), w.overallRoi != null && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u5168\u90E8 +", w.overallRoi, "%"))), /*#__PURE__*/React.createElement("td", {
    className: "num",
    title: money(w.avgCash)
  }, compactMoney(w.avgCash)), /*#__PURE__*/React.createElement("td", null, w.recent != null ? /*#__PURE__*/React.createElement(TrendValue, {
    value: w.recent,
    percent: true
  }) : /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u2013")), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "scope-list"
  }, w.scope.map((s, j) => /*#__PURE__*/React.createElement("span", {
    key: j,
    className: "scope-item"
  }, /*#__PURE__*/React.createElement(GameIcon, {
    game: s.game,
    base: ASSET_BASE,
    size: "sm"
  }), /*#__PURE__*/React.createElement("span", null, s.market))))), !q && /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, w.followRec), !q && /*#__PURE__*/React.createElement("td", {
    className: pnlClass(w.followPnl) + " num strong"
  }, signedMoney(w.followPnl)), /*#__PURE__*/React.createElement("td", {
    className: "muted"
  }, w.lastTrade), /*#__PURE__*/React.createElement("td", {
    className: "row-action"
  }, !q && /*#__PURE__*/React.createElement(Button, {
    variant: "danger",
    size: "sm",
    className: "tbl-action danger",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "circle-minus",
      style: {
        width: 12,
        height: 12
      }
    }),
    onClick: e => {
      e.stopPropagation();
      isolate(w);
    }
  }, "\u9694\u79BB"), q && w.manual && /*#__PURE__*/React.createElement(Button, {
    variant: "secondary",
    size: "sm",
    className: "tbl-action restore",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "rotate-ccw",
      style: {
        width: 12,
        height: 12
      }
    }),
    onClick: e => {
      e.stopPropagation();
      restore(w.addr);
    }
  }, "\u6062\u590D")))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "12",
    className: "empty-cell"
  }, "\u6682\u65E0", view === "favorite" ? "收藏" : view === "quarantined" ? "隔离" : "活跃", "\u94B1\u5305"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u94B1\u5305"
  }))));
}

/* ---------- Events ---------- */
function EventsPage() {
  const [tab, setTab] = React.useState("active");
  const [game, setGame] = React.useState("all");
  const base = tab === "archive" ? D.archive : D.events;
  const rows = game === "all" ? base : base.filter(e => e.game === game);
  const archive = tab === "archive";
  const PAGE = 4;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [tab, game]);
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement(Tabs, {
    value: tab,
    onChange: setTab,
    tabs: [{
      id: "active",
      label: "进行中 / 即将开始",
      count: D.events.length
    }, {
      id: "archive",
      label: "已结算",
      count: D.archive.length
    }]
  }), /*#__PURE__*/React.createElement("div", {
    className: "filter-group"
  }, /*#__PURE__*/React.createElement("label", {
    htmlFor: "game-f"
  }, "\u9879\u76EE"), /*#__PURE__*/React.createElement("select", {
    id: "game-f",
    className: "ps-select",
    value: game,
    onChange: e => setGame(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: "all"
  }, "\u5168\u90E8"), /*#__PURE__*/React.createElement("option", {
    value: "dota2"
  }, "Dota 2"), /*#__PURE__*/React.createElement("option", {
    value: "cs2"
  }, "CS2"), /*#__PURE__*/React.createElement("option", {
    value: "lol"
  }, "LoL"), /*#__PURE__*/React.createElement("option", {
    value: "valorant"
  }, "Valorant"))))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, archive ? "结算 PNL" : "跟单情况 (A : B)"))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map(e => /*#__PURE__*/React.createElement("tr", {
    key: e.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: e
  })), /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement("div", {
    className: "evt-status"
  }, e.status === "live" && /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }), e.status === "upcoming" && /*#__PURE__*/React.createElement(Badge, {
    tone: "accent",
    dot: true
  }, "\u5373\u5C06\u5F00\u59CB"), e.status === "settled" && /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97"), e.countdown && !archive && /*#__PURE__*/React.createElement("span", {
    className: "evt-count"
  }, e.countdown))), /*#__PURE__*/React.createElement("td", null, archive ? /*#__PURE__*/React.createElement("span", {
    className: pnlClass(e.pnl) + " strong num",
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, signedMoney(e.pnl)) : /*#__PURE__*/React.createElement("div", {
    className: "follow-count-line"
  }, /*#__PURE__*/React.createElement(SplitBar, {
    a: e.followA,
    b: e.followB
  }), /*#__PURE__*/React.createElement("span", null, /*#__PURE__*/React.createElement("b", {
    className: "ca"
  }, e.followA), " : ", /*#__PURE__*/React.createElement("b", {
    className: "cb"
  }, e.followB)))))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "3",
    className: "empty-cell"
  }, "\u5F53\u524D\u7A97\u53E3\u6682\u65E0\u76D1\u63A7\u8D5B\u4E8B"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u8D5B\u4E8B"
  }))));
}

/* ---------- Follows ---------- */
function FollowsPage({
  strategy: s,
  goStrategy
}) {
  const [status, setStatus] = React.useState("all");
  const rows = status === "all" ? D.follows : D.follows.filter(f => f.status === status);
  const dg = s ? strategyDigest(s) : null;
  const PAGE = 4;
  const [pg, setPg] = React.useState(1);
  React.useEffect(() => {
    setPg(1);
  }, [status]);
  const pages = Math.max(1, Math.ceil(rows.length / PAGE));
  const cur = Math.min(pg, pages);
  const pageRows = rows.slice((cur - 1) * PAGE, cur * PAGE);
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner"
  }, dg ? /*#__PURE__*/React.createElement("div", {
    className: "strat-banner"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sb-left"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sb-icon"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "crosshair"
  })), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "sb-k"
  }, "\u751F\u6548\u7B56\u7565"), /*#__PURE__*/React.createElement("span", {
    className: "sb-sizing"
  }, dg.sizing))), /*#__PURE__*/React.createElement("div", {
    className: "sb-chips"
  }, dg.chips.map(c => /*#__PURE__*/React.createElement("span", {
    className: "sb-chip",
    key: c
  }, c))), /*#__PURE__*/React.createElement(Button, {
    size: "sm",
    variant: "ghost",
    iconLeft: /*#__PURE__*/React.createElement("i", {
      "data-lucide": "sliders-horizontal"
    }),
    onClick: goStrategy
  }, "\u8C03\u6574\u7B56\u7565")) : null, /*#__PURE__*/React.createElement(Card, {
    pad: "flush"
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--sp-6) var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "panel-toolbar",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "sec-head",
    style: {
      marginBottom: 0
    }
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("h2", {
    style: {
      fontSize: "var(--fs-h4)"
    }
  }, "\u8DDF\u5355\u5217\u8868"), /*#__PURE__*/React.createElement("div", {
    className: "sec-sub"
  }, "\u6309\u76EE\u6807\u94B1\u5305\u4E70\u5165\u6BD4\u4F8B\u955C\u50CF\u5EFA\u4ED3"))), /*#__PURE__*/React.createElement("div", {
    className: "filter-group"
  }, /*#__PURE__*/React.createElement("label", {
    htmlFor: "st-f"
  }, "\u72B6\u6001"), /*#__PURE__*/React.createElement("select", {
    id: "st-f",
    className: "ps-select",
    value: status,
    onChange: e => setStatus(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: "all"
  }, "\u5168\u90E8"), /*#__PURE__*/React.createElement("option", {
    value: "open"
  }, "\u8FDB\u884C\u4E2D"), /*#__PURE__*/React.createElement("option", {
    value: "settled"
  }, "\u5DF2\u7ED3\u7B97"))))), /*#__PURE__*/React.createElement("div", {
    className: "tbl-wrap"
  }, /*#__PURE__*/React.createElement("table", {
    className: "ps-table"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("th", null, "\u8D5B\u4E8B"), /*#__PURE__*/React.createElement("th", null, "\u72B6\u6001"), /*#__PURE__*/React.createElement("th", null, "\u7ED3\u7B97"), /*#__PURE__*/React.createElement("th", null, "\u94B1\u5305\u6570"), /*#__PURE__*/React.createElement("th", null, "\u5355\u6570"), /*#__PURE__*/React.createElement("th", null, "\u6295\u5165"), /*#__PURE__*/React.createElement("th", null, "\u76C8\u4E8F"), /*#__PURE__*/React.createElement("th", null, "\u8D28\u91CF"))), /*#__PURE__*/React.createElement("tbody", {
    key: cur,
    className: "tbl-fade"
  }, pageRows.map(f => /*#__PURE__*/React.createElement("tr", {
    key: f.cid,
    className: "clickable"
  }, /*#__PURE__*/React.createElement("td", null, /*#__PURE__*/React.createElement(MatchCell, {
    ev: f
  })), /*#__PURE__*/React.createElement("td", null, f.status === "open" ? /*#__PURE__*/React.createElement(StatusPill, {
    status: "live",
    label: "\u8FDB\u884C\u4E2D"
  }) : /*#__PURE__*/React.createElement(Badge, {
    tone: "neutral"
  }, "\u5DF2\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", null, f.settlement === "盈利" ? /*#__PURE__*/React.createElement("span", {
    className: "pnl-up strong"
  }, "\u76C8\u5229") : f.settlement === "亏损" ? /*#__PURE__*/React.createElement("span", {
    className: "pnl-down strong"
  }, "\u4E8F\u635F") : /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u7ED3\u7B97")), /*#__PURE__*/React.createElement("td", {
    className: "strong"
  }, f.wallets), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, f.legs), /*#__PURE__*/React.createElement("td", {
    className: "num"
  }, money(f.stake)), /*#__PURE__*/React.createElement("td", {
    className: pnlClass(f.pnl)
  }, /*#__PURE__*/React.createElement("div", {
    className: "cell-stack"
  }, /*#__PURE__*/React.createElement("span", {
    className: "strong num"
  }, signedMoney(f.pnl)), f.pnlKind === "unrealized" && /*#__PURE__*/React.createElement("span", {
    className: "muted"
  }, "\u672A\u5B9E\u73B0"))), /*#__PURE__*/React.createElement("td", null, qualityBadge(f.quality)))), !rows.length && /*#__PURE__*/React.createElement("tr", null, /*#__PURE__*/React.createElement("td", {
    colSpan: "8",
    className: "empty-cell"
  }, "\u6682\u65E0\u8DDF\u5355\u8BB0\u5F55"))))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "0 var(--sp-6) var(--sp-5)"
    }
  }, /*#__PURE__*/React.createElement(Pager, {
    total: rows.length,
    pageSize: PAGE,
    page: cur,
    onChange: setPg,
    unit: "\u7B14"
  }))));
}

/* ---------- Shell ---------- */
const PAGES = {
  overview: {
    title: "概览",
    comp: OverviewPage
  },
  strategy: {
    title: "跟单策略",
    comp: StrategyPage
  },
  leaderboard: {
    title: "Leaderboard",
    comp: LeaderboardPage
  },
  events: {
    title: "关注赛事",
    comp: EventsPage
  },
  follows: {
    title: "跟单列表",
    comp: FollowsPage
  }
};
const STRATEGY_DEFAULTS = {
  usableMode: "all",
  usableCap: "5000",
  minSignalOn: true,
  minSignal: "10",
  sizing: "ratio",
  ratio: "10",
  ratioCapOn: false,
  ratioCap: "100",
  fixed: "50",
  balancePct: "1",
  countOn: false,
  countMode: "event",
  count: "10",
  spendOn: false,
  spendMode: "fixed",
  spendFixed: "200",
  spendPct: "5"
};
const clampNum = v => v.replace(/[^\d.]/g, "");
function strategyDigest(s) {
  const n = v => Number(v) || 0;
  const sizing = s.sizing === "ratio" ? `比例 ${s.ratio || 0}%${s.ratioCapOn ? `（封顶 ${usdInt(n(s.ratioCap))}）` : ""}` : s.sizing === "fixed" ? `固定 ${usdInt(n(s.fixed))}` : `余额 ${s.balancePct || 0}%`;
  const filter = `门槛 ${usdInt(n(s.minSignal))}`;
  const count = s.countOn ? s.countMode === "event" ? `单场 ${s.count} 笔` : `每钱包 ${s.count} 笔` : null;
  const spend = s.spendOn ? s.spendMode === "fixed" ? `单场 ≤ ${usdInt(n(s.spendFixed))}` : `单场 ≤ 余额 ${s.spendPct}%` : null;
  return {
    sizing,
    chips: [filter, count, spend].filter(Boolean)
  };
}
function NumField({
  value,
  onChange,
  unit,
  width = 76,
  lead,
  disabled
}) {
  return /*#__PURE__*/React.createElement("span", {
    className: "num-field" + (disabled ? " is-disabled" : "")
  }, lead ? /*#__PURE__*/React.createElement("span", {
    className: "nf-lead"
  }, lead) : null, /*#__PURE__*/React.createElement("input", {
    value: value,
    onChange: e => onChange(clampNum(e.target.value)),
    style: {
      width
    },
    inputMode: "decimal",
    disabled: disabled
  }), unit ? /*#__PURE__*/React.createElement("span", {
    className: "nf-unit"
  }, unit) : null);
}
function StageCard({
  no,
  title,
  sub,
  badge,
  children
}) {
  return /*#__PURE__*/React.createElement(Card, {
    pad: "lg"
  }, /*#__PURE__*/React.createElement("div", {
    className: "stage-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "stage-no"
  }, no), /*#__PURE__*/React.createElement("div", {
    className: "stage-titles"
  }, /*#__PURE__*/React.createElement("div", {
    className: "stage-title-row"
  }, /*#__PURE__*/React.createElement("h3", {
    className: "stage-title"
  }, title), badge), /*#__PURE__*/React.createElement("p", {
    className: "stage-sub"
  }, sub))), /*#__PURE__*/React.createElement("div", {
    className: "stage-body"
  }, children));
}
function SizingOption({
  id,
  active,
  onSelect,
  title,
  desc,
  children
}) {
  return /*#__PURE__*/React.createElement("div", {
    className: "opt-card" + (active ? " is-active" : ""),
    onClick: () => onSelect(id),
    role: "radio",
    "aria-checked": active
  }, /*#__PURE__*/React.createElement("div", {
    className: "opt-head"
  }, /*#__PURE__*/React.createElement("span", {
    className: "opt-radio",
    "aria-hidden": "true"
  }), /*#__PURE__*/React.createElement("div", {
    className: "opt-titles"
  }, /*#__PURE__*/React.createElement("span", {
    className: "opt-title"
  }, title), /*#__PURE__*/React.createElement("span", {
    className: "opt-desc"
  }, desc))), active && children ? /*#__PURE__*/React.createElement("div", {
    className: "opt-body",
    onClick: e => e.stopPropagation()
  }, children) : null);
}
function StrategyPage({
  strategy: s,
  setStrategy,
  running,
  setRunning
}) {
  const up = k => v => setStrategy(p => ({
    ...p,
    [k]: v
  }));
  const n = v => Number(v) || 0;
  const wallet = D.overview.walletBalance;
  const avail = s.usableMode === "cap" ? Math.min(n(s.usableCap), wallet) : wallet;
  const [sample, setSample] = React.useState("1200");
  const t = n(sample);
  let ex;
  if (t < n(s.minSignal)) {
    ex = {
      ignored: true,
      reason: `目标买入低于门槛 ${usdInt(n(s.minSignal))}`
    };
  } else {
    let raw, basis;
    if (s.sizing === "ratio") {
      raw = t * n(s.ratio) / 100;
      basis = `${s.ratio || 0}% × ${usdInt(t)}`;
      if (s.ratioCapOn && raw > n(s.ratioCap)) {
        raw = n(s.ratioCap);
        basis = `命中封顶 ${usdInt(n(s.ratioCap))}`;
      }
    } else if (s.sizing === "fixed") {
      raw = n(s.fixed);
      basis = "固定金额";
    } else {
      raw = avail * n(s.balancePct) / 100;
      basis = `${s.balancePct || 0}% × 可用 ${usdInt(avail)}`;
    }
    ex = {
      amount: Math.floor(Math.max(0, raw)),
      basis
    };
  }
  const sizingPrimary = s.sizing === "ratio" ? s.ratio : s.sizing === "fixed" ? s.fixed : s.balancePct;
  const issues = [];
  if (!(n(sizingPrimary) > 0)) issues.push("单笔金额");
  if (s.usableMode === "cap" && !(n(s.usableCap) > 0)) issues.push("可动用上限");
  if (!(n(s.minSignal) > 0)) issues.push("最小信号金额");
  if (s.sizing === "ratio" && s.ratioCapOn && !(n(s.ratioCap) > 0)) issues.push("单笔封顶金额");
  if (s.countOn && !(n(s.count) > 0)) issues.push("单场笔数");
  if (s.spendOn && !(n(s.spendMode === "fixed" ? s.spendFixed : s.spendPct) > 0)) issues.push("单场投入上限");
  const ready = issues.length === 0;
  const dg = strategyDigest(s);
  const ico = nm => /*#__PURE__*/React.createElement("i", {
    "data-lucide": nm
  });
  return /*#__PURE__*/React.createElement("div", {
    className: "page-inner strat-page"
  }, /*#__PURE__*/React.createElement("section", {
    className: "strat-overview"
  }, /*#__PURE__*/React.createElement("div", {
    className: "so-bar"
  }, /*#__PURE__*/React.createElement("div", {
    className: "so-title"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ps-card-eyebrow"
  }, "\u7B56\u7565\u603B\u89C8"), /*#__PURE__*/React.createElement("h2", {
    className: "so-h"
  }, running ? "跟单引擎运行中" : "策略待启动")), /*#__PURE__*/React.createElement("div", {
    className: "so-example"
  }, /*#__PURE__*/React.createElement("span", {
    className: "soe-label"
  }, "\u793A\u4F8B\u63A8\u6F14"), /*#__PURE__*/React.createElement("span", {
    className: "soe-row"
  }, "\u76EE\u6807\u4E70\u5165", /*#__PURE__*/React.createElement(NumField, {
    value: sample,
    onChange: setSample,
    unit: "USDC",
    width: 58
  }), /*#__PURE__*/React.createElement("i", {
    "data-lucide": "arrow-right",
    className: "soe-arrow"
  }), ex.ignored ? /*#__PURE__*/React.createElement("span", {
    className: "soe-out ignored"
  }, "\u4FE1\u53F7\u5FFD\u7565") : /*#__PURE__*/React.createElement("span", {
    className: "soe-out"
  }, "\u5B9E\u9645\u8DDF\u5355 ", /*#__PURE__*/React.createElement("b", null, usdInt(ex.amount))))), /*#__PURE__*/React.createElement("div", {
    className: "so-right"
  }, /*#__PURE__*/React.createElement(StatusPill, {
    status: running ? "live" : "idle",
    label: running ? "运行中" : "已停止",
    extra: running ? "02:14:08" : undefined
  }), /*#__PURE__*/React.createElement(Button, {
    variant: "ghost",
    size: "sm",
    iconLeft: ico("rotate-ccw"),
    onClick: () => setStrategy({
      ...STRATEGY_DEFAULTS
    })
  }, "\u91CD\u7F6E"), running ? /*#__PURE__*/React.createElement(Button, {
    variant: "danger",
    iconLeft: /*#__PURE__*/React.createElement("svg", {
      viewBox: "0 0 24 24",
      width: "15",
      height: "15",
      fill: "currentColor",
      "aria-hidden": "true"
    }, /*#__PURE__*/React.createElement("rect", {
      x: "6",
      y: "6",
      width: "12",
      height: "12",
      rx: "2.5"
    })),
    onClick: () => setRunning(false)
  }, "\u505C\u6B62\u8DDF\u5355") : /*#__PURE__*/React.createElement(Button, {
    variant: "primary",
    iconLeft: /*#__PURE__*/React.createElement("svg", {
      viewBox: "0 0 24 24",
      width: "15",
      height: "15",
      fill: "currentColor",
      "aria-hidden": "true"
    }, /*#__PURE__*/React.createElement("path", {
      d: "M7 4.5v15a1 1 0 0 0 1.5.87l13-7.5a1 1 0 0 0 0-1.74l-13-7.5A1 1 0 0 0 7 4.5z"
    })),
    disabled: !ready,
    onClick: () => setRunning(true)
  }, ready ? "启动跟单" : "待完善"))), /*#__PURE__*/React.createElement("ol", {
    className: "so-pipe"
  }, [{
    k: "资金池",
    v: s.usableMode === "all" ? "全部余额" : `上限 ${usdInt(n(s.usableCap))}`
  }, {
    k: "信号门槛",
    v: `忽略 < ${usdInt(n(s.minSignal))}`
  }, {
    k: "单笔金额",
    v: dg.sizing,
    key: true
  }, {
    k: "单场笔数",
    v: s.countOn ? s.countMode === "event" ? `整场 ${s.count} 笔` : `每钱包 ${s.count} 笔` : "不限"
  }, {
    k: "单场投入",
    v: s.spendOn ? s.spendMode === "fixed" ? `≤ ${usdInt(n(s.spendFixed))}` : `≤ 余额 ${s.spendPct}%` : "不限"
  }].map(nd => /*#__PURE__*/React.createElement("li", {
    key: nd.k,
    className: "so-node" + (nd.key ? " is-key" : "")
  }, /*#__PURE__*/React.createElement("span", {
    className: "son-k"
  }, nd.k), /*#__PURE__*/React.createElement("span", {
    className: "son-v"
  }, nd.v)))), !ready && !running ? /*#__PURE__*/React.createElement("div", {
    className: "so-todo"
  }, ico("circle-alert"), " \u5F85\u5B8C\u5584\u5FC5\u586B\u9879\uFF1A", issues.join("、")) : null), /*#__PURE__*/React.createElement(Card, {
    pad: "lg"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-split"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-col"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-mini"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cm-head"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "wallet"
  }), /*#__PURE__*/React.createElement("span", null, "\u53EF\u52A8\u7528\u8D44\u91D1")), /*#__PURE__*/React.createElement("div", {
    className: "cm-body"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.usableMode,
    onChange: up("usableMode"),
    options: [{
      value: "all",
      label: "全部余额"
    }, {
      value: "cap",
      label: "指定上限"
    }]
  }), s.usableMode === "cap" ? /*#__PURE__*/React.createElement(NumField, {
    value: s.usableCap,
    onChange: up("usableCap"),
    unit: "USDC",
    width: 84
  }) : /*#__PURE__*/React.createElement("span", {
    className: "mc-note"
  }, "\u94B1\u5305 ", money(wallet)))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-head"
  }, /*#__PURE__*/React.createElement("h3", null, "\u5355\u7B14\u8DDF\u5355\u91D1\u989D"), /*#__PURE__*/React.createElement(Badge, {
    tone: "up",
    outline: true
  }, "\u5FC5\u586B")), /*#__PURE__*/React.createElement("p", {
    className: "cfg-sub"
  }, "\u6BCF\u4E2A\u6709\u6548\u4FE1\u53F7\u4E70\u5165\u591A\u5C11 \xB7 \u4E09\u9009\u4E00\uFF0C\u91D1\u989D\u5411\u4E0B\u53D6\u6574\u89C4\u907F\u4E0B\u5355\u5F02\u5E38"), /*#__PURE__*/React.createElement("div", {
    className: "opt-list"
  }, /*#__PURE__*/React.createElement(SizingOption, {
    id: "ratio",
    active: s.sizing === "ratio",
    onSelect: up("sizing"),
    title: "\u6309\u76EE\u6807\u6BD4\u4F8B",
    desc: "\u8DDF\u968F\u76EE\u6807\u94B1\u5305\u4E70\u5165\u989D\u7684\u56FA\u5B9A\u6BD4\u4F8B"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ratio-rows"
  }, /*#__PURE__*/React.createElement("span", {
    className: "rr-lead"
  }, /*#__PURE__*/React.createElement("span", {
    className: "rr-check-slot"
  }), "\u8DDF\u5355\u6BD4\u4F8B"), /*#__PURE__*/React.createElement(NumField, {
    value: s.ratio,
    onChange: up("ratio"),
    unit: "%",
    width: 56
  }), /*#__PURE__*/React.createElement("label", {
    className: "rr-lead is-check",
    onClick: e => e.stopPropagation()
  }, /*#__PURE__*/React.createElement("input", {
    type: "checkbox",
    checked: s.ratioCapOn,
    onChange: e => up("ratioCapOn")(e.target.checked)
  }), "\u5355\u7B14\u5C01\u9876"), /*#__PURE__*/React.createElement(NumField, {
    value: s.ratioCap,
    onChange: up("ratioCap"),
    unit: "USDC",
    width: 56,
    disabled: !s.ratioCapOn
  }))), /*#__PURE__*/React.createElement(SizingOption, {
    id: "fixed",
    active: s.sizing === "fixed",
    onSelect: up("sizing"),
    title: "\u56FA\u5B9A\u91D1\u989D",
    desc: "\u6BCF\u7B14\u4E70\u5165\u56FA\u5B9A\u91D1\u989D\uFF0C\u4E0E\u76EE\u6807\u4E0B\u5355\u989D\u65E0\u5173"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.fixed,
    onChange: up("fixed"),
    unit: "USDC",
    lead: "\u6BCF\u7B14\u4E70\u5165"
  }))), /*#__PURE__*/React.createElement(SizingOption, {
    id: "balancePct",
    active: s.sizing === "balancePct",
    onSelect: up("sizing"),
    title: "\u6309\u672C\u91D1\u767E\u5206\u6BD4",
    desc: "\u6309\u5F53\u524D\u53EF\u52A8\u7528\u4F59\u989D\u7684\u767E\u5206\u6BD4\u52A8\u6001\u4E70\u5165"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.balancePct,
    onChange: up("balancePct"),
    unit: "%",
    lead: "\u6BCF\u7B14\u5360\u7528"
  }))))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-col"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cfg-mini"
  }, /*#__PURE__*/React.createElement("div", {
    className: "cm-head"
  }, /*#__PURE__*/React.createElement("i", {
    "data-lucide": "filter"
  }), /*#__PURE__*/React.createElement("span", null, "\u4FE1\u53F7\u95E8\u69DB"), /*#__PURE__*/React.createElement(Badge, {
    tone: "up",
    outline: true
  }, "\u5FC5\u586B")), /*#__PURE__*/React.createElement("div", {
    className: "cm-body"
  }, /*#__PURE__*/React.createElement(NumField, {
    value: s.minSignal,
    onChange: up("minSignal"),
    unit: "USDC",
    lead: "\u5FFD\u7565\u76EE\u6807\u4E70\u5165 <",
    width: 64
  }))), /*#__PURE__*/React.createElement("div", {
    className: "cfg-head"
  }, /*#__PURE__*/React.createElement("h3", null, "\u5355\u573A\u98CE\u63A7\u4E0A\u9650")), /*#__PURE__*/React.createElement("p", {
    className: "cfg-sub"
  }, "\u5BF9\u5355\u573A\u8D5B\u4E8B\u7684\u7D2F\u8BA1\u8DDF\u5355\u8BBE\u9632 \xB7 \u4E24\u9879\u53EF\u72EC\u7ACB\u5F00\u542F"), /*#__PURE__*/React.createElement("div", {
    className: "sub-block"
  }, /*#__PURE__*/React.createElement("div", {
    className: "switch-row"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sr-text"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sr-title"
  }, "\u5355\u573A\u7B14\u6570\u4E0A\u9650"), /*#__PURE__*/React.createElement("span", {
    className: "sr-desc"
  }, "\u9650\u5236\u4E00\u573A\u8D5B\u4E8B\u7D2F\u8BA1\u53EF\u8DDF\u7684\u7B14\u6570")), /*#__PURE__*/React.createElement(Switch, {
    checked: s.countOn,
    onChange: v => up("countOn")(v),
    accent: true
  })), s.countOn ? /*#__PURE__*/React.createElement("div", {
    className: "sub-controls"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.countMode,
    onChange: up("countMode"),
    options: [{
      value: "event",
      label: "按赛事合计"
    }, {
      value: "wallet",
      label: "按每个钱包"
    }]
  }), /*#__PURE__*/React.createElement(NumField, {
    value: s.count,
    onChange: up("count"),
    unit: "\u7B14",
    width: 58
  }))) : null), /*#__PURE__*/React.createElement("div", {
    className: "sub-block"
  }, /*#__PURE__*/React.createElement("div", {
    className: "switch-row"
  }, /*#__PURE__*/React.createElement("div", {
    className: "sr-text"
  }, /*#__PURE__*/React.createElement("span", {
    className: "sr-title"
  }, "\u5355\u573A\u6295\u5165\u4E0A\u9650"), /*#__PURE__*/React.createElement("span", {
    className: "sr-desc"
  }, "\u9650\u5236\u4E00\u573A\u8D5B\u4E8B\u7684\u7D2F\u8BA1\u4E70\u5165\u91D1\u989D")), /*#__PURE__*/React.createElement(Switch, {
    checked: s.spendOn,
    onChange: v => up("spendOn")(v),
    accent: true
  })), s.spendOn ? /*#__PURE__*/React.createElement("div", {
    className: "sub-controls"
  }, /*#__PURE__*/React.createElement("div", {
    className: "ctrl-row"
  }, /*#__PURE__*/React.createElement(SegmentedControl, {
    value: s.spendMode,
    onChange: up("spendMode"),
    options: [{
      value: "fixed",
      label: "固定金额"
    }, {
      value: "balancePct",
      label: "余额百分比"
    }]
  }), s.spendMode === "fixed" ? /*#__PURE__*/React.createElement(NumField, {
    value: s.spendFixed,
    onChange: up("spendFixed"),
    unit: "USDC",
    width: 88
  }) : /*#__PURE__*/React.createElement(NumField, {
    value: s.spendPct,
    onChange: up("spendPct"),
    unit: "%",
    width: 58
  }))) : null)))));
}
function App() {
  const [page, setPage] = React.useState("overview");
  const [running, setRunning] = React.useState(true);
  const [light, setLight] = React.useState(true);
  const [strategy, setStrategy] = React.useState({
    ...STRATEGY_DEFAULTS
  });
  React.useEffect(() => {
    window.lucide && window.lucide.createIcons();
  });
  React.useEffect(() => {
    document.documentElement.setAttribute("data-theme", light ? "light" : "dark");
  }, [light]);
  const Page = PAGES[page].comp;
  const ico = n => /*#__PURE__*/React.createElement("i", {
    "data-lucide": n
  });
  return /*#__PURE__*/React.createElement("div", {
    className: "app-shell",
    "data-theme": light ? "light" : "dark"
  }, /*#__PURE__*/React.createElement(SidebarNav, {
    value: page,
    onChange: setPage,
    groupLabel: "\u5DE5\u4F5C\u53F0",
    items: [{
      id: "overview",
      label: "概览",
      icon: ico("layout-dashboard")
    }, {
      id: "strategy",
      label: "跟单策略",
      icon: ico("crosshair")
    }, {
      id: "leaderboard",
      label: "Leaderboard",
      icon: ico("trophy"),
      count: 30
    }, {
      id: "events",
      label: "关注赛事",
      icon: ico("swords"),
      count: 18
    }, {
      id: "follows",
      label: "跟单列表",
      icon: ico("list-checks"),
      count: 7
    }],
    footer: /*#__PURE__*/React.createElement("div", {
      className: "theme-toggle"
    }, /*#__PURE__*/React.createElement("span", null, ico(light ? "sun" : "moon"), " ", light ? "浅色" : "深色"), /*#__PURE__*/React.createElement(Switch, {
      checked: !light,
      onChange: () => setLight(v => !v),
      accent: true
    }))
  }), /*#__PURE__*/React.createElement("div", {
    className: "app-main"
  }, /*#__PURE__*/React.createElement("header", {
    className: "topbar"
  }, /*#__PURE__*/React.createElement("h1", {
    className: "topbar-title"
  }, PAGES[page].title), /*#__PURE__*/React.createElement("div", {
    className: "topbar-spacer"
  }), /*#__PURE__*/React.createElement("div", {
    className: "topbar-actions"
  }, /*#__PURE__*/React.createElement(StatusPill, {
    status: running ? "live" : "idle",
    label: running ? "运行中" : "已停止",
    extra: running ? "02:14:08" : undefined
  }))), /*#__PURE__*/React.createElement("div", {
    className: "page-scroll"
  }, /*#__PURE__*/React.createElement(Page, {
    strategy: strategy,
    setStrategy: setStrategy,
    running: running,
    setRunning: setRunning,
    goStrategy: () => setPage("strategy"),
    onNav: setPage
  }))));
}
ReactDOM.createRoot(document.getElementById("root")).render(/*#__PURE__*/React.createElement(App, null));
setTimeout(() => window.lucide && window.lucide.createIcons(), 80);
})(); } catch (e) { __ds_ns.__errors.push({ path: "ui_kits/dashboard/app.jsx", error: String((e && e.message) || e) }); }

// ui_kits/dashboard/data.js
try { (() => {
/* Mock data for the Poly Sniper dashboard UI kit. All fake, illustrative only. */

window.PS_DATA = function () {
  const overview = {
    realizedPnl: 2481.5,
    realizedRoi: 12.34,
    totalStake: 20100,
    settledCount: 42,
    openExposure: 3180,
    walletBalance: 8642.18,
    usable: 5000,
    watchedEvents: 18,
    openFollows: 7,
    // In-progress follows split across games (sum === openFollows)
    openByGame: [{
      game: "dota2",
      name: "Dota 2",
      count: 3
    }, {
      game: "cs2",
      name: "CS2",
      count: 2
    }, {
      game: "lol",
      name: "LoL",
      count: 2
    }],
    cleanCount: 14,
    twoSidedCount: 2,
    disagreementCount: 2,
    winRate: {
      wins: 38,
      losses: 14
    }
  };

  // Per-game copy-trading win rates (overview win-rate rings)
  const winRates = [{
    game: "dota2",
    name: "Dota 2",
    wins: 18,
    losses: 7
  }, {
    game: "lol",
    name: "LoL",
    wins: 9,
    losses: 5
  }, {
    game: "cs2",
    name: "CS2",
    wins: 11,
    losses: 2
  }];

  // Follow distribution by game & sub-market (overview category donut)
  const followTypes = {
    total: 62,
    totalStake: 20100,
    segments: [{
      group: "Dota 2",
      gameId: "dota2",
      label: "Moneyline",
      value: 14,
      stake: 5200,
      color: "#f0512f"
    }, {
      group: "Dota 2",
      gameId: "dota2",
      label: "Map Winner",
      value: 7,
      stake: 1850,
      color: "#ff9a72"
    }, {
      group: "CS2",
      gameId: "cs2",
      label: "Moneyline",
      value: 12,
      stake: 4400,
      color: "#d98a1e"
    }, {
      group: "CS2",
      gameId: "cs2",
      label: "Map Winner",
      value: 9,
      stake: 2600,
      color: "#f0c074"
    }, {
      group: "LoL",
      gameId: "lol",
      label: "Moneyline",
      value: 11,
      stake: 3650,
      color: "#1f7a73"
    }, {
      group: "LoL",
      gameId: "lol",
      label: "Map Winner",
      value: 9,
      stake: 2400,
      color: "#6cc0b8"
    }]
  };

  // 7-day equity curve points (cumulative realized pnl)
  const equity = [0, 180, 140, 520, 880, 760, 1340, 1180, 1720, 2060, 1980, 2481.5];
  const games = ["dota2", "cs2", "lol", "valorant"];
  const wallets = [{
    rank: 1,
    addr: "0x8f3c2a1b9d4e5f60718293a4b5c6d7e8f9012345",
    score: 96,
    roi: 28.4,
    overallRoi: 14.2,
    wins: 19,
    losses: 4,
    avgCash: 12480,
    recent: 22.1,
    scope: [{
      game: "dota2",
      market: "Moneyline"
    }, {
      game: "cs2",
      market: "Moneyline"
    }],
    settled: 9,
    open: 2,
    followRec: "6-1",
    followPnl: 412.8,
    lastTrade: "12分钟前",
    fav: true
  }, {
    rank: 2,
    addr: "0x2b71e9c4a8d3f50612839a4b5c6d7e8f90126789",
    score: 93,
    roi: 24.7,
    overallRoi: 11.8,
    wins: 31,
    losses: 9,
    avgCash: 8240,
    recent: 18.6,
    scope: [{
      game: "cs2",
      market: "Map 1"
    }],
    settled: 14,
    open: 1,
    followRec: "9-3",
    followPnl: 286.4,
    lastTrade: "3分钟前",
    fav: false
  }, {
    rank: 3,
    addr: "0x5d92f0a6b1c8e7430219384a5b6c7d8e9f013579",
    score: 91,
    roi: 21.2,
    overallRoi: 9.4,
    wins: 12,
    losses: 3,
    avgCash: 19600,
    recent: 15.3,
    scope: [{
      game: "lol",
      market: "Moneyline"
    }],
    settled: 5,
    open: 1,
    followRec: "4-1",
    followPnl: 198.2,
    lastTrade: "27分钟前",
    fav: true
  }, {
    rank: 4,
    addr: "0x1a40c7e2b9d6f8530412938a4b5c6d7e8f024680",
    score: 88,
    roi: 18.9,
    overallRoi: 8.1,
    wins: 22,
    losses: 8,
    avgCash: 6420,
    recent: -4.2,
    scope: [{
      game: "valorant",
      market: "Moneyline"
    }, {
      game: "cs2",
      market: "Moneyline"
    }],
    settled: 11,
    open: 0,
    followRec: "7-4",
    followPnl: -42.6,
    lastTrade: "1小时前",
    fav: false
  }, {
    rank: 5,
    addr: "0x9c08b3d1e6a4f7250318394a5b6c7d8e9f035791",
    score: 86,
    roi: 17.4,
    overallRoi: 7.7,
    wins: 16,
    losses: 6,
    avgCash: 9870,
    recent: 9.8,
    scope: [{
      game: "dota2",
      market: "Moneyline"
    }],
    settled: 7,
    open: 2,
    followRec: "5-2",
    followPnl: 156.0,
    lastTrade: "44分钟前",
    fav: false
  }, {
    rank: 6,
    addr: "0x4e15a8f2c7b9d6340218493a5b6c7d8e9f046802",
    score: 84,
    roi: 15.8,
    overallRoi: 6.9,
    wins: 27,
    losses: 12,
    avgCash: 5310,
    recent: 6.1,
    scope: [{
      game: "cs2",
      market: "Map 2"
    }, {
      game: "valorant",
      market: "Moneyline"
    }],
    settled: 13,
    open: 1,
    followRec: "8-5",
    followPnl: 88.4,
    lastTrade: "2小时前",
    fav: false
  }, {
    rank: 7,
    addr: "0x7b62d9e0a3c5f8140319483a5b6c7d8e9f057913",
    score: 82,
    roi: 14.2,
    overallRoi: 5.5,
    wins: 14,
    losses: 5,
    avgCash: 7640,
    recent: 11.4,
    scope: [{
      game: "lol",
      market: "Moneyline"
    }],
    settled: 6,
    open: 0,
    followRec: "4-2",
    followPnl: 64.2,
    lastTrade: "5小时前",
    fav: false
  }, {
    rank: 8,
    addr: "0x3f84c1b7e2a9d05603194a3b5c6d7e8f9f068024",
    score: 80,
    roi: 12.9,
    overallRoi: 4.8,
    wins: 20,
    losses: 9,
    avgCash: 4980,
    recent: -2.7,
    scope: [{
      game: "dota2",
      market: "Moneyline"
    }, {
      game: "lol",
      market: "Moneyline"
    }],
    settled: 10,
    open: 1,
    followRec: "6-3",
    followPnl: 31.8,
    lastTrade: "8小时前",
    fav: false
  }];
  const quarantined = [{
    rank: null,
    addr: "0xaa31f7c2e9b4d6850219384a5b6c7d8e9f079135",
    score: 71,
    roi: -8.4,
    reason: "同盘双边下注",
    reasonTime: "2小时前",
    avgCash: 3200,
    scope: [{
      game: "cs2",
      market: "Moneyline"
    }],
    lastTrade: "2小时前"
  }, {
    rank: null,
    addr: "0xbb52e8d3f0a5c7960318493a5b6c7d8e9f08a246",
    score: 64,
    roi: -12.1,
    reason: "尾盘高价进场",
    reasonTime: "6小时前",
    avgCash: 2740,
    scope: [{
      game: "valorant",
      market: "Moneyline"
    }],
    lastTrade: "6小时前"
  }];
  const events = [{
    cid: "e1",
    game: "dota2",
    teamA: "Team Spirit",
    teamB: "Falcons",
    meta: "BO3 · 半决赛",
    marketType: "Moneyline",
    start: "今天 20:00",
    end: "今天 23:30",
    status: "live",
    countdown: "进行中",
    followA: 4,
    followB: 1,
    pnl: null
  }, {
    cid: "e2",
    game: "cs2",
    teamA: "NAVI",
    teamB: "Vitality",
    meta: "BO3 · 总决赛",
    marketType: "Moneyline",
    start: "今天 21:30",
    end: "今天 24:00",
    status: "upcoming",
    countdown: "1小时后",
    followA: 3,
    followB: 2,
    pnl: null
  }, {
    cid: "e3",
    game: "lol",
    teamA: "T1",
    teamB: "Gen.G",
    meta: "BO5 · 季后赛",
    marketType: "Moneyline",
    start: "明天 17:00",
    end: "明天 21:00",
    status: "upcoming",
    countdown: "19小时后",
    followA: 2,
    followB: 0,
    pnl: null
  }, {
    cid: "e4",
    game: "valorant",
    teamA: "Paper Rex",
    teamB: "Fnatic",
    meta: "BO3 · 小组赛",
    marketType: "Map 1",
    start: "今天 18:00",
    end: "今天 20:30",
    status: "live",
    countdown: "进行中",
    followA: 1,
    followB: 3,
    pnl: null
  }, {
    cid: "e5",
    game: "dota2",
    teamA: "Team Liquid",
    teamB: "BetBoom",
    meta: "BO3 · 胜者组",
    marketType: "Moneyline",
    start: "明天 22:00",
    end: "后天 01:00",
    status: "upcoming",
    countdown: "1天后",
    followA: 0,
    followB: 2,
    pnl: null
  }];
  const archive = [{
    cid: "a1",
    game: "cs2",
    teamA: "FaZe",
    teamB: "G2",
    meta: "BO3 · 八强",
    marketType: "Moneyline",
    start: "06-10 19:00",
    end: "06-10 22:00",
    status: "settled",
    pnl: 318.4
  }, {
    cid: "a2",
    game: "dota2",
    teamA: "Gaimin Gladiators",
    teamB: "Tundra",
    meta: "BO3 · 败者组",
    marketType: "Moneyline",
    start: "06-09 20:00",
    end: "06-09 23:00",
    status: "settled",
    pnl: -86.2
  }, {
    cid: "a3",
    game: "lol",
    teamA: "JDG",
    teamB: "BLG",
    meta: "BO5 · 决赛",
    marketType: "Moneyline",
    start: "06-08 17:00",
    end: "06-08 21:30",
    status: "settled",
    pnl: 502.7
  }, {
    cid: "a4",
    game: "valorant",
    teamA: "Sentinels",
    teamB: "EG",
    meta: "BO3 · 半决赛",
    marketType: "Map 2",
    start: "06-07 18:00",
    end: "06-07 20:00",
    status: "settled",
    pnl: 144.0
  }];
  const follows = [{
    cid: "f1",
    game: "dota2",
    teamA: "Team Spirit",
    teamB: "Falcons",
    meta: "BO3 · 半决赛",
    marketType: "Moneyline",
    status: "open",
    settlement: "未结算",
    wallets: 5,
    legs: 11,
    stake: 880,
    pnl: 142.6,
    pnlKind: "unrealized",
    quality: "clean",
    start: "今天 20:00",
    end: "今天 23:30"
  }, {
    cid: "f2",
    game: "cs2",
    teamA: "NAVI",
    teamB: "Vitality",
    meta: "BO3 · 总决赛",
    marketType: "Moneyline",
    status: "open",
    settlement: "未结算",
    wallets: 4,
    legs: 8,
    stake: 640,
    pnl: -38.2,
    pnlKind: "unrealized",
    quality: "contested",
    start: "今天 21:30",
    end: "今天 24:00"
  }, {
    cid: "f3",
    game: "lol",
    teamA: "JDG",
    teamB: "BLG",
    meta: "BO5 · 决赛",
    marketType: "Moneyline",
    status: "settled",
    settlement: "盈利",
    wallets: 6,
    legs: 14,
    stake: 1200,
    pnl: 502.7,
    pnlKind: "realized",
    quality: "clean",
    start: "06-08 17:00",
    end: "06-08 21:30"
  }, {
    cid: "f4",
    game: "cs2",
    teamA: "FaZe",
    teamB: "G2",
    meta: "BO3 · 八强",
    marketType: "Moneyline",
    status: "settled",
    settlement: "盈利",
    wallets: 3,
    legs: 6,
    stake: 480,
    pnl: 318.4,
    pnlKind: "realized",
    quality: "clean",
    start: "06-10 19:00",
    end: "06-10 22:00"
  }, {
    cid: "f5",
    game: "dota2",
    teamA: "Gaimin Gladiators",
    teamB: "Tundra",
    meta: "BO3 · 败者组",
    marketType: "Moneyline",
    status: "settled",
    settlement: "亏损",
    wallets: 4,
    legs: 9,
    stake: 720,
    pnl: -86.2,
    pnlKind: "realized",
    quality: "two-sided",
    start: "06-09 20:00",
    end: "06-09 23:00"
  }, {
    cid: "f6",
    game: "valorant",
    teamA: "Sentinels",
    teamB: "EG",
    meta: "BO3 · 半决赛",
    marketType: "Map 2",
    status: "settled",
    settlement: "盈利",
    wallets: 2,
    legs: 5,
    stake: 320,
    pnl: 144.0,
    pnlKind: "realized",
    quality: "clean",
    start: "06-07 18:00",
    end: "06-07 20:00"
  }];

  // Team accent colors for monogram fallbacks
  const teamColors = {
    "Team Spirit": "#f5c451",
    "Falcons": "#1fd3a7",
    "NAVI": "#ffd60a",
    "Vitality": "#f5c451",
    "T1": "#e63946",
    "Gen.G": "#c8932a",
    "Paper Rex": "#ff6ad5",
    "Fnatic": "#ff8a3d",
    "Team Liquid": "#0a84ff",
    "BetBoom": "#ff453a",
    "FaZe": "#e63946",
    "G2": "#f5f6fb",
    "Gaimin Gladiators": "#7d5cff",
    "Tundra": "#64d2ff",
    "JDG": "#e63946",
    "BLG": "#f5c451",
    "Sentinels": "#e63946",
    "EG": "#0a84ff"
  };
  return {
    overview,
    equity,
    winRates,
    followTypes,
    games,
    wallets,
    quarantined,
    events,
    archive,
    follows,
    teamColors
  };
}();
})(); } catch (e) { __ds_ns.__errors.push({ path: "ui_kits/dashboard/data.js", error: String((e && e.message) || e) }); }

__ds_ns.Badge = __ds_scope.Badge;

__ds_ns.Button = __ds_scope.Button;

__ds_ns.Card = __ds_scope.Card;

__ds_ns.IconButton = __ds_scope.IconButton;

__ds_ns.CategoryDonut = __ds_scope.CategoryDonut;

__ds_ns.GameIcon = __ds_scope.GameIcon;

__ds_ns.RankBadge = __ds_scope.RankBadge;

__ds_ns.StatTile = __ds_scope.StatTile;

__ds_ns.TrendValue = __ds_scope.TrendValue;

__ds_ns.WalletAddress = __ds_scope.WalletAddress;

__ds_ns.WinRateRing = __ds_scope.WinRateRing;

__ds_ns.StatusPill = __ds_scope.StatusPill;

__ds_ns.Input = __ds_scope.Input;

__ds_ns.SegmentedControl = __ds_scope.SegmentedControl;

__ds_ns.Switch = __ds_scope.Switch;

__ds_ns.SidebarNav = __ds_scope.SidebarNav;

__ds_ns.Tabs = __ds_scope.Tabs;

})();
