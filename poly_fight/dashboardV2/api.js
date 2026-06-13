/* Poly Sniper dashboard — data layer (plain JS, attaches to window.PSApi).
   Wraps the existing Python backend (poly_fight/dashboard.py). All responses
   use the {ok, data, generated_at} envelope; errors use {ok:false, error}.   */
(function () {
  "use strict";

  class AuthError extends Error {
    constructor() { super("unauthorized"); this.name = "AuthError"; }
  }
  class ApiError extends Error {
    constructor(error, detail, status, data) {
      super(error || "request_failed");
      this.name = "ApiError";
      this.error = error; this.detail = detail; this.status = status; this.data = data;
    }
  }

  async function parse(res) {
    let body = null;
    try { body = await res.json(); } catch (_e) { body = null; }
    if (res.status === 401) throw new AuthError();
    if (body && typeof body === "object" && "ok" in body) {
      if (body.ok) return body.data;
      throw new ApiError(body.error, body.detail, res.status, body.data);
    }
    if (!res.ok) throw new ApiError("http_" + res.status, null, res.status, body);
    return body;
  }

  async function get(path) {
    const res = await fetch(path, { credentials: "same-origin", headers: { Accept: "application/json" } });
    return parse(res);
  }
  async function post(path, body) {
    const res = await fetch(path, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(body || {}),
    });
    return parse(res);
  }

  /* ---- auth ---- */
  async function login(username, password) {
    const res = await fetch("/api/login", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ username, password }),
    });
    if (res.status === 401) throw new ApiError("invalid_login", null, 401);
    const body = await res.json().catch(() => null);
    if (!res.ok || !body || body.ok !== true) throw new ApiError((body && body.error) || "login_failed", null, res.status);
    return true;
  }
  const logout = () => post("/api/logout");

  /* ---- reads ---- */
  const health = () => get("/api/health");
  const overview = () => get("/api/overview");
  const wallets = () => get("/api/wallets");
  const events = () => get("/api/events");
  const runner = () => get("/api/runner");
  const followStrategy = () => get("/api/follow-strategy");
  const walletRefreshStatus = () => get("/api/wallet-refresh");
  const follows = (opts) => {
    const q = new URLSearchParams();
    const o = opts || {};
    q.set("page", o.page || 1);
    q.set("size", o.size || 25);
    if (o.status) q.set("status", o.status);
    if (o.category) q.set("category", o.category);
    return get("/api/follows?" + q.toString());
  };
  const followDetail = (cid) => get("/api/follows/" + encodeURIComponent(cid));
  const walletFollows = (wallet, opts) => {
    const q = new URLSearchParams();
    const o = opts || {};
    q.set("wallet", wallet);
    q.set("page", o.page || 1);
    q.set("size", o.size || 20);
    if (o.status) q.set("status", o.status);
    return get("/api/wallet-follows?" + q.toString());
  };
  const marketPrices = (cid) => get("/api/markets/" + encodeURIComponent(cid) + "/prices");
  const walletTrades = (wallet, opts) => {
    const o = opts || {};
    const q = new URLSearchParams({ page: o.page || 1, size: o.size || 10 });
    return get("/api/wallets/" + encodeURIComponent(wallet) + "/trades?" + q.toString());
  };

  /* ---- writes ---- */
  const setFavorite = (wallet, category, favorite) =>
    post("/api/wallet-favorites", { wallet, category, favorite });
  const setQuarantine = (wallet, category, quarantined) =>
    post("/api/wallet-quarantine", { wallet, category, quarantined });
  const setAccountBalance = (balance_usdc) => post("/api/account-balance", { balance_usdc });
  const saveStrategy = (strategy) => post("/api/follow-strategy", strategy);
  const strategies = () => get("/api/follow-strategies");
  const createStrategy = (name, strategy) => post("/api/follow-strategies", { name, strategy });
  const updateStrategy = (slug, name, strategy) =>
    post("/api/follow-strategies/" + encodeURIComponent(slug) + "/update", { name, strategy });
  const activateStrategy = (slug) => post("/api/follow-strategies/" + encodeURIComponent(slug) + "/activate", {});
  const deleteStrategy = (slug) => post("/api/follow-strategies/" + encodeURIComponent(slug) + "/delete", {});
  const runnerStart = () => post("/api/runner/start", {});
  const runnerStop = () => post("/api/runner/stop", {});
  const resetData = () => post("/api/reset-data", {});
  const walletRefresh = (category) =>
    post("/api/wallet-refresh?category=" + encodeURIComponent(category), {});

  /* ---- live stream (SSE) with polling fallback ---- */
  function openStream(onFrame, onStatus) {
    let es = null, closed = false;
    try {
      es = new EventSource("/api/stream");
      es.onmessage = (e) => {
        if (!e.data) return;
        try { onFrame(JSON.parse(e.data)); } catch (_err) {}
      };
      es.onopen = () => onStatus && onStatus("connected");
      es.onerror = () => onStatus && onStatus("error");
    } catch (_e) {
      onStatus && onStatus("error");
    }
    return { close() { closed = true; if (es) es.close(); }, get closed() { return closed; } };
  }

  const api = {
    AuthError, ApiError,
    get, post, login, logout,
    health, overview, wallets, events, runner, followStrategy, walletRefreshStatus,
    follows, followDetail, walletFollows, marketPrices, walletTrades,
    setFavorite, setQuarantine, setAccountBalance, saveStrategy,
    strategies, createStrategy, updateStrategy, activateStrategy, deleteStrategy,
    runnerStart, runnerStop, resetData, walletRefresh,
    openStream,
  };

  /* ---- mock mode: ?mock=1 returns canned fixtures (window.PSMock) ---- */
  const MOCK = (() => { try { return new URLSearchParams(location.search).get("mock") === "1"; } catch (_e) { return false; } })();
  if (MOCK && window.PSMock) {
    const m = window.PSMock;
    const wrap = (fn) => (...args) => Promise.resolve(fn(...args));
    api.login = () => Promise.resolve(true);
    api.logout = () => Promise.resolve(true);
    api.health = wrap(m.health);
    api.overview = wrap(m.overview);
    api.wallets = wrap(m.wallets);
    api.events = wrap(m.events);
    api.runner = wrap(m.runner);
    api.followStrategy = wrap(m.followStrategy);
    api.walletRefreshStatus = wrap(m.walletRefreshStatus);
    api.follows = wrap(m.follows);
    api.followDetail = wrap(m.followDetail);
    api.walletFollows = wrap(m.walletFollows);
    api.marketPrices = wrap(m.marketPrices);
    const okEcho = (x) => Promise.resolve(x || { ok: true });
    api.setFavorite = (w, c, f) => okEcho({ wallet: w, category: c, favorite: f });
    api.setQuarantine = (w, c, q) => okEcho({ wallet: w, category: c, quarantined: q });
    api.setAccountBalance = (b) => okEcho({ configured: true, balance_usdc: b });
    api.saveStrategy = (s) => okEcho(Object.assign({ configured: true }, s));
    const tryMock = (fn) => (...args) => { try { return Promise.resolve(fn(...args)); } catch (e) { return Promise.reject(e); } };
    api.strategies = wrap(m.strategies);
    api.createStrategy = tryMock((name, s) => m.createStrategy(name, s));
    api.updateStrategy = tryMock((slug, name, s) => m.updateStrategy(slug, name, s));
    api.activateStrategy = tryMock((slug) => m.activateStrategy(slug));
    api.deleteStrategy = tryMock((slug) => m.deleteStrategy(slug));
    api.runnerStart = m.runnerStart ? wrap(m.runnerStart) : () => okEcho({ status: "running" });
    api.runnerStop = m.runnerStop ? wrap(m.runnerStop) : () => okEcho({ status: "stopped" });
    api.resetData = () => okEcho({ status: "reset" });
    api.walletRefresh = m.walletRefresh ? wrap(m.walletRefresh) : () => okEcho({ status: "running" });
    api.openStream = () => ({ close() {}, get closed() { return true; } });
  }

  window.PSApi = api;
})();
