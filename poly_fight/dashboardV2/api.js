/* Poly Sniper dashboard — data layer (plain JS, attaches to window.PSApi).
   Wraps the existing Python backend (poly_fight/dashboard.py). All responses
   use the {ok, data, generated_at} envelope; errors use {ok:false, error}.   */
(function () {
  "use strict";

  class AuthError extends Error {
    constructor() { super("unauthorized"); this.name = "AuthError"; }
  }

  // 会话过期(cookie TTL 到期)全局钩子:任何 API(读/写)收到 401 时触发一次,让 app 立刻
  // 切回登录页强制重登 —— 否则 mutation 静默失败、页面停在旧数据(停止/重采按钮像坏了)。
  let onAuthExpired = null;

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
    if (res.status === 401) {
      if (onAuthExpired) { try { onAuthExpired(); } catch (_e) { /* never let the hook mask the error */ } }
      throw new AuthError();
    }
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
  const aiRisk = () => get("/api/ai-risk");
  const aiWrapKey = () => get("/api/ai-risk/wrap-key");
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
  const runnerStart = (body) => post("/api/runner/start", body || {});
  const runnerStop = () => post("/api/runner/stop", {});
  const resetData = () => post("/api/reset-data", {});
  const walletRefresh = (category, body) =>
    post("/api/wallet-refresh?category=" + encodeURIComponent(category), body || {});
  const saveAiCredential = (envelope) => post("/api/ai-risk/credential", { envelope });
  const testAiCredential = () => post("/api/ai-risk/credential/test", {});
  const deleteAiCredential = () => post("/api/ai-risk/credential/delete", {});
  const saveAiDataCredential = (envelope) => post("/api/ai-risk/data-credential", { envelope });
  const testAiDataCredential = () => post("/api/ai-risk/data-credential/test", {});
  const deleteAiDataCredential = () => post("/api/ai-risk/data-credential/delete", {});
  const saveAiSettings = (enabled) => post("/api/ai-risk/settings", { enabled: !!enabled });

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
    // 注册会话过期回调(传 null 注销)。app 用它在 401 时强制切登录页。
    setAuthExpiredHandler: (fn) => { onAuthExpired = typeof fn === "function" ? fn : null; },
    get, post, login, logout,
    health, overview, wallets, events, runner, followStrategy, walletRefreshStatus,
    follows, followDetail, walletFollows, marketPrices, walletTrades, aiRisk, aiWrapKey,
    setFavorite, setQuarantine, setAccountBalance, saveStrategy,
    strategies, createStrategy, updateStrategy, activateStrategy, deleteStrategy,
    runnerStart, runnerStop, resetData, walletRefresh,
    saveAiCredential, testAiCredential, deleteAiCredential,
    saveAiDataCredential, testAiDataCredential, deleteAiDataCredential, saveAiSettings,
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
    api.aiRisk = m.aiRisk ? wrap(m.aiRisk) : () => Promise.resolve({ settings: { enabled: false, model: "deepseek-v4-pro", win_probability_threshold: 65, confidence_threshold: 75 }, credential: { configured: false, status: "not_configured" }, balance: null, summary: {} });
    api.aiWrapKey = () => Promise.resolve({ ready: true, envelopeVersion: 1, keyId: "mock", spki: "" });
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
    api.saveAiCredential = () => okEcho({ configured: true, status: "valid" });
    api.testAiCredential = () => okEcho({ configured: true, status: "valid" });
    api.deleteAiCredential = () => okEcho({ deleted: true, enabled: false });
    api.saveAiDataCredential = () => okEcho({ configured: true, status: "valid" });
    api.testAiDataCredential = () => okEcho({ configured: true, status: "valid" });
    api.deleteAiDataCredential = () => okEcho({ deleted: true, enabled: false });
    api.saveAiSettings = (enabled) => okEcho({ enabled: !!enabled });
    api.resetData = () => okEcho({ status: "reset" });
    api.walletRefresh = m.walletRefresh ? wrap(m.walletRefresh) : () => okEcho({ status: "running" });
    api.openStream = () => ({ close() {}, get closed() { return true; } });
  }

  window.PSApi = api;
  window.PSEncryptCredential = async function encryptCredential(secret, wrapKey) {
    if (wrapKey && wrapKey.keyId === "mock") return { envelopeVersion: 1, keyId: "mock", wrappedKey: "mock", nonce: "mock", ciphertext: btoa(secret) };
    if (!window.crypto || !window.crypto.subtle || !wrapKey || !wrapKey.spki) throw new Error("secure_context_required");
    const b64ToBytes = (value) => Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
    const bytesToB64 = (value) => {
      const bytes = new Uint8Array(value); let binary = "";
      for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
      return btoa(binary);
    };
    const publicKey = await window.crypto.subtle.importKey("spki", b64ToBytes(wrapKey.spki), { name: "RSA-OAEP", hash: "SHA-256" }, false, ["encrypt"]);
    const dek = await window.crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt"]);
    const rawDek = await window.crypto.subtle.exportKey("raw", dek);
    const nonce = window.crypto.getRandomValues(new Uint8Array(12));
    const ciphertext = await window.crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, dek, new TextEncoder().encode(secret));
    const wrappedKey = await window.crypto.subtle.encrypt({ name: "RSA-OAEP" }, publicKey, rawDek);
    return { envelopeVersion: wrapKey.envelopeVersion, keyId: wrapKey.keyId, wrappedKey: bytesToB64(wrappedKey), nonce: bytesToB64(nonce), ciphertext: bytesToB64(ciphertext) };
  };
})();
