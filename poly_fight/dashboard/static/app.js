const { createApp } = Vue;

createApp({
  data() {
    return {
      authenticated: false,
      activeTab: "overview",
      tabs: [
        { id: "overview", label: "Overview" },
        { id: "wallets", label: "Wallets" },
        { id: "follows", label: "Follows" },
        { id: "events", label: "Events" },
      ],
      loginForm: { username: "admin", password: "" },
      loginError: "",
      loading: {},
      health: {},
      overview: {},
      wallets: { wallets: [] },
      follows: { follows: [], total: 0 },
      events: { events: [] },
      refreshStatus: { status: "idle" },
      runner: { status: "checking" },
      pauseFollow: null,
      followPage: 1,
      followSize: 25,
      followDetail: { wallets: [] },
      detailModal: { open: false, loading: false, conditionId: "" },
      tradesModal: { open: false, loading: false, wallet: null, trades: [] },
      toasts: [],
      intervals: [],
      eventSource: null,
      pollingFallback: false,
      liveStatus: "starting",
      streamRetryMs: 2000,
      streamRetryTimer: null,
      streamProbeRunning: false,
    };
  },
  computed: {
    runnerStatusClass() {
      if (this.runner.status === "running") return "status-healthy";
      if (this.runner.status === "stopping") return "status-stale";
      return "status-error";
    },
    refreshStatusText() {
      const status = this.refreshStatus || {};
      if (status.status === "running") return `started ${this.formatTime(status.started_at)}`;
      if (status.status === "succeeded") return `finished ${this.formatTime(status.finished_at)}`;
      if (status.status === "failed") {
        const rc = status.returncode != null ? ` rc=${status.returncode}` : "";
        return `failed ${this.formatTime(status.finished_at)}${rc}`;
      }
      return "ready";
    },
    pauseFollowActive() {
      const pause = this.pauseFollow;
      if (!pause) return false;
      if (pause === true) return true;
      if (typeof pause !== "object") return false;
      if (pause.active === false || pause.enabled === false) return false;
      return Boolean(pause.active || pause.enabled || pause.phase || pause.reason || pause.started_at);
    },
    pauseFollowTitle() {
      const pause = this.pauseFollow;
      if (pause && typeof pause === "object") {
        return pause.phase || pause.reason || "follow paused";
      }
      return "follow paused";
    },
    pauseFollowText() {
      const pause = this.pauseFollow;
      if (pause && typeof pause === "object" && pause.started_at) {
        return `since ${this.formatTime(pause.started_at)} · open signals stay tracked; new follow ticks resume after refresh.`;
      }
      return "open signals stay tracked; new follow ticks resume after refresh.";
    },
  },
  mounted() {
    this.bootstrap();
  },
  beforeUnmount() {
    this.stopRealtime();
  },
  methods: {
    async request(path, options = {}) {
      const response = await fetch(path, {
        credentials: "same-origin",
        headers: { Accept: "application/json", ...(options.headers || {}) },
        ...options,
      });
      const payload = await response.json().catch(() => ({}));
      if (response.status === 401) {
        this.authenticated = false;
        throw new Error("unauthorized");
      }
      if (!response.ok || payload.ok === false) {
        const error = new Error(payload.error || response.statusText || "request_failed");
        error.status = response.status;
        error.payload = payload;
        throw error;
      }
      return payload.data ?? payload;
    },
    async bootstrap() {
      try {
        this.health = await this.request("/api/health");
        this.authenticated = true;
        await this.loadDashboard();
        this.startRealtime();
      } catch (error) {
        if (error.message !== "unauthorized") this.showToast(`Health failed: ${error.message}`, "error");
      }
    },
    async login() {
      this.loginError = "";
      this.loading.login = true;
      try {
        await this.request("/api/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this.loginForm),
        });
        this.authenticated = true;
        await this.loadDashboard();
        this.startRealtime();
      } catch (_error) {
        this.loginError = "登录失败，请检查用户名和密码。";
      } finally {
        this.loading.login = false;
      }
    },
    async logout() {
      await this.request("/api/logout", { method: "POST" }).catch(() => null);
      this.authenticated = false;
      this.stopRealtime();
    },
    async loadDashboard() {
      await Promise.allSettled([
        this.loadHealth(),
        this.loadOverview(),
        this.loadWallets(),
        this.loadFollows(),
        this.loadEvents(),
        this.loadWalletRefreshStatus(),
        this.loadRunner(),
      ]);
    },
    startPolling() {
      this.intervals.forEach((id) => clearInterval(id));
      this.intervals = [
        setInterval(() => {
          this.loadHealth();
          this.loadRunner();
          this.loadOverview();
          this.loadEvents();
        }, 30000),
        setInterval(() => {
          this.loadWallets();
          this.loadFollows();
        }, 60000),
        setInterval(() => {
          if (this.refreshStatus.status === "running") this.loadWalletRefreshStatus();
          if (this.runner.status === "stopping") this.loadRunner();
        }, 5000),
      ];
    },
    stopPolling() {
      this.intervals.forEach((id) => clearInterval(id));
      this.intervals = [];
      this.pollingFallback = false;
    },
    startRealtime() {
      this.stopRealtime();
      if (!this.authenticated || typeof EventSource === "undefined") {
        this.liveStatus = "polling";
        this.pollingFallback = true;
        this.startPolling();
        return;
      }
      this.liveStatus = "connecting";
      const source = new EventSource("/api/stream");
      this.eventSource = source;
      source.onmessage = (event) => {
        this.streamRetryMs = 2000;
        try {
          this.applyStreamPayload(JSON.parse(event.data || "{}"));
        } catch (_error) {
          this.showToast("Live stream payload parse failed", "error");
        }
      };
      source.onerror = () => {
        this.handleStreamError(source);
      };
    },
    stopRealtime() {
      if (this.eventSource) {
        this.eventSource.close();
        this.eventSource = null;
      }
      if (this.streamRetryTimer) {
        clearTimeout(this.streamRetryTimer);
        this.streamRetryTimer = null;
      }
      this.stopPolling();
      this.liveStatus = "stopped";
      this.streamProbeRunning = false;
    },
    applyStreamPayload(payload) {
      this.liveStatus = "live";
      this.pollingFallback = false;
      this.stopPolling();
      if (payload.health) this.health = payload.health;
      if (payload.overview) this.overview = payload.overview;
      if (payload.runner) this.runner = payload.runner;
      if (payload.refresh) this.refreshStatus = payload.refresh;
      if (Object.prototype.hasOwnProperty.call(payload, "pause_follow")) this.pauseFollow = payload.pause_follow;
      if (payload.follows_dirty) this.loadFollows().catch(() => null);
      if (payload.events_dirty) this.loadEvents().catch(() => null);
      if (payload.wallets_dirty) this.loadWallets().catch(() => null);
    },
    async handleStreamError(source) {
      if (this.streamProbeRunning) return;
      this.streamProbeRunning = true;
      source.close();
      if (this.eventSource === source) this.eventSource = null;
      try {
        const response = await fetch("/api/health", {
          credentials: "same-origin",
          headers: { Accept: "application/json" },
        });
        if (response.status === 401) {
          this.stopRealtime();
          this.authenticated = false;
          this.liveStatus = "auth_expired";
          return;
        }
        this.enterPollingFallback();
      } catch (_error) {
        this.enterPollingFallback();
      } finally {
        this.streamProbeRunning = false;
      }
    },
    enterPollingFallback() {
      if (!this.authenticated) return;
      this.liveStatus = "polling";
      if (!this.pollingFallback) {
        this.pollingFallback = true;
        this.startPolling();
      }
      const delay = this.streamRetryMs;
      this.streamRetryMs = Math.min(this.streamRetryMs * 2, 30000);
      if (this.streamRetryTimer) clearTimeout(this.streamRetryTimer);
      this.streamRetryTimer = setTimeout(() => {
        this.streamRetryTimer = null;
        if (this.authenticated && this.pollingFallback) this.startRealtime();
      }, delay);
    },
    async refreshAll() {
      this.loading.any = true;
      try {
        await this.loadDashboard();
        this.showToast("页面数据已刷新");
      } finally {
        this.loading.any = false;
      }
    },
    async loadHealth() {
      this.health = await this.request("/api/health");
    },
    async loadOverview() {
      this.overview = await this.request("/api/overview");
    },
    async loadWallets() {
      this.wallets = await this.request("/api/wallets");
    },
    async loadFollows() {
      this.follows = await this.request(`/api/follows?page=${this.followPage}&size=${this.followSize}`);
    },
    async loadEvents() {
      this.events = await this.request("/api/events");
    },
    async loadWalletRefreshStatus() {
      const result = await this.request("/api/wallet-refresh");
      this.refreshStatus = result.status || { status: "idle" };
    },
    async loadRunner() {
      this.runner = await this.request("/api/runner");
    },
    async startRunner() {
      this.loading.runner = true;
      try {
        this.runner = await this.request("/api/runner/start", { method: "POST" });
        this.showToast("跟单脚本已启动");
      } catch (error) {
        if (error.status === 409 && error.payload && error.payload.data) {
          this.runner = error.payload.data;
          this.showToast("跟单脚本已经在运行", "error");
        } else {
          this.showToast(`启动失败: ${error.message}`, "error");
        }
      } finally {
        this.loading.runner = false;
        await this.loadRunner().catch(() => null);
      }
    },
    async stopRunner() {
      this.loading.runner = true;
      try {
        this.runner = await this.request("/api/runner/stop", { method: "POST" });
        this.showToast("已请求停止跟单脚本");
      } catch (error) {
        this.showToast(`停止失败: ${error.message}`, "error");
      } finally {
        this.loading.runner = false;
        await this.loadRunner().catch(() => null);
      }
    },
    async startWalletRefresh() {
      this.loading.walletRefresh = true;
      try {
        const result = await this.request("/api/wallet-refresh", { method: "POST" });
        this.refreshStatus = result;
        this.showToast("候选钱包刷新已启动");
      } catch (error) {
        if (error.status === 409) {
          this.refreshStatus = error.payload && error.payload.data ? error.payload.data : this.refreshStatus;
          this.showToast("已有钱包刷新任务在运行", "error");
        } else {
          this.showToast(`刷新启动失败: ${error.message}`, "error");
        }
      } finally {
        this.loading.walletRefresh = false;
        await this.loadWalletRefreshStatus().catch(() => null);
      }
    },
    async setFollowPage(page) {
      this.followPage = Math.max(1, page);
      await this.loadFollows();
    },
    async openFollowDetail(conditionId) {
      this.detailModal = { open: true, loading: true, conditionId };
      this.followDetail = { wallets: [] };
      try {
        this.followDetail = await this.request(`/api/follows/${encodeURIComponent(conditionId)}`);
      } catch (error) {
        this.showToast(`详情加载失败: ${error.message}`, "error");
      } finally {
        this.detailModal.loading = false;
      }
    },
    closeDetail() {
      this.detailModal.open = false;
    },
    async openWalletTrades(wallet) {
      this.tradesModal = { open: true, loading: true, wallet, trades: [] };
      try {
        const data = await this.request(`/api/wallets/${encodeURIComponent(wallet.wallet)}/trades?page=1&size=10`);
        this.tradesModal.trades = data.trades || [];
      } catch (error) {
        this.showToast(`最近交易加载失败: ${error.message}`, "error");
      } finally {
        this.tradesModal.loading = false;
      }
    },
    closeTrades() {
      this.tradesModal.open = false;
    },
    showToast(message, kind = "info") {
      const id = Date.now() + Math.random();
      this.toasts.push({ id, message, kind });
      setTimeout(() => {
        this.toasts = this.toasts.filter((toast) => toast.id !== id);
      }, 4200);
    },
    profileUrl(wallet) {
      return `https://polymarket.com/@${wallet}?tab=activity`;
    },
    statusClass(status) {
      return `status-${status || "error"}`;
    },
    gradeClass(grade) {
      if (grade === "A") return "badge-success";
      if (grade === "B") return "badge-info";
      if (grade === "C") return "badge-warning";
      return "badge-neutral";
    },
    pnlClass(value) {
      const num = Number(value);
      if (!Number.isFinite(num) || Math.abs(num) < 0.000001) return "text-flat";
      return num > 0 ? "text-good" : "text-bad";
    },
    numberOrDash(value) {
      const num = Number(value);
      return Number.isFinite(num) ? new Intl.NumberFormat("en-US").format(num) : "-";
    },
    money(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(num);
    },
    percent(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      return `${(num * 100).toFixed(1)}%`;
    },
    signedPctPoints(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      const sign = num > 0 ? "+" : "";
      return `${sign}${(num * 100).toFixed(1)}pt`;
    },
    price(value) {
      const num = Number(value);
      return Number.isFinite(num) ? num.toFixed(3) : "-";
    },
    duration(seconds) {
      const num = Number(seconds);
      if (!Number.isFinite(num) || num <= 0) return "-";
      if (num < 60) return `${Math.round(num)}s`;
      return `${Math.round(num / 60)}m`;
    },
    formatTime(value) {
      const ts = this.normalizeTs(value);
      if (!ts) return "-";
      return new Date(ts * 1000).toLocaleString([], { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
    },
    timeAgo(value) {
      const ts = this.normalizeTs(value);
      if (!ts) return "-";
      const delta = Math.round((Date.now() / 1000 - ts) / 60);
      if (Math.abs(delta) < 1) return "now";
      if (delta >= 0 && delta < 60) return `${delta}m ago`;
      if (delta >= 0) return `${Math.round(delta / 60)}h ago`;
      const future = Math.abs(delta);
      return future < 60 ? `in ${future}m` : `in ${Math.round(future / 60)}h`;
    },
    normalizeTs(value) {
      if (value == null || value === "") return 0;
      if (typeof value === "number") return value > 100000000000 ? Math.round(value / 1000) : Math.round(value);
      const parsed = Date.parse(String(value));
      return Number.isFinite(parsed) ? Math.round(parsed / 1000) : 0;
    },
    shortId(value) {
      const text = String(value || "");
      return text.length > 18 ? `${text.slice(0, 10)}...${text.slice(-6)}` : text;
    },
  },
}).mount("#app");
