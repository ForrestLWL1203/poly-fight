const { createApp } = Vue;

createApp({
  data() {
    return {
      authenticated: false,
      authChecking: true,
      activeTab: "follows",
      activeCategory: "esports",
      tabs: [
        { id: "follows", label: "跟单" },
        { id: "events", label: "赛事" },
      ],
      categoryTabs: [
        { id: "esports", label: "eSports" },
        { id: "sports", label: "Sports" },
      ],
      loginForm: { username: "admin", password: "" },
      loginError: "",
      loading: {},
      runnerActionText: "",
      health: {},
      overview: {},
      wallets: { wallets: [] },
      follows: { follows: [], total: 0 },
      events: { events: [] },
      refreshStatus: { status: "idle" },
      runner: { status: "checking" },
      pauseFollow: null,
      walletPage: 1,
      walletSize: 10,
      followPage: 1,
      followSize: 10,
      followStatusFilter: "",
      followStatusOptions: [
        { value: "", label: "全部状态" },
        { value: "open", label: "跟单中" },
        { value: "settled", label: "已结算" },
        { value: "exited", label: "已退出" },
        { value: "mixed", label: "混合" },
      ],
      eventPage: 1,
      eventSize: 10,
      eventView: "active",
      eventGameFilter: "",
      eventStatusFilter: "",
      eventStatusOptions: [
        { value: "", label: "全部状态" },
        { value: "upcoming", label: "未开始" },
        { value: "live", label: "进行中" },
      ],
      followDetail: { wallets: [] },
      detailModal: { open: false, loading: false, conditionId: "" },
      walletFollowDetail: { signals: [] },
      walletFollowModal: { open: false, loading: false, wallet: "", status: "" },
      resetModal: { open: false, loading: false },
      detailLegPages: {},
      detailPriceRefreshing: false,
      detailPriceCooldown: 0,
      detailPriceCooldownTimer: null,
      clockNow: Math.floor(Date.now() / 1000),
      clockTimer: null,
      walletRefreshStartedLocal: 0,
      toasts: [],
      intervals: [],
      eventSource: null,
      pollingFallback: false,
      liveStatus: "starting",
      streamRetryMs: 2000,
      streamRetryTimer: null,
      streamProbeRunning: false,
      panelRequestSeq: { follows: 0, events: 0 },
      matchTitleCache: {},
    };
  },
  computed: {
    runnerStatusClass() {
      if (this.runner.status === "running") return "status-healthy";
      if (this.runner.status === "stopping") return "status-stale";
      return "status-error";
    },
    runnerPillText() {
      if (this.runner.status === "running") return "运行中";
      if (this.runner.status === "stopping") return "停止中";
      return "停止";
    },
    runnerStatusIcon() {
      return this.runner.status === "running"
        ? "/icons/flaticon/play.png"
        : "/icons/flaticon/stop.png";
    },
    runnerControlLabel() {
      return this.runner.status === "running" ? "停止跟单" : "启动跟单";
    },
    runnerControlIcon() {
      return this.runner.status === "running" ? "■" : "▶";
    },
    hasFollowWallets() {
      return Number(this.wallets?.count || (this.wallets?.wallets || []).length || 0) > 0;
    },
    runnerStartBlocked() {
      return this.runner.status !== "running" && !this.hasFollowWallets;
    },
    runnerControlTitle() {
      if (this.runnerStartBlocked) return "需先采集目标跟单钱包";
      return this.runner.status === "running" ? "停止跟单脚本" : "启动跟单脚本";
    },
    healthPillText() {
      if (this.runner.status !== "running") return "tick 停止";
      if (this.health.status === "healthy") return "tick 正常";
      if (this.health.status === "stale") return "tick 延迟";
      if (this.health.status === "waiting_for_runner") return "等待首个 tick";
      return this.statusText(this.health.status) || "检查中";
    },
    healthPillClass() {
      if (this.runner.status !== "running") return "status-error";
      return this.statusClass(this.health.status);
    },
    walletPageCount() {
      const count = this.walletRowsByCategory.length;
      return Math.max(1, Math.ceil(count / this.walletSize));
    },
    walletRowsByCategory() {
      return (this.wallets.wallets || []).filter((wallet) => (wallet.category || "esports") === this.activeCategory);
    },
    walletPageRows() {
      const rows = this.walletRowsByCategory;
      const page = Math.min(Math.max(1, this.walletPage), this.walletPageCount);
      const start = (page - 1) * this.walletSize;
      return rows.slice(start, start + this.walletSize);
    },
    followPageCount() {
      const count = Number(this.follows.total || 0);
      return Math.max(1, Math.ceil(count / this.followSize));
    },
    eventPageCount() {
      const count = this.eventFilteredRows.length;
      return Math.max(1, Math.ceil(count / this.eventSize));
    },
    eventSourceRows() {
      const rows = this.eventView === "archive" ? (this.events.archived_events || []) : (this.events.events || []);
      return rows.filter((event) => (event.category || "esports") === this.activeCategory);
    },
    eventViewOptions() {
      const category = this.activeCategory;
      const activeCount = (this.events.events || []).filter((event) => (event.category || "esports") === category).length;
      const archiveCount = (this.events.archived_events || []).filter((event) => (event.category || "esports") === category).length;
      return [
        { value: "active", label: "监控中赛事", count: activeCount },
        { value: "archive", label: "已结算赛事", count: archiveCount },
      ];
    },
    eventGameOptions() {
      const games = new Set();
      for (const event of this.eventSourceRows) {
        const game = this.eventGame(event);
        if (game) games.add(game);
      }
      return [
        { value: "", label: "全部项目" },
        ...Array.from(games).sort((a, b) => a.localeCompare(b)).map((game) => ({ value: game, label: game })),
      ];
    },
    eventFilteredRows() {
      const rows = this.eventSourceRows;
      return rows.filter((event) => {
        if (this.eventView === "active" && this.eventStatusFilter && this.eventStatus(event) !== this.eventStatusFilter) return false;
        if (this.eventGameFilter && this.eventGame(event) !== this.eventGameFilter) return false;
        return true;
      });
    },
    eventPageRows() {
      const rows = this.eventFilteredRows;
      const page = Math.min(Math.max(1, this.eventPage), this.eventPageCount);
      const start = (page - 1) * this.eventSize;
      return rows.slice(start, start + this.eventSize);
    },
    walletRefreshBusy() {
      return this.walletRefreshBusyFor(this.activeCategory);
    },
    walletRefreshElapsedText() {
      if (!this.walletRefreshBusy) return "已用时 -";
      const status = this.categoryRefreshStatus(this.activeCategory);
      const startedAt = this.normalizeTs(status?.started_at) || this.walletRefreshStartedLocal;
      if (!startedAt) return "已用时 0s";
      return `已用时 ${this.elapsedDuration(Math.max(0, this.clockNow - startedAt))}`;
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
        return pause.phase || pause.reason || "跟单暂停";
      }
      return "跟单暂停";
    },
    pauseFollowText() {
      const pause = this.pauseFollow;
      if (pause && typeof pause === "object" && pause.started_at) {
        return `${this.formatTime(pause.started_at)} 开始暂停；已开的跟单继续跟踪，刷新结束后恢复新 tick。`;
      }
      return "已开的跟单继续跟踪，刷新结束后恢复新 tick。";
    },
  },
  mounted() {
    this.clockTimer = setInterval(() => {
      this.clockNow = Math.floor(Date.now() / 1000);
    }, 1000);
    this.bootstrap();
  },
  beforeUnmount() {
    this.stopRealtime();
    this.clearDetailPriceCooldown();
    if (this.clockTimer) clearInterval(this.clockTimer);
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
        this.authChecking = false;
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
      } finally {
        this.authChecking = false;
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
        this.authChecking = false;
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
      this.authChecking = false;
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
      if (this.refreshStatus?.status !== "running") this.walletRefreshStartedLocal = 0;
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
    async loadHealth() {
      this.health = await this.request("/api/health");
    },
    async loadOverview() {
      this.overview = await this.request("/api/overview");
    },
    async loadWallets() {
      this.wallets = await this.request("/api/wallets");
      if (this.walletPage > this.walletPageCount) this.walletPage = this.walletPageCount;
    },
    async loadFollows() {
      const seq = ++this.panelRequestSeq.follows;
      this.loading.follows = true;
      const params = new URLSearchParams({
        page: String(this.followPage),
        size: String(this.followSize),
      });
      if (this.followStatusFilter) params.set("status", this.followStatusFilter);
      params.set("category", this.activeCategory);
      try {
        this.follows = await this.request(`/api/follows?${params.toString()}`);
        if (this.followPage > this.followPageCount) this.followPage = this.followPageCount;
      } finally {
        if (this.panelRequestSeq.follows === seq) this.loading.follows = false;
      }
    },
    async loadEvents() {
      const seq = ++this.panelRequestSeq.events;
      this.loading.events = true;
      try {
        const result = await this.request("/api/events");
        result.events = (result.events || []).slice().sort((a, b) => {
          const aFollowed = (a.open_signals || []).length > 0 ? 0 : (a.results || []).length > 0 ? 1 : 2;
          const bFollowed = (b.open_signals || []).length > 0 ? 0 : (b.results || []).length > 0 ? 1 : 2;
          if (aFollowed !== bFollowed) return aFollowed - bFollowed;
          return this.normalizeTs(a.match_start_time) - this.normalizeTs(b.match_start_time);
        });
        this.events = result;
        if (this.eventPage > this.eventPageCount) this.eventPage = this.eventPageCount;
      } finally {
        if (this.panelRequestSeq.events === seq) this.loading.events = false;
      }
    },
    async loadWalletRefreshStatus() {
      const result = await this.request("/api/wallet-refresh");
      this.refreshStatus = result.status || { status: "idle" };
      if (!this.walletRefreshBusyFor(this.activeCategory)) this.walletRefreshStartedLocal = 0;
    },
    async loadRunner() {
      this.runner = await this.request("/api/runner");
    },
    async refreshAfterRunnerChange() {
      await Promise.allSettled([
        this.loadHealth(),
        this.loadRunner(),
        this.loadOverview(),
        this.loadFollows(),
        this.loadEvents(),
        this.loadWallets(),
      ]);
    },
    async startRunner() {
      if (this.runnerStartBlocked) {
        this.showToast("需先采集目标跟单钱包", "error");
        return;
      }
      await this.loadRunner();
      if (this.runner.status === "running") {
        this.showToast("跟单脚本已经在运行");
        return;
      }
      this.loading.runner = true;
      this.runnerActionText = "正在启动跟单脚本并刷新数据";
      try {
        this.runner = await this.request("/api/runner/start", { method: "POST" });
        this.showToast("跟单脚本已启动");
        await this.refreshAfterRunnerChange();
      } catch (error) {
        if (error.status === 409 && error.payload && error.payload.data) {
          this.runner = error.payload.data;
          this.showToast("跟单脚本已经在运行", "error");
          await this.refreshAfterRunnerChange();
        } else {
          this.showToast(`启动失败: ${error.message}`, "error");
        }
      } finally {
        this.loading.runner = false;
        this.runnerActionText = "";
      }
    },
    async stopRunner() {
      this.loading.runner = true;
      this.runnerActionText = "正在停止跟单脚本并刷新数据";
      try {
        this.runner = await this.request("/api/runner/stop", { method: "POST" });
        this.showToast("已请求停止跟单脚本");
        await this.refreshAfterRunnerChange();
      } catch (error) {
        this.showToast(`停止失败: ${error.message}`, "error");
      } finally {
        this.loading.runner = false;
        this.runnerActionText = "";
      }
    },
    openResetModal() {
      this.resetModal.open = true;
    },
    closeResetModal() {
      if (this.resetModal.loading) return;
      this.resetModal.open = false;
    },
    async confirmResetData() {
      if (this.resetModal.loading) return;
      this.resetModal.loading = true;
      try {
        await this.request("/api/reset-data", { method: "POST" });
        this.walletPage = 1;
        this.followPage = 1;
        this.eventPage = 1;
        this.followStatusFilter = "";
        this.eventStatusFilter = "";
        this.eventGameFilter = "";
        this.refreshStatus = { status: "idle" };
        this.followDetail = { wallets: [] };
        this.walletFollowDetail = { signals: [] };
        this.detailModal = { open: false, loading: false, conditionId: "" };
        this.walletFollowModal = { open: false, loading: false, wallet: "", status: "" };
        await Promise.allSettled([
          this.loadHealth(),
          this.loadRunner(),
          this.loadOverview(),
          this.loadFollows(),
          this.loadEvents(),
          this.loadWallets(),
          this.loadWalletRefreshStatus(),
        ]);
        this.resetModal.open = false;
        this.showToast("历史数据已重置");
      } catch (error) {
        const map = {
          runner_running: "跟单脚本正在运行，请先停止后再重置",
          wallet_refresh_running: "钱包采样正在运行，请等待结束后再重置",
        };
        this.showToast(`重置失败: ${map[error.message] || error.message}`, "error");
      } finally {
        this.resetModal.loading = false;
      }
    },
    async startWalletRefresh() {
      this.loading.walletRefresh = true;
      this.walletRefreshStartedLocal = Math.floor(Date.now() / 1000);
      try {
        const result = await this.request(`/api/wallet-refresh?category=${encodeURIComponent(this.activeCategory)}`, { method: "POST" });
        this.refreshStatus = { ...(this.refreshStatus || {}), [this.activeCategory]: result };
        this.walletRefreshStartedLocal = this.normalizeTs(result?.started_at) || this.walletRefreshStartedLocal;
        this.showToast("候选钱包采样已启动");
      } catch (error) {
        if (error.status === 409) {
          this.refreshStatus = error.payload && error.payload.data ? error.payload.data : this.refreshStatus;
          this.walletRefreshStartedLocal = this.normalizeTs(this.categoryRefreshStatus(this.activeCategory)?.started_at) || this.walletRefreshStartedLocal;
          this.showToast("已有采样任务在运行", "error");
        } else {
          this.showToast(`采样启动失败: ${error.message}`, "error");
          this.walletRefreshStartedLocal = 0;
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
    async setFollowStatusFilter() {
      this.followPage = 1;
      await this.loadFollows();
    },
    setWalletPage(page) {
      this.walletPage = Math.min(Math.max(1, page), this.walletPageCount);
    },
    async setCategory(category) {
      this.activeCategory = category === "sports" ? "sports" : "esports";
      this.walletPage = 1;
      this.followPage = 1;
      this.eventPage = 1;
      await Promise.allSettled([this.loadFollows(), this.loadEvents()]);
    },
    setEventPage(page) {
      this.eventPage = Math.min(Math.max(1, page), this.eventPageCount);
    },
    setEventView(view) {
      this.eventView = view === "archive" ? "archive" : "active";
      this.eventPage = 1;
      this.eventStatusFilter = "";
    },
    setEventStatusFilter() {
      this.eventPage = 1;
    },
    setEventGameFilter() {
      this.eventPage = 1;
    },
    async toggleRunner() {
      if (this.runnerStartBlocked) {
        this.showToast("需先采集目标跟单钱包", "error");
        return;
      }
      const visibleStatus = this.runner.status;
      try {
        await this.loadRunner();
      } catch (error) {
        this.showToast(`检查脚本状态失败: ${error.message}`, "error");
        return;
      }
      if (visibleStatus !== "running" && this.runner.status === "running") {
        this.showToast("跟单脚本其实已经在运行，已同步状态");
        return;
      }
      if (this.runner.status === "running") {
        await this.stopRunner();
      } else {
        await this.startRunner();
      }
    },
    async openFollowDetail(conditionId) {
      this.detailModal = { open: true, loading: true, conditionId };
      this.followDetail = { wallets: [] };
      this.detailLegPages = {};
      try {
        this.followDetail = await this.request(`/api/follows/${encodeURIComponent(conditionId)}`);
        await this.refreshDetailPrices({ silent: true });
      } catch (error) {
        this.showToast(`详情加载失败: ${error.message}`, "error");
      } finally {
        this.detailModal.loading = false;
      }
    },
    closeDetail() {
      this.detailModal.open = false;
      this.detailLegPages = {};
      this.clearDetailPriceCooldown();
    },
    async openWalletFollowDetail(wallet, status) {
      const walletAddr = wallet?.wallet || wallet;
      if (!walletAddr) return;
      this.walletFollowModal = { open: true, loading: true, wallet: walletAddr, status };
      this.walletFollowDetail = { signals: [] };
      try {
        const query = new URLSearchParams({ wallet: walletAddr, status });
        try {
          this.walletFollowDetail = await this.request(`/api/wallet-follows?${query.toString()}`);
        } catch (error) {
          if (error.message !== "not_found") throw error;
          this.walletFollowDetail = await this.request(`/api/wallets/${encodeURIComponent(walletAddr)}/follows?status=${encodeURIComponent(status)}`);
        }
      } catch (error) {
        this.showToast(`钱包跟单详情加载失败: ${error.message}`, "error");
      } finally {
        this.walletFollowModal.loading = false;
      }
    },
    closeWalletFollowDetail() {
      this.walletFollowModal.open = false;
      this.walletFollowDetail = { signals: [] };
    },
    async refreshDetailPrices({ silent = false } = {}) {
      const conditionId = this.detailModal.conditionId || this.followDetail.condition_id;
      if (!conditionId || this.detailPriceRefreshing || this.detailPriceCooldown > 0) return;
      this.detailPriceRefreshing = true;
      try {
        const prices = await this.request(`/api/markets/${encodeURIComponent(conditionId)}/prices`);
        this.followDetail = {
          ...this.followDetail,
          outcomes: prices.outcomes || [],
          outcome_prices: prices.outcome_prices || [],
          price_refreshed_at: prices.updated_at || Math.round(Date.now() / 1000),
        };
        this.startDetailPriceCooldown();
      } catch (error) {
        if (!silent) this.showToast(`盘口刷新失败: ${error.message}`, "error");
      } finally {
        this.detailPriceRefreshing = false;
      }
    },
    startDetailPriceCooldown() {
      this.clearDetailPriceCooldown();
      this.detailPriceCooldown = 3;
      this.detailPriceCooldownTimer = setInterval(() => {
        this.detailPriceCooldown = Math.max(0, this.detailPriceCooldown - 1);
        if (this.detailPriceCooldown <= 0) this.clearDetailPriceCooldown();
      }, 1000);
    },
    clearDetailPriceCooldown() {
      if (this.detailPriceCooldownTimer) {
        clearInterval(this.detailPriceCooldownTimer);
        this.detailPriceCooldownTimer = null;
      }
      this.detailPriceCooldown = 0;
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
    statusText(status) {
      const map = {
        running: "运行中",
        stopping: "停止中",
        stopped: "已停止",
        checking: "检查中",
        healthy: "正常",
        stale: "延迟",
        error: "异常",
        waiting_for_runner: "等待脚本",
        open: "跟单中",
        settled: "已结算",
        exited: "已退出",
        mixed: "混合",
        idle: "空闲",
        succeeded: "已完成",
        failed: "失败",
      };
      return map[status] || status || "-";
    },
    categoryLabel(category) {
      return category === "sports" ? "Sports" : "eSports";
    },
    categoryRefreshStatus(category) {
      const status = this.refreshStatus || {};
      if (status[category]) return status[category];
      if (status.category === category) return status;
      return { status: "idle" };
    },
    walletRefreshBusyFor(category) {
      return Boolean(this.loading.walletRefresh && category === this.activeCategory) || this.categoryRefreshStatus(category).status === "running";
    },
    overviewCategory(category) {
      return (this.overview.by_category || {})[category] || {};
    },
    healthWatchedByCategory(category) {
      return Number((this.health.by_category || {})[category]?.watched_market_count || 0);
    },
    walletCountByCategory(category) {
      return Number((this.wallets.by_category || {})[category]?.count || 0);
    },
    eventStatus(event) {
      if (event?.archived) return event?.settled_count ? "settled" : "ended";
      const start = this.normalizeTs(event?.match_start_time);
      const end = this.normalizeTs(event?.end_date);
      const now = this.clockNow || Math.floor(Date.now() / 1000);
      if (end && now >= end) return "settled";
      if (start && now < start) return "upcoming";
      if (start && (!end || now < end)) return "live";
      return "upcoming";
    },
    eventStatusText(event) {
      const map = { upcoming: "未开始", live: "进行中", settled: "已结算", ended: "已结束" };
      return map[this.eventStatus(event)] || "-";
    },
    compactDuration(seconds) {
      const num = Math.max(0, Math.floor(Number(seconds) || 0));
      const days = Math.floor(num / 86400);
      const hours = Math.floor((num % 86400) / 3600);
      const minutes = Math.floor((num % 3600) / 60);
      if (days > 0) return `${days}d ${hours}h`;
      if (hours > 0) return `${hours}h ${minutes}m`;
      return `${Math.max(1, minutes)}m`;
    },
    eventCountdownText(event) {
      const status = this.eventStatus(event);
      const now = this.clockNow || Math.floor(Date.now() / 1000);
      if (status === "upcoming") {
        const start = this.normalizeTs(event?.match_start_time);
        return start ? `${this.compactDuration(start - now)} 后开始` : "";
      }
      if (status === "live") {
        const end = this.normalizeTs(event?.end_date);
        return end ? `${this.compactDuration(end - now)} 后结束` : "";
      }
      return "";
    },
    eventGame(event) {
      return this.matchParts(event)?.game || "";
    },
    eventFollowText(event) {
      const parts = this.eventFollowParts(event);
      const sideText = parts.sides.map((side) => `${side.label}: ${side.count}单`).join(" ");
      return sideText ? `${parts.total}笔跟单中 · ${sideText}` : `${parts.total}笔跟单中`;
    },
    eventFollowParts(event) {
      const signals = [...(event?.open_signals || []), ...(event?.results || [])];
      const total = signals.length;
      const parts = this.matchParts(event);
      const counts = event?.side_counts || {};
      const countFor = (label, index) => {
        const keys = [label, String(index), String(label || "").trim().toLowerCase()];
        for (const key of keys) {
          if (Object.prototype.hasOwnProperty.call(counts, key)) return Number(counts[key]) || 0;
        }
        for (const [key, value] of Object.entries(counts)) {
          if (String(key).trim().toLowerCase() === String(label || "").trim().toLowerCase()) return Number(value) || 0;
        }
        return 0;
      };
      if (parts) {
        const teamA = parts.teamA || "A";
        const teamB = parts.teamB || "B";
        return {
          total,
          sides: [
            { label: teamA, count: countFor(teamA, 0), logo: this.teamLogo(event, "teamA"), tone: "team-a" },
            { label: teamB, count: countFor(teamB, 1), logo: this.teamLogo(event, "teamB"), tone: "team-b" },
          ],
        };
      }
      return {
        total,
        sides: Object.entries(counts).map(([side, count]) => ({ label: side, count: Number(count) || 0, tone: "" })),
      };
    },
    reasonText(reason) {
      const map = {
        has_losses: "有亏损",
        thin_sample: "样本少",
        low_volume: "资金少",
        weak_entry_price: "入场偏高",
        weak_wilson: "胜率不足",
        weak_entry_edge: "优势不足",
        unstable_returns: "收益不稳",
        stale: "不活跃",
        sold_before_resolution: "提前卖出",
        two_sided_trading: "双边交易",
        low_historical_roi: "ROI偏低",
        negative_roi: "负收益",
        bot_like: "疑似机器人",
      };
      return map[reason] || reason || "-";
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
    elapsedDuration(seconds) {
      const num = Math.max(0, Math.floor(Number(seconds) || 0));
      const hours = Math.floor(num / 3600);
      const minutes = Math.floor((num % 3600) / 60);
      const secs = num % 60;
      if (hours > 0) return `${hours}h ${minutes}m`;
      if (minutes > 0) return `${minutes}m ${secs}s`;
      return `${secs}s`;
    },
    formatTime(value) {
      const ts = this.normalizeTs(value);
      if (!ts) return "-";
      const date = new Date(ts * 1000);
      const parts = new Intl.DateTimeFormat("zh-CN", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      })
        .formatToParts(date)
        .reduce((acc, part) => {
          acc[part.type] = part.value;
          return acc;
        }, {});
      return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
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
    followTitle(follow) {
      return follow.title || follow.question || "未命名赛事";
    },
    marketTypeLabels(row) {
      const labels = row?.eligible_market_type_labels;
      if (Array.isArray(labels) && labels.length) return labels;
      const map = { main_match: "主盘", game_winner: "单局", map_winner: "地图" };
      const types = row?.eligible_market_types || [];
      return Array.isArray(types) ? types.map((type) => map[type] || type).filter(Boolean) : [];
    },
    observedMarketTypeLabels(row) {
      const labels = row?.observed_market_type_labels;
      if (Array.isArray(labels) && labels.length) return labels;
      const map = { main_match: "主盘", game_winner: "单局", map_winner: "地图" };
      const types = row?.observed_market_types || [];
      return Array.isArray(types) ? types.map((type) => map[type] || type).filter(Boolean) : [];
    },
    rankIcon(wallet) {
      const rank = Number(wallet?.rank);
      if (rank === 1) return "/icons/medal-gold.png";
      if (rank === 2) return "/icons/medal-silver.png";
      if (rank === 3) return "/icons/medal-bronze.png";
      return "";
    },
    rankTitle(wallet) {
      const rank = Number(wallet?.rank);
      if (rank === 1) return "Rank 1";
      if (rank === 2) return "Rank 2";
      if (rank === 3) return "Rank 3";
      return `Rank ${rank || "-"}`;
    },
    walletHistoryText(wallet) {
      const wins = Number(wallet?.esports_win_count);
      const losses = Number(wallet?.esports_loss_count);
      if (!Number.isFinite(wins) && !Number.isFinite(losses)) return "-";
      const safeWins = Number.isFinite(wins) ? wins : 0;
      const safeLosses = Number.isFinite(losses) ? losses : 0;
      const total = safeWins + safeLosses;
      const winRate = total > 0 ? ` (${this.percent(safeWins / total)})` : "";
      return `${safeWins}W / ${safeLosses}L${winRate}`;
    },
    walletObservedSettled(wallet) {
      const observed = wallet?.observed || {};
      const signals = Number(observed.signals || 0);
      return Number.isFinite(signals) ? signals : 0;
    },
    walletObservedOpen(wallet) {
      const observed = wallet?.observed || {};
      const open = Number(observed.open || 0);
      return Number.isFinite(open) ? open : 0;
    },
    walletObservedExited(wallet) {
      const observed = wallet?.observed || {};
      const exits = Number(observed.exits || 0);
      return Number.isFinite(exits) ? exits : 0;
    },
    walletObservedRecord(wallet) {
      const observed = wallet?.observed || {};
      const signals = Number(observed.signals || 0);
      const wins = Number(observed.wins || 0);
      const losses = Number(observed.losses || 0);
      if (!signals) return "-";
      return `${wins}W / ${losses}L`;
    },
    walletObservedPnl(wallet) {
      const observed = wallet?.observed || {};
      const signals = Number(observed.signals || 0);
      const exits = Number(observed.exits || 0);
      const pnl = Number(observed.our_pnl);
      if ((!signals && !exits) || !Number.isFinite(pnl)) return "-";
      return this.money(pnl);
    },
    walletFollowModalTitle() {
      const map = { settled: "已结算跟单", open: "持仓中跟单", exited: "提前退出跟单" };
      const label = map[this.walletFollowModal.status] || "钱包跟单";
      const addr = this.walletFollowDetail.short_addr || this.shortId(this.walletFollowModal.wallet);
      return `${addr} · ${label}`;
    },
    signalTitle(signal) {
      return signal.event_title || signal.title || signal.market_question || signal.question || signal.condition_id || "未命名赛事";
    },
    signalFollowTime(signal) {
      const legs = signal.legs || [];
      const times = legs.map((leg) => this.normalizeTs(leg.leg_at || leg.created_at)).filter(Boolean);
      return times.length ? Math.min(...times) : this.normalizeTs(signal.created_at || signal.updated_at);
    },
    signalAverageEntry(signal) {
      let weighted = 0;
      let totalStake = 0;
      for (const leg of signal.legs || []) {
        const stake = Number(leg.stake);
        const entry = Number(leg.our_entry_price);
        if (!Number.isFinite(stake) || !Number.isFinite(entry) || stake <= 0) continue;
        weighted += stake * entry;
        totalStake += stake;
      }
      return totalStake > 0 ? weighted / totalStake : Number(signal.our_entry_price);
    },
    signalStatusText(signal) {
      const status = String(signal.status || "");
      if (status === "settled") return "已结算";
      if (status === "exited") return "提前退出";
      if (status === "open") return "持仓中";
      return this.statusText(status);
    },
    signalSettlementText(signal) {
      const status = String(signal.status || "");
      if (status === "open") return "未结算";
      if (status === "exited") return this.price(signal.exit_price);
      if (status === "settled") return signal.outcome_won ? "1.000（胜）" : "0.000（负）";
      return "-";
    },
    matchParts(value) {
      if (value && typeof value === "object") {
        const parts = value.match_parts;
        if (parts && typeof parts === "object" && parts.teamA && parts.teamB) return parts;
        value = value.title || value.question || value.market_title || value.event_title || value.market_question || value.condition_id || "";
      }
      const text = String(value || "");
      if (!text) return null;
      if (this.matchTitleCache[text] !== undefined) return this.matchTitleCache[text];
      const match = text.match(/^([^:]+):\s+(.+?)\s+vs\s+(.+?)(\s+\([^)]+\))?\s+-\s+(.+)$/i);
      if (!match) {
        this.matchTitleCache[text] = null;
        return null;
      }
      const parsed = {
        game: match[1].trim(),
        teamA: match[2].trim(),
        teamB: match[3].trim(),
        meta: `${(match[4] || "").trim()} ${match[5].trim()}`.trim(),
      };
      this.matchTitleCache[text] = parsed;
      return parsed;
    },
    teamLogo(row, side) {
      const logos = row?.team_logos || {};
      return typeof logos[side] === "string" ? logos[side] : "";
    },
    signalOutcomeSide(signal) {
      const outcome = String(signal?.outcome || "").trim().toLowerCase();
      const parts = this.matchParts(this.followDetail);
      if (!outcome || !parts) return "";
      if (outcome === String(parts.teamA || "").trim().toLowerCase()) return "teamA";
      if (outcome === String(parts.teamB || "").trim().toLowerCase()) return "teamB";
      return "";
    },
    signalOutcomeLogo(signal) {
      const side = this.signalOutcomeSide(signal);
      return side ? this.teamLogo(this.followDetail, side) : "";
    },
    signalOutcomeSideClass(signal) {
      const side = this.signalOutcomeSide(signal);
      if (side === "teamA") return "team-a";
      if (side === "teamB") return "team-b";
      return "";
    },
    detailTitle() {
      return this.followDetail.title || this.followDetail.question || "跟单详情";
    },
    detailEventUrl() {
      return this.followDetail.event_url || "";
    },
    detailSignals() {
      return (this.followDetail.wallets || []).flatMap((wallet) => wallet.signals || []);
    },
    detailSettlementText() {
      const signals = this.detailSignals();
      const winning = signals.find((signal) => String(signal.status || "") === "settled" && signal.outcome_won === true);
      if (winning?.outcome) return `胜方 - ${winning.outcome}`;
      if (signals.some((signal) => String(signal.status || "") === "settled")) return "已结算";
      return "未结算";
    },
    detailSettlementClass() {
      return this.detailSettlementText() === "未结算" ? "detail-settlement-pending" : "detail-settlement-done";
    },
    detailMarketPrices() {
      const outcomes = this.asArray(this.followDetail.outcomes);
      const prices = this.asArray(this.followDetail.outcome_prices);
      return outcomes
        .map((outcome, index) => ({ outcome: String(outcome || `方向 ${index + 1}`), price: Number(prices[index]) }))
        .filter((row) => Number.isFinite(row.price));
    },
    detailMarketPriceByOutcomeIndex(index) {
      const prices = this.asArray(this.followDetail.outcome_prices);
      const price = Number(prices[Number(index)]);
      return Number.isFinite(price) ? price : null;
    },
    signalUnrealizedPnl(signal) {
      if (String(signal?.status || "open") !== "open") return 0;
      const currentPrice = this.detailMarketPriceByOutcomeIndex(signal?.outcome_index);
      if (!Number.isFinite(currentPrice)) return 0;
      return (signal.legs || []).reduce((total, leg) => {
        if (leg.would_follow === false) return total;
        const stake = Number(leg.stake);
        const entry = Number(leg.our_entry_price);
        if (!Number.isFinite(stake) || !Number.isFinite(entry) || entry <= 0) return total;
        return total + (stake * (currentPrice - entry)) / entry;
      }, 0);
    },
    detailUnrealizedPnl() {
      const signals = this.detailSignals();
      if (!signals.length || !this.detailMarketPrices().length) return null;
      const value = signals.reduce((total, signal) => total + this.signalUnrealizedPnl(signal), 0);
      return Number.isFinite(value) ? value : null;
    },
    detailHasUnrealizedPnl() {
      return this.detailUnrealizedPnl() !== null && this.detailSignals().some((signal) => String(signal?.status || "open") === "open");
    },
    asArray(value) {
      if (Array.isArray(value)) return value;
      if (typeof value !== "string") return [];
      try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed : [];
      } catch (_error) {
        return [];
      }
    },
    walletLegs(wallet) {
      return (wallet.signals || []).flatMap((signal) => signal.legs || []);
    },
    walletFollowedLegs(wallet) {
      return this.walletLegs(wallet).filter((leg) => leg.would_follow !== false);
    },
    walletTotalStake(wallet) {
      return this.walletFollowedLegs(wallet).reduce((total, leg) => {
        const stake = Number(leg.stake);
        return Number.isFinite(stake) ? total + stake : total;
      }, 0);
    },
    walletAverageEntry(wallet) {
      let weighted = 0;
      let totalStake = 0;
      for (const leg of this.walletFollowedLegs(wallet)) {
        const stake = Number(leg.stake);
        const entry = Number(leg.our_entry_price);
        if (!Number.isFinite(stake) || !Number.isFinite(entry) || stake <= 0) continue;
        weighted += stake * entry;
        totalStake += stake;
      }
      return totalStake > 0 ? weighted / totalStake : null;
    },
    walletRealizedPnl(wallet) {
      return (wallet.signals || []).reduce((total, signal) => {
        const status = String(signal.status || "");
        if (status !== "settled" && status !== "exited") return total;
        const pnl = Number(signal.our_paper_pnl ?? signal.our_realized_pnl);
        return Number.isFinite(pnl) ? total + pnl : total;
      }, 0);
    },
    walletHasRealizedPnl(wallet) {
      return (wallet.signals || []).some((signal) => {
        const status = String(signal.status || "");
        return status === "settled" || status === "exited";
      });
    },
    signalPageKey(signal) {
      return signal.signal_id || `${signal.condition_id || "signal"}:${signal.outcome_index || signal.outcome || "side"}`;
    },
    signalLegPage(signal) {
      return this.detailLegPages[this.signalPageKey(signal)] || 1;
    },
    signalLegPageCount(signal) {
      return Math.max(1, Math.ceil(((signal.legs || []).length || 0) / 10));
    },
    signalVisibleLegs(signal) {
      const page = Math.min(this.signalLegPage(signal), this.signalLegPageCount(signal));
      const start = (page - 1) * 10;
      return (signal.legs || []).slice(start, start + 10);
    },
    setSignalLegPage(signal, page) {
      const key = this.signalPageKey(signal);
      const next = Math.min(Math.max(1, page), this.signalLegPageCount(signal));
      this.detailLegPages = { ...this.detailLegPages, [key]: next };
    },
    legWalletTradeCash(leg) {
      const direct = Number(leg.wallet_trade_cash ?? leg.wallet_trade_value ?? leg.cash);
      if (Number.isFinite(direct) && direct > 0) return direct;
      const size = Number(leg.wallet_trade_size ?? leg.wallet_trade_amount ?? leg.size ?? leg.amount);
      const price = Number(leg.wallet_fill_price ?? leg.wallet_avg_price);
      return Number.isFinite(size) && Number.isFinite(price) ? size * price : null;
    },
    legSlippageValue(leg) {
      const value = leg.slippage_over_wallet_entry ?? leg.slippage;
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    },
    legSlippageText(leg) {
      const value = this.legSlippageValue(leg);
      return value == null ? "-" : this.signedPctPoints(value);
    },
  },
}).mount("#app");
