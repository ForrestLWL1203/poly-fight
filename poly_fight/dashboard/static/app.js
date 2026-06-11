const { createApp } = Vue;

createApp({
  data() {
    return {
      authenticated: false,
      authChecking: true,
      activeTab: "follows",
      activeCategory: "esports",
      walletView: "active",
      tabs: [
        { id: "follows", label: "跟单" },
        { id: "events", label: "赛事" },
      ],
      categoryTabs: [
        { id: "esports", label: "eSports" },
      ],
      loginForm: { username: "admin", password: "" },
      loginError: "",
      loading: {},
      runnerActionText: "",
      runnerStakeRatioInput: "",
      runnerMaxStakeInput: "",
      runnerSignalStakePercentInput: "",
      accountBalanceInput: "",
      accountBalanceDirty: false,
      accountBalanceSaving: false,
      health: {},
      overview: {},
      wallets: { wallets: [] },
      follows: { follows: [], total: 0 },
      events: { events: [] },
      refreshStatus: { status: "idle" },
      runner: { status: "checking" },
      runnerWarningMessage: "",
      runnerWarningTimer: null,
      pauseFollow: null,
      favoriteBusy: {},
      quarantineBusy: {},
      walletPage: 1,
      walletSize: 10,
      walletFollowPage: 1,
      walletFollowSize: 20,
      followPage: 1,
      followSize: 10,
      followStatusFilter: "",
      followStatusOptions: [
        { value: "", label: "全部状态" },
        { value: "open", label: "跟单中" },
        { value: "insufficient_balance", label: "余额不足" },
        { value: "settled", label: "已结算" },
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
      return this.runner.status !== "running" && (
        !this.hasFollowWallets
        || !this.runnerStakeRatioValid
        || !this.runnerMaxStakeValid
        || !this.runnerSignalStakePercentValid
      );
    },
    runnerStakeRatioValue() {
      const value = Number(this.runnerStakeRatioInput);
      return Number.isFinite(value) ? value : 0;
    },
    runnerStakeRatioValid() {
      return this.runnerStakeRatioValue > 0;
    },
    runnerMaxStakeValue() {
      const raw = String(this.runnerMaxStakeInput || "").trim();
      if (!raw) return 0;
      const value = Number(raw);
      return Number.isFinite(value) ? value : -1;
    },
    runnerMaxStakeValid() {
      return this.runnerMaxStakeValue >= 0;
    },
    runnerSignalStakePercentValue() {
      const raw = String(this.runnerSignalStakePercentInput || "").trim();
      if (!raw) return 0;
      const value = Number(raw);
      return Number.isFinite(value) ? value : -1;
    },
    runnerSignalStakePercentValid() {
      return this.runnerSignalStakePercentValue >= 0;
    },
    runnerControlTitle() {
      if (this.runner.status !== "running" && !this.hasFollowWallets) return "需先采集目标跟单钱包";
      if (this.runner.status !== "running" && !this.runnerStakeRatioValid) return "需填写跟单比例";
      if (this.runner.status !== "running" && !this.runnerMaxStakeValid) return "单笔跟单限额格式不正确";
      if (this.runner.status !== "running" && !this.runnerSignalStakePercentValid) return "单场余额比例格式不正确";
      return this.runner.status === "running" ? "停止跟单脚本" : "启动跟单脚本";
    },
    accountBalanceState() {
      return this.overview?.account_balance || {};
    },
    accountBalanceConfigured() {
      return Boolean(this.accountBalanceState.configured);
    },
    accountBalanceText() {
      return this.accountBalanceConfigured ? this.money(this.accountBalanceState.balance_usdc) : "无限 Paper";
    },
    accountBalanceTitle() {
      return this.accountBalanceConfigured ? "当前钱包余额；v1 由可动用金额上限初始化" : "未设置可动用金额上限时沿用无限 paper 资金";
    },
    accountBalanceLocked() {
      return this.runner.status === "running" || this.runner.status === "stopping";
    },
    accountBalanceInputValue() {
      return this.parseUsdcInput(this.accountBalanceInput);
    },
    accountBalanceMaxDisabled() {
      const value = Number(this.accountBalanceState.balance_usdc);
      return this.accountBalanceLocked || !this.accountBalanceConfigured || !Number.isFinite(value) || value < 0;
    },
    accountBalanceSaveBlocked() {
      const raw = String(this.accountBalanceInput || "").trim();
      const value = this.accountBalanceInputValue;
      return !raw || !Number.isFinite(value) || value < 0 || this.accountBalanceSaving || this.accountBalanceLocked;
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
      return (this.wallets.wallets || []).filter((wallet) => {
        if ((wallet.category || "esports") !== this.activeCategory) return false;
        if (this.walletView === "favorite") return Boolean(wallet.favorite);
        if (this.walletView === "quarantined") return Boolean(wallet.quarantined) && !wallet.favorite;
        return !wallet.favorite && !wallet.quarantined;
      });
    },
    walletViewOptions() {
      const category = this.activeCategory;
      const summary = (this.wallets.by_category || {})[category] || {};
      const activeCount = Number(summary.active_count ?? (this.wallets.active_count || 0));
      const favoriteCount = Number(summary.favorite_count ?? (this.wallets.favorite_count || 0));
      const quarantinedCount = Number(summary.quarantined_count ?? (this.wallets.quarantined_count || 0));
      return [
        { value: "active", label: "生效中", count: activeCount },
        { value: "favorite", label: "收藏", count: favoriteCount },
        { value: "quarantined", label: "隔离", count: quarantinedCount },
      ];
    },
    walletEmptyText() {
      if (this.walletView === "favorite") return "暂无收藏钱包";
      if (this.walletView === "quarantined") return "暂无隔离钱包";
      return "暂无生效钱包";
    },
    walletEmptyColspan() {
      return this.walletView === "quarantined" ? 16 : 15;
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
    walletFollowPageCount() {
      const count = Number(this.walletFollowDetail.total || this.walletFollowDetail.count || 0);
      return Math.max(1, Math.ceil(count / this.walletFollowSize));
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
      if (payload.overview) {
        this.overview = payload.overview;
        this.syncAccountBalanceInput(payload.overview);
      }
      if (payload.runner) {
        this.runner = payload.runner;
        this.syncRunnerInputs();
      }
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
      this.syncAccountBalanceInput(this.overview);
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
          const aFollowed = Number(a.open_signal_count || 0) > 0 ? 0 : Number(a.result_count || 0) > 0 ? 1 : 2;
          const bFollowed = Number(b.open_signal_count || 0) > 0 ? 0 : Number(b.result_count || 0) > 0 ? 1 : 2;
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
      this.syncRunnerInputs();
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
        const message = !this.hasFollowWallets
          ? "需先采集目标跟单钱包"
          : !this.runnerStakeRatioValid
            ? "需填写跟单比例"
            : !this.runnerMaxStakeValid
              ? "单笔跟单限额格式不正确"
              : "单场余额比例格式不正确";
        this.showRunnerWarning(message);
        return;
      }
      await this.loadRunner();
      if (this.runner.status === "running") {
        this.showToast("跟单脚本已经在运行");
        return;
      }
      if (!this.runnerStakeRatioValid) {
        this.showRunnerWarning("需填写跟单比例");
        return;
      }
      if (!this.runnerMaxStakeValid) {
        this.showRunnerWarning("单笔跟单限额格式不正确");
        return;
      }
      if (!this.runnerSignalStakePercentValid) {
        this.showRunnerWarning("单场余额比例格式不正确");
        return;
      }
      this.loading.runner = true;
      this.runnerActionText = "正在启动跟单脚本并刷新数据";
      try {
        const body = { stake_ratio_percent: this.runnerStakeRatioValue };
        if (this.runnerMaxStakeValue > 0) body.max_stake_usdc = this.runnerMaxStakeValue;
        if (this.runnerSignalStakePercentValue > 0) {
          body.max_signal_stake_balance_percent = this.runnerSignalStakePercentValue;
        }
        this.runner = await this.request("/api/runner/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
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
    syncRunnerInputs() {
      if (this.runner.status === "running" && this.runner.stake_ratio_percent) {
        this.runnerStakeRatioInput = String(this.runner.stake_ratio_percent);
      }
      const maxStake = Number(this.runner.max_stake_usdc);
      if (this.runner.status === "running" && Number.isFinite(maxStake) && maxStake > 0) {
        this.runnerMaxStakeInput = String(maxStake);
      }
      const signalStakePercent = Number(this.runner.max_signal_stake_balance_percent);
      if (this.runner.status === "running" && Number.isFinite(signalStakePercent) && signalStakePercent > 0) {
        this.runnerSignalStakePercentInput = String(signalStakePercent);
      }
    },
    syncAccountBalanceInput(overview) {
      if (this.accountBalanceDirty) return;
      const state = overview?.account_balance || {};
      const value = Number(state.balance_usdc);
      this.accountBalanceInput = state.configured && Number.isFinite(value) ? this.formatInputAmount(value) : "";
    },
    parseUsdcInput(value) {
      const raw = String(value || "").replace(/,/g, "").trim();
      if (!raw) return NaN;
      if (/^\d+(?:\.\d+)?$/.test(raw)) return Number(raw);
      const match = raw.match(/\d+(?:\.\d+)?/);
      return match ? Number(match[0]) : NaN;
    },
    formatInputAmount(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "";
      return String(Math.round(num * 100000000) / 100000000);
    },
    fillAccountBalanceMax() {
      if (this.accountBalanceMaxDisabled) return;
      this.accountBalanceInput = this.formatInputAmount(this.accountBalanceState.balance_usdc);
      this.accountBalanceDirty = true;
    },
    async saveAccountBalance() {
      if (this.accountBalanceSaveBlocked) {
        this.showToast(this.accountBalanceLocked ? "跟单运行中不可修改可动用金额上限" : "请输入有效的可动用金额上限", "error");
        return;
      }
      const nextBalance = this.accountBalanceInputValue;
      this.accountBalanceSaving = true;
      try {
        await this.request("/api/account-balance", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ balance_usdc: nextBalance }),
        });
        this.accountBalanceInput = this.formatInputAmount(nextBalance);
        this.accountBalanceDirty = false;
        await this.loadOverview();
        this.showToast("可动用金额上限已更新");
      } catch (error) {
        this.showToast(`上限保存失败: ${error.message}`, "error");
      } finally {
        this.accountBalanceSaving = false;
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
    setWalletView(view) {
      this.walletView = ["active", "favorite", "quarantined"].includes(view) ? view : "active";
      this.walletPage = 1;
    },
    async setCategory(category) {
      this.activeCategory = "esports";
      this.walletView = "active";
      this.walletPage = 1;
      this.followPage = 1;
      this.eventPage = 1;
      await Promise.allSettled([this.loadFollows(), this.loadEvents()]);
    },
    walletFavoriteKey(wallet) {
      return `${wallet?.category || "esports"}:${wallet?.wallet || ""}`;
    },
    favoriteButtonTitle(wallet) {
      return wallet.favorite ? "取消收藏" : "收藏钱包";
    },
    async toggleWalletFavorite(wallet) {
      if (!wallet?.wallet) return;
      const key = this.walletFavoriteKey(wallet);
      if (this.favoriteBusy[key]) return;
      const nextFavorite = !wallet.favorite;
      this.favoriteBusy = { ...this.favoriteBusy, [key]: true };
      try {
        await this.request("/api/wallet-favorites", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            wallet: wallet.wallet,
            category: wallet.category || this.activeCategory,
            favorite: nextFavorite,
          }),
        });
        await this.loadWallets();
        if (nextFavorite) {
          this.setWalletView("favorite");
          this.showToast("已收藏钱包");
        } else {
          const remaining = (this.wallets.wallets || []).find((row) => (
            row.wallet === wallet.wallet && (row.category || "esports") === (wallet.category || this.activeCategory)
          ));
          if (remaining?.favorite) {
            this.setWalletView("favorite");
          } else if (remaining?.quarantined) {
            this.setWalletView("quarantined");
          } else if (remaining) {
            this.setWalletView("active");
          } else {
            this.walletPage = 1;
          }
          this.showToast(remaining ? "已取消收藏" : "已取消收藏，等待下次评分回榜");
        }
      } catch (error) {
        this.showToast(`收藏操作失败: ${error.message}`, "error");
      } finally {
        const nextBusy = { ...this.favoriteBusy };
        delete nextBusy[key];
        this.favoriteBusy = nextBusy;
      }
    },
    quarantineButtonTitle(wallet) {
      return `隔离 ${wallet?.short_addr || wallet?.wallet || "钱包"}`;
    },
    unquarantineButtonTitle(wallet) {
      return `解除隔离 ${wallet?.short_addr || wallet?.wallet || "钱包"}`;
    },
    async quarantineWallet(wallet) {
      if (!wallet?.wallet) return;
      const key = this.walletFavoriteKey(wallet);
      if (this.quarantineBusy[key]) return;
      this.quarantineBusy = { ...this.quarantineBusy, [key]: true };
      try {
        await this.request("/api/wallet-quarantine", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            wallet: wallet.wallet,
            category: wallet.category || this.activeCategory,
          }),
        });
        await this.loadWallets();
        this.setWalletView("quarantined");
        this.showToast("已隔离钱包，runner 下一轮不会继续开新跟单");
      } catch (error) {
        this.showToast(`隔离失败: ${error.message}`, "error");
      } finally {
        const nextBusy = { ...this.quarantineBusy };
        delete nextBusy[key];
        this.quarantineBusy = nextBusy;
      }
    },
    async unquarantineWallet(wallet) {
      if (!wallet?.wallet) return;
      const key = this.walletFavoriteKey(wallet);
      if (this.quarantineBusy[key]) return;
      this.quarantineBusy = { ...this.quarantineBusy, [key]: true };
      try {
        await this.request("/api/wallet-quarantine", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            wallet: wallet.wallet,
            category: wallet.category || this.activeCategory,
            quarantined: false,
          }),
        });
        await this.loadWallets();
        const stillVisible = (this.wallets.wallets || []).find((row) => (
          row.wallet === wallet.wallet && (row.category || "esports") === (wallet.category || this.activeCategory)
        ));
        if (stillVisible?.favorite) {
          this.setWalletView("favorite");
        } else if (stillVisible?.quarantined) {
          this.setWalletView("quarantined");
        } else if (stillVisible) {
          this.setWalletView("active");
        } else {
          this.walletPage = 1;
        }
        this.showToast(stillVisible ? "已解除隔离" : "已解除隔离，等待下次采样评分回榜");
      } catch (error) {
        this.showToast(`解除隔离失败: ${error.message}`, "error");
      } finally {
        const nextBusy = { ...this.quarantineBusy };
        delete nextBusy[key];
        this.quarantineBusy = nextBusy;
      }
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
      this.walletFollowPage = 1;
      await this.loadWalletFollowDetail();
    },
    async loadWalletFollowDetail() {
      if (!this.walletFollowModal.wallet) return;
      this.walletFollowModal.loading = true;
      try {
        const query = new URLSearchParams({
          wallet: this.walletFollowModal.wallet,
          status: this.walletFollowModal.status || "",
          page: String(this.walletFollowPage),
          size: String(this.walletFollowSize),
        });
        try {
          this.walletFollowDetail = await this.request(`/api/wallet-follows?${query.toString()}`);
        } catch (error) {
          if (error.message !== "not_found") throw error;
          const params = new URLSearchParams({
            status: this.walletFollowModal.status || "",
            page: String(this.walletFollowPage),
            size: String(this.walletFollowSize),
          });
          this.walletFollowDetail = await this.request(`/api/wallets/${encodeURIComponent(this.walletFollowModal.wallet)}/follows?${params.toString()}`);
        }
      } catch (error) {
        this.showToast(`钱包跟单详情加载失败: ${error.message}`, "error");
      } finally {
        this.walletFollowModal.loading = false;
      }
    },
    async setWalletFollowPage(page) {
      const next = Math.min(Math.max(1, page), this.walletFollowPageCount);
      if (next === this.walletFollowPage) return;
      this.walletFollowPage = next;
      await this.loadWalletFollowDetail();
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
    showRunnerWarning(message) {
      this.runnerWarningMessage = message;
      if (this.runnerWarningTimer) clearTimeout(this.runnerWarningTimer);
      this.runnerWarningTimer = setTimeout(() => {
        this.runnerWarningMessage = "";
        this.runnerWarningTimer = null;
      }, 3200);
    },
    profileUrl(wallet) {
      return `https://polymarket.com/@${wallet}?tab=activity`;
    },
    eventUrl(event) {
      const explicit = String(event?.event_url || "").trim();
      if (explicit) return explicit;
      const slug = String(event?.event_slug || "").trim();
      return slug ? `https://polymarket.com/event/${encodeURIComponent(slug)}` : "";
    },
    eventRowAriaLabel(event) {
      if (!this.eventUrl(event)) return null;
      const title = event?.title || event?.question || event?.condition_id || "赛事";
      return `打开 Polymarket 赛事主页：${title}`;
    },
    openEventPage(event, domEvent) {
      const url = this.eventUrl(event);
      if (!url) return;
      const target = domEvent?.target;
      if (target?.closest?.("a,button,input,select,textarea,label")) return;
      const opened = window.open(url, "_blank");
      if (opened) {
        opened.opener = null;
      } else {
        window.location.assign(url);
      }
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
        insufficient_balance: "余额不足",
        settled: "已结算",
        exited: "已结算",
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
        return end ? `${this.compactDuration(end - now)} 后截止` : "";
      }
      return "";
    },
    eventGame(event) {
      return this.matchParts(event)?.game || "";
    },
    eventFollowText(event) {
      const rows = this.eventMarketFollowRows(event);
      return rows
        .map((row) => {
          const sideText = row.sides.map((side) => `${side.label}: ${side.count}单`).join(" ");
          return sideText ? `${row.label} ${sideText}` : row.label;
        })
        .join(" · ");
    },
    eventFollowParts(event) {
      const total = Number(event?.signal_count ?? (Number(event?.open_signal_count || 0) + Number(event?.result_count || 0))) || 0;
      const parts = this.matchParts(event);
      const counts = event?.side_counts || {};
      if (parts) {
        const teamA = parts.teamA || "A";
        const teamB = parts.teamB || "B";
        return {
          total,
          sides: [
            { label: teamA, count: this.sideCountValue(counts, teamA, 0), logo: this.teamLogo(event, "teamA"), tone: "team-a" },
            { label: teamB, count: this.sideCountValue(counts, teamB, 1), logo: this.teamLogo(event, "teamB"), tone: "team-b" },
          ],
        };
      }
      return {
        total,
        sides: Object.entries(counts).map(([side, count]) => ({ label: side, count: Number(count) || 0, tone: "" })),
      };
    },
    eventMarketFollowRows(event) {
      const breakdown = Array.isArray(event?.market_breakdown) && event.market_breakdown.length
        ? event.market_breakdown
        : [
            {
              condition_id: event?.condition_id,
              question: event?.question,
              title: event?.title,
              market_type: event?.market_type,
              market_type_label: event?.market_type_label,
              outcomes: event?.outcomes,
              signal_count: this.eventFollowParts(event).total,
              side_counts: event?.side_counts || {},
            },
          ];
      const parts = this.matchParts(event);
      return breakdown.map((market, index) => {
        const counts = market?.side_counts || {};
        const countTotal = Object.values(counts).reduce((sum, value) => sum + (Number(value) || 0), 0);
        const signalCount = Number(market?.signal_count ?? countTotal) || countTotal;
        let sides = [];
        if (parts) {
          const teamA = parts.teamA || "A";
          const teamB = parts.teamB || "B";
          sides = [
            { label: teamA, count: this.sideCountValue(counts, teamA, 0), logo: this.teamLogo(event, "teamA"), tone: "team-a" },
            { label: teamB, count: this.sideCountValue(counts, teamB, 1), logo: this.teamLogo(event, "teamB"), tone: "team-b" },
          ];
        } else {
          const outcomes = this.marketOutcomes(market);
          if (outcomes.length) {
            sides = outcomes.slice(0, 2).map((label, outcomeIndex) => ({
              label,
              count: this.sideCountValue(counts, label, outcomeIndex),
              logo: "",
              tone: outcomeIndex === 0 ? "team-a" : outcomeIndex === 1 ? "team-b" : "",
            }));
          } else {
            sides = Object.entries(counts).map(([label, count]) => ({ label, count: Number(count) || 0, logo: "", tone: "" }));
          }
        }
        return {
          key: market?.condition_id || `${event?.condition_id || "event"}:${index}`,
          label: this.eventMarketDisplayLabel(market, index),
          signalCount,
          sides,
        };
      });
    },
    eventMarketFollowTable(event) {
      const rows = this.eventMarketFollowRows(event);
      const firstSides = rows.find((row) => (row.sides || []).length)?.sides || [];
      let headers = firstSides.slice(0, 2);
      if (!headers.length) {
        headers = [
          { label: "A", count: 0, logo: "", tone: "team-a" },
          { label: "B", count: 0, logo: "", tone: "team-b" },
        ];
      }
      return {
        headers,
        rows: rows.map((row) => ({
          ...row,
          cells: headers.map((header, index) => {
            const side = (row.sides || [])[index] || {};
            return {
              label: header.label,
              count: Number(side.count) || 0,
              logo: header.logo || side.logo || "",
              tone: header.tone || side.tone || "",
            };
          }),
        })),
      };
    },
    eventMarketDisplayLabel(market, index) {
      const type = String(market?.market_type || "").trim();
      const rawLabel = String(market?.market_type_label || "").trim();
      const text = String(market?.question || market?.title || "").trim();
      const mapMatch = text.match(/\bmap\s*(\d+)\b/i) || text.match(/地图\s*(\d+)/i);
      const gameMatch = text.match(/\bgame\s*(\d+)\b/i) || text.match(/第\s*(\d+)\s*局/i);
      if (type === "main_match" || rawLabel === "主盘") return "主盘";
      if (type === "map_winner") return mapMatch ? `地图${mapMatch[1]}` : rawLabel || `地图${index + 1}`;
      if (type === "game_winner") return gameMatch ? `第${gameMatch[1]}局` : rawLabel || `第${index + 1}局`;
      return rawLabel || type || `盘口${index + 1}`;
    },
    marketOutcomes(market) {
      const outcomes = market?.outcomes;
      if (Array.isArray(outcomes)) return outcomes.map((value) => String(value || "").trim()).filter(Boolean);
      if (typeof outcomes === "string") {
        try {
          const parsed = JSON.parse(outcomes);
          if (Array.isArray(parsed)) return parsed.map((value) => String(value || "").trim()).filter(Boolean);
        } catch (_err) {
          return outcomes.split(",").map((value) => value.trim()).filter(Boolean);
        }
      }
      return [];
    },
    sideCountValue(counts, label, index) {
      const normalized = String(label || "").trim().toLowerCase();
      const keys = [label, String(index), normalized];
      for (const key of keys) {
        if (Object.prototype.hasOwnProperty.call(counts, key)) return Number(counts[key]) || 0;
      }
      for (const [key, value] of Object.entries(counts || {})) {
        if (String(key).trim().toLowerCase() === normalized) return Number(value) || 0;
      }
      return 0;
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
        material_sell: "旧规则提前卖出",
        two_sided_switch: "旧规则双边切换",
        observed_paper_underperformance: "近期表现差",
        recent_chop_loss: "反复双边",
        manual_dashboard_quarantine: "手动隔离",
        low_historical_roi: "ROI偏低",
        negative_roi: "负收益",
        bot_like: "疑似机器人",
      };
      return map[reason] || reason || "-";
    },
    quarantineReasonText(wallet) {
      return this.reasonText((wallet?.quarantine || {}).reason);
    },
    quarantineTimeText(wallet) {
      return this.formatTime((wallet?.quarantine || {}).quarantined_at);
    },
    gradeClass(grade) {
      if (grade === "A") return "badge-success";
      if (grade === "B") return "badge-info";
      if (grade === "C") return "badge-warning";
      return "badge-neutral";
    },
    qualityFlags(row) {
      const label = String(row?.quality_label || "");
      const twoSided = label.includes("two_sided") || Number(row?.two_sided_signal_count || 0) > 0 || row?.quality_two_sided === true;
      const disagreement =
        label.includes("disagreement") ||
        Number(row?.disagreement_signal_count || 0) > 0 ||
        row?.quality_disagreement === true ||
        (!twoSided && row?.contested === true);
      return { twoSided, disagreement };
    },
    qualityBadgeText(row) {
      const { twoSided, disagreement } = this.qualityFlags(row);
      if (twoSided && disagreement) return "双边 + 分歧";
      if (disagreement) return "分歧";
      if (twoSided) return "双边";
      return "单向";
    },
    qualityBadgeClass(row) {
      const { twoSided, disagreement } = this.qualityFlags(row);
      if (twoSided && disagreement) return "badge-error badge-outline";
      if (disagreement) return "badge-error badge-outline";
      if (twoSided) return "badge-warning badge-outline";
      return "badge-success badge-outline";
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
    signedMoney(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      const formatted = this.money(Math.abs(num));
      if (Math.abs(num) < 0.000001) return formatted;
      return `${num > 0 ? "+" : "-"}${formatted}`;
    },
    compactMoney(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        notation: "compact",
        maximumFractionDigits: num >= 1000 ? 1 : 0,
      }).format(num);
    },
    stakeModeLabel(mode) {
      const value = String(mode || "fixed");
      if (value === "proportional") return "比例";
      if (value === "minimum") return "下限";
      if (value === "capped") return "封顶";
      if (value === "skipped") return "跳过";
      return "固定";
    },
    stakeModeClass(mode) {
      const value = String(mode || "fixed");
      if (value === "proportional") return "tier-conviction";
      if (value === "minimum") return "tier-normal";
      if (value === "capped") return "tier-downgraded";
      if (value === "skipped") return "tier-skipped";
      return "tier-normal";
    },
    primaryStakeMode(row) {
      const counts = row?.stake_mode_counts || {};
      for (const mode of ["proportional", "minimum", "capped", "fixed"]) {
        if (Number(counts[mode] || 0) > 0) return mode;
      }
      return "fixed";
    },
    stakeModeSummary(row) {
      const counts = row?.stake_mode_counts || {};
      const parts = [];
      for (const mode of ["proportional", "minimum", "capped", "fixed"]) {
        const count = Number(counts[mode] || 0);
        if (count > 0) parts.push(`${this.stakeModeLabel(mode)} ${count}`);
      }
      return parts.length ? parts.join(" / ") : "固定";
    },
    signalStakeRange(row) {
      const min = Number(row?.signal_stake_min);
      const max = Number(row?.signal_stake_max);
      if (!Number.isFinite(min) || min <= 0) return "";
      if (!Number.isFinite(max) || max <= 0 || Math.abs(max - min) < 0.000001) return this.money(min);
      return `${this.money(min)}-${this.money(max)}`;
    },
    followListPnl(row) {
      const display = Number(row?.display_pnl);
      if (Number.isFinite(display)) return display;
      const realized = Number(row?.our_realized_pnl);
      return Number.isFinite(realized) ? realized : null;
    },
    percent(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "-";
      return `${(num * 100).toFixed(1)}%`;
    },
    bucketScoreText(row) {
      const num = Number(row?.best_bucket_score);
      return Number.isFinite(num) ? num.toFixed(2) : "-";
    },
    overallRoiText(row) {
      if ((row?.category || "esports") === "sports") return "";
      const overall = Number(row?.overall_esports_roi);
      const bucket = Number(row?.esports_roi);
      if (!Number.isFinite(overall) || !Number.isFinite(bucket)) return "";
      if (Math.abs(overall - bucket) < 0.0005) return "";
      return `Overall ${this.percent(overall)}`;
    },
    recentBucketText(row) {
      const count = Number(row?.recent_bucket_market_count);
      const windowDays = Number(row?.recent_bucket_window_days);
      if (!Number.isFinite(count) || count < 3 || !Number.isFinite(windowDays) || windowDays <= 0) {
        return "样本不足";
      }
      return `ROI: ${this.percent(row?.recent_bucket_roi)}`;
    },
    recentBucketSubtext(row) {
      const count = Number(row?.recent_bucket_market_count);
      const windowDays = Number(row?.recent_bucket_window_days);
      if (!Number.isFinite(count) || count <= 0) return [];
      if (!Number.isFinite(windowDays) || windowDays <= 0 || count < 3) return [`${count} 场`];
      return [`${count} 场`, `胜率: ${this.percent(row?.recent_bucket_positive_rate)}`];
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
    trackingDurationText() {
      const startedAt = this.normalizeTs(this.overview?.tracking_started_at);
      const rawDuration = Number(this.overview?.tracking_duration_seconds);
      const duration = Number.isFinite(rawDuration) && rawDuration >= 0
        ? rawDuration
        : (startedAt ? this.clockNow - startedAt : 0);
      if (!startedAt && duration <= 0) return "";
      const seconds = Math.max(0, Math.floor(duration));
      if (seconds < 3600) return "已跟踪 <1小时";
      const days = Math.floor(seconds / 86400);
      const hours = Math.floor((seconds % 86400) / 3600);
      if (days > 0) return `已跟踪 ${days}天${hours}小时`;
      return `已跟踪 ${hours}小时`;
    },
    trackingDurationTitle() {
      const startedAt = this.normalizeTs(this.overview?.tracking_started_at);
      return startedAt ? `开始跟踪：${this.formatTime(startedAt)}` : "";
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
      const deltaSeconds = Math.floor(Date.now() / 1000 - ts);
      const formatDuration = (seconds) => {
        if (seconds < 5) return "刚刚";
        if (seconds < 60) return `${seconds}s`;
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return `${minutes}m`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h`;
        const days = Math.floor(hours / 24);
        return `${days}d${hours % 24}h`;
      };
      if (deltaSeconds >= 0) {
        const duration = formatDuration(deltaSeconds);
        return duration === "刚刚" ? duration : `${duration} ago`;
      }
      const duration = formatDuration(Math.abs(deltaSeconds));
      return duration === "刚刚" ? duration : `${duration} 后`;
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
    marketTypeLabel(type) {
      const map = { main_match: "主盘", game_winner: "单局", map_winner: "地图" };
      return map[type] || type || "";
    },
    marketTypeLabels(row) {
      const labels = row?.eligible_market_type_labels;
      if (Array.isArray(labels) && labels.length) return labels;
      const types = row?.eligible_market_types || [];
      return Array.isArray(types) ? types.map((type) => this.marketTypeLabel(type)).filter(Boolean) : [];
    },
    gameFamilyLabel(game) {
      const map = {
        cs2: "CS2",
        "counter-strike 2": "CS2",
        "counter-strike": "CS2",
        "counter-strike-2": "CS2",
        dota2: "Dota 2",
        "dota 2": "Dota 2",
        "dota-2": "Dota 2",
        lol: "LoL",
        "league of legends": "LoL",
        "league-of-legends": "LoL",
      };
      return map[String(game || "").toLowerCase()] || game || "";
    },
    gameFamilyIcon(game) {
      const map = {
        cs2: "/icons/esports/cs2.png",
        "counter-strike 2": "/icons/esports/cs2.png",
        "counter-strike": "/icons/esports/cs2.png",
        "counter-strike-2": "/icons/esports/cs2.png",
        dota2: "/icons/esports/dota2.png",
        "dota 2": "/icons/esports/dota2.png",
        "dota-2": "/icons/esports/dota2.png",
        lol: "/icons/esports/lol.png",
        "league of legends": "/icons/esports/lol.png",
        "league-of-legends": "/icons/esports/lol.png",
      };
      return map[String(game || "").toLowerCase()] || "";
    },
    gameIconForRow(row) {
      const parts = this.matchParts(row);
      return this.gameFamilyIcon(row?.best_game_family || row?.game || row?.league || parts?.game || "");
    },
    splitBucketKey(bucket) {
      const [game, marketType] = String(bucket || "").split(":");
      return { game: game || "", marketType: marketType || "" };
    },
    walletScopeItems(row) {
      if ((row?.category || "esports") === "sports") {
        const label = row?.game_label || row?.league_label || (row?.league ? String(row.league).toUpperCase() : "");
        return label ? [{ key: `sports-${label}`, icon: "", gameLabel: label, marketLabel: "Moneyline", title: `${label} Moneyline` }] : [];
      }
      const buckets = Array.isArray(row?.eligible_buckets) ? row.eligible_buckets : [];
      if (buckets.length) {
        return buckets.map((bucket) => {
          const { game, marketType } = this.splitBucketKey(bucket);
          const gameLabel = this.gameFamilyLabel(game);
          const marketLabel = this.marketTypeLabel(marketType);
          return {
            key: `bucket-${bucket}`,
            icon: this.gameFamilyIcon(game),
            gameLabel,
            marketLabel,
            title: [gameLabel, marketLabel].filter(Boolean).join(" - "),
          };
        }).filter((scope) => scope.marketLabel);
      }
      return this.marketTypeLabels(row).map((label) => ({
        key: `type-${label}`,
        icon: "",
        gameLabel: "",
        marketLabel: label,
        title: label,
      }));
    },
    walletScopeLabels(row) {
      return this.walletScopeItems(row).map((scope) => scope.title || scope.marketLabel).filter(Boolean);
    },
    sportsParticipationText(row) {
      const participated = Number(row?.participated_events);
      const eligible = Number(row?.eligible_event_count);
      const rate = row?.participation_rate;
      const countText = Number.isFinite(participated) && Number.isFinite(eligible) && eligible > 0 ? `${participated}/${eligible}` : "-";
      const rateText = rate === null || rate === undefined ? "-" : this.percent(rate);
      return `${countText} · ${rateText}`;
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
    walletRecordText(wallet) {
      const wins = Number(wallet?.esports_win_count);
      const losses = Number(wallet?.esports_loss_count);
      if (!Number.isFinite(wins) && !Number.isFinite(losses)) return "-";
      const safeWins = Number.isFinite(wins) ? wins : 0;
      const safeLosses = Number.isFinite(losses) ? losses : 0;
      return `${safeWins}W / ${safeLosses}L`;
    },
    walletWinRateText(wallet) {
      const wins = Number(wallet?.esports_win_count);
      const losses = Number(wallet?.esports_loss_count);
      if (!Number.isFinite(wins) && !Number.isFinite(losses)) return "";
      const safeWins = Number.isFinite(wins) ? wins : 0;
      const safeLosses = Number.isFinite(losses) ? losses : 0;
      const total = safeWins + safeLosses;
      return total > 0 ? this.percent(safeWins / total) : "";
    },
    walletObservedSettled(wallet) {
      const observed = wallet?.observed || {};
      const signals = Number(observed.signals || 0);
      return Number.isFinite(signals) ? signals : 0;
    },
    walletObservedClosed(wallet) {
      return this.walletObservedSettled(wallet) + this.walletObservedExited(wallet);
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
      const map = { settled: "已结算跟单", closed: "已结算跟单", open: "持仓中跟单", exited: "提前退出跟单" };
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
      if (status === "exited") return "已结算";
      if (status === "insufficient_balance") return "余额不足";
      if (status === "open" && this.signalFundingStatus(signal) === "insufficient_balance") return "余额不足";
      if (status === "open") return "持仓中";
      return this.statusText(status);
    },
    signalFundingStatus(signal) {
      const legs = signal?.legs || [];
      if (!legs.length) return "";
      const hasFunded = legs.some((leg) => this.legFundedStake(leg) > 0);
      const hasInsufficient = legs.some((leg) => String(leg?.funding_status || "") === "insufficient_balance");
      if (!hasFunded && hasInsufficient) return "insufficient_balance";
      return "";
    },
    legStatusText(leg, signal) {
      const funding = String(leg?.funding_status || "");
      if (funding === "insufficient_balance") return "余额不足";
      if (funding === "blocked") return "未跟";
      return this.signalStatusText(signal);
    },
    followSettlementTypeText(follow) {
      const type = String(follow?.settlement_type || "");
      if (type === "manual_exit") return "提前退出";
      if (type === "auto_settlement") return "自动结算";
      if (type === "auto_and_manual") return "自动+提前";
      if (String(follow?.status || "") === "open") return "-";
      return "-";
    },
    signalSettlementText(signal) {
      const status = String(signal.status || "");
      if (status === "open") return "未结算";
      if (status === "exited") return this.price(signal.exit_price);
      if (status === "settled") return signal.outcome_won ? "1.000（胜）" : "0.000（负）";
      return "-";
    },
    signalSettlementTypeText(signal) {
      const type = String(signal.settlement_type || "");
      if (type === "manual_exit") return "主动退出";
      if (type === "auto_settlement") return "自动结算";
      const status = String(signal.status || "");
      if (status === "exited") return "主动退出";
      if (status === "settled") return "自动结算";
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
    matchMeta(row) {
      const parts = this.matchParts(row);
      const meta = String(parts?.meta || "").trim();
      const label = String(row?.market_type_label || "").trim();
      if (!meta || (label && meta === label)) return "";
      return meta;
    },
    showMarketTypeChip(row) {
      const label = String(row?.market_type_label || "").trim();
      if (!label) return false;
      if (/^\d+\s*盘口$/.test(label)) return false;
      if (label.toLowerCase() === "moneyline") return false;
      const parts = this.matchParts(row);
      const meta = String(parts?.meta || "").trim();
      return !parts || meta !== label;
    },
    teamLogo(row, side) {
      const logos = row?.team_logos || {};
      return typeof logos[side] === "string" ? logos[side] : "";
    },
    teamInitials(name) {
      const words = String(name || "")
        .replace(/[^a-zA-Z0-9\s]/g, " ")
        .trim()
        .split(/\s+/)
        .filter(Boolean);
      if (!words.length) return "-";
      if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
      return `${words[0][0] || ""}${words[words.length - 1][0] || ""}`.toUpperCase();
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
        const stake = this.legFundedStake(leg);
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
      return this.walletLegs(wallet).filter((leg) => this.legFundedStake(leg) > 0);
    },
    legFundedStake(leg) {
      if (leg && Object.prototype.hasOwnProperty.call(leg, "funded_stake")) {
        const funded = Number(leg.funded_stake);
        return Number.isFinite(funded) ? Math.max(0, funded) : 0;
      }
      if (leg?.would_follow === false) return 0;
      const stake = Number(leg?.stake);
      return Number.isFinite(stake) ? Math.max(0, stake) : 0;
    },
    walletTotalStake(wallet) {
      const direct = Number(wallet?.follow_total_stake);
      if (Number.isFinite(direct)) return direct;
      return this.walletFollowedLegs(wallet).reduce((total, leg) => {
        const stake = this.legFundedStake(leg);
        return Number.isFinite(stake) ? total + stake : total;
      }, 0);
    },
    walletAverageEntry(wallet) {
      if (wallet?.follow_mixed_outcomes) return null;
      const direct = Number(wallet?.follow_avg_entry_price);
      if (Number.isFinite(direct)) return direct;
      let weighted = 0;
      let totalStake = 0;
      for (const leg of this.walletFollowedLegs(wallet)) {
        const stake = this.legFundedStake(leg);
        const entry = Number(leg.our_entry_price);
        if (!Number.isFinite(stake) || !Number.isFinite(entry) || stake <= 0) continue;
        weighted += stake * entry;
        totalStake += stake;
      }
      return totalStake > 0 ? weighted / totalStake : null;
    },
    walletAverageEntryText(wallet) {
      if (wallet?.follow_mixed_outcomes) return "多方向";
      return this.price(this.walletAverageEntry(wallet));
    },
    signalRealizedPnl(signal) {
      const direct = Number(signal?.follow_realized_pnl);
      if (Number.isFinite(direct)) return direct;
      const status = String(signal?.status || "");
      const realized = Number(signal?.our_realized_pnl);
      const paper = Number(signal?.our_paper_pnl);
      if (status === "exited") {
        if (Number.isFinite(realized)) return realized;
        return Number.isFinite(paper) ? paper : null;
      }
      if (status === "settled") {
        if (Number.isFinite(paper)) return paper;
        return Number.isFinite(realized) ? realized : null;
      }
      if (Number.isFinite(realized)) return realized;
      return Number.isFinite(paper) ? paper : null;
    },
    walletRealizedPnl(wallet) {
      const direct = Number(wallet?.follow_realized_pnl);
      if (Number.isFinite(direct)) return direct;
      return (wallet.signals || []).reduce((total, signal) => {
        const status = String(signal.status || "");
        if (status !== "settled" && status !== "exited") return total;
        const pnl = this.signalRealizedPnl(signal);
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
    legWalletEntryPrice(leg) {
      const value = Number(leg.wallet_fill_price ?? leg.wallet_avg_price);
      return Number.isFinite(value) ? value : null;
    },
    legSlippageValue(leg) {
      const walletEntry = this.legWalletEntryPrice(leg);
      const ourEntry = Number(leg.our_entry_price);
      if (Number.isFinite(walletEntry) && Number.isFinite(ourEntry)) return ourEntry - walletEntry;
      const value = leg.slippage_over_wallet_entry ?? leg.slippage;
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    },
    legSlippageText(leg) {
      const value = this.legSlippageValue(leg);
      return value == null ? "-" : this.signedPctPoints(value);
    },
    legFollowDelayText(leg) {
      const targetTs = this.normalizeTs(leg?.wallet_trade_at || leg?.created_at);
      const followTs = this.normalizeTs(leg?.observed_at || leg?.leg_at || leg?.created_at);
      if (!targetTs || !followTs) return "-";
      const delta = followTs - targetTs;
      const sign = delta >= 0 ? "+" : "-";
      const seconds = Math.abs(delta);
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      const secs = seconds % 60;
      if (hours > 0) return `${sign} ${hours}小时${minutes}分`;
      if (minutes > 0) return `${sign} ${minutes}分${secs}秒`;
      return `${sign} ${secs}秒`;
    },
  },
}).mount("#app");
