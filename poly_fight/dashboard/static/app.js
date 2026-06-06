const { createApp } = Vue;

createApp({
  data() {
    return {
      authenticated: false,
      activeTab: "follows",
      tabs: [
        { id: "follows", label: "跟单" },
        { id: "events", label: "赛事" },
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
      followSize: 25,
      followDetail: { wallets: [] },
      detailModal: { open: false, loading: false, conditionId: "" },
      detailLegPages: {},
      detailPriceRefreshing: false,
      detailPriceCooldown: 0,
      detailPriceCooldownTimer: null,
      toasts: [],
      intervals: [],
      eventSource: null,
      pollingFallback: false,
      liveStatus: "starting",
      streamRetryMs: 2000,
      streamRetryTimer: null,
      streamProbeRunning: false,
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
    runnerControlLabel() {
      return this.runner.status === "running" ? "■" : "▶";
    },
    runnerControlTitle() {
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
      const count = Number(this.wallets.count || (this.wallets.wallets || []).length || 0);
      return Math.max(1, Math.ceil(count / this.walletSize));
    },
    walletPageRows() {
      const rows = this.wallets.wallets || [];
      const page = Math.min(Math.max(1, this.walletPage), this.walletPageCount);
      const start = (page - 1) * this.walletSize;
      return rows.slice(start, start + this.walletSize);
    },
    walletRefreshBusy() {
      return this.loading.walletRefresh || (this.refreshStatus && this.refreshStatus.status === "running");
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
    this.bootstrap();
  },
  beforeUnmount() {
    this.stopRealtime();
    this.clearDetailPriceCooldown();
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
      this.follows = await this.request(`/api/follows?page=${this.followPage}&size=${this.followSize}`);
    },
    async loadEvents() {
      const result = await this.request("/api/events");
      result.events = (result.events || []).slice().sort((a, b) => this.normalizeTs(a.match_start_time) - this.normalizeTs(b.match_start_time));
      this.events = result;
    },
    async loadWalletRefreshStatus() {
      const result = await this.request("/api/wallet-refresh");
      this.refreshStatus = result.status || { status: "idle" };
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
    async startWalletRefresh() {
      this.loading.walletRefresh = true;
      try {
        const result = await this.request("/api/wallet-refresh", { method: "POST" });
        this.refreshStatus = result;
        this.showToast("候选钱包采样已启动");
      } catch (error) {
        if (error.status === 409) {
          this.refreshStatus = error.payload && error.payload.data ? error.payload.data : this.refreshStatus;
          this.showToast("已有采样任务在运行", "error");
        } else {
          this.showToast(`采样启动失败: ${error.message}`, "error");
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
    setWalletPage(page) {
      this.walletPage = Math.min(Math.max(1, page), this.walletPageCount);
    },
    async toggleRunner() {
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
        open: "进行中",
        settled: "已结算",
        exited: "已退出",
        mixed: "混合",
        idle: "空闲",
        succeeded: "已完成",
        failed: "失败",
      };
      return map[status] || status || "-";
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
    matchParts(title) {
      const text = String(title || "");
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
    detailTitle() {
      return this.followDetail.title || this.followDetail.question || "跟单详情";
    },
    detailEventUrl() {
      return this.followDetail.event_url || "";
    },
    detailMarketPrices() {
      const outcomes = this.asArray(this.followDetail.outcomes);
      const prices = this.asArray(this.followDetail.outcome_prices);
      return outcomes
        .map((outcome, index) => ({ outcome: String(outcome || `方向 ${index + 1}`), price: Number(prices[index]) }))
        .filter((row) => Number.isFinite(row.price));
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
