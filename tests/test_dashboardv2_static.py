"""Static contract tests for the dashboardV2 (React + design-system) frontend.

The V2 dashboard lives in ``poly_fight/dashboardV2`` and is a Babel-in-browser
React app composing the compiled design-system bundle. These tests pin the
wiring (entry point, vendored libs, page set, adapters) without a browser.
"""

import unittest
from pathlib import Path

V2 = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboardV2"


def _read(name: str) -> str:
    return (V2 / name).read_text(encoding="utf-8")


class DashboardV2StaticTests(unittest.TestCase):
    def test_required_files_exist(self):
        for rel in (
            "index.html", "app.jsx", "api.js", "adapt.js", "mock.js", "app.css",
            "ds/_ds_bundle.js", "ds/styles.css", "ds/kit.css",
            "ds/tokens/colors.css", "ds/assets/games/dota2.png",
            "vendor/react-18.3.1.production.min.js",
            "vendor/react-dom-18.3.1.production.min.js",
            "vendor/babel-standalone-7.29.0.min.js",
            "vendor/lucide-0.460.0.min.js",
        ):
            self.assertTrue((V2 / rel).is_file(), f"missing {rel}")

    def test_index_loads_react_stack_and_app(self):
        html = _read("index.html").lower()
        for needed in (
            "/vendor/react-18.3.1.production.min.js",
            "/vendor/react-dom-18.3.1.production.min.js",
            "/vendor/babel-standalone-7.29.0.min.js",
            "/vendor/lucide-0.460.0.min.js",
            "/ds/_ds_bundle.js",
            "/ds/styles.css",
            "/ds/kit.css",
            "/mock.js",
            "/api.js",
            "/adapt.js",
            'src="/app.jsx"',
            'type="text/babel"',
            'id="root"',
        ):
            self.assertIn(needed, html, f"index.html missing {needed}")

    def test_index_drops_legacy_vue_stack(self):
        html = _read("index.html").lower()
        for banned in ("vue-3", "daisyui", "tailwind-browser", "v-cloak"):
            self.assertNotIn(banned, html, f"index.html should not reference {banned}")

    def test_app_exposes_dashboard_pages(self):
        app = _read("app.jsx")
        for comp in (
            "function OverviewPage", "function StrategyPage", "function LeaderboardPage",
            "function EventsPage", "function FollowsPage", "function AiRiskPage",
        ):
            self.assertIn(comp, app, f"app.jsx missing {comp}")
        for label in ("概览", "跟单策略", "Leaderboard", "关注赛事", "跟单列表", "AI 风控"):
            self.assertIn(label, app, f"app.jsx missing sidebar label {label}")

    def test_app_has_auth_and_detail_modals(self):
        app = _read("app.jsx")
        for token in ("function LoginPanel", "function FollowDetailModal", "function WalletFollowsModal", "ReactDOM.createRoot"):
            self.assertIn(token, app, f"app.jsx missing {token}")
        self.assertNotIn("function AiRiskDetailCard", app)
        self.assertNotIn('<AiRiskDetailCard risk={detail.ai_risk}', app)

    def test_ai_risk_ui_uses_compact_operational_language(self):
        app = _read("app.jsx")
        css = _read("app.css")
        for token in ("AI 风控雷达", "AI 研判记录", "证据不足", "ai-record-table", "ai-decision-badge", "钱包风控", "自营影子"):
            self.assertIn(token, app)
        for noisy in ("PRE-MATCH INTELLIGENCE", "让历史实力成为主盘的第二道门", "独立赛前结论", "AI不确定"):
            self.assertNotIn(noisy, app)
        self.assertIn("DeepSeek 凭证", app)
        self.assertIn(".ai-control-card", css)
        self.assertIn(".ai-record-table", css)
        self.assertIn("const aiRiskPageCache = new Map()", app)
        initial_load = app.split("const load = React.useCallback", 1)[1].split("const ensureWrap", 1)[0]
        self.assertNotIn("Api.aiWrapKey", initial_load)

    def test_follow_ai_badge_only_shows_decision_action(self):
        app = _read("app.jsx")
        badge = app.split("function AiDecisionBadge", 1)[1].split("function MatchCell", 1)[0]
        for label in ("判定：一致", "判定：拦截", "判定：证据不足"):
            self.assertIn(label, badge)
        for removed_detail in ("winner", "probability", "AI判定 ·"):
            self.assertNotIn(removed_detail, badge)

    def test_ai_risk_layout_does_not_override_game_chip_or_leak_live_accent(self):
        app = _read("app.jsx")
        css = _read("app.css")
        self.assertIn('className="ai-record-copy"', app)
        self.assertIn(".ai-record-match > .ps-gamechip { display: inline-flex", css)
        self.assertNotIn(".ai-record-match b, .ai-record-match span", css)
        self.assertNotIn(".ai-provider-head b, .ai-provider-head span", css)
        self.assertIn(".ai-provider-head > div > span", css)
        self.assertIn(".ai-provider-head > .ps-badge { flex: none; }", css)
        live_rule = css.split(".ai-control-card.is-live", 1)[1].split("}", 1)[0]
        self.assertNotIn("inset", live_rule)
        self.assertIn(".ai-control-card.is-live { border-color: var(--border-hairline)", css)

    def test_app_consumes_design_system_bundle(self):
        app = _read("app.jsx")
        self.assertIn("window.PolySniperDesignSystem_8d05e5", app)
        for comp in ("SidebarNav", "Card", "StatTile", "WinRateRing", "CategoryDonut", "RankBadge", "StatusPill"):
            self.assertIn(comp, app, f"app.jsx should use DS component {comp}")

    def test_adapter_exposes_page_mappers(self):
        adapt = _read("adapt.js")
        for fn in ("overview", "wallets", "events", "follows", "strategyToKit", "strategyFromKit"):
            self.assertIn(fn, adapt, f"adapt.js missing mapper {fn}")
        self.assertIn("window.PSAdapt", adapt)

    def test_api_layer_covers_endpoints(self):
        api = _read("api.js")
        self.assertIn("window.PSApi", api)
        for ep in ("/api/login", "/api/overview", "/api/wallets", "/api/events",
                   "/api/follows", "/api/follow-strategy", "/api/runner/start",
                   "/api/runner/stop", "/api/wallet-favorites", "/api/wallet-quarantine",
                   "/api/ai-risk", "/api/ai-risk/wrap-key", "/api/ai-risk/credential",
                   "/api/ai-risk/settings", "/api/stream"):
            self.assertIn(ep, api, f"api.js missing endpoint {ep}")

    def test_mock_mode_available(self):
        self.assertIn("window.PSMock", _read("mock.js"))
        self.assertIn('"mock"', _read("api.js"))

    def test_strategy_amount_summary_truncates_with_full_hover_text(self):
        app = _read("app.jsx")
        css = _read("app.css")
        self.assertIn('title={nd.key ? nd.v : undefined}', app)
        self.assertIn('.srow-pipe .so-node.is-key .son-v', css)
        self.assertIn('text-overflow: ellipsis', css)
        self.assertIn('white-space: nowrap', css)


if __name__ == "__main__":
    unittest.main()
