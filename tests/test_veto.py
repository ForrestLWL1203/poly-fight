"""veto 旁路佐证:解析 + 评分单测。

fixture 用 2026-06-21 从 bo3 api.bo3.gg 实拉的真实 veto(Spirit/Falcons、KOLESIE/INOX、
g2-vs-furia),并对照 follow.db 的真实跟单结果断言决策方向。
"""
import unittest

from poly_fight import veto


def _match(team1, team2, steps):
    """从紧凑 veto 规格构造 bo3 match dict。

    steps: [(order, map_slug, team_idx 1/2/None, choice_type)]
    """
    t1id, t1name = team1
    t2id, t2name = team2
    idmap = {1: t1id, 2: t2id, None: None}
    mm = []
    for order, slug, who, ct in steps:
        mm.append({
            "order": order,
            "team_id": idmap[who],
            "choice_type": ct,
            "maps": {"slug": slug, "name": slug.capitalize()},
        })
    return {
        "team1_id": t1id, "team2_id": t2id,
        "team1": {"id": t1id, "name": t1name},
        "team2": {"id": t2id, "name": t2name},
        "match_maps": mm,
    }


# 真实 fixture --------------------------------------------------------------- #
P, B, D = veto.CHOICE_PICK, veto.CHOICE_BAN, veto.CHOICE_DECIDER

# Falcons(654) vs Spirit(... 用 793 占位) — Anubis=Falcons选 / Mirage=Spirit选 / Dust2决胜
SPIRIT_FALCONS = _match(
    (654, "Falcons"), (999, "Spirit"),
    [(1, "overpass", 1, B), (2, "inferno", 2, B), (3, "anubis", 1, P),
     (4, "mirage", 2, P), (5, "ancient", 2, B), (6, "nuke", 1, B), (7, "dust2", None, D)],
)

# KOLESIE(1) vs INOX(2) — Nuke=KOLESIE选 / Dust2=INOX选 / Overpass决胜
KOLESIE_INOX = _match(
    (1, "KOLESIE"), (2, "INOX Division"),
    [(1, "inferno", 1, B), (2, "mirage", 2, B), (3, "nuke", 1, P),
     (4, "dust2", 2, P), (5, "ancient", 1, B), (6, "anubis", 2, B), (7, "overpass", None, D)],
)

# 合成:对手在平衡图选 Map1,被跟买对家(逆选图方)→ 干净 skip
G2_PICKS_BALANCED = _match(
    (10, "G2"), (20, "Spirit"),
    [(1, "nuke", 1, B), (2, "overpass", 2, B), (3, "ancient", 1, P),
     (4, "vertigo", 2, P), (5, "train", 1, B), (6, "inferno", 2, B), (7, "dust2", None, D)],
)

# 真实(HLTV)Spirit(654) vs G2(793) IEM Cologne —— 那笔 −$500 的原场比赛:
# G2 ban Nuke / Spirit ban Inferno / G2 PICK Overpass / Spirit PICK Dust2 /
# Spirit ban Anubis / G2 ban Ancient / Mirage DECIDER。结果 Map1 G2赢、Map2/3 Spirit赢。
SPIRIT_G2 = _match(
    (654, "Spirit"), (793, "G2"),
    [(1, "nuke", 2, B), (2, "inferno", 1, B), (3, "overpass", 2, P),
     (4, "dust2", 1, P), (5, "anubis", 1, B), (6, "ancient", 2, B), (7, "mirage", None, D)],
)


class TestParseMarketQuestion(unittest.TestCase):
    def test_map_winner(self):
        r = veto.parse_market_question("Counter-Strike: Spirit vs G2 - Map 1 Winner")
        self.assertEqual(r, {"team_a": "Spirit", "team_b": "G2", "map_number": 1})

    def test_map2(self):
        r = veto.parse_market_question("Counter-Strike: Spirit vs Team Falcons - Map 2 Winner")
        self.assertEqual(r["map_number"], 2)
        self.assertEqual(r["team_b"], "Team Falcons")

    def test_main_match_no_map(self):
        r = veto.parse_market_question("Counter-Strike: WW Team vs CYBERSHOKE Esports (BO3) - Stake Ranked")
        self.assertIsNone(r["map_number"])
        self.assertEqual(r["team_a"], "WW Team")
        self.assertEqual(r["team_b"], "CYBERSHOKE Esports")

    def test_garbage(self):
        self.assertIsNone(veto.parse_market_question("Will s1mple retire?"))


class TestSlugCandidates(unittest.TestCase):
    def test_alias_and_order(self):
        cands = veto.slug_candidates("FURIA", "Team Falcons", "2026-06-21T15:00:00Z")
        self.assertIn("furia-vs-falcons-esports-21-06-2026", cands)
        # 反序也在(队序未知时兜底)
        self.assertIn("falcons-esports-vs-furia-21-06-2026", cands)

    def test_virtus_pro_alias(self):
        cands = veto.slug_candidates("Virtus.pro", "1WIN", "2026-06-20T10:00:00Z")
        self.assertIn("virtus-pro-vs-1win-20-06-2026", cands)


class TestParseVeto(unittest.TestCase):
    def test_played_order_and_pickers(self):
        v = veto.parse_veto(SPIRIT_FALCONS)
        self.assertEqual([p["map"] for p in v["played"]], ["anubis", "mirage", "dust2"])
        self.assertEqual(v["played"][0]["picker_id"], 654)   # Anubis = Falcons
        self.assertEqual(v["played"][1]["picker_id"], 999)   # Mirage = Spirit
        self.assertIsNone(v["played"][2]["picker_id"])       # Dust2 decider

    def test_empty_match_maps_returns_none(self):
        self.assertIsNone(veto.parse_veto({"team1_id": 1, "team2_id": 2, "match_maps": []}))


class TestCorroborate(unittest.TestCase):
    def setUp(self):
        self.sf = veto.parse_veto(SPIRIT_FALCONS)
        self.ki = veto.parse_veto(KOLESIE_INOX)
        self.g2 = veto.parse_veto(G2_PICKS_BALANCED)

    def test_spirit_falcons_map1_back_falcons_follow(self):
        # Map1 Anubis = Falcons 的图;买 Falcons = 顺选图方 → comfort +1 → follow。真实:Falcons 赢
        r = veto.corroborate("Falcons", 1, self.sf)
        self.assertEqual(r["comfort"], 1.0)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_spirit_falcons_map1_back_spirit_skip(self):
        # 买 Spirit = 逆选图方 → comfort -1;Anubis 极端图 Spirit 握选边(side>0)但 fade 一律 SKIP。真实:Spirit 输
        r = veto.corroborate("Spirit", 1, self.sf)
        self.assertEqual(r["comfort"], -1.0)
        self.assertGreater(r["side"], 0.0)  # Spirit 是选边方,但救不了 fade
        self.assertEqual(r["decision"], veto.DECISION_SKIP)

    def test_spirit_g2_map1_the_minus_500_skip(self):
        # 真实 −$500 案例:Map1 Overpass 是 G2 选的图,我们买 Spirit(逆)。
        # Overpass 极端大警图、Spirit 握选边(side>0),但 fade 一律 SKIP → 正确拦住这笔。
        v = veto.parse_veto(SPIRIT_G2)
        r = veto.corroborate("Spirit", 1, v)
        self.assertEqual(r["comfort"], -1.0)
        self.assertGreater(r["side"], 0.0)
        self.assertEqual(r["decision"], veto.DECISION_SKIP)
        # Map2 Dust2 是 Spirit 自己的图 → follow;Map3 Mirage 决胜 → follow(均真实赢)
        self.assertEqual(veto.corroborate("Spirit", 2, v)["decision"], veto.DECISION_FOLLOW)
        self.assertEqual(veto.corroborate("Spirit", 3, v)["decision"], veto.DECISION_FOLLOW)

    def test_spirit_falcons_map2_back_spirit_follow(self):
        # Map2 Mirage = Spirit 的图;买 Spirit = 顺选图方 → follow。真实:Spirit 赢
        r = veto.corroborate("Spirit", 2, self.sf)
        self.assertEqual(r["comfort"], 1.0)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_kolesie_inox_map2_back_inox_follow_but_lost(self):
        # Map2 Dust2 = INOX 自己的图(平衡图,side=0)→ comfort +1 → follow。
        # 真实:INOX 输(KOLESIE 2-0 横扫)——记录为"风控过滤器非胜负保证"的已知局限。
        r = veto.corroborate("INOX Division", 2, self.ki)
        self.assertEqual(r["comfort"], 1.0)
        self.assertEqual(r["side"], 0.0)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_clean_skip_balanced_opponent_pick(self):
        # 对手 G2 在平衡图(ancient)选 Map1,买 Spirit(逆) → comfort -1 + side 0 → 干净 skip
        r = veto.corroborate("Spirit", 1, self.g2)
        self.assertEqual(r["comfort"], -1.0)
        self.assertEqual(r["side"], 0.0)
        self.assertEqual(r["decision"], veto.DECISION_SKIP)

    def test_decider_neutral_follows(self):
        # Map3 决胜图(无人选)→ comfort 0 → score 0 → FOLLOW(无负佐证 → 正常跟,非缩量/无脑)
        r = veto.corroborate("Spirit", 3, self.sf)
        self.assertEqual(r["comfort"], 0.0)
        self.assertEqual(r["side"], 0.0)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_non_map_market_na(self):
        r = veto.corroborate("Spirit", None, self.sf)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_unknown_team_no_veto(self):
        r = veto.corroborate("Some Random Org", 1, self.sf)
        self.assertEqual(r["decision"], veto.DECISION_NO_VETO)


class TestVetoGate(unittest.TestCase):
    """veto_gate:供 follow 调用的门(注入 fetch,不打网络)。"""

    def setUp(self):
        veto.reset_breaker()  # 隔离:熔断器是模块级状态,每个用例前清零

    def _fetch_g2(self, *_a, **_k):
        v = veto.parse_veto(SPIRIT_G2)
        v["slug"] = "spirit-vs-g2-19-06-2026"
        return v

    Q1 = "Counter-Strike: Spirit vs G2 - Map 1 Winner"
    Q2 = "Counter-Strike: Spirit vs G2 - Map 2 Winner"

    def test_not_cs2_map_market_na(self):
        r = veto.veto_gate("Counter-Strike: Spirit vs G2 (BO3)", "Spirit", "2026-06-19T13:45:00Z",
                           game_family="cs2", market_type="main_match", fetch=self._fetch_g2)
        self.assertFalse(r["applies"])

    def test_fade_skips(self):
        # 买 Spirit 赢 Map1(G2 的 Overpass) → fade → SKIP(那笔 −$500 会被拦)
        r = veto.veto_gate(self.Q1, "Spirit", "2026-06-19T13:45:00Z",
                           game_family="cs2", market_type="map_winner", fetch=self._fetch_g2)
        self.assertTrue(r["applies"])
        self.assertEqual(r["decision"], veto.DECISION_SKIP)

    def test_back_picker_follows(self):
        # 买 Spirit 赢 Map2(Spirit 自己的 Dust2) → FOLLOW
        r = veto.veto_gate(self.Q2, "Spirit", "2026-06-19T13:45:00Z",
                           game_family="cs2", market_type="map_winner", fetch=self._fetch_g2)
        self.assertEqual(r["decision"], veto.DECISION_FOLLOW)

    def test_no_veto_fail_open(self):
        r = veto.veto_gate(self.Q1, "Spirit", "2026-06-19T13:45:00Z",
                           game_family="cs2", market_type="map_winner",
                           fetch=lambda *a, **k: None)
        self.assertEqual(r["decision"], veto.DECISION_NO_VETO)

    def test_fetch_exception_fail_open(self):
        def boom(*_a, **_k):
            raise RuntimeError("bo3 down")
        r = veto.veto_gate(self.Q1, "Spirit", "2026-06-19T13:45:00Z",
                           game_family="cs2", market_type="map_winner", fetch=boom)
        self.assertEqual(r["decision"], veto.DECISION_NO_VETO)  # 故障绝不阻断跟单

    def test_cache_avoids_refetch(self):
        calls = {"n": 0}
        def counting(*_a, **_k):
            calls["n"] += 1
            return self._fetch_g2()
        cache: dict = {}
        veto.veto_gate(self.Q1, "Spirit", "2026-06-19T13:45:00Z", game_family="cs2",
                       market_type="map_winner", fetch=counting, cache=cache)
        veto.veto_gate(self.Q2, "Spirit", "2026-06-19T13:45:00Z", game_family="cs2",
                       market_type="map_winner", fetch=counting, cache=cache)
        self.assertEqual(calls["n"], 1)  # 同场 Map1/Map2 只打一次

    def test_breaker_opens_after_consecutive_failures(self):
        # bo3 连续故障 → 熔断打开后直接 no_veto、不再调 fetch(避免逐笔等超时)
        calls = {"n": 0}
        def boom(*_a, **_k):
            calls["n"] += 1
            raise RuntimeError("bo3 down")
        for i in range(veto._BREAKER_FAIL_THRESHOLD):
            r = veto.veto_gate(f"Counter-Strike: A{i} vs B{i} - Map 1 Winner", "A%d" % i,
                               "2026-06-19T13:45:00Z", game_family="cs2",
                               market_type="map_winner", fetch=boom)
            self.assertEqual(r["decision"], veto.DECISION_NO_VETO)
        calls_at_open = calls["n"]
        # 熔断已开:再来一笔应直接放行、不再调 fetch
        r = veto.veto_gate("Counter-Strike: X vs Y - Map 1 Winner", "X",
                           "2026-06-19T13:45:00Z", game_family="cs2",
                           market_type="map_winner", fetch=boom)
        self.assertEqual(r["decision"], veto.DECISION_NO_VETO)
        self.assertTrue(r.get("veto_unavailable"))
        self.assertEqual(calls["n"], calls_at_open)  # fetch 未被再调用

    def test_breaker_resets_on_success(self):
        # 故障未达阈值时一次成功即清零,不会误熔断
        veto.veto_gate("Counter-Strike: A vs B - Map 1 Winner", "A", "2026-06-19T13:45:00Z",
                       game_family="cs2", market_type="map_winner",
                       fetch=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
        self.assertEqual(veto._breaker_state["fails"], 1)
        veto.veto_gate(self.Q1, "Spirit", "2026-06-19T13:45:00Z", game_family="cs2",
                       market_type="map_winner", fetch=self._fetch_g2)
        self.assertEqual(veto._breaker_state["fails"], 0)


class TestVetoFollowIntegration(unittest.TestCase):
    """端到端:CS2 map-winner 的 BUY 经 process_follow_trades,fade→不开仓、follow→开仓。"""

    def _run(self, gate_decision):
        import datetime
        from unittest import mock
        from poly_fight import follow

        start_ts = int(datetime.datetime(2026, 6, 19, 13, 45, tzinfo=datetime.timezone.utc).timestamp())
        now_ts = start_ts - 1800  # 赛前 30min(pre-match,过 require_pre_match 门)
        cid = "0xcs2map1"
        market = {
            "conditionId": cid, "category": "esports", "game_family": "cs2",
            "market_type": "map_winner",
            "question": "Counter-Strike: Spirit vs G2 - Map 1 Winner",
            "outcomes": ["Spirit", "G2"], "outcome_prices": ["0.5", "0.5"],
            "match_start_time": "2026-06-19T13:45:00Z", "league": "cs2",
        }
        trade = {"conditionId": cid, "outcomeIndex": 0, "side": "BUY",
                 "price": 0.5, "size": 100, "timestamp": now_ts - 60, "id": "t1"}
        fake_gate = lambda *a, **k: {"applies": True, "decision": gate_decision}
        with mock.patch.object(follow, "veto_gate", fake_gate):
            signals, stats = follow.process_follow_trades(
                [], wallet="0xWALLET", trades=[trade],
                markets_by_condition={cid: market}, now_ts=now_ts,
                stake_usdc=1.0, max_follow_legs=3, max_slippage=1.0,
            )
        return signals, stats

    def test_fade_buy_opens_no_signal(self):
        signals, stats = self._run("skip")
        self.assertEqual(len(signals), 0)                      # fade → 零曝光
        self.assertEqual(stats["veto_fade_skip_count"], 1)

    def test_follow_buy_opens_signal(self):
        signals, stats = self._run("follow")
        self.assertEqual(len(signals), 1)                      # 顺选图 → 正常开仓
        self.assertEqual(stats["veto_fade_skip_count"], 0)
        self.assertEqual(signals[0]["veto_corroboration"]["decision"], "follow")

    def test_no_veto_buy_still_follows(self):
        signals, stats = self._run("no_veto")
        self.assertEqual(len(signals), 1)                      # veto 不可得 → fail-open 照常跟
        self.assertEqual(stats["veto_no_data_count"], 1)


if __name__ == "__main__":
    unittest.main()
