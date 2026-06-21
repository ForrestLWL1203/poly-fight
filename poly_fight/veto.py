"""CS2 map-winner 跟单旁路佐证(side-corroboration)。

被跟钱包买入某场 CS2 比赛的 **map-winner** 份额时,不无脑镜像;先用 bo3.gg 的
veto(选图/选边)数据判断这笔买入是不是在 *逆结构下注*(赌对手的优势图、或赌劣势边),
是则跳过/缩量,否则照跟。设计与勘察:review/cs2-veto-corroboration-plan.md。

数据源(无需登录,但 **必须带浏览器 Origin/Referer 头**,否则 `with` 被静默忽略、
`match_maps` 返回空)::

    GET https://api.bo3.gg/api/v1/matches/{slug}?with=match_maps
    Origin: https://bo3.gg   Referer: https://bo3.gg/

`match_maps[]`(按 ``order`` 1..7,标准 BO3 = ban,ban,pick,pick,ban,ban,decider)::

    order        veto 步骤序
    maps.slug    地图(dust2/mirage/inferno/nuke/overpass/ancient/anubis/...)
    team_id      执行该步的队(decider 为 null)
    choice_type  1=PICK  2=ban  3=decider

策略两轴(详见 plan §1):
  轴A 选图舒适度(主):被跟=选图方 +1 / 逆选图方 -1 / 决胜图 0。
  轴B 选边结构优势(次,只极端图):BO3 选边权固定(map1 对手选边、map2 对手选边、
      map3 拼刀);选边方拿优势边,整图 MR12 对消后只留被稀释残余 → 权重 ∝ 图警匪 gap。
  score = comfort + side;≤-1 跳过、(-1,+0.5) 缩量、≥+0.5 照跟。

纯 stdlib。只读公开数据,不下单、不碰私钥。
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# --------------------------------------------------------------------------- #
# 常量
# --------------------------------------------------------------------------- #
BO3_API = "https://api.bo3.gg/api/v1"
# bo3 的真实数据 API 只在带浏览器 Origin/Referer 时才接受 `with` 关联;否则静默丢弃。
BO3_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://bo3.gg",
    "Referer": "https://bo3.gg/",
}

# choice_type 解码(对 g2-vs-furia 等多场 BO3 实测一致)。
CHOICE_BAN = 2
CHOICE_PICK = 1
CHOICE_DECIDER = 3

# 图警匪偏向 = CT% − T%(签名值,+为大警图)。来源:用户提供的职业 CT/T 胜率图。
# 注:整张图 MR12 双方各打两边 → 图偏向对"整图 winner"基本对消;此表只用于 side 轴的
# 稀释残余加权,且只在极端图(|bias|≥SIDE_GAP_FLOOR)生效。Anubis 2026-01 被 rework 削过
# 失衡,数值会漂 → 视为可更新的种子,长期应迁到 SQLite 带 as_of。键用 bo3 map slug。
MAP_SIDE_BIAS: dict[str, float] = {
    "nuke": 16.2,
    "overpass": 15.2,
    "train": 12.0,
    "mirage": 8.4,
    "anubis": -8.2,
    "vertigo": 7.2,
    "inferno": 6.8,
    "cobblestone": 4.4,
    "ancient": 1.8,
    "dust2": 0.6,
    "cache": 0.2,
}

# side 轴权重:side_weight = SIDE_WEIGHT_PER_GAP × gap(gap 单位=胜率百分点)。
# 取 0.03 让 side 远小于 comfort(±1):Nuke gap16→0.49、Mirage 8→0.25、Ancient 2→0.06。
# 必须回测校准,别信这个常数。
SIDE_WEIGHT_PER_GAP = 0.03
SIDE_GAP_FLOOR = 4.0  # gap 小于此 → 视为平衡图,忽略 side(与"差距不大可忽略"一致)

# 决策门。这门是【风控过滤器】:只拦"逆结构"的高风险单。
#   comfort < 0(逆选图方)→ 一律 SKIP。选边优势【救不了】fade:实测两笔 fade 单
#     (Spirit/G2 的 Overpass、Spirit/Falcons 的 Anubis)都握着选边权、也都输了——
#     map-winner 里选图舒适度压倒性强过选边。故 side 轴对 fade 仅留作 sizing/记录,不改门。
#   否则 score ≥ 0 → 照跟(正佐证 comfort>0;决胜图/平衡 comfort=0 这类"无结构风险"的中性单);
#     0 > score → 缩量(目前两轴下非 fade 不会落此区,留作未来 sizing 分档)。
# 决胜图(无人选图)落在 score=0 → 照跟:它恰是本门要防的风险(逆对手堡垒图)*不存在*的情形,
# 反映更真实的实力对比,smart wallet 自己的判断就是最好信号 → 正常跟(base),非缩量更非"无脑"。
SCORE_FOLLOW_AT = 0.0     # comfort≥0 时:score ≥ 此值 → 照跟;否则缩量

DECISION_SKIP = "skip"
DECISION_REDUCE = "reduce"
DECISION_FOLLOW = "follow"
DECISION_NO_VETO = "no_veto"  # veto 暂不可得 → 上层决定 held/降级,不在本模块判

# ── 熔断器:bo3 持续不通时直接放弃佐证、跟单,不再逐笔等超时拖慢跟单 tick ──
# fail-open 已保证"不通→照常跟";熔断器进一步保证"不通时也不卡"。
_BREAKER_FAIL_THRESHOLD = 3        # 连续 N 次网络故障 → 打开熔断
_BREAKER_COOLDOWN_SECONDS = 300.0  # 打开后冷却 5min,期间 veto_gate 直接 no_veto、零网络
_breaker_state: dict[str, float] = {"fails": 0.0, "open_until": 0.0}


def reset_breaker() -> None:
    """重置熔断器(供单测/手动恢复)。"""
    _breaker_state["fails"] = 0.0
    _breaker_state["open_until"] = 0.0


def _breaker_is_open() -> bool:
    import time
    return time.monotonic() < _breaker_state["open_until"]


def _breaker_record_ok() -> None:
    _breaker_state["fails"] = 0.0


def _breaker_record_fail() -> None:
    import time
    _breaker_state["fails"] += 1
    if _breaker_state["fails"] >= _BREAKER_FAIL_THRESHOLD:
        _breaker_state["open_until"] = time.monotonic() + _BREAKER_COOLDOWN_SECONDS

# 队名 → bo3 slug 别名(bo3 用自己的 slug 形态,简单归一化覆盖不到)。可持续补。
TEAM_SLUG_ALIASES: dict[str, str] = {
    "team falcons": "falcons-esports",
    "falcons": "falcons-esports",
    "virtus pro": "virtus-pro",
    "virtuspro": "virtus-pro",
    "team spirit": "spirit",
    "natus vincere": "natus-vincere",
    "navi": "natus-vincere",
    "aurora gaming": "aurora-gaming",
    "aurora": "aurora-gaming",
    "gentle mates": "gentle-mates-cs",
    "cybershoke esports": "cybershoke",
    "cybershoke": "cybershoke",
    "fokus": "fokus-cs",
    "inox division": "inox-division",
    "1win": "1win",
}


# --------------------------------------------------------------------------- #
# 纯函数:解析市场 / veto / 评分(无网络,可单测)
# --------------------------------------------------------------------------- #
def parse_market_question(question: str) -> dict[str, Any] | None:
    """从 follow 信号的 market_question 抽出两队 + map 序号。

    例: "Counter-Strike: Spirit vs G2 - Map 1 Winner" ->
        {"team_a": "Spirit", "team_b": "G2", "map_number": 1}
    主赛盘(无 "Map N")返回 map_number=None;无法解析返回 None。
    """
    if not question:
        return None
    text = str(question)
    # 去掉游戏前缀 "Counter-Strike:" / "CS2:" 等。
    text = re.sub(r"^[^:]{0,20}:\s*", "", text, count=1)
    map_number: int | None = None
    m = re.search(r"\bmap\s*([1-5])\b", text, flags=re.IGNORECASE)
    if m:
        map_number = int(m.group(1))
    # 队名 = "A vs B" 段(在 " - " 之前)。
    head = re.split(r"\s+[-–]\s+", text, maxsplit=1)[0]
    vs = re.split(r"\s+vs\.?\s+", head, flags=re.IGNORECASE)
    if len(vs) != 2:
        return None
    team_a = vs[0].strip()
    team_b = re.sub(r"\s*\(BO\d\).*$", "", vs[1], flags=re.IGNORECASE).strip()
    if not team_a or not team_b:
        return None
    return {"team_a": team_a, "team_b": team_b, "map_number": map_number}


def normalize_team(name: str) -> str:
    """队名 → bo3 slug 片段(小写、去标点、空格转连字符);先查别名表。"""
    base = re.sub(r"[^a-z0-9]+", " ", str(name).lower()).strip()
    if base in TEAM_SLUG_ALIASES:
        return TEAM_SLUG_ALIASES[base]
    # 去常见后缀噪声,再查一次别名。
    stripped = re.sub(r"\b(esports|gaming|team|cs|cs2)\b", " ", base).strip()
    stripped = re.sub(r"\s+", " ", stripped)
    if stripped in TEAM_SLUG_ALIASES:
        return TEAM_SLUG_ALIASES[stripped]
    return base.replace(" ", "-")


def _date_ddmmyyyy(start_iso: str) -> str | None:
    """ISO 时间(UTC) -> bo3 slug 用的 DD-MM-YYYY。"""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(start_iso or ""))
    if not m:
        return None
    y, mo, d = m.groups()
    return f"{d}-{mo}-{y}"


def slug_candidates(team_a: str, team_b: str, start_iso: str) -> list[str]:
    """生成候选 bo3 slug(两种队序),供逐个试取。"""
    date = _date_ddmmyyyy(start_iso)
    if not date:
        return []
    a, b = normalize_team(team_a), normalize_team(team_b)
    out = []
    for x, y in ((a, b), (b, a)):
        if x and y:
            out.append(f"{x}-vs-{y}-{date}")
    # 去重保序
    seen: set[str] = set()
    return [s for s in out if not (s in seen or seen.add(s))]


def parse_veto(match: dict[str, Any]) -> dict[str, Any] | None:
    """把 bo3 match(含 match_maps)解析成结构化 veto。

    返回 None 表示该场无 veto 数据(match_maps 空 → bo3 未收录,上层按 no_veto 处理)。
    否则返回::

        {
          "team1_id": int, "team2_id": int,
          "team_name": {id: name},
          "steps": [{order, map, team_id, choice_type, kind}],  # kind: pick/ban/decider
          "played": [{"map": slug, "picker_id": id|None, "kind": "pick"|"decider"}],  # 按播放序(pick序)
        }
    """
    mm = match.get("match_maps")
    if not isinstance(mm, list) or not mm:
        return None
    t1, t2 = match.get("team1_id"), match.get("team2_id")
    names = {}
    for key, tid in (("team1", t1), ("team2", t2)):
        obj = match.get(key) or {}
        if tid is not None:
            names[tid] = obj.get("name")
    kind_of = {CHOICE_PICK: "pick", CHOICE_BAN: "ban", CHOICE_DECIDER: "decider"}
    steps = []
    for row in sorted(mm, key=lambda r: r.get("order", 0)):
        maps = row.get("maps") or {}
        steps.append({
            "order": row.get("order"),
            "map": maps.get("slug") or maps.get("name"),
            "team_id": row.get("team_id"),
            "choice_type": row.get("choice_type"),
            "kind": kind_of.get(row.get("choice_type"), str(row.get("choice_type"))),
        })
    # 播放序 ≈ pick 序:picks 按 order,最后接 decider。
    picks = [s for s in steps if s["kind"] == "pick"]
    decider = [s for s in steps if s["kind"] == "decider"]
    played = [{"map": s["map"], "picker_id": s["team_id"], "kind": "pick"} for s in picks]
    played += [{"map": s["map"], "picker_id": None, "kind": "decider"} for s in decider]
    return {
        "team1_id": t1, "team2_id": t2, "team_name": names,
        "steps": steps, "played": played,
    }


def _resolve_team_id(team_name: str, veto: dict[str, Any]) -> int | None:
    """把被跟队名/对手队名对到 bo3 team_id(归一化匹配)。"""
    want = normalize_team(team_name)
    for tid, nm in veto["team_name"].items():
        if nm and (normalize_team(nm) == want or want in normalize_team(nm) or normalize_team(nm) in want):
            return tid
    return None


def corroborate(
    backed_team: str,
    map_number: int | None,
    veto: dict[str, Any],
    *,
    bias_table: dict[str, float] | None = None,
) -> dict[str, Any]:
    """对"被跟 backed_team 赢 Map N"算佐证 score + 决策。

    map_number None(主赛盘)→ 不适用,返回 follow(本门只管 map_winner)。
    """
    bias_table = bias_table if bias_table is not None else MAP_SIDE_BIAS
    if map_number is None:
        return {"decision": DECISION_FOLLOW, "score": 0.0, "reason": "non-map market, gate N/A"}

    played = veto["played"]
    if not (1 <= map_number <= len(played)):
        return {"decision": DECISION_NO_VETO, "score": 0.0,
                "reason": f"map {map_number} 超出已知 {len(played)} 张播放图"}

    backed_id = _resolve_team_id(backed_team, veto)
    if backed_id is None:
        return {"decision": DECISION_NO_VETO, "score": 0.0,
                "reason": f"无法把 {backed_team!r} 对到 bo3 队"}
    opponent_id = veto["team2_id"] if backed_id == veto["team1_id"] else veto["team1_id"]

    this_map = played[map_number - 1]
    map_slug = this_map["map"]
    picker_id = this_map["picker_id"]  # None = 决胜图

    # ── 轴A 选图舒适度 ──
    if picker_id is None:
        comfort = 0.0
    elif picker_id == backed_id:
        comfort = 1.0
    else:
        comfort = -1.0

    # ── 轴B 选边结构优势(只极端图) ──
    bias = bias_table.get(str(map_slug or "").lower(), 0.0)
    gap = abs(bias)
    side = 0.0
    side_owner_id: int | None = None
    if picker_id is not None and gap >= SIDE_GAP_FLOOR:
        # BO3 规则:选图方的对手拿选边权 → 选边方 = 选图方的对手。
        side_owner_id = veto["team2_id"] if picker_id == veto["team1_id"] else veto["team1_id"]
        weight = SIDE_WEIGHT_PER_GAP * gap
        side = weight if side_owner_id == backed_id else -weight

    score = comfort + side
    if comfort < 0:
        # 逆选图方一律跳过(选边救不了 fade,见上方常量注释)。side 已算入 score 供 sizing/记录。
        decision = DECISION_SKIP
    elif score >= SCORE_FOLLOW_AT:
        decision = DECISION_FOLLOW
    else:
        decision = DECISION_REDUCE

    picker_name = veto["team_name"].get(picker_id) if picker_id else None
    return {
        "decision": decision,
        "score": round(score, 3),
        "comfort": comfort,
        "side": round(side, 3),
        "map": map_slug,
        "map_number": map_number,
        "picked_by": picker_name or ("decider" if picker_id is None else picker_id),
        "backed_is_picker": (picker_id == backed_id) if picker_id is not None else None,
        "map_bias_ct_minus_t": bias,
        "side_owner": veto["team_name"].get(side_owner_id) if side_owner_id else None,
        "reason": _explain(comfort, side, map_slug, picker_name, map_number),
    }


def _explain(comfort: float, side: float, map_slug: str, picker_name: str | None, n: int) -> str:
    if comfort > 0:
        base = f"顺选图方(Map{n} {map_slug} 是被跟队的图)"
    elif comfort < 0:
        base = f"逆选图方(Map{n} {map_slug} 是对手 {picker_name} 的图)"
    else:
        base = f"决胜图(Map{n} {map_slug},无人选,中性)"
    if side > 0:
        base += " +选边优势"
    elif side < 0:
        base += " −选边劣势"
    return base


# --------------------------------------------------------------------------- #
# 网络:取单场 veto(带正确头)+ slug 解析
# --------------------------------------------------------------------------- #
def _get(path: str, *, timeout: float = 20.0) -> Any:
    req = urllib.request.Request(BO3_API + path, headers=BO3_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "ignore"))


def fetch_match(slug: str, *, timeout: float = 20.0) -> dict[str, Any] | None:
    """取单场 match(含 match_maps);404/异常返回 None。"""
    try:
        return _get(f"/matches/{urllib.parse.quote(slug)}?with=match_maps", timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def find_match_veto(
    team_a: str, team_b: str, start_iso: str, *, timeout: float = 20.0,
) -> dict[str, Any] | None:
    """按队名+日期解析 bo3 match 并返回解析后的 veto(无 veto/找不到 → None)。

    先试候选 slug(两种队序);后续可扩展为按日期扫 matches 列表兜底(见 plan §5d)。
    返回完整解析 veto(带 slug)或 None。"找到 slug 但 veto 未出/未收录"也归为 None
    ——需要区分"没找到 vs 找到但空"的(如赛前时序轮询)请直接用 fetch_match + parse_veto。
    """
    slug, _match, veto = resolve(team_a, team_b, start_iso, timeout=timeout)
    if veto is not None:
        veto["slug"] = slug
    return veto


def veto_gate(
    market_question: str,
    backed_outcome: str,
    match_start_iso: str,
    *,
    game_family: str | None = None,
    market_type: str | None = None,
    cache: dict[str, Any] | None = None,
    timeout: float = 8.0,
    fetch: Any = None,
) -> dict[str, Any]:
    """一笔(可能的)CS2 map-winner 跟单的佐证门 —— 供 follow 跟单路径调用。

    返回 dict,关键字段:
      - ``applies``: False = 本门不适用(非 cs2 map_winner / 无法解析 map 序号)→ 调用方照常跟。
      - ``decision``: skip(fade,拦) / follow / reduce / no_veto(veto 暂不可得→fail-open 照常跟)。
    fail-safe:**任何网络/解析异常都 → no_veto(放行)**,绝不因 bo3 故障阻断跟单;
    且连续故障会触发熔断(见 _breaker_*),期间直接 no_veto、零网络,避免 outage 拖慢 tick。
    ``cache``: 调用方持有的 dict,只缓存"就绪 veto"(no_veto 不缓存→下个 tick 重查)。
    ``fetch``: 可注入 (team_a,team_b,start_iso)->veto|None,便于单测;默认 find_match_veto。
    """
    if str(market_type or "").lower() != "map_winner" or str(game_family or "").lower() != "cs2":
        return {"applies": False, "decision": "na", "reason": "not cs2 map_winner"}
    parsed = parse_market_question(market_question)
    if not parsed or parsed.get("map_number") is None:
        return {"applies": False, "decision": "na", "reason": "无法解析 map 序号"}
    team_a, team_b, n = parsed["team_a"], parsed["team_b"], parsed["map_number"]

    key = f"{normalize_team(team_a)}|{normalize_team(team_b)}|{_date_ddmmyyyy(match_start_iso)}"
    veto: dict[str, Any] | None = None
    if cache is not None and key in cache:
        veto = cache[key]
    elif _breaker_is_open():
        # 熔断中:bo3 连续不通,直接放弃佐证、照常跟,连网络都不打(避免逐笔等超时拖慢 tick)。
        return {"applies": True, "decision": DECISION_NO_VETO, "map_number": n,
                "veto_unavailable": True, "reason": "veto api 熔断中 → fail-open 照常跟"}
    else:
        try:
            if fetch is not None:
                veto = fetch(team_a, team_b, match_start_iso)
            else:
                veto = find_match_veto(team_a, team_b, match_start_iso, timeout=timeout)
            _breaker_record_ok()  # 成功(含干净 404→None)即视为 api 可达,清故障计数
        except Exception:
            # 网络/超时/HTTP 故障 → fail-open(照常跟)+ 计入熔断。
            _breaker_record_fail()
            return {"applies": True, "decision": DECISION_NO_VETO, "map_number": n,
                    "veto_unavailable": True, "reason": "veto api 不通 → fail-open 照常跟"}
        if cache is not None and veto is not None:
            cache[key] = veto

    if veto is None:
        # slug 没命中/veto 未发布(api 是通的,只是没数据)→ 照常跟,不计熔断。
        return {"applies": True, "decision": DECISION_NO_VETO, "map_number": n,
                "reason": "veto 暂不可得(fail-open,照常跟)"}
    result = corroborate(backed_outcome, n, veto)
    result["applies"] = True
    result["slug"] = veto.get("slug")
    return result


def resolve(
    team_a: str, team_b: str, start_iso: str, *, timeout: float = 20.0,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """底层解析,返回 (slug, match, veto)。供赛前时序轮询区分三态:
      - (None, None, None)          : 任何候选 slug 都没命中 → 比赛还没建/队名没对上
      - (slug, match, None)         : slug 命中但 match_maps 空 → veto 尚未发布/未收录
      - (slug, match, veto)         : veto 就绪
    """
    for slug in slug_candidates(team_a, team_b, start_iso):
        match = fetch_match(slug, timeout=timeout)
        if match is None:
            continue
        return slug, match, parse_veto(match)
    return None, None, None


# --------------------------------------------------------------------------- #
# CLI:单场佐证 + 赛前时序轮询(今晚决赛实测)
# --------------------------------------------------------------------------- #
def _now_utc() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _print_veto(veto: dict[str, Any]) -> None:
    nm = veto["team_name"]
    print(f"  slug={veto.get('slug')}  {nm.get(veto['team1_id'])} vs {nm.get(veto['team2_id'])}")
    for s in veto["steps"]:
        who = nm.get(s["team_id"], s["team_id"])
        mark = "  <-- PLAYED" if s["kind"] in ("pick", "decider") else ""
        print(f"    {s['order']}. {s['kind']:7s} {str(s['map']):9s} by {who}{mark}")


def main(argv: list[str] | None = None) -> int:
    import argparse
    import time

    ap = argparse.ArgumentParser(description="CS2 map-winner 旁路佐证 / 赛前 veto 时序轮询")
    ap.add_argument("--team-a", required=True)
    ap.add_argument("--team-b", required=True)
    ap.add_argument("--start", required=True, help="开赛 ISO 时间(UTC),如 2026-06-21T15:00:00Z")
    ap.add_argument("--backed", help="被跟队(给出则打印佐证判定)")
    ap.add_argument("--map", type=int, dest="map_number", help="map 序号(1/2/3)")
    ap.add_argument("--poll", action="store_true", help="轮询直到 veto 就绪(实测时序)")
    ap.add_argument("--interval", type=float, default=180.0, help="轮询间隔秒(默认 180)")
    ap.add_argument("--max-minutes", type=float, default=90.0, help="轮询最长分钟(默认 90)")
    args = ap.parse_args(argv)

    def check() -> dict[str, Any] | None:
        slug, _m, veto = resolve(args.team_a, args.team_b, args.start)
        if veto is None:
            state = "NOT_FOUND(无命中 slug)" if slug is None else f"MATCHED but VETO_EMPTY(slug={slug})"
            print(f"[{_now_utc()}] {state}")
            return None
        veto["slug"] = slug
        print(f"[{_now_utc()}] VETO_READY ✅")
        return veto

    if not args.poll:
        veto = check()
    else:
        deadline = time.monotonic() + args.max_minutes * 60
        veto = None
        while True:
            veto = check()
            if veto is not None or time.monotonic() >= deadline:
                break
            time.sleep(args.interval)

    if veto is not None:
        _print_veto(veto)
        if args.backed:
            r = corroborate(args.backed, args.map_number, veto)
            print(f"\n  佐证: backed={args.backed} map={args.map_number} "
                  f"→ score={r['score']} 决策={r['decision'].upper()}  ({r.get('reason')})")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
