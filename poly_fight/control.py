from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


CONTROL_FILENAME = "follow_control.json"

# 新单暂停的孤儿自愈参数。pause 由各刷新流程设置、其 finally 清除;若设置它的进程
# 中途死亡(dashboard 重启 / collect 被杀 / 机器休眠),finally 不会执行,pause 会
# 永久残留并静默挡住所有跟单。因此 pause 记录属主 pid(set_pause_new_signals 自动
# 盖),消费方(runner)每 tick 校验:属主进程已死即判为孤儿当场清除 —— 重启即自愈,
# 无需人工干预。
PAUSE_LEGACY_TTL_SECONDS = 30 * 60        # 无 owner_pid 的旧格式 pause:超 30 分钟自愈
PAUSE_HARD_TTL_SECONDS = 2 * 3600         # 绝对上限,防 pid 复用误判;任何刷新都不会跑这么久


def follow_control_path(data_dir: Path) -> Path:
    data_dir = Path(data_dir)
    if data_dir.name == "follow":
        return data_dir / CONTROL_FILENAME
    return data_dir / "follow" / CONTROL_FILENAME


def read_follow_control(data_dir: Path) -> dict[str, Any]:
    path = follow_control_path(data_dir)
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_follow_control(data_dir: Path, value: dict[str, Any]) -> None:
    path = follow_control_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def update_wallet_refresh_status(data_dir: Path, status: dict[str, Any]) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    control["wallet_refresh"] = status
    write_follow_control(data_dir, control)
    return control


def set_pause_new_signals(data_dir: Path, category: str, status: dict[str, Any] | None) -> dict[str, Any]:
    category = str(category or "").lower()
    control = read_follow_control(data_dir)
    pauses = control.get("pause_new_signals") if isinstance(control.get("pause_new_signals"), dict) else {}
    pauses = dict(pauses)
    if status:
        # 自动盖属主 pid:设置 pause 的进程就是负责清除它的进程。属主死亡 → 孤儿 → 被自愈。
        entry = {**status, "category": category}
        entry.setdefault("owner_pid", os.getpid())
        pauses[category] = entry
    else:
        pauses.pop(category, None)
    if pauses:
        control["pause_new_signals"] = pauses
    else:
        control.pop("pause_new_signals", None)
    write_follow_control(data_dir, control)
    return control


def _pid_alive(pid: int) -> bool:
    """属主进程是否存活。signal 0 不发信号、只做存在性探测。"""
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True          # 进程存在,只是属于别的用户
    except OSError:
        return False
    return True


def _pause_is_active(status: Any, now_ts: int) -> bool:
    """一条 pause 记录此刻是否仍然有效(非孤儿)。"""
    if not isinstance(status, dict) or status.get("status") != "paused":
        return False
    try:
        started = int(status.get("started_at") or 0)
    except (TypeError, ValueError):
        started = 0
    age = now_ts - started if started > 0 else 0
    owner_pid = status.get("owner_pid")
    if isinstance(owner_pid, int) and owner_pid > 0:
        if not _pid_alive(owner_pid):
            return False                                   # 属主已死 → 孤儿
        return not (started > 0 and age > PAUSE_HARD_TTL_SECONDS)   # 防 pid 复用的绝对上限
    # 旧格式 / 无属主:退化为 TTL 自愈
    return not (started > 0 and age > PAUSE_LEGACY_TTL_SECONDS)


def reconcile_pause_new_signals(data_dir: Path, *, now_ts: int | None = None) -> dict[str, Any]:
    """清除孤儿的新单暂停(属主进程已死,或旧格式超时),返回仍然有效的 pauses。
    仅在确有清除时才落盘。runner 每个 tick 调用一次 → 重启/进程死亡即自愈。"""
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    control = read_follow_control(data_dir)
    pauses = control.get("pause_new_signals")
    if not isinstance(pauses, dict) or not pauses:
        return {}
    survivors: dict[str, Any] = {}
    changed = False
    for category, status in pauses.items():
        if _pause_is_active(status, now_ts):
            survivors[category] = status
        else:
            changed = True
    if changed:
        if survivors:
            control["pause_new_signals"] = survivors
        else:
            control.pop("pause_new_signals", None)
        write_follow_control(data_dir, control)
    return survivors
