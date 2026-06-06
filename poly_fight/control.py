from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


CONTROL_FILENAME = "follow_control.json"


def follow_control_path(data_dir: Path) -> Path:
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


def set_follow_pause(
    data_dir: Path,
    *,
    reason: str,
    now_ts: int | None = None,
    ttl_seconds: int = 7200,
    detail: str | None = None,
) -> dict[str, Any]:
    now_ts = now_ts or int(time.time())
    control = read_follow_control(data_dir)
    pause = {
        "paused": True,
        "reason": reason,
        "started_at": now_ts,
        "expires_at": now_ts + ttl_seconds,
    }
    if detail:
        pause["detail"] = detail
    control["pause_follow"] = pause
    write_follow_control(data_dir, control)
    return pause


def clear_follow_pause(data_dir: Path, *, reason: str | None = None) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    pause = control.get("pause_follow")
    if isinstance(pause, dict) and (reason is None or pause.get("reason") == reason):
        control.pop("pause_follow", None)
        write_follow_control(data_dir, control)
    return control


def active_follow_pause(data_dir: Path, *, now_ts: int | None = None) -> dict[str, Any] | None:
    now_ts = now_ts or int(time.time())
    control = read_follow_control(data_dir)
    pause = control.get("pause_follow")
    if not isinstance(pause, dict) or not pause.get("paused"):
        return None
    expires_at = int(pause.get("expires_at") or 0)
    if expires_at and now_ts >= expires_at:
        clear_follow_pause(data_dir)
        return None
    return pause


def update_wallet_refresh_status(data_dir: Path, status: dict[str, Any]) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    control["wallet_refresh"] = status
    write_follow_control(data_dir, control)
    return control
