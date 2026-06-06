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


def update_wallet_refresh_status(data_dir: Path, status: dict[str, Any]) -> dict[str, Any]:
    control = read_follow_control(data_dir)
    control["wallet_refresh"] = status
    write_follow_control(data_dir, control)
    return control
