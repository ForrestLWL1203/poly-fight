#!/usr/bin/env python3
"""poly-fight launcher — a tiny local control panel for starting the dashboard
locally or deploying it to the VPS.

Why a local python (not a pure static HTML): browsers cannot open SSH / raw TCP,
so a static page can't drive a VPS. This script serves the HTML UI on localhost
AND does the real work (local subprocess / `ssh`+`scp` to the VPS). Stdlib only,
to match the project. Secrets live only in the gitignored secret/launcher.json;
they reach the VPS over the encrypted SSH channel via stdin (never argv, never the
process table).

Run:  python3 launcher/launcher.py            # opens the UI in your browser
      python3 launcher/launcher.py --init      # build secret/launcher.json from
                                               # existing secret/* files
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "secret" / "launcher.json"
LAUNCHER_PORT = 8799


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    return json.loads((HERE / "config.example.json").read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(CONFIG_PATH, 0o600)


def init_config() -> None:
    """Consolidate existing secret/* files into one secret/launcher.json."""
    cfg = load_config()
    rpc_lines = _read_text(ROOT / "secret" / "rpc").splitlines()
    for line in rpc_lines:
        line = line.strip()
        if line.startswith("https://"):
            cfg.setdefault("rpc", {})["https"] = line
        elif line.startswith("wss://"):
            cfg.setdefault("rpc", {})["wss"] = line
    pw = _read_text(ROOT / "secret" / "dashboard-password")
    if pw:
        cfg.setdefault("dashboard", {})["password"] = pw
    cfg.pop("_comment", None)
    save_config(cfg)
    print(f"wrote {CONFIG_PATH} (chmod 600) — fill in remote.vps_host etc.")


def masked(cfg: dict) -> dict:
    """Config for the UI: secret values replaced with a 'set/unset' marker."""
    out = json.loads(json.dumps(cfg))
    out.pop("_comment", None)
    def mark(d, k):
        if isinstance(d, dict):
            d[k] = "••••" if str(d.get(k) or "") else ""
    mark(out.get("dashboard", {}), "password")
    mark(out.get("rpc", {}), "https")
    mark(out.get("rpc", {}), "wss")
    mark(out.get("polymarket", {}), "private_key")
    mark(out.get("remote", {}), "vps_password")
    return out


def merge_ui(cfg: dict, ui: dict) -> dict:
    """Apply UI edits; '••••' means 'keep existing secret'."""
    for section, fields in ui.items():
        if not isinstance(fields, dict):
            continue
        cur = cfg.setdefault(section, {})
        for k, v in fields.items():
            if v == "••••":
                continue
            cur[k] = v
    return cfg


# --------------------------------------------------------------------------- #
# remote deploy script (runs on the VPS via `ssh host bash -s` < this)
# --------------------------------------------------------------------------- #
REMOTE_SCRIPT = r"""
set -euo pipefail
@@SECRETS@@
REPO=@@REPO@@
DATADIR=@@DATADIR@@
GITHUB=@@GITHUB@@
PY=@@PYTHON@@
PORT=@@PORT@@
DOMAIN=@@DOMAIN@@
CADDYFILE=@@CADDYFILE@@

echo "[1/6] 确保依赖 (git / python3 / caddy) — 裸机首跑会装,已装则跳过"
export DEBIAN_FRONTEND=noninteractive
if ! command -v git >/dev/null 2>&1; then
  apt-get update -qq && apt-get install -y -qq git
  echo "    installed git"
fi
if ! command -v python3 >/dev/null 2>&1; then
  apt-get update -qq && apt-get install -y -qq python3
  echo "    installed python3"
fi
if ! command -v caddy >/dev/null 2>&1; then
  apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl gnupg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
    | gpg --batch --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
    > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq && apt-get install -y -qq caddy
  echo "    installed caddy"
fi

echo "[2/6] ensure repo at $REPO"
if [ ! -d "$REPO/.git" ]; then
  git clone "$GITHUB" "$REPO"
else
  cd "$REPO"
  DIRTY="$(git status --porcelain)"
  if [ -n "$DIRTY" ]; then
    echo "ABORT: VPS worktree has local changes — refusing to overwrite (policy):"
    echo "$DIRTY"
    exit 3
  fi
  git fetch --quiet origin
  git checkout --quiet main
  git reset --hard --quiet origin/main
fi
cd "$REPO"
echo "    at $(git rev-parse --short HEAD)"

echo "[3/6] write secrets (chmod 600)"
mkdir -p secret
# secret vars (RPC_HTTPS/RPC_WSS/DASH_PW/DASH_SECRET) are injected at the top of
# this script by the launcher — only ever travels over SSH stdin, never argv.
printf '%s\n%s\n' "${RPC_HTTPS:-}" "${RPC_WSS:-}" > secret/rpc
chmod 600 secret/rpc
printf 'POLY_FIGHT_DASH_PASSWORD=%s\nPOLY_FIGHT_DASH_COOKIE_SECRET=%s\n' "${DASH_PW:-}" "${DASH_SECRET:-}" > secret/dashboard.env
chmod 600 secret/dashboard.env

echo "[4/6] systemd unit (dashboard 托管模式 — runner 由面板「启动跟单」spawn)"
mkdir -p "$DATADIR"
# 旧版把 runner 当独立常驻服务;现模型是 dashboard spawn runner，停用旧 runner 服务避免冲突。
if systemctl list-unit-files 2>/dev/null | grep -q '^poly-fight-runner.service'; then
  systemctl disable --now poly-fight-runner.service 2>/dev/null || true
  echo "    disabled stale poly-fight-runner.service"
fi
cat > /etc/systemd/system/poly-fight-dashboard.service <<UNIT
[Unit]
Description=poly-fight dashboard
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
WorkingDirectory=$REPO
EnvironmentFile=$REPO/secret/dashboard.env
ExecStart=$PY -m poly_fight.cli --data-dir $DATADIR serve --host 127.0.0.1 --port $PORT
Restart=always
RestartSec=4
# 只管 dashboard 进程本身 — 重启/停 dashboard 不连带杀掉它 spawn 的 runner+observe，
# 这样「停 dashboard」和「停 runner」可以分开控制。
KillMode=process
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable poly-fight-dashboard

echo "[5/6] caddy reverse_proxy for $DOMAIN"
# 防火墙:ufw 默认 deny incoming,只放 22 会挡死 80/443 → Let's Encrypt ACME 验证连不进来,
# 证书签不下来,HTTPS 不可用。所以在装反代前先放行 80/443(先确保 22,绝不把自己锁在外面)。
# 只在 ufw 已启用时动它;未启用就不碰(裸机 ufw 默认 inactive,端口本就开放)。
if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
  ufw allow 22/tcp  >/dev/null 2>&1 || true
  ufw allow 80/tcp  >/dev/null 2>&1 || true
  ufw allow 443/tcp >/dev/null 2>&1 || true
  ufw reload >/dev/null 2>&1 || true
  echo "    ufw active — 已放行 22/80/443"
else
  echo "    ufw 未启用/未安装 — 跳过(端口默认开放)"
fi
if [ -f "$CADDYFILE" ] && ! grep -q "$DOMAIN" "$CADDYFILE"; then
  printf '\n%s {\n    reverse_proxy 127.0.0.1:%s\n}\n' "$DOMAIN" "$PORT" >> "$CADDYFILE"
  systemctl reload caddy || caddy reload --config "$CADDYFILE" || true
  echo "    added caddy block + reloaded"
else
  echo "    caddy block already present (or no Caddyfile) — skipped"
fi

echo "[6/6] 环境就绪"
echo "PREPARED — 代码/密钥/服务/反代已就绪，点「启动」拉起 dashboard"
"""


def build_remote_payload(cfg: dict) -> str:
    r = cfg.get("remote", {})
    script = REMOTE_SCRIPT
    for token, value in {
        "@@REPO@@": shlex.quote(r.get("repo_dir", "/opt/poly-fight/repo")),
        "@@DATADIR@@": shlex.quote(r.get("data_dir", "/opt/poly-fight/data")),
        "@@GITHUB@@": shlex.quote(cfg.get("github_url", "")),
        "@@PYTHON@@": shlex.quote(r.get("python", "python3")),
        "@@PORT@@": str(int(r.get("port", 8787))),
        "@@DOMAIN@@": shlex.quote(r.get("domain", "")),
        "@@CADDYFILE@@": shlex.quote(r.get("caddyfile", "/etc/caddy/Caddyfile")),
    }.items():
        script = script.replace(token, value)
    rpc = cfg.get("rpc", {})
    dash = cfg.get("dashboard", {})
    # Secrets inlined as shell-quoted assignments — the whole script (incl. these)
    # is piped to `bash -s` over SSH stdin, so secrets never reach argv / the
    # process table, and the script reads them as ordinary vars (no stdin trick).
    cookie = f"{dash.get('password','')}-cookie-{r.get('domain','')}"
    secrets = "\n".join([
        f"RPC_HTTPS={shlex.quote(rpc.get('https',''))}",
        f"RPC_WSS={shlex.quote(rpc.get('wss',''))}",
        f"DASH_PW={shlex.quote(dash.get('password',''))}",
        f"DASH_SECRET={shlex.quote(cookie)}",
    ])
    return script.replace("@@SECRETS@@", secrets) + "\n"


def ssh_base(cfg: dict) -> list[str]:
    r = cfg.get("remote", {})
    cmd = ["ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=12"]
    key = os.path.expanduser(str(r.get("ssh_key") or ""))
    if key and Path(key).exists():
        cmd += ["-i", key]
    cmd.append(f"{r.get('vps_user','root')}@{r.get('vps_host','')}")
    return cmd


# --------------------------------------------------------------------------- #
# SSH bootstrap (fresh box: no key paired yet — install our pubkey via password)
# --------------------------------------------------------------------------- #
def _have(cmd: str) -> bool:
    return subprocess.run(["which", cmd], capture_output=True).returncode == 0


def _key_auth_ok(cfg: dict) -> bool:
    """Non-interactive probe: does passwordless key auth already work?"""
    r = cfg.get("remote", {})
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
           "-o", "ConnectTimeout=10", "-o", "PasswordAuthentication=no"]
    key = os.path.expanduser(str(r.get("ssh_key") or ""))
    if key and Path(key).exists():
        cmd += ["-i", key]
    cmd.append(f"{r.get('vps_user','root')}@{r.get('vps_host','')}")
    cmd.append("true")
    try:
        return subprocess.run(cmd, capture_output=True, timeout=25).returncode == 0
    except subprocess.TimeoutExpired:
        return False


def _ensure_local_key(cfg: dict, emit) -> tuple[str, str]:
    """Return (private_key_path, pubkey_path), generating an ed25519 key if missing."""
    r = cfg.get("remote", {})
    key = os.path.expanduser(str(r.get("ssh_key") or "~/.ssh/id_ed25519"))
    pub = key + ".pub"
    if Path(key).exists() and Path(pub).exists():
        return key, pub
    Path(key).parent.mkdir(parents=True, exist_ok=True)
    os.chmod(Path(key).parent, 0o700)
    emit(f"本地未找到 SSH key,生成 {key} (ed25519, 无口令) …")
    subprocess.run(["ssh-keygen", "-t", "ed25519", "-N", "", "-q", "-f", key], check=True)
    return key, pub


def _vps_password(cfg: dict) -> str:
    pw = (cfg.get("remote", {}) or {}).get("vps_password") or ""
    return pw or _read_text(ROOT / "secret" / "vps-password")


def install_pubkey(cfg: dict, emit) -> bool:
    """Ensure our pubkey is in the VPS authorized_keys, using password auth once."""
    r = cfg.get("remote", {})
    host, user = r.get("vps_host", ""), r.get("vps_user", "root")
    key, pub = _ensure_local_key(cfg, emit)
    pubtext = Path(pub).read_text(encoding="utf-8").strip()
    password = _vps_password(cfg)
    if not password:
        emit("ERROR: key 认证不通且无 VPS 密码 — 在面板填「VPS 密码」或写入 secret/vps-password")
        return False
    if not _have("sshpass"):
        emit("ERROR: 需要 sshpass 用密码装公钥 (macOS: brew install hudochenkov/sshpass/sshpass)")
        return False
    # reused-IP guard: drop any stale host key so accept-new can re-pin the new box.
    subprocess.run(["ssh-keygen", "-R", host], capture_output=True)
    emit(f"用密码登录 {user}@{host} 安装公钥(SSH 配对)…")
    remote_cmd = (
        "set -e; umask 077; mkdir -p ~/.ssh; touch ~/.ssh/authorized_keys; "
        f"grep -qxF {shlex.quote(pubtext)} ~/.ssh/authorized_keys || "
        f"echo {shlex.quote(pubtext)} >> ~/.ssh/authorized_keys; echo KEY_INSTALLED"
    )
    # password via SSHPASS env (never argv / process table); pubkey is not secret.
    cmd = ["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=accept-new",
           "-o", "ConnectTimeout=15", "-o", "PreferredAuthentications=password",
           "-o", "PubkeyAuthentication=no", f"{user}@{host}", remote_cmd]
    rc = _stream(cmd, emit, env={"SSHPASS": password})
    if rc != 0:
        emit(f"ERROR: 密码登录/装公钥失败 (exit {rc}) — 检查 IP / 用户 / 密码")
        return False
    if not _key_auth_ok(cfg):
        emit("ERROR: 公钥已写入但 key 认证仍不通 — 检查 VPS sshd PubkeyAuthentication 是否开启")
        return False
    emit("公钥安装成功,后续免密登录 ✓")
    return True


# --------------------------------------------------------------------------- #
# orchestration (streams log lines to a callback)
# --------------------------------------------------------------------------- #
def _stream(cmd: list[str], emit, *, stdin_data: str | None = None, env: dict | None = None) -> int:
    emit(f"$ {' '.join(shlex.quote(c) for c in cmd[:3])} …")
    p = subprocess.Popen(
        cmd, cwd=str(ROOT), stdin=subprocess.PIPE if stdin_data else None,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        env={**os.environ, **(env or {})},
    )
    if stdin_data:
        p.stdin.write(stdin_data)
        p.stdin.close()
    for line in p.stdout:
        emit(line.rstrip("\n"))
    return p.wait()


# ---- 环境准备(prepare):重活,不启动 dashboard ----
def prepare_local(cfg: dict, emit) -> bool:
    rpc = cfg.get("rpc", {})
    if rpc.get("https") or rpc.get("wss"):
        (ROOT / "secret").mkdir(exist_ok=True)
        (ROOT / "secret" / "rpc").write_text(f"{rpc.get('https','')}\n{rpc.get('wss','')}\n", encoding="utf-8")
        os.chmod(ROOT / "secret" / "rpc", 0o600)
        emit("已写入 secret/rpc(链上检测用)")
    emit("RESULT_OK 本地环境就绪 — 点「启动」")
    return True


def prepare_remote(cfg: dict, emit) -> bool:
    r = cfg.get("remote", {})
    if not r.get("vps_host"):
        emit("ERROR: remote.vps_host 未设置")
        return False
    if not cfg.get("dashboard", {}).get("password"):
        emit("ERROR: dashboard.password 未设置")
        return False
    # 裸机引导:key 认证不通时,用密码登录把本地公钥装进 VPS(SSH 配对),之后全程免密。
    if _key_auth_ok(cfg):
        emit("SSH key 认证已通过 ✓")
    else:
        emit("SSH key 认证不通 — 进入裸机引导(SSH 配对)…")
        if not install_pubkey(cfg, emit):
            emit("RESULT_FAIL SSH 配对失败")
            return False
    emit(f"SSH → {r.get('vps_user','root')}@{r.get('vps_host')}  环境准备(装依赖/拉代码/密钥/服务/反代)…")
    rc = _stream(ssh_base(cfg) + ["bash", "-s"], emit, stdin_data=build_remote_payload(cfg))
    if rc == 0:
        emit("RESULT_OK 环境就绪 — 点「启动」拉起 dashboard")
        return True
    emit(f"RESULT_FAIL (exit {rc})")
    return False


# ---- 启动(start):只起 dashboard ----
def start_local(cfg: dict, emit) -> bool:
    loc = cfg.get("local", {})
    dash = cfg.get("dashboard", {})
    if not dash.get("password"):
        emit("ERROR: dashboard.password 未设置")
        return False
    port = int(loc.get("port", 8787))
    env = {
        "POLY_FIGHT_DASH_PASSWORD": dash.get("password", ""),
        "POLY_FIGHT_DASH_COOKIE_SECRET": f"{dash.get('password','')}-local-cookie",
    }
    (ROOT / "logs").mkdir(exist_ok=True)
    log = ROOT / "logs" / "dashboard-serve.out"
    emit(f"启动本地 dashboard → http://{loc.get('host','127.0.0.1')}:{port}")
    cmd = [sys.executable, "-m", "poly_fight.cli", "--data-dir", loc.get("data_dir", "data"),
           "serve", "--host", loc.get("host", "127.0.0.1"), "--port", str(port)]
    with log.open("ab") as f:
        subprocess.Popen(cmd, cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True,
                         env={**os.environ, **env})
    emit("RESULT_OK " + f"http://{loc.get('host','127.0.0.1')}:{port}")
    return True


def start_remote(cfg: dict, emit) -> bool:
    r = cfg.get("remote", {})
    if not r.get("vps_host"):
        emit("ERROR: remote.vps_host 未设置")
        return False
    port = int(r.get("port", 8787))
    emit("重启 VPS dashboard (清端口 + systemctl restart)…")
    # 先停 systemd 单元,再杀掉任何仍占用端口的残留进程(历史手动/孤儿进程会让 systemd
    # 撞端口 Address already in use 起不来),最后重启。重复点也能收敛到唯一 systemd 实例。
    sh = ("set +e\n"
          "systemctl stop poly-fight-dashboard 2>/dev/null\n"
          f"PORT={port}\n"
          'for sig in TERM KILL; do\n'
          '  pids=$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oE "pid=[0-9]+" | cut -d= -f2 | sort -u)\n'
          '  [ -z "$pids" ] && break\n'
          '  for p in $pids; do kill -$sig "$p" 2>/dev/null && echo "  清理残留进程 $p (SIG$sig)"; done\n'
          '  sleep 1\n'
          'done\n'
          "systemctl restart poly-fight-dashboard\n"
          "sleep 1\n"
          'echo "状态: $(systemctl is-active poly-fight-dashboard)"\n')
    rc = _stream(ssh_base(cfg) + ["bash", "-s"], emit, stdin_data=sh)
    if rc == 0:
        emit("RESULT_OK " + f"https://{r.get('domain','')}")
        return True
    emit(f"RESULT_FAIL (exit {rc})")
    return False


def stop_local(cfg: dict, emit) -> None:
    port = int(cfg.get("local", {}).get("port", 8787))
    emit(f"停止本地 dashboard (port {port})…")
    # match the exact serve process for this port; never broad pkill
    out = subprocess.run(["pgrep", "-f", f"poly_fight.cli .*serve .*--port {port}"],
                         capture_output=True, text=True).stdout.split()
    if not out:
        out = subprocess.run(["pgrep", "-f", "poly_fight.cli.*serve"], capture_output=True, text=True).stdout.split()
    for pid in out:
        subprocess.run(["kill", "-TERM", pid])
        emit(f"  killed {pid}")
    emit("RESULT_OK 已停止")


def stop_remote(cfg: dict, emit) -> None:
    port = int(cfg.get("remote", {}).get("port", 8787))
    emit("停止 VPS dashboard (systemctl stop + 清端口残留)…")
    # systemctl stop 只停 systemd 托管的实例;再杀掉任何仍占端口的孤儿进程,确保真停干净。
    sh = ("set +e\n"
          "systemctl stop poly-fight-dashboard 2>/dev/null\n"
          f"PORT={port}\n"
          'for sig in TERM KILL; do\n'
          '  pids=$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oE "pid=[0-9]+" | cut -d= -f2 | sort -u)\n'
          '  [ -z "$pids" ] && break\n'
          '  for p in $pids; do kill -$sig "$p" 2>/dev/null && echo "  清理残留进程 $p (SIG$sig)"; done\n'
          '  sleep 1\n'
          'done\n'
          'echo "状态: $(systemctl is-active poly-fight-dashboard)"\n')
    rc = _stream(ssh_base(cfg) + ["bash", "-s"], emit, stdin_data=sh)
    emit("RESULT_OK dashboard 已停" if rc == 0 else f"RESULT_FAIL (exit {rc})")


# --------------------------------------------------------------------------- #
# http server
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("", "/"):
            self._send(200, (HERE / "ui.html").read_bytes(), "text/html; charset=utf-8")
        elif u.path == "/api/config":
            self._send(200, json.dumps(masked(load_config())))
        elif u.path in ("/api/prepare", "/api/start", "/api/stop"):
            self._sse_run(u.path, parse_qs(u.query))
        else:
            self._send(404, json.dumps({"error": "not_found"}))

    def do_POST(self):
        u = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or "{}")
        if u.path == "/api/config":
            save_config(merge_ui(load_config(), body))
            self._send(200, json.dumps({"ok": True}))
        elif u.path == "/api/quit":
            self._send(200, json.dumps({"ok": True}))
            threading.Timer(0.3, lambda: os._exit(0)).start()
        else:
            self._send(404, json.dumps({"error": "not_found"}))

    def _sse_open(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _emit(self, line):
        try:
            self.wfile.write(f"data: {line}\n\n".encode("utf-8"))
            self.wfile.flush()
        except (BrokenPipeError, OSError):
            pass

    def _sse_run(self, path, qs):
        self._sse_open()
        mode = (qs.get("mode") or ["local"])[0]
        cfg = load_config()
        action = {"/api/prepare": "prepare", "/api/start": "start", "/api/stop": "stop"}.get(path, "start")
        fn = {
            ("prepare", "local"): prepare_local, ("prepare", "remote"): prepare_remote,
            ("start", "local"): start_local, ("start", "remote"): start_remote,
            ("stop", "local"): stop_local, ("stop", "remote"): stop_remote,
        }.get((action, mode))
        try:
            if fn:
                fn(cfg, self._emit)
            else:
                self._emit("RESULT_FAIL unknown action")
        except Exception as exc:  # noqa: BLE001
            self._emit(f"RESULT_FAIL {exc}")
        self._emit("__DONE__")


def main():
    if "--init" in sys.argv:
        init_config()
        return
    if not CONFIG_PATH.exists():
        print(f"no {CONFIG_PATH} yet — creating from template; run --init to import existing secret/*")
        save_config(load_config())
    srv = ThreadingHTTPServer(("127.0.0.1", LAUNCHER_PORT), Handler)
    url = f"http://127.0.0.1:{LAUNCHER_PORT}/"
    print(f"poly-fight launcher → {url}  (Ctrl-C to quit)")
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
