# poly-fight launcher

A tiny local control panel to start the dashboard **locally** or deploy it to the
**VPS** — without remembering commands.

## Why not a pure static HTML
Browsers can't open SSH / raw TCP, so a double-clicked `.html` cannot drive a VPS.
This launcher is a stdlib-only Python script that serves the HTML UI on localhost
**and** does the real work (local subprocess / `ssh`+`scp`). You still just
double-click — it opens the UI in your browser.

## Use
```bash
python3 launcher/launcher.py --init   # one-time: build secret/launcher.json from
                                      # your existing secret/rpc + dashboard-password
open launcher/launcher.command        # or double-click it in Finder
```
The UI opens at `http://127.0.0.1:8799`.

- **本地**: fills creds from config, click 启动 → runs `serve` locally on the port shown.
- **远程 VPS**: fill VPS host / user / **VPS password** (first time only) / domain,
  click **环境准备** → it bootstraps a bare box end-to-end:
  0. if key auth doesn't work yet, generates a local ed25519 key (if missing) and
     uses the **VPS password** to install your pubkey into the box's
     `authorized_keys` (SSH pairing) — every later run is passwordless,
  then over SSH:
  1. installs missing deps (`git` / `python3` / `caddy`) — idempotent, skipped if present,
  2. clones or `git pull`s the repo (aborts if the VPS worktree is dirty — policy),
  3. writes `secret/rpc` + `secret/dashboard.env` on the VPS (chmod 600),
  4. installs/enables a `poly-fight-dashboard` **systemd** unit (auto-restart, boot-start),
  5. if `ufw` is active, opens 80/443 (keeping 22) so Let's Encrypt's ACME
     challenge can reach the box — otherwise the cert never issues and HTTPS hangs,
     then adds a Caddy `reverse_proxy` block for the domain (idempotent) + reloads Caddy.

  Then click **启动** to bring the dashboard up; it reports `https://<domain>`.

Secrets travel to the VPS over the encrypted SSH channel via **stdin** — never as
command-line args (so they don't show up in the VPS process table) and never in git.

## Config
`secret/launcher.json` (gitignored — schema in `launcher/config.example.json`).
Secrets are only ever stored in this one local file. The follow runner reads
`secret/rpc` from disk as usual; for real-money phase the Polymarket key can be
added later (currently the tool is read-only / paper).

## Notes
- SSH uses key auth (`remote.ssh_key`). On a **fresh box** you don't need to run
  `ssh-copy-id` yourself — fill the **VPS 密码** field once and 环境准备 pairs the
  key for you (needs `sshpass` locally: `brew install hudochenkov/sshpass/sshpass`).
  The password is sent over SSH via the `SSHPASS` env var (never argv / process
  table); clear the field after the first successful run.
- The dashboard binds `127.0.0.1` on the VPS — it's reachable only through Caddy
  over HTTPS, never exposed directly.
- The launcher never broad-`pkill`s; it stops the systemd unit (remote) or the
  exact serve pid (local).
