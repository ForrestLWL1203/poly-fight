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
- **远程 VPS**: fill VPS host / SSH key / domain, click 启动 → over SSH it:
  1. clones or `git pull`s the repo (aborts if the VPS worktree is dirty — policy),
  2. writes `secret/rpc` + `secret/dashboard.env` on the VPS (chmod 600),
  3. installs/enables a `poly-fight-dashboard` **systemd** unit (auto-restart, boot-start),
  4. adds a Caddy `reverse_proxy` block for the domain (idempotent) + reloads Caddy,
  5. reports `https://<domain>`.

Secrets travel to the VPS over the encrypted SSH channel via **stdin** — never as
command-line args (so they don't show up in the VPS process table) and never in git.

## Config
`secret/launcher.json` (gitignored — schema in `launcher/config.example.json`).
Secrets are only ever stored in this one local file. The follow runner reads
`secret/rpc` from disk as usual; for real-money phase the Polymarket key can be
added later (currently the tool is read-only / paper).

## Notes
- SSH uses key auth (`remote.ssh_key`); set it up once with
  `ssh-copy-id -i ~/.ssh/id_ed25519.pub <user>@<vps>`.
- The dashboard binds `127.0.0.1` on the VPS — it's reachable only through Caddy
  over HTTPS, never exposed directly.
- The launcher never broad-`pkill`s; it stops the systemd unit (remote) or the
  exact serve pid (local).
