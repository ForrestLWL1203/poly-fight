#!/bin/bash
# Double-click this on macOS to open the poly-fight launcher in your browser.
cd "$(dirname "$0")/.." || exit 1
exec python3 launcher/launcher.py
