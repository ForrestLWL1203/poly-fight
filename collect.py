#!/usr/bin/env python3
"""Run one full esports wallet collection pass."""

import sys

from poly_fight.cli import build_parser


def main() -> int:
    if any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
        print("用法: python3 collect.py")
        print()
        print("执行一次 esports 钱包 collector，自动更新 data/esports/leaderboard.db。")
        print("高级参数请用: python3 -m poly_fight.cli collect --help")
        return 0
    parser = build_parser()
    args = parser.parse_args(["collect", *sys.argv[1:]])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
