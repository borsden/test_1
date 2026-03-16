#!/usr/bin/env python3
"""Helper script to build the native decoder and run it on a directory of PCAP files."""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from typing import List

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(cmd: List[str]) -> None:
    print("$", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def ensure_built(build_dir: pathlib.Path, skip: bool) -> None:
    if skip:
        return
    run(["cmake", "-S", ".", "-B", str(build_dir)])
    run(["cmake", "--build", str(build_dir)])


def main(argv: List[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", dest="inputs", required=True,
                        help="File or directory with .pcap files (can be repeated)")
    parser.add_argument("--output", required=True, help="Directory for parquet outputs")
    parser.add_argument("--tables", help="Comma-separated list of tables to emit", default="")
    parser.add_argument("--max-packets", type=int, default=0,
                        help="Optional per-file cap for debugging runs")
    parser.add_argument("--no-validate", action="store_true", help="Skip schema validation")
    parser.add_argument("--build-dir", default="build")
    parser.add_argument("--skip-build", action="store_true",
                        help="Assume cmake project is already configured/built")
    args = parser.parse_args(argv)

    build_dir = REPO_ROOT / args.build_dir
    ensure_built(build_dir, args.skip_build)
    binary = build_dir / "b3sbe_decode"
    if not binary.exists():
        raise SystemExit(f"Decoder binary not found at {binary}. Did the build complete?")

    cmd = [str(binary), "--output", args.output]
    for item in args.inputs:
        cmd.extend(["--input", item])
    if args.tables:
        cmd.extend(["--tables", args.tables])
    if args.max_packets:
        cmd.extend(["--max-packets", str(args.max_packets)])
    if args.no_validate:
        cmd.append("--no-validate")
    run(cmd)

if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
