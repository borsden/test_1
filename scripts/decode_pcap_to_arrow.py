#!/usr/bin/env python3
"""Helper script to configure/build the native decoder and run it on PCAP inputs."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Sequence

import click

REPO_ROOT = Path(__file__).resolve().parents[1]


def run(cmd: Sequence[str]) -> None:
    click.echo("$ " + " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    if proc.returncode != 0:
        raise click.ClickException(f"Command failed with exit code {proc.returncode}")


def ensure_built(build_dir: Path, skip: bool) -> None:
    if skip:
        return
    run(["cmake", "-S", ".", "-B", str(build_dir)])
    run(["cmake", "--build", str(build_dir)])


@click.command()
@click.option("--input", "inputs", multiple=True, required=True,
              type=click.Path(path_type=Path),
              help="File or directory with .pcap files (option can be repeated)")
@click.option("--output", required=True, type=click.Path(path_type=Path),
              help="Directory for parquet outputs")
@click.option("--tables", default="", show_default=True,
              help="Comma-separated list of tables to emit")
@click.option("--max-packets", type=int, default=0, show_default=True,
              help="Optional per-file cap for debugging runs")
@click.option("--no-validate", is_flag=True, help="Skip schema validation in the native decoder")
@click.option("--build-dir", default="build", show_default=True,
              type=click.Path(path_type=Path))
@click.option("--skip-build", is_flag=True,
              help="Assume cmake project is already configured and built")
def main(inputs: Sequence[Path], output: Path, tables: str, max_packets: int, no_validate: bool,
         build_dir: Path, skip_build: bool) -> None:
    ensure_built(build_dir, skip_build)
    binary = build_dir / "b3sbe_decode"
    if not binary.exists():
        raise click.ClickException(f"Decoder binary not found at {binary}. Did the build complete?")

    cmd = [str(binary), "--output", str(output)]
    for item in inputs:
        cmd.extend(["--input", str(item)])
    if tables:
        cmd.extend(["--tables", tables])
    if max_packets:
        cmd.extend(["--max-packets", str(max_packets)])
    if no_validate:
        cmd.append("--no-validate")
    run(cmd)


if __name__ == "__main__":  # pragma: no cover
    main()
