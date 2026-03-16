#!/usr/bin/env python3
"""Wrapper around the SBE code generator with convenient CLI flags."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import Iterable

import click

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = REPO_ROOT / "schema" / "b3-market-data-messages-1.8.0.xml"
DEFAULT_JAR = REPO_ROOT / "third_party" / "sbe-all-1.38.0.jar"
DEFAULT_OUTPUT = REPO_ROOT / "cpp_decoder" / "sbe-generated"


def resolve_java_bin(explicit: Path | None) -> str:
    if explicit:
        return str(explicit)
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.exists():
            return str(candidate)
    return "java"


def build_props(output_dir: Path, target_language: str, stubs: bool,
                schema_version: str | None) -> dict[str, str]:
    props = {
        "sbe.output.dir": str(output_dir),
        "sbe.target.language": target_language,
        "sbe.generate.stubs": "true" if stubs else "false",
        "sbe.validation.stop.on.error": "true",
    }
    if schema_version:
        props["sbe.schema.version"] = schema_version
    return props


@click.command()
@click.option("--schema", "schema_path", type=click.Path(dir_okay=False, path_type=Path),
              default=DEFAULT_SCHEMA, show_default=True, help="Path to the SBE XML schema")
@click.option("--jar", "jar_path", type=click.Path(dir_okay=False, path_type=Path),
              default=DEFAULT_JAR, show_default=True, help="Path to sbe-all-<version>.jar")
@click.option("--output", "output_dir", type=click.Path(file_okay=False, path_type=Path),
              default=DEFAULT_OUTPUT, show_default=True,
              help="Destination directory for the generated codecs")
@click.option("--java-bin", type=click.Path(dir_okay=False, path_type=Path),
              help="Path to the java executable (defaults to $JAVA_HOME/bin/java or system java)")
@click.option("--java-opts", default="-Xmx2g", show_default=True,
              help="Extra options passed to the JVM")
@click.option("--target-language", default="cpp", show_default=True,
              help="SBE target language")
@click.option("--stubs/--no-stubs", default=True, show_default=True,
              help="Toggle generation of stub implementations")
@click.option("--define", "defines", multiple=True,
              help="Additional -Dkey=value properties passed to the generator")
@click.option("--schema-version", default=None,
              help="Override sbe.schema.version property if necessary")
@click.option("--dry-run", is_flag=True, help="Print the java command without executing it")
def main(schema_path: Path, jar_path: Path, output_dir: Path, java_bin: Path | None, java_opts: str,
         target_language: str, stubs: bool, defines: Iterable[str], schema_version: str | None,
         dry_run: bool) -> None:
    if not schema_path.exists():
        raise click.ClickException(f"Schema not found: {schema_path}")
    if not jar_path.exists():
        raise click.ClickException(f"SBE jar not found: {jar_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    java_exe = resolve_java_bin(java_bin)
    props = build_props(output_dir, target_language, stubs, schema_version)

    cmd: list[str] = [java_exe]
    if java_opts:
        cmd.extend(shlex.split(java_opts))
    for key, value in props.items():
        cmd.append(f"-D{key}={value}")
    for item in defines:
        cmd.append(f"-D{item}")
    cmd.extend(["-jar", str(jar_path), str(schema_path)])

    click.echo("Running: " + " ".join(shlex.quote(part) for part in cmd))
    if dry_run:
        return
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(f"Generator failed with exit code {exc.returncode}") from exc


if __name__ == "__main__":  # pragma: no cover
    main()
