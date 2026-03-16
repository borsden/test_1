#!/usr/bin/env python3
"""Flatten decoder parquet outputs into per-channel/per-instrument CSV bundles."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, Sequence

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

CHANNEL_RE = re.compile(r"^(?P<chan>\d+)")
FEED_LEG_RE = re.compile(r"feed(?P<leg>[AB])", re.IGNORECASE)
BATCH_SIZE = 65_536

INSTRUMENT_TABLES = [
    "instruments",
    "instrument_underlyings",
    "instrument_legs",
    "instrument_attributes",
]

PARTITIONED_TABLES: dict[str, Sequence[str]] = {
    "snapshot_headers": ["channel_hint", "security_id"],
    "snapshot_orders": ["channel_hint", "security_id"],
    "incremental_orders": ["channel_hint", "security_id", "feed_leg"],
    "incremental_deletes": ["channel_hint", "security_id", "feed_leg"],
    "incremental_mass_deletes": ["channel_hint", "security_id", "feed_leg"],
    "incremental_trades": ["channel_hint", "security_id", "feed_leg"],
}


def parse_channel_from_name(source_file: str) -> int:
    match = CHANNEL_RE.search(Path(source_file).name)
    if not match:
        raise ValueError(f"Cannot infer channel from source '{source_file}'")
    return int(match.group("chan"))


def parse_leg_from_name(source_file: str) -> str | None:
    match = FEED_LEG_RE.search(source_file)
    return match.group("leg").upper() if match else None


class FlattenContext:
    """Helpers for lightweight CSV writers (instrument metadata, extras)."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self._written_flags: Dict[Path, bool] = {}

    def channel_dir(self, channel: int) -> Path:
        path = self.output_dir / f"{channel}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def instrument_dir(self, channel: int, security_id: int) -> Path:
        return self.channel_dir(channel) / f"{security_id}"

    def leg_dir(self, channel: int, security_id: int, leg: str) -> Path:
        return self.instrument_dir(channel, security_id) / leg

    def _write_df(self, path: Path, df: pd.DataFrame) -> None:
        if df.empty:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        already = self._written_flags.get(path, False)
        mode = "a" if already else "w"
        with path.open(mode, newline="") as handle:
            df.to_csv(handle, header=not already, index=False)
        self._written_flags[path] = True

    def write_instrument_row(self, channel: int, row: pd.Series) -> None:
        inst_dir = self.instrument_dir(channel, int(row["security_id"]))
        df = row.to_frame().T
        self._write_df(inst_dir / "instrument.csv", df)

    def write_channel_table(self, channel: int, name: str, df: pd.DataFrame) -> None:
        self._write_df(self.channel_dir(channel) / f"{name}.csv", df)

    def write_incremental(self, channel: int, security_id: int, leg: str,
                          name: str, df: pd.DataFrame) -> None:
        leg_dir = self.leg_dir(channel, security_id, leg)
        self._write_df(leg_dir / f"{name}.csv", df)


def ensure_channel(df: pd.DataFrame) -> None:
    if "channel_hint" in df.columns:
        return
    df["channel_hint"] = df["source_file"].map(parse_channel_from_name)


def ensure_feed_leg(df: pd.DataFrame) -> None:
    if "feed_leg" in df.columns:
        return
    df["feed_leg"] = df["source_file"].map(parse_leg_from_name)


def load_table(parsed_dir: Path, name: str) -> pd.DataFrame:
    path = parsed_dir / f"{name}.parquet"
    if not path.exists():
        raise click.ClickException(f"Missing parquet table: {path}")
    table = pq.read_table(path)
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def write_instrument_tables(ctx: FlattenContext, parsed_dir: Path) -> None:
    instruments = load_table(parsed_dir, "instruments")
    ensure_channel(instruments)
    for channel, chan_df in instruments.groupby("channel_hint"):
        channel_int = int(channel)
        ctx.write_channel_table(channel_int, "instruments", chan_df)
        for _, row in chan_df.iterrows():
            ctx.write_instrument_row(channel_int, row)

    for extra in ("instrument_underlyings", "instrument_legs", "instrument_attributes"):
        df = load_table(parsed_dir, extra)
        ensure_channel(df)
        for channel, chan_df in df.groupby("channel_hint"):
            ctx.write_channel_table(int(channel), extra, chan_df)


def iter_batches(pf: pq.ParquetFile, desc: str, progress: bool) -> Iterable[pa.RecordBatch]:
    iterator = pf.iter_batches(batch_size=BATCH_SIZE)
    if not progress:
        yield from iterator
        return
    total = pf.metadata.num_rows if pf.metadata is not None else None
    with tqdm(total=total, desc=desc, unit="rows") as bar:
        for batch in iterator:
            bar.update(batch.num_rows)
            yield batch


def partition_schema(schema: pa.Schema, columns: Sequence[str]) -> pa.Schema:
    fields = []
    for name in columns:
        idx = schema.get_field_index(name)
        if idx == -1:
            raise click.ClickException(
                f"Column '{name}' not present in schema {schema.names}")
        fields.append(schema.field(idx))
    return pa.schema(fields)


def relocate_partitions(tmp_base: Path, final_base: Path, table_name: str) -> None:
    for csv_path in tmp_base.rglob(f"{table_name}.csv"):
        relative = csv_path.relative_to(tmp_base)
        parts = list(relative.parts)
        *dirs, filename = parts
        stripped: list[str] = []
        for part in dirs:
            if "=" in part:
                _, value = part.split("=", 1)
            else:
                value = part
            if value == "__HIVE_DEFAULT_PARTITION__":
                value = "null"
            stripped.append(value)
        dest_dir = final_base.joinpath(*stripped)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / filename
        if dest_path.exists():
            dest_path.unlink()
        csv_path.rename(dest_path)


def write_partitioned_table(parsed_dir: Path, output_dir: Path, name: str,
                            partition_cols: Sequence[str], progress: bool) -> None:
    source = parsed_dir / f"{name}.parquet"
    if not source.exists():
        return
    pf = pq.ParquetFile(source)
    part_schema = partition_schema(pf.schema_arrow, partition_cols)
    tmp_base = output_dir / "._tmp" / name
    if tmp_base.exists():
        shutil.rmtree(tmp_base)
    tmp_base.mkdir(parents=True, exist_ok=True)
    ds.write_dataset(
        data=iter_batches(pf, name, progress),
        schema=pf.schema_arrow,
        base_dir=str(tmp_base),
        format="csv",
        partitioning=ds.partitioning(part_schema, flavor="hive"),
        basename_template=f"{name}-{{i}}.csv",
        max_partitions=1_000_000,
        existing_data_behavior="overwrite_or_ignore",
    )
    relocate_partitions(tmp_base, output_dir, name)
    shutil.rmtree(tmp_base)


def write_manual_incremental_other(ctx: FlattenContext, parsed_dir: Path, progress: bool) -> None:
    name = "incremental_other"
    path = parsed_dir / f"{name}.parquet"
    if not path.exists():
        return
    pf = pq.ParquetFile(path)
    for batch in iter_batches(pf, name, progress):
        df = batch.to_pandas(types_mapper=pd.ArrowDtype)
        if df.empty:
            continue
        ensure_channel(df)
        ensure_feed_leg(df)
        for (channel, security_id, leg), group in df.groupby(
                ["channel_hint", "security_id", "feed_leg"]):
            if pd.isna(channel) or pd.isna(security_id) or pd.isna(leg):
                continue
            ctx.write_incremental(int(channel), int(security_id), str(leg), name, group)


@click.command()
@click.option("--parsed-dir", required=True, type=click.Path(path_type=Path, exists=True),
              help="Directory produced by the native decoder (with *.parquet tables)")
@click.option("--output-dir", required=True, type=click.Path(path_type=Path),
              help="Destination directory for flattened CSV layout")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True,
              help="Allow removing an existing output directory before writing")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Display tqdm progress bars while streaming batches")
def main(parsed_dir: Path, output_dir: Path, overwrite: bool, progress: bool) -> None:
    if output_dir.exists():
        if not overwrite:
            raise click.ClickException(
                f"Output directory {output_dir} already exists. Use --overwrite to replace it.")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ctx = FlattenContext(output_dir)
    write_instrument_tables(ctx, parsed_dir)
    for table, partitions in PARTITIONED_TABLES.items():
        write_partitioned_table(parsed_dir, output_dir, table, partitions, progress)
    write_manual_incremental_other(ctx, parsed_dir, progress)
    tmp_root = output_dir / "._tmp"
    if tmp_root.exists():
        shutil.rmtree(tmp_root)
    click.echo(f"Flattened data written to {output_dir}")


if __name__ == "__main__":  # pragma: no cover
    main()
