#!/usr/bin/env python3
"""Flatten decoder parquet outputs into per-table Hive-partitioned Parquet dirs."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, Sequence, Set, Tuple

import click
import pandas as pd  # only used for Arrow dtype conversions when needed
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

DEFAULT_BATCH_SIZE = 2_000_000
CHANNEL_RE = re.compile(r"^(?P<chan>\d+)")
FEED_LEG_RE = re.compile(r"feed(?P<leg>[AB])", re.IGNORECASE)

TABLE_PARTITIONS: dict[str, Sequence[str]] = {
    "instruments": ["channel_hint", "security_id"],
    "instrument_underlyings": ["channel_hint", "security_id"],
    "instrument_legs": ["channel_hint", "security_id"],
    "instrument_attributes": ["channel_hint", "security_id"],
    "snapshot_headers": ["channel_hint", "security_id"],
    "snapshot_orders": ["channel_hint", "security_id"],
    "incremental_orders": ["channel_hint", "security_id", "feed_leg"],
    "incremental_deletes": ["channel_hint", "security_id", "feed_leg"],
    "incremental_mass_deletes": ["channel_hint", "security_id", "feed_leg"],
    "incremental_trades": ["channel_hint", "security_id", "feed_leg"],
    "incremental_other": ["channel_hint", "security_id", "feed_leg"],
}

SMALL_TABLES = {
    "instruments",
    "instrument_underlyings",
    "instrument_legs",
    "instrument_attributes",
}


def parse_channel_from_name(source_file: str) -> int:
    match = CHANNEL_RE.search(Path(source_file).name)
    if not match:
        raise ValueError(f"Cannot infer channel from source '{source_file}'")
    return int(match.group("chan"))


def parse_leg_from_name(source_file: str) -> str | None:
    match = FEED_LEG_RE.search(source_file)
    return match.group("leg").upper() if match else None


def parse_table_filter(arg: str) -> Set[str]:
    if not arg or arg.strip() in {"*", "all"}:
        return set(TABLE_PARTITIONS)
    names = {piece.strip() for piece in arg.split(",") if piece.strip()}
    unknown = names - set(TABLE_PARTITIONS)
    if unknown:
        raise click.ClickException(
            "Unknown table names: " + ", ".join(sorted(unknown)))
    return names


def ensure_extra_columns(table: pa.Table, partition_cols: Sequence[str]) -> pa.Table:
    needs_channel = "channel_hint" in partition_cols and "channel_hint" not in table.schema.names
    needs_leg = "feed_leg" in partition_cols and "feed_leg" not in table.schema.names
    if not needs_channel and not needs_leg:
        return table
    if "source_file" not in table.schema.names:
        raise click.ClickException(
            "Cannot derive channel/feed_leg because 'source_file' column is missing")
    source_array = table.column(table.schema.get_field_index("source_file"))
    data = source_array.to_pylist()
    new_columns = []
    if needs_channel:
        channels = [parse_channel_from_name(s) for s in data]
        new_columns.append(pa.field("channel_hint", pa.int32()))
        table = table.append_column("channel_hint", pa.array(channels, type=pa.int32()))
    if needs_leg:
        legs = [parse_leg_from_name(s) for s in data]
        new_columns.append(pa.field("feed_leg", pa.string()))
        table = table.append_column("feed_leg", pa.array(legs, type=pa.string()))
    return table


def iter_batches(pf: pq.ParquetFile, desc: str, progress: bool,
                 batch_size: int) -> Iterable[pa.RecordBatch]:
    iterator = pf.iter_batches(batch_size=batch_size)
    if not progress:
        yield from iterator
        return
    total = pf.metadata.num_rows if pf.metadata is not None else None
    with tqdm(total=total, desc=desc, unit="rows") as bar:
        for batch in iterator:
            bar.update(batch.num_rows)
            yield batch


def split_table(table: pa.Table, partition_cols: Sequence[str]) -> Iterable[Tuple[Tuple, pa.Table]]:
    if table.num_rows == 0:
        return
    table = table.combine_chunks()
    sort_keys = [(col, "ascending") for col in partition_cols]
    indices = pc.sort_indices(table, sort_keys=sort_keys)
    sorted_table = table.take(indices)
    arrays = [sorted_table[col] for col in partition_cols]
    values = list(zip(*(arr.to_pylist() for arr in arrays)))
    start = 0
    current_key = values[0]
    for idx, key in enumerate(values):
        if key != current_key:
            yield current_key, sorted_table.slice(start, idx - start)
            current_key = key
            start = idx
    yield current_key, sorted_table.slice(start, len(values) - start)


class PartitionedWriter:
    def __init__(self, base_dir: Path, table_name: str, partition_cols: Sequence[str]):
        self.partition_cols = partition_cols
        self.table_dir = base_dir / table_name
        self.table_dir.mkdir(parents=True, exist_ok=True)
        self.counters: Dict[Tuple, int] = {}

    def _value_str(self, value: object) -> str:
        if value is None:
            return "null"
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    def write(self, keys: Tuple, subset: pa.Table) -> None:
        path = self.table_dir
        for name, value in zip(self.partition_cols, keys):
            path = path / f"{name}={self._value_str(value)}"
        path.mkdir(parents=True, exist_ok=True)
        counter = self.counters.get(keys, 0)
        file_path = path / f"part-{counter}.parquet"
        pq.write_table(subset, file_path)
        self.counters[keys] = counter + 1


def write_small_table(parsed_dir: Path, output_dir: Path, name: str,
                      partition_cols: Sequence[str]) -> None:
    path = parsed_dir / f"{name}.parquet"
    if not path.exists():
        return
    table = pq.read_table(path)
    table = ensure_extra_columns(table, partition_cols)
    writer = PartitionedWriter(output_dir, name, partition_cols)
    for keys, subset in split_table(table, partition_cols):
        writer.write(keys, subset)


def write_stream_table(parsed_dir: Path, output_dir: Path, name: str,
                       partition_cols: Sequence[str], progress: bool,
                       batch_size: int) -> None:
    source = parsed_dir / f"{name}.parquet"
    if not source.exists():
        return
    pf = pq.ParquetFile(source)
    writer = PartitionedWriter(output_dir, name, partition_cols)
    for batch in iter_batches(pf, name, progress, batch_size):
        if batch.num_rows == 0:
            continue
        table = pa.Table.from_batches([batch])
        table = ensure_extra_columns(table, partition_cols)
        for keys, subset in split_table(table, partition_cols):
            writer.write(keys, subset)


def table_exists(output_dir: Path, table: str) -> bool:
    target = output_dir / table
    return target.exists() and any(target.glob("**/*.parquet"))


def remove_table_outputs(output_dir: Path, table: str) -> None:
    shutil.rmtree(output_dir / table, ignore_errors=True)


@click.command()
@click.option("--parsed-dir", required=True, type=click.Path(path_type=Path, exists=True),
              help="Directory produced by the native decoder (with *.parquet tables)")
@click.option("--output-dir", required=True, type=click.Path(path_type=Path),
              help="Destination directory for partitioned outputs")
@click.option("--overwrite/--no-overwrite", default=False, show_default=True,
              help="Replace existing partitions for the requested tables")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Display tqdm progress bars while reading Parquet batches")
@click.option("--tables", default="*", show_default=True,
              help="Comma-separated list of tables to process")
@click.option("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, show_default=True,
              help="Row count per Parquet batch when streaming large tables")
def main(parsed_dir: Path, output_dir: Path, overwrite: bool, progress: bool,
         tables: str, batch_size: int) -> None:
    if batch_size <= 0:
        raise click.ClickException("--batch-size must be a positive integer")
    selected = parse_table_filter(tables)

    output_dir.mkdir(parents=True, exist_ok=True)
    for table in selected:
        if table_exists(output_dir, table):
            if not overwrite:
                raise click.ClickException(
                    f"Table '{table}' already exists in {output_dir}. "
                    "Pass --overwrite to regenerate it.")
            remove_table_outputs(output_dir, table)

    for name in selected:
        cols = TABLE_PARTITIONS[name]
        if name in SMALL_TABLES:
            write_small_table(parsed_dir, output_dir, name, cols)
        else:
            write_stream_table(parsed_dir, output_dir, name, cols, progress, batch_size)

    click.echo(f"Flattened data written to {output_dir}")


if __name__ == "__main__":  # pragma: no cover
    main()
