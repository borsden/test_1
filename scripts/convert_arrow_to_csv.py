"""Конвертация дерева parquet-файлов (например, data/parsed) в CSV."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator, Sequence

import click
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from tqdm import tqdm


def iter_parquet_files(
    root: Path,
    channels: Sequence[str] | None,
    feeds: Sequence[str] | None,
) -> Iterator[Path]:
    channel_filter = set(channels) if channels else None
    feed_filter = set(feeds) if feeds else None

    for channel_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if channel_filter and channel_dir.name not in channel_filter:
            continue

        for parquet_file in sorted(channel_dir.glob("*.parquet")):
            yield parquet_file

        for feed_dir in sorted(p for p in channel_dir.iterdir() if p.is_dir()):
            if not feed_dir.name.startswith("feed_"):
                continue
            if feed_filter and feed_dir.name not in feed_filter:
                continue
            for parquet_file in sorted(feed_dir.rglob("*.parquet")):
                yield parquet_file


def convert_file(src: Path, dst: Path) -> None:
    pf = pq.ParquetFile(src)
    dst.parent.mkdir(parents=True, exist_ok=True)

    with pa.OSFile(str(dst), "wb") as sink:
        with pacsv.CSVWriter(sink, schema=pf.schema_arrow) as writer:
            for batch in pf.iter_batches():
                writer.write_batch(batch)


@click.command()
@click.option(
    "--input",
    "input_root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/parsed"),
    show_default=True,
    help="Каталог с исходными parquet-файлами",
)
@click.option(
    "--output",
    "output_root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/parsed_csv"),
    show_default=True,
    help="Каталог, куда будут сложены CSV",
)
@click.option(
    "--channel",
    "channels",
    multiple=True,
    help="Ограничить набор каналов (можно указать несколько опций)",
)
@click.option(
    "--feed",
    "feeds",
    multiple=True,
    help="Ограничить набор фидов (можно указать несколько опций)",
)
def main(input_root: Path, output_root: Path, channels: Sequence[str], feeds: Sequence[str]) -> None:
    """Построить CSV-копию структуры decoded/parquet."""

    if not input_root.exists():
        raise SystemExit(f"Input directory {input_root} не существует")

    channel_filter = list(channels) or None
    feed_filter = list(feeds) or None

    tasks: Iterable[Path] = list(iter_parquet_files(input_root, channel_filter, feed_filter))
    if not tasks:
        click.echo("Нечего конвертировать: проверьте фильтры")
        return

    base_input = input_root.resolve()
    base_output = output_root.resolve()

    progress = tqdm(tasks, desc="Конвертация", unit="файл")
    for src in progress:
        rel = src.resolve().relative_to(base_input)
        dst = (base_output / rel).with_suffix(".csv")
        progress.set_postfix_str(rel.as_posix())
        convert_file(src, dst)


if __name__ == "__main__":
    main()
