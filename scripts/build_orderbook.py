import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence, TextIO

import click
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderbook import BookEventSnapshot, BookReplayer, InstrumentUniverse, PipelineConfig
from orderbook.output_builder import (
    iter_snapshot_bbo_rows,
    iter_snapshot_level_rows,
    iter_snapshot_order_rows,
)

LOGGER = logging.getLogger(__name__)


def _configure_logging(verbose: bool, *, suppress_warnings: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")
    if suppress_warnings:
        logging.getLogger("orderbook").setLevel(logging.ERROR)


@click.command()
@click.option(
    "--data-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/parsed"),
    show_default=True,
    help="Директория с parquet-файлами, созданными декодером",
)
@click.option(
    "--output-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/orderbook"),
    show_default=True,
    help="Куда записывать book_l3/book_l2/book_l1",
)
@click.option(
    "--ticker",
    "tickers",
    multiple=True,
    required=True,
    help="Тикер, для которого нужно собрать стакан (можно указать несколько параметров)",
)
@click.option(
    "--feed",
    "feeds",
    multiple=True,
    default=("feed_A", "feed_B"),
    show_default=True,
    help="Приоритетный порядок чтения фидов",
)
@click.option(
    "--since",
    type=str,
    default=None,
    help="Отсекать события до этого времени (ISO-8601 или ns с эпохи)",
)
@click.option(
    "--progress/--no-progress",
    default=False,
    show_default=True,
    help="Показывать tqdm прогресс во время загрузки/реплея",
)
@click.option(
    "--csv/--parquet",
    "csv_output",
    default=False,
    show_default=True,
    help="Если указать --csv, book_* будут сохранены в CSV, иначе в Parquet",
)
@click.option(
    "--no-warnings",
    is_flag=True,
    default=False,
    help="Не выводить предупреждения из пайплайна",
)
@click.option(
    "--no-rpt-warnings",
    is_flag=True,
    default=False,
    help="Не логировать предупреждения о разрывах rptSeq",
)
@click.option("--verbose/--no-verbose", default=False, show_default=True)
def main(
    data_root: Path,
    output_root: Path,
    tickers: Sequence[str],
    feeds: Sequence[str],
    since: str | None,
    progress: bool,
    csv_output: bool,
    no_warnings: bool,
    no_rpt_warnings: bool,
    verbose: bool,
) -> None:
    """Построить сведённый стакан по декодированным parquet-файлам."""

    _configure_logging(verbose, suppress_warnings=no_warnings)
    since_ns = _parse_since(since)
    config = PipelineConfig(
        data_root=data_root,
        output_root=output_root,
        tickers=tickers,
        feeds=feeds,
        since_ns=since_ns,
        show_progress=progress,
        suppress_rpt_warnings=no_rpt_warnings,
    )
    universe = InstrumentUniverse.from_config(config)
    drop_fields = {"symbol"}
    output_manager = SnapshotOutputManager(
        tickers=config.tickers,
        output_root=output_root,
        csv_output=csv_output,
        drop_fields=drop_fields,
    )
    replayer = BookReplayer(config, universe)
    result = replayer.replay(snapshot_consumer=output_manager.handle_snapshot)
    output_manager.close()
    if result.stats.events_emitted == 0:
        click.echo("События с matchEventIndicator для указанных тикеров не найдены")
        return

    output_manager.emit_missing_warnings()
    click.echo("Готово")
    for path in output_manager.files_written:
        click.echo(f"  {path}")
    click.echo(
        f"Применено событий: {result.stats.events_emitted}, обновлений: {result.stats.order_updates}, "
        f"удалений: {result.stats.order_deletes}"
    )


class SnapshotOutputManager:
    def __init__(
        self,
        *,
        tickers: Sequence[str],
        output_root: Path,
        csv_output: bool,
        drop_fields: set[str],
    ) -> None:
        self._ticker_order = list(tickers)
        self._ticker_filter = {ticker for ticker in tickers}
        self._output_root = output_root
        self._csv_output = csv_output
        self._drop_fields = drop_fields
        self._order_sinks: dict[str, _BaseRowSink] = {}
        self._level_sinks: dict[str, _BaseRowSink] = {}
        self._bbo_sinks: dict[str, _BaseRowSink] = {}
        self.files_written: list[Path] = []

    def handle_snapshot(self, snapshot: BookEventSnapshot) -> None:
        ticker = snapshot.instrument.symbol
        if ticker not in self._ticker_filter:
            return
        order_rows = _filter_and_clean_rows(
            iter_snapshot_order_rows(snapshot), ticker, self._drop_fields
        )
        self._write_rows(ticker, order_rows, kind="orders")
        level_rows = _filter_and_clean_rows(
            iter_snapshot_level_rows(snapshot), ticker, self._drop_fields
        )
        self._write_rows(ticker, level_rows, kind="levels")
        bbo_rows = _filter_and_clean_rows(
            iter_snapshot_bbo_rows(snapshot), ticker, self._drop_fields
        )
        self._write_rows(ticker, bbo_rows, kind="bbo")

    def emit_missing_warnings(self) -> None:
        for ticker in self._ticker_order:
            order_path = self._path_for(ticker, kind="orders")
            level_path = self._path_for(ticker, kind="levels")
            bbo_path = self._path_for(ticker, kind="bbo")
            if ticker not in self._order_sinks:
                LOGGER.warning("Нет данных для %s, файл пропущен", order_path.name)
            if ticker not in self._level_sinks:
                LOGGER.warning("Нет данных для %s, файл пропущен", level_path.name)
            if ticker not in self._bbo_sinks:
                LOGGER.warning("Нет данных для %s, файл пропущен", bbo_path.name)

    def close(self) -> None:
        for sink in (
            list(self._order_sinks.values())
            + list(self._level_sinks.values())
            + list(self._bbo_sinks.values())
        ):
            sink.close()

    def _write_rows(self, ticker: str, rows: list[dict], *, kind: str) -> None:
        if not rows:
            return
        sink = self._ensure_sink(ticker, kind)
        created_now = sink.write_rows(rows)
        if created_now:
            self.files_written.append(sink.path)

    def _ensure_sink(self, ticker: str, kind: str) -> "_BaseRowSink":
        storages = {
            "orders": self._order_sinks,
            "levels": self._level_sinks,
            "bbo": self._bbo_sinks,
        }
        try:
            storage = storages[kind]
        except KeyError as err:
            raise ValueError(f"Unknown sink kind: {kind}") from err
        sink = storage.get(ticker)
        if sink is None:
            path = self._path_for(ticker, kind=kind)
            sink = _CsvRowSink(path) if self._csv_output else _ParquetRowSink(path)
            storage[ticker] = sink
        return sink

    def _path_for(self, ticker: str, *, kind: str) -> Path:
        folder = self._output_root / ticker
        suffix = "csv" if self._csv_output else "parquet"
        names = {
            "orders": "book_l3",
            "levels": "book_l2",
            "bbo": "book_l1",
        }
        try:
            name = names[kind]
        except KeyError as err:
            raise ValueError(f"Unknown sink kind: {kind}") from err
        return folder / f"{name}.{suffix}"


class _BaseRowSink:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._created = False

    def write_rows(self, rows: Sequence[dict]) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class _CsvRowSink(_BaseRowSink):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._fieldnames: list[str] | None = None
        self._fh: TextIO | None = None
        self._writer: csv.DictWriter | None = None

    def write_rows(self, rows: Sequence[dict]) -> bool:
        if not rows:
            return False
        created_now = False
        if self._fh is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fieldnames = list(rows[0].keys())
            self._fh = self.path.open("w", newline="", encoding="utf-8")
            self._writer = csv.DictWriter(self._fh, fieldnames=self._fieldnames)
            self._writer.writeheader()
            self._created = True
            created_now = True
        assert self._writer is not None
        self._writer.writerows(rows)
        return created_now

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


class _ParquetRowSink(_BaseRowSink):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._writer: pq.ParquetWriter | None = None

    def write_rows(self, rows: Sequence[dict]) -> bool:
        if not rows:
            return False
        table = pa.Table.from_pylist(rows)
        created_now = False
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self.path, table.schema)
            self._created = True
            created_now = True
        self._writer.write_table(table)
        return created_now

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None


def _filter_and_clean_rows(rows: Iterable[dict], ticker: str, drop_fields: set[str]) -> list[dict]:
    cleaned: list[dict] = []
    for row in rows:
        if row.get("symbol") != ticker:
            continue
        new_row = dict(row)
        for field in drop_fields:
            new_row.pop(field, None)
        side_value = new_row.get("side")
        if side_value:
            normalized = side_value.strip().upper()
            if normalized in {"OFFER", "SELL"}:
                new_row["side"] = "ASK"
            elif normalized in {"BID", "BUY"}:
                new_row["side"] = "BID"
        cleaned.append(new_row)
    return cleaned


def _parse_since(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.isdigit():
        return int(stripped)
    try:
        dt = datetime.fromisoformat(stripped)
    except ValueError as err:  # noqa: TRY003 - convert into click error
        raise click.BadParameter(
            "Ожидался ISO-8601 (`2024-11-18T12:34:00`) или число наносекунд"
        ) from err
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


if __name__ == "__main__":
    main()
