"""Сборка полной таблицы инструментов из output'а декодера."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Mapping, Sequence

import click
import pandas as pd

LOGGER = logging.getLogger(__name__)


@click.command()
@click.option(
    "--data-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("data/parsed"),
    show_default=True,
    help="Каталог с parquet-таблицами (по умолчанию data/parsed)",
)
@click.option(
    "--channel",
    "channels",
    multiple=True,
    help="Ограничить набор каналов (можно несколько параметров)",
)
@click.option(
    "--ticker",
    "tickers",
    multiple=True,
    help="Ограничить результат заданными тикерами (без фильтра собираются все)",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Путь для сохранения таблицы (.parquet/.csv/.json). Если не указан — только предпросмотр",
)
@click.option(
    "--preview-rows",
    type=click.IntRange(min=0),
    default=5,
    show_default=True,
    help="Сколько строк показать в stdout после сборки",
)
@click.option("--verbose/--quiet", default=False, show_default=True)
def main(
    data_root: Path,
    channels: Sequence[str],
    tickers: Sequence[str],
    output: Path | None,
    preview_rows: int,
    verbose: bool,
) -> None:
    """Построить агрегированную pandas-таблицу по всем instrument_* файлам."""

    _configure_logging(verbose)
    table = collect_instruments(
        data_root=data_root,
        channels=list(channels) or None,
        tickers=list(tickers) or None,
    )
    if table.empty:
        click.echo("Не найдено ни одного инструмента по заданным фильтрам")
        return

    click.echo(
        f"Собрано {len(table)} инструментов в {table['channel'].nunique()} каналах; "
        f"тикеров: {table['symbol'].nunique()}"
    )
    if output is not None:
        _export_table(table, output)
        click.echo(f"Таблица сохранена в {output}")

    if preview_rows > 0:
        click.echo("\nПредпросмотр:")
        with pd.option_context("display.max_columns", None, "display.width", 200):
            click.echo(table.head(preview_rows).to_string(index=False))


def collect_instruments(
    *, data_root: Path, channels: Sequence[str] | None, tickers: Sequence[str] | None
) -> pd.DataFrame:
    channel_dirs = _resolve_channels(data_root, channels)
    ticker_filter = {ticker.upper() for ticker in tickers} if tickers else None
    frames: List[pd.DataFrame] = []
    for channel_dir in channel_dirs:
        channel_frame = _load_channel(channel_dir, ticker_filter)
        if channel_frame is None or channel_frame.empty:
            continue
        frames.append(channel_frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_channels(root: Path, channels: Sequence[str] | None) -> List[Path]:
    if channels:
        resolved = []
        for name in channels:
            path = root / name
            if not path.is_dir():
                raise click.ClickException(f"Канал {name} не найден в {root}")
            resolved.append(path)
        return resolved
    resolved = [p for p in sorted(root.iterdir()) if p.is_dir() and p.name.startswith("channel_")]
    if not resolved:
        raise click.ClickException(f"В {root} не найдены каталоги channel_*")
    return resolved


def _load_channel(channel_dir: Path, ticker_filter: set[str] | None) -> pd.DataFrame | None:
    base_path = channel_dir / "instruments.parquet"
    if not base_path.exists():
        LOGGER.warning("Пропускаю %s: нет instruments.parquet", channel_dir.name)
        return None
    df = pd.read_parquet(base_path)
    if df.empty:
        LOGGER.info("%s/instruments.parquet пустой", channel_dir)
        return df
    if ticker_filter:
        symbols = df["symbol"].fillna("").str.upper()
        df = df[symbols.isin(ticker_filter)].copy()
        if df.empty:
            LOGGER.info("В %s нет тикеров из фильтра", channel_dir)
            return df
    df = _deduplicate_instruments(df)
    df.insert(0, "channel", channel_dir.name)
    latest_packets = df.set_index("security_id")["packet_sequence_number"].to_dict()

    underlyings = _load_repeating_group(
        channel_dir / "instrument_underlyings.parquet",
        latest_packets,
        sort_fields=["position_in_group"],
        dedup_fields=["position_in_group"],
        value_fields=[
            "underlying_security_id",
            "underlying_security_exchange",
            "underlying_symbol",
            "underlying_security_type",
            "position_in_group",
        ],
    )
    legs = _load_repeating_group(
        channel_dir / "instrument_legs.parquet",
        latest_packets,
        sort_fields=["position_in_group"],
        dedup_fields=["position_in_group"],
        value_fields=[
            "leg_security_id",
            "leg_symbol",
            "leg_side",
            "leg_side_raw",
            "leg_ratio_qty_mantissa",
            "leg_ratio_qty",
            "position_in_group",
        ],
    )
    attributes = _load_repeating_group(
        channel_dir / "instrument_attributes.parquet",
        latest_packets,
        sort_fields=["position_in_group"],
        dedup_fields=["position_in_group"],
        value_fields=["attribute_type", "attribute_value", "position_in_group"],
    )
    df["underlyings"] = df["security_id"].map(lambda sid: underlyings.get(int(sid), []))
    df["legs"] = df["security_id"].map(lambda sid: legs.get(int(sid), []))
    df["attributes"] = df["security_id"].map(lambda sid: attributes.get(int(sid), []))
    return df


def _deduplicate_instruments(df: pd.DataFrame) -> pd.DataFrame:
    sort_fields = [
        "security_id",
        "packet_sequence_number",
        "packet_sending_time_ns",
    ]
    present_fields = [field for field in sort_fields if field in df.columns]
    sorted_df = df.sort_values(present_fields, kind="mergesort") if present_fields else df
    return sorted_df.drop_duplicates("security_id", keep="last")


def _load_repeating_group(
    path: Path,
    latest_packets: Mapping[int, int],
    *,
    sort_fields: Sequence[str],
    dedup_fields: Sequence[str],
    value_fields: Sequence[str],
) -> Dict[int, List[Dict]]:
    if not path.exists():
        LOGGER.info("Файл %s отсутствует", path.name)
        return {}
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    packet_ids = set(latest_packets.keys())
    df = df[df["security_id"].isin(packet_ids)]
    if df.empty:
        return {}
    df = df.copy()
    df["target_packet"] = df["security_id"].map(latest_packets)
    df = df[df["packet_sequence_number"] == df["target_packet"]]
    if df.empty:
        return {}
    df = df.drop(columns=["target_packet"])
    dedup_on = ["security_id", *dedup_fields]
    df = df.drop_duplicates(dedup_on, keep="last")
    sort_order = ["security_id", *sort_fields]
    df = df.sort_values(sort_order, kind="mergesort")
    grouped: Dict[int, List[Dict]] = {}
    for security_id, group in df.groupby("security_id", sort=False):
        grouped[int(security_id)] = group[list(value_fields)].to_dict("records")
    return grouped


def _export_table(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        df.to_parquet(output_path, index=False)
    elif suffix == ".csv":
        df.to_csv(output_path, index=False)
    elif suffix == ".json":
        output_path.write_text(
            df.to_json(orient="records", force_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        raise click.ClickException(
            "Неподдерживаемый формат вывода: используйте расширение .parquet/.csv/.json"
        )


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


if __name__ == "__main__":
    main()
