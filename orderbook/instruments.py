from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

import pyarrow.dataset as ds

from .config import PipelineConfig


@dataclass(frozen=True)
class InstrumentRecord:
    channel: str
    security_id: int
    symbol: str
    min_price_increment: float
    price_divisor: float

    @property
    def book_symbol(self) -> str:
        return self.symbol.upper()


class InstrumentUniverse:
    """Holds instrument metadata filtered to the requested ticker set."""

    def __init__(self, instruments: Sequence[InstrumentRecord]):
        if not instruments:
            raise ValueError("Instrument universe cannot be empty")
        self._instruments = instruments
        self._by_symbol: Dict[str, List[InstrumentRecord]] = {}
        self._by_security_id: Dict[int, InstrumentRecord] = {}
        for instrument in instruments:
            self._by_symbol.setdefault(instrument.book_symbol, []).append(instrument)
            self._by_security_id[instrument.security_id] = instrument

    @property
    def instruments(self) -> Sequence[InstrumentRecord]:
        return self._instruments

    @property
    def channels(self) -> Sequence[str]:
        return sorted({instrument.channel for instrument in self._instruments})

    def for_symbol(self, symbol: str) -> Sequence[InstrumentRecord]:
        return self._by_symbol.get(symbol.upper(), ())

    def for_security_id(self, security_id: int) -> InstrumentRecord | None:
        return self._by_security_id.get(security_id)

    def by_symbol(self) -> Mapping[str, Sequence[InstrumentRecord]]:
        return self._by_symbol

    def by_security_id(self) -> Mapping[int, InstrumentRecord]:
        return self._by_security_id

    def security_ids_by_channel(self) -> Mapping[str, List[int]]:
        channel_map: Dict[str, List[int]] = {}
        for instrument in self._instruments:
            channel_map.setdefault(instrument.channel, []).append(instrument.security_id)
        return channel_map

    @classmethod
    def from_config(cls, config: PipelineConfig) -> "InstrumentUniverse":
        records: List[InstrumentRecord] = []
        for channel_path in _iter_channels(config.data_root, config.channels):
            table = _load_instruments_table(channel_path, config.tickers)
            if table is None:
                continue
            channel_name = channel_path.name
            security_ids = table.column("security_id").to_pylist()
            symbols = table.column("symbol").to_pylist()
            increments = table.column("min_price_increment").to_pylist()
            divisors = table.column("price_divisor").to_pylist()
            for security_id, symbol, inc, divisor in zip(
                security_ids, symbols, increments, divisors, strict=True
            ):
                records.append(
                    InstrumentRecord(
                        channel=channel_name,
                        security_id=int(security_id),
                        symbol=str(symbol),
                        min_price_increment=float(inc),
                        price_divisor=float(divisor),
                    )
                )
        missing = _missing_tickers(records, config.tickers)
        if missing:
            raise ValueError(
                "Tickers not found in any channel: " + ", ".join(sorted(missing))
            )
        return cls(records)


def _iter_channels(root: Path, allowlist: Sequence[str] | None):
    if allowlist is not None:
        for name in allowlist:
            path = root / name
            if not path.is_dir():
                raise FileNotFoundError(f"Channel directory '{name}' not found in {root}")
            yield path
        return
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("channel_"):
            yield entry


def _load_instruments_table(channel_path: Path, tickers: Sequence[str]):
    file_path = channel_path / "instruments.parquet"
    if not file_path.exists():
        raise FileNotFoundError(f"Missing instruments parquet in {channel_path}")
    dataset = ds.dataset(str(file_path), format="parquet")
    symbol_filter = ds.field("symbol").isin(tickers)
    table = dataset.to_table(
        filter=symbol_filter,
        columns=["security_id", "symbol", "min_price_increment", "price_divisor"],
    )
    if table.num_rows == 0:
        return None
    return table


def _missing_tickers(records: Iterable[InstrumentRecord], requested: Sequence[str]) -> set[str]:
    present = {record.book_symbol for record in records}
    missing = {symbol for symbol in requested if symbol.upper() not in present}
    return missing
