from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Sequence

from .instruments import InstrumentUniverse
from .replay import BookEventSnapshot


def build_event_order_rows(
    snapshots: Sequence[BookEventSnapshot], universe: InstrumentUniverse
) -> List[dict]:
    del universe  # snapshots already carry instrument metadata
    rows: List[dict] = []
    for snapshot in snapshots:
        rows.extend(iter_snapshot_order_rows(snapshot))
    return rows


def build_event_level_rows(
    snapshots: Sequence[BookEventSnapshot], universe: InstrumentUniverse
) -> List[dict]:
    del universe
    rows: List[dict] = []
    for snapshot in snapshots:
        rows.extend(iter_snapshot_level_rows(snapshot))
    return rows


def build_bbo_rows(
    snapshots: Sequence[BookEventSnapshot], universe: InstrumentUniverse
) -> List[dict]:
    del universe
    rows: List[dict] = []
    for snapshot in snapshots:
        rows.extend(iter_snapshot_bbo_rows(snapshot))
    return rows


def iter_snapshot_order_rows(snapshot: BookEventSnapshot) -> Iterator[dict]:
    base = _snapshot_row_base(snapshot)
    yield from _rows_for_side(snapshot.bids, "BID", base)
    yield from _rows_for_side(snapshot.asks, "ASK", base)


def iter_snapshot_level_rows(snapshot: BookEventSnapshot) -> Iterator[dict]:
    base = _snapshot_row_base(snapshot)
    yield from _levels_for_side(snapshot.bids, "BID", base)
    yield from _levels_for_side(snapshot.asks, "ASK", base)


def iter_snapshot_bbo_rows(snapshot: BookEventSnapshot) -> Iterator[dict]:
    base = _snapshot_row_base(snapshot)
    bid = snapshot.bids[0] if snapshot.bids else None
    ask = snapshot.asks[0] if snapshot.asks else None
    row = dict(base)
    row.update(
        {
            "bid_price": bid.price if bid else None,
            "bid_size": bid.size if bid else None,
            "ask_price": ask.price if ask else None,
            "ask_size": ask.size if ask else None,
        }
    )
    yield row


def _rows_for_side(orders, side: str, base: Mapping[str, object]) -> Iterator[dict]:
    for position_no, order in enumerate(orders, start=1):
        row = dict(base)
        row.update(
            {
                "side": side,
                "position_no": position_no,
                "price": order.price,
                "size": order.size,
                "secondary_order_id": order.secondary_order_id,
                "md_entry_timestamp_ns": order.timestamp_ns,
                "order_packet_sequence_number": order.packet_sequence_number,
            }
        )
        yield row


def _levels_for_side(orders, side: str, base: Mapping[str, object]) -> Iterator[dict]:
    aggregated: List[dict] = []
    current_price = None
    for order in orders:
        if current_price is None or order.price != current_price:
            aggregated.append({"price": order.price, "size": order.size, "order_count": 1})
            current_price = order.price
        else:
            aggregated[-1]["size"] += order.size
            aggregated[-1]["order_count"] += 1
    for level_index, level in enumerate(aggregated, start=1):
        row = dict(base)
        row.update(
            {
                "side": side,
                "level_index": level_index,
                "price": level["price"],
                "size": level["size"],
                "order_count": level["order_count"],
            }
        )
        yield row


def _snapshot_row_base(snapshot: BookEventSnapshot) -> Mapping[str, object]:
    return {
        "symbol": snapshot.instrument.symbol,
        "event_index": snapshot.event_index,
        "packet_sequence_number": snapshot.packet_sequence_number,
        "packet_sending_time_ns": snapshot.packet_sending_time_ns,
    }
