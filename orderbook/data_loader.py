from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

import pyarrow.dataset as ds
from pyarrow import RecordBatch, Table

from .config import PipelineConfig
from .progress import iter_progress
from .instruments import InstrumentUniverse
from .models import (
    BookSide,
    ChannelResetMessage,
    EmptyBookMessage,
    IncrementalMessage,
    NonBookMessage,
    OrderDeleteMessage,
    OrderMassDeleteMessage,
    OrderUpdateMessage,
    SequenceResetMessage,
    SnapshotBook,
    SnapshotHeader,
    SnapshotOrder,
    TradeMessage,
)

LOGGER = logging.getLogger(__name__)


SnapshotKey = Tuple[str, int]
EventKey = Tuple[str, str]


def _event_sort_key(message: IncrementalMessage) -> Tuple[int, int, int, int]:
    version = message.packet_sequence_version if message.packet_sequence_version is not None else -1
    message_index = message.message_index_in_packet if message.message_index_in_packet is not None else -1
    timestamp = message.md_entry_timestamp_ns if message.md_entry_timestamp_ns is not None else 0
    return (version, message.packet_sequence_number, message_index, timestamp)


class DataLoader:
    """Reads filtered Parquet tables and emits typed snapshot/event structures."""

    def __init__(self, config: PipelineConfig, universe: InstrumentUniverse):
        self.config = config
        self.universe = universe
        self.since_ns = config.since_ns

    def load_snapshots(self) -> Dict[SnapshotKey, SnapshotBook]:
        books: Dict[SnapshotKey, SnapshotBook] = {}
        channel_items = list(self.universe.security_ids_by_channel().items())
        for channel, security_ids in iter_progress(
            channel_items,
            enabled=self.config.show_progress,
            desc="Snapshots",
            total=len(channel_items) if channel_items else None,
        ):
            channel_path = self.config.data_root / channel
            if not channel_path.exists():
                LOGGER.warning("Channel directory %s is missing", channel_path)
                continue
            header_table = _read_table(
                channel_path / "snapshot_headers.parquet",
                columns=[
                    "security_id",
                    "packet_sequence_number",
                    "packet_sequence_version",
                    "packet_sending_time_ns",
                    "last_msg_seq_num_processed",
                    "tot_num_bids",
                    "tot_num_offers",
                    "last_rpt_seq",
                    "snapshot_header_row_id",
                ],
                filter_expr=ds.field("security_id").isin(security_ids),
            )
            if header_table is None or header_table.num_rows == 0:
                continue
            selected_headers = _pick_headers(channel, header_table, self.since_ns)
            if not selected_headers:
                continue
            header_ids = {header.snapshot_header_row_id for header in selected_headers.values()}
            order_table = _read_table(
                channel_path / "snapshot_orders.parquet",
                columns=[
                    "snapshot_header_row_id",
                    "security_id",
                    "side",
                    "price",
                    "size",
                    "position_no",
                    "secondary_order_id",
                    "md_insert_timestamp_ns",
                ],
                filter_expr=ds.field("snapshot_header_row_id").isin(header_ids),
            )
            channel_books = _build_snapshot_books(channel, selected_headers, order_table)
            books.update(channel_books)
        return books

    def load_incremental_events(self) -> Dict[EventKey, List[IncrementalMessage]]:
        events: Dict[EventKey, List[IncrementalMessage]] = {}
        security_map = self.universe.security_ids_by_channel()
        channel_items = list(security_map.items())
        for channel, security_ids in iter_progress(
            channel_items,
            enabled=self.config.show_progress,
            desc="Incremental",
            total=len(channel_items) if channel_items else None,
        ):
            channel_path = self.config.data_root / channel
            for feed in self.config.feeds:
                feed_path = channel_path / feed
                if not feed_path.is_dir():
                    LOGGER.warning("Feed %s missing in %s", feed, channel_path)
                    continue
                feed_events: List[IncrementalMessage] = []
                feed_events.extend(
                    _load_order_updates(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                feed_events.extend(
                    _load_order_deletes(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                feed_events.extend(
                    _load_mass_deletes(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                feed_events.extend(
                    _load_trades(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                has_empty_books_file = (feed_path / "incremental_empty_books.parquet").exists()
                has_channel_reset_file = (feed_path / "incremental_channel_resets.parquet").exists()
                feed_events.extend(
                    _load_empty_books(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                feed_events.extend(
                    _load_channel_resets(
                        feed_path,
                        channel,
                        feed,
                        batch_size=self.config.batch_size,
                        since_ns=self.since_ns,
                    )
                )
                feed_events.extend(
                    _load_other_messages(
                        feed_path,
                        channel,
                        feed,
                        security_ids,
                        include_empty_book=not has_empty_books_file,
                        include_channel_reset=not has_channel_reset_file,
                        since_ns=self.since_ns,
                        batch_size=self.config.batch_size,
                    )
                )
                if not feed_events:
                    continue
                feed_events.sort(key=_event_sort_key)
                events[(channel, feed)] = feed_events
        return events


def _read_table(
    file_path: Path,
    *,
    columns: Sequence[str],
    filter_expr,
    batch_size: int | None = None,
) -> Table | None:
    if not file_path.exists():
        LOGGER.debug("Skipping absent parquet %s", file_path)
        return None
    dataset = ds.dataset(str(file_path), format="parquet")
    try:
        projected = _project_columns(dataset, columns)
        return dataset.to_table(columns=projected, filter=filter_expr)
    except FileNotFoundError:
        LOGGER.warning("Parquet file %s disappeared during read", file_path)
        return None


def _pick_headers(channel: str, table: Table, since_ns: int | None) -> Dict[int, SnapshotHeader]:
    selected: Dict[int, SnapshotHeader] = {}
    earliest: Dict[int, SnapshotHeader] = {}
    if table is None:
        return selected
    security_ids = _column_values(table, "security_id")
    packet_numbers = _column_values(table, "packet_sequence_number")
    packet_versions = _column_values(table, "packet_sequence_version")
    sending_times = _column_values(table, "packet_sending_time_ns")
    last_msgs = _column_values(table, "last_msg_seq_num_processed")
    bids = _column_values(table, "tot_num_bids")
    offers = _column_values(table, "tot_num_offers")
    last_rpts = _column_values(table, "last_rpt_seq")
    header_ids = _column_values(table, "snapshot_header_row_id")
    for idx in range(table.num_rows):
        security_id = int(security_ids[idx])
        candidate = SnapshotHeader(
            channel=channel,
            security_id=security_id,
            packet_sequence_number=int(packet_numbers[idx]),
            packet_sequence_version=
                int(packet_versions[idx]) if packet_versions[idx] is not None else None,
            packet_sending_time_ns=int(sending_times[idx]),
            last_msg_seq_num_processed=int(last_msgs[idx]),
            tot_num_bids=int(bids[idx]),
            tot_num_offers=int(offers[idx]),
            last_rpt_seq=int(last_rpts[idx]),
            snapshot_header_row_id=int(header_ids[idx]),
        )
        current_earliest = earliest.get(security_id)
        if current_earliest is None or candidate.packet_sending_time_ns < current_earliest.packet_sending_time_ns:
            earliest[security_id] = candidate

        if since_ns is None:
            continue
        if candidate.packet_sending_time_ns > since_ns:
            continue
        current_selected = selected.get(security_id)
        if current_selected is None or candidate.packet_sending_time_ns > current_selected.packet_sending_time_ns:
            selected[security_id] = candidate

    if since_ns is None:
        return earliest
    for sec_id, header in earliest.items():
        selected.setdefault(sec_id, header)
    return selected


def _build_snapshot_books(
    channel: str,
    headers: Dict[int, SnapshotHeader],
    order_table: Table | None,
) -> Dict[SnapshotKey, SnapshotBook]:
    books = {(channel, sec_id): SnapshotBook(header=header) for sec_id, header in headers.items()}
    if order_table is None or order_table.num_rows == 0:
        return books
    header_row_ids = _column_values(order_table, "snapshot_header_row_id")
    security_ids = _column_values(order_table, "security_id")
    sides = _column_values(order_table, "side")
    prices = _column_values(order_table, "price")
    sizes = _column_values(order_table, "size")
    positions = _column_values(order_table, "position_no")
    order_ids = _column_values(order_table, "secondary_order_id")
    timestamps = _column_values(order_table, "md_insert_timestamp_ns")
    for idx in range(order_table.num_rows):
        header_row_id = int(header_row_ids[idx])
        security_id = int(security_ids[idx])
        side = BookSide.from_string(sides[idx])
        key = (channel, security_id)
        book = books.get(key)
        if book is None:
            continue
        if book.header.snapshot_header_row_id != header_row_id:
            continue
        order = SnapshotOrder(
            channel=channel,
            security_id=security_id,
            side=side,
            price=float(prices[idx]),
            size=int(sizes[idx]),
            position_no=int(positions[idx]),
            secondary_order_id=int(order_ids[idx]),
            md_insert_timestamp_ns=int(timestamps[idx]),
        )
        if side is BookSide.BID:
            book.bids.append(order)
        else:
            book.asks.append(order)
    for book in books.values():
        book.bids.sort(key=lambda order: order.position_no)
        book.asks.sort(key=lambda order: order.position_no)
    return books


def _load_order_updates(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    if not security_ids:
        return []
    file_path = feed_path / "incremental_orders.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "security_id",
            "packet_sequence_version",
            "match_event_indicator_raw",
            "md_update_action",
            "side",
            "price",
            "size",
            "position_no",
            "secondary_order_id",
            "rpt_seq",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=ds.field("security_id").isin(security_ids),
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        security_col = _column_values(batch, "security_id")
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        action_col = _column_values(batch, "md_update_action")
        side_col = _column_values(batch, "side")
        price_col = _column_values(batch, "price")
        size_col = _column_values(batch, "size")
        pos_col = _column_values(batch, "position_no")
        order_col = _column_values(batch, "secondary_order_id")
        rpt_col = _column_values(batch, "rpt_seq")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            events.append(
                OrderUpdateMessage(
                    channel=channel,
                    feed=feed,
                    security_id=int(security_col[idx]),
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=int(rpt_col[idx]) if rpt_col[idx] is not None else None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                    side=BookSide.from_string(side_col[idx]),
                    position_no=int(pos_col[idx]),
                    price=float(price_col[idx]),
                    size=int(size_col[idx]),
                    secondary_order_id=int(order_col[idx]),
                    md_update_action=str(action_col[idx]).upper(),
                )
            )
    return events


def _load_order_deletes(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    if not security_ids:
        return []
    file_path = feed_path / "incremental_deletes.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "security_id",
            "packet_sequence_version",
            "match_event_indicator_raw",
            "side",
            "position_no",
            "size",
            "secondary_order_id",
            "md_entry_timestamp_ns",
            "rpt_seq",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=ds.field("security_id").isin(security_ids),
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        security_col = _column_values(batch, "security_id")
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        side_col = _column_values(batch, "side")
        pos_col = _column_values(batch, "position_no")
        size_col = _column_values(batch, "size")
        order_col = _column_values(batch, "secondary_order_id")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        rpt_col = _column_values(batch, "rpt_seq")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            events.append(
                OrderDeleteMessage(
                    channel=channel,
                    feed=feed,
                    security_id=int(security_col[idx]),
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=int(rpt_col[idx]) if rpt_col[idx] is not None else None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                    side=BookSide.from_string(side_col[idx]),
                    position_no=int(pos_col[idx]),
                    size=int(size_col[idx]) if size_col[idx] is not None else 0,
                    secondary_order_id=int(order_col[idx]),
                )
            )
    return events


def _load_mass_deletes(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    if not security_ids:
        return []
    file_path = feed_path / "incremental_mass_deletes.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "security_id",
            "packet_sequence_version",
            "match_event_indicator_raw",
            "md_update_action",
            "side",
            "position_no",
            "rpt_seq",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=ds.field("security_id").isin(security_ids),
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        security_col = _column_values(batch, "security_id")
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        action_col = _column_values(batch, "md_update_action")
        side_col = _column_values(batch, "side")
        position_col = _column_values(batch, "position_no")
        rpt_col = _column_values(batch, "rpt_seq")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            position_value = int(position_col[idx]) if position_col[idx] is not None else 0
            action_text = str(action_col[idx]).upper() if action_col[idx] is not None else ""
            events.append(
                OrderMassDeleteMessage(
                    channel=channel,
                    feed=feed,
                    security_id=int(security_col[idx]),
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=int(rpt_col[idx]) if rpt_col[idx] is not None else None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                    side=BookSide.from_string(side_col[idx]),
                    position_no=position_value,
                    md_update_action=action_text,
                )
            )
    return events


def _load_trades(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    if not security_ids:
        return []
    file_path = feed_path / "incremental_trades.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "security_id",
            "packet_sequence_version",
            "match_event_indicator_raw",
            "price",
            "size",
            "trade_id",
            "md_entry_timestamp_ns",
            "rpt_seq",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=ds.field("security_id").isin(security_ids),
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        security_col = _column_values(batch, "security_id")
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        price_col = _column_values(batch, "price")
        size_col = _column_values(batch, "size")
        trade_col = _column_values(batch, "trade_id")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        rpt_col = _column_values(batch, "rpt_seq")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            events.append(
                TradeMessage(
                    channel=channel,
                    feed=feed,
                    security_id=int(security_col[idx]),
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=int(rpt_col[idx]) if rpt_col[idx] is not None else None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                    price=float(price_col[idx]),
                    size=int(size_col[idx]),
                    trade_id=int(trade_col[idx]),
                )
            )
    return events


def _load_empty_books(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    if not security_ids:
        return []
    file_path = feed_path / "incremental_empty_books.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "security_id",
            "packet_sequence_version",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=ds.field("security_id").isin(security_ids),
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        security_col = _column_values(batch, "security_id")
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            events.append(
                EmptyBookMessage(
                    channel=channel,
                    feed=feed,
                    security_id=int(security_col[idx]),
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                )
            )
    return events


def _load_channel_resets(
    feed_path: Path,
    channel: str,
    feed: str,
    *,
    batch_size: int,
    since_ns: int | None,
) -> List[IncrementalMessage]:
    file_path = feed_path / "incremental_channel_resets.parquet"
    batches = _iter_batches(
        file_path,
        columns=[
            "packet_sequence_version",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sending_time_ns",
            "message_index_in_packet",
        ],
        filter_expr=None,
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        version_col = _column_values(batch, "packet_sequence_version")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        pkt_col = _column_values(batch, "packet_sequence_number")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        for idx in range(batch.num_rows):
            timestamp = int(ts_col[idx]) if ts_col[idx] is not None else None
            events.append(
                ChannelResetMessage(
                    channel=channel,
                    feed=feed,
                    security_id=0,
                    packet_sequence_number=int(pkt_col[idx]),
                    packet_sequence_version=
                        int(version_col[idx]) if version_col[idx] is not None else None,
                    packet_sending_time_ns=int(send_ts_col[idx]),
                    match_event_indicator_raw=int(mei_col[idx]),
                    rpt_seq=None,
                    md_entry_timestamp_ns=timestamp,
                    message_index_in_packet=
                        int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
                )
            )
    return events


def _load_other_messages(
    feed_path: Path,
    channel: str,
    feed: str,
    security_ids: Sequence[int],
    *,
    include_empty_book: bool,
    include_channel_reset: bool,
    since_ns: int | None,
    batch_size: int,
) -> List[IncrementalMessage]:
    file_path = feed_path / "incremental_other.parquet"
    filter_expr = None
    special_templates = [1]
    if include_empty_book:
        special_templates.append(9)
    if include_channel_reset:
        special_templates.append(11)
    if security_ids:
        filter_ids = sorted(int(sec_id) for sec_id in security_ids)
        filter_expr = ds.field("security_id").isin(filter_ids) | ds.field("template_id").isin(
            special_templates
        )
    else:
        filter_expr = ds.field("template_id").isin(special_templates)
    batches = _iter_batches(
        file_path,
        columns=[
            "template_id",
            "template_name",
            "security_id",
            "rpt_seq",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sequence_version",
            "packet_sending_time_ns",
            "message_index_in_packet",
            "body_hex",
            "decode_status",
        ],
        filter_expr=filter_expr,
        batch_size=batch_size,
    )
    events: List[IncrementalMessage] = []
    for batch in batches:
        template_col = _column_values(batch, "template_id")
        name_col = _column_values(batch, "template_name")
        security_col = _column_values(batch, "security_id")
        rpt_col = _column_values(batch, "rpt_seq")
        mei_col = _column_values(batch, "match_event_indicator_raw")
        ts_col = _column_values(batch, "md_entry_timestamp_ns")
        pkt_col = _column_values(batch, "packet_sequence_number")
        version_col = _column_values(batch, "packet_sequence_version")
        send_ts_col = _column_values(batch, "packet_sending_time_ns")
        msg_idx_col = _column_values(batch, "message_index_in_packet")
        body_col = _column_values(batch, "body_hex")
        for idx in range(batch.num_rows):
            template_id = int(template_col[idx])
            raw_security = security_col[idx]
            security_id = int(raw_security) if raw_security is not None else 0
            base_kwargs = dict(
                channel=channel,
                feed=feed,
                security_id=security_id,
                packet_sequence_number=int(pkt_col[idx]),
                packet_sequence_version=
                    int(version_col[idx]) if version_col[idx] is not None else None,
                packet_sending_time_ns=int(send_ts_col[idx]),
                match_event_indicator_raw=int(mei_col[idx]),
                rpt_seq=int(rpt_col[idx]) if rpt_col[idx] is not None else None,
                md_entry_timestamp_ns=int(ts_col[idx]) if ts_col[idx] is not None else None,
                message_index_in_packet=
                    int(msg_idx_col[idx]) if msg_idx_col[idx] is not None else None,
            )
            if template_id == 1:
                events.append(
                    SequenceResetMessage(
                        **base_kwargs,
                        sequence_version=None,
                    )
                )
            elif template_id == 11 and include_channel_reset:
                events.append(ChannelResetMessage(**base_kwargs))
            elif template_id == 9 and include_empty_book:
                events.append(EmptyBookMessage(**base_kwargs))
            else:
                events.append(
                    NonBookMessage(
                        **base_kwargs,
                        template_id=template_id,
                        template_name=str(name_col[idx]),
                        body_hex=str(body_col[idx]),
                    )
                )
    return events


def _iter_batches(
    file_path: Path,
    *,
    columns: Sequence[str],
    filter_expr,
    batch_size: int,
) -> Iterator[RecordBatch]:
    if not file_path.exists():
        return iter(())
    dataset = ds.dataset(str(file_path), format="parquet")
    table = dataset.to_table(columns=_project_columns(dataset, columns), filter=filter_expr)
    if table is None or table.num_rows == 0:
        return iter(())
    return iter(table.to_batches(max_chunksize=batch_size))


def _project_columns(dataset, columns: Sequence[str]) -> Sequence[str] | None:
    if not columns:
        return None
    available = set(dataset.schema.names)
    selected = [name for name in columns if name in available]
    return selected or None


def _column_values(container: Table | RecordBatch, name: str, default=None) -> List:
    idx = container.schema.get_field_index(name)
    if idx == -1:
        return [default] * container.num_rows
    return container.column(idx).to_pylist()
