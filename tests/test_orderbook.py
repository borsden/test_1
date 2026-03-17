from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from orderbook.data_loader import _pick_headers
from orderbook.instruments import InstrumentRecord
from orderbook.models import (
    BookSide,
    OrderDeleteMessage,
    OrderMassDeleteMessage,
    OrderUpdateMessage,
    SnapshotBook,
    SnapshotHeader,
    SnapshotOrder,
)
from orderbook.order_book import OrderBook, OrderState
from orderbook.output_builder import build_bbo_rows
from orderbook.replay import BookEventSnapshot, EventRecorder, ReplayStats
from scripts.build_orderbook import SnapshotOutputManager, _parse_since


def _snapshot_book(channel: str, security_id: int) -> SnapshotBook:
    header = SnapshotHeader(
        channel=channel,
        security_id=security_id,
        packet_sequence_number=10,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        last_msg_seq_num_processed=5,
        tot_num_bids=1,
        tot_num_offers=1,
        last_rpt_seq=2,
        snapshot_header_row_id=1,
    )
    book = SnapshotBook(header=header)
    book.bids.append(
        SnapshotOrder(
            channel=channel,
            security_id=security_id,
            side=BookSide.BID,
            price=100.0,
            size=2,
            position_no=1,
            secondary_order_id=111,
            md_insert_timestamp_ns=0,
        )
    )
    book.asks.append(
        SnapshotOrder(
            channel=channel,
            security_id=security_id,
            side=BookSide.ASK,
            price=105.0,
            size=3,
            position_no=1,
            secondary_order_id=222,
            md_insert_timestamp_ns=0,
        )
    )
    return book


def _instrument_record(
    channel: str = "channel_1",
    security_id: int = 1,
    symbol: str = "TEST",
) -> InstrumentRecord:
    return InstrumentRecord(
        channel=channel,
        security_id=security_id,
        symbol=symbol,
        min_price_increment=0.01,
        price_divisor=1.0,
    )


def test_order_book_handles_updates_and_deletes() -> None:
    snapshot = _snapshot_book("channel_1", 1)
    book = OrderBook(channel="channel_1", security_id=1, snapshot=snapshot)

    new_msg = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=11,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=3,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        price=101.0,
        size=5,
        secondary_order_id=333,
        md_update_action="NEW",
    )
    book.handle_update(new_msg)
    assert book.bids.orders[0].price == 101.0
    assert book.bids.orders[1].price == 100.0

    change_msg = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=12,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=4,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=2,
        price=100.5,
        size=4,
        secondary_order_id=444,
        md_update_action="CHANGE",
    )
    book.handle_update(change_msg)
    assert book.bids.orders[1].price == 100.5

    delete_msg = OrderDeleteMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=13,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=5,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        size=5,
        secondary_order_id=333,
    )
    book.handle_delete(delete_msg)
    assert len(book.bids.orders) == 1

    mass_msg = OrderMassDeleteMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=14,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=6,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=10,
        md_update_action="DELETE_THRU",
    )
    book.handle_mass_delete(mass_msg)
    assert len(book.bids.orders) == 0


def test_event_recorder_emits_snapshots() -> None:
    book = OrderBook(channel="channel_1", security_id=1, snapshot=None)
    update = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=1,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=1,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        price=10.0,
        size=1,
        secondary_order_id=1,
        md_update_action="NEW",
    )
    book.handle_update(update)
    instrument = _instrument_record(symbol="TST")
    recorder = EventRecorder(instruments={("channel_1", 1): instrument})
    key = ("channel_1", 1)
    recorder.touch(key)
    marker = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=2,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0x80,
        rpt_seq=2,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        price=10.0,
        size=1,
        secondary_order_id=1,
        md_update_action="CHANGE",
    )
    recorder.handle_match_event(marker, {key: book}, ReplayStats())
    assert len(recorder.snapshots) == 1
    snapshot = recorder.snapshots[0]
    assert snapshot.bids[0].price == 10.0


def test_event_recorder_streams_to_consumer() -> None:
    book = OrderBook(channel="channel_1", security_id=1, snapshot=None)
    update = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=1,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        rpt_seq=1,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        price=10.0,
        size=1,
        secondary_order_id=1,
        md_update_action="NEW",
    )
    book.handle_update(update)
    collected: list[BookEventSnapshot] = []
    instrument = _instrument_record(symbol="TST")
    recorder = EventRecorder(
        instruments={("channel_1", 1): instrument}, snapshot_consumer=collected.append
    )
    key = ("channel_1", 1)
    recorder.touch(key)
    marker = OrderUpdateMessage(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_number=2,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0x80,
        rpt_seq=2,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
        side=BookSide.BID,
        position_no=1,
        price=10.0,
        size=1,
        secondary_order_id=1,
        md_update_action="CHANGE",
    )
    recorder.handle_match_event(marker, {key: book}, ReplayStats())
    assert not recorder.snapshots
    assert len(collected) == 1
    assert collected[0].bids[0].price == 10.0


def test_event_recorder_level_one_limits_snapshot_depth() -> None:
    book = OrderBook(channel="channel_1", security_id=1, snapshot=None)
    base_kwargs = dict(
        channel="channel_1",
        feed="feed_A",
        security_id=1,
        packet_sequence_version=1,
        packet_sending_time_ns=0,
        match_event_indicator_raw=0,
        md_entry_timestamp_ns=0,
        message_index_in_packet=1,
    )
    updates = [
        OrderUpdateMessage(
            **base_kwargs,
            packet_sequence_number=1,
            rpt_seq=1,
            side=BookSide.BID,
            position_no=1,
            price=10.0,
            size=1,
            secondary_order_id=1,
            md_update_action="NEW",
        ),
        OrderUpdateMessage(
            **base_kwargs,
            packet_sequence_number=2,
            rpt_seq=2,
            side=BookSide.BID,
            position_no=2,
            price=9.5,
            size=2,
            secondary_order_id=2,
            md_update_action="NEW",
        ),
        OrderUpdateMessage(
            **base_kwargs,
            packet_sequence_number=3,
            rpt_seq=3,
            side=BookSide.ASK,
            position_no=1,
            price=11.0,
            size=3,
            secondary_order_id=3,
            md_update_action="NEW",
        ),
        OrderUpdateMessage(
            **base_kwargs,
            packet_sequence_number=4,
            rpt_seq=4,
            side=BookSide.ASK,
            position_no=2,
            price=11.5,
            size=4,
            secondary_order_id=4,
            md_update_action="NEW",
        ),
    ]
    for msg in updates:
        book.handle_update(msg)
    instrument = _instrument_record(symbol="TST")
    recorder = EventRecorder(
        instruments={("channel_1", 1): instrument},
        level=1,
    )
    key = ("channel_1", 1)
    recorder.touch(key)
    marker_kwargs = dict(base_kwargs)
    marker_kwargs["match_event_indicator_raw"] = 0x80
    marker = OrderUpdateMessage(
        **marker_kwargs,
        packet_sequence_number=5,
        rpt_seq=5,
        side=BookSide.BID,
        position_no=1,
        price=10.0,
        size=1,
        secondary_order_id=1,
        md_update_action="CHANGE",
    )
    recorder.handle_match_event(marker, {key: book}, ReplayStats())
    snapshot = recorder.snapshots[0]
    assert len(snapshot.bids) == 1
    assert snapshot.bids[0].price == 10.0
    assert len(snapshot.asks) == 1
    assert snapshot.asks[0].price == 11.0


def test_bbo_builder_uses_first_orders() -> None:
    snapshot = BookEventSnapshot(
        instrument=_instrument_record(symbol="TEST"),
        event_index=0,
        packet_sequence_number=10,
        packet_sending_time_ns=0,
        bids=(
            OrderState(
                price=9.0,
                size=1,
                secondary_order_id=1,
                timestamp_ns=0,
                packet_sequence_number=1,
                rpt_seq=1,
            ),
        ),
        asks=(
            OrderState(
                price=11.0,
                size=2,
                secondary_order_id=2,
                timestamp_ns=0,
                packet_sequence_number=1,
                rpt_seq=1,
            ),
        ),
    )

    class DummyUniverse:
        def __init__(self) -> None:
            self.record = type("Instrument", (), {"symbol": "TEST"})()

        def for_security_id(self, security_id: int):  # noqa: D401
            return self.record if security_id == 1 else None

    rows = build_bbo_rows([snapshot], DummyUniverse())
    assert rows[0]["bid_price"] == 9.0
    assert rows[0]["ask_price"] == 11.0
    assert rows[0]["symbol"] == "TEST"


def test_pick_headers_since_uses_snapshot_before_timestamp() -> None:
    table = pa.table(
        {
            "security_id": [1, 1, 1],
            "packet_sequence_number": [10, 20, 30],
            "packet_sequence_version": [1, 1, 1],
            "packet_sending_time_ns": [100, 200, 400],
            "last_msg_seq_num_processed": [5, 15, 25],
            "tot_num_bids": [1, 1, 1],
            "tot_num_offers": [1, 1, 1],
            "last_rpt_seq": [2, 3, 4],
            "snapshot_header_row_id": [111, 222, 333],
        }
    )

    headers = _pick_headers("channel_1", table, since_ns=250)
    assert headers[1].snapshot_header_row_id == 222

    earliest = _pick_headers("channel_1", table, since_ns=None)
    assert earliest[1].snapshot_header_row_id == 111


def test_parse_since_supports_iso8601_and_ns() -> None:
    expected = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    assert _parse_since("2024-01-01T00:00:00+00:00") == expected
    assert _parse_since(str(expected)) == expected


def test_snapshot_output_manager_respects_level(tmp_path) -> None:
    instrument = _instrument_record(symbol="TEST")
    snapshot = BookEventSnapshot(
        instrument=instrument,
        event_index=0,
        packet_sequence_number=1,
        packet_sending_time_ns=0,
        bids=(
            OrderState(
                price=9.0,
                size=1,
                secondary_order_id=1,
                timestamp_ns=0,
                packet_sequence_number=1,
                rpt_seq=1,
            ),
        ),
        asks=(),
    )
    manager = SnapshotOutputManager(
        tickers=[instrument.symbol],
        output_root=tmp_path,
        csv_output=True,
        drop_fields=set(),
        level=1,
    )
    manager.handle_snapshot(snapshot)
    manager.close()
    bbo_path = tmp_path / instrument.symbol / "book_l1.csv"
    l2_path = tmp_path / instrument.symbol / "book_l2.csv"
    l3_path = tmp_path / instrument.symbol / "book_l3.csv"
    assert bbo_path.exists()
    assert not l2_path.exists()
    assert not l3_path.exists()
