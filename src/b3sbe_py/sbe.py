"""Specialised SBE decoders for a subset of B3 UMDF templates."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .utils import (
    NULL_INT64,
    read_char_array,
    read_i64,
    read_price,
    read_u16,
    read_u32,
    read_u64,
)

PRICE_EXPONENT = -4


@dataclass
class MessageResult:
    template_id: int
    rows: Dict[str, Dict[str, List]]


class TypedTable:
    def __init__(self, columns, writer=None):
        self.columns = columns
        self.writer = writer
        self.data = None if writer else {name: [] for name in columns}

    def append(self, row):
        if self.writer is not None:
            self.writer.append(row)
            return
        for name in self.columns:
            self.data[name].append(row.get(name))

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()


class RowSet:
    def __init__(self, writers: Optional[Dict[str, object]] = None):
        self.tables: Dict[str, TypedTable] = {}
        self.writers = writers or {}

    def ensure(self, table_name: str, columns: Tuple[str, ...]) -> TypedTable:
        table = self.tables.get(table_name)
        if table is None:
            writer = self.writers.get(table_name)
            table = TypedTable(columns, writer)
            self.tables[table_name] = table
        return table

    def close(self) -> None:
        for table in self.tables.values():
            table.close()

    def to_dict(self) -> Dict[str, Dict[str, List]]:
        result = {}
        for name, table in self.tables.items():
            if table.data is not None:
                result[name] = table.data
        return result


def decode_order_mbo(body: memoryview, ctx: Dict[str, object], out: RowSet) -> None:
    table = out.ensure(
        "incremental_orders",
        (
            "source_file",
            "channel_hint",
            "feed_leg",
            "security_id",
            "match_event_indicator",
            "md_update_action",
            "md_entry_type",
            "price_mantissa",
            "price",
            "size",
            "position_no",
            "entering_firm",
            "md_insert_timestamp_ns",
            "secondary_order_id",
            "rpt_seq",
            "md_entry_timestamp_ns",
            "packet_sequence_number",
            "packet_sending_time_ns",
        ),
    )
    security_id = read_u64(body, 0)
    match_event_indicator = body[8]
    md_update_action = body[9]
    md_entry_type = body[10]
    price_mantissa, price = read_price(body, 12, PRICE_EXPONENT)
    size = read_i64(body, 20)
    position_no = read_u32(body, 28)
    entering_firm = read_u32(body, 32)
    md_insert = read_u64(body, 36)
    secondary_order_id = read_u64(body, 44)
    rpt_seq = read_u32(body, 52)
    md_entry_timestamp = read_u64(body, 56)
    table.append(
        {
            "source_file": ctx["source_file"],
            "channel_hint": ctx["channel_hint"],
            "feed_leg": ctx["feed_leg"],
            "security_id": security_id,
            "match_event_indicator": match_event_indicator,
            "md_update_action": md_update_action,
            "md_entry_type": md_entry_type,
            "price_mantissa": price_mantissa,
            "price": price,
            "size": size,
            "position_no": position_no,
            "entering_firm": entering_firm,
            "md_insert_timestamp_ns": md_insert,
            "secondary_order_id": secondary_order_id,
            "rpt_seq": rpt_seq,
            "md_entry_timestamp_ns": md_entry_timestamp,
            "packet_sequence_number": ctx["packet_sequence_number"],
            "packet_sending_time_ns": ctx["packet_sending_time_ns"],
        }
    )


def decode_delete_order(body: memoryview, ctx: Dict[str, object], out: RowSet) -> None:
    table = out.ensure(
        "incremental_deletes",
        (
            "source_file",
            "channel_hint",
            "feed_leg",
            "security_id",
            "match_event_indicator",
            "md_entry_type",
            "position_no",
            "size",
            "secondary_order_id",
            "md_entry_timestamp_ns",
            "rpt_seq",
            "packet_sequence_number",
            "packet_sending_time_ns",
        ),
    )
    security_id = read_u64(body, 0)
    match_event_indicator = body[8]
    md_entry_type = body[10]
    position_no = read_u32(body, 12)
    size = read_i64(body, 16)
    secondary_order_id = read_u64(body, 24)
    md_entry_timestamp = read_u64(body, 32)
    rpt_seq = read_u32(body, 40)
    table.append(
        {
            "source_file": ctx["source_file"],
            "channel_hint": ctx["channel_hint"],
            "feed_leg": ctx["feed_leg"],
            "security_id": security_id,
            "match_event_indicator": match_event_indicator,
            "md_entry_type": md_entry_type,
            "position_no": position_no,
            "size": size,
            "secondary_order_id": secondary_order_id,
            "md_entry_timestamp_ns": md_entry_timestamp,
            "rpt_seq": rpt_seq,
            "packet_sequence_number": ctx["packet_sequence_number"],
            "packet_sending_time_ns": ctx["packet_sending_time_ns"],
        }
    )


def decode_mass_delete(body: memoryview, ctx: Dict[str, object], out: RowSet) -> None:
    table = out.ensure(
        "incremental_mass_deletes",
        (
            "source_file",
            "channel_hint",
            "feed_leg",
            "security_id",
            "match_event_indicator",
            "md_update_action",
            "md_entry_type",
            "position_no",
            "md_entry_timestamp_ns",
            "rpt_seq",
            "packet_sequence_number",
            "packet_sending_time_ns",
        ),
    )
    security_id = read_u64(body, 0)
    match_event_indicator = body[8]
    md_update_action = body[9]
    md_entry_type = body[10]
    position_no = read_u32(body, 12)
    md_entry_timestamp = read_u64(body, 16)
    rpt_seq = read_u32(body, 24)
    table.append(
        {
            "source_file": ctx["source_file"],
            "channel_hint": ctx["channel_hint"],
            "feed_leg": ctx["feed_leg"],
            "security_id": security_id,
            "match_event_indicator": match_event_indicator,
            "md_update_action": md_update_action,
            "md_entry_type": md_entry_type,
            "position_no": position_no,
            "md_entry_timestamp_ns": md_entry_timestamp,
            "rpt_seq": rpt_seq,
            "packet_sequence_number": ctx["packet_sequence_number"],
            "packet_sending_time_ns": ctx["packet_sending_time_ns"],
        }
    )


def decode_trade(body: memoryview, ctx: Dict[str, object], out: RowSet) -> None:
    table = out.ensure(
        "incremental_trades",
        (
            "source_file",
            "channel_hint",
            "feed_leg",
            "security_id",
            "match_event_indicator",
            "trading_session_id",
            "trade_condition",
            "price_mantissa",
            "price",
            "size",
            "trade_id",
            "buyer_firm",
            "seller_firm",
            "trade_date",
            "md_entry_timestamp_ns",
            "rpt_seq",
            "packet_sequence_number",
            "packet_sending_time_ns",
        ),
    )
    security_id = read_u64(body, 0)
    match_event_indicator = body[8]
    trading_session_id = body[9]
    trade_condition = read_u16(body, 10)
    price_mantissa, price = read_price(body, 12, PRICE_EXPONENT)
    size = read_i64(body, 20)
    trade_id = read_u64(body, 28)
    buyer_firm = read_u32(body, 36)
    seller_firm = read_u32(body, 40)
    trade_date = read_u32(body, 44)
    md_entry_timestamp = read_u64(body, 48)
    rpt_seq = read_u32(body, 56)
    table.append(
        {
            "source_file": ctx["source_file"],
            "channel_hint": ctx["channel_hint"],
            "feed_leg": ctx["feed_leg"],
            "security_id": security_id,
            "match_event_indicator": match_event_indicator,
            "trading_session_id": trading_session_id,
            "trade_condition": trade_condition,
            "price_mantissa": price_mantissa,
            "price": price,
            "size": size,
            "trade_id": trade_id,
            "buyer_firm": buyer_firm,
            "seller_firm": seller_firm,
            "trade_date": trade_date,
            "md_entry_timestamp_ns": md_entry_timestamp,
            "rpt_seq": rpt_seq,
            "packet_sequence_number": ctx["packet_sequence_number"],
            "packet_sending_time_ns": ctx["packet_sending_time_ns"],
        }
    )

def _read_group_header(buffer: memoryview, offset: int) -> Tuple[int, int, int]:
    block_length = read_u16(buffer, offset)
    num_in_group = buffer[offset + 2]
    return block_length, num_in_group, offset + 3


def decode_snapshot_orders(body: memoryview, ctx: Dict[str, object], out: RowSet) -> None:
    root_length = 24
    security_id = read_u64(body, 0)
    table = out.ensure(
        "snapshot_orders",
        (
            "source_file",
            "channel_hint",
            "security_id",
            "side",
            "side_raw",
            "price_mantissa",
            "price",
            "size",
            "position_no",
            "entering_firm",
            "md_insert_timestamp_ns",
            "secondary_order_id",
            "row_in_group",
            "packet_sequence_number",
            "packet_sending_time_ns",
        ),
    )
    _, count, position = _read_group_header(body, root_length)
    for index in range(count):
        price_mantissa, price = read_price(body, position, PRICE_EXPONENT)
        size = read_i64(body, position + 8)
        position_no = read_u32(body, position + 16)
        entering_firm = read_u32(body, position + 20)
        md_insert = read_u64(body, position + 24)
        secondary_order_id = read_u64(body, position + 32)
        side_raw = body[position + 40]
        table.append(
            {
                "source_file": ctx["source_file"],
                "channel_hint": ctx["channel_hint"],
                "security_id": security_id,
                "side": "buy" if side_raw == 0x30 else "sell",
                "side_raw": side_raw,
                "price_mantissa": price_mantissa,
                "price": price,
                "size": size,
                "position_no": position_no,
                "entering_firm": entering_firm,
                "md_insert_timestamp_ns": md_insert,
                "secondary_order_id": secondary_order_id,
                "row_in_group": index,
                "packet_sequence_number": ctx["packet_sequence_number"],
                "packet_sending_time_ns": ctx["packet_sending_time_ns"],
            }
        )
        position += 41


DECODE_DISPATCH = {
    50: decode_order_mbo,
    51: decode_delete_order,
    52: decode_mass_delete,
    53: decode_trade,
    71: decode_snapshot_orders,
}


def decode_body(template_id: int, body: memoryview, ctx: Dict[str, object], out: RowSet) -> bool:
    handler = DECODE_DISPATCH.get(template_id)
    if handler is None:
        return False
    handler(body, ctx, out)
    return True
