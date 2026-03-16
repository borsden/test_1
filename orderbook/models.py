from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class BookSide(str, Enum):
    BID = "BID"
    ASK = "ASK"

    @classmethod
    def from_string(cls, value: str) -> "BookSide":
        try:
            normalized = value.strip().upper()
            if normalized in {"BUY", "BID"}:
                normalized = "BID"
            elif normalized in {"SELL", "ASK", "OFFER"}:
                normalized = "ASK"
            return cls(normalized)
        except ValueError as exc:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported book side: {value}") from exc


@dataclass(slots=True)
class SnapshotHeader:
    channel: str
    security_id: int
    packet_sequence_number: int
    packet_sequence_version: int | None
    packet_sending_time_ns: int
    last_msg_seq_num_processed: int
    tot_num_bids: int
    tot_num_offers: int
    last_rpt_seq: int
    snapshot_header_row_id: int


@dataclass(slots=True)
class SnapshotOrder:
    channel: str
    security_id: int
    side: BookSide
    price: float
    size: int
    position_no: int
    secondary_order_id: int
    md_insert_timestamp_ns: int


@dataclass(slots=True)
class SnapshotBook:
    header: SnapshotHeader
    bids: List[SnapshotOrder] = field(default_factory=list)
    asks: List[SnapshotOrder] = field(default_factory=list)


@dataclass(slots=True)
class IncrementalMessage:
    channel: str
    feed: str
    security_id: int
    packet_sequence_number: int
    packet_sequence_version: int | None
    packet_sending_time_ns: int
    match_event_indicator_raw: int
    rpt_seq: int | None
    md_entry_timestamp_ns: int | None
    message_index_in_packet: int | None


@dataclass(slots=True)
class OrderUpdateMessage(IncrementalMessage):
    side: BookSide
    position_no: int
    price: float
    size: int
    secondary_order_id: int
    md_update_action: str


@dataclass(slots=True)
class OrderDeleteMessage(IncrementalMessage):
    side: BookSide
    position_no: int
    size: int
    secondary_order_id: int


@dataclass(slots=True)
class OrderMassDeleteMessage(IncrementalMessage):
    side: BookSide
    position_no: int
    md_update_action: str


@dataclass(slots=True)
class EmptyBookMessage(IncrementalMessage):
    pass


@dataclass(slots=True)
class ChannelResetMessage(IncrementalMessage):
    pass


@dataclass(slots=True)
class SequenceResetMessage(IncrementalMessage):
    sequence_version: int | None = None


@dataclass(slots=True)
class NonBookMessage(IncrementalMessage):
    template_id: int
    template_name: str
    body_hex: str


@dataclass(slots=True)
class TradeMessage(IncrementalMessage):
    price: float
    size: int
    trade_id: int
