from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Sequence

from .models import (
    BookSide,
    ChannelResetMessage,
    EmptyBookMessage,
    IncrementalMessage,
    OrderDeleteMessage,
    OrderMassDeleteMessage,
    OrderUpdateMessage,
    SnapshotBook,
    SnapshotOrder,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class OrderState:
    price: float
    size: int
    secondary_order_id: int
    timestamp_ns: int
    packet_sequence_number: int
    rpt_seq: int | None
    md_update_action: str | None = None

    def clone(self) -> "OrderState":
        return OrderState(
            price=self.price,
            size=self.size,
            secondary_order_id=self.secondary_order_id,
            timestamp_ns=self.timestamp_ns,
            packet_sequence_number=self.packet_sequence_number,
            rpt_seq=self.rpt_seq,
            md_update_action=self.md_update_action,
        )


def _format_context(context: str | None) -> str:
    return f" ctx={context}" if context else ""


class BookSideState:
    def __init__(self, side: BookSide, warn):
        self.side = side
        self.orders: List[OrderState] = []
        self._warn = warn

    def clear(self) -> None:
        self.orders.clear()

    def __len__(self) -> int:
        return len(self.orders)

    def insert(self, position_no: int, order: OrderState, *, context: str | None = None) -> None:
        index = max(0, min(position_no - 1, len(self.orders)))
        if position_no < 1 or position_no > len(self.orders) + 1:
            if len(self.orders) > 0:
                self._warn(
                    f"[{self.side}] insert position {position_no} outside current depth {len(self.orders)}"
                    f"{_format_context(context)}"
                )
        self.orders.insert(index, order)

    def update(self, position_no: int, order: OrderState, *, context: str | None = None) -> None:
        if position_no < 1 or position_no > len(self.orders):
            if len(self.orders) > 0:
                self._warn(
                    f"[{self.side}] update position {position_no} outside current depth {len(self.orders)}"
                    f"{_format_context(context)}"
                )
            return
        self.orders[position_no - 1] = order

    def delete(self, position_no: int, *, context: str | None = None) -> None:
        if position_no < 1 or position_no > len(self.orders):
            # Suppress warning if the book is completely empty (often happens at start of day/after resets)
            if len(self.orders) > 0:
                self._warn(
                    f"[{self.side}] delete position {position_no} outside current depth {len(self.orders)}"
                    f"{_format_context(context)}"
                )
            return
        del self.orders[position_no - 1]

    def mass_delete(self, position_no: int, action: str | None = None) -> None:
        if not self.orders:
            return
        action_upper = (action or "").upper()
        if action_upper == "DELETE_THRU":
            self.orders.clear()
            return
        if action_upper == "DELETE_FROM":
            end = min(len(self.orders), max(1, position_no))
            del self.orders[:end]
            return
        self._warn(
            f"[{self.side}] unsupported mass delete action {action_upper or 'UNKNOWN'}"
            f" pos={position_no}"
        )

    def snapshot_orders(self) -> Sequence[OrderState]:
        return tuple(order.clone() for order in self.orders)


class OrderBook:
    """Mutable order book state for a single instrument."""

    def __init__(
        self,
        channel: str,
        security_id: int,
        snapshot: SnapshotBook | None = None,
        warning_sink: List[str] | None = None,
        suppress_rpt_warnings: bool = False,
    ):
        self.channel = channel
        self.security_id = security_id
        self._warnings = warning_sink
        self._suppress_rpt_warnings = suppress_rpt_warnings
        self.bids = BookSideState(BookSide.BID, self._emit_warning)
        self.asks = BookSideState(BookSide.ASK, self._emit_warning)
        self.last_snapshot: SnapshotBook | None = snapshot
        self.next_rpt_seq: int | None = None
        if snapshot is not None:
            self._apply_snapshot(snapshot)

    def _apply_snapshot(self, snapshot: SnapshotBook) -> None:
        LOGGER.debug(
            "[%s:%s] apply-snapshot bids=%d asks=%d last_rpt_seq=%s",
            self.channel,
            self.security_id,
            len(snapshot.bids),
            len(snapshot.asks),
            snapshot.header.last_rpt_seq,
        )
        self.bids.clear()
        self.asks.clear()
        for order in snapshot.bids:
            self.bids.insert(order.position_no, _order_state_from_snapshot(order))
        for order in snapshot.asks:
            self.asks.insert(order.position_no, _order_state_from_snapshot(order))
        self.next_rpt_seq = snapshot.header.last_rpt_seq + 1 if snapshot.header.last_rpt_seq else 1

    def handle_update(self, message: OrderUpdateMessage) -> None:
        self._check_rpt_seq(message)
        LOGGER.debug(
            "[%s:%s] update side=%s pos=%d action=%s price=%s size=%s",
            self.channel,
            self.security_id,
            message.side,
            message.position_no,
            message.md_update_action,
            message.price,
            message.size,
        )
        state = OrderState(
            price=message.price,
            size=message.size,
            secondary_order_id=message.secondary_order_id,
            timestamp_ns=message.md_entry_timestamp_ns or 0,
            packet_sequence_number=message.packet_sequence_number,
            rpt_seq=message.rpt_seq,
            md_update_action=message.md_update_action,
        )
        target = self.bids if message.side is BookSide.BID else self.asks
        action = message.md_update_action.upper()
        if action in {"NEW", "ADD"}:
            target.insert(
                message.position_no,
                state,
                context=
                    f"price={message.price} size={message.size} pkt={message.packet_sequence_number}"
                    f" action={message.md_update_action}",
            )
        else:
            target.update(
                message.position_no,
                state,
                context=
                    f"price={message.price} size={message.size} pkt={message.packet_sequence_number}"
                    f" action={message.md_update_action}",
            )

    def handle_delete(self, message: OrderDeleteMessage) -> None:
        self._check_rpt_seq(message)
        LOGGER.debug(
            "[%s:%s] delete side=%s pos=%d size=%s",
            self.channel,
            self.security_id,
            message.side,
            message.position_no,
            message.size,
        )
        target = self.bids if message.side is BookSide.BID else self.asks
        target.delete(
            message.position_no,
            context=f"size={message.size} pkt={message.packet_sequence_number}",
        )

    def handle_mass_delete(self, message: OrderMassDeleteMessage) -> None:
        self._check_rpt_seq(message)
        LOGGER.debug(
            "[%s:%s] mass-delete side=%s position=%s action=%s",
            self.channel,
            self.security_id,
            message.side,
            message.position_no,
            message.md_update_action,
        )
        target = self.bids if message.side is BookSide.BID else self.asks
        target.mass_delete(message.position_no, message.md_update_action)

    def handle_empty_book(self, message: EmptyBookMessage) -> None:
        self._check_rpt_seq(message, reset=True)
        LOGGER.warning("[%s:%s] empty-book", self.channel, self.security_id)
        self.bids.clear()
        self.asks.clear()
        self.next_rpt_seq = 1

    def handle_channel_reset(self, message: ChannelResetMessage) -> None:
        LOGGER.warning("[%s] channel-reset", self.channel)
        del message  # channel reset affects routing layer; actual clearing happens upstream
        self.bids.clear()
        self.asks.clear()
        self.next_rpt_seq = None

    def touch_sequence(self, message: IncrementalMessage) -> None:
        self._check_rpt_seq(message)

    def _check_rpt_seq(self, message: IncrementalMessage, *, reset: bool = False) -> None:
        if reset:
            self.next_rpt_seq = 1
            return
        if message.rpt_seq is None:
            return
        if self.next_rpt_seq is None:
            self.next_rpt_seq = message.rpt_seq
        expected = self.next_rpt_seq
        if (
            expected is not None
            and message.rpt_seq != expected
            and not self._suppress_rpt_warnings
        ):
            self._emit_warning(
                f"[{self.channel}:{self.security_id}] rptSeq gap: expected {expected} got {message.rpt_seq} "
                f"(packet={message.packet_sequence_number})"
            )
        self.next_rpt_seq = (message.rpt_seq or 0) + 1

    def snapshot(self, depth: int | None = None) -> Dict[str, Sequence[OrderState]]:
        return {
            "bids": self._snapshot_side(self.bids, depth),
            "asks": self._snapshot_side(self.asks, depth),
        }

    @staticmethod
    def _snapshot_side(side: BookSideState, depth: int | None) -> Sequence[OrderState]:
        if depth is None:
            source = side.orders
        elif depth <= 0:
            return tuple()
        else:
            source = side.orders[:depth]
        return tuple(order.clone() for order in source)

    def _emit_warning(self, text: str) -> None:
        LOGGER.warning(text)
        if self._warnings is not None:
            self._warnings.append(text)


def _order_state_from_snapshot(order: SnapshotOrder) -> OrderState:
    return OrderState(
        price=order.price,
        size=order.size,
        secondary_order_id=order.secondary_order_id,
        timestamp_ns=order.md_insert_timestamp_ns,
        packet_sequence_number=0,
        rpt_seq=None,
        md_update_action="SNAPSHOT",
    )
