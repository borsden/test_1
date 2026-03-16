from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Sequence, Tuple

from .config import PipelineConfig
from .data_loader import DataLoader, SnapshotKey
from .instruments import InstrumentRecord, InstrumentUniverse
from .match_event import is_end_of_event, is_recovery
from .progress import iter_progress
from .models import (
    ChannelResetMessage,
    EmptyBookMessage,
    IncrementalMessage,
    NonBookMessage,
    OrderDeleteMessage,
    OrderMassDeleteMessage,
    OrderUpdateMessage,
    SequenceResetMessage,
    SnapshotBook,
    TradeMessage,
)
from .order_book import OrderBook, OrderState

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ReplayStats:
    order_updates: int = 0
    order_deletes: int = 0
    mass_deletes: int = 0
    empty_books: int = 0
    channel_resets: int = 0
    trades: int = 0
    other_messages: int = 0
    events_emitted: int = 0


@dataclass(slots=True)
class BookEventSnapshot:
    instrument: InstrumentRecord
    event_index: int
    packet_sequence_number: int
    packet_sending_time_ns: int
    bids: Sequence[OrderState]
    asks: Sequence[OrderState]


@dataclass(slots=True)
class BookReplayResult:
    books: Mapping[SnapshotKey, OrderBook]
    event_snapshots: Sequence[BookEventSnapshot]
    stats: ReplayStats
    warnings: Sequence[str] = field(default_factory=tuple)


class BookReplayer:
    """Replays incremental streams for the requested instruments."""

    def __init__(self, config: PipelineConfig, universe: InstrumentUniverse):
        self.config = config
        self.universe = universe
        self.loader = DataLoader(config, universe)
        self._channel_packet_thresholds: Dict[str, int] = {}
        self._initial_rpt_seq: Dict[SnapshotKey, int] = {}
        self._warnings: List[str] = []
        self._notified_recovery_feeds: set[Tuple[str, str]] = set()
        self._instrument_by_key: Dict[SnapshotKey, InstrumentRecord] = {}

    def replay(self, snapshot_consumer: Callable[[BookEventSnapshot], None] | None = None) -> BookReplayResult:
        self._warnings = []
        self._notified_recovery_feeds = set()
        snapshots = self.loader.load_snapshots()
        books = self._initialize_books(snapshots)
        events = self.loader.load_incremental_events()
        feed_selection = self._select_feed_events(events)
        stats = ReplayStats()
        recorder = EventRecorder(
            instruments=self._instrument_by_key,
            snapshot_consumer=snapshot_consumer,
        )
        channel_iter = list(feed_selection.items())
        for channel, feed_events in iter_progress(
            channel_iter,
            enabled=self.config.show_progress,
            desc="Replay",
            total=len(channel_iter) if channel_iter else None,
        ):
            message_iter = list(feed_events)
            iterator = iter_progress(
                message_iter,
                enabled=self.config.show_progress,
                desc=f"{channel} events",
                total=len(message_iter) if message_iter else None,
            )
            for message in iterator:
                key = (message.channel, message.security_id)
                if not self._should_process(message):
                    continue
                record_event = self._should_record(message)
                if is_recovery(message.match_event_indicator_raw):
                    feed_key = (message.channel, message.feed)
                    if feed_key not in self._notified_recovery_feeds:
                        self._warnings.append(
                            f"Recovery bit seen on {message.channel}/{message.feed}; replay currently does not "
                            "differentiate recovery segments"
                        )
                        self._notified_recovery_feeds.add(feed_key)
                if isinstance(message, OrderUpdateMessage):
                    book = books.get(key)
                    if not book:
                        self._warnings.append(
                            f"Skipping update for {key}: no snapshot loaded (need re-run with broader filters)"
                        )
                        continue
                    book.handle_update(message)
                    if record_event:
                        recorder.touch(key)
                        stats.order_updates += 1
                elif isinstance(message, OrderDeleteMessage):
                    book = books.get(key)
                    if not book:
                        self._warnings.append(
                            f"Skipping delete for {key}: no snapshot loaded"
                        )
                        continue
                    book.handle_delete(message)
                    if record_event:
                        recorder.touch(key)
                        stats.order_deletes += 1
                elif isinstance(message, OrderMassDeleteMessage):
                    book = books.get(key)
                    if not book:
                        self._warnings.append(
                            f"Skipping mass delete for {key}: no snapshot loaded"
                        )
                        continue
                    book.handle_mass_delete(message)
                    if record_event:
                        recorder.touch(key)
                        stats.mass_deletes += 1
                elif isinstance(message, EmptyBookMessage):
                    book = books.get(key)
                    if not book:
                        self._warnings.append(
                            f"EmptyBook for unknown {key} ignored"
                        )
                        continue
                    book.handle_empty_book(message)
                    if record_event:
                        recorder.touch(key)
                        stats.empty_books += 1
                elif isinstance(message, ChannelResetMessage):
                    for candidate_key, candidate in books.items():
                        if candidate_key[0] == message.channel:
                            candidate.handle_channel_reset(message)
                            if record_event:
                                recorder.touch(candidate_key)
                    if record_event:
                        stats.channel_resets += 1
                elif isinstance(message, TradeMessage):
                    book = books.get(key)
                    if book:
                        book.touch_sequence(message)
                        LOGGER.debug(
                            "[%s:%s] trade price=%s size=%s rpt_seq=%s pkt=%s",
                            message.channel,
                            message.security_id,
                            message.price,
                            message.size,
                            message.rpt_seq,
                            message.packet_sequence_number,
                        )
                        if record_event:
                            recorder.touch(key)
                            stats.trades += 1
                    elif record_event:
                        stats.trades += 1
                elif isinstance(message, NonBookMessage):
                    if record_event:
                        recorder.touch(key)
                        stats.other_messages += 1
                elif isinstance(message, SequenceResetMessage):
                    text = (
                        f"SequenceReset on channel {message.channel} feed {message.feed} at packet "
                        f"{message.packet_sequence_number}"
                    )
                    self._warnings.append(text)
                    continue
                else:
                    LOGGER.debug("Skipping unsupported message type %s", type(message))
                if record_event:
                    recorder.handle_match_event(message, books, stats)
        return BookReplayResult(
            books=books,
            event_snapshots=recorder.snapshots,
            stats=stats,
            warnings=tuple(self._warnings),
        )

    def _initialize_books(self, snapshots: Mapping[SnapshotKey, SnapshotBook | None]) -> Dict[SnapshotKey, OrderBook]:
        books: Dict[SnapshotKey, OrderBook] = {}
        channel_thresholds: Dict[str, int] = {}
        rpt_seq_thresholds: Dict[SnapshotKey, int] = {}
        instrument_by_key: Dict[SnapshotKey, InstrumentRecord] = {}
        for instrument in self.universe.instruments:
            key = (instrument.channel, instrument.security_id)
            snapshot = snapshots.get(key)
            books[key] = OrderBook(
                channel=instrument.channel,
                security_id=instrument.security_id,
                snapshot=snapshot,
                warning_sink=self._warnings,
                suppress_rpt_warnings=self.config.suppress_rpt_warnings,
            )
            instrument_by_key[key] = instrument
            if snapshot is None:
                continue
            header = snapshot.header
            current_channel = channel_thresholds.get(instrument.channel)
            if current_channel is None or header.last_msg_seq_num_processed > current_channel:
                channel_thresholds[instrument.channel] = header.last_msg_seq_num_processed
            rpt_seq_thresholds[key] = header.last_rpt_seq
        self._channel_packet_thresholds = channel_thresholds
        self._initial_rpt_seq = rpt_seq_thresholds
        self._instrument_by_key = instrument_by_key
        return books

    def _select_feed_events(self, events: Mapping[Tuple[str, str], Sequence[IncrementalMessage]]):
        selection: Dict[str, Sequence[IncrementalMessage]] = {}
        for channel in self.universe.channels:
            for feed in self.config.feeds:
                key = (channel, feed)
                if key in events:
                    selection[channel] = events[key]
                    break
        return selection

    def _should_process(self, message: IncrementalMessage) -> bool:
        # always apply events to book to catch up with snapshot
        until_ns = self.config.until_ns
        if until_ns is not None and message.packet_sending_time_ns > until_ns:
            return False
        channel_threshold = self._channel_packet_thresholds.get(message.channel)
        if channel_threshold is not None and message.packet_sequence_number <= channel_threshold:
            return False
        key = (message.channel, message.security_id)
        last_rpt = self._initial_rpt_seq.get(key)
        if last_rpt is not None and message.rpt_seq is not None and message.rpt_seq <= last_rpt:
            return False
        return True

    def _should_record(self, message: IncrementalMessage) -> bool:
        since_ns = self.config.since_ns
        if since_ns is None:
            return True
        return message.packet_sending_time_ns >= since_ns


class EventRecorder:
    def __init__(
        self,
        *,
        instruments: Mapping[SnapshotKey, InstrumentRecord],
        snapshot_consumer: Callable[[BookEventSnapshot], None] | None = None,
    ) -> None:
        self.pending_keys: set[SnapshotKey] = set()
        self.snapshots: List[BookEventSnapshot] = []
        self._snapshot_consumer = snapshot_consumer
        self._instruments = instruments
        self.event_index = 0

    def touch(self, key: SnapshotKey) -> None:
        self.pending_keys.add(key)

    def handle_match_event(self, message: IncrementalMessage, books: Mapping[SnapshotKey, OrderBook], stats: ReplayStats) -> None:
        if not is_end_of_event(message.match_event_indicator_raw):
            return
        flushed = self._flush(message, books)
        stats.events_emitted += flushed

    def _flush(self, marker: IncrementalMessage, books: Mapping[SnapshotKey, OrderBook]) -> int:
        if not self.pending_keys:
            return 0
        flushed = 0
        for key in sorted(self.pending_keys):
            book = books.get(key)
            instrument = self._instruments.get(key)
            if not book or instrument is None:
                continue
            snapshot = book.snapshot()
            self._emit_snapshot(
                BookEventSnapshot(
                    instrument=instrument,
                    event_index=self.event_index,
                    packet_sequence_number=marker.packet_sequence_number,
                    packet_sending_time_ns=marker.packet_sending_time_ns,
                    bids=snapshot["bids"],
                    asks=snapshot["asks"],
                )
            )
            flushed += 1
        self.pending_keys.clear()
        self.event_index += 1
        return flushed

    def _emit_snapshot(self, snapshot: BookEventSnapshot) -> None:
        if self._snapshot_consumer is not None:
            self._snapshot_consumer(snapshot)
            return
        self.snapshots.append(snapshot)
