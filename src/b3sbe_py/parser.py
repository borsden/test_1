"""High-level parser wiring PCAP reader and SBE decoders."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .headers import (
    FRAMING_HEADER_STRUCT,
    MESSAGE_HEADER_STRUCT,
    PACKET_HEADER_STRUCT,
    FramingHeader,
    MessageHeader,
    PacketHeader,
)
from .pcap import PcapReader, extract_udp_payload
from .sbe import RowSet, decode_body

PACKET_TABLE_COLUMNS = (
    "source_file",
    "channel_hint_from_filename",
    "stream_kind",
    "feed_leg",
    "packet_index_in_file",
    "pcap_ts_ns",
    "udp_payload_len",
    "packet_channel_number",
    "packet_sequence_version",
    "packet_sequence_number",
    "packet_sending_time_ns",
    "packet_parse_ok",
    "error_code",
    "error_text",
)

MESSAGE_TABLE_COLUMNS = (
    "source_file",
    "packet_index_in_file",
    "message_index_in_packet",
    "message_offset_in_payload",
    "message_length",
    "encoding_type",
    "block_length",
    "template_id",
    "schema_id",
    "schema_version",
    "body_length",
    "packet_channel_number",
    "packet_sequence_version",
    "packet_sequence_number",
    "packet_sending_time_ns",
)

ROWSET_TABLES: Dict[str, tuple[str, ...]] = {
    "incremental_orders": (
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
    "incremental_deletes": (
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
    "incremental_mass_deletes": (
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
    "incremental_trades": (
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
    "snapshot_orders": (
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
}


@dataclass
class FileMetadata:
    source_file: str
    channel_hint: int
    stream_kind: str
    feed_leg: str


_FILE_PATTERN = re.compile(r"(?P<chan>\d+)_(?P<stream>[A-Za-z]+)(?:_(?P<detail>[A-Za-z]+))?")


def infer_file_metadata(path: Path | str) -> FileMetadata:
    name = Path(path).name
    match = _FILE_PATTERN.match(name)
    channel = int(match.group("chan")) if match else 0
    stream_kind = match.group("stream") if match else "unknown"
    detail = match.group("detail") if match else ""
    feed_leg = "-"
    if detail and detail.lower().startswith("feed"):
        feed_leg = detail[-1].upper()
    return FileMetadata(source_file=name, channel_hint=channel, stream_kind=stream_kind, feed_leg=feed_leg)


class ParquetTableWriter:
    def __init__(self, path: Path, columns: Iterable[str], batch_size: int = 5000) -> None:
        self.path = path
        self.columns = list(columns)
        self.batch_size = batch_size
        self.buffer: Dict[str, List[object]] = {col: [] for col in self.columns}
        self.writer: Optional[pq.ParquetWriter] = None

    def append(self, row: Dict[str, object]) -> None:
        for column in self.columns:
            self.buffer[column].append(row.get(column))
        if len(self.buffer[self.columns[0]]) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        first_col = self.columns[0]
        if not self.buffer[first_col]:
            return
        arrays = [pa.array(self.buffer[col]) for col in self.columns]
        table = pa.Table.from_arrays(arrays, names=self.columns)
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(self.path, table.schema, compression="zstd")
        self.writer.write_table(table)
        for col in self.columns:
            self.buffer[col].clear()

    def close(self) -> None:
        self.flush()
        if self.writer is not None:
            self.writer.close()
            self.writer = None


class PythonParser:
    def __init__(self, output_dir: Optional[Path] = None, batch_size: int = 5000, max_packets: Optional[int] = None) -> None:
        self.output_dir = Path(output_dir) if output_dir else None
        self.batch_size = batch_size
        self.max_packets = max_packets
        writer_map: Dict[str, ParquetTableWriter] = {}
        if self.output_dir is not None:
            for name, columns in ROWSET_TABLES.items():
                writer_map[name] = ParquetTableWriter(self.output_dir / f"{name}.parquet", columns, batch_size)
            self.packet_writer = ParquetTableWriter(
                self.output_dir / "packets.parquet", PACKET_TABLE_COLUMNS, batch_size
            )
            self.message_writer = ParquetTableWriter(
                self.output_dir / "messages.parquet", MESSAGE_TABLE_COLUMNS, batch_size
            )
        else:
            writer_map = {}
            self.packet_writer = None
            self.message_writer = None
        self.rowset = RowSet(writer_map)
        self.packets = None if self.packet_writer else {name: [] for name in PACKET_TABLE_COLUMNS}
        self.messages = None if self.message_writer else {name: [] for name in MESSAGE_TABLE_COLUMNS}

    def _append_packet(self, row: Dict[str, object]) -> None:
        if self.packet_writer is not None:
            self.packet_writer.append(row)
            return
        for column in PACKET_TABLE_COLUMNS:
            self.packets[column].append(row.get(column))

    def _append_message(self, row: Dict[str, object]) -> None:
        if self.message_writer is not None:
            self.message_writer.append(row)
            return
        for column in MESSAGE_TABLE_COLUMNS:
            self.messages[column].append(row.get(column))

    def _close(self) -> None:
        self.rowset.close()
        if self.packet_writer is not None:
            self.packet_writer.close()
        if self.message_writer is not None:
            self.message_writer.close()

    def parse_file(self, path: Path | str, max_packets: Optional[int] = None) -> Dict[str, Dict[str, list]]:
        meta = infer_file_metadata(path)
        packet_limit = max_packets if max_packets is not None else self.max_packets
        with PcapReader(path) as reader:
            for packet_index, packet in enumerate(reader.packets()):
                if packet_limit is not None and packet_index >= packet_limit:
                    break
                packet_row = {
                    "source_file": meta.source_file,
                    "channel_hint_from_filename": meta.channel_hint,
                    "stream_kind": meta.stream_kind,
                    "feed_leg": meta.feed_leg,
                    "packet_index_in_file": packet_index,
                    "pcap_ts_ns": packet.timestamp_ns,
                    "udp_payload_len": 0,
                    "packet_channel_number": 0,
                    "packet_sequence_version": 0,
                    "packet_sequence_number": 0,
                    "packet_sending_time_ns": 0,
                    "packet_parse_ok": False,
                    "error_code": "",
                    "error_text": "",
                }
                try:
                    udp_payload, _, _ = extract_udp_payload(packet)
                except Exception as exc:  # noqa: BLE001
                    packet_row["error_code"] = "udp"
                    packet_row["error_text"] = str(exc)
                    self._append_packet(packet_row)
                    continue
                packet_row["udp_payload_len"] = len(udp_payload)
                if len(udp_payload) < PACKET_HEADER_STRUCT.size:
                    packet_row["error_code"] = "packet_header"
                    packet_row["error_text"] = "payload too small"
                    self._append_packet(packet_row)
                    continue
                packet_header = PacketHeader.parse(udp_payload[: PACKET_HEADER_STRUCT.size])
                packet_row["packet_channel_number"] = packet_header.channel_number
                packet_row["packet_sequence_version"] = packet_header.sequence_version
                packet_row["packet_sequence_number"] = packet_header.sequence_number
                packet_row["packet_sending_time_ns"] = packet_header.sending_time_ns
                packet_row["packet_parse_ok"] = True
                self._append_packet(packet_row)

                cursor = PACKET_HEADER_STRUCT.size
                message_index = 0
                payload_len = len(udp_payload)
                while cursor + FRAMING_HEADER_STRUCT.size <= payload_len:
                    framing = FramingHeader.parse(udp_payload[cursor : cursor + FRAMING_HEADER_STRUCT.size])
                    cursor += FRAMING_HEADER_STRUCT.size
                    if framing.message_length == 0 or cursor + framing.message_length > payload_len:
                        break
                    message_buffer = udp_payload[cursor : cursor + framing.message_length]
                    cursor += framing.message_length
                    if len(message_buffer) < MESSAGE_HEADER_STRUCT.size:
                        continue
                    msg_header = MessageHeader.parse(message_buffer[: MESSAGE_HEADER_STRUCT.size])
                    body = message_buffer[MESSAGE_HEADER_STRUCT.size :]
                    self._append_message(
                        {
                            "source_file": meta.source_file,
                            "packet_index_in_file": packet_index,
                            "message_index_in_packet": message_index,
                            "message_offset_in_payload": cursor - framing.message_length,
                            "message_length": framing.message_length,
                            "encoding_type": framing.encoding_type,
                            "block_length": msg_header.block_length,
                            "template_id": msg_header.template_id,
                            "schema_id": msg_header.schema_id,
                            "schema_version": msg_header.version,
                            "body_length": len(body),
                            "packet_channel_number": packet_header.channel_number,
                            "packet_sequence_version": packet_header.sequence_version,
                            "packet_sequence_number": packet_header.sequence_number,
                            "packet_sending_time_ns": packet_header.sending_time_ns,
                        }
                    )
                    ctx = {
                        "source_file": meta.source_file,
                        "channel_hint": meta.channel_hint,
                        "feed_leg": meta.feed_leg,
                        "packet_sequence_number": packet_header.sequence_number,
                        "packet_sending_time_ns": packet_header.sending_time_ns,
                    }
                    decode_body(msg_header.template_id, body, ctx, self.rowset)
                    message_index += 1
        self._close()
        if self.output_dir is not None:
            return {name: str((self.output_dir / f"{name}.parquet")) for name in ROWSET_TABLES.keys()}
        result = self.rowset.to_dict()
        result["packets"] = self.packets
        result["messages"] = self.messages
        return result


def parse_pcap_file(
    path: Path | str, output_dir: Optional[Path] = None, max_packets: Optional[int] = None
) -> Dict[str, Dict[str, list]]:
    parser = PythonParser(output_dir=output_dir, max_packets=max_packets)
    return parser.parse_file(path, max_packets=max_packets)
