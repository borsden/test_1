"""SBE packet/framing/message headers."""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Tuple

PACKET_HEADER_STRUCT = struct.Struct("<BBH I Q")
FRAMING_HEADER_STRUCT = struct.Struct("<HH")
MESSAGE_HEADER_STRUCT = struct.Struct("<HHHH")


@dataclass
class PacketHeader:
    channel_number: int
    sequence_version: int
    sequence_number: int
    sending_time_ns: int

    @classmethod
    def parse(cls, buffer: memoryview) -> "PacketHeader":
        channel_number, reserved, sequence_version, sequence_number, sending_time = PACKET_HEADER_STRUCT.unpack_from(
            buffer, 0
        )
        return cls(
            channel_number=channel_number,
            sequence_version=sequence_version,
            sequence_number=sequence_number,
            sending_time_ns=sending_time,
        )


@dataclass
class FramingHeader:
    message_length: int
    encoding_type: int

    @classmethod
    def parse(cls, buffer: memoryview) -> "FramingHeader":
        message_length, encoding_type = FRAMING_HEADER_STRUCT.unpack_from(buffer, 0)
        return cls(message_length=message_length, encoding_type=encoding_type)


@dataclass
class MessageHeader:
    block_length: int
    template_id: int
    schema_id: int
    version: int

    @classmethod
    def parse(cls, buffer: memoryview) -> "MessageHeader":
        block_length, template_id, schema_id, version = MESSAGE_HEADER_STRUCT.unpack_from(buffer, 0)
        return cls(
            block_length=block_length,
            template_id=template_id,
            schema_id=schema_id,
            version=version,
        )
