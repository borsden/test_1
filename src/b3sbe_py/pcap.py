"""Minimal PCAP reader implemented in Python."""
from __future__ import annotations

import io
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Tuple

PCAP_GLOBAL_HEADER = struct.Struct("<IHHIIII")
PCAP_GLOBAL_HEADER_SWAPPED = struct.Struct(">IHHIIII")
PCAP_PACKET_HEADER = struct.Struct("<IIII")
PCAP_PACKET_HEADER_SWAPPED = struct.Struct(">IIII")

MAGIC_USEC = 0xA1B2C3D4
MAGIC_USEC_SWAPPED = 0xD4C3B2A1
MAGIC_NSEC = 0xA1B23C4D
MAGIC_NSEC_SWAPPED = 0x4D3CB2A1


@dataclass
class PcapPacket:
    timestamp_ns: int
    data: memoryview


class PcapReader:
    """Iterates packets from a PCAP file without external dependencies."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._file: io.BufferedReader | None = None
        self._swapped = False
        self._nano_precision = False

    def __enter__(self) -> "PcapReader":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        if self._file is not None:
            return
        stream = self.path.open("rb")
        header_bytes = stream.read(PCAP_GLOBAL_HEADER.size)
        if len(header_bytes) != PCAP_GLOBAL_HEADER.size:
            raise IOError("File too short for PCAP header")
        magic = struct.unpack_from("<I", header_bytes)[0]
        if magic == MAGIC_USEC:
            parser = PCAP_GLOBAL_HEADER
        elif magic == MAGIC_NSEC:
            parser = PCAP_GLOBAL_HEADER
            self._nano_precision = True
        elif magic == MAGIC_USEC_SWAPPED:
            parser = PCAP_GLOBAL_HEADER_SWAPPED
            self._swapped = True
        elif magic == MAGIC_NSEC_SWAPPED:
            parser = PCAP_GLOBAL_HEADER_SWAPPED
            self._swapped = True
            self._nano_precision = True
        else:
            stream.close()
            raise ValueError(f"Unknown PCAP magic: 0x{magic:08x}")
        parser.unpack(header_bytes)
        self._file = stream

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def packets(self) -> Iterator[PcapPacket]:
        if self._file is None:
            self.open()
        assert self._file is not None
        header_struct = PCAP_PACKET_HEADER_SWAPPED if self._swapped else PCAP_PACKET_HEADER
        read = self._file.read
        while True:
            header_bytes = read(header_struct.size)
            if not header_bytes:
                return
            if len(header_bytes) != header_struct.size:
                raise IOError("Truncated PCAP packet header")
            ts_sec, ts_frac, incl_len, orig_len = header_struct.unpack(header_bytes)
            payload = read(incl_len)
            if len(payload) != incl_len:
                raise IOError("Truncated packet payload")
            timestamp_ns = ts_sec * 1_000_000_000
            if self._nano_precision:
                timestamp_ns += ts_frac
            else:
                timestamp_ns += ts_frac * 1_000
            yield PcapPacket(timestamp_ns=timestamp_ns, data=memoryview(payload))


EtherHeader = struct.Struct(">6s6sH")
IPV4_HEADER_MIN = 20
UDP_HEADER = struct.Struct(">HHHH")


def extract_udp_payload(packet: PcapPacket) -> Tuple[memoryview, int, int]:
    """Strip Ethernet/IP/UDP headers and return payload view."""
    data = packet.data
    if len(data) < EtherHeader.size:
        raise ValueError("Packet shorter than Ethernet header")
    _, _, ether_type = EtherHeader.unpack_from(data, 0)
    offset = EtherHeader.size
    if ether_type == 0x8100 or ether_type == 0x88A8:
        if len(data) < offset + 4:
            raise ValueError("Packet shorter than VLAN header")
        ether_type = struct.unpack_from(">H", data, offset + 2)[0]
        offset += 4
    if ether_type != 0x0800:
        raise ValueError("Non-IPv4 packet")
    if len(data) < offset + IPV4_HEADER_MIN:
        raise ValueError("IPv4 header truncated")
    version_ihl = data[offset]
    ihl = (version_ihl & 0x0F) * 4
    protocol = data[offset + 9]
    total_length = struct.unpack_from(">H", data, offset + 2)[0]
    if protocol != 17:
        raise ValueError("Non-UDP packet")
    ip_offset = offset + ihl
    if len(data) < ip_offset + UDP_HEADER.size:
        raise ValueError("UDP header truncated")
    _, _, udp_length, _ = UDP_HEADER.unpack_from(data, ip_offset)
    payload_offset = ip_offset + UDP_HEADER.size
    payload_end = payload_offset + udp_length - UDP_HEADER.size
    if len(data) < payload_end:
        raise ValueError("UDP payload truncated")
    payload = data[payload_offset:payload_end]
    src_port, dst_port = struct.unpack_from(">HH", data, ip_offset)
    return payload, src_port, dst_port
