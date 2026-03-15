#include "core/pcap_reader.h"

#include <cstring>

namespace b3::sbe {
namespace {
constexpr std::uint32_t kMagicMicro = 0xa1b2c3d4u;
constexpr std::uint32_t kMagicMicroSwapped = 0xd4c3b2a1u;
constexpr std::uint32_t kMagicNano = 0xa1b23c4du;
constexpr std::uint32_t kMagicNanoSwapped = 0x4d3cb2a1u;

struct PcapGlobalHeaderRaw {
    std::uint32_t magic_number;
    std::uint16_t version_major;
    std::uint16_t version_minor;
    std::int32_t thiszone;
    std::uint32_t sigfigs;
    std::uint32_t snaplen;
    std::uint32_t network;
};

struct PcapRecordHeaderRaw {
    std::uint32_t ts_sec;
    std::uint32_t ts_frac;
    std::uint32_t incl_len;
    std::uint32_t orig_len;
};

std::uint32_t swap32(std::uint32_t v) {
    return (v >> 24u) | ((v >> 8u) & 0xff00u) | ((v & 0xff00u) << 8u) | (v << 24u);
}

std::uint16_t swap16(std::uint16_t v) {
    return static_cast<std::uint16_t>((v >> 8u) | (v << 8u));
}

}  // namespace

PcapReader::PcapReader(const std::filesystem::path &path) : path_(path) {
    stream_.open(path_, std::ios::binary);
    if (!stream_) {
        error_ = "unable to open pcap file";
        return;
    }

    if (!read_global_header()) {
        return;
    }

    good_ = true;
}

PcapReader::~PcapReader() = default;

bool PcapReader::read_global_header() {
    PcapGlobalHeaderRaw header{};
    stream_.read(reinterpret_cast<char *>(&header), sizeof(header));
    if (!stream_) {
        error_ = "unable to read pcap global header";
        return false;
    }

    std::uint32_t magic = header.magic_number;
    if (magic == kMagicMicro || magic == kMagicNano) {
        swap_endian_ = false;
        nano_precision_ = (magic == kMagicNano);
    } else if (magic == kMagicMicroSwapped || magic == kMagicNanoSwapped) {
        swap_endian_ = true;
        nano_precision_ = (magic == kMagicNanoSwapped);
    } else {
        error_ = "unsupported pcap magic";
        return false;
    }

    if (swap_endian_) {
        header.version_major = swap16(header.version_major);
        header.version_minor = swap16(header.version_minor);
    }

    if (header.version_major != 2) {
        error_ = "unexpected pcap version";
        return false;
    }

    return true;
}

std::uint32_t PcapReader::read_u32(const char *ptr) const {
    std::uint32_t val;
    std::memcpy(&val, ptr, sizeof(val));
    return swap_endian_ ? swap32(val) : val;
}

std::uint16_t PcapReader::read_u16(const char *ptr) const {
    std::uint16_t val;
    std::memcpy(&val, ptr, sizeof(val));
    return swap_endian_ ? swap16(val) : val;
}

bool PcapReader::next(PcapPacketView &out) {
    if (!good_) {
        return false;
    }

    PcapRecordHeaderRaw record{};
    stream_.read(reinterpret_cast<char *>(&record), sizeof(record));
    if (!stream_) {
        return false;
    }

    if (swap_endian_) {
        record.ts_sec = swap32(record.ts_sec);
        record.ts_frac = swap32(record.ts_frac);
        record.incl_len = swap32(record.incl_len);
        record.orig_len = swap32(record.orig_len);
    }

    buffer_.resize(record.incl_len);
    stream_.read(reinterpret_cast<char *>(buffer_.data()), record.incl_len);
    if (!stream_) {
        good_ = false;
        error_ = "truncated packet";
        return false;
    }

    std::uint64_t ts_ns = static_cast<std::uint64_t>(record.ts_sec) * 1'000'000'000ull;
    if (nano_precision_) {
        ts_ns += record.ts_frac;
    } else {
        ts_ns += static_cast<std::uint64_t>(record.ts_frac) * 1'000ull;
    }

    out.data = buffer_.data();
    out.length = record.incl_len;
    out.timestamp_ns = ts_ns;
    return true;
}

}  // namespace b3::sbe
