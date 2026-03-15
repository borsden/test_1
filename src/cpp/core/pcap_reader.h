#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace b3::sbe {

struct PcapPacketView {
    const std::uint8_t *data{nullptr};
    std::size_t length{0};
    std::uint64_t timestamp_ns{0};
};

class PcapReader {
  public:
    explicit PcapReader(const std::filesystem::path &path);
    ~PcapReader();

    bool good() const { return good_; }
    const std::string &error() const { return error_; }
    bool next(PcapPacketView &out);
    const std::filesystem::path &path() const { return path_; }

  private:
    bool read_global_header();
    std::uint32_t read_u32(const char *ptr) const;
    std::uint16_t read_u16(const char *ptr) const;

    std::filesystem::path path_;
    std::ifstream stream_;
    bool good_{false};
    bool swap_endian_{false};
    bool nano_precision_{false};
    std::string error_;
    std::vector<std::uint8_t> buffer_;
};

}  // namespace b3::sbe
