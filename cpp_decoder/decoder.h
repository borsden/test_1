#pragma once

#include "batch_builder.h"
#include "parquet_writer.h"
#include "pcap_reader.h"

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace b3::sbe {

struct DecodeOptions {
    bool validate_schema{true};
    std::uint64_t max_packets_per_file{0};
};

struct FileMetadata {
    std::filesystem::path path;
    std::string source_file;
    std::int32_t channel_hint{0};
    std::string stream_kind;
    std::string feed_leg;
};

struct OutputOptions {
    std::filesystem::path output_dir;
    std::vector<std::string> tables;
};

class NativeDecoder {
  public:
    explicit NativeDecoder(DecodeOptions options = {});
    std::unordered_map<std::string, std::int64_t> parse_files(
        const std::vector<std::filesystem::path> &files, const OutputOptions &output);

  private:
    void parse_one(const FileMetadata &meta);
    void open_writers(const OutputOptions &output);
    void close_writers();
    bool should_emit(const std::string &table) const;
    void log_error(const FileMetadata &meta, const std::string &stage,
                   std::uint64_t packet_index, std::uint32_t message_index,
                   std::uint16_t template_id, const std::string &code,
                   const std::string &text, const std::string &raw_hex);

    DecodeOptions options_{};
    OutputOptions current_output_{};
    std::unordered_set<std::string> enabled_tables_;
    std::unordered_map<std::string, std::int64_t> row_counts_;
    std::unique_ptr<TableStreamWriter<InstrumentRow>> instruments_writer_;
    std::unique_ptr<TableStreamWriter<InstrumentUnderlyingRow>> instrument_underlyings_writer_;
    std::unique_ptr<TableStreamWriter<InstrumentLegRow>> instrument_legs_writer_;
    std::unique_ptr<TableStreamWriter<InstrumentAttributeRow>> instrument_attributes_writer_;
    std::unique_ptr<TableStreamWriter<SnapshotHeaderRow>> snapshot_headers_writer_;
    std::unique_ptr<TableStreamWriter<SnapshotOrderRow>> snapshot_orders_writer_;
    std::unique_ptr<TableStreamWriter<IncrementalOrderRow>> incremental_orders_writer_;
    std::unique_ptr<TableStreamWriter<IncrementalDeleteRow>> incremental_deletes_writer_;
    std::unique_ptr<TableStreamWriter<IncrementalMassDeleteRow>> incremental_mass_deletes_writer_;
    std::unique_ptr<TableStreamWriter<IncrementalTradeRow>> incremental_trades_writer_;
    std::unique_ptr<TableStreamWriter<IncrementalOtherRow>> incremental_other_writer_;
    std::unique_ptr<TableStreamWriter<ErrorRow>> errors_writer_;
    std::uint64_t snapshot_header_row_id_{1};
    std::unordered_map<std::int64_t, std::uint64_t> last_snapshot_row_for_security_;
};

FileMetadata infer_file_metadata(const std::filesystem::path &path);

}  // namespace b3::sbe
