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

struct FeedKey {
    std::int32_t channel{0};
    std::string feed;

    bool operator==(const FeedKey &other) const noexcept
    {
        return channel == other.channel && feed == other.feed;
    }
};

struct FeedKeyHasher {
    std::size_t operator()(const FeedKey &key) const noexcept
    {
        const auto h1 = std::hash<std::int32_t>{}(key.channel);
        const auto h2 = std::hash<std::string>{}(key.feed);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

template <typename Row>
using ChannelWriterMap = std::unordered_map<std::int32_t, std::unique_ptr<TableStreamWriter<Row>>>;

template <typename Row>
using FeedWriterMap = std::unordered_map<FeedKey, std::unique_ptr<TableStreamWriter<Row>>, FeedKeyHasher>;

using SchemaFactory = std::shared_ptr<parquet::schema::GroupNode> (*)();

class NativeDecoder {
  public:
    explicit NativeDecoder(DecodeOptions options = {});
    std::unordered_map<std::string, std::int64_t> parse_files(
        const std::vector<std::filesystem::path> &files, const OutputOptions &output);

  private:
    std::unordered_map<std::string, std::int64_t> parse_files_sequential(
        const std::vector<FileMetadata> &metas, const OutputOptions &output);
    void parse_one(const FileMetadata &meta);
    void open_writers(const OutputOptions &output);
    void close_writers();
    bool should_emit(const std::string &table) const;
    void log_error(const FileMetadata &meta, const std::string &stage,
                   std::uint64_t packet_index, std::uint32_t message_index,
                   std::uint16_t template_id, const std::string &code,
                   const std::string &text, const std::string &raw_hex);
    std::filesystem::path channel_path(std::int32_t channel);
    std::filesystem::path feed_path(std::int32_t channel, const std::string &feed);
    std::shared_ptr<parquet::schema::GroupNode> schema_for(const std::string &table,
                                                           SchemaFactory factory);

    template <typename Row>
    TableStreamWriter<Row> *get_channel_writer(ChannelWriterMap<Row> &storage, std::int32_t channel,
                                               const std::string &table,
                                               SchemaFactory schema_factory,
                                               typename TableStreamWriter<Row>::WriteFunc func);

    template <typename Row>
    TableStreamWriter<Row> *get_feed_writer(FeedWriterMap<Row> &storage, std::int32_t channel,
                                            const std::string &feed, const std::string &table,
                                            SchemaFactory schema_factory,
                                            typename TableStreamWriter<Row>::WriteFunc func);

    DecodeOptions options_{};
    OutputOptions current_output_{};
    std::unordered_set<std::string> enabled_tables_;
    std::unordered_map<std::string, std::int64_t> row_counts_;
    ChannelWriterMap<InstrumentRow> instruments_writers_;
    ChannelWriterMap<InstrumentUnderlyingRow> instrument_underlyings_writers_;
    ChannelWriterMap<InstrumentLegRow> instrument_legs_writers_;
    ChannelWriterMap<InstrumentAttributeRow> instrument_attributes_writers_;
    ChannelWriterMap<SnapshotHeaderRow> snapshot_headers_writers_;
    ChannelWriterMap<SnapshotOrderRow> snapshot_orders_writers_;
    FeedWriterMap<IncrementalOrderRow> incremental_orders_writers_;
    FeedWriterMap<IncrementalDeleteRow> incremental_deletes_writers_;
    FeedWriterMap<IncrementalMassDeleteRow> incremental_mass_deletes_writers_;
    FeedWriterMap<IncrementalTradeRow> incremental_trades_writers_;
    FeedWriterMap<EmptyBookRow> incremental_empty_books_writers_;
    FeedWriterMap<ChannelResetRow> incremental_channel_resets_writers_;
    FeedWriterMap<IncrementalOtherRow> incremental_other_writers_;
    ChannelWriterMap<IncrementalOtherRow> snapshot_other_writers_;
    ChannelWriterMap<IncrementalOtherRow> instrument_other_writers_;
    ChannelWriterMap<ErrorRow> errors_writers_;
    std::unordered_map<std::int32_t, std::filesystem::path> channel_dirs_;
    std::unordered_map<FeedKey, std::filesystem::path, FeedKeyHasher> feed_dirs_;
    std::uint64_t snapshot_header_row_id_{1};
    std::unordered_map<std::int64_t, std::uint64_t> last_snapshot_row_for_security_;
    std::unordered_map<std::string, std::shared_ptr<parquet::schema::GroupNode>> schema_cache_;
};

FileMetadata infer_file_metadata(const std::filesystem::path &path);

template <typename Row>
TableStreamWriter<Row> *NativeDecoder::get_channel_writer(ChannelWriterMap<Row> &storage,
                                                          std::int32_t channel, const std::string &table,
                                                          SchemaFactory schema_factory,
                                                          typename TableStreamWriter<Row>::WriteFunc func)
{
    if (!should_emit(table)) {
        return nullptr;
    }
    auto it = storage.find(channel);
    if (it == storage.end()) {
        auto path = channel_path(channel) / (table + ".parquet");
        auto schema = schema_for(table, schema_factory);
        auto writer = std::make_unique<TableStreamWriter<Row>>(path, std::move(schema), func);
        it = storage.emplace(channel, std::move(writer)).first;
    }
    return it->second.get();
}

template <typename Row>
TableStreamWriter<Row> *NativeDecoder::get_feed_writer(FeedWriterMap<Row> &storage, std::int32_t channel,
                                                       const std::string &feed, const std::string &table,
                                                       SchemaFactory schema_factory,
                                                       typename TableStreamWriter<Row>::WriteFunc func)
{
    if (!should_emit(table)) {
        return nullptr;
    }
    const bool channel_level = feed.empty() || feed == "-";
    FeedKey key{channel, channel_level ? std::string{} : feed};
    auto it = storage.find(key);
    if (it == storage.end()) {
        std::filesystem::path path;
        if (channel_level) {
            path = channel_path(channel) / (table + ".parquet");
        } else {
            path = feed_path(channel, feed) / (table + ".parquet");
        }
        auto schema = schema_for(table, schema_factory);
        auto writer = std::make_unique<TableStreamWriter<Row>>(path, std::move(schema), func);
        it = storage.emplace(std::move(key), std::move(writer)).first;
    }
    return it->second.get();
}

}  // namespace b3::sbe
