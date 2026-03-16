#include "decoder.h"

#include "sbe-generated/b3_umdf_mbo_sbe/DeleteOrder_MBO_51.h"
#include "sbe-generated/b3_umdf_mbo_sbe/FramingHeader.h"
#include "sbe-generated/b3_umdf_mbo_sbe/MassDeleteOrders_MBO_52.h"
#include "sbe-generated/b3_umdf_mbo_sbe/MessageHeader.h"
#include "sbe-generated/b3_umdf_mbo_sbe/Order_MBO_50.h"
#include "sbe-generated/b3_umdf_mbo_sbe/PacketHeader.h"
#include "sbe-generated/b3_umdf_mbo_sbe/SecurityDefinition_12.h"
#include "sbe-generated/b3_umdf_mbo_sbe/SnapshotFullRefresh_Header_30.h"
#include "sbe-generated/b3_umdf_mbo_sbe/SnapshotFullRefresh_Orders_MBO_71.h"
#include "sbe-generated/b3_umdf_mbo_sbe/Trade_53.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

namespace b3::sbe {

namespace sbe_types = b3::umdf::mbo::sbe;

namespace {

#define INSTRUMENT_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, security_id, row.security_id) \
    X(String, security_exchange, row.security_exchange) \
    X(String, symbol, row.symbol) \
    X(String, security_desc, row.security_desc) \
    X(String, security_type, row.security_type) \
    X(Int32, security_type_raw, static_cast<int32_t>(row.security_type_raw)) \
    X(String, cfi_code, row.cfi_code) \
    X(String, asset, row.asset) \
    X(Int32, market_segment_id, row.market_segment_id) \
    X(Int32, maturity_date, static_cast<int32_t>(row.maturity_date)) \
    X(Int32, issue_date, static_cast<int32_t>(row.issue_date)) \
    X(Int64, strike_price_mantissa, row.strike_price_mantissa) \
    X(Double, strike_price, row.strike_price) \
    X(Int64, min_price_increment_mantissa, row.min_price_increment_mantissa) \
    X(Double, min_price_increment, row.min_price_increment) \
    X(Int64, contract_multiplier_mantissa, row.contract_multiplier_mantissa) \
    X(Double, contract_multiplier, row.contract_multiplier) \
    X(Int64, price_divisor_mantissa, row.price_divisor_mantissa) \
    X(Double, price_divisor, row.price_divisor) \
    X(String, currency, row.currency) \
    X(String, settl_currency, row.settl_currency) \
    X(String, base_currency, row.base_currency) \
    X(Int64, quantity_multiplier, row.quantity_multiplier) \
    X(String, security_group, row.security_group) \
    X(String, put_or_call, row.put_or_call) \
    X(Int32, put_or_call_raw, static_cast<int32_t>(row.put_or_call_raw)) \
    X(String, exercise_style, row.exercise_style) \
    X(Int32, exercise_style_raw, static_cast<int32_t>(row.exercise_style_raw)) \
    X(String, product, row.product) \
    X(Int32, product_raw, static_cast<int32_t>(row.product_raw)) \
    X(String, security_sub_type, row.security_sub_type) \
    X(Int64, trading_reference_price_mantissa, row.trading_reference_price_mantissa) \
    X(Double, trading_reference_price, row.trading_reference_price) \
    X(String, unit_of_measure, row.unit_of_measure) \
    X(Int64, unit_of_measure_qty, row.unit_of_measure_qty) \
    X(Int32, raw_template_id, static_cast<int32_t>(row.raw_template_id)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns))

#define INSTRUMENT_UNDERLYING_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int64, security_id, row.security_id) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, underlying_security_id, static_cast<int64_t>(row.underlying_security_id)) \
    X(String, underlying_security_exchange, row.underlying_security_exchange) \
    X(String, underlying_symbol, row.underlying_symbol) \
    X(String, underlying_security_type, row.underlying_security_type) \
    X(Int64, position_in_group, static_cast<int64_t>(row.position_in_group))

#define INSTRUMENT_LEG_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int64, security_id, row.security_id) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, leg_security_id, static_cast<int64_t>(row.leg_security_id)) \
    X(String, leg_symbol, row.leg_symbol) \
    X(String, leg_side, row.leg_side) \
    X(Int32, leg_side_raw, static_cast<int32_t>(row.leg_side_raw)) \
    X(Int64, leg_ratio_qty_mantissa, row.leg_ratio_qty_mantissa) \
    X(Double, leg_ratio_qty, row.leg_ratio_qty) \
    X(Int64, position_in_group, static_cast<int64_t>(row.position_in_group))

#define INSTRUMENT_ATTRIBUTE_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int64, security_id, row.security_id) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int32, attribute_type, static_cast<int32_t>(row.attribute_type)) \
    X(String, attribute_value, row.attribute_value) \
    X(Int64, position_in_group, static_cast<int64_t>(row.position_in_group))

#define SNAPSHOT_HEADER_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns)) \
    X(Int64, security_id, row.security_id) \
    X(Int32, last_msg_seq_num_processed, static_cast<int32_t>(row.last_msg_seq_num_processed)) \
    X(Int32, tot_num_bids, static_cast<int32_t>(row.tot_num_bids)) \
    X(Int32, tot_num_offers, static_cast<int32_t>(row.tot_num_offers)) \
    X(Int32, last_rpt_seq, static_cast<int32_t>(row.last_rpt_seq)) \
    X(Int64, snapshot_header_row_id, static_cast<int64_t>(row.snapshot_header_row_id))

#define SNAPSHOT_ORDER_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(Int64, snapshot_header_row_id, static_cast<int64_t>(row.snapshot_header_row_id)) \
    X(Int64, security_id, row.security_id) \
    X(String, security_exchange, row.security_exchange) \
    X(String, side, row.side) \
    X(Int32, side_raw, static_cast<int32_t>(row.side_raw)) \
    X(Int64, price_mantissa, row.price_mantissa) \
    X(Double, price, row.price) \
    X(Int64, size, row.size) \
    X(Int32, position_no, static_cast<int32_t>(row.position_no)) \
    X(Int32, entering_firm, static_cast<int32_t>(row.entering_firm)) \
    X(Int64, md_insert_timestamp_ns, static_cast<int64_t>(row.md_insert_timestamp_ns)) \
    X(Int64, secondary_order_id, static_cast<int64_t>(row.secondary_order_id)) \
    X(Int32, row_in_group, static_cast<int32_t>(row.row_in_group)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns))

#define INCREMENTAL_ORDER_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(String, feed_leg, row.feed_leg) \
    X(Int64, security_id, row.security_id) \
    X(Int32, match_event_indicator_raw, static_cast<int32_t>(row.match_event_indicator_raw)) \
    X(Int32, md_update_action_raw, static_cast<int32_t>(row.md_update_action_raw)) \
    X(String, md_update_action, row.md_update_action) \
    X(Int32, md_entry_type_raw, static_cast<int32_t>(row.md_entry_type_raw)) \
    X(String, side, row.side) \
    X(Int64, price_mantissa, row.price_mantissa) \
    X(Double, price, row.price) \
    X(Int64, size, row.size) \
    X(Int32, position_no, static_cast<int32_t>(row.position_no)) \
    X(Int32, entering_firm, static_cast<int32_t>(row.entering_firm)) \
    X(Int64, md_insert_timestamp_ns, static_cast<int64_t>(row.md_insert_timestamp_ns)) \
    X(Int64, secondary_order_id, static_cast<int64_t>(row.secondary_order_id)) \
    X(Int32, rpt_seq, static_cast<int32_t>(row.rpt_seq)) \
    X(Int64, md_entry_timestamp_ns, static_cast<int64_t>(row.md_entry_timestamp_ns)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns)) \
    X(Int32, message_index_in_packet, static_cast<int32_t>(row.message_index_in_packet))

#define INCREMENTAL_DELETE_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(String, feed_leg, row.feed_leg) \
    X(Int64, security_id, row.security_id) \
    X(Int32, match_event_indicator_raw, static_cast<int32_t>(row.match_event_indicator_raw)) \
    X(Int32, md_update_action_raw, static_cast<int32_t>(row.md_update_action_raw)) \
    X(String, md_update_action, row.md_update_action) \
    X(Int32, md_entry_type_raw, static_cast<int32_t>(row.md_entry_type_raw)) \
    X(String, side, row.side) \
    X(Int32, position_no, static_cast<int32_t>(row.position_no)) \
    X(Int64, size, row.size) \
    X(Int64, secondary_order_id, static_cast<int64_t>(row.secondary_order_id)) \
    X(Int64, md_entry_timestamp_ns, static_cast<int64_t>(row.md_entry_timestamp_ns)) \
    X(Int32, rpt_seq, static_cast<int32_t>(row.rpt_seq)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns))

#define INCREMENTAL_MASS_DELETE_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(String, feed_leg, row.feed_leg) \
    X(Int64, security_id, row.security_id) \
    X(Int32, match_event_indicator_raw, static_cast<int32_t>(row.match_event_indicator_raw)) \
    X(Int32, md_update_action_raw, static_cast<int32_t>(row.md_update_action_raw)) \
    X(String, md_update_action, row.md_update_action) \
    X(Int32, md_entry_type_raw, static_cast<int32_t>(row.md_entry_type_raw)) \
    X(String, side, row.side) \
    X(Int32, start_position, static_cast<int32_t>(row.start_position)) \
    X(Int32, end_position, static_cast<int32_t>(row.end_position)) \
    X(Int32, rpt_seq, static_cast<int32_t>(row.rpt_seq)) \
    X(Int64, md_entry_timestamp_ns, static_cast<int64_t>(row.md_entry_timestamp_ns)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns))

#define INCREMENTAL_TRADE_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, channel_hint, row.channel_hint) \
    X(String, feed_leg, row.feed_leg) \
    X(Int64, security_id, row.security_id) \
    X(Int32, match_event_indicator_raw, static_cast<int32_t>(row.match_event_indicator_raw)) \
    X(Int32, trading_session_id, static_cast<int32_t>(row.trading_session_id)) \
    X(Int32, trade_condition_raw, static_cast<int32_t>(row.trade_condition_raw)) \
    X(Int32, trd_sub_type_raw, static_cast<int32_t>(row.trd_sub_type_raw)) \
    X(Int64, price_mantissa, row.price_mantissa) \
    X(Double, price, row.price) \
    X(Int64, size, row.size) \
    X(Int64, trade_id, static_cast<int64_t>(row.trade_id)) \
    X(Int32, buyer_firm, static_cast<int32_t>(row.buyer_firm)) \
    X(Int32, seller_firm, static_cast<int32_t>(row.seller_firm)) \
    X(Int32, trade_date, static_cast<int32_t>(row.trade_date)) \
    X(Int64, md_entry_timestamp_ns, static_cast<int64_t>(row.md_entry_timestamp_ns)) \
    X(Int32, rpt_seq, static_cast<int32_t>(row.rpt_seq)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns))

#define INCREMENTAL_OTHER_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(Int32, template_id, static_cast<int32_t>(row.template_id)) \
    X(String, template_name, row.template_name) \
    X(Int64, security_id, row.security_id) \
    X(Int32, rpt_seq, static_cast<int32_t>(row.rpt_seq)) \
    X(Int64, packet_sequence_number, static_cast<int64_t>(row.packet_sequence_number)) \
    X(Int64, packet_sending_time_ns, static_cast<int64_t>(row.packet_sending_time_ns)) \
    X(String, body_hex, row.body_hex) \
    X(String, decode_status, row.decode_status)

#define ERROR_COLUMNS(X) \
    X(String, source_file, row.source_file) \
    X(String, stage, row.stage) \
    X(Int64, packet_index_in_file, static_cast<int64_t>(row.packet_index_in_file)) \
    X(Int32, message_index_in_packet, static_cast<int32_t>(row.message_index_in_packet)) \
    X(Int32, template_id, static_cast<int32_t>(row.template_id)) \
    X(String, error_code, row.error_code) \
    X(String, error_text, row.error_text) \
    X(String, raw_context_hex, row.raw_context_hex)

const std::vector<std::string> kAllTables = {
    "instruments",
    "instrument_underlyings",
    "instrument_legs",
    "instrument_attributes",
    "snapshot_headers",
    "snapshot_orders",
    "incremental_orders",
    "incremental_deletes",
    "incremental_mass_deletes",
    "incremental_trades",
    "incremental_other",
    "errors",
};

#define APPLY_SCHEMA_FIELD(type, name, expr) fields.push_back(Make##type##Column(#name));

inline void WriteString(parquet::StreamWriter &writer, const std::string &value)
{
    writer << value;
}

inline void WriteString(parquet::StreamWriter &writer, std::string_view value)
{
    writer << value;
}

inline void WriteString(parquet::StreamWriter &writer, const char *value)
{
    writer << value;
}

inline void WriteBinary(parquet::StreamWriter &writer, const std::string &value)
{
    writer << value;
}

inline void WriteInt32(parquet::StreamWriter &writer, std::int32_t value)
{
    writer << value;
}

inline void WriteInt64(parquet::StreamWriter &writer, std::int64_t value)
{
    writer << value;
}

inline void WriteDouble(parquet::StreamWriter &writer, double value)
{
    writer << value;
}

inline void WriteBool(parquet::StreamWriter &writer, bool value)
{
    writer << value;
}

#define APPLY_WRITE_FIELD(type, name, expr) Write##type(writer, expr);

#define DEFINE_SCHEMA_AND_WRITER(TABLE, ROWTYPE, COLUMNS, NAME)                                    \
    std::shared_ptr<parquet::schema::GroupNode> Make##TABLE##Schema() {                           \
        std::vector<parquet::schema::NodePtr> fields;                                             \
        fields.reserve(64);                                                                       \
        COLUMNS(APPLY_SCHEMA_FIELD)                                                               \
        return MakeStringSchema(NAME, fields);                                                    \
    }                                                                                             \
    void Write##TABLE(parquet::StreamWriter &writer, const ROWTYPE &row) {                        \
        COLUMNS(APPLY_WRITE_FIELD)                                                                \
    }

DEFINE_SCHEMA_AND_WRITER(Instrument, InstrumentRow, INSTRUMENT_COLUMNS, "instruments")
DEFINE_SCHEMA_AND_WRITER(InstrumentUnderlyings, InstrumentUnderlyingRow,
                         INSTRUMENT_UNDERLYING_COLUMNS, "instrument_underlyings")
DEFINE_SCHEMA_AND_WRITER(InstrumentLegs, InstrumentLegRow, INSTRUMENT_LEG_COLUMNS, "instrument_legs")
DEFINE_SCHEMA_AND_WRITER(InstrumentAttributes, InstrumentAttributeRow, INSTRUMENT_ATTRIBUTE_COLUMNS,
                         "instrument_attributes")
DEFINE_SCHEMA_AND_WRITER(SnapshotHeaders, SnapshotHeaderRow, SNAPSHOT_HEADER_COLUMNS, "snapshot_headers")
DEFINE_SCHEMA_AND_WRITER(SnapshotOrders, SnapshotOrderRow, SNAPSHOT_ORDER_COLUMNS, "snapshot_orders")
DEFINE_SCHEMA_AND_WRITER(IncrementalOrders, IncrementalOrderRow, INCREMENTAL_ORDER_COLUMNS,
                         "incremental_orders")
DEFINE_SCHEMA_AND_WRITER(IncrementalDeletes, IncrementalDeleteRow, INCREMENTAL_DELETE_COLUMNS,
                         "incremental_deletes")
DEFINE_SCHEMA_AND_WRITER(IncrementalMassDeletes, IncrementalMassDeleteRow,
                         INCREMENTAL_MASS_DELETE_COLUMNS, "incremental_mass_deletes")
DEFINE_SCHEMA_AND_WRITER(IncrementalTrades, IncrementalTradeRow, INCREMENTAL_TRADE_COLUMNS,
                         "incremental_trades")
DEFINE_SCHEMA_AND_WRITER(IncrementalOther, IncrementalOtherRow, INCREMENTAL_OTHER_COLUMNS,
                         "incremental_other")
DEFINE_SCHEMA_AND_WRITER(Errors, ErrorRow, ERROR_COLUMNS, "errors")
#undef APPLY_SCHEMA_FIELD
#undef APPLY_WRITE_FIELD

std::string bytes_to_hex(const std::uint8_t *data, std::size_t len) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (std::size_t i = 0; i < len; ++i) {
        const auto byte = data[i];
        out.push_back(kHex[(byte >> 4u) & 0x0fu]);
        out.push_back(kHex[byte & 0x0fu]);
    }
    return out;
}

double scale_decimal(std::int64_t mantissa, std::int8_t exponent) {
    if (mantissa == std::numeric_limits<std::int64_t>::min()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    double scale = std::pow(10.0, static_cast<double>(exponent));
    return static_cast<double>(mantissa) * scale;
}

const char *side_from_md_entry_type(sbe_types::MDEntryType::Value v) {
    switch (v) {
        case sbe_types::MDEntryType::Value::BID:
            return "buy";
        case sbe_types::MDEntryType::Value::OFFER:
            return "sell";
        default:
            return "unknown";
    }
}

std::string update_action_name(sbe_types::MDUpdateAction::Value v) {
    switch (v) {
        case sbe_types::MDUpdateAction::Value::NEW:
            return "add";
        case sbe_types::MDUpdateAction::Value::CHANGE:
            return "change";
        case sbe_types::MDUpdateAction::Value::DELETE:
            return "delete";
        case sbe_types::MDUpdateAction::Value::DELETE_THRU:
            return "delete_thru";
        case sbe_types::MDUpdateAction::Value::DELETE_FROM:
            return "delete_from";
        case sbe_types::MDUpdateAction::Value::OVERLAY:
            return "overlay";
        default:
            return "unknown";
    }
}

constexpr std::uint16_t kExpectedSchemaId = sbe_types::MessageHeader::sbeSchemaId();
constexpr std::uint16_t kExpectedSchemaVersion = sbe_types::MessageHeader::sbeSchemaVersion();
constexpr std::uint16_t kExpectedEncodingType = 0xeb50;

std::string template_name(std::uint16_t template_id) {
    switch (template_id) {
        case 12:
            return "SecurityDefinition_12";
        case 30:
            return "SnapshotFullRefresh_Header_30";
        case 50:
            return "Order_MBO_50";
        case 51:
            return "DeleteOrder_MBO_51";
        case 52:
            return "MassDeleteOrders_MBO_52";
        case 53:
            return "Trade_53";
        case 71:
            return "SnapshotFullRefresh_Orders_MBO_71";
        default:
            return "unknown";
    }
}

std::string derive_stream_kind(const std::string &token) {
    if (token.empty()) {
        return "unknown";
    }
    std::string lower = token;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (lower.find("incremental") != std::string::npos) {
        return "Incremental";
    }
    if (lower.find("snapshot") != std::string::npos) {
        return "Snapshot";
    }
    if (lower.find("instrument") != std::string::npos) {
        return "Instrument";
    }
    return token;
}

}  // namespace

FileMetadata infer_file_metadata(const std::filesystem::path &path) {
    FileMetadata meta{};
    meta.path = path;
    meta.source_file = path.filename().string();
    const auto stem = path.stem().string();
    std::vector<std::string> parts;
    std::stringstream ss(stem);
    std::string token;
    while (std::getline(ss, token, '_')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    if (!parts.empty()) {
        try {
            meta.channel_hint = std::stoi(parts[0]);
        } catch (const std::exception &) {
            meta.channel_hint = 0;
        }
    }
    if (parts.size() >= 2) {
        meta.stream_kind = derive_stream_kind(parts[1]);
    } else {
        meta.stream_kind = "unknown";
    }
    if (parts.size() >= 3) {
        std::string_view leg_view(parts[2]);
        if (leg_view.rfind("feed", 0) == 0 && leg_view.size() >= 5) {
            meta.feed_leg = std::string(1, static_cast<char>(std::toupper(leg_view.back())));
        }
    }
    if (meta.feed_leg.empty()) {
        meta.feed_leg = "-";
    }
    return meta;
}

NativeDecoder::NativeDecoder(DecodeOptions options) : options_(options) {}

std::unordered_map<std::string, std::int64_t> NativeDecoder::parse_files(
    const std::vector<std::filesystem::path> &files, const OutputOptions &output) {
    std::vector<FileMetadata> metas;
    metas.reserve(files.size());
    for (const auto &p : files) {
        metas.push_back(infer_file_metadata(p));
    }
    open_writers(output);
    for (const auto &meta : metas) {
        parse_one(meta);
    }
    close_writers();
    return row_counts_;
}

void NativeDecoder::open_writers(const OutputOptions &output)
{
    current_output_ = output;
    std::filesystem::create_directories(current_output_.output_dir);
    enabled_tables_.clear();
    if (current_output_.tables.empty()) {
        enabled_tables_.insert(kAllTables.begin(), kAllTables.end());
    } else {
        enabled_tables_.insert(current_output_.tables.begin(), current_output_.tables.end());
    }
    row_counts_.clear();

    if (should_emit("instruments")) {
        instruments_writer_ = std::make_unique<TableStreamWriter<InstrumentRow>>(current_output_.output_dir,
                                                                                "instruments",
                                                                                MakeInstrumentSchema(),
                                                                                WriteInstrument);
        row_counts_["instruments"] = 0;
    } else {
        instruments_writer_.reset();
    }

    if (should_emit("instrument_underlyings")) {
        instrument_underlyings_writer_ =
            std::make_unique<TableStreamWriter<InstrumentUnderlyingRow>>(current_output_.output_dir,
                                                                         "instrument_underlyings",
                                                                         MakeInstrumentUnderlyingsSchema(),
                                                                         WriteInstrumentUnderlyings);
        row_counts_["instrument_underlyings"] = 0;
    } else {
        instrument_underlyings_writer_.reset();
    }

    if (should_emit("instrument_legs")) {
        instrument_legs_writer_ =
            std::make_unique<TableStreamWriter<InstrumentLegRow>>(current_output_.output_dir, "instrument_legs",
                                                                  MakeInstrumentLegsSchema(), WriteInstrumentLegs);
        row_counts_["instrument_legs"] = 0;
    } else {
        instrument_legs_writer_.reset();
    }

    if (should_emit("instrument_attributes")) {
        instrument_attributes_writer_ = std::make_unique<TableStreamWriter<InstrumentAttributeRow>>(
            current_output_.output_dir, "instrument_attributes", MakeInstrumentAttributesSchema(),
            WriteInstrumentAttributes);
        row_counts_["instrument_attributes"] = 0;
    } else {
        instrument_attributes_writer_.reset();
    }

    if (should_emit("snapshot_headers")) {
        snapshot_headers_writer_ = std::make_unique<TableStreamWriter<SnapshotHeaderRow>>(
            current_output_.output_dir, "snapshot_headers", MakeSnapshotHeadersSchema(), WriteSnapshotHeaders);
        row_counts_["snapshot_headers"] = 0;
    } else {
        snapshot_headers_writer_.reset();
    }

    if (should_emit("snapshot_orders")) {
        snapshot_orders_writer_ = std::make_unique<TableStreamWriter<SnapshotOrderRow>>(
            current_output_.output_dir, "snapshot_orders", MakeSnapshotOrdersSchema(), WriteSnapshotOrders);
        row_counts_["snapshot_orders"] = 0;
    } else {
        snapshot_orders_writer_.reset();
    }

    if (should_emit("incremental_orders")) {
        incremental_orders_writer_ = std::make_unique<TableStreamWriter<IncrementalOrderRow>>(
            current_output_.output_dir, "incremental_orders", MakeIncrementalOrdersSchema(), WriteIncrementalOrders);
        row_counts_["incremental_orders"] = 0;
    } else {
        incremental_orders_writer_.reset();
    }

    if (should_emit("incremental_deletes")) {
        incremental_deletes_writer_ = std::make_unique<TableStreamWriter<IncrementalDeleteRow>>(
            current_output_.output_dir, "incremental_deletes", MakeIncrementalDeletesSchema(),
            WriteIncrementalDeletes);
        row_counts_["incremental_deletes"] = 0;
    } else {
        incremental_deletes_writer_.reset();
    }

    if (should_emit("incremental_mass_deletes")) {
        incremental_mass_deletes_writer_ = std::make_unique<TableStreamWriter<IncrementalMassDeleteRow>>(
            current_output_.output_dir, "incremental_mass_deletes", MakeIncrementalMassDeletesSchema(),
            WriteIncrementalMassDeletes);
        row_counts_["incremental_mass_deletes"] = 0;
    } else {
        incremental_mass_deletes_writer_.reset();
    }

    if (should_emit("incremental_trades")) {
        incremental_trades_writer_ = std::make_unique<TableStreamWriter<IncrementalTradeRow>>(
            current_output_.output_dir, "incremental_trades", MakeIncrementalTradesSchema(), WriteIncrementalTrades);
        row_counts_["incremental_trades"] = 0;
    } else {
        incremental_trades_writer_.reset();
    }

    if (should_emit("incremental_other")) {
        incremental_other_writer_ = std::make_unique<TableStreamWriter<IncrementalOtherRow>>(
            current_output_.output_dir, "incremental_other", MakeIncrementalOtherSchema(), WriteIncrementalOther);
        row_counts_["incremental_other"] = 0;
    } else {
        incremental_other_writer_.reset();
    }

    if (should_emit("errors")) {
        errors_writer_ = std::make_unique<TableStreamWriter<ErrorRow>>(current_output_.output_dir, "errors",
                                                                      MakeErrorsSchema(), WriteErrors);
        row_counts_["errors"] = 0;
    } else {
        errors_writer_.reset();
    }
    snapshot_header_row_id_ = 1;
    last_snapshot_row_for_security_.clear();
}

void NativeDecoder::close_writers()
{
    auto finalize = [&](const std::string &name, auto &writer) {
        if (writer) {
            writer->Close();
            row_counts_[name] = writer->rows();
            writer.reset();
        } else if (should_emit(name) && !row_counts_.count(name)) {
            row_counts_[name] = 0;
        }
    };

    finalize("instruments", instruments_writer_);
    finalize("instrument_underlyings", instrument_underlyings_writer_);
    finalize("instrument_legs", instrument_legs_writer_);
    finalize("instrument_attributes", instrument_attributes_writer_);
    finalize("snapshot_headers", snapshot_headers_writer_);
    finalize("snapshot_orders", snapshot_orders_writer_);
    finalize("incremental_orders", incremental_orders_writer_);
    finalize("incremental_deletes", incremental_deletes_writer_);
    finalize("incremental_mass_deletes", incremental_mass_deletes_writer_);
    finalize("incremental_trades", incremental_trades_writer_);
    finalize("incremental_other", incremental_other_writer_);
    finalize("errors", errors_writer_);
}

bool NativeDecoder::should_emit(const std::string &table) const
{
    return enabled_tables_.find(table) != enabled_tables_.end();
}

void NativeDecoder::log_error(const FileMetadata &meta, const std::string &stage,
                              std::uint64_t packet_index, std::uint32_t message_index,
                              std::uint16_t template_id, const std::string &code,
                              const std::string &text, const std::string &raw_hex)
{
    if (!errors_writer_) {
        return;
    }
    ErrorRow row;
    row.source_file = meta.source_file;
    row.stage = stage;
    row.packet_index_in_file = packet_index;
    row.message_index_in_packet = message_index;
    row.template_id = template_id;
    row.error_code = code;
    row.error_text = text;
    row.raw_context_hex = raw_hex;
    errors_writer_->Append(row);
}

struct UdpPayloadView {
    const std::uint8_t *data{nullptr};
    std::size_t size{0};
};

bool extract_udp_payload(const PcapPacketView &packet,
                         UdpPayloadView &view,
                         std::string &error_text) {
    const std::uint8_t *cursor = packet.data;
    const std::size_t total = packet.length;
    if (total < 14) {
        error_text = "packet too short for ethernet header";
        return false;
    }
    std::size_t offset = 14;
    std::uint16_t ether_type = (static_cast<std::uint16_t>(cursor[12]) << 8u) | cursor[13];
    if (ether_type == 0x8100 || ether_type == 0x88a8) {
        if (total < offset + 4) {
            error_text = "packet too short for vlan tag";
            return false;
        }
        ether_type = (static_cast<std::uint16_t>(cursor[offset]) << 8u) | cursor[offset + 1];
        offset += 4;
    }
    if (ether_type != 0x0800) {
        error_text = "non-ipv4 packet";
        return false;
    }
    if (total < offset + 20) {
        error_text = "packet too short for ipv4";
        return false;
    }
    const std::uint8_t ihl = cursor[offset] & 0x0fU;
    if (ihl < 5) {
        error_text = "invalid ipv4 header";
        return false;
    }
    const std::uint8_t protocol = cursor[offset + 9];
    if (protocol != 0x11) {
        error_text = "non-udp packet";
        return false;
    }
    const std::size_t ip_header_len = static_cast<std::size_t>(ihl) * 4;
    offset += ip_header_len;
    if (total < offset + 8) {
        error_text = "packet too short for udp header";
        return false;
    }
    const std::uint16_t udp_length = static_cast<std::uint16_t>(cursor[offset + 4] << 8u | cursor[offset + 5]);
    if (udp_length < 8) {
        error_text = "invalid udp length";
        return false;
    }
    if (total < offset + udp_length) {
        error_text = "truncated udp payload";
        return false;
    }
    view.data = cursor + offset + 8;
    view.size = udp_length - 8;
    return true;
}

struct MessageContext {
    const FileMetadata *meta{nullptr};
    const PacketRow *packet_row{nullptr};
    std::uint32_t message_index{0};
    std::uint16_t template_id{0};
};

void decode_security_definition(const MessageContext &ctx,
                                sbe_types::SecurityDefinition_12 &msg,
                                TableStreamWriter<InstrumentRow> *instruments,
                                TableStreamWriter<InstrumentLegRow> *legs_writer,
                                TableStreamWriter<InstrumentUnderlyingRow> *underlyings_writer,
                                TableStreamWriter<InstrumentAttributeRow> *attributes_writer) {
    InstrumentRow row{};
    row.source_file = ctx.meta->source_file;
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    row.security_id = static_cast<std::int64_t>(msg.securityID());
    row.security_exchange = msg.getSecurityExchangeAsString();
    row.symbol = msg.getSymbolAsString();
    row.security_desc = msg.getSecurityDescAsString();
    row.security_group = msg.getSecurityGroupAsString();
    row.security_type_raw = static_cast<std::uint16_t>(msg.securityTypeRaw());
    row.security_type = sbe_types::SecurityType::c_str(msg.securityType());
    row.security_sub_type = std::to_string(msg.securitySubType());
    row.cfi_code = msg.getCfiCodeAsString();
    row.asset = msg.getAssetAsString();
    row.market_segment_id = static_cast<std::int32_t>(msg.marketSegmentID());
    row.maturity_date = msg.maturityDate();
    row.issue_date = msg.issueDate();
    auto &strike_price = msg.strikePrice();
    row.strike_price_mantissa = strike_price.mantissa();
    row.strike_price = scale_decimal(row.strike_price_mantissa, sbe_types::PriceOptional::exponent());
    auto &min_price_increment = msg.minPriceIncrement();
    row.min_price_increment_mantissa = min_price_increment.mantissa();
    row.min_price_increment = scale_decimal(row.min_price_increment_mantissa, sbe_types::Fixed8::exponent());
    auto &contract_multiplier = msg.contractMultiplier();
    row.contract_multiplier_mantissa = contract_multiplier.mantissa();
    row.contract_multiplier = scale_decimal(row.contract_multiplier_mantissa, sbe_types::Fixed8::exponent());
    auto &price_divisor = msg.priceDivisor();
    row.price_divisor_mantissa = price_divisor.mantissa();
    row.price_divisor = scale_decimal(row.price_divisor_mantissa, sbe_types::Fixed8::exponent());
    row.currency = msg.getCurrencyAsString();
    row.settl_currency = msg.getSettlCurrencyAsString();
    row.put_or_call_raw = msg.putOrCallRaw();
    row.put_or_call = sbe_types::PutOrCall::c_str(msg.putOrCall());
    row.exercise_style_raw = msg.exerciseStyleRaw();
    row.exercise_style = sbe_types::ExerciseStyle::c_str(msg.exerciseStyle());
    row.product_raw = static_cast<std::uint8_t>(msg.product());
    row.product = sbe_types::Product::c_str(msg.product());
    row.raw_template_id = sbe_types::SecurityDefinition_12::sbeTemplateId();
    if (instruments) {
        instruments->Append(row);
    }

    std::uint64_t position = 0;
    auto leg_group = msg.noLegs();
    leg_group.forEach([&](sbe_types::SecurityDefinition_12::NoLegs &leg) {
        InstrumentLegRow leg_row{};
        leg_row.source_file = ctx.meta->source_file;
        leg_row.security_id = row.security_id;
        leg_row.packet_sequence_number = row.packet_sequence_number;
        leg_row.leg_security_id = leg.legSecurityID();
        leg_row.leg_symbol = leg.getLegSymbolAsString();
        leg_row.leg_side_raw = leg.legSideRaw();
        switch (static_cast<sbe_types::Side::Value>(leg_row.leg_side_raw)) {
            case sbe_types::Side::Value::BUY:
                leg_row.leg_side = "buy";
                break;
            case sbe_types::Side::Value::SELL:
                leg_row.leg_side = "sell";
                break;
            default:
                leg_row.leg_side = "unknown";
                break;
        }
        leg_row.leg_ratio_qty_mantissa = leg.legRatioQty().mantissa();
        leg_row.leg_ratio_qty = scale_decimal(leg_row.leg_ratio_qty_mantissa, sbe_types::RatioQty::exponent());
        leg_row.position_in_group = position++;
        if (legs_writer) {
            legs_writer->Append(leg_row);
        }
    });

    position = 0;
    auto underlyings_group = msg.noUnderlyings();
    underlyings_group.forEach([&](sbe_types::SecurityDefinition_12::NoUnderlyings &underlying) {
        InstrumentUnderlyingRow underlying_row{};
        underlying_row.source_file = ctx.meta->source_file;
        underlying_row.security_id = row.security_id;
        underlying_row.packet_sequence_number = row.packet_sequence_number;
        underlying_row.underlying_security_id = underlying.underlyingSecurityID();
        underlying_row.underlying_security_exchange = underlying.getUnderlyingSecurityExchangeAsString();
        underlying_row.underlying_symbol = underlying.getUnderlyingSymbolAsString();
        underlying_row.position_in_group = position++;
        if (underlyings_writer) {
            underlyings_writer->Append(underlying_row);
        }
    });

    position = 0;
    auto attributes_group = msg.noInstrAttribs();
    attributes_group.forEach([&](sbe_types::SecurityDefinition_12::NoInstrAttribs &attr) {
        InstrumentAttributeRow attr_row{};
        attr_row.source_file = ctx.meta->source_file;
        attr_row.security_id = row.security_id;
        attr_row.packet_sequence_number = row.packet_sequence_number;
        attr_row.attribute_type = attr.instrAttribTypeRaw();
        const auto value_raw = attr.instrAttribValueRaw();
        try {
            attr_row.attribute_value =
                sbe_types::InstrAttribValue::c_str(sbe_types::InstrAttribValue::get(value_raw));
        } catch (const std::exception &) {
            attr_row.attribute_value = std::to_string(static_cast<int>(value_raw));
        }
        attr_row.position_in_group = position++;
        if (attributes_writer) {
            attributes_writer->Append(attr_row);
        }
    });
}

void decode_snapshot_header(const MessageContext &ctx,
                            const sbe_types::SnapshotFullRefresh_Header_30 &msg,
                            TableStreamWriter<SnapshotHeaderRow> *headers,
                            std::uint64_t &next_row_id,
                            std::unordered_map<std::int64_t, std::uint64_t> &row_map) {
    SnapshotHeaderRow row{};
    row.source_file = ctx.meta->source_file;
    row.channel_hint = ctx.meta->channel_hint;
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    row.security_id = msg.securityID();
    row.last_msg_seq_num_processed = msg.lastMsgSeqNumProcessed();
    row.tot_num_bids = msg.totNumBids();
    row.tot_num_offers = msg.totNumOffers();
    row.last_rpt_seq = msg.lastRptSeq();
    row.snapshot_header_row_id = next_row_id++;
    if (headers) {
        headers->Append(row);
    }
    row_map[row.security_id] = row.snapshot_header_row_id;
}

void decode_snapshot_orders(const MessageContext &ctx,
                            sbe_types::SnapshotFullRefresh_Orders_MBO_71 &msg,
                            TableStreamWriter<SnapshotOrderRow> *orders,
                            const std::unordered_map<std::int64_t, std::uint64_t> &row_map) {
    auto &group = msg.noMDEntries();
    std::uint32_t row_index = 0;
    std::uint64_t header_row_id = 0;
    auto it = row_map.find(msg.securityID());
    if (it != row_map.end()) {
        header_row_id = it->second;
    }
    group.forEach([&](sbe_types::SnapshotFullRefresh_Orders_MBO_71::NoMDEntries &entry) {
        SnapshotOrderRow row{};
        row.source_file = ctx.meta->source_file;
        row.channel_hint = ctx.meta->channel_hint;
        row.snapshot_header_row_id = header_row_id;
        row.security_id = msg.securityID();
        row.security_exchange = msg.getSecurityExchangeAsString();
        const auto entry_type = entry.mDEntryType();
        row.side_raw = static_cast<std::uint8_t>(entry_type);
        row.side = side_from_md_entry_type(entry_type);
        row.price_mantissa = entry.mDEntryPx().mantissa();
        row.price = scale_decimal(row.price_mantissa, sbe_types::PriceOptional::exponent());
        row.size = entry.mDEntrySize();
        row.position_no = entry.mDEntryPositionNo();
        row.entering_firm = entry.enteringFirm();
        row.md_insert_timestamp_ns = entry.mDInsertTimestamp().time();
        row.secondary_order_id = entry.secondaryOrderID();
        row.row_in_group = row_index++;
        row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
        row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
        if (orders) {
            orders->Append(row);
        }
    });
}

IncrementalOrderRow make_incremental_order_row(const MessageContext &ctx,
                                               sbe_types::Order_MBO_50 &msg) {
    IncrementalOrderRow row{};
    row.source_file = ctx.meta->source_file;
    row.channel_hint = ctx.meta->channel_hint;
    row.feed_leg = ctx.meta->feed_leg;
    row.security_id = msg.securityID();
    row.match_event_indicator_raw = msg.matchEventIndicator().rawValue();
    row.md_update_action_raw = msg.mDUpdateActionRaw();
    row.md_update_action = update_action_name(msg.mDUpdateAction());
    const auto entry_type = msg.mDEntryType();
    row.md_entry_type_raw = static_cast<std::uint8_t>(entry_type);
    row.side = side_from_md_entry_type(entry_type);
    row.price_mantissa = msg.mDEntryPx().mantissa();
    row.price = scale_decimal(row.price_mantissa, sbe_types::PriceOptional::exponent());
    row.size = msg.mDEntrySize();
    row.position_no = msg.mDEntryPositionNo();
    row.entering_firm = msg.enteringFirm();
    row.md_insert_timestamp_ns = msg.mDInsertTimestamp().time();
    row.secondary_order_id = msg.secondaryOrderID();
    row.rpt_seq = msg.rptSeq();
    row.md_entry_timestamp_ns = msg.mDEntryTimestamp().time();
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    row.message_index_in_packet = ctx.message_index;
    return row;
}

IncrementalDeleteRow make_incremental_delete_row(const MessageContext &ctx,
                                                 sbe_types::DeleteOrder_MBO_51 &msg) {
    IncrementalDeleteRow row{};
    row.source_file = ctx.meta->source_file;
    row.channel_hint = ctx.meta->channel_hint;
    row.feed_leg = ctx.meta->feed_leg;
    row.security_id = msg.securityID();
    row.match_event_indicator_raw = msg.matchEventIndicator().rawValue();
    row.md_update_action_raw = msg.mDUpdateActionRaw();
    row.md_update_action = update_action_name(msg.mDUpdateAction());
    const auto entry_type = msg.mDEntryType();
    row.md_entry_type_raw = static_cast<std::uint8_t>(entry_type);
    row.side = side_from_md_entry_type(entry_type);
    row.position_no = msg.mDEntryPositionNo();
    row.size = msg.mDEntrySize();
    row.secondary_order_id = msg.secondaryOrderID();
    row.md_entry_timestamp_ns = msg.mDEntryTimestamp().time();
    row.rpt_seq = msg.rptSeq();
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    return row;
}

IncrementalMassDeleteRow make_mass_delete_row(const MessageContext &ctx,
                                              sbe_types::MassDeleteOrders_MBO_52 &msg) {
    IncrementalMassDeleteRow row{};
    row.source_file = ctx.meta->source_file;
    row.channel_hint = ctx.meta->channel_hint;
    row.feed_leg = ctx.meta->feed_leg;
    row.security_id = msg.securityID();
    row.match_event_indicator_raw = msg.matchEventIndicator().rawValue();
    row.md_update_action_raw = msg.mDUpdateActionRaw();
    row.md_update_action = update_action_name(msg.mDUpdateAction());
    const auto entry_type = msg.mDEntryType();
    row.md_entry_type_raw = static_cast<std::uint8_t>(entry_type);
    row.side = side_from_md_entry_type(entry_type);
    row.start_position = msg.mDEntryPositionNo();
    row.end_position =
        msg.mDUpdateAction() == sbe_types::MDUpdateAction::Value::DELETE_THRU ? row.start_position : 0;
    row.md_entry_timestamp_ns = msg.mDEntryTimestamp().time();
    row.rpt_seq = msg.rptSeq();
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    return row;
}

IncrementalTradeRow make_trade_row(const MessageContext &ctx, sbe_types::Trade_53 &msg) {
    IncrementalTradeRow row{};
    row.source_file = ctx.meta->source_file;
    row.channel_hint = ctx.meta->channel_hint;
    row.feed_leg = ctx.meta->feed_leg;
    row.security_id = msg.securityID();
    row.match_event_indicator_raw = msg.matchEventIndicator().rawValue();
    row.trading_session_id = msg.tradingSessionIDRaw();
    row.trade_condition_raw = msg.tradeCondition().rawValue();
    row.trd_sub_type_raw = static_cast<std::uint16_t>(msg.trdSubType());
    row.price_mantissa = msg.mDEntryPx().mantissa();
    row.price = scale_decimal(row.price_mantissa, sbe_types::Price::exponent());
    row.size = msg.mDEntrySize();
    row.trade_id = msg.tradeID();
    row.buyer_firm = msg.mDEntryBuyer();
    row.seller_firm = msg.mDEntrySeller();
    row.trade_date = msg.tradeDate();
    row.md_entry_timestamp_ns = msg.mDEntryTimestamp().time();
    row.rpt_seq = msg.rptSeq();
    row.packet_sequence_number = ctx.packet_row->packet_sequence_number;
    row.packet_sending_time_ns = ctx.packet_row->packet_sending_time_ns;
    return row;
}

void NativeDecoder::parse_one(const FileMetadata &meta) {
    PcapReader reader(meta.path);
    if (!reader.good()) {
        log_error(meta, "pcap", 0, 0, 0, "pcap_open", reader.error(), "");
        return;
    }

    PcapPacketView packet{};
    std::uint64_t packet_index = 0;
    while (reader.next(packet)) {
        PacketRow packet_row{};
        packet_row.source_file = meta.source_file;
        packet_row.channel_hint_from_filename = meta.channel_hint;
        packet_row.stream_kind = meta.stream_kind;
        packet_row.feed_leg = meta.feed_leg;
        packet_row.packet_index_in_file = packet_index;
        packet_row.pcap_ts_ns = packet.timestamp_ns;
        packet_row.udp_payload_len = packet.length;

        std::string error_text;
        UdpPayloadView payload{};
        if (!extract_udp_payload(packet, payload, error_text)) {
            log_error(meta, "pcap", packet_index, 0, 0, "udp_parse", error_text, "");
            ++packet_index;
            if (options_.max_packets_per_file > 0 && packet_index >= options_.max_packets_per_file) {
                break;
            }
            continue;
        }

        packet_row.udp_payload_len = static_cast<std::uint32_t>(payload.size);

        if (payload.size < sbe_types::PacketHeader::encodedLength()) {
            log_error(meta, "packet_header", packet_index, 0, 0, "packet_header",
                      "payload too small for PacketHeader", "");
            ++packet_index;
            continue;
        }

        auto *mutable_payload = reinterpret_cast<char *>(const_cast<std::uint8_t *>(payload.data));
        const std::size_t message_header_len = sbe_types::MessageHeader::encodedLength();
        try {
            sbe_types::PacketHeader packet_header(mutable_payload, 0, payload.size,
                                                  sbe_types::PacketHeader::sbeSchemaVersion());
            packet_row.packet_channel_number = packet_header.channelNumber();
            packet_row.packet_sequence_version = packet_header.sequenceVersion();
            packet_row.packet_sequence_number = packet_header.sequenceNumber();
            packet_row.packet_sending_time_ns = packet_header.sendingTime();

            std::size_t cursor = sbe_types::PacketHeader::encodedLength();
            std::uint32_t message_index = 0;
            while (cursor + sbe_types::FramingHeader::encodedLength() <= payload.size) {
                const std::size_t framing_offset = cursor;
                sbe_types::FramingHeader framing(
                    mutable_payload, framing_offset, payload.size, sbe_types::FramingHeader::sbeSchemaVersion());
                cursor += sbe_types::FramingHeader::encodedLength();
                const std::uint16_t message_length = framing.messageLength();
                if (message_length == 0) {
                    cursor = payload.size;
                    break;
                }
                if (message_length < message_header_len) {
                    log_error(meta, "framing", packet_index, message_index, 0, "message_bounds",
                              "message shorter than header", "");
                    cursor += message_length;
                    ++message_index;
                    continue;
                }
                if (cursor + message_length > payload.size) {
                    log_error(meta, "framing", packet_index, message_index, 0, "message_bounds",
                              "message exceeds payload bounds", bytes_to_hex(payload.data + cursor,
                                                                               payload.size - cursor));
                    break;
                }
                const std::size_t message_start = cursor;
                auto *message_ptr = mutable_payload + message_start;
                const std::size_t remaining_buffer = payload.size - message_start;
                sbe_types::MessageHeader message_header(
                    message_ptr, 0, remaining_buffer, sbe_types::MessageHeader::sbeSchemaVersion());
                const std::uint16_t template_id = message_header.templateId();
                const std::uint16_t schema_id = message_header.schemaId();
                const std::uint16_t schema_version = message_header.version();
                const std::uint16_t block_length = message_header.blockLength();
                const std::size_t header_length = sbe_types::MessageHeader::encodedLength();
                const std::size_t body_length = message_length - header_length;

                if (options_.validate_schema && framing.encodingType() != kExpectedEncodingType) {
                    log_error(meta, "framing", packet_index, message_index, template_id, "encoding_type",
                              "unexpected encoding type", "");
                }

                if (options_.validate_schema &&
                    (schema_id != kExpectedSchemaId || schema_version != kExpectedSchemaVersion)) {
                    std::ostringstream oss;
                    oss << "schema=" << schema_id << " version=" << schema_version;
                    log_error(meta, "message_header", packet_index, message_index, template_id, "schema_mismatch",
                              oss.str(), bytes_to_hex(reinterpret_cast<std::uint8_t *>(message_ptr),
                                                     message_length));
                    cursor += message_length;
                    ++message_index;
                    continue;
                }

                MessageContext msg_ctx{&meta, &packet_row, message_index, template_id};
                auto *message_body = message_ptr;
                std::size_t consumed_body = body_length;
                try {
                    switch (template_id) {
                        case sbe_types::SecurityDefinition_12::sbeTemplateId(): {
                            sbe_types::SecurityDefinition_12 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            decode_security_definition(msg_ctx, msg_wrapper, instruments_writer_.get(),
                                                        instrument_legs_writer_.get(),
                                                        instrument_underlyings_writer_.get(),
                                                        instrument_attributes_writer_.get());
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::SnapshotFullRefresh_Header_30::sbeTemplateId(): {
                            sbe_types::SnapshotFullRefresh_Header_30 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            decode_snapshot_header(msg_ctx, msg_wrapper, snapshot_headers_writer_.get(),
                                                   snapshot_header_row_id_,
                                                   last_snapshot_row_for_security_);
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::SnapshotFullRefresh_Orders_MBO_71::sbeTemplateId(): {
                            sbe_types::SnapshotFullRefresh_Orders_MBO_71 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            decode_snapshot_orders(msg_ctx, msg_wrapper, snapshot_orders_writer_.get(),
                                                   last_snapshot_row_for_security_);
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::Order_MBO_50::sbeTemplateId(): {
                            sbe_types::Order_MBO_50 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            if (incremental_orders_writer_) {
                                incremental_orders_writer_->Append(make_incremental_order_row(msg_ctx, msg_wrapper));
                            }
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::DeleteOrder_MBO_51::sbeTemplateId(): {
                            sbe_types::DeleteOrder_MBO_51 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            if (incremental_deletes_writer_) {
                                incremental_deletes_writer_->Append(
                                    make_incremental_delete_row(msg_ctx, msg_wrapper));
                            }
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::MassDeleteOrders_MBO_52::sbeTemplateId(): {
                            sbe_types::MassDeleteOrders_MBO_52 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            if (incremental_mass_deletes_writer_) {
                                incremental_mass_deletes_writer_->Append(
                                    make_mass_delete_row(msg_ctx, msg_wrapper));
                            }
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        case sbe_types::Trade_53::sbeTemplateId(): {
                            sbe_types::Trade_53 msg_wrapper;
                            msg_wrapper.wrapForDecode(message_body, sbe_types::MessageHeader::encodedLength(),
                                                      block_length, schema_version, remaining_buffer);
                            if (incremental_trades_writer_) {
                                incremental_trades_writer_->Append(make_trade_row(msg_ctx, msg_wrapper));
                            }
                            consumed_body = static_cast<std::size_t>(msg_wrapper.encodedLength());
                            break;
                        }
                        default: {
                            if (incremental_other_writer_) {
                                IncrementalOtherRow row{};
                                row.source_file = meta.source_file;
                                row.template_id = template_id;
                                row.template_name = template_name(template_id);
                                row.security_id = 0;
                                row.rpt_seq = 0;
                                row.packet_sequence_number = packet_row.packet_sequence_number;
                                row.packet_sending_time_ns = packet_row.packet_sending_time_ns;
                                row.body_hex = bytes_to_hex(reinterpret_cast<std::uint8_t *>(message_body),
                                                            body_length);
                                row.decode_status = "unsupported_template";
                                incremental_other_writer_->Append(row);
                            }
                            break;
                        }
                    }
                } catch (const std::exception &ex) {
                    log_error(meta, "message", packet_index, message_index, template_id, "decode", ex.what(),
                              bytes_to_hex(reinterpret_cast<std::uint8_t *>(message_body), body_length));
                }

                const std::size_t total_consumed = header_length + consumed_body;
                cursor = message_start + total_consumed;
                ++message_index;
            }
        } catch (const std::exception &ex) {
            log_error(meta, "packet_header", packet_index, 0, 0, "packet_header", ex.what(), "");
        }

        ++packet_index;
        if (options_.max_packets_per_file > 0 && packet_index >= options_.max_packets_per_file) {
            break;
        }
    }
}

}  // namespace b3::sbe
