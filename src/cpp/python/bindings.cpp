#include "core/decoder.h"

#include <nanobind/ndarray.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>


namespace nb = nanobind;

namespace b3::sbe::binding {

template <typename T>
using nb_array = nb::ndarray<nb::numpy, T, nb::shape<-1>>;

template <typename T>
nb_array<T> vector_to_ndarray(std::vector<T> &&values) {
    auto *storage = new std::vector<T>(std::move(values));
    auto capsule = nb::capsule(storage, [](void *ptr) noexcept {
        delete reinterpret_cast<std::vector<T> *>(ptr);
    });
    const size_t size = storage->size();
    return nb_array<T>(storage->data(), {size}, capsule);
}

inline nb_array<std::uint8_t> vector_bool_to_ndarray(std::vector<std::uint8_t> &&values) {
    auto *storage = new std::vector<std::uint8_t>(std::move(values));
    auto capsule = nb::capsule(storage, [](void *ptr) noexcept {
        delete reinterpret_cast<std::vector<std::uint8_t> *>(ptr);
    });
    const size_t size = storage->size();
    return nb_array<std::uint8_t>(storage->data(), {size}, capsule);
}

template <typename Row, typename Getter>
void add_numeric_column(nb::dict &dest, const char *name, const std::vector<Row> &rows, Getter getter) {
    using Raw = std::decay_t<std::invoke_result_t<Getter, const Row &>>;
    if constexpr (std::is_same_v<Raw, bool>) {
        std::vector<std::uint8_t> converted;
        converted.reserve(rows.size());
        for (const auto &row : rows) {
            converted.push_back(static_cast<std::uint8_t>(getter(row) ? 1 : 0));
        }
        dest[name] = vector_bool_to_ndarray(std::move(converted));
    } else {
        std::vector<Raw> converted;
        converted.reserve(rows.size());
        for (const auto &row : rows) {
            converted.push_back(static_cast<Raw>(getter(row)));
        }
        dest[name] = vector_to_ndarray(std::move(converted));
    }
}

template <typename Row, typename Getter>
void add_string_column(nb::dict &dest, const char *name, const std::vector<Row> &rows, Getter getter) {
    std::vector<std::string> values;
    values.reserve(rows.size());
    for (const auto &row : rows) {
        values.emplace_back(getter(row));
    }
    dest[name] = nb::cast(std::move(values));
}

nb::dict packets_to_dict(const PacketsTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const PacketRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint_from_filename", rows, [](const PacketRow &r) { return r.channel_hint_from_filename; });
    add_string_column(out, "stream_kind", rows, [](const PacketRow &r) -> const std::string & { return r.stream_kind; });
    add_string_column(out, "feed_leg", rows, [](const PacketRow &r) -> const std::string & { return r.feed_leg; });
    add_numeric_column(out, "packet_index_in_file", rows, [](const PacketRow &r) { return r.packet_index_in_file; });
    add_numeric_column(out, "pcap_ts_ns", rows, [](const PacketRow &r) { return r.pcap_ts_ns; });
    add_numeric_column(out, "udp_payload_len", rows, [](const PacketRow &r) { return r.udp_payload_len; });
    add_numeric_column(out, "packet_channel_number", rows, [](const PacketRow &r) { return r.packet_channel_number; });
    add_numeric_column(out, "packet_sequence_version", rows, [](const PacketRow &r) { return r.packet_sequence_version; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const PacketRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const PacketRow &r) { return r.packet_sending_time_ns; });
    add_numeric_column(out, "packet_parse_ok", rows, [](const PacketRow &r) { return r.packet_parse_ok; });
    add_string_column(out, "error_code", rows, [](const PacketRow &r) -> const std::string & { return r.error_code; });
    add_string_column(out, "error_text", rows, [](const PacketRow &r) -> const std::string & { return r.error_text; });
    return out;
}

nb::dict messages_to_dict(const MessagesTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const MessageRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "packet_index_in_file", rows, [](const MessageRow &r) { return r.packet_index_in_file; });
    add_numeric_column(out, "message_index_in_packet", rows, [](const MessageRow &r) { return r.message_index_in_packet; });
    add_numeric_column(out, "message_offset_in_payload", rows, [](const MessageRow &r) { return r.message_offset_in_payload; });
    add_numeric_column(out, "message_length", rows, [](const MessageRow &r) { return r.message_length; });
    add_numeric_column(out, "encoding_type", rows, [](const MessageRow &r) { return r.encoding_type; });
    add_numeric_column(out, "block_length", rows, [](const MessageRow &r) { return r.block_length; });
    add_numeric_column(out, "template_id", rows, [](const MessageRow &r) { return r.template_id; });
    add_numeric_column(out, "schema_id", rows, [](const MessageRow &r) { return r.schema_id; });
    add_numeric_column(out, "schema_version", rows, [](const MessageRow &r) { return r.schema_version; });
    add_numeric_column(out, "body_length", rows, [](const MessageRow &r) { return r.body_length; });
    add_numeric_column(out, "packet_channel_number", rows, [](const MessageRow &r) { return r.packet_channel_number; });
    add_numeric_column(out, "packet_sequence_version", rows, [](const MessageRow &r) { return r.packet_sequence_version; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const MessageRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const MessageRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict instruments_to_dict(const InstrumentsTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const InstrumentRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const InstrumentRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "security_id", rows, [](const InstrumentRow &r) { return r.security_id; });
    add_string_column(out, "security_exchange", rows, [](const InstrumentRow &r) -> const std::string & { return r.security_exchange; });
    add_string_column(out, "symbol", rows, [](const InstrumentRow &r) -> const std::string & { return r.symbol; });
    add_string_column(out, "security_desc", rows, [](const InstrumentRow &r) -> const std::string & { return r.security_desc; });
    add_string_column(out, "security_type", rows, [](const InstrumentRow &r) -> const std::string & { return r.security_type; });
    add_numeric_column(out, "security_type_raw", rows, [](const InstrumentRow &r) { return r.security_type_raw; });
    add_string_column(out, "cfi_code", rows, [](const InstrumentRow &r) -> const std::string & { return r.cfi_code; });
    add_string_column(out, "asset", rows, [](const InstrumentRow &r) -> const std::string & { return r.asset; });
    add_numeric_column(out, "market_segment_id", rows, [](const InstrumentRow &r) { return r.market_segment_id; });
    add_numeric_column(out, "maturity_date", rows, [](const InstrumentRow &r) { return r.maturity_date; });
    add_numeric_column(out, "issue_date", rows, [](const InstrumentRow &r) { return r.issue_date; });
    add_numeric_column(out, "strike_price_mantissa", rows, [](const InstrumentRow &r) { return r.strike_price_mantissa; });
    add_numeric_column(out, "strike_price", rows, [](const InstrumentRow &r) { return r.strike_price; });
    add_numeric_column(out, "min_price_increment_mantissa", rows, [](const InstrumentRow &r) { return r.min_price_increment_mantissa; });
    add_numeric_column(out, "min_price_increment", rows, [](const InstrumentRow &r) { return r.min_price_increment; });
    add_numeric_column(out, "contract_multiplier_mantissa", rows, [](const InstrumentRow &r) { return r.contract_multiplier_mantissa; });
    add_numeric_column(out, "contract_multiplier", rows, [](const InstrumentRow &r) { return r.contract_multiplier; });
    add_numeric_column(out, "price_divisor_mantissa", rows, [](const InstrumentRow &r) { return r.price_divisor_mantissa; });
    add_numeric_column(out, "price_divisor", rows, [](const InstrumentRow &r) { return r.price_divisor; });
    add_string_column(out, "currency", rows, [](const InstrumentRow &r) -> const std::string & { return r.currency; });
    add_string_column(out, "settl_currency", rows, [](const InstrumentRow &r) -> const std::string & { return r.settl_currency; });
    add_string_column(out, "base_currency", rows, [](const InstrumentRow &r) -> const std::string & { return r.base_currency; });
    add_numeric_column(out, "quantity_multiplier", rows, [](const InstrumentRow &r) { return r.quantity_multiplier; });
    add_string_column(out, "security_group", rows, [](const InstrumentRow &r) -> const std::string & { return r.security_group; });
    add_string_column(out, "put_or_call", rows, [](const InstrumentRow &r) -> const std::string & { return r.put_or_call; });
    add_numeric_column(out, "put_or_call_raw", rows, [](const InstrumentRow &r) { return r.put_or_call_raw; });
    add_string_column(out, "exercise_style", rows, [](const InstrumentRow &r) -> const std::string & { return r.exercise_style; });
    add_numeric_column(out, "exercise_style_raw", rows, [](const InstrumentRow &r) { return r.exercise_style_raw; });
    add_string_column(out, "product", rows, [](const InstrumentRow &r) -> const std::string & { return r.product; });
    add_numeric_column(out, "product_raw", rows, [](const InstrumentRow &r) { return r.product_raw; });
    add_string_column(out, "security_sub_type", rows, [](const InstrumentRow &r) -> const std::string & { return r.security_sub_type; });
    add_numeric_column(out, "trading_reference_price_mantissa", rows, [](const InstrumentRow &r) { return r.trading_reference_price_mantissa; });
    add_numeric_column(out, "trading_reference_price", rows, [](const InstrumentRow &r) { return r.trading_reference_price; });
    add_string_column(out, "unit_of_measure", rows, [](const InstrumentRow &r) -> const std::string & { return r.unit_of_measure; });
    add_numeric_column(out, "unit_of_measure_qty", rows, [](const InstrumentRow &r) { return r.unit_of_measure_qty; });
    add_numeric_column(out, "raw_template_id", rows, [](const InstrumentRow &r) { return r.raw_template_id; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const InstrumentRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict instrument_underlyings_to_dict(const InstrumentUnderlyingsTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const InstrumentUnderlyingRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "security_id", rows, [](const InstrumentUnderlyingRow &r) { return r.security_id; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const InstrumentUnderlyingRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "underlying_security_id", rows, [](const InstrumentUnderlyingRow &r) { return r.underlying_security_id; });
    add_string_column(out, "underlying_security_exchange", rows, [](const InstrumentUnderlyingRow &r) -> const std::string & { return r.underlying_security_exchange; });
    add_string_column(out, "underlying_symbol", rows, [](const InstrumentUnderlyingRow &r) -> const std::string & { return r.underlying_symbol; });
    add_string_column(out, "underlying_security_type", rows, [](const InstrumentUnderlyingRow &r) -> const std::string & { return r.underlying_security_type; });
    add_numeric_column(out, "position_in_group", rows, [](const InstrumentUnderlyingRow &r) { return r.position_in_group; });
    return out;
}

nb::dict instrument_legs_to_dict(const InstrumentLegsTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const InstrumentLegRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "security_id", rows, [](const InstrumentLegRow &r) { return r.security_id; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const InstrumentLegRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "leg_security_id", rows, [](const InstrumentLegRow &r) { return r.leg_security_id; });
    add_string_column(out, "leg_symbol", rows, [](const InstrumentLegRow &r) -> const std::string & { return r.leg_symbol; });
    add_string_column(out, "leg_side", rows, [](const InstrumentLegRow &r) -> const std::string & { return r.leg_side; });
    add_numeric_column(out, "leg_side_raw", rows, [](const InstrumentLegRow &r) { return r.leg_side_raw; });
    add_numeric_column(out, "leg_ratio_qty_mantissa", rows, [](const InstrumentLegRow &r) { return r.leg_ratio_qty_mantissa; });
    add_numeric_column(out, "leg_ratio_qty", rows, [](const InstrumentLegRow &r) { return r.leg_ratio_qty; });
    add_numeric_column(out, "position_in_group", rows, [](const InstrumentLegRow &r) { return r.position_in_group; });
    return out;
}

nb::dict instrument_attributes_to_dict(const InstrumentAttributesTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const InstrumentAttributeRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "security_id", rows, [](const InstrumentAttributeRow &r) { return r.security_id; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const InstrumentAttributeRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "attribute_type", rows, [](const InstrumentAttributeRow &r) { return r.attribute_type; });
    add_string_column(out, "attribute_value", rows, [](const InstrumentAttributeRow &r) -> const std::string & { return r.attribute_value; });
    add_numeric_column(out, "position_in_group", rows, [](const InstrumentAttributeRow &r) { return r.position_in_group; });
    return out;
}

nb::dict snapshot_headers_to_dict(const SnapshotHeadersTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const SnapshotHeaderRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const SnapshotHeaderRow &r) { return r.channel_hint; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const SnapshotHeaderRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const SnapshotHeaderRow &r) { return r.packet_sending_time_ns; });
    add_numeric_column(out, "security_id", rows, [](const SnapshotHeaderRow &r) { return r.security_id; });
    add_numeric_column(out, "last_msg_seq_num_processed", rows, [](const SnapshotHeaderRow &r) { return r.last_msg_seq_num_processed; });
    add_numeric_column(out, "tot_num_bids", rows, [](const SnapshotHeaderRow &r) { return r.tot_num_bids; });
    add_numeric_column(out, "tot_num_offers", rows, [](const SnapshotHeaderRow &r) { return r.tot_num_offers; });
    add_numeric_column(out, "last_rpt_seq", rows, [](const SnapshotHeaderRow &r) { return r.last_rpt_seq; });
    add_numeric_column(out, "snapshot_header_row_id", rows, [](const SnapshotHeaderRow &r) { return r.snapshot_header_row_id; });
    return out;
}

nb::dict snapshot_orders_to_dict(const SnapshotOrdersTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const SnapshotOrderRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const SnapshotOrderRow &r) { return r.channel_hint; });
    add_numeric_column(out, "snapshot_header_row_id", rows, [](const SnapshotOrderRow &r) { return r.snapshot_header_row_id; });
    add_numeric_column(out, "security_id", rows, [](const SnapshotOrderRow &r) { return r.security_id; });
    add_string_column(out, "security_exchange", rows, [](const SnapshotOrderRow &r) -> const std::string & { return r.security_exchange; });
    add_string_column(out, "side", rows, [](const SnapshotOrderRow &r) -> const std::string & { return r.side; });
    add_numeric_column(out, "side_raw", rows, [](const SnapshotOrderRow &r) { return r.side_raw; });
    add_numeric_column(out, "price_mantissa", rows, [](const SnapshotOrderRow &r) { return r.price_mantissa; });
    add_numeric_column(out, "price", rows, [](const SnapshotOrderRow &r) { return r.price; });
    add_numeric_column(out, "size", rows, [](const SnapshotOrderRow &r) { return r.size; });
    add_numeric_column(out, "position_no", rows, [](const SnapshotOrderRow &r) { return r.position_no; });
    add_numeric_column(out, "entering_firm", rows, [](const SnapshotOrderRow &r) { return r.entering_firm; });
    add_numeric_column(out, "md_insert_timestamp_ns", rows, [](const SnapshotOrderRow &r) { return r.md_insert_timestamp_ns; });
    add_numeric_column(out, "secondary_order_id", rows, [](const SnapshotOrderRow &r) { return r.secondary_order_id; });
    add_numeric_column(out, "row_in_group", rows, [](const SnapshotOrderRow &r) { return r.row_in_group; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const SnapshotOrderRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const SnapshotOrderRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict incremental_orders_to_dict(const IncrementalOrdersTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const IncrementalOrderRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const IncrementalOrderRow &r) { return r.channel_hint; });
    add_string_column(out, "feed_leg", rows, [](const IncrementalOrderRow &r) -> const std::string & { return r.feed_leg; });
    add_numeric_column(out, "security_id", rows, [](const IncrementalOrderRow &r) { return r.security_id; });
    add_numeric_column(out, "match_event_indicator_raw", rows, [](const IncrementalOrderRow &r) { return r.match_event_indicator_raw; });
    add_numeric_column(out, "md_update_action_raw", rows, [](const IncrementalOrderRow &r) { return r.md_update_action_raw; });
    add_string_column(out, "md_update_action", rows, [](const IncrementalOrderRow &r) -> const std::string & { return r.md_update_action; });
    add_numeric_column(out, "md_entry_type_raw", rows, [](const IncrementalOrderRow &r) { return r.md_entry_type_raw; });
    add_string_column(out, "side", rows, [](const IncrementalOrderRow &r) -> const std::string & { return r.side; });
    add_numeric_column(out, "price_mantissa", rows, [](const IncrementalOrderRow &r) { return r.price_mantissa; });
    add_numeric_column(out, "price", rows, [](const IncrementalOrderRow &r) { return r.price; });
    add_numeric_column(out, "size", rows, [](const IncrementalOrderRow &r) { return r.size; });
    add_numeric_column(out, "position_no", rows, [](const IncrementalOrderRow &r) { return r.position_no; });
    add_numeric_column(out, "entering_firm", rows, [](const IncrementalOrderRow &r) { return r.entering_firm; });
    add_numeric_column(out, "md_insert_timestamp_ns", rows, [](const IncrementalOrderRow &r) { return r.md_insert_timestamp_ns; });
    add_numeric_column(out, "secondary_order_id", rows, [](const IncrementalOrderRow &r) { return r.secondary_order_id; });
    add_numeric_column(out, "rpt_seq", rows, [](const IncrementalOrderRow &r) { return r.rpt_seq; });
    add_numeric_column(out, "md_entry_timestamp_ns", rows, [](const IncrementalOrderRow &r) { return r.md_entry_timestamp_ns; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const IncrementalOrderRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const IncrementalOrderRow &r) { return r.packet_sending_time_ns; });
    add_numeric_column(out, "message_index_in_packet", rows, [](const IncrementalOrderRow &r) { return r.message_index_in_packet; });
    return out;
}

nb::dict incremental_deletes_to_dict(const IncrementalDeletesTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const IncrementalDeleteRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const IncrementalDeleteRow &r) { return r.channel_hint; });
    add_string_column(out, "feed_leg", rows, [](const IncrementalDeleteRow &r) -> const std::string & { return r.feed_leg; });
    add_numeric_column(out, "security_id", rows, [](const IncrementalDeleteRow &r) { return r.security_id; });
    add_numeric_column(out, "match_event_indicator_raw", rows, [](const IncrementalDeleteRow &r) { return r.match_event_indicator_raw; });
    add_numeric_column(out, "md_update_action_raw", rows, [](const IncrementalDeleteRow &r) { return r.md_update_action_raw; });
    add_string_column(out, "md_update_action", rows, [](const IncrementalDeleteRow &r) -> const std::string & { return r.md_update_action; });
    add_numeric_column(out, "md_entry_type_raw", rows, [](const IncrementalDeleteRow &r) { return r.md_entry_type_raw; });
    add_string_column(out, "side", rows, [](const IncrementalDeleteRow &r) -> const std::string & { return r.side; });
    add_numeric_column(out, "position_no", rows, [](const IncrementalDeleteRow &r) { return r.position_no; });
    add_numeric_column(out, "size", rows, [](const IncrementalDeleteRow &r) { return r.size; });
    add_numeric_column(out, "secondary_order_id", rows, [](const IncrementalDeleteRow &r) { return r.secondary_order_id; });
    add_numeric_column(out, "md_entry_timestamp_ns", rows, [](const IncrementalDeleteRow &r) { return r.md_entry_timestamp_ns; });
    add_numeric_column(out, "rpt_seq", rows, [](const IncrementalDeleteRow &r) { return r.rpt_seq; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const IncrementalDeleteRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const IncrementalDeleteRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict incremental_mass_deletes_to_dict(const IncrementalMassDeletesTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const IncrementalMassDeleteRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const IncrementalMassDeleteRow &r) { return r.channel_hint; });
    add_string_column(out, "feed_leg", rows, [](const IncrementalMassDeleteRow &r) -> const std::string & { return r.feed_leg; });
    add_numeric_column(out, "security_id", rows, [](const IncrementalMassDeleteRow &r) { return r.security_id; });
    add_numeric_column(out, "match_event_indicator_raw", rows, [](const IncrementalMassDeleteRow &r) { return r.match_event_indicator_raw; });
    add_numeric_column(out, "md_update_action_raw", rows, [](const IncrementalMassDeleteRow &r) { return r.md_update_action_raw; });
    add_string_column(out, "md_update_action", rows, [](const IncrementalMassDeleteRow &r) -> const std::string & { return r.md_update_action; });
    add_numeric_column(out, "md_entry_type_raw", rows, [](const IncrementalMassDeleteRow &r) { return r.md_entry_type_raw; });
    add_string_column(out, "side", rows, [](const IncrementalMassDeleteRow &r) -> const std::string & { return r.side; });
    add_numeric_column(out, "start_position", rows, [](const IncrementalMassDeleteRow &r) { return r.start_position; });
    add_numeric_column(out, "end_position", rows, [](const IncrementalMassDeleteRow &r) { return r.end_position; });
    add_numeric_column(out, "md_entry_timestamp_ns", rows, [](const IncrementalMassDeleteRow &r) { return r.md_entry_timestamp_ns; });
    add_numeric_column(out, "rpt_seq", rows, [](const IncrementalMassDeleteRow &r) { return r.rpt_seq; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const IncrementalMassDeleteRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const IncrementalMassDeleteRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict incremental_trades_to_dict(const IncrementalTradesTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const IncrementalTradeRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "channel_hint", rows, [](const IncrementalTradeRow &r) { return r.channel_hint; });
    add_string_column(out, "feed_leg", rows, [](const IncrementalTradeRow &r) -> const std::string & { return r.feed_leg; });
    add_numeric_column(out, "security_id", rows, [](const IncrementalTradeRow &r) { return r.security_id; });
    add_numeric_column(out, "match_event_indicator_raw", rows, [](const IncrementalTradeRow &r) { return r.match_event_indicator_raw; });
    add_numeric_column(out, "trading_session_id", rows, [](const IncrementalTradeRow &r) { return r.trading_session_id; });
    add_numeric_column(out, "trade_condition_raw", rows, [](const IncrementalTradeRow &r) { return r.trade_condition_raw; });
    add_numeric_column(out, "trd_sub_type_raw", rows, [](const IncrementalTradeRow &r) { return r.trd_sub_type_raw; });
    add_numeric_column(out, "price_mantissa", rows, [](const IncrementalTradeRow &r) { return r.price_mantissa; });
    add_numeric_column(out, "price", rows, [](const IncrementalTradeRow &r) { return r.price; });
    add_numeric_column(out, "size", rows, [](const IncrementalTradeRow &r) { return r.size; });
    add_numeric_column(out, "trade_id", rows, [](const IncrementalTradeRow &r) { return r.trade_id; });
    add_numeric_column(out, "buyer_firm", rows, [](const IncrementalTradeRow &r) { return r.buyer_firm; });
    add_numeric_column(out, "seller_firm", rows, [](const IncrementalTradeRow &r) { return r.seller_firm; });
    add_numeric_column(out, "trade_date", rows, [](const IncrementalTradeRow &r) { return r.trade_date; });
    add_numeric_column(out, "md_entry_timestamp_ns", rows, [](const IncrementalTradeRow &r) { return r.md_entry_timestamp_ns; });
    add_numeric_column(out, "rpt_seq", rows, [](const IncrementalTradeRow &r) { return r.rpt_seq; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const IncrementalTradeRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const IncrementalTradeRow &r) { return r.packet_sending_time_ns; });
    return out;
}

nb::dict incremental_other_to_dict(const IncrementalOtherTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const IncrementalOtherRow &r) -> const std::string & { return r.source_file; });
    add_numeric_column(out, "template_id", rows, [](const IncrementalOtherRow &r) { return r.template_id; });
    add_string_column(out, "template_name", rows, [](const IncrementalOtherRow &r) -> const std::string & { return r.template_name; });
    add_numeric_column(out, "security_id", rows, [](const IncrementalOtherRow &r) { return r.security_id; });
    add_numeric_column(out, "rpt_seq", rows, [](const IncrementalOtherRow &r) { return r.rpt_seq; });
    add_numeric_column(out, "packet_sequence_number", rows, [](const IncrementalOtherRow &r) { return r.packet_sequence_number; });
    add_numeric_column(out, "packet_sending_time_ns", rows, [](const IncrementalOtherRow &r) { return r.packet_sending_time_ns; });
    add_string_column(out, "body_hex", rows, [](const IncrementalOtherRow &r) -> const std::string & { return r.body_hex; });
    add_string_column(out, "decode_status", rows, [](const IncrementalOtherRow &r) -> const std::string & { return r.decode_status; });
    return out;
}

nb::dict errors_to_dict(const ErrorsTable &table) {
    const auto &rows = table.all();
    nb::dict out;
    add_string_column(out, "source_file", rows, [](const ErrorRow &r) -> const std::string & { return r.source_file; });
    add_string_column(out, "stage", rows, [](const ErrorRow &r) -> const std::string & { return r.stage; });
    add_numeric_column(out, "packet_index_in_file", rows, [](const ErrorRow &r) { return r.packet_index_in_file; });
    add_numeric_column(out, "message_index_in_packet", rows, [](const ErrorRow &r) { return r.message_index_in_packet; });
    add_numeric_column(out, "template_id", rows, [](const ErrorRow &r) { return r.template_id; });
    add_string_column(out, "error_code", rows, [](const ErrorRow &r) -> const std::string & { return r.error_code; });
    add_string_column(out, "error_text", rows, [](const ErrorRow &r) -> const std::string & { return r.error_text; });
    add_string_column(out, "raw_context_hex", rows, [](const ErrorRow &r) -> const std::string & { return r.raw_context_hex; });
    return out;
}

nb::dict table_collection_to_dict(const TableCollection &tables) {
    nb::dict result;
    result["packets"] = packets_to_dict(tables.packets);
    result["messages"] = messages_to_dict(tables.messages);
    result["instruments"] = instruments_to_dict(tables.instruments);
    result["instrument_underlyings"] = instrument_underlyings_to_dict(tables.instrument_underlyings);
    result["instrument_legs"] = instrument_legs_to_dict(tables.instrument_legs);
    result["instrument_attributes"] = instrument_attributes_to_dict(tables.instrument_attributes);
    result["snapshot_headers"] = snapshot_headers_to_dict(tables.snapshot_headers);
    result["snapshot_orders"] = snapshot_orders_to_dict(tables.snapshot_orders);
    result["incremental_orders"] = incremental_orders_to_dict(tables.incremental_orders);
    result["incremental_deletes"] = incremental_deletes_to_dict(tables.incremental_deletes);
    result["incremental_mass_deletes"] = incremental_mass_deletes_to_dict(tables.incremental_mass_deletes);
    result["incremental_trades"] = incremental_trades_to_dict(tables.incremental_trades);
    result["incremental_other"] = incremental_other_to_dict(tables.incremental_other);
    result["errors"] = errors_to_dict(tables.errors);
    return result;
}

nb::dict metadata_to_dict(const FileMetadata &meta) {
    nb::dict result;
    result["path"] = meta.path.string();
    result["source_file"] = meta.source_file;
    result["channel_hint"] = meta.channel_hint;
    result["stream_kind"] = meta.stream_kind;
    result["feed_leg"] = meta.feed_leg;
    return result;
}

}  // namespace b3::sbe::binding

NB_MODULE(_b3sbe_native, m) {
    using namespace b3::sbe;
    namespace binding = b3::sbe::binding;

    nb::class_<DecodeOptions>(m, "DecodeOptions")
        .def(nb::init<>())
        .def_rw("validate_schema", &DecodeOptions::validate_schema)
        .def_rw("max_packets_per_file", &DecodeOptions::max_packets_per_file);

    m.def("infer_file_metadata", [](const std::string &path) {
        auto meta = infer_file_metadata(path);
        return binding::metadata_to_dict(meta);
    });

    m.def("decode_files", [](const std::vector<std::string> &paths, const DecodeOptions &options) {
        std::vector<std::filesystem::path> fs_paths;
        fs_paths.reserve(paths.size());
        for (const auto &p : paths) {
            fs_paths.emplace_back(p);
        }
        NativeDecoder decoder(options);
        decoder.parse_files(fs_paths);
        nb::dict result = binding::table_collection_to_dict(decoder.tables());
        decoder.clear_tables();
        return result;
    }, nb::arg("paths"), nb::arg("options"));
}
