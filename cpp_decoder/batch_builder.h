#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace b3::sbe {

struct PacketRow {
    std::int32_t channel_hint_from_filename{0};
    std::uint64_t packet_index_in_file{0};
    std::uint64_t pcap_ts_ns{0};
    std::uint32_t udp_payload_len{0};
    std::uint32_t packet_channel_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sequence_number{0};
    std::uint64_t packet_sending_time_ns{0};
    bool packet_parse_ok{true};
};

struct MessageRow {
    std::string source_file;
    std::uint64_t packet_index_in_file{0};
    std::uint32_t message_index_in_packet{0};
    std::uint32_t message_offset_in_payload{0};
    std::uint32_t message_length{0};
    std::uint16_t encoding_type{0};
    std::uint16_t block_length{0};
    std::uint16_t template_id{0};
    std::uint16_t schema_id{0};
    std::uint16_t schema_version{0};
    std::uint32_t body_length{0};
    std::uint32_t packet_channel_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sequence_number{0};
    std::uint64_t packet_sending_time_ns{0};
};

struct InstrumentRow {
    std::uint64_t packet_sequence_number{0};
    std::int64_t security_id{0};
    std::string security_exchange;
    std::string symbol;
    std::string security_desc;
    std::string security_type;
    std::uint16_t security_type_raw{0};
    std::string cfi_code;
    std::string asset;
    std::int32_t market_segment_id{0};
    std::uint32_t maturity_date{0};
    std::uint32_t issue_date{0};
    std::int64_t strike_price_mantissa{0};
    double strike_price{0.0};
    std::int64_t min_price_increment_mantissa{0};
    double min_price_increment{0.0};
    std::int64_t contract_multiplier_mantissa{0};
    double contract_multiplier{0.0};
    std::int64_t price_divisor_mantissa{0};
    double price_divisor{0.0};
    std::string currency;
    std::string settl_currency;
    std::string base_currency;
    std::int64_t quantity_multiplier{0};
    std::string security_group;
    std::string put_or_call;
    std::uint8_t put_or_call_raw{0};
    std::string exercise_style;
    std::uint8_t exercise_style_raw{0};
    std::string product;
    std::uint8_t product_raw{0};
    std::string security_sub_type;
    std::int64_t trading_reference_price_mantissa{0};
    double trading_reference_price{0.0};
    std::string unit_of_measure;
    std::int64_t unit_of_measure_qty{0};
    std::uint16_t raw_template_id{0};
    std::uint64_t packet_sending_time_ns{0};
};

struct InstrumentUnderlyingRow {
    std::int64_t security_id{0};
    std::uint64_t packet_sequence_number{0};
    std::uint64_t underlying_security_id{0};
    std::string underlying_security_exchange;
    std::string underlying_symbol;
    std::string underlying_security_type;
    std::uint64_t position_in_group{0};
};

struct InstrumentLegRow {
    std::int64_t security_id{0};
    std::uint64_t packet_sequence_number{0};
    std::uint64_t leg_security_id{0};
    std::string leg_symbol;
    std::string leg_side;
    std::uint8_t leg_side_raw{0};
    std::int64_t leg_ratio_qty_mantissa{0};
    double leg_ratio_qty{0.0};
    std::uint64_t position_in_group{0};
};

struct InstrumentAttributeRow {
    std::int64_t security_id{0};
    std::uint64_t packet_sequence_number{0};
    std::uint8_t attribute_type{0};
    std::string attribute_value;
    std::uint64_t position_in_group{0};
};

struct SnapshotHeaderRow {
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::int64_t security_id{0};
    std::uint32_t last_msg_seq_num_processed{0};
    std::uint32_t tot_num_bids{0};
    std::uint32_t tot_num_offers{0};
    std::uint32_t last_rpt_seq{0};
    std::uint64_t snapshot_header_row_id{0};
};

struct SnapshotOrderRow {
    std::uint64_t snapshot_header_row_id{0};
    std::int64_t security_id{0};
    std::string security_exchange;
    std::string side;
    std::uint8_t side_raw{0};
    std::int64_t price_mantissa{0};
    double price{0.0};
    std::int64_t size{0};
    std::uint32_t position_no{0};
    std::uint32_t entering_firm{0};
    std::uint64_t md_insert_timestamp_ns{0};
    std::uint64_t secondary_order_id{0};
    std::uint32_t row_in_group{0};
    std::uint64_t packet_sequence_number{0};
    std::uint64_t packet_sending_time_ns{0};
};

struct IncrementalOrderRow {
    std::int64_t security_id{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint8_t md_update_action_raw{0};
    std::string_view md_update_action;
    std::uint8_t md_entry_type_raw{0};
    std::string_view side;
    std::int64_t price_mantissa{0};
    double price{0.0};
    std::int64_t size{0};
    std::uint32_t position_no{0};
    std::uint32_t entering_firm{0};
    std::uint64_t md_insert_timestamp_ns{0};
    std::uint64_t secondary_order_id{0};
    std::uint32_t rpt_seq{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct IncrementalDeleteRow {
    std::int64_t security_id{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint8_t md_update_action_raw{0};
    std::string_view md_update_action;
    std::uint8_t md_entry_type_raw{0};
    std::string_view side;
    std::uint32_t position_no{0};
    std::int64_t size{0};
    std::uint64_t secondary_order_id{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint32_t rpt_seq{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct IncrementalMassDeleteRow {
    std::int64_t security_id{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint8_t md_update_action_raw{0};
    std::string_view md_update_action;
    std::uint8_t md_entry_type_raw{0};
    std::string_view side;
    std::uint32_t position_no{0};
    std::uint32_t rpt_seq{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct IncrementalTradeRow {
    std::int64_t security_id{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint8_t trading_session_id{0};
    std::uint16_t trade_condition_raw{0};
    std::uint16_t trd_sub_type_raw{0};
    std::int64_t price_mantissa{0};
    double price{0.0};
    std::int64_t size{0};
    std::uint64_t trade_id{0};
    std::uint32_t buyer_firm{0};
    std::uint32_t seller_firm{0};
    std::uint32_t trade_date{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint32_t rpt_seq{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct EmptyBookRow {
    std::int64_t security_id{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct ChannelResetRow {
    std::uint8_t match_event_indicator_raw{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
};

struct IncrementalOtherRow {
    std::uint16_t template_id{0};
    std::string_view template_name;
    std::string template_name_storage;
    std::int64_t security_id{0};
    std::uint32_t rpt_seq{0};
    std::uint8_t match_event_indicator_raw{0};
    std::uint64_t md_entry_timestamp_ns{0};
    std::uint64_t packet_sequence_number{0};
    std::uint16_t packet_sequence_version{0};
    std::uint64_t packet_sending_time_ns{0};
    std::uint32_t message_index_in_packet{0};
    std::string body_hex;
    std::string_view decode_status;
};

struct ErrorRow {
    std::string source_file;
    std::string stage;
    std::uint64_t packet_index_in_file{0};
    std::uint32_t message_index_in_packet{0};
    std::uint16_t template_id{0};
    std::string error_code;
    std::string error_text;
    std::string raw_context_hex;
};

class PacketsTable {
  public:
    void add(const PacketRow &row) { rows.push_back(row); }
    const std::vector<PacketRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<PacketRow> rows;
};

class MessagesTable {
  public:
    void add(const MessageRow &row) { rows.push_back(row); }
    const std::vector<MessageRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<MessageRow> rows;
};

class InstrumentsTable {
  public:
    void add(const InstrumentRow &row) { rows.push_back(row); }
    const std::vector<InstrumentRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<InstrumentRow> rows;
};

class InstrumentUnderlyingsTable {
  public:
    void add(const InstrumentUnderlyingRow &row) { rows.push_back(row); }
    const std::vector<InstrumentUnderlyingRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<InstrumentUnderlyingRow> rows;
};

class InstrumentLegsTable {
  public:
    void add(const InstrumentLegRow &row) { rows.push_back(row); }
    const std::vector<InstrumentLegRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<InstrumentLegRow> rows;
};

class InstrumentAttributesTable {
  public:
    void add(const InstrumentAttributeRow &row) { rows.push_back(row); }
    const std::vector<InstrumentAttributeRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<InstrumentAttributeRow> rows;
};

class SnapshotHeadersTable {
  public:
    void add(const SnapshotHeaderRow &row) { rows.push_back(row); }
    const std::vector<SnapshotHeaderRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<SnapshotHeaderRow> rows;
};

class SnapshotOrdersTable {
  public:
    void add(const SnapshotOrderRow &row) { rows.push_back(row); }
    const std::vector<SnapshotOrderRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<SnapshotOrderRow> rows;
};

class IncrementalOrdersTable {
  public:
    void add(const IncrementalOrderRow &row) { rows.push_back(row); }
    const std::vector<IncrementalOrderRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<IncrementalOrderRow> rows;
};

class IncrementalDeletesTable {
  public:
    void add(const IncrementalDeleteRow &row) { rows.push_back(row); }
    const std::vector<IncrementalDeleteRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<IncrementalDeleteRow> rows;
};

class IncrementalMassDeletesTable {
  public:
    void add(const IncrementalMassDeleteRow &row) { rows.push_back(row); }
    const std::vector<IncrementalMassDeleteRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<IncrementalMassDeleteRow> rows;
};

class IncrementalTradesTable {
  public:
    void add(const IncrementalTradeRow &row) { rows.push_back(row); }
    const std::vector<IncrementalTradeRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<IncrementalTradeRow> rows;
};

class IncrementalEmptyBooksTable {
  public:
    void add(const EmptyBookRow &row) { rows.push_back(row); }
    const std::vector<EmptyBookRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<EmptyBookRow> rows;
};

class IncrementalChannelResetsTable {
  public:
    void add(const ChannelResetRow &row) { rows.push_back(row); }
    const std::vector<ChannelResetRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<ChannelResetRow> rows;
};

class IncrementalOtherTable {
  public:
    void add(const IncrementalOtherRow &row) { rows.push_back(row); }
    const std::vector<IncrementalOtherRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<IncrementalOtherRow> rows;
};

class ErrorsTable {
  public:
    void add(const ErrorRow &row) { rows.push_back(row); }
    const std::vector<ErrorRow> &all() const { return rows; }
    void clear() { rows.clear(); }
  private:
    std::vector<ErrorRow> rows;
};

struct TableCollection {
    PacketsTable packets;
    MessagesTable messages;
    InstrumentsTable instruments;
    InstrumentUnderlyingsTable instrument_underlyings;
    InstrumentLegsTable instrument_legs;
    InstrumentAttributesTable instrument_attributes;
    SnapshotHeadersTable snapshot_headers;
    SnapshotOrdersTable snapshot_orders;
    IncrementalOrdersTable incremental_orders;
    IncrementalDeletesTable incremental_deletes;
    IncrementalMassDeletesTable incremental_mass_deletes;
    IncrementalTradesTable incremental_trades;
    IncrementalEmptyBooksTable incremental_empty_books;
    IncrementalChannelResetsTable incremental_channel_resets;
    IncrementalOtherTable incremental_other;
    ErrorsTable errors;

    void clear()
    {
        packets.clear();
        messages.clear();
        instruments.clear();
        instrument_underlyings.clear();
        instrument_legs.clear();
        instrument_attributes.clear();
        snapshot_headers.clear();
        snapshot_orders.clear();
        incremental_orders.clear();
        incremental_deletes.clear();
        incremental_mass_deletes.clear();
        incremental_trades.clear();
        incremental_empty_books.clear();
        incremental_channel_resets.clear();
        incremental_other.clear();
        errors.clear();
    }
};

}  // namespace b3::sbe
