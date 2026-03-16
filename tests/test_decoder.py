import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "20241118"
SCRIPT = REPO_ROOT / "scripts" / "decode_pcap_to_arrow.py"

FEED_TABLES = [
    "incremental_orders",
    "incremental_deletes",
    "incremental_mass_deletes",
    "incremental_trades",
]
OPTIONAL_FEED_TABLES = [
    "incremental_other",
]
IGNORED_DIFF_COLUMNS = {"packet_sending_time_ns"}


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


@pytest.fixture(scope="session")
def build_decoder() -> Path:
    run(["cmake", "-S", ".", "-B", "build"])
    run(["cmake", "--build", "build"])
    binary = REPO_ROOT / "build" / "b3sbe_decode"
    assert binary.exists(), "Decoder binary missing after build"
    return binary


def decode(output_dir: Path, inputs: list[Path], *, tables: list[str] | None = None,
           max_packets: int = 0) -> None:
    cmd = ["python", str(SCRIPT), "--output", str(output_dir)]
    for path in inputs:
        cmd.extend(["--input", str(path)])
    if tables:
        cmd.extend(["--tables", ",".join(tables)])
    if max_packets:
        cmd.extend(["--max-packets", str(max_packets)])
    cmd.append("--skip-build")
    run(cmd)


def read_parquet(path: Path):
    assert path.exists(), f"Missing parquet file: {path}"
    return pq.read_table(path)


def read_first(output: Path, pattern: str):
    matches = list(output.glob(pattern))
    assert matches, f"No files match pattern '{pattern}' under {output}"
    return read_parquet(matches[0])


def read_optional(output: Path, pattern: str):
    matches = list(output.glob(pattern))
    if not matches:
        return None
    return read_parquet(matches[0])


def assert_empty_errors(output: Path) -> None:
    files = list(output.glob("channel_*/errors.parquet"))
    for path in files:
        table = read_parquet(path)
        assert table.num_rows == 0


def read_feed_table(output: Path, channel: int, feed: str, table: str):
    path = output / f"channel_{channel}" / f"feed_{feed}" / f"{table}.parquet"
    assert path.exists(), f"Missing {table} for channel={channel} feed={feed}"
    return pq.read_table(path)


def load_feed_dataframe(output: Path, channel: int, feed: str, table: str):
    path = output / f"channel_{channel}" / f"feed_{feed}" / f"{table}.parquet"
    if not path.exists():
        return None
    df = pq.read_table(path).to_pandas()
    drop = [c for c in IGNORED_DIFF_COLUMNS if c in df.columns]
    if drop:
        df = df.drop(columns=drop)
    cols = sorted(df.columns)
    if cols:
        df = df[cols].sort_values(cols).reset_index(drop=True)
    return df


def assert_columns(table, expected):
    names = set(table.schema.names)
    missing = [col for col in expected if col not in names]
    assert not missing, f"Missing columns {missing} in {table.schema}"


def test_decode_instruments(build_decoder, tmp_path):
    output = tmp_path / "instr"
    decode(
        output,
        [DATA_DIR / "78_Instrument.pcap", DATA_DIR / "88_Instrument.pcap"],
        tables=[
            "instruments",
            "instrument_underlyings",
            "instrument_legs",
            "instrument_attributes",
            "instrument_other",
            "errors",
        ],
    )
    instruments = read_first(output, "channel_*/instruments.parquet")
    assert instruments.num_rows > 0
    assert_columns(
        instruments,
        [
            "security_id",
            "symbol",
            "security_exchange",
            "security_type",
            "security_group",
            "min_price_increment",
            "strike_price",
            "contract_multiplier",
            "packet_sequence_number",
        ],
    )
    legs = read_first(output, "channel_*/instrument_legs.parquet")
    assert legs.num_rows > 0
    assert_columns(legs, ["security_id", "leg_security_id", "leg_symbol", "leg_side"])
    underlyings = read_first(output, "channel_*/instrument_underlyings.parquet")
    assert underlyings.num_rows > 0
    assert_columns(
        underlyings,
        ["security_id", "underlying_security_id", "underlying_symbol", "position_in_group"],
    )
    attrs = read_first(output, "channel_*/instrument_attributes.parquet")
    assert attrs.num_rows > 0
    assert_columns(attrs, ["security_id", "attribute_type", "attribute_value"])
    other = read_optional(output, "channel_*/instrument_other.parquet")
    if other is not None:
        assert_columns(
            other,
            [
                "template_id",
                "template_name",
                "match_event_indicator_raw",
                "md_entry_timestamp_ns",
                "body_hex",
                "decode_status",
            ],
        )
    assert_empty_errors(output)


def test_decode_snapshots(build_decoder, tmp_path):
    output = tmp_path / "snapshot"
    decode(
        output,
        [DATA_DIR / "78_Snapshot.pcap", DATA_DIR / "88_Snapshot.pcap"],
        tables=["snapshot_headers", "snapshot_orders", "snapshot_other", "errors"],
    )
    headers = read_first(output, "channel_*/snapshot_headers.parquet")
    assert headers.num_rows > 0
    assert_columns(
        headers,
        [
            "security_id",
            "last_msg_seq_num_processed",
            "tot_num_bids",
            "snapshot_header_row_id",
        ],
    )
    orders = read_first(output, "channel_*/snapshot_orders.parquet")
    assert orders.num_rows > 0
    assert_columns(
        orders,
        [
            "snapshot_header_row_id",
            "security_id",
            "side",
            "price",
            "size",
            "position_no",
        ],
    )
    other = read_first(output, "channel_*/snapshot_other.parquet")
    assert other.num_rows > 0
    assert_columns(
        other,
        [
            "template_id",
            "template_name",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "body_hex",
            "decode_status",
        ],
    )
    assert_empty_errors(output)


def test_decode_incrementals(build_decoder, tmp_path):
    output = tmp_path / "incremental"
    inputs = [
        DATA_DIR / "78_Incremental_feedA.pcap",
        DATA_DIR / "78_Incremental_feedB.pcap",
        DATA_DIR / "88_Incremental_feedA.pcap",
        DATA_DIR / "88_Incremental_feedB.pcap",
    ]
    decode(
        output,
        inputs,
        tables=[
            "incremental_orders",
            "incremental_deletes",
            "incremental_mass_deletes",
            "incremental_trades",
            "incremental_empty_books",
            "incremental_channel_resets",
            "incremental_other",
            "errors",
        ],
        max_packets=2000,
    )
    orders = read_first(output, "channel_*/feed_*/incremental_orders.parquet")
    assert orders.num_rows > 0
    assert_columns(
        orders,
        [
            "security_id",
            "match_event_indicator_raw",
            "md_update_action",
            "price",
            "size",
            "position_no",
            "secondary_order_id",
        ],
    )
    deletes = read_first(output, "channel_*/feed_*/incremental_deletes.parquet")
    assert deletes.num_rows > 0
    assert_columns(
        deletes,
        [
            "security_id",
            "side",
            "position_no",
            "rpt_seq",
        ],
    )
    trades = read_first(output, "channel_*/feed_*/incremental_trades.parquet")
    assert trades.num_rows > 0
    assert_columns(
        trades,
        [
            "security_id",
            "price",
            "size",
            "trade_id",
            "buyer_firm",
            "seller_firm",
        ],
    )
    empty_books = read_optional(output, "channel_*/feed_*/incremental_empty_books.parquet")
    if empty_books is not None:
        assert_columns(
            empty_books,
            [
                "security_id",
                "match_event_indicator_raw",
                "md_entry_timestamp_ns",
            ],
        )

    resets = read_optional(output, "channel_*/feed_*/incremental_channel_resets.parquet")
    if resets is not None:
        assert_columns(
            resets,
            [
                "match_event_indicator_raw",
                "md_entry_timestamp_ns",
            ],
        )

    other = read_first(output, "channel_*/feed_*/incremental_other.parquet")
    assert other.num_rows > 0
    assert_columns(
        other,
        [
            "template_id",
            "template_name",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "body_hex",
            "decode_status",
        ],
    )
    assert_empty_errors(output)


def test_errors_table_with_corrupt_pcap(build_decoder, tmp_path):
    bad_pcap = tmp_path / "broken.pcap"
    bad_pcap.write_bytes(b"short")
    output = tmp_path / "broken_output"
    decode(output, [bad_pcap], tables=["errors"])
    errors = read_first(output, "channel_*/errors.parquet")
    assert errors.num_rows > 0
    stages = set(errors.column("stage").to_pylist())
    assert "pcap" in stages


def test_feed_legs_match(build_decoder, tmp_path):
    for channel in (78, 88):
        output = tmp_path / f"legs_{channel}"
        decode(
            output,
            [
                DATA_DIR / f"{channel}_Incremental_feedA.pcap",
                DATA_DIR / f"{channel}_Incremental_feedB.pcap",
            ],
            tables=FEED_TABLES + ["errors"],
            max_packets=20000,
        )
        assert_empty_errors(output)

        for table in FEED_TABLES:
            df_a = load_feed_dataframe(output, channel, "A", table)
            df_b = load_feed_dataframe(output, channel, "B", table)
            if df_a is None and df_b is None:
                continue
            assert df_a is not None and df_b is not None, f"Missing {table} for channel {channel}"
            assert abs(len(df_a) - len(df_b)) <= 1, f"Row count mismatch for {table} channel {channel}"
            min_len = min(len(df_a), len(df_b))
            df_a_trim = df_a.iloc[:min_len].reset_index(drop=True)
            df_b_trim = df_b.iloc[:min_len].reset_index(drop=True)
            pd.testing.assert_frame_equal(df_a_trim, df_b_trim, check_dtype=False)

        for table in OPTIONAL_FEED_TABLES:
            df_a = load_feed_dataframe(output, channel, "A", table)
            df_b = load_feed_dataframe(output, channel, "B", table)
            if df_a is None and df_b is None:
                continue
            assert df_a is not None and df_b is not None, f"Missing {table} for channel {channel}"
            assert abs(len(df_a) - len(df_b)) <= 10, f"Row count mismatch for {table} channel {channel}"
