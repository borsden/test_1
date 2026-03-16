import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "20241118"
SCRIPT = REPO_ROOT / "scripts" / "decode_pcap_to_arrow.py"


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
            "errors",
        ],
    )
    instruments = read_parquet(output / "instruments.parquet")
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
    legs = read_parquet(output / "instrument_legs.parquet")
    assert legs.num_rows > 0
    assert_columns(legs, ["security_id", "leg_security_id", "leg_symbol", "leg_side"])
    underlyings = read_parquet(output / "instrument_underlyings.parquet")
    assert underlyings.num_rows > 0
    assert_columns(
        underlyings,
        ["security_id", "underlying_security_id", "underlying_symbol", "position_in_group"],
    )
    attrs = read_parquet(output / "instrument_attributes.parquet")
    assert attrs.num_rows > 0
    assert_columns(attrs, ["security_id", "attribute_type", "attribute_value"])
    errors = read_parquet(output / "errors.parquet")
    assert errors.num_rows == 0


def test_decode_snapshots(build_decoder, tmp_path):
    output = tmp_path / "snapshot"
    decode(
        output,
        [DATA_DIR / "78_Snapshot.pcap", DATA_DIR / "88_Snapshot.pcap"],
        tables=["snapshot_headers", "snapshot_orders", "errors"],
    )
    headers = read_parquet(output / "snapshot_headers.parquet")
    assert headers.num_rows > 0
    assert_columns(
        headers,
        [
            "security_id",
            "channel_hint",
            "last_msg_seq_num_processed",
            "tot_num_bids",
            "snapshot_header_row_id",
        ],
    )
    orders = read_parquet(output / "snapshot_orders.parquet")
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
    errors = read_parquet(output / "errors.parquet")
    assert errors.num_rows == 0


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
    orders = read_parquet(output / "incremental_orders.parquet")
    assert orders.num_rows > 0
    assert_columns(
        orders,
        [
            "security_id",
            "channel_hint",
            "feed_leg",
            "match_event_indicator_raw",
            "md_update_action",
            "price",
            "size",
            "position_no",
            "secondary_order_id",
        ],
    )
    deletes = read_parquet(output / "incremental_deletes.parquet")
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
    trades = read_parquet(output / "incremental_trades.parquet")
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
    empty_books = read_parquet(output / "incremental_empty_books.parquet")
    assert_columns(
        empty_books,
        [
            "channel_hint",
            "feed_leg",
            "security_id",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
        ],
    )

    resets = read_parquet(output / "incremental_channel_resets.parquet")
    assert_columns(
        resets,
        [
            "channel_hint",
            "feed_leg",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
        ],
    )

    other = read_parquet(output / "incremental_other.parquet")
    assert other.num_rows > 0
    assert_columns(
        other,
        [
            "channel_hint",
            "feed_leg",
            "template_id",
            "match_event_indicator_raw",
            "md_entry_timestamp_ns",
            "body_hex",
            "decode_status",
        ],
    )
    errors = read_parquet(output / "errors.parquet")
    assert errors.num_rows == 0


def test_errors_table_with_corrupt_pcap(build_decoder, tmp_path):
    bad_pcap = tmp_path / "broken.pcap"
    bad_pcap.write_bytes(b"short")
    output = tmp_path / "broken_output"
    decode(output, [bad_pcap], tables=["errors"])
    errors = read_parquet(output / "errors.parquet")
    assert errors.num_rows > 0
    stages = set(errors.column("stage").to_pylist())
    assert "pcap" in stages
