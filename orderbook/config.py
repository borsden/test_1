from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Tuple


@dataclass(frozen=True)
class PipelineConfig:
    """High-level configuration for the consolidated book pipeline."""

    data_root: Path
    tickers: Tuple[str, ...]
    output_root: Path
    feeds: Tuple[str, ...] = ("feed_A", "feed_B")
    channels: Tuple[str, ...] | None = None
    batch_size: int = 1_000
    since_ns: int | None = None
    until_ns: int | None = None
    show_progress: bool = False
    suppress_rpt_warnings: bool = False

    def __post_init__(self) -> None:  # noqa: D401 - short normalisation logic
        normalized_root = self.data_root.resolve()
        object.__setattr__(self, "data_root", normalized_root)

        normalized_output = self.output_root.resolve()
        object.__setattr__(self, "output_root", normalized_output)

        normalized_tickers = tuple(_normalize_symbol(t) for t in self.tickers)
        if not normalized_tickers:
            raise ValueError("At least one ticker must be specified")
        object.__setattr__(self, "tickers", normalized_tickers)

        normalized_feeds = tuple(_normalize_feed_name(feed) for feed in self.feeds)
        if not normalized_feeds:
            raise ValueError("At least one feed must be specified")
        object.__setattr__(self, "feeds", normalized_feeds)

        if self.channels is not None:
            normalized_channels = tuple(sorted(set(self.channels)))
            if not normalized_channels:
                raise ValueError("channels must be a non-empty iterable or None")
            for channel in normalized_channels:
                if not channel.startswith("channel_"):
                    raise ValueError(
                        f"Channel '{channel}' must use directory name channel_XX"
                    )
            object.__setattr__(self, "channels", normalized_channels)

        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.since_ns is not None and self.since_ns < 0:
            raise ValueError("since_ns must be non-negative nanoseconds")

        if self.until_ns is not None and self.until_ns < 0:
            raise ValueError("until_ns must be non-negative nanoseconds")
        if self.since_ns is not None and self.until_ns is not None:
            if self.since_ns > self.until_ns:
                raise ValueError("since_ns must be <= until_ns")

        object.__setattr__(self, "show_progress", bool(self.show_progress))


def _normalize_symbol(symbol: str) -> str:
    cleaned = symbol.strip()
    if not cleaned:
        raise ValueError("Ticker symbols cannot be empty")
    return cleaned.upper()


def _normalize_feed_name(feed: str) -> str:
    cleaned = feed.strip()
    if not cleaned:
        raise ValueError("Feed names cannot be empty")
    return cleaned
