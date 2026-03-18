"""CLI для оценки моментума и волатильности по L1-стакану."""

from __future__ import annotations

from collections import deque
from pathlib import Path
import math
import statistics
import time

import click
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DEFAULT_COLUMNS = ("bid_price", "bid_size", "ask_price", "ask_size")
PRICE_COLUMNS = ("bid_price", "ask_price", "mid_price")
DURATION_UNITS = (("min", 60_000), ("ms", 1), ("s", 1_000))


def parse_duration_ms(value: str | None, *, param_name: str, allow_none: bool = False) -> int | None:
    """Конвертирует строку вида '500ms', '2s', '1.5min' в миллисекунды."""

    if value is None:
        if allow_none:
            return None
        raise click.BadParameter("Значение должно быть указано", param_hint=param_name)

    text = str(value).strip().lower()
    text = text.replace("_", "").replace(" ", "")
    if not text:
        raise click.BadParameter("Значение не должно быть пустым", param_hint=param_name)

    multiplier = 1
    magnitude_part = text
    for suffix, factor in DURATION_UNITS:
        if text.endswith(suffix):
            magnitude_part = text[: -len(suffix)] or ""
            multiplier = factor
            break

    if not magnitude_part:
        raise click.BadParameter("Не удалось определить числовую часть", param_hint=param_name)

    try:
        magnitude = float(magnitude_part)
    except ValueError as exc:
        raise click.BadParameter("Некорректное число", param_hint=param_name) from exc

    ms_value = int(round(magnitude * multiplier))
    if ms_value <= 0:
        raise click.BadParameter("Значение должно быть > 0", param_hint=param_name)
    return ms_value


def format_duration_label(ms_value: int) -> str:
    """Читабельное представление миллисекунд."""

    if ms_value >= 60_000:
        value = ms_value / 60_000
        unit = "мин"
    elif ms_value >= 1_000:
        value = ms_value / 1_000
        unit = "с"
    else:
        value = ms_value
        unit = "мс"

    if isinstance(value, float):
        value_str = f"{value:.3f}".rstrip("0").rstrip(".")
    else:
        value_str = str(value)
    return f"{value_str} {unit}"


class OnlineMetrics:
    """Онлайн-алгоритм для моментума и волатильности по временным окнам."""

    def __init__(
        self,
        momentum_lookback_ms: int,
        momentum_mode: str,
        volatility_lookback_ms: int,
        volatility_frequency_ms: int,
    ) -> None:
        self.momentum_lookback_ns = self._ms_to_ns(momentum_lookback_ms)
        self.momentum_mode = momentum_mode
        self.volatility_lookback_ns = self._ms_to_ns(volatility_lookback_ms)
        self.volatility_frequency_ns = self._ms_to_ns(volatility_frequency_ms)
        self._volatility_min_count = max(
            2,
            math.ceil(
                self.volatility_lookback_ns / self.volatility_frequency_ns
            )
            if self.volatility_frequency_ns > 0
            else 2,
        )

        self._momentum_prices: deque[tuple[int, float]] = deque()
        self._volatility_prices: deque[tuple[int, float]] = deque()
        self._returns: deque[tuple[int, float]] = deque()
        self._returns_sum = 0.0
        self._returns_sum_sq = 0.0

    @staticmethod
    def _ms_to_ns(value_ms: int) -> int:
        return max(int(value_ms), 0) * 1_000_000

    def update(self, timestamp_ns: int, price: float) -> tuple[float | None, float | None, float]:
        start = time.monotonic()
        momentum = self._update_momentum(timestamp_ns, price)
        volatility = self._update_volatility(timestamp_ns, price)
        elapsed = time.monotonic() - start
        return momentum, volatility, elapsed

    def _update_momentum(self, timestamp_ns: int, price: float) -> float | None:
        if self.momentum_lookback_ns <= 0:
            return 0.0

        self._momentum_prices.append((timestamp_ns, price))
        target_ns = timestamp_ns - self.momentum_lookback_ns

        while len(self._momentum_prices) >= 2 and self._momentum_prices[1][0] <= target_ns:
            self._momentum_prices.popleft()

        if self._momentum_prices and self._momentum_prices[0][0] <= target_ns:
            reference_price = self._momentum_prices[0][1]
            if self.momentum_mode == "log":
                if reference_price > 0 and price > 0:
                    return math.log(price / reference_price)
                return None
            return price - reference_price
        return None

    def _update_volatility(self, timestamp_ns: int, price: float) -> float | None:
        if self.volatility_lookback_ns <= 0 or self.volatility_frequency_ns <= 0:
            return 0.0

        self._volatility_prices.append((timestamp_ns, price))
        target_ns = timestamp_ns - self.volatility_frequency_ns

        while len(self._volatility_prices) >= 2 and self._volatility_prices[1][0] <= target_ns:
            self._volatility_prices.popleft()

        base_price = None
        if self._volatility_prices and self._volatility_prices[0][0] <= target_ns:
            base_price = self._volatility_prices[0][1]

        ret = None
        if base_price is not None and base_price > 0 and price > 0:
            ret = math.log(price / base_price)

        if ret is not None:
            self._returns.append((timestamp_ns, ret))
            self._returns_sum += ret
            self._returns_sum_sq += ret * ret

        cutoff_ns = timestamp_ns - self.volatility_lookback_ns
        while self._returns and self._returns[0][0] < cutoff_ns:
            _, old_ret = self._returns.popleft()
            self._returns_sum -= old_ret
            self._returns_sum_sq -= old_ret * old_ret

        if len(self._returns) >= self._volatility_min_count:
            earliest_ts = self._returns[0][0]
            if earliest_ts - cutoff_ns <= self.volatility_frequency_ns:
                n = len(self._returns)
                mean = self._returns_sum / n
                variance = (self._returns_sum_sq - n * mean * mean) / (n - 1)
                variance = max(variance, 0.0)
                return math.sqrt(variance)
        return None


def remove_duplicates(df: pd.DataFrame, key_columns: tuple[str, ...] = DEFAULT_COLUMNS) -> pd.DataFrame:
    columns = tuple(key_columns) or DEFAULT_COLUMNS
    subset = df.loc[:, columns]
    prev = subset.shift()
    same = subset.eq(prev) | (subset.isna() & prev.isna())
    keep_mask = ~same.all(axis=1)
    keep_mask.iloc[0] = True
    return df.loc[keep_mask]


def load_order_book(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = remove_duplicates(df)

    df["timestamp"] = pd.to_datetime(df["packet_sending_time_ns"], unit="ns")
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

    numeric_cols = list(DEFAULT_COLUMNS)
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df[numeric_cols] = df[numeric_cols].ffill()
    df = df.dropna(subset=["bid_price", "ask_price"])
    df["mid_price"] = df[["bid_price", "ask_price"]].mean(axis=1)
    keep_cols = ["timestamp", *numeric_cols, "mid_price"]
    return df.loc[:, keep_cols]


def resample_book(df: pd.DataFrame, frequency_ms: int) -> pd.DataFrame:
    freq = f"{frequency_ms}ms"
    indexed = df.set_index("timestamp")
    grouped = indexed.resample(freq).last()
    grouped = grouped.ffill()
    grouped = grouped.dropna(subset=["bid_price", "ask_price"])
    grouped["mid_price"] = grouped[["bid_price", "ask_price"]].mean(axis=1)
    return grouped.reset_index()


def compute_metrics(
    df: pd.DataFrame,
    momentum_lookback_ms: int,
    momentum_mode: str,
    volatility_lookback_ms: int,
    volatility_frequency_ms: int,
) -> tuple[pd.DataFrame, dict[str, list[float]]]:
    enriched = df.copy()
    duration_stats: dict[str, list[float]] = {}
    timestamps = enriched["timestamp"].astype("int64").to_numpy()

    for column in PRICE_COLUMNS:
        metrics = OnlineMetrics(
            momentum_lookback_ms,
            momentum_mode,
            volatility_lookback_ms,
            volatility_frequency_ms,
        )
        momentum_values: list[float | None] = []
        volatility_values: list[float | None] = []
        durations: list[float] = []

        for timestamp_ns, price in zip(timestamps, enriched[column].to_numpy()):
            momentum, volatility, elapsed = metrics.update(int(timestamp_ns), float(price))
            momentum_values.append(momentum)
            volatility_values.append(volatility)
            durations.append(elapsed)

        enriched[f"{column}_momentum"] = momentum_values
        enriched[f"{column}_volatility"] = volatility_values
        enriched[f"{column}_calc_ms"] = [d * 1000.0 for d in durations]
        duration_stats[column] = durations

    return enriched, duration_stats


def plot_metrics(
    df: pd.DataFrame,
    columns: tuple[str, ...],
    output_path: Path,
    freq_ms: int | None,
    momentum_lookback: int,
    momentum_mode: str,
    volatility_lookback: int,
    volatility_frequency: int,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("Momentum", "Volatility"),
    )

    colors = {"bid_price": "#ef553b", "ask_price": "#00cc96", "mid_price": "#636efa"}
    names = {"bid_price": "Bid", "ask_price": "Ask", "mid_price": "Mid"}

    for column in columns:
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df[f"{column}_momentum"],
                mode="lines",
                name=f"{names.get(column, column)} Momentum",
                line=dict(color=colors.get(column)),
                legendgroup=column,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df[f"{column}_volatility"],
                mode="lines",
                name=f"{names.get(column, column)} Volatility",
                line=dict(color=colors.get(column), dash="dot"),
                legendgroup=column,
            ),
            row=2,
            col=1,
        )

    freq_label = format_duration_label(freq_ms) if freq_ms else "raw"
    momentum_label = format_duration_label(momentum_lookback)
    volatility_label = format_duration_label(volatility_lookback)
    volatility_freq_label = format_duration_label(volatility_frequency)
    mode_name = {
        "price": "ценовой",
        "log": "лог-доходность",
    }.get(momentum_mode, momentum_mode)
    fig.update_layout(
        title=(
            "Momentum & Volatility | "
            f"дискретизация: {freq_label}, "
            f"моментум: {mode_name} (горизонт {momentum_label}), "
            f"волатильность: окно {volatility_label}, шаг {volatility_freq_label}"
        ),
        height=800,
        template="plotly_white",
        showlegend=True,
    )
    fig.update_xaxes(title="Timestamp", row=2, col=1)
    fig.update_yaxes(title="Momentum", row=1, col=1)
    fig.update_yaxes(title="Volatility", row=2, col=1)

    fig.write_image(output_path, width=1400, height=800, scale=2)
    return output_path


def summarize_durations(durations: dict[str, list[float]]) -> list[str]:
    lines: list[str] = []
    for column, samples in durations.items():
        if not samples:
            continue
        ms_values = [s * 1000.0 for s in samples]
        mean_ms = statistics.fmean(ms_values)
        std_ms = statistics.stdev(ms_values) if len(ms_values) > 1 else 0.0
        lines.append(
            f"{column}: mean={mean_ms:.4f} ms, std={std_ms:.4f} ms over {len(ms_values)} updates"
        )
    return lines


@click.command()
@click.argument("l1_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--frequency",
    type=str,
    default=None,
    help="Шаг дискретизации (ms/s/min).",
)
@click.option(
    "--momentum-lookback",
    type=str,
    default="1000ms",
    show_default=True,
    help="Горизонт моментума (ms/s/min).",
)
@click.option(
    "--momentum-mode",
    type=click.Choice(["price", "log"], case_sensitive=False),
    default="price",
    show_default=True,
    help="Метрика моментума: 'price' (P(t)-P(t-Δt)) или 'log' (ln P(t)/P(t-Δt)).",
)
@click.option(
    "--volatility-lookback",
    type=str,
    default="10000ms",
    show_default=True,
    help="Длина окна волатильности (ms/s/min).",
)
@click.option(
    "--volatility-frequency",
    type=str,
    default="1000ms",
    show_default=True,
    help="Шаг между точками при расчёте лог-доходностей (ms/s/min).",
)
@click.option(
    "--output-plot",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("volatility_momentum_robot/combined_metrics.png"),
    show_default=True,
    help="PNG-файл для графика.",
)
def main(
    l1_path: Path,
    frequency: str | None,
    momentum_lookback: str,
    momentum_mode: str,
    volatility_lookback: str,
    volatility_frequency: str,
    output_plot: Path,
) -> None:
    """Считает онлайн-моментум и волатильность, выводит графики и статистику."""

    frequency_ms = parse_duration_ms(frequency, param_name="--frequency", allow_none=True)
    momentum_lookback_ms = parse_duration_ms(momentum_lookback, param_name="--momentum-lookback")
    momentum_mode = momentum_mode.lower()
    volatility_lookback_ms = parse_duration_ms(volatility_lookback, param_name="--volatility-lookback")
    volatility_frequency_ms = parse_duration_ms(volatility_frequency, param_name="--volatility-frequency")

    df = load_order_book(l1_path)
    if frequency_ms:
        df = resample_book(df, frequency_ms)

    enriched, durations = compute_metrics(
        df,
        momentum_lookback_ms=momentum_lookback_ms,
        momentum_mode=momentum_mode,
        volatility_lookback_ms=volatility_lookback_ms,
        volatility_frequency_ms=volatility_frequency_ms,
    )

    print("\nСтатистика по времени вычислений:")
    for line in summarize_durations(durations):
        print(line)

    print("\nГенерируем PNG-график...")
    path = plot_metrics(
        enriched,
        PRICE_COLUMNS,
        output_plot,
        frequency_ms,
        momentum_lookback_ms,
        momentum_mode,
        volatility_lookback_ms,
        volatility_frequency_ms,
    )
    print(f"Saved: {path}")



if __name__ == "__main__":
    main()
