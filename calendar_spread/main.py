"""Расчёт и визуализация календарного спреда для выбранных фьючерсов WDO."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

DEFAULT_COLUMNS = ("bid_price", "bid_size", "ask_price", "ask_size")
RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "orderbook"
LEG_MERGE_TOLERANCE = pd.Timedelta("25ms")
COMBO_MERGE_TOLERANCE = pd.Timedelta("2s")
PLOT_OUTPUT = Path(__file__).resolve().parent / "spread_plot.html"

# Configuration for the legs we want to combine into a calendar spread.
LEG_DEFINITIONS = (
    {"symbol": "WDOF25", "side": "buy", "ratio": 1.0},
    {"symbol": "WDOZ24", "side": "sell", "ratio": 1.0},
)

# Ready-made exchange instrument for comparison.
COMBO_SYMBOL = "WD1Z24F25"


def remove_duplicates(df: pd.DataFrame, key_columns: tuple = DEFAULT_COLUMNS) -> pd.DataFrame:
    """Удаляет подряд идущие строки с одинаковыми лучшими котировками.

    Биржевые фиды часто присылают несколько сообщений с идентичными значениями
    бид/аск, поэтому перед расчётами избавляемся от полных дублей: сравниваем
    набор ключевых колонок с предыдущей строкой и оставляем только изменение.

    Args:
        df: исходный датафрейм с колонками стакана.
        key_columns: колонки, по которым проверяем равенство записей.

    Returns:
        Отфильтрованный датафрейм без дубликатов подряд.
    """

    columns = tuple(key_columns) or DEFAULT_COLUMNS

    subset = df.loc[:, columns]
    prev = subset.shift()
    same = subset.eq(prev) | (subset.isna() & prev.isna())
    keep_mask = ~same.all(axis=1)
    keep_mask.iloc[0] = True

    cleaned = df.loc[keep_mask]
    return cleaned


def load_order_book(symbol: str) -> pd.DataFrame:
    """Загружает L1-стакан по символу, нормализует числовые поля и рассчитывает mid.

    На выходе получаем набор, где каждая запись снабжена меткой времени,
    очищена от заглушек и имеет рассчитанный mid-price, что упрощает дальнейшее
    объединение ног.

    Args:
        symbol: код фьючерса из папки `data/orderbook`.

    Returns:
        Датафрейм с колонками timestamp, bid/ask и mid-price.
    """

    csv_path = RAW_DATA_DIR / symbol / "book_l1.csv"
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


def prepare_leg_frame(symbol: str) -> pd.DataFrame:
    """Готовит датафрейм для отдельной ноги с уникальными именами колонок.

    Каждой ноге присваиваем постфикс с тикером, чтобы при merge не путать
    котировки и объёмы разных контрактов.
    """

    df = load_order_book(symbol)
    rename_map = {col: f"{col}_{symbol}" for col in df.columns if col != "timestamp"}
    leg_df = df.rename(columns=rename_map)
    return leg_df


def merge_leg_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Последовательно совмещает ноги по времени с помощью merge_asof.

    Используем ближайшие наблюдения в пределах заданного допускa, чтобы смерджить
    котировки разных контрактов в единый временной ряд.
    """

    sorted_frames = [frame.sort_values("timestamp") for frame in frames]
    merged = sorted_frames[0]
    for frame in sorted_frames[1:]:
        merged = pd.merge_asof(
            merged,
            frame,
            on="timestamp",
            direction="nearest",
            tolerance=LEG_MERGE_TOLERANCE,
        )
    return merged


def calc_mid_spread(merged: pd.DataFrame, legs: tuple[dict, ...]) -> pd.Series:
    """Считает теоретический спред как сумму mid по ногам с учётом знаков и весов."""

    contributions = []
    for leg in legs:
        symbol = leg["symbol"]
        sign = 1.0 if leg["side"] == "buy" else -1.0
        contributions.append(sign * leg["ratio"] * merged[f"mid_price_{symbol}"])
    return sum(contributions)


def calc_executable_spread(merged: pd.DataFrame, legs: tuple[dict, ...]) -> pd.Series:
    """Возвращает исполнимый спред: покупки на ask, продажи на bid."""

    total = 0.0
    for leg in legs:
        symbol = leg["symbol"]
        price_col = "ask_price" if leg["side"] == "buy" else "bid_price"
        sign = 1.0 if leg["side"] == "buy" else -1.0
        total += sign * leg["ratio"] * merged[f"{price_col}_{symbol}"]
    return total


def invert_leg_sides(legs: tuple[dict, ...]) -> tuple[dict, ...]:
    """Меняет направление ног, чтобы посчитать обратный спред."""

    flipped = []
    for leg in legs:
        flipped.append({**leg, "side": "sell" if leg["side"] == "buy" else "buy"})
    return tuple(flipped)


def build_spread_frame() -> pd.DataFrame:
    """Формирует единый датафрейм со всем нужным: трейспредами и котировкой комбо.

    1. Загружаем ноги и объединяем их по времени.
    2. Считаем mid и исполнимые спреды в обоих направлениях.
    3. Приклеиваем биржевой инструмент WD1Z24F25 для сравнения.
    """

    leg_frames = [prepare_leg_frame(config["symbol"]) for config in LEG_DEFINITIONS]
    merged = merge_leg_frames(leg_frames)

    required_mid_cols = [f"mid_price_{leg['symbol']}" for leg in LEG_DEFINITIONS]
    merged = merged.dropna(subset=required_mid_cols)

    spread = merged[["timestamp"]].copy()
    spread["spread_mid"] = calc_mid_spread(merged, LEG_DEFINITIONS)
    spread["spread_exec_buy_sell"] = calc_executable_spread(merged, LEG_DEFINITIONS)
    spread["spread_exec_sell_buy"] = calc_executable_spread(merged, invert_leg_sides(LEG_DEFINITIONS))

    combo_frame = prepare_leg_frame(COMBO_SYMBOL)
    combo_frame = combo_frame.rename(columns={f"mid_price_{COMBO_SYMBOL}": "wd1_mid_price"})
    spread = pd.merge_asof(
        spread.sort_values("timestamp"),
        combo_frame[["timestamp", "wd1_mid_price"]].sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=COMBO_MERGE_TOLERANCE,
    )
    return spread.dropna(subset=["spread_mid"])


def plot_spreads(spread_df: pd.DataFrame) -> Path:
    """Создаёт интерактивный Plotly-график спредов и сохраняет его в HTML."""
    fig = go.Figure()
    lines = [
        ("spread_mid", "Mid spread"),
        ("spread_exec_buy_sell", "Buy front / Sell back"),
        ("spread_exec_sell_buy", "Sell front / Buy back"),
    ]
    if "wd1_mid_price" in spread_df.columns:
        lines.append(("wd1_mid_price", f"{COMBO_SYMBOL} mid"))

    for column, label in lines:
        if column not in spread_df.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=spread_df["timestamp"],
                y=spread_df[column],
                mode="lines",
                name=label,
            )
        )

    leg_symbols = " vs ".join(leg["symbol"] for leg in LEG_DEFINITIONS)
    fig.update_layout(
        title=f"WDO calendar spread ({leg_symbols})",
        xaxis_title="Timestamp",
        yaxis_title="Spread (points)",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig.write_html(PLOT_OUTPUT, auto_open=False, include_plotlyjs="cdn")
    return PLOT_OUTPUT


def main() -> None:
    """Точка входа: собирает данные, печатает срез и рисует график."""
    spread_df = build_spread_frame()
    preview_cols = [
        "timestamp",
        "spread_mid",
        "spread_exec_buy_sell",
        "spread_exec_sell_buy",
    ]
    if "wd1_mid_price" in spread_df.columns:
        preview_cols.append("wd1_mid_price")

    print("Spread snapshot:")
    print(spread_df.loc[:, preview_cols].tail())

    output_path = plot_spreads(spread_df)
    print(f"Interactive plot saved to: {output_path}")


if __name__ == "__main__":
    main()
