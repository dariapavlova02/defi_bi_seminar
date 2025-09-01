"""Basic feature engineering for DeFi BI-ETL pipeline."""

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from ..utils.io import get_processed_data_path, load_csv, save_csv


def calculate_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate price-based features."""
    df = df.copy()

    # Price change features
    if "pct_24h" in df.columns:
        df["price_change_24h_flag"] = np.where(
            df["pct_24h"] > 10,
            "high_gain",
            np.where(df["pct_24h"] < -10, "high_loss", "normal"),
        )

        df["price_change_24h_category"] = pd.cut(
            df["pct_24h"],
            bins=[-np.inf, -20, -10, -5, 0, 5, 10, 20, np.inf],
            labels=[
                "extreme_loss",
                "high_loss",
                "moderate_loss",
                "slight_loss",
                "slight_gain",
                "moderate_gain",
                "high_gain",
                "extreme_gain",
            ],
        )

    if "pct_7d" in df.columns:
        df["price_change_7d_flag"] = np.where(
            df["pct_7d"] > 20,
            "strong_gain",
            np.where(df["pct_7d"] < -20, "strong_loss", "stable"),
        )

    # Market cap features
    if "market_cap" in df.columns:
        df["market_cap_billion"] = df["market_cap"] / 1e9
        df["market_cap_category"] = pd.cut(
            df["market_cap"],
            bins=[0, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, np.inf],
            labels=["micro", "small", "medium", "large", "mega", "giga", "tera"],
        )

    # Volume features
    if "total_volume" in df.columns and "market_cap" in df.columns:
        df["volume_market_cap_ratio"] = df["total_volume"] / df["market_cap"]
        df["volume_activity"] = pd.cut(
            df["volume_market_cap_ratio"],
            bins=[0, 0.01, 0.05, 0.1, 0.2, np.inf],
            labels=["very_low", "low", "medium", "high", "very_high"],
        )

    return df


def calculate_rolling_features(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "tvl_usd",
    window_days: List[int] = [7, 30],
) -> pd.DataFrame:
    """Calculate rolling window features for time series data."""
    df = df.copy()

    # Ensure date column is datetime
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)

    # Calculate rolling statistics for each protocol
    if "protocol_name" in df.columns:
        for window in window_days:
            # Rolling mean
            df[f"{value_col}_rolling_mean_{window}d"] = df.groupby("protocol_name")[
                value_col
            ].transform(lambda x: x.rolling(window=window, min_periods=1).mean())

            # Rolling std
            df[f"{value_col}_rolling_std_{window}d"] = df.groupby("protocol_name")[
                value_col
            ].transform(lambda x: x.rolling(window=window, min_periods=1).std())

            # Rolling change
            df[f"{value_col}_rolling_change_{window}d"] = df.groupby("protocol_name")[
                value_col
            ].transform(lambda x: x.pct_change(periods=window))

            # Rolling volatility (coefficient of variation)
            df[f"{value_col}_rolling_volatility_{window}d"] = (
                df[f"{value_col}_rolling_std_{window}d"]
                / df[f"{value_col}_rolling_mean_{window}d"]
            )

    return df


def calculate_tvl_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate TVL-specific features."""
    df = df.copy()

    if "tvl_usd" in df.columns:
        # TVL change flags
        if "change_1d" in df.columns:
            df["tvl_change_1d_flag"] = np.where(
                df["change_1d"] > 5,
                "strong_growth",
                np.where(df["change_1d"] < -5, "strong_decline", "stable"),
            )

        if "change_7d" in df.columns:
            df["tvl_change_7d_flag"] = np.where(
                df["change_7d"] > 10,
                "weekly_growth",
                np.where(df["change_7d"] < -10, "weekly_decline", "stable"),
            )

        # TVL categories
        df["tvl_category"] = pd.cut(
            df["tvl_usd"],
            bins=[0, 1e6, 1e7, 1e8, 1e9, 1e10, np.inf],
            labels=["micro", "small", "medium", "large", "mega", "giga"],
        )

        # TVL in billions for readability
        df["tvl_billion"] = df["tvl_usd"] / 1e9

    return df


def calculate_market_sentiment_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate market sentiment indicators."""
    df = df.copy()

    # Fear & Greed indicators based on price changes
    if "pct_24h" in df.columns:
        df["sentiment_24h"] = np.where(
            df["pct_24h"] > 5, "greed", np.where(df["pct_24h"] < -5, "fear", "neutral")
        )

    if "pct_7d" in df.columns:
        df["sentiment_7d"] = np.where(
            df["pct_7d"] > 10,
            "bullish",
            np.where(df["pct_7d"] < -10, "bearish", "sideways"),
        )

    # Market dominance (if market cap data available)
    if "market_cap" in df.columns:
        total_market_cap = df["market_cap"].sum()
        df["market_dominance_pct"] = (df["market_cap"] / total_market_cap) * 100

        df["dominance_category"] = pd.cut(
            df["market_dominance_pct"],
            bins=[0, 1, 5, 10, 25, 50, np.inf],
            labels=["minimal", "low", "moderate", "significant", "major", "dominant"],
        )

    return df


def add_time_features(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Add time-based features."""
    df = df.copy()

    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])

        # Extract time components
        df["year"] = df[date_col].dt.year
        df["month"] = df[date_col].dt.month
        df["day"] = df[date_col].dt.day
        df["day_of_week"] = df[date_col].dt.dayofweek
        df["quarter"] = df[date_col].dt.quarter

        # Week of year
        df["week_of_year"] = df[date_col].dt.isocalendar().week

        # Month name
        df["month_name"] = df[date_col].dt.month_name()

        # Day name
        df["day_name"] = df[date_col].dt.day_name()

        # Is weekend
        df["is_weekend"] = df[date_col].dt.dayofweek.isin([5, 6])

        # Days since epoch
        df["days_since_epoch"] = (df[date_col] - pd.Timestamp("1970-01-01")).dt.days

    return df


def process_markets_features(input_file: str) -> str:
    """Process markets data and add features."""
    print(f"Adding features to markets data: {input_file}")

    df = load_csv(input_file)
    df = calculate_price_features(df)
    df = calculate_market_sentiment_features(df)

    output_path = get_processed_data_path("cg_markets_with_features.csv")
    save_csv(df, output_path)

    return str(output_path)


def process_tvl_features(input_file: str) -> str:
    """Process TVL data and add features."""
    print(f"Adding features to TVL data: {input_file}")

    df = load_csv(input_file)
    df = calculate_tvl_features(df)
    df = calculate_rolling_features(df)
    df = add_time_features(df)

    output_path = get_processed_data_path("llama_tvl_with_features.csv")
    save_csv(df, output_path)

    return str(output_path)


def process_all_features() -> Dict[str, str]:
    """Process all data files and add features."""
    results = {}

    # Process markets data
    try:
        markets_file = get_processed_data_path("cg_markets_latest.csv")
        if Path(markets_file).exists():
            results["markets_features"] = process_markets_features(str(markets_file))
    except Exception as e:
        print(f"Error processing markets features: {e}")

    # Process TVL data
    try:
        tvl_file = get_processed_data_path("llama_tvl_protocols_30d.csv")
        if Path(tvl_file).exists():
            results["tvl_features"] = process_tvl_features(str(tvl_file))
    except Exception as e:
        print(f"Error processing TVL features: {e}")

    # Process categories data
    try:
        categories_file = get_processed_data_path("cg_categories_snapshot.csv")
        if Path(categories_file).exists():
            df = load_csv(categories_file)
            df = add_time_features(df, "timestamp")

            output_path = get_processed_data_path("cg_categories_with_features.csv")
            save_csv(df, output_path)
            results["categories_features"] = str(output_path)
    except Exception as e:
        print(f"Error processing categories features: {e}")

    print(f"Feature engineering completed. Files created: {len(results)}")
    return results
