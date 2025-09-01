"""Normalize CoinGecko markets data for DeFi BI-ETL pipeline."""

from typing import Any, Dict, List

import pandas as pd

from ..utils.io import get_processed_data_path, load_json, save_csv
from ..utils.time import utc_now


def normalize_markets_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transform raw markets data to tidy format."""
    normalized_data = []

    for item in raw_data:
        normalized_item = {
            "timestamp": utc_now().isoformat(),
            "id": item.get("id", ""),
            "symbol": item.get("symbol", "").upper(),
            "name": item.get("name", ""),
            "current_price": item.get("current_price", 0.0),
            "market_cap": item.get("market_cap", 0.0),
            "total_volume": item.get("total_volume", 0.0),
            "pct_1h": item.get("price_change_percentage_1h_in_currency", 0.0),
            "pct_24h": item.get("price_change_percentage_24h_in_currency", 0.0),
            "pct_7d": item.get("price_change_percentage_7d_in_currency", 0.0),
            "last_updated": item.get("last_updated", ""),
            "market_cap_rank": item.get("market_cap_rank", 0),
            "circulating_supply": item.get("circulating_supply", 0.0),
            "total_supply": item.get("total_supply", 0.0),
            "max_supply": item.get("max_supply", 0.0),
            "ath": item.get("ath", 0.0),
            "ath_change_percentage": item.get("ath_change_percentage", 0.0),
            "atl": item.get("atl", 0.0),
            "atl_change_percentage": item.get("atl_change_percentage", 0.0),
        }
        normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = [
        "current_price",
        "market_cap",
        "total_volume",
        "pct_1h",
        "pct_24h",
        "pct_7d",
        "market_cap_rank",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "ath_change_percentage",
        "atl",
        "atl_change_percentage",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def normalize_categories_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transform raw categories data to tidy format."""
    normalized_data = []

    for item in raw_data:
        normalized_item = {
            "timestamp": utc_now().isoformat(),
            "id": item.get("id", ""),
            "name": item.get("name", ""),
            "market_cap": item.get("market_cap", 0.0),
            "market_cap_change_24h": item.get("market_cap_change_24h", 0.0),
            "content": item.get("content", ""),
            "top_3_coins": ", ".join(item.get("top_3_coins", [])),
            "volume_24h": item.get("volume_24h", 0.0),
            "updated_at": item.get("updated_at", ""),
        }
        normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = ["market_cap", "market_cap_change_24h", "volume_24h"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def normalize_token_history_data(
    raw_data: Dict[str, Any], token_id: str
) -> pd.DataFrame:
    """Transform raw token history data to tidy format."""
    prices = raw_data.get("prices", [])
    market_caps = raw_data.get("market_caps", [])
    volumes = raw_data.get("total_volumes", [])

    normalized_data = []

    for i, (timestamp, price) in enumerate(prices):
        item = {
            "timestamp": pd.to_datetime(timestamp, unit="ms"),
            "token_id": token_id,
            "price": price,
            "market_cap": market_caps[i][1] if i < len(market_caps) else 0.0,
            "volume": volumes[i][1] if i < len(volumes) else 0.0,
        }
        normalized_data.append(item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = ["price", "market_cap", "volume"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def process_markets_files(raw_files: List[str]) -> str:
    """Process multiple markets files and combine them."""
    all_data: List[Dict[str, Any]] = []

    for file_path in raw_files:
        try:
            raw_data = load_json(file_path)
            if isinstance(raw_data, list):
                all_data.extend(raw_data)
            else:
                print(f"Warning: {file_path} does not contain list data")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    if not all_data:
        raise ValueError("No valid data found in any files")

    # Remove duplicates based on id
    df = normalize_markets_data(all_data)
    df = df.drop_duplicates(subset=["id"], keep="last")

    # Save combined data
    output_path = get_processed_data_path("cg_markets_latest.csv")
    save_csv(df, output_path)

    return str(output_path)


def process_categories_file(raw_file: str) -> str:
    """Process categories file."""
    raw_data = load_json(raw_file)
    if isinstance(raw_data, dict):
        df = normalize_categories_data([raw_data])
    else:
        df = normalize_categories_data(raw_data)

    output_path = get_processed_data_path("cg_categories_snapshot.csv")
    save_csv(df, output_path)

    return str(output_path)


def process_token_history_file(raw_file: str, token_id: str) -> str:
    """Process token history file."""
    raw_data = load_json(raw_file)
    df = normalize_token_history_data(raw_data, token_id)

    output_path = get_processed_data_path(f"cg_token_history_{token_id}_30d.csv")
    save_csv(df, output_path)

    return str(output_path)
