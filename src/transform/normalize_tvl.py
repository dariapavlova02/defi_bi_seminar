"""Normalize DeFiLlama TVL data for DeFi BI-ETL pipeline."""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from ..utils.io import get_processed_data_path, load_json, save_csv
from ..utils.time import from_unix, utc_now


def normalize_tvl_overview_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transform raw TVL overview data to tidy format."""
    normalized_data = []

    for item in raw_data:
        normalized_item = {
            "timestamp": utc_now().isoformat(),
            "protocol_name": item.get("name", ""),
            "protocol_slug": item.get("slug", ""),
            "tvl_usd": item.get("tvl", 0.0),
            "change_1h": item.get("change_1h", 0.0),
            "change_1d": item.get("change_1d", 0.0),
            "change_7d": item.get("change_7d", 0.0),
            "chains": ", ".join(item.get("chains", [])),
            "category": item.get("category", ""),
            "url": item.get("url", ""),
            "description": item.get("description", ""),
            "audit": item.get("audit", ""),
            "audit_note": item.get("audit_note", ""),
            "gecko_id": item.get("gecko_id", ""),
            "cmc_id": item.get("cmc_id", ""),
            "logo": item.get("logo", ""),
            "audits": item.get("audits", 0),
            "audit_links": ", ".join(item.get("audit_links", [])),
            "oracles": ", ".join(item.get("oracles", [])),
            "forks": item.get("forks", 0),
            "referral_url": item.get("referral_url", ""),
            "twitter": item.get("twitter", ""),
            "github": item.get("github", []),
        }
        normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = [
        "tvl_usd",
        "change_1h",
        "change_1d",
        "change_7d",
        "audits",
        "forks",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def normalize_protocol_tvl_data(
    raw_data: Dict[str, Any], protocol_name: str
) -> pd.DataFrame:
    """Transform raw protocol TVL data to tidy format."""
    normalized_data = []

    # Extract historical chain TVLs
    chain_tvls = raw_data.get("chainTvls", {})

    for chain, chain_data in chain_tvls.items():
        if "tvl" in chain_data and isinstance(chain_data["tvl"], list):
            for tvl_entry in chain_data["tvl"]:
                if (
                    isinstance(tvl_entry, dict)
                    and "date" in tvl_entry
                    and "totalLiquidityUSD" in tvl_entry
                ):
                    normalized_item = {
                        "date": from_unix(tvl_entry["date"]).strftime("%Y-%m-%d"),
                        "protocol_name": raw_data.get(
                            "name", protocol_name
                        ),  # Use name from data if available
                        "chain": chain,
                        "tvl_usd": float(tvl_entry["totalLiquidityUSD"]),
                        "tvl_billion": float(tvl_entry["totalLiquidityUSD"])
                        / 1_000_000_000,
                    }
                    normalized_data.append(normalized_item)

    # If no historical data, fall back to current data
    if not normalized_data:
        current_tvls = raw_data.get("currentChainTvls", {})
        for chain, tvl_usd in current_tvls.items():
            normalized_item = {
                "date": utc_now().strftime("%Y-%m-%d"),
                "protocol_name": protocol_name,
                "chain": chain,
                "tvl_usd": float(tvl_usd),
                "tvl_billion": float(tvl_usd) / 1_000_000_000,
            }
            normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = ["tvl_usd", "tvl_billion"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort by date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

    return df


def normalize_chains_tvl_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transform raw chains TVL data to tidy format."""
    normalized_data = []

    for item in raw_data:
        normalized_item = {
            "timestamp": utc_now().isoformat(),
            "chain_name": item.get("name", ""),
            "chain_id": item.get("id", ""),
            "tvl_usd": item.get("tvl", 0.0),
            "change_1h": item.get("change_1h", 0.0),
            "change_1d": item.get("change_1d", 0.0),
            "change_7d": item.get("change_7d", 0.0),
            "protocols": item.get("protocols", 0),
            "logo": item.get("logo", ""),
            "url": item.get("url", ""),
            "gecko_id": item.get("gecko_id", ""),
            "cmc_id": item.get("cmc_id", ""),
            "tokenSymbol": item.get("tokenSymbol", ""),
            "tokenAddress": item.get("tokenAddress", ""),
            "stablecoins": item.get("stablecoins", []),
        }
        normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = ["tvl_usd", "change_1h", "change_1d", "change_7d", "protocols"]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def normalize_stablecoins_data(raw_data: Dict[str, Any]) -> pd.DataFrame:
    """Transform raw stablecoins data to tidy format."""
    normalized_data = []

    for chain, data in raw_data.items():
        for stablecoin in data:
            normalized_item = {
                "timestamp": utc_now().isoformat(),
                "chain": chain,
                "name": stablecoin.get("name", ""),
                "symbol": stablecoin.get("symbol", ""),
                "price": stablecoin.get("price", 0.0),
                "circulating": stablecoin.get("circulating", 0.0),
                "circulatingPrevDay": stablecoin.get("circulatingPrevDay", 0.0),
                "circulatingPrevWeek": stablecoin.get("circulatingPrevWeek", 0.0),
                "circulatingPrevMonth": stablecoin.get("circulatingPrevMonth", 0.0),
                "mcap": stablecoin.get("mcap", 0.0),
                "mcapPrevDay": stablecoin.get("mcapPrevDay", 0.0),
                "mcapPrevWeek": stablecoin.get("mcapPrevWeek", 0.0),
                "mcapPrevMonth": stablecoin.get("mcapPrevMonth", 0.0),
            }
            normalized_data.append(normalized_item)

    df = pd.DataFrame(normalized_data)

    # Convert numeric columns
    numeric_columns = [
        "price",
        "circulating",
        "circulatingPrevDay",
        "circulatingPrevWeek",
        "circulatingPrevMonth",
        "mcap",
        "mcapPrevDay",
        "mcapPrevWeek",
        "mcapPrevMonth",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def process_tvl_overview_file(raw_file: str) -> str:
    """Process TVL overview file."""
    raw_data = load_json(raw_file)
    if isinstance(raw_data, dict):
        df = normalize_tvl_overview_data([raw_data])
    else:
        df = normalize_tvl_overview_data(raw_data)

    output_path = get_processed_data_path("llama_all_protocols.csv")
    save_csv(df, output_path)

    print(f"Processed all protocols: {len(df)} records")
    return str(output_path)


def process_protocol_tvl_files(raw_files: List[str]) -> str:
    """Process multiple protocol TVL files and combine them."""
    all_data = []

    for file_path in raw_files:
        try:
            raw_data = load_json(file_path)
            if isinstance(raw_data, dict):
                # Extract protocol name from filename and clean it
                filename = Path(file_path).stem
                protocol_name = filename.replace("protocol_tvl_", "").replace(
                    "2025-08-31_0045_", ""
                )
                df = normalize_protocol_tvl_data(raw_data, protocol_name)
                all_data.append(df)
                print(f"Processed {protocol_name}: {len(df)} historical records")
            else:
                print(f"Warning: {file_path} does not contain dict data")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    if not all_data:
        raise ValueError("No valid data found in any files")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"Total combined records: {len(combined_df)}")

    # Save combined data
    output_path = get_processed_data_path("llama_tvl_protocols_30d.csv")
    save_csv(combined_df, output_path)

    # Also save as main protocols file
    main_output_path = get_processed_data_path("llama_tvl_protocols.csv")
    save_csv(combined_df, main_output_path)

    return str(output_path)


def process_chains_tvl_file(raw_file: str) -> str:
    """Process chains TVL file."""
    raw_data = load_json(raw_file)
    if isinstance(raw_data, dict):
        df = normalize_chains_tvl_data([raw_data])
    else:
        df = normalize_chains_tvl_data(raw_data)

    output_path = get_processed_data_path("llama_tvl_chains.csv")
    save_csv(df, output_path)

    return str(output_path)


def process_stablecoins_file(raw_file: str) -> str:
    """Process stablecoins file."""
    raw_data = load_json(raw_file)
    df = normalize_stablecoins_data(raw_data)

    output_path = get_processed_data_path("llama_stablecoins.csv")
    save_csv(df, output_path)

    return str(output_path)
