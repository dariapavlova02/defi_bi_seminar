"""
Generate historical TVL data for all protocols.
Goes through all protocols and collects historical TVL data.
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from ..clients.defillama import DeFiLlamaClient
from ..utils.io import get_processed_data_path, save_csv
from ..utils.time import utc_now


def generate_historical_tvl_for_all_protocols(
    max_protocols: int | None = None,
    delay_between_requests: float = 0.1,
    save_progress: bool = True,
) -> str:
    """
    Generate historical TVL data for all protocols.

    Args:
        max_protocols: Maximum number of protocols to process (None for all)
        delay_between_requests: Delay between API requests to avoid rate limiting
        save_progress: Whether to save progress after each protocol

    Returns:
        Path to the generated CSV file
    """
    print("Starting historical TVL generation for all protocols...")

    # Initialize client
    client = DeFiLlamaClient()

    # Get all protocols
    print("Fetching list of all protocols...")
    all_protocols = client.get_protocols()

    if not all_protocols:
        raise ValueError("Failed to fetch protocols list")

    print(f"Found {len(all_protocols)} protocols")

    # Limit protocols if specified
    if max_protocols:
        all_protocols = all_protocols[:max_protocols]
        print(f"Processing first {max_protocols} protocols")

    # Prepare data collection
    all_historical_data = []
    successful_protocols = 0
    failed_protocols = 0

    # Process each protocol
    for i, protocol in enumerate(all_protocols):
        protocol_name = protocol.get("name", "Unknown")
        protocol_slug = protocol.get("slug", "")
        current_tvl = protocol.get("tvl", 0)

        # Handle None values
        if current_tvl is None:
            current_tvl = 0

        print(
            f"[{i+1}/{len(all_protocols)}] Processing {protocol_name} (slug: {protocol_slug})"
        )

        try:
            # Get historical TVL data
            historical_data = client.get_protocol_tvl(protocol_slug)

            if historical_data and isinstance(historical_data, dict):
                # Extract historical TVL from chainTvls
                chain_tvls = historical_data.get("chainTvls", {})

                for chain, chain_data in chain_tvls.items():
                    if "tvl" in chain_data and isinstance(chain_data["tvl"], list):
                        for tvl_entry in chain_data["tvl"]:
                            if (
                                isinstance(tvl_entry, dict)
                                and "date" in tvl_entry
                                and "totalLiquidityUSD" in tvl_entry
                            ):
                                historical_record = {
                                    "date": datetime.fromtimestamp(
                                        tvl_entry["date"]
                                    ).strftime("%Y-%m-%d"),
                                    "protocol_name": protocol_name,
                                    "protocol_slug": protocol_slug,
                                    "chain": chain,
                                    "tvl_usd": float(tvl_entry["totalLiquidityUSD"]),
                                    "tvl_billion": float(tvl_entry["totalLiquidityUSD"])
                                    / 1_000_000_000,
                                    "data_type": "historical",
                                }
                                all_historical_data.append(historical_record)

                # If no historical data, create current snapshot
                if not any(
                    record["protocol_name"] == protocol_name
                    for record in all_historical_data
                ):
                    current_record = {
                        "date": utc_now().strftime("%Y-%m-%d"),
                        "protocol_name": protocol_name,
                        "protocol_slug": protocol_slug,
                        "chain": "current",
                        "tvl_usd": float(current_tvl),
                        "tvl_billion": float(current_tvl) / 1_000_000_000,
                        "data_type": "current",
                    }
                    all_historical_data.append(current_record)

                successful_protocols += 1
                print(
                    f"  ✓ Success: {len([r for r in all_historical_data if r['protocol_name'] == protocol_name])} records"
                )

            else:
                # Create current snapshot if no historical data
                current_record = {
                    "date": utc_now().strftime("%Y-%m-%d"),
                    "protocol_name": protocol_name,
                    "protocol_slug": protocol_slug,
                    "chain": "current",
                    "tvl_usd": float(current_tvl),
                    "tvl_billion": float(current_tvl) / 1_000_000_000,
                    "data_type": "current",
                }
                all_historical_data.append(current_record)
                successful_protocols += 1
                print("  ✓ Current snapshot only: 1 record")

        except Exception as e:
            failed_protocols += 1
            print(f"  ✗ Error: {e}")

            # Still create current snapshot
            current_record = {
                "date": utc_now().strftime("%Y-%m-%d"),
                "protocol_name": protocol_name,
                "protocol_slug": protocol_slug,
                "chain": "current",
                "tvl_usd": float(current_tvl),
                "tvl_billion": float(current_tvl) / 1_000_000_000,
                "data_type": "current",
            }
            all_historical_data.append(current_record)

        # Save progress periodically
        if save_progress and (i + 1) % 100 == 0:
            progress_df = pd.DataFrame(all_historical_data)
            progress_path = get_processed_data_path(
                f"llama_historical_tvl_progress_{i+1}.csv"
            )
            save_csv(progress_df, progress_path)
            print(f"  💾 Progress saved: {progress_path}")

        # Delay between requests to avoid rate limiting
        if delay_between_requests > 0:
            time.sleep(delay_between_requests)

    # Create final DataFrame
    print("\nProcessing completed!")
    print(f"Successful protocols: {successful_protocols}")
    print(f"Failed protocols: {failed_protocols}")
    print(f"Total records: {len(all_historical_data)}")

    df = pd.DataFrame(all_historical_data)

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Filter last 30 days
    cutoff_date = datetime.now() - timedelta(days=30)
    df_filtered = df[df["date"] >= cutoff_date].copy()

    print(f"Records in last 30 days: {len(df_filtered)}")

    # Sort by date and protocol
    df_filtered = df_filtered.sort_values(["date", "protocol_name"])

    # Save final result
    output_path = get_processed_data_path("llama_all_protocols_historical_30d.csv")
    save_csv(df_filtered, output_path)

    # Also save unfiltered data
    unfiltered_path = get_processed_data_path("llama_all_protocols_historical_all.csv")
    save_csv(df, unfiltered_path)

    print("Historical data saved:")
    print(f"  - 30 days filtered: {output_path}")
    print(f"  - All historical: {unfiltered_path}")

    return str(output_path)


def generate_historical_tvl_batch(
    batch_size: int = 100, delay_between_batches: float = 1.0
) -> str:
    """
    Generate historical TVL data in batches to avoid overwhelming the API.

    Args:
        batch_size: Number of protocols to process in each batch
        delay_between_batches: Delay between batches

    Returns:
        Path to the generated CSV file
    """
    print(f"Starting batch processing with batch size {batch_size}")

    # Initialize client
    client = DeFiLlamaClient()

    # Get all protocols
    all_protocols = client.get_protocols()

    if not all_protocols:
        raise ValueError("Failed to fetch protocols list")

    print(f"Total protocols to process: {len(all_protocols)}")

    all_historical_data = []

    # Process in batches
    for batch_start in range(0, len(all_protocols), batch_size):
        batch_end = min(batch_start + batch_size, len(all_protocols))
        batch_protocols = all_protocols[batch_start:batch_end]

        print(
            f"\nProcessing batch {batch_start//batch_size + 1}: protocols {batch_start+1}-{batch_end}"
        )

        batch_data = process_protocol_batch(batch_protocols, client)
        all_historical_data.extend(batch_data)

        print(f"Batch completed: {len(batch_data)} records")

        # Save batch progress
        batch_df = pd.DataFrame(all_historical_data)
        batch_path = get_processed_data_path(
            f"llama_historical_tvl_batch_{batch_end}.csv"
        )
        save_csv(batch_df, batch_path)
        print(f"Batch progress saved: {batch_path}")

        # Delay between batches
        if batch_end < len(all_protocols) and delay_between_batches > 0:
            print(f"Waiting {delay_between_batches}s before next batch...")
            time.sleep(delay_between_batches)

    # Create final DataFrame
    df = pd.DataFrame(all_historical_data)

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Filter last 30 days
    cutoff_date = datetime.now() - timedelta(days=30)
    df_filtered = df[df["date"] >= cutoff_date].copy()

    print("\nFinal results:")
    print(f"Total records: {len(df)}")
    print(f"Records in last 30 days: {len(df_filtered)}")

    # Sort by date and protocol
    df_filtered = df_filtered.sort_values(["date", "protocol_name"])

    # Save final result
    output_path = get_processed_data_path("llama_all_protocols_historical_30d.csv")
    save_csv(df_filtered, output_path)

    return str(output_path)


def process_protocol_batch(
    protocols: List[Dict[str, Any]], client: DeFiLlamaClient
) -> List[Dict[str, Any]]:
    """Process a batch of protocols."""
    batch_data = []

    for protocol in protocols:
        protocol_name = protocol.get("name", "Unknown")
        protocol_slug = protocol.get("slug", "")
        current_tvl = protocol.get("tvl", 0)

        # Handle None values
        if current_tvl is None:
            current_tvl = 0

        try:
            # Get historical TVL data
            historical_data = client.get_protocol_tvl(protocol_slug)

            if historical_data and isinstance(historical_data, dict):
                # Extract historical TVL from chainTvls
                chain_tvls = historical_data.get("chainTvls", {})

                for chain, chain_data in chain_tvls.items():
                    if "tvl" in chain_data and isinstance(chain_data["tvl"], list):
                        for tvl_entry in chain_data["tvl"]:
                            if (
                                isinstance(tvl_entry, dict)
                                and "date" in tvl_entry
                                and "totalLiquidityUSD" in tvl_entry
                            ):
                                historical_record = {
                                    "date": datetime.fromtimestamp(
                                        tvl_entry["date"]
                                    ).strftime("%Y-%m-%d"),
                                    "protocol_name": protocol_name,
                                    "protocol_slug": protocol_slug,
                                    "chain": chain,
                                    "tvl_usd": float(tvl_entry["totalLiquidityUSD"]),
                                    "tvl_billion": float(tvl_entry["totalLiquidityUSD"])
                                    / 1_000_000_000,
                                    "data_type": "historical",
                                }
                                batch_data.append(historical_record)

                # If no historical data, create current snapshot
                if not any(
                    record["protocol_name"] == protocol_name for record in batch_data
                ):
                    current_record = {
                        "date": utc_now().strftime("%Y-%m-%d"),
                        "protocol_name": protocol_name,
                        "protocol_slug": protocol_slug,
                        "chain": "current",
                        "tvl_usd": float(current_tvl),
                        "tvl_billion": float(current_tvl) / 1_000_000_000,
                        "data_type": "current",
                    }
                    batch_data.append(current_record)

            else:
                # Create current snapshot if no historical data
                current_record = {
                    "date": utc_now().strftime("%Y-%m-%d"),
                    "protocol_name": protocol_name,
                    "protocol_slug": protocol_slug,
                    "chain": "current",
                    "tvl_usd": float(current_tvl),
                    "tvl_billion": float(current_tvl) / 1_000_000_000,
                    "data_type": "current",
                }
                batch_data.append(current_record)

        except Exception:
            # Still create current snapshot
            current_record = {
                "date": utc_now().strftime("%Y-%m-%d"),
                "protocol_name": protocol_name,
                "protocol_slug": protocol_slug,
                "chain": "current",
                "tvl_usd": float(current_tvl),
                "tvl_billion": float(current_tvl) / 1_000_000_000,
                "data_type": "current",
            }
            batch_data.append(current_record)

    return batch_data


if __name__ == "__main__":
    # Test with a small number first
    print("Testing with first 10 protocols...")
    result = generate_historical_tvl_for_all_protocols(max_protocols=10)
    print(f"Test completed: {result}")

    # Uncomment to process all protocols
    # print("\nProcessing all protocols...")
    # result = generate_historical_tvl_batch(batch_size=50)
    # print(f"All protocols completed: {result}")
