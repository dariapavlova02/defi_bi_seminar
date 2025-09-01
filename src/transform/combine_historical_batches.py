"""
Combine all available historical TVL batch files into one final file.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ..utils.io import get_processed_data_path, save_csv


def combine_historical_batches() -> str:
    """Combine all available historical TVL batch files."""
    print("Combining historical TVL batch files...")

    processed_dir = Path("data/processed")

    # Find all batch files
    batch_files = list(processed_dir.glob("llama_historical_tvl_batch_*.csv"))
    batch_files.sort(key=lambda x: int(x.stem.split("_")[-1]))

    if not batch_files:
        raise ValueError("No batch files found")

    print(f"Found {len(batch_files)} batch files")

    # Load and combine all batches
    all_data = []

    for i, batch_file in enumerate(batch_files):
        print(f"Loading batch {i+1}/{len(batch_files)}: {batch_file.name}")

        try:
            df = pd.read_csv(batch_file)
            all_data.append(df)
            print(f"  ✓ Loaded {len(df)} records")
        except Exception as e:
            print(f"  ✗ Error loading {batch_file.name}: {e}")
            continue

    if not all_data:
        raise ValueError("No data loaded from any batch files")

    # Combine all data
    print("\nCombining all batches...")
    combined_df = pd.concat(all_data, ignore_index=True)

    print(f"Total records before deduplication: {len(combined_df)}")

    # Remove duplicates
    combined_df = combined_df.drop_duplicates()
    print(f"Total records after deduplication: {len(combined_df)}")

    # Convert date column to datetime
    combined_df["date"] = pd.to_datetime(combined_df["date"])

    # Filter last 30 days
    cutoff_date = datetime.now() - timedelta(days=30)
    filtered_df = combined_df[combined_df["date"] >= cutoff_date].copy()

    print(f"Records in last 30 days: {len(filtered_df)}")

    # Sort by date and protocol
    filtered_df = filtered_df.sort_values(["date", "protocol_name"])

    # Save final result
    output_path = get_processed_data_path("llama_all_protocols_historical_30d.csv")
    save_csv(filtered_df, output_path)

    # Also save unfiltered data
    unfiltered_path = get_processed_data_path("llama_all_protocols_historical_all.csv")
    save_csv(combined_df, unfiltered_path)

    print("\nHistorical data combined and saved:")
    print(f"  - 30 days filtered: {output_path}")
    print(f"  - All historical: {unfiltered_path}")

    # Show statistics
    print("\n=== Final Statistics ===")
    print(f"Total protocols: {filtered_df['protocol_name'].nunique()}")
    print(f"Total records: {len(filtered_df)}")
    print(f"Date range: {filtered_df['date'].min()} to {filtered_df['date'].max()}")
    print(f"Data types: {filtered_df['data_type'].value_counts().to_dict()}")

    return str(output_path)


if __name__ == "__main__":
    result = combine_historical_batches()
    print(f"Combination completed: {result}")
