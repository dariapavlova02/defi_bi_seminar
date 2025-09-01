"""
Efficiently combine historical TVL batch files with memory optimization.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ..utils.io import get_processed_data_path, save_csv


def combine_historical_batches_efficient() -> str:
    """Combine historical TVL batch files efficiently."""
    print("Combining historical TVL batch files efficiently...")

    processed_dir = Path("data/processed")

    # Find all batch files
    batch_files = list(processed_dir.glob("llama_historical_tvl_batch_*.csv"))
    batch_files.sort(key=lambda x: int(x.stem.split("_")[-1]))

    if not batch_files:
        raise ValueError("No batch files found")

    print(f"Found {len(batch_files)} batch files")

    # Process in smaller chunks to save memory
    chunk_size = 10  # Process 10 files at a time
    all_protocols = set()
    total_records = 0

    # First pass: count total records and get unique protocols
    print("Analyzing data structure...")
    for i, batch_file in enumerate(batch_files):
        print(f"Analyzing batch {i+1}/{len(batch_files)}: {batch_file.name}")

        try:
            # Read only necessary columns to save memory
            df = pd.read_csv(batch_file, usecols=["protocol_name", "date", "data_type"])
            all_protocols.update(df["protocol_name"].unique())
            total_records += len(df)
            print(f"  ✓ {len(df)} records, {df['protocol_name'].nunique()} protocols")
        except Exception as e:
            print(f"  ✗ Error analyzing {batch_file.name}: {e}")
            continue

    print("\nTotal analysis:")
    print(f"  - Total records: {total_records:,}")
    print(f"  - Unique protocols: {len(all_protocols)}")

    # Create final file by processing chunks
    output_path = get_processed_data_path("llama_all_protocols_historical_30d.csv")
    unfiltered_path = get_processed_data_path("llama_all_protocols_historical_all.csv")

    # Process in chunks
    for chunk_start in range(0, len(batch_files), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(batch_files))
        chunk_files = batch_files[chunk_start:chunk_end]

        print(
            f"\nProcessing chunk {chunk_start//chunk_size + 1}: files {chunk_start+1}-{chunk_end}"
        )

        chunk_data = []

        for batch_file in chunk_files:
            try:
                df = pd.read_csv(batch_file)
                chunk_data.append(df)
                print(f"  ✓ Loaded {batch_file.name}: {len(df)} records")
            except Exception as e:
                print(f"  ✗ Error loading {batch_file.name}: {e}")
                continue

        if chunk_data:
            # Combine chunk data
            chunk_df = pd.concat(chunk_data, ignore_index=True)

            # Convert date column to datetime
            chunk_df["date"] = pd.to_datetime(chunk_df["date"])

            # Filter last 30 days
            cutoff_date = datetime.now() - timedelta(days=30)
            chunk_filtered = chunk_df[chunk_df["date"] >= cutoff_date].copy()

            # Save chunk to final file (append mode)
            if chunk_start == 0:
                # First chunk: create new file
                save_csv(chunk_filtered, output_path)
                save_csv(chunk_df, unfiltered_path)
                print("  💾 Created new files")
            else:
                # Append to existing files
                chunk_filtered.to_csv(output_path, mode="a", header=False, index=False)
                chunk_df.to_csv(unfiltered_path, mode="a", header=False, index=False)
                print("  💾 Appended to existing files")

            # Clear memory
            del chunk_data, chunk_df, chunk_filtered

    print("\nCombination completed!")
    print(f"  - 30 days filtered: {output_path}")
    print(f"  - All historical: {unfiltered_path}")

    # Show final statistics
    print("\n=== Final Statistics ===")
    final_df = pd.read_csv(output_path)
    print(f"Total protocols: {final_df['protocol_name'].nunique()}")
    print(f"Total records (30 days): {len(final_df)}")
    print(f"Date range: {final_df['date'].min()} to {final_df['date'].max()}")
    print(f"Data types: {final_df['data_type'].value_counts().to_dict()}")

    return str(output_path)


def create_sample_30d_file() -> str:
    """Create a sample 30-day file from the largest batch for testing."""
    print("Creating sample 30-day file from largest batch...")

    processed_dir = Path("data/processed")

    # Find the largest batch file
    batch_files = list(processed_dir.glob("llama_historical_tvl_batch_*.csv"))
    if not batch_files:
        raise ValueError("No batch files found")

    # Get file sizes and find the largest
    file_sizes = [(f, f.stat().st_size) for f in batch_files]
    largest_file = max(file_sizes, key=lambda x: x[1])[0]

    print(f"Using largest batch file: {largest_file.name}")

    # Load the largest batch
    df = pd.read_csv(largest_file)
    print(f"Loaded {len(df):,} records")

    # Convert date column to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Filter last 30 days
    cutoff_date = datetime.now() - timedelta(days=30)
    filtered_df = df[df["date"] >= cutoff_date].copy()

    print(f"Records in last 30 days: {len(filtered_df):,}")

    # Sort by date and protocol
    filtered_df = filtered_df.sort_values(["date", "protocol_name"])

    # Save sample file
    output_path = get_processed_data_path("llama_sample_historical_30d.csv")
    save_csv(filtered_df, output_path)

    print(f"Sample file saved: {output_path}")
    print(f"Protocols: {filtered_df['protocol_name'].nunique()}")
    print(f"Date range: {filtered_df['date'].min()} to {filtered_df['date'].max()}")

    return str(output_path)


if __name__ == "__main__":
    # Create sample file first for testing
    print("=== Creating Sample File ===")
    sample_result = create_sample_30d_file()

    # Uncomment to run full combination
    # print("\n=== Running Full Combination ===")
    # full_result = combine_historical_batches_efficient()
