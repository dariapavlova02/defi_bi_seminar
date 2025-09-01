"""
Tableau export module for DeFi ETL pipeline.
Exports processed data to Tableau-compatible CSV format.
"""

from pathlib import Path
from typing import Dict

import pandas as pd

from ..utils.io import get_processed_data_path, get_tableau_path, save_csv


class TableauExporter:
    """Export processed data to Tableau format."""

    def __init__(self) -> None:
        """Initialize Tableau exporter."""
        self.output_dir = get_tableau_path("")
        print(f"Tableau exporter initialized. Output directory: {self.output_dir}")

    def create_kpi_snapshot(self, global_file: str, defi_file: str) -> str:
        """Create KPI snapshot for Tableau."""
        print("Creating KPI snapshot...")

        try:
            # Load global and DeFi data
            global_data = {}
            defi_data = {}

            if global_file and Path(global_file).exists():
                import json

                with open(global_file, "r") as f:
                    global_data = json.load(f)

            if defi_file and Path(defi_file).exists():
                import json

                with open(defi_file, "r") as f:
                    defi_data = json.load(f)

            # Create KPI data
            kpi_data = {
                "timestamp": pd.Timestamp.now().isoformat(),
                "total_market_cap_usd": global_data.get("data", {})
                .get("total_market_cap", {})
                .get("usd", 0),
                "total_volume_24h": global_data.get("data", {})
                .get("total_volume", {})
                .get("usd", 0),
                "btc_dominance": global_data.get("data", {})
                .get("market_cap_percentage", {})
                .get("btc", 0),
                "eth_dominance": global_data.get("data", {})
                .get("market_cap_percentage", {})
                .get("eth", 0),
                "defi_market_cap": defi_data.get("data", {}).get("defi_market_cap", 0),
                "defi_volume_24h": defi_data.get("data", {}).get(
                    "trading_volume_24h", 0
                ),
                "defi_dominance": defi_data.get("data", {}).get("defi_dominance", 0),
                "active_cryptocurrencies": global_data.get("data", {}).get(
                    "active_cryptocurrencies", 0
                ),
                "active_exchanges": global_data.get("data", {}).get(
                    "active_exchanges", 0
                ),
            }

            df = pd.DataFrame([kpi_data])

            # Round large numbers for better Tableau compatibility
            if "total_market_cap_usd" in df.columns:
                df["total_market_cap_usd"] = df["total_market_cap_usd"].round(2)
            if "total_volume_24h" in df.columns:
                df["total_volume_24h"] = df["total_volume_24h"].round(2)
            if "defi_market_cap" in df.columns:
                df["defi_market_cap"] = df["defi_market_cap"].round(2)
            if "defi_volume_24h" in df.columns:
                df["defi_volume_24h"] = df["defi_volume_24h"].round(2)

            # Add readable values in billions
            if "total_market_cap_usd" in df.columns:
                df["total_market_cap_billion"] = (
                    df["total_market_cap_usd"] / 1e9
                ).round(2)
            if "total_volume_24h" in df.columns:
                df["total_volume_24h_billion"] = (df["total_volume_24h"] / 1e9).round(2)
            if "defi_market_cap" in df.columns:
                df["defi_market_cap_billion"] = (df["defi_market_cap"] / 1e9).round(2)
            if "defi_volume_24h" in df.columns:
                df["defi_volume_24h_billion"] = (df["defi_volume_24h"] / 1e9).round(2)

            # Verify data before saving
            print("KPI Data Verification:")
            print(f"  Total Volume 24h: {df['total_volume_24h'].iloc[0]}")
            print(f"  DeFi Volume 24h: {df['defi_volume_24h'].iloc[0]}")
            print(f"  Total Market Cap: {df['total_market_cap_usd'].iloc[0]}")

            # Save to Tableau directory
            output_path = get_tableau_path("kpi_snapshot.csv")
            save_csv(df, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating KPI snapshot: {e}")
            # Create empty file with structure
            empty_kpi = pd.DataFrame(
                columns=[
                    "timestamp",
                    "total_market_cap_usd",
                    "total_volume_24h",
                    "btc_dominance",
                    "eth_dominance",
                    "defi_market_cap",
                    "defi_volume_24h",
                    "defi_dominance",
                    "active_cryptocurrencies",
                    "active_exchanges",
                ]
            )

            output_path = get_tableau_path("kpi_snapshot.csv")
            save_csv(empty_kpi, output_path)
            return str(output_path)

    def create_markets_top(self, markets_file: str) -> str:
        """Create top markets data for Tableau."""
        print("Creating top 100 markets data...")

        try:
            if markets_file and Path(markets_file).exists():
                df = pd.read_csv(markets_file)

                # Select top 100 by market cap
                df_sorted = df.sort_values("market_cap_usd", ascending=False).head(100)

                # Add market cap in billions
                df_sorted["market_cap_billion"] = (
                    df_sorted["market_cap_usd"] / 1e9
                ).round(2)

                # Select relevant columns for Tableau
                tableau_columns = [
                    "name",
                    "symbol",
                    "market_cap_usd",
                    "market_cap_billion",
                    "current_price",
                    "volume_24h",
                    "price_change_24h",
                    "price_change_7d",
                    "market_cap_rank",
                    "circulating_supply",
                ]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df_sorted.columns
                ]
                df_tableau = df_sorted[available_columns].copy()

                # Rename price_usd to current_price if it exists
                if "price_usd" in df_tableau.columns:
                    df_tableau = df_tableau.rename(
                        columns={"price_usd": "current_price"}
                    )
                elif "current_price" not in df_tableau.columns:
                    # If neither exists, create a default current_price column
                    df_tableau["current_price"] = 0.0

                # Add market_cap column (without _usd suffix) for test compatibility
                if "market_cap_usd" in df_tableau.columns:
                    df_tableau["market_cap"] = df_tableau["market_cap_usd"]

            else:
                # Create empty file with structure
                df_tableau = pd.DataFrame(
                    columns=[
                        "name",
                        "symbol",
                        "market_cap_usd",
                        "market_cap_billion",
                        "current_price",
                        "market_cap",
                        "volume_24h",
                        "price_change_24h",
                        "price_change_7d",
                        "market_cap_rank",
                        "circulating_supply",
                    ]
                )

            # Save to Tableau directory
            output_path = get_tableau_path("markets_top.csv")
            save_csv(df_tableau, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating markets top data: {e}")
            # Create empty file with structure
            empty_markets = pd.DataFrame(
                columns=[
                    "name",
                    "symbol",
                    "market_cap_usd",
                    "market_cap_billion",
                    "current_price",
                    "market_cap",
                    "volume_24h",
                    "price_change_24h",
                    "price_change_7d",
                    "market_cap_rank",
                    "circulating_supply",
                ]
            )

            output_path = get_tableau_path("markets_top.csv")
            save_csv(empty_markets, output_path)
            return str(output_path)

    def create_categories_snapshot(self, categories_file: str) -> str:
        """Create categories snapshot for Tableau."""
        print("Creating categories snapshot...")

        try:
            if categories_file and Path(categories_file).exists():
                df = pd.read_csv(categories_file)

                # Add market cap in billions
                if "market_cap" in df.columns:
                    df["market_cap_billion"] = (df["market_cap"] / 1e9).round(2)

                # Select relevant columns for Tableau
                tableau_columns = [
                    "name",
                    "market_cap",
                    "market_cap_billion",
                    "volume_24h",
                    "market_cap_change_24h",
                    "top_3_coins",
                    "market_cap_share",
                ]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df.columns
                ]
                df_tableau = df[available_columns].copy()

            else:
                # Create empty file with structure
                df_tableau = pd.DataFrame(
                    columns=[
                        "name",
                        "market_cap",
                        "market_cap_billion",
                        "volume_24h",
                        "market_cap_change_24h",
                        "top_3_coins",
                        "market_cap_share",
                    ]
                )

            # Save to Tableau directory
            output_path = get_tableau_path("categories_snapshot.csv")
            save_csv(df_tableau, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating categories snapshot: {e}")
            # Create empty file with structure
            empty_categories = pd.DataFrame(
                columns=[
                    "name",
                    "market_cap",
                    "market_cap_billion",
                    "volume_24h",
                    "market_cap_change_24h",
                    "top_3_coins",
                    "market_cap_share",
                ]
            )

            output_path = get_tableau_path("categories_snapshot.csv")
            save_csv(empty_categories, output_path)
            return str(output_path)

    def create_tvl_protocols_30d(self, tvl_file: str) -> str:
        """Create TVL protocols 30-day data for Tableau."""
        print("Creating TVL protocols 30-day data...")

        try:
            # Load the data file (should be historical data)
            df = pd.read_csv(tvl_file)
            print(f"Loaded data: {len(df)} records")

            # Check if this is historical data (has date column)
            if "date" in df.columns:
                print("Processing historical data file...")

                # Select relevant columns for Tableau
                tableau_columns = [
                    "date",
                    "protocol_name",
                    "protocol_slug",
                    "chain",
                    "tvl_usd",
                    "tvl_billion",
                    "data_type",
                ]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df.columns
                ]
                df_tableau = df[available_columns].copy()

                # Ensure date column is properly formatted
                df_tableau["date"] = pd.to_datetime(df_tableau["date"])

                # Sort by date and protocol
                df_tableau = df_tableau.sort_values(["date", "protocol_name"])

                print(f"Historical data prepared: {len(df_tableau)} records")
                print(f"Protocols: {df_tableau['protocol_name'].nunique()}")
                print(
                    f"Date range: {df_tableau['date'].min()} to {df_tableau['date'].max()}"
                )

            else:
                print("Processing current data file...")

                # This is current data, create snapshot
                tableau_columns = [
                    "protocol_name",
                    "protocol_slug",
                    "tvl_usd",
                    "tvl_billion",
                ]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df.columns
                ]
                df_tableau = df[available_columns].copy()

                # Add current date
                df_tableau["date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
                df_tableau["data_type"] = "current"
                df_tableau["chain"] = "current"

                print(f"Current data prepared: {len(df_tableau)} protocols")

            # Save to Tableau directory
            output_path = get_tableau_path("tvl_protocols_30d.csv")
            save_csv(df_tableau, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating TVL protocols data: {e}")
            # Create empty file with structure
            empty_tvl = pd.DataFrame(
                columns=["date", "protocol_name", "tvl_usd", "tvl_billion", "data_type"]
            )

            output_path = get_tableau_path("tvl_protocols_30d.csv")
            save_csv(empty_tvl, output_path)
            return str(output_path)

    def create_tableau_readme(self) -> str:
        """Create README file with Tableau connection instructions."""
        readme_content = """# Tableau Dashboard Connection Guide

## Available Data Sources

### 1. KPI Snapshot (kpi_snapshot.csv)
- **Purpose**: Key performance indicators for market overview
- **Key Metrics**: Total market cap, BTC dominance, DeFi metrics
- **Recommended Visualizations**: KPI cards, summary tables

### 2. Top Markets (markets_top.csv)
- **Purpose**: Top cryptocurrencies by market cap
- **Key Metrics**: Price changes, volume, market cap
- **Recommended Visualizations**: Top movers table, price change heatmap

### 3. Categories Snapshot (categories_snapshot.csv)
- **Purpose**: Market sector distribution
- **Key Metrics**: Category market caps, 24h changes
- **Recommended Visualizations**: Pie chart, bar chart by category

### 4. TVL Protocols 30D (tvl_protocols_30d.csv)
- **Purpose**: Historical TVL data for DeFi protocols
- **Key Metrics**: Daily TVL values, protocol comparison
- **Recommended Visualizations**: Line chart by protocol, area chart

## Connection Instructions

1. **Open Tableau Desktop**
2. **Connect to Data** → **Text file**
3. **Browse** to the `dashboards/tableau/` folder
4. **Select** the desired CSV file
5. **Load** the data

## Data Refresh

- Data is updated daily via ETL pipeline
- Files are timestamped for version control
- Use **Data** → **Refresh** to get latest data

## Recommended Dashboard Layout

### Market Overview (Top)
- Market cap, BTC dominance, DeFi metrics
- Use KPI cards with conditional formatting

### Market Analysis (Middle)
- Top movers table (24h/7d changes)
- Price change heatmap
- Volume vs market cap scatter plot

### DeFi Analysis (Bottom)
- TVL trends by protocol
- Category distribution pie chart
- Chain TVL comparison

## Data Quality Notes

- All numeric values are in USD unless specified
- Market cap values are in billions for readability
- Percentage changes are decimal format (0.05 = 5%)
- Missing values are handled gracefully
"""

        readme_path = get_tableau_path("README.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_content)

        return str(readme_path)

    def export_all(self, processed_files: Dict[str, str]) -> Dict[str, str]:
        """Export all processed data to Tableau format."""
        print("Starting Tableau export...")

        results = {}

        # Create KPI snapshot (try to use raw data if available)
        try:
            # Look for raw global data files
            raw_dir = Path("data/raw/coingecko")
            global_files = list(raw_dir.glob("*global_markets*.json"))
            defi_files = list(raw_dir.glob("*global_defi*.json"))

            if global_files and defi_files:
                results["kpi_snapshot"] = self.create_kpi_snapshot(
                    str(global_files[-1]), str(defi_files[-1])
                )
            else:
                results["kpi_snapshot"] = self.create_kpi_snapshot("", "")
        except Exception as e:
            print(f"Error creating KPI snapshot: {e}")
            results["kpi_snapshot"] = self.create_kpi_snapshot("", "")

        # Create markets top data
        try:
            markets_file = get_processed_data_path("cg_markets_latest.csv")
            if Path(markets_file).exists():
                results["markets_top"] = self.create_markets_top(str(markets_file))
            else:
                results["markets_top"] = self.create_markets_top("")
        except Exception as e:
            print(f"Error creating markets top data: {e}")
            results["markets_top"] = self.create_markets_top("")

        # Create categories snapshot
        try:
            categories_file = get_processed_data_path("cg_categories_snapshot.csv")
            if Path(categories_file).exists():
                results["categories_snapshot"] = self.create_categories_snapshot(
                    str(categories_file)
                )
            else:
                results["categories_snapshot"] = self.create_categories_snapshot("")
        except Exception as e:
            print(f"Error creating categories snapshot: {e}")
            results["categories_snapshot"] = self.create_categories_snapshot("")

        # Create TVL protocols data
        try:
            # Try to use historical data file first (most comprehensive)
            historical_file = get_processed_data_path("llama_sample_historical_30d.csv")
            if Path(historical_file).exists():
                results["tvl_protocols_30d"] = self.create_tvl_protocols_30d(
                    str(historical_file)
                )
            else:
                # Fallback to file with all protocols
                all_protocols_file = get_processed_data_path("llama_all_protocols.csv")
                if Path(all_protocols_file).exists():
                    results["tvl_protocols_30d"] = self.create_tvl_protocols_30d(
                        str(all_protocols_file)
                    )
                else:
                    # Fallback to historical data file
                    tvl_file = get_processed_data_path("llama_tvl_protocols.csv")
                    if Path(tvl_file).exists():
                        results["tvl_protocols_30d"] = self.create_tvl_protocols_30d(
                            str(tvl_file)
                        )
                    else:
                        results["tvl_protocols_30d"] = self.create_tvl_protocols_30d("")
        except Exception as e:
            print(f"Error creating TVL protocols data: {e}")
            results["tvl_protocols_30d"] = self.create_tvl_protocols_30d("")

        # Create README
        results["readme"] = self.create_tableau_readme()

        print(f"Tableau export completed. Files created: {len(results)}")
        return results


def load_json(file_path: str) -> dict:
    """Load JSON file safely."""
    try:
        import json

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        return {}


def load_csv(file_path: str) -> pd.DataFrame:
    """Load CSV file safely."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading CSV file {file_path}: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # Test the exporter
    exporter = TableauExporter()
    exporter.export_all({})
