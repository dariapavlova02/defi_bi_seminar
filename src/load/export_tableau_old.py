"""Export processed data to Tableau format for DeFi BI-ETL pipeline."""

from pathlib import Path
from typing import Dict

import pandas as pd

from ..utils.io import (
    get_processed_data_path,
    get_tableau_path,
    load_csv,
    safe_mkdir,
    save_csv,
)


class TableauExporter:
    """Export processed data to Tableau-compatible format."""

    def __init__(self) -> None:
        self.tableau_dir = Path("dashboards/tableau")
        safe_mkdir(self.tableau_dir)
        print(f"Tableau exporter initialized. Output directory: {self.tableau_dir}")

    def create_kpi_snapshot(self, global_file: str, defi_global_file: str) -> str:
        """Create KPI snapshot combining global and DeFi data."""
        print("Creating KPI snapshot...")

        try:
            # Load global data
            global_data = load_json(global_file)
            defi_data = load_json(defi_global_file)

            # Extract key metrics
            total_volume = (
                global_data.get("data", {}).get("total_volume", {}).get("usd", 0)
            )
            defi_volume = defi_data.get("data", {}).get("trading_volume_24h", 0)

            kpi_data = {
                "timestamp": pd.Timestamp.now().isoformat(),
                "total_market_cap_usd": global_data.get("data", {})
                .get("total_market_cap", {})
                .get("usd", 0),
                "total_volume_24h": total_volume,
                "btc_dominance": global_data.get("data", {})
                .get("market_cap_percentage", {})
                .get("btc", 0),
                "eth_dominance": global_data.get("data", {})
                .get("market_cap_percentage", {})
                .get("eth", 0),
                "defi_market_cap": defi_data.get("data", {}).get("defi_market_cap", 0),
                "defi_volume_24h": defi_volume,
                "defi_dominance": defi_data.get("data", {}).get("defi_dominance", 0),
                "active_cryptocurrencies": global_data.get("data", {}).get(
                    "active_cryptocurrencies", 0
                ),
                "active_exchanges": global_data.get("data", {}).get(
                    "active_exchanges", 0
                ),
            }

            # Convert to DataFrame
            df = pd.DataFrame([kpi_data])

            # Convert numeric columns
            numeric_columns = [
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

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Format large numbers for better Tableau compatibility
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

            # Save to Tableau directory
            output_path = get_tableau_path("kpi_snapshot.csv")
            save_csv(df, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating KPI snapshot: {e}")
            # Create empty KPI file with structure
            empty_kpi = pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp.now().isoformat(),
                        "total_market_cap_usd": 0,
                        "total_volume_24h": 0,
                        "btc_dominance": 0,
                        "eth_dominance": 0,
                        "defi_market_cap": 0,
                        "defi_volume_24h": 0,
                        "defi_dominance": 0,
                        "active_cryptocurrencies": 0,
                        "active_exchanges": 0,
                    }
                ]
            )

            output_path = get_tableau_path("kpi_snapshot.csv")
            save_csv(empty_kpi, output_path)
            return str(output_path)

    def create_markets_top(self, markets_file: str, top_n: int = 100) -> str:
        """Create top markets data for Tableau."""
        print(f"Creating top {top_n} markets data...")

        try:
            df = load_csv(markets_file)

            # Sort by market cap and take top N
            if "market_cap" in df.columns:
                df = df.sort_values("market_cap", ascending=False).head(top_n)

            # Select relevant columns for Tableau
            tableau_columns = [
                "symbol",
                "name",
                "current_price",
                "market_cap",
                "total_volume",
                "pct_1h",
                "pct_24h",
                "pct_7d",
                "market_cap_rank",
            ]

            # Filter columns that exist
            available_columns = [col for col in tableau_columns if col in df.columns]
            df_tableau = df[available_columns].copy()

            # Add market cap in billions for readability
            if "market_cap" in df_tableau.columns:
                df_tableau["market_cap_billion"] = df_tableau["market_cap"] / 1e9

            # Save to Tableau directory
            output_path = get_tableau_path("markets_top.csv")
            save_csv(df_tableau, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating markets top data: {e}")
            # Create empty file with structure
            empty_markets = pd.DataFrame(
                columns=[
                    "symbol",
                    "name",
                    "current_price",
                    "market_cap",
                    "total_volume",
                    "pct_1h",
                    "pct_24h",
                    "pct_7d",
                    "market_cap_rank",
                    "market_cap_billion",
                ]
            )

            output_path = get_tableau_path("markets_top.csv")
            save_csv(empty_markets, output_path)
            return str(output_path)

    def create_categories_snapshot(self, categories_file: str) -> str:
        """Create categories snapshot for Tableau."""
        print("Creating categories snapshot...")

        try:
            df = load_csv(categories_file)

            # Select relevant columns for Tableau
            tableau_columns = [
                "name",
                "market_cap",
                "market_cap_change_24h",
                "volume_24h",
            ]

            # Filter columns that exist
            available_columns = [col for col in tableau_columns if col in df.columns]
            df_tableau = df[available_columns].copy()

            # Add market cap in billions for readability
            if "market_cap" in df_tableau.columns:
                df_tableau["market_cap_billion"] = df_tableau["market_cap"] / 1e9

            # Calculate percentage of total market cap
            if "market_cap" in df_tableau.columns:
                total_market_cap = df_tableau["market_cap"].sum()
                df_tableau["market_cap_pct"] = (
                    df_tableau["market_cap"] / total_market_cap
                ) * 100

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
                    "market_cap_change_24h",
                    "volume_24h",
                    "market_cap_billion",
                    "market_cap_pct",
                ]
            )

            output_path = get_tableau_path("categories_snapshot.csv")
            save_csv(empty_categories, output_path)
            return str(output_path)

    def create_tvl_protocols_30d(self, tvl_file: str) -> str:
        """Create TVL protocols 30-day data for Tableau."""
        print("Creating TVL protocols 30-day data...")

        try:
            df = load_csv(tvl_file)

            # Check if this is historical data (has date column) or current data
            if "date" in df.columns:
                # Historical data - filter last 30 days
                tableau_columns = ["date", "protocol_name", "tvl_usd", "tvl_billion"]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df.columns
                ]
                df_tableau = df[available_columns].copy()

                # Ensure date column is properly formatted and filter last 30 days
                df_tableau["date"] = pd.to_datetime(df_tableau["date"])
                df_tableau = df_tableau.sort_values("date")

                # Filter last 30 days only
                from datetime import datetime, timedelta

                cutoff_date = datetime.now() - timedelta(days=30)
                df_tableau = df_tableau[df_tableau["date"] >= cutoff_date]

                print(
                    f"Historical data: Filtered to last 30 days: {len(df_tableau)} records from {df_tableau['date'].min()} to {df_tableau['date'].max()}"
                )

            else:
                # Current data - create snapshot with all protocols
                tableau_columns = [
                    "timestamp",
                    "protocol_name",
                    "protocol_slug",
                    "tvl_usd",
                    "change_1h",
                    "change_1d",
                    "change_7d",
                    "chains",
                    "category",
                ]

                # Filter columns that exist
                available_columns = [
                    col for col in tableau_columns if col in df.columns
                ]
                df_tableau = df[available_columns].copy()

                # Add TVL in billions for readability
                if "tvl_usd" in df_tableau.columns:
                    df_tableau["tvl_billion"] = (df_tableau["tvl_usd"] / 1e9).round(2)

                # Ensure we have timestamp column (rename date if needed)
                if (
                    "date" in df_tableau.columns
                    and "timestamp" not in df_tableau.columns
                ):
                    df_tableau["timestamp"] = df_tableau["date"]
                elif "timestamp" not in df_tableau.columns:
                    # If neither exists, create a default timestamp
                    df_tableau["timestamp"] = pd.Timestamp.now().strftime("%Y-%m-%d")

                print(
                    f"Current data: Created snapshot with {len(df_tableau)} protocols"
                )

            # Save to Tableau directory
            output_path = get_tableau_path("tvl_protocols_30d.csv")
            save_csv(df_tableau, output_path)

            return str(output_path)

        except Exception as e:
            print(f"Error creating TVL protocols data: {e}")
            # Create empty file with structure
            empty_tvl = pd.DataFrame(
                columns=["timestamp", "protocol_name", "tvl_usd", "tvl_billion"]
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

### KPI Section (Top)
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
            # Try to use file with all protocols first
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
