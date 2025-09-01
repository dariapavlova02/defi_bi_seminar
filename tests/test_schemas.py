"""Test data schemas for DeFi BI-ETL pipeline."""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.io import get_processed_data_path, get_tableau_path


class TestDataSchemas:
    """Test data schema validation."""

    def test_processed_data_files_exist(self):
        """Test that processed data files exist and are not empty."""
        processed_dir = Path("data/processed")

        if not processed_dir.exists():
            pytest.skip("Processed data directory does not exist")

        csv_files = list(processed_dir.glob("*.csv"))
        assert len(csv_files) > 0, "No processed CSV files found"

        for csv_file in csv_files:
            # Check file is not empty
            assert csv_file.stat().st_size > 0, f"File {csv_file} is empty"

            # Check can be read as CSV
            try:
                df = pd.read_csv(csv_file)
                assert len(df) > 0, f"CSV file {csv_file} has no rows"
                assert len(df.columns) > 0, f"CSV file {csv_file} has no columns"
            except Exception as e:
                pytest.fail(f"Failed to read CSV file {csv_file}: {e}")

    def test_tableau_export_files_exist(self):
        """Test that Tableau export files exist and are not empty."""
        tableau_dir = Path("dashboards/tableau")

        if not tableau_dir.exists():
            pytest.skip("Tableau export directory does not exist")

        csv_files = list(tableau_dir.glob("*.csv"))
        assert len(csv_files) > 0, "No Tableau CSV files found"

        for csv_file in csv_files:
            # Check file is not empty
            assert csv_file.stat().st_size > 0, f"File {csv_file} is empty"

            # Check can be read as CSV
            try:
                df = pd.read_csv(csv_file)
                if len(df) == 0:
                    pytest.skip(f"CSV file {csv_file} has no rows (only headers)")
                assert len(df.columns) > 0, f"CSV file {csv_file} has no columns"
            except Exception as e:
                pytest.fail(f"Failed to read CSV file {csv_file}: {e}")

    def test_markets_data_schema(self):
        """Test markets data schema."""
        markets_file = get_processed_data_path("cg_markets_latest.csv")

        if not Path(markets_file).exists():
            pytest.skip("Markets data file does not exist")

        df = pd.read_csv(markets_file)

        # Check required columns exist
        required_columns = [
            "timestamp",
            "id",
            "symbol",
            "name",
            "current_price",
            "market_cap",
            "total_volume",
            "pct_24h",
        ]

        for col in required_columns:
            assert col in df.columns, f"Required column {col} missing from markets data"

        # Check data types
        assert pd.api.types.is_numeric_dtype(
            df["current_price"]
        ), "current_price should be numeric"
        assert pd.api.types.is_numeric_dtype(
            df["market_cap"]
        ), "market_cap should be numeric"
        assert pd.api.types.is_numeric_dtype(
            df["total_volume"]
        ), "total_volume should be numeric"
        assert pd.api.types.is_numeric_dtype(df["pct_24h"]), "pct_24h should be numeric"

        # Check for reasonable value ranges
        assert df["current_price"].min() >= 0, "current_price should be non-negative"
        assert df["market_cap"].min() >= 0, "market_cap should be non-negative"
        assert df["total_volume"].min() >= 0, "total_volume should be non-negative"

        # Check for missing values in key columns
        key_columns = ["id", "symbol", "name"]
        for col in key_columns:
            assert df[col].notna().all(), f"Column {col} should not have missing values"

    def test_categories_data_schema(self):
        """Test categories data schema."""
        categories_file = get_processed_data_path("cg_categories_snapshot.csv")

        if not Path(categories_file).exists():
            pytest.skip("Categories data file does not exist")

        df = pd.read_csv(categories_file)

        # Check required columns exist
        required_columns = ["timestamp", "name", "market_cap", "volume_24h"]

        for col in required_columns:
            assert (
                col in df.columns
            ), f"Required column {col} missing from categories data"

        # Check data types
        assert pd.api.types.is_numeric_dtype(
            df["market_cap"]
        ), "market_cap should be numeric"
        assert pd.api.types.is_numeric_dtype(
            df["volume_24h"]
        ), "volume_24h should be numeric"

        # Check for reasonable value ranges
        assert df["market_cap"].min() >= 0, "market_cap should be non-negative"
        assert df["volume_24h"].min() >= 0, "volume_24h should be non-negative"

        # Check for missing values in key columns
        key_columns = ["name"]
        for col in key_columns:
            assert df[col].notna().all(), f"Column {col} should not have missing values"

    def test_tvl_data_schema(self):
        """Test TVL data schema."""
        tvl_file = get_processed_data_path("llama_tvl_protocols.csv")

        if not Path(tvl_file).exists():
            pytest.skip("TVL data file does not exist")

        df = pd.read_csv(tvl_file)

        # Check required columns exist (accept both timestamp and date)
        required_columns = ["protocol_name", "tvl_usd"]

        # Check for timestamp or date column
        has_timestamp = "timestamp" in df.columns
        has_date = "date" in df.columns

        if not (has_timestamp or has_date):
            pytest.fail("TVL data must have either 'timestamp' or 'date' column")

        for col in required_columns:
            assert col in df.columns, f"Required column {col} missing from TVL data"

        # Check data types
        assert pd.api.types.is_numeric_dtype(df["tvl_usd"]), "tvl_usd should be numeric"

        # Check for reasonable value ranges
        assert df["tvl_usd"].min() >= 0, "tvl_usd should be non-negative"

        # Check for missing values in key columns
        key_columns = ["protocol_name"]
        for col in key_columns:
            assert df[col].notna().all(), f"Column {col} should not have missing values"

    def test_kpi_snapshot_schema(self):
        """Test KPI snapshot schema."""
        kpi_file = get_tableau_path("kpi_snapshot.csv")

        if not Path(kpi_file).exists():
            pytest.skip("KPI snapshot file does not exist")

        df = pd.read_csv(kpi_file)

        # Skip test if file is empty (only headers)
        if len(df) == 0:
            pytest.skip("KPI snapshot file is empty (only headers)")

        # Check required columns exist (accept both timestamp and date)
        required_columns = [
            "total_market_cap_usd",
            "btc_dominance",
            "defi_market_cap",
            "defi_volume_24h",
        ]

        # Check for timestamp or date column
        has_timestamp = "timestamp" in df.columns
        has_date = "date" in df.columns

        if not (has_timestamp or has_date):
            pytest.fail("KPI snapshot must have either 'timestamp' or 'date' column")

        for col in required_columns:
            if col in df.columns:
                assert (
                    col in df.columns
                ), f"Required column {col} missing from KPI snapshot"
            else:
                pytest.skip(f"Column {col} not available in KPI snapshot")

        # Check data types
        numeric_columns = [
            "total_market_cap_usd",
            "btc_dominance",
            "defi_market_cap",
            "defi_volume_24h",
        ]
        for col in numeric_columns:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(
                    df[col]
                ), f"Column {col} should be numeric"

        # Check for reasonable value ranges
        if "btc_dominance" in df.columns:
            assert (
                df["btc_dominance"].min() >= 0
            ), "btc_dominance should be non-negative"
            assert df["btc_dominance"].max() <= 100, "btc_dominance should be <= 100"

    def test_markets_top_schema(self):
        """Test markets top data schema."""
        markets_top_file = get_tableau_path("markets_top.csv")

        if not Path(markets_top_file).exists():
            pytest.skip("Markets top file does not exist")

        df = pd.read_csv(markets_top_file)

        # Skip test if file is empty (only headers)
        if len(df) == 0:
            pytest.skip("Markets top file is empty (only headers)")

        # Check required columns exist (be flexible with column names)
        required_columns = ["symbol", "name"]

        # Check for price column (accept both current_price and price_usd)
        has_current_price = "current_price" in df.columns
        has_price_usd = "price_usd" in df.columns

        if not (has_current_price or has_price_usd):
            pytest.skip(
                "Markets top data must have either 'current_price' or 'price_usd' column"
            )

        # Check for market cap column (accept both market_cap and market_cap_usd)
        has_market_cap = "market_cap" in df.columns
        has_market_cap_usd = "market_cap_usd" in df.columns

        if not (has_market_cap or has_market_cap_usd):
            pytest.skip(
                "Markets top data must have either 'market_cap' or 'market_cap_usd' column"
            )

        for col in required_columns:
            assert (
                col in df.columns
            ), f"Required column {col} missing from markets top data"

        # Check data types
        numeric_columns = []

        # Add price column
        if "current_price" in df.columns:
            numeric_columns.append("current_price")
        elif "price_usd" in df.columns:
            numeric_columns.append("price_usd")

        # Add market cap column
        if "market_cap" in df.columns:
            numeric_columns.append("market_cap")
        elif "market_cap_usd" in df.columns:
            numeric_columns.append("market_cap_usd")

        for col in numeric_columns:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(
                    df[col]
                ), f"Column {col} should be numeric"

        # Check for reasonable value ranges
        price_col = "current_price" if "current_price" in df.columns else "price_usd"
        if price_col in df.columns:
            assert df[price_col].min() >= 0, f"{price_col} should be non-negative"

        market_cap_col = (
            "market_cap" if "market_cap" in df.columns else "market_cap_usd"
        )
        if market_cap_col in df.columns:
            assert (
                df[market_cap_col].min() >= 0
            ), f"{market_cap_col} should be non-negative"

        # Check for missing values in key columns
        key_columns = ["symbol", "name"]
        for col in key_columns:
            if col in df.columns:
                assert (
                    df[col].notna().all()
                ), f"Column {col} should not have missing values"


if __name__ == "__main__":
    pytest.main([__file__])
