"""IO utilities for DeFi BI-ETL pipeline."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Union

import pandas as pd


def safe_mkdir(path: Union[str, Path]) -> Path:
    """Safely create directory and all parent directories."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_timestamped_filename(name: str, extension: str = "json") -> str:
    """Generate timestamped filename."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M")
    return f"{timestamp}_{name}.{extension}"


def save_json(data: Any, path: Union[str, Path]) -> Path:
    """Save data as JSON file, creating parent directories if needed."""
    path = Path(path)
    safe_mkdir(path.parent)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Saved JSON: {path}")
    return path


def save_csv(df: pd.DataFrame, path: Union[str, Path], **kwargs) -> Path:
    """Save DataFrame as CSV, creating parent directories if needed."""
    path = Path(path)
    safe_mkdir(path.parent)

    # Ensure proper CSV formatting for Tableau compatibility
    csv_kwargs = {
        "index": False,
        "float_format": "%.2f",  # Avoid scientific notation
        "encoding": "utf-8",
        **kwargs,
    }

    df.to_csv(path, **csv_kwargs)
    print(f"Saved CSV: {path}")
    return path


def load_json(path: Union[str, Path]) -> Any:
    """Load data from JSON file."""
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Union[str, Path]) -> pd.DataFrame:
    """Load data from CSV file."""
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    return pd.read_csv(path)


def get_raw_data_path(source: str, filename: str) -> Path:
    """Get path for raw data file."""
    return Path(f"data/raw/{source}/{filename}")


def get_processed_data_path(filename: str) -> Path:
    """Get path for processed data file."""
    return Path(f"data/processed/{filename}")


def get_tableau_path(filename: str) -> Path:
    """Get path for Tableau export file."""
    return Path(f"dashboards/tableau/{filename}")
