"""Time utilities for DeFi BI-ETL pipeline."""

from datetime import datetime, timedelta, timezone
from typing import Union


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def to_unix(dt: Union[datetime, str]) -> int:
    """Convert datetime to Unix timestamp."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp())


def from_unix(timestamp: int) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format datetime to string."""
    return dt.strftime(format_str)


def parse_datetime(date_str: str, format_str: str = "%Y-%m-%d") -> datetime:
    """Parse datetime from string."""
    return datetime.strptime(date_str, format_str)


def days_ago(days: int) -> datetime:
    """Get datetime N days ago."""
    return utc_now() - timedelta(days=days)
