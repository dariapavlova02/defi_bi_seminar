"""CoinGecko API client for DeFi BI-ETL pipeline."""

import os
from typing import Any, Dict, List

from ..utils.http import get_json


class CoinGeckoClient:
    """Client for CoinGecko API with demo/pro key support."""

    def __init__(self) -> None:
        self.api_key = os.getenv("COINGECKO_API_KEY")
        self.api_tier = os.getenv("COINGECKO_API_TIER", "demo").lower()

        if self.api_tier in ["pro", "analyst"]:
            self.base_url = "https://pro-api.coingecko.com/api/v3"
            self.headers = {"x-cg-pro-api-key": self.api_key}
        else:
            self.base_url = "https://api.coingecko.com/api/v3"
            self.headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}

        print(f"CoinGecko client initialized: {self.api_tier} tier")

    def get_global(self) -> Dict[str, Any]:
        """Get global market data."""
        url = f"{self.base_url}/global"
        return get_json(url, headers=self.headers)

    def get_global_defi(self) -> Dict[str, Any]:
        """Get global DeFi market data."""
        url = f"{self.base_url}/global/decentralized_finance_defi"
        return get_json(url, headers=self.headers)

    def get_categories(self) -> List[Dict[str, Any]]:
        """Get coin categories."""
        url = f"{self.base_url}/coins/categories"
        return get_json(url, headers=self.headers)

    def get_markets(
        self, vs_currency: str = "usd", per_page: int = 200, page: int = 1
    ) -> List[Dict[str, Any]]:
        """Get market data for coins."""
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": False,
            "price_change_percentage": "1h,24h,7d",
            "locale": "en",
        }
        return get_json(url, headers=self.headers, params=params)

    def get_market_chart(
        self, token_id: str, vs_currency: str = "usd", days: int = 30
    ) -> Dict[str, Any]:
        """Get historical market data for a coin."""
        url = f"{self.base_url}/coins/{token_id}/market_chart"
        params = {"vs_currency": vs_currency, "days": days}
        return get_json(url, headers=self.headers, params=params)

    def get_coin_info(self, token_id: str) -> Dict[str, Any]:
        """Get detailed information about a coin."""
        url = f"{self.base_url}/coins/{token_id}"
        params = {
            "localization": False,
            "tickers": False,
            "market_data": True,
            "community_data": False,
            "developer_data": False,
            "sparkline": False,
        }
        return get_json(url, headers=self.headers, params=params)

    def search_coins(self, query: str) -> Dict[str, Any]:
        """Search for coins."""
        url = f"{self.base_url}/search"
        params = {"query": query}
        return get_json(url, headers=self.headers, params=params)

    def get_exchange_rates(self) -> Dict[str, Any]:
        """Get exchange rates."""
        url = f"{self.base_url}/exchange_rates"
        return get_json(url, headers=self.headers)

    def get_trending(self) -> Dict[str, Any]:
        """Get trending coins."""
        url = f"{self.base_url}/search/trending"
        return get_json(url, headers=self.headers)
