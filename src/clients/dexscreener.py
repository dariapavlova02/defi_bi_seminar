"""DexScreener API client for DeFi BI-ETL pipeline."""

from typing import Any

from ..utils.http import get_json


class DexScreenerClient:
    """Client for DexScreener API (no key required)."""

    def __init__(self) -> None:
        self.base_url = "https://api.dexscreener.com"
        print("DexScreener client initialized")

    def get_token_pairs(self, token_address: str) -> Any:
        """Get all pairs for a specific token."""
        url = f"{self.base_url}/latest/dex/tokens/{token_address}"
        return get_json(url)

    def get_pairs_by_chain(self, chain: str, limit: int = 100) -> Any:
        """Get pairs for a specific chain."""
        url = f"{self.base_url}/latest/dex/search"
        params = {"q": chain, "limit": limit}
        return get_json(url, params=params)

    def search_pairs(self, query: str) -> Any:
        """Search for pairs."""
        url = f"{self.base_url}/latest/dex/search"
        params = {"q": query}
        return get_json(url, params=params)

    def get_trending_pairs(self) -> Any:
        """Get trending pairs."""
        url = f"{self.base_url}/latest/dex/tokens/trending"
        return get_json(url)

    def get_chain_stats(self, chain: str) -> Any:
        """Get statistics for a specific chain."""
        url = f"{self.base_url}/latest/dex/chains/{chain}"
        return get_json(url)

    def get_dex_stats(self, dex: str) -> Any:
        """Get statistics for a specific DEX."""
        url = f"{self.base_url}/latest/dex/dexes/{dex}"
        return get_json(url)

    def get_pair_by_address(self, pair_address: str) -> Any:
        """Get specific pair by address."""
        url = f"{self.base_url}/latest/dex/pairs/{pair_address}"
        return get_json(url)

    def get_token_info(self, token_address: str) -> Any:
        """Get token information."""
        url = f"{self.base_url}/latest/dex/tokens/{token_address}"
        return get_json(url)

    def get_chain_pairs_sorted(
        self, chain: str, sort_by: str = "volume24h", limit: int = 100
    ) -> Any:
        """Get chain pairs sorted by specific criteria."""
        url = f"{self.base_url}/latest/dex/pairs/{chain}"
        params = {"limit": limit, "sort": sort_by}
        return get_json(url, params=params)
