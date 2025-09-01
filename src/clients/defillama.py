"""DeFiLlama API client for DeFi BI-ETL pipeline."""

from typing import Any

from ..utils.http import get_json


class DeFiLlamaClient:
    """Client for DeFiLlama API (no key required)."""

    def __init__(self) -> None:
        self.base_url = "https://api.llama.fi"
        print("DeFiLlama client initialized")

    def get_tvl_overview(self) -> Any:
        """Get TVL overview for all protocols."""
        url = f"{self.base_url}/protocols"
        return get_json(url)

    def get_protocols(self) -> Any:
        """Get list of all protocols."""
        url = f"{self.base_url}/protocols"
        return get_json(url)

    def get_protocol_tvl(self, name_or_slug: str) -> Any:
        """Get historical TVL data for a specific protocol."""
        url = f"{self.base_url}/protocol/{name_or_slug}"
        return get_json(url)

    def get_chains_tvl(self) -> Any:
        """Get TVL data for all chains."""
        url = f"{self.base_url}/v2/chains"
        return get_json(url)

    def get_stablecoins(self) -> Any:
        """Get stablecoin data."""
        url = f"{self.base_url}/stablecoins"
        return get_json(url)

    def get_bridges(self) -> Any:
        """Get bridge data."""
        url = f"{self.base_url}/bridges"
        return get_json(url)

    def get_yields(self) -> Any:
        """Get yield farming data."""
        url = f"{self.base_url}/yields"
        return get_json(url)

    def get_fees(self) -> Any:
        """Get protocol fee data."""
        url = f"{self.base_url}/fees"
        return get_json(url)

    def get_volume(self) -> Any:
        """Get volume data."""
        url = f"{self.base_url}/volume"
        return get_json(url)

    def get_treasuries(self) -> Any:
        """Get treasury data."""
        url = f"{self.base_url}/treasuries"
        return get_json(url)

    def get_airdrops(self) -> Any:
        """Get airdrop data."""
        url = f"{self.base_url}/airdrops"
        return get_json(url)
