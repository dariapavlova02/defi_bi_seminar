"""DexScreener data extraction for DeFi BI-ETL pipeline."""

from typing import List

from ..clients.dexscreener import DexScreenerClient
from ..utils.io import get_raw_data_path, get_timestamped_filename, save_json


class DexScreenerExtractor:
    """Extract raw data from DexScreener API."""

    def __init__(self) -> None:
        self.client = DexScreenerClient()

    def extract_pairs_by_chain(
        self, chains: List[str] | None = None, limit: int = 100
    ) -> List[str]:
        """Extract pairs data for specific chains."""
        if chains is None:
            chains = ["ethereum", "bsc", "solana"]

        print(f"Extracting pairs data for {len(chains)} chains (limit: {limit})...")
        paths = []

        for chain in chains:
            print(f"  Chain: {chain}")
            try:
                data = self.client.get_pairs_by_chain(chain, limit)

                filename = get_timestamped_filename(f"pairs_{chain}")
                path = get_raw_data_path("dexscreener", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {chain}: {e}")
                continue

        return paths

    def extract_trending_pairs(self) -> str:
        """Extract trending pairs data."""
        print("Extracting trending pairs...")
        data = self.client.get_trending_pairs()

        filename = get_timestamped_filename("trending_pairs")
        path = get_raw_data_path("dexscreener", filename)

        save_json(data, path)
        return str(path)

    def extract_chain_stats(self, chains: List[str] | None = None) -> List[str]:
        """Extract statistics for specific chains."""
        if chains is None:
            chains = ["ethereum", "bsc", "solana", "polygon", "arbitrum"]

        print(f"Extracting chain statistics for {len(chains)} chains...")
        paths = []

        for chain in chains:
            print(f"  Chain: {chain}")
            try:
                data = self.client.get_chain_stats(chain)

                filename = get_timestamped_filename(f"chain_stats_{chain}")
                path = get_raw_data_path("dexscreener", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {chain}: {e}")
                continue

        return paths

    def extract_dex_stats(self, dexes: List[str] | None = None) -> List[str]:
        """Extract statistics for specific DEXes."""
        if dexes is None:
            dexes = ["uniswap", "pancakeswap", "raydium", "sushiswap"]

        print(f"Extracting DEX statistics for {len(dexes)} DEXes...")
        paths = []

        for dex in dexes:
            print(f"  DEX: {dex}")
            try:
                data = self.client.get_dex_stats(dex)

                filename = get_timestamped_filename(f"dex_stats_{dex}")
                path = get_raw_data_path("dexscreener", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {dex}: {e}")
                continue

        return paths

    def extract_top_pairs_by_volume(
        self, chains: List[str] | None = None, limit: int = 50
    ) -> List[str]:
        """Extract top pairs by volume for specific chains."""
        if chains is None:
            chains = ["ethereum", "bsc", "solana"]

        print(f"Extracting top pairs by volume for {len(chains)} chains...")
        paths = []

        for chain in chains:
            print(f"  Chain: {chain}")
            try:
                data = self.client.get_chain_pairs_sorted(
                    chain=chain, sort_by="volume24h", limit=limit
                )

                filename = get_timestamped_filename(f"top_pairs_volume_{chain}")
                path = get_raw_data_path("dexscreener", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {chain}: {e}")
                continue

        return paths

    def extract_token_pairs(
        self, token_addresses: List[str] | None = None
    ) -> List[str]:
        """Extract pairs data for specific tokens."""
        if token_addresses is None:
            # Real popular token addresses
            token_addresses = [
                "0xa0b86a33e6441b8b4b0d3325a7e3944f0a0e5b8a",  # Wrapped Bitcoin (WBTC)
                "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # Wrapped Bitcoin (WBTC) - real address
                "0xbb4cdb9cbd36b01bd1cbaef2af08854d3d3d31f5",  # Wrapped BNB (WBNB)
                "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # Wrapped Ether (WETH)
                "So11111111111111111111111111111111111111112",  # Wrapped SOL (WSOL)
            ]

        print(f"Extracting pairs data for {len(token_addresses)} tokens...")
        paths = []

        for token_address in token_addresses:
            print(f"  Token: {token_address}")
            try:
                data = self.client.get_token_pairs(token_address)

                filename = get_timestamped_filename(f"token_pairs_{token_address[:8]}")
                path = get_raw_data_path("dexscreener", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {token_address}: {e}")
                continue

        return paths

    def extract_all(self, chains: List[str] | None = None, limit: int = 100) -> dict:
        """Extract all available data."""
        if chains is None:
            chains = ["ethereum", "bsc", "solana", "polygon", "arbitrum"]

        results = {
            "pairs_by_chain": self.extract_pairs_by_chain(chains, limit),
            "trending_pairs": self.extract_trending_pairs(),
            "chain_stats": self.extract_chain_stats(chains),
            "dex_stats": self.extract_dex_stats(),
            "top_pairs_volume": self.extract_top_pairs_by_volume(chains, limit),
            "token_pairs": self.extract_token_pairs(),
        }

        print(
            f"Extraction completed. Files saved: {sum(len(v) if isinstance(v, list) else 1 for v in results.values())}"
        )
        return results
