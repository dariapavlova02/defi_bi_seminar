"""CoinGecko data extraction for DeFi BI-ETL pipeline."""

import os
from typing import List

from ..clients.coingecko import CoinGeckoClient
from ..utils.io import get_raw_data_path, get_timestamped_filename, save_json


class CoinGeckoExtractor:
    """Extract raw data from CoinGecko API."""

    def __init__(self) -> None:
        self.client = CoinGeckoClient()
        self.vs_currency = os.getenv("VS_CURRENCY", "usd")
        self.per_page = int(os.getenv("COINGECKO_PER_PAGE", "200"))

    def extract_global(self) -> str:
        """Extract global market data."""
        print("Extracting global market data...")
        data = self.client.get_global()

        filename = get_timestamped_filename("global_markets")
        path = get_raw_data_path("coingecko", filename)

        save_json(data, path)
        return str(path)

    def extract_defi_global(self) -> str:
        """Extract global DeFi market data."""
        print("Extracting global DeFi market data...")
        data = self.client.get_global_defi()

        filename = get_timestamped_filename("global_defi")
        path = get_raw_data_path("coingecko", filename)

        save_json(data, path)
        return str(path)

    def extract_categories(self) -> str:
        """Extract coin categories."""
        print("Extracting coin categories...")
        data = self.client.get_categories()

        filename = get_timestamped_filename("categories")
        path = get_raw_data_path("coingecko", filename)

        save_json(data, path)
        return str(path)

    def extract_markets(self, pages: int = 1) -> List[str]:
        """Extract market data for coins."""
        print(f"Extracting market data for {pages} pages...")
        paths = []

        for page in range(1, pages + 1):
            print(f"  Page {page}/{pages}")
            data = self.client.get_markets(
                vs_currency=self.vs_currency, per_page=self.per_page, page=page
            )

            filename = get_timestamped_filename(f"markets_page_{page}")
            path = get_raw_data_path("coingecko", filename)

            save_json(data, path)
            paths.append(str(path))

        return paths

    def extract_token_history(
        self, token_ids: List[str] | None, days: int = 30
    ) -> List[str]:
        """Extract historical data for specific tokens."""
        if not token_ids:
            token_ids = os.getenv(
                "TOKEN_IDS_FOR_HISTORY", "bitcoin,ethereum,solana"
            ).split(",")

        print(f"Extracting {days}-day history for {len(token_ids)} tokens...")
        paths = []

        for token_id in token_ids:
            print(f"  Token: {token_id}")
            try:
                data = self.client.get_market_chart(
                    token_id=token_id, vs_currency=self.vs_currency, days=days
                )

                filename = get_timestamped_filename(f"token_history_{token_id}_{days}d")
                path = get_raw_data_path("coingecko", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {token_id}: {e}")
                continue

        return paths

    def extract_trending(self) -> str:
        """Extract trending search coins."""
        print("Extracting trending coins...")
        data = self.client.get_trending()

        filename = get_timestamped_filename("trending")
        path = get_raw_data_path("coingecko", filename)

        save_json(data, path)
        return str(path)

    def extract_all(
        self, pages: int = 1, token_ids: List[str] | None = None, days: int = 30
    ) -> dict:
        """Extract all available data."""
        if token_ids is None:
            token_ids = os.getenv(
                "TOKEN_IDS_FOR_HISTORY", "bitcoin,ethereum,solana"
            ).split(",")

        results = {
            "global": self.extract_global(),
            "defi_global": self.extract_defi_global(),
            "categories": self.extract_categories(),
            "markets": self.extract_markets(pages),
            "token_history": self.extract_token_history(token_ids, days),
            "trending": self.extract_trending(),
        }

        print(
            f"Extraction completed. Files saved: {sum(len(v) if isinstance(v, list) else 1 for v in results.values())}"
        )
        return results
