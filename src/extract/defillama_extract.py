"""DeFiLlama data extraction for DeFi BI-ETL pipeline."""

from typing import List

from ..clients.defillama import DeFiLlamaClient
from ..utils.io import get_raw_data_path, get_timestamped_filename, save_json


class DeFiLlamaExtractor:
    """Extract raw data from DeFiLlama API."""

    def __init__(self) -> None:
        self.client = DeFiLlamaClient()

    def extract_tvl_overview(self) -> str:
        """Extract TVL overview for all protocols."""
        print("Extracting TVL overview...")
        data = self.client.get_tvl_overview()

        filename = get_timestamped_filename("tvl_overview")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_protocols(self) -> str:
        """Extract list of all protocols."""
        print("Extracting protocols list...")
        data = self.client.get_protocols()

        filename = get_timestamped_filename("protocols")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_protocol_tvl(self, slug_list: List[str] | None) -> List[str]:
        """Extract historical TVL data for specific protocols."""
        if not slug_list:
            slug_list = ["uniswap", "aave", "compound", "makerdao", "curve"]

        print(f"Extracting TVL data for {len(slug_list)} protocols...")
        paths = []

        for slug in slug_list:
            print(f"  Protocol: {slug}")
            try:
                data = self.client.get_protocol_tvl(slug)

                filename = get_timestamped_filename(f"protocol_tvl_{slug}")
                path = get_raw_data_path("defillama", filename)

                save_json(data, path)
                paths.append(str(path))

            except Exception as e:
                print(f"    Error extracting {slug}: {e}")
                continue

        return paths

    def extract_chains_tvl(self) -> str:
        """Extract TVL data for all chains."""
        print("Extracting chains TVL data...")
        data = self.client.get_chains_tvl()

        filename = get_timestamped_filename("chains_tvl")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_stablecoins(self) -> str:
        """Extract stablecoin data."""
        print("Extracting stablecoin data...")
        data = self.client.get_stablecoins()

        filename = get_timestamped_filename("stablecoins")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_bridges(self) -> str:
        """Extract bridge data."""
        print("Extracting bridge data...")
        data = self.client.get_bridges()

        filename = get_timestamped_filename("bridges")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_yields(self) -> str:
        """Extract yield farming data."""
        print("Extracting yield farming data...")
        data = self.client.get_yields()

        filename = get_timestamped_filename("yields")
        path = get_raw_data_path("defillama", filename)

        save_json(data, path)
        return str(path)

    def extract_all(self, slug_list: List[str] | None = None) -> dict:
        """Extract all available data."""
        if slug_list is None:
            slug_list = ["uniswap", "aave", "compound", "makerdao", "curve"]

        results = {
            "tvl_overview": self.extract_tvl_overview(),
            "protocols": self.extract_protocols(),
            "protocol_tvl": self.extract_protocol_tvl(slug_list),
            "chains_tvl": self.extract_chains_tvl(),
            "stablecoins": self.extract_stablecoins(),
            "bridges": self.extract_bridges(),
            "yields": self.extract_yields(),
        }

        print(
            f"Extraction completed. Files saved: {sum(len(v) if isinstance(v, list) else 1 for v in results.values())}"
        )
        return results
