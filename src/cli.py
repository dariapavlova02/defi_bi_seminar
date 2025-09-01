"""Command-line interface for DeFi BI-ETL pipeline."""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

# Import ETL modules
from .extract.coingecko_extract import CoinGeckoExtractor
from .extract.defillama_extract import DeFiLlamaExtractor
from .extract.dexscreener_extract import DexScreenerExtractor
from .load.export_tableau import TableauExporter
from .transform.features_basic import process_all_features
from .transform.generate_historical_tvl import generate_historical_tvl_batch
from .transform.normalize_markets import (
    process_categories_file,
    process_markets_files,
    process_token_history_file,
)
from .transform.normalize_tvl import (
    process_chains_tvl_file,
    process_protocol_tvl_files,
    process_tvl_overview_file,
)

# Load environment variables
load_dotenv()


def extract_coingecko(args) -> dict:
    """Extract data from CoinGecko."""
    print("Starting CoinGecko data extraction...")

    extractor = CoinGeckoExtractor()

    if args.all:
        # Extract all data
        results = extractor.extract_all(
            pages=args.pages,
            token_ids=args.token_ids.split(",") if args.token_ids else None,
            days=args.days,
        )
        print("CoinGecko extraction completed successfully!")
        return results

    results = {}

    if args.global_data:
        results["global"] = extractor.extract_global()

    if args.defi_global:
        results["defi_global"] = extractor.extract_defi_global()

    if args.categories:
        results["categories"] = extractor.extract_categories()

    if args.markets:
        results["markets"] = extractor.extract_markets(pages=args.pages)

    if args.history:
        token_ids: list[str] | None = (
            args.token_ids.split(",") if args.token_ids else None
        )
        results["token_history"] = extractor.extract_token_history(
            token_ids=token_ids, days=args.days
        )

    if args.trending:
        results["trending"] = extractor.extract_trending()

    if not results:
        print("No extraction options specified. Use --help for available options.")
        return {}

    print("CoinGecko extraction completed successfully!")
    return results


def extract_defillama(args) -> dict:
    """Extract data from DeFiLlama."""
    print("Starting DeFiLlama data extraction...")

    extractor = DeFiLlamaExtractor()

    if args.all:
        # Extract all data
        results = extractor.extract_all()
        print("DeFiLlama extraction completed successfully!")
        return results

    results = {}

    if args.tvl:
        results["tvl_overview"] = extractor.extract_tvl_overview()

    if args.protocols:
        results["protocols"] = extractor.extract_protocols()

    if args.chains:
        results["chains_tvl"] = extractor.extract_chains_tvl()

    if args.protocol_tvl:
        slug_list: list[str] | None = (
            args.protocol_tvl.split(",") if args.protocol_tvl else None
        )
        results["protocol_tvl"] = extractor.extract_protocol_tvl(slug_list)

    if args.stablecoins:
        results["stablecoins"] = extractor.extract_stablecoins()

    if args.bridges:
        results["bridges"] = extractor.extract_bridges()

    if args.yields:
        results["yields"] = extractor.extract_yields()

    if not results:
        print("No extraction options specified. Use --help for available options.")
        return {}

    print("DeFiLlama extraction completed successfully!")
    return results


def extract_dexscreener(args) -> dict:
    """Extract data from DexScreener."""
    print("Starting DexScreener data extraction...")

    extractor = DexScreenerExtractor()

    if args.all:
        # Extract all data
        results = extractor.extract_all(
            chains=args.chains.split(",") if args.chains else None, limit=args.limit
        )
        print("DexScreener extraction completed successfully!")
        return results

    results = {}

    if args.pairs:
        chains = args.chains.split(",") if args.chains else None
        results["pairs_by_chain"] = extractor.extract_pairs_by_chain(
            chains=chains, limit=args.limit
        )

    if args.token_pairs:
        results["token_pairs"] = extractor.extract_token_pairs()

    if args.trending:
        results["trending_pairs"] = extractor.extract_trending_pairs()

    if args.chain_stats:
        chains = args.chains.split(",") if args.chains else None
        results["chain_stats"] = extractor.extract_chain_stats(chains)

    if args.dex_stats:
        results["dex_stats"] = extractor.extract_dex_stats()

    if args.top_volume:
        chains = args.chains.split(",") if args.chains else None
        results["top_pairs_volume"] = extractor.extract_top_pairs_by_volume(
            chains=chains, limit=args.limit
        )

    if not results:
        print("No extraction options specified. Use --help for available options.")
        return {}

    print("DexScreener extraction completed successfully!")
    return results


def transform_data(args) -> None:
    """Transform extracted data."""
    print("Starting data transformation...")

    if args.all:
        # Process all available data
        results = {}

        # Process CoinGecko data
        try:
            # Look for markets files
            raw_dir = Path("data/raw/coingecko")
            markets_files = list(raw_dir.glob("*markets*.json"))
            if markets_files:
                results["markets"] = process_markets_files(
                    [str(f) for f in markets_files]
                )

            # Look for categories file
            categories_files = list(raw_dir.glob("*categories*.json"))
            if categories_files:
                results["categories"] = process_categories_file(
                    str(categories_files[-1])
                )

            # Look for token history files
            history_files = list(raw_dir.glob("*token_history*.json"))
            for history_file in history_files:
                filename = history_file.stem
                token_id = filename.split("_")[-2]  # Extract token ID
                results[f"token_history_{token_id}"] = process_token_history_file(
                    str(history_file), token_id
                )

        except Exception as e:
            print(f"Error processing CoinGecko data: {e}")

        # Process DeFiLlama data
        try:
            # Look for TVL overview file
            raw_dir = Path("data/raw/defillama")
            tvl_files = list(raw_dir.glob("*tvl_overview*.json"))
            if tvl_files:
                results["tvl_overview"] = process_tvl_overview_file(str(tvl_files[-1]))

            # Look for protocols file
            protocols_files = list(raw_dir.glob("*protocols*.json"))
            if protocols_files:
                results["protocols"] = process_protocol_tvl_files(
                    [str(f) for f in protocols_files]
                )

            # Look for chains TVL file
            chains_files = list(raw_dir.glob("*chains_tvl*.json"))
            if chains_files:
                results["chains_tvl"] = process_chains_tvl_file(str(chains_files[-1]))

        except Exception as e:
            print(f"Error processing DeFiLlama data: {e}")

        # Add features
        try:
            features_results = process_all_features()
            results.update(features_results)
        except Exception as e:
            print(f"Error adding features: {e}")

        print("Data transformation completed successfully!")
        return results

    # Process specific data types
    results = {}

    if args.markets:
        try:
            raw_dir = Path("data/raw/coingecko")
            markets_files = list(raw_dir.glob("*markets*.json"))
            if markets_files:
                results["markets"] = process_markets_files(
                    [str(f) for f in markets_files]
                )
        except Exception as e:
            print(f"Error processing markets data: {e}")

    if args.categories:
        try:
            raw_dir = Path("data/raw/coingecko")
            categories_files = list(raw_dir.glob("*categories*.json"))
            if categories_files:
                results["categories"] = process_categories_file(
                    str(categories_files[-1])
                )
        except Exception as e:
            print(f"Error processing categories data: {e}")

    if args.tvl:
        try:
            raw_dir = Path("data/raw/defillama")

            # Process main protocols file (all 6000+ protocols)
            protocols_files = list(raw_dir.glob("*protocols*.json"))
            if protocols_files:
                results["all_protocols"] = process_tvl_overview_file(
                    str(protocols_files[-1])
                )

            # Process individual protocol TVL files (for historical data)
            protocol_tvl_files = list(raw_dir.glob("*protocol_tvl*.json"))
            if protocol_tvl_files:
                results["protocol_tvl"] = process_protocol_tvl_files(
                    [str(f) for f in protocol_tvl_files]
                )
        except Exception as e:
            print(f"Error processing TVL data: {e}")

    if args.features:
        try:
            features_results = process_all_features()
            results.update(features_results)
        except Exception as e:
            print(f"Error adding features: {e}")

    if args.historical_tvl:
        try:
            print("Generating historical TVL data for all protocols...")
            # Use batch processing for better performance
            results["historical_tvl"] = generate_historical_tvl_batch(batch_size=50)
        except Exception as e:
            print(f"Error generating historical TVL data: {e}")

    if not results:
        print("No transformation options specified. Use --help for available options.")
        return {}

    print("Data transformation completed successfully!")
    return results


def load_tableau(args) -> None:
    """Load data to Tableau format."""
    print("Starting Tableau export...")

    exporter = TableauExporter()

    # Get list of processed files
    processed_dir = Path("data/processed")
    processed_files = {}

    if processed_dir.exists():
        for csv_file in processed_dir.glob("*.csv"):
            processed_files[csv_file.stem] = str(csv_file)

    # Export to Tableau
    results = exporter.export_all(processed_files)

    print("Tableau export completed successfully!")
    return results


def quickrun(args) -> None:
    """Run complete ETL pipeline."""
    print("Starting complete ETL pipeline...")

    # Step 1: Extract
    print("\n=== STEP 1: EXTRACT ===")
    coingecko_results = extract_coingecko(
        argparse.Namespace(all=True, pages=1, token_ids=None, days=30)
    )

    defillama_results = extract_defillama(
        argparse.Namespace(all=True, protocol_tvl=None)
    )

    dexscreener_results = extract_dexscreener(
        argparse.Namespace(all=True, chains=None, limit=100)
    )

    # Step 2: Transform
    print("\n=== STEP 2: TRANSFORM ===")
    transform_results = transform_data(argparse.Namespace(all=True))

    # Step 3: Load
    print("\n=== STEP 3: LOAD ===")
    tableau_results = load_tableau(argparse.Namespace())

    print("\n=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")
    print(
        f"Extracted: {len(coingecko_results) + len(defillama_results) + len(dexscreener_results)} files"
    )
    print(f"Transformed: {len(transform_results)} files")
    print(f"Exported to Tableau: {len(tableau_results)} files")

    return {
        "extract": {**coingecko_results, **defillama_results, **dexscreener_results},
        "transform": transform_results,
        "load": tableau_results,
    }


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DeFi BI-ETL Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract CoinGecko data
  python -m src.cli extract coingecko --markets --global --defi --categories

  # Extract DeFiLlama TVL data
  python -m src.cli extract defillama --tvl --protocols --chains

  # Transform all data
  python -m src.cli transform all

  # Export to Tableau
  python -m src.cli load tableau

  # Run complete pipeline
  python -m src.cli quickrun
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract data from APIs")
    extract_subparsers = extract_parser.add_subparsers(
        dest="source", help="Data source"
    )

    # CoinGecko extract
    cg_parser = extract_subparsers.add_parser(
        "coingecko", help="Extract from CoinGecko"
    )
    cg_parser.add_argument(
        "--all", action="store_true", help="Extract all available data"
    )
    cg_parser.add_argument(
        "--global",
        dest="global_data",
        action="store_true",
        help="Extract global market data",
    )
    cg_parser.add_argument(
        "--defi",
        dest="defi_global",
        action="store_true",
        help="Extract DeFi global data",
    )
    cg_parser.add_argument(
        "--categories", action="store_true", help="Extract categories data"
    )
    cg_parser.add_argument(
        "--markets", action="store_true", help="Extract markets data"
    )
    cg_parser.add_argument(
        "--history", action="store_true", help="Extract token history data"
    )
    cg_parser.add_argument(
        "--trending", action="store_true", help="Extract trending coins data"
    )
    cg_parser.add_argument(
        "--pages", type=int, default=1, help="Number of pages for markets (default: 1)"
    )
    cg_parser.add_argument(
        "--token-ids", type=str, help="Comma-separated list of token IDs for history"
    )
    cg_parser.add_argument(
        "--days", type=int, default=30, help="Days of history to extract (default: 30)"
    )

    # DeFiLlama extract
    dl_parser = extract_subparsers.add_parser(
        "defillama", help="Extract from DeFiLlama"
    )
    dl_parser.add_argument(
        "--all", action="store_true", help="Extract all available data"
    )
    dl_parser.add_argument("--tvl", action="store_true", help="Extract TVL overview")
    dl_parser.add_argument(
        "--protocols", action="store_true", help="Extract protocols list"
    )
    dl_parser.add_argument(
        "--chains", action="store_true", help="Extract chains TVL data"
    )
    dl_parser.add_argument(
        "--protocol-tvl",
        type=str,
        help="Comma-separated list of protocol slugs for TVL history",
    )
    dl_parser.add_argument(
        "--stablecoins", action="store_true", help="Extract stablecoins data"
    )
    dl_parser.add_argument(
        "--bridges", action="store_true", help="Extract bridges data"
    )
    dl_parser.add_argument("--yields", action="store_true", help="Extract yields data")

    # DexScreener extract
    ds_parser = extract_subparsers.add_parser(
        "dexscreener", help="Extract from DexScreener"
    )
    ds_parser.add_argument(
        "--all", action="store_true", help="Extract all available data"
    )
    ds_parser.add_argument(
        "--pairs", action="store_true", help="Extract pairs by chain"
    )
    ds_parser.add_argument(
        "--token-pairs", action="store_true", help="Extract pairs by token address"
    )
    ds_parser.add_argument(
        "--trending", action="store_true", help="Extract trending pairs"
    )
    ds_parser.add_argument(
        "--chain-stats", action="store_true", help="Extract chain statistics"
    )
    ds_parser.add_argument(
        "--dex-stats", action="store_true", help="Extract DEX statistics"
    )
    ds_parser.add_argument(
        "--top-volume", action="store_true", help="Extract top pairs by volume"
    )
    ds_parser.add_argument(
        "--chains",
        type=str,
        default="ethereum,bsc,solana",
        help="Comma-separated list of chains",
    )
    ds_parser.add_argument(
        "--limit", type=int, default=100, help="Limit number of results (default: 100)"
    )

    # Transform command
    transform_parser = subparsers.add_parser(
        "transform", help="Transform extracted data"
    )
    transform_parser.add_argument(
        "--all", action="store_true", help="Transform all available data"
    )
    transform_parser.add_argument(
        "--markets", action="store_true", help="Transform markets data"
    )
    transform_parser.add_argument(
        "--categories", action="store_true", help="Transform categories data"
    )
    transform_parser.add_argument(
        "--tvl", action="store_true", help="Transform TVL data"
    )
    transform_parser.add_argument(
        "--features", action="store_true", help="Add calculated features"
    )

    # Load command
    load_parser = subparsers.add_parser("load", help="Load data to target systems")
    load_parser.add_argument("tableau", help="Export to Tableau format")

    # Quickrun command
    subparsers.add_parser("quickrun", help="Run complete ETL pipeline")

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "extract":
            if args.source == "coingecko":
                extract_coingecko(args)
            elif args.source == "defillama":
                extract_defillama(args)
            elif args.source == "dexscreener":
                extract_dexscreener(args)
            else:
                print(
                    "Please specify a data source: coingecko, defillama, or dexscreener"
                )

        elif args.command == "transform":
            transform_data(args)

        elif args.command == "load":
            if "tableau" in args.tableau:
                load_tableau(args)
            else:
                print("Only Tableau export is currently supported")

        elif args.command == "quickrun":
            quickrun(args)

        else:
            print(f"Unknown command: {args.command}")
            parser.print_help()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
