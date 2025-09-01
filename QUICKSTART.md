# Quick Start Guide

## Quick Start

### 1. Install Dependencies

```bash
# Poetry (recommended)
poetry install

# Or pip
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
# Copy example
cp .env.example .env

# Edit .env file
# Add your COINGECKO_API_KEY
```

### 3. First Run

```bash
# Complete ETL cycle
poetry run python -m src.cli quickrun

# Or step by step:
# 1. Data extraction
poetry run python -m src.cli extract coingecko --all
poetry run python -m src.cli extract defillama --all
poetry run python -m src.cli extract dexscreener --all

# 2. Data transformation
poetry run python -m src.cli transform all

# 3. Tableau export
poetry run python -m src.cli load tableau
```

### 4. Verify Results

```bash
# Run EDA
poetry run python notebooks/exploration.py

# Run tests
poetry run pytest

# Check created files
ls -la data/processed/
ls -la dashboards/tableau/
```

## What You'll Get

After successful execution, you'll have:

- **Raw data** in `data/raw/` (JSON files)
- **Processed data** in `data/processed/` (CSV files)
- **Tableau export** in `dashboards/tableau/` (ready-to-connect CSV)

## Basic Commands

```bash
# Help
poetry run python -m src.cli --help

# Extract specific data
poetry run python -m src.cli extract coingecko --markets --global
poetry run python -m src.cli extract defillama --tvl --protocols

# Transform specific data
poetry run python -m src.cli transform --markets --tvl

# Export to Tableau only
poetry run python -m src.cli load tableau
```

## File Structure

```
defi_bi_seminar/
├── data/
│   ├── raw/          # Raw JSON data
│   └── processed/    # Processed CSV
├── dashboards/
│   └── tableau/      # Ready CSV for Tableau
├── src/              # Source code
└── tests/            # Tests
```

## Common Issues

1. **API key not configured**: Check `.env` file
2. **Rate limiting**: Wait and retry
3. **Missing data**: Run `extract` before `transform`

## Next Steps

1. Set up automatic execution (cron, GitHub Actions)
2. Add monitoring and alerts
3. Expand Tableau dashboards
4. Add new data sources
