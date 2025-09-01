# Project Structure

This document provides a comprehensive overview of the DeFi BI-ETL project structure, explaining the purpose and organization of each directory and file.

## Root Directory

```
defi_bi_seminar/
├── .github/                    # GitHub-specific configurations
├── config/                     # Configuration files
├── data/                       # Data storage directories
├── dashboards/                 # Dashboard and visualization files
├── notebooks/                  # Jupyter notebooks and analysis scripts
├── src/                        # Source code
├── tests/                      # Test files
├── .gitignore                  # Git ignore rules
├── CHANGELOG.md                # Version change history
├── CONTRIBUTING.md             # Contribution guidelines
├── env.example                 # Environment variables template
├── LICENSE                     # Apache 2.0 license
├── poetry.lock                 # Poetry dependency lock file
├── PROJECT_STRUCTURE.md        # This file
├── pyproject.toml              # Project configuration and dependencies
├── QUICKSTART.md               # Quick start guide
├── README.md                   # Main project documentation
└── requirements.txt             # Python dependencies
```

## Detailed Directory Structure

### `.github/` - GitHub Configuration

Contains GitHub-specific configurations and workflows:

- **`.github/workflows/ci.yml`** - Continuous Integration workflow
- **`.github/workflows/etl.yml`** - ETL pipeline automation workflow

### `config/` - Configuration Files

Configuration files for the application:

- **`settings.yaml`** - Main application configuration
- **`logging.yaml`** - Logging configuration (if applicable)

### `data/` - Data Storage

Data storage directories organized by processing stage:

#### `data/raw/` - Raw Data
- **`coingecko/`** - Raw data from CoinGecko API
- **`defillama/`** - Raw data from DeFiLlama API
- **`dexscreener/`** - Raw data from DexScreener API

#### `data/processed/` - Processed Data
- **`cg_*.csv`** - CoinGecko processed data files
- **`llama_*.csv`** - DeFiLlama processed data files
- **`dex_*.csv`** - DexScreener processed data files

### `dashboards/` - Dashboard Files

Dashboard and visualization files:

- **`tableau/`** - Tableau-ready CSV exports
  - `kpi_snapshot.csv` - Key Performance Indicators
  - `markets_top.csv` - Top market movers
  - `categories_snapshot.csv` - Category distribution
  - `tvl_protocols_30d.csv` - TVL protocol data

### `notebooks/` - Data Analysis and Exploration

Data analysis tools and exploration scripts:

- **`exploration.ipynb`** - Interactive Jupyter notebook for EDA and pipeline monitoring
- **`exploration.py`** - Source Python script for CLI analysis and automation
- **`convert_to_notebook.sh`** - Script to convert between Python and notebook formats
- **`README.md`** - Documentation for analysis tools and usage

**Purpose**: Provides interactive data validation, pipeline monitoring, and exploratory analysis capabilities

### `src/` - Source Code

Main application source code organized by functionality:

#### `src/cli.py` - Command Line Interface
Main entry point and CLI interface for the application.

#### `src/clients/` - API Clients
HTTP clients for external APIs:

- **`coingecko.py`** - CoinGecko API client
- **`defillama.py`** - DeFiLlama API client
- **`dexscreener.py`** - DexScreener API client

#### `src/extract/` - Data Extraction
Data extraction modules:

- **`coingecko_extract.py`** - CoinGecko data extraction
- **`defillama_extract.py`** - DeFiLlama data extraction
- **`dexscreener_extract.py`** - DexScreener data extraction

#### `src/transform/` - Data Transformation
Data transformation and processing modules:

- **`features_basic.py`** - Basic feature engineering
- **`generate_historical_tvl.py`** - Historical TVL generation
- **`normalize_markets.py`** - Market data normalization
- **`normalize_tvl.py`** - TVL data normalization

#### `src/load/` - Data Loading
Data loading and export modules:

- **`export_tableau.py`** - Tableau export functionality
- **`export_tableau_fixed.py`** - Fixed version of Tableau export
- **`export_tableau_old.py`** - Legacy Tableau export

#### `src/utils/` - Utility Functions
Common utility functions:

- **`http.py`** - HTTP client utilities
- **`io.py`** - Input/output utilities
- **`time.py`** - Time-related utilities

### `tests/` - Test Files

Test files organized by module:

- **`test_schemas.py`** - Data schema validation tests
- **`test_extract/`** - Extraction module tests
- **`test_transform/` - Transformation module tests
- **`test_load/`** - Loading module tests
- **`test_utils/`** - Utility function tests

## Configuration Files

### `pyproject.toml`
Main project configuration file containing:
- Project metadata and description
- Dependencies and development dependencies
- Build system configuration
- Tool configurations (Black, isort, mypy, pytest)

### `.env.example`
Environment variables template with:
- API keys and endpoints
- Configuration options
- Development settings

### Code Quality Tools
Code quality tools configured in pyproject.toml:
- Code formatting (Black, isort)
- Linting (flake8, mypy)
- Security checks (bandit)

## Data Flow

```
External APIs → Extract → Transform → Load → Output Files
     ↓            ↓         ↓         ↓         ↓
CoinGecko    raw/      processed/  tableau/  CSV Files
DeFiLlama    ↓         ↓           ↓
DexScreener  JSON      CSV         CSV
```

## File Naming Conventions

### Data Files
- **Raw data**: `{source}_{type}_{timestamp}.json`
- **Processed data**: `{source}_{type}.csv`
- **Tableau export**: `{type}_snapshot.csv`

### Source Code Files
- **Modules**: `{functionality}_{type}.py`
- **Tests**: `test_{module_name}.py`
- **Configuration**: `{tool}_config.yaml`

## Development Workflow

1. **Setup**: Clone repository and install dependencies
2. **Development**: Make changes in feature branches
3. **Testing**: Run tests and quality checks
4. **Review**: Submit pull request for review
5. **Integration**: Merge to main branch
6. **Deployment**: Automated CI/CD pipeline

## Key Files for Contributors

- **`README.md`** - Start here for project overview
- **`QUICKSTART.md`** - Quick setup and usage
- **`CONTRIBUTING.md`** - Contribution guidelines
- **`pyproject.toml`** - Project configuration
- **`pyproject.toml`** - Code quality tools configuration

## Notes

- All source code is in the `src/` directory
- Configuration files are in the `config/` directory
- Data files are organized by processing stage
- Tests mirror the source code structure
- Documentation is in the root directory
