# DeFi BI-ETL Pipeline

A comprehensive Business Intelligence and ETL pipeline for collecting, processing, and analyzing DeFi (Decentralized Finance) data from multiple sources including CoinGecko, DeFiLlama, and DexScreener.

## 🚀 Features

- **Multi-source Data Extraction**: Collect data from CoinGecko, DeFiLlama, and DexScreener APIs
- **Automated ETL Pipeline**: End-to-end data processing with configurable workflows
- **Data Transformation**: Clean, normalize, and enrich raw data
- **Tableau Integration**: Export processed data in Tableau-ready formats
- **Comprehensive Testing**: Full test coverage with pytest
- **Code Quality**: Automated formatting, linting, and type checking

## 📊 Data Sources

- **CoinGecko**: Market data, token prices, categories, and trending tokens
- **DeFiLlama**: Total Value Locked (TVL) data and protocol information
- **DexScreener**: DEX trading data and market analytics

## 🛠️ Installation

### Prerequisites

- Python 3.10+
- Poetry (recommended) or pip
- Git

### Quick Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/dariapavlova02/defi_bi_seminar.git
   cd defi_bi_seminar
   ```

2. **Install dependencies**:
   ```bash
   # Using Poetry (recommended)
   poetry install
   
   # Or using pip
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env file with your API keys
   ```

## 🚀 Quick Start

### Complete ETL Pipeline

```bash
# Run the complete pipeline
poetry run python -m src.cli quickrun
```

### Step-by-step Execution

```bash
# 1. Extract data
poetry run python -m src.cli extract coingecko --all
poetry run python -m src.cli extract defillama --all
poetry run python -m src.cli extract dexscreener --all

# 2. Transform data
poetry run python -m src.cli transform all

# 3. Export to Tableau
poetry run python -m src.cli load tableau
```

## 📁 Project Structure

```
defi_bi_seminar/
├── src/                    # Source code
│   ├── clients/           # API clients
│   ├── extract/           # Data extraction modules
│   ├── transform/         # Data transformation modules
│   ├── load/              # Data loading modules
│   └── utils/             # Utility functions
├── data/                  # Data storage
│   ├── raw/              # Raw API responses
│   └── processed/        # Processed CSV files
├── dashboards/            # Dashboard exports
│   └── tableau/          # Tableau-ready CSV files
├── notebooks/             # Jupyter notebooks
├── tests/                 # Test files
└── config/                # Configuration files
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# CoinGecko API (optional, for higher rate limits)
COINGECKO_API_KEY=your_api_key_here

# Logging level
LOG_LEVEL=INFO
```

### Settings

Configuration files are located in the `config/` directory:

- `settings.yaml`: Main application configuration
- `logging.yaml`: Logging configuration

## 🧪 Testing

### Run Tests

```bash
# All tests
poetry run pytest

# With coverage
poetry run pytest --cov=src

# Specific test file
poetry run pytest tests/test_schemas.py
```

### Code Quality

```bash
# Format code
poetry run black src/ tests/

# Sort imports
poetry run isort src/ tests/

# Lint code
poetry run flake8 src/ tests/

# Type checking
poetry run mypy src/
```

## 📈 Usage Examples

### Extract Specific Data

```bash
# Extract only market data from CoinGecko
poetry run python -m src.cli extract coingecko --markets

# Extract TVL data from DeFiLlama
poetry run python -m src.cli extract defillama --tvl

# Extract trending tokens
poetry run python -m src.cli extract coingecko --trending
```

### Transform Specific Data

```bash
# Transform only market data
poetry run python -m src.cli transform --markets

# Transform only TVL data
poetry run python -m src.cli transform --tvl
```

### Export Options

```bash
# Export to Tableau
poetry run python -m src.cli load tableau

# Check available export formats
poetry run python -m src.cli load --help
```

## 📊 Output Files

After successful execution, you'll find:

- **Raw data**: `data/raw/` (JSON files from APIs)
- **Processed data**: `data/processed/` (CSV files)
- **Tableau exports**: `dashboards/tableau/` (ready-to-connect CSV files)

### Tableau Export Files

- `kpi_snapshot.csv`: Key Performance Indicators
- `markets_top.csv`: Top market movers
- `categories_snapshot.csv`: Category distribution
- `tvl_protocols_30d.csv`: TVL protocol data

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### Development Setup

```bash
# Install development dependencies
poetry install --with dev

# Setup pre-commit hooks
poetry run pre-commit install

# Run quality checks
make quick-test
```

## 📚 Documentation

- [Quick Start Guide](QUICKSTART.md) - Get up and running quickly
- [Project Structure](PROJECT_STRUCTURE.md) - Detailed project overview
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute

## 🐛 Troubleshooting

### Common Issues

1. **API Rate Limiting**: Wait and retry, or add API keys
2. **Missing Dependencies**: Run `poetry install` or `pip install -r requirements.txt`
3. **Data Not Found**: Ensure extraction step completed successfully

### Getting Help

- Check existing [issues](https://github.com/dariapavlova02/defi_bi_seminar/issues)
- Review the documentation
- Run tests to verify your setup

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- **Daria Pavlova** - *Initial work* - [dariapavlova02](https://github.com/dariapavlova02)

## 🙏 Acknowledgments

- CoinGecko for providing comprehensive cryptocurrency data
- DeFiLlama for TVL and protocol information
- DexScreener for DEX trading data
- The open-source community for tools and libraries

---

**Note**: This project is designed for educational and research purposes. Please ensure compliance with API terms of service and data usage policies.
