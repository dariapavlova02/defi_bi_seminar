# Contributing to DeFi BI-ETL

Thank you for your interest in contributing to the DeFi BI-ETL project! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)

## Code of Conduct

This project and everyone participating in it is governed by our Code of Conduct. By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

- Use the GitHub issue tracker
- Include a clear and descriptive title
- Describe the exact steps to reproduce the bug
- Provide specific examples to demonstrate the steps
- Describe the behavior you observed after following the steps
- Explain which behavior you expected to see instead and why
- Include details about your configuration and environment

### Suggesting Enhancements

- Use the GitHub issue tracker
- Provide a clear and descriptive title
- Describe the current behavior and explain which behavior you expected to see instead
- Provide specific examples to demonstrate the steps
- Explain why this enhancement would be useful to most users

### Pull Requests

- Fork the repository
- Create a new branch for your feature
- Make your changes
- Add tests for new functionality
- Ensure all tests pass
- Update documentation as needed
- Submit a pull request

## Development Setup

### Prerequisites

- Python 3.10+
- Poetry
- Git

### Setup Steps

1. Fork and clone the repository:
```bash
git clone https://github.com/dariapavlova02/defi_bi_seminar.git
cd defi_bi_seminar
```

2. Install dependencies:
```bash
poetry install
```

3. Pre-commit hooks are disabled for this project

4. Create a virtual environment:
```bash
poetry shell
```

## Code Style

### Python Code

We use several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking

### Running Code Quality Tools

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

### Code Style Guidelines

- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Write docstrings for all public functions and classes
- Keep functions small and focused
- Use meaningful variable and function names
- Add comments for complex logic

### Example Code Style

```python
from typing import List, Optional, Dict, Any
import pandas as pd


def process_market_data(
    data: pd.DataFrame,
    filters: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Process market data with optional filtering.

    Args:
        data: Input market data DataFrame
        filters: Optional filters to apply

    Returns:
        Processed market data DataFrame

    Raises:
        ValueError: If data is empty or invalid
    """
    if data.empty:
        raise ValueError("Input data cannot be empty")

    # Process data logic here
    processed_data = data.copy()

    if filters:
        processed_data = _apply_filters(processed_data, filters)

    return processed_data


def _apply_filters(data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """Apply filters to the data."""
    # Implementation here
    pass
```

## Testing

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src

# Run specific test file
poetry run pytest tests/test_schemas.py

# Run specific test function
poetry run pytest tests/test_schemas.py::TestDataSchemas::test_markets_data_schema

# Run tests with markers
poetry run pytest -m "not slow"
```

### Writing Tests

- Write tests for all new functionality
- Use descriptive test names
- Test both positive and negative cases
- Mock external dependencies
- Use fixtures for common test data

### Test Structure

```python
import pytest
from unittest.mock import Mock, patch
from src.transform.normalize_markets import process_markets_data


class TestMarketDataProcessing:
    """Test market data processing functionality."""

    def test_process_markets_data_success(self, sample_market_data):
        """Test successful market data processing."""
        result = process_markets_data(sample_market_data)

        assert not result.empty
        assert "processed" in result.columns

    def test_process_markets_data_empty_input(self):
        """Test processing with empty input data."""
        with pytest.raises(ValueError, match="Input data cannot be empty"):
            process_markets_data(pd.DataFrame())

    @patch("src.transform.normalize_markets._external_api_call")
    def test_process_markets_data_with_api_call(self, mock_api):
        """Test processing with external API call."""
        mock_api.return_value = {"status": "success"}

        result = process_markets_data(sample_market_data)

        mock_api.assert_called_once()
        assert result is not None


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    return pd.DataFrame({
        "symbol": ["BTC", "ETH"],
        "price": [50000, 3000],
        "volume": [1000, 5000]
    })
```

## Documentation

### Code Documentation

- All public functions and classes must have docstrings
- Use Google-style docstrings
- Include type hints
- Document exceptions and edge cases

### README Updates

- Update README.md for new features
- Include usage examples
- Update installation instructions if needed
- Add new dependencies to requirements

### API Documentation

- Document all public APIs
- Include parameter descriptions
- Provide usage examples
- Document return values and exceptions

## Pull Request Process

### Before Submitting

1. Ensure your code follows the style guidelines
2. Run all tests and ensure they pass
3. Update documentation as needed
4. Check that your changes don't break existing functionality

### Pull Request Template

```markdown
## Description
Brief description of the changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] All tests pass
- [ ] New tests added for new functionality
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes introduced
```

### Review Process

- All pull requests require at least one review
- Address review comments promptly
- Maintainers may request changes before merging
- Squash commits before merging if requested

## Release Process

### Versioning

We use semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

### Release Steps

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create release tag
4. Build and publish to PyPI (if applicable)

## Getting Help

If you need help with contributing:

- Check existing issues and pull requests
- Ask questions in GitHub discussions
- Contact maintainers directly
- Review the documentation

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.

Thank you for contributing to DeFi BI-ETL!
