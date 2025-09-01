#!/bin/bash

# DeFi BI-ETL Project Setup Script
# This script sets up the development environment for new contributors

set -e  # Exit on any error

echo "🚀 Setting up DeFi BI-ETL development environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running on supported OS
check_os() {
    print_status "Checking operating system..."

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
        print_success "Linux detected"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        print_success "macOS detected"
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        OS="windows"
        print_warning "Windows detected - some features may not work as expected"
    else
        print_error "Unsupported operating system: $OSTYPE"
        exit 1
    fi
}

# Check Python version
check_python() {
    print_status "Checking Python version..."

    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
        PYTHON_CMD="python"
    else
        print_error "Python not found. Please install Python 3.10+"
        exit 1
    fi

    # Extract major and minor version numbers
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

    if [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -ge 10 ]; then
        print_success "Python $PYTHON_VERSION found (Python 3.10+ required)"
    else
        print_error "Python $PYTHON_VERSION found, but Python 3.10+ is required"
        exit 1
    fi
}

# Install Poetry
install_poetry() {
    print_status "Checking Poetry installation..."

    if command -v poetry &> /dev/null; then
        POETRY_VERSION=$(poetry --version | awk '{print $3}')
        print_success "Poetry $POETRY_VERSION already installed"
    else
        print_status "Installing Poetry..."

        if [ "$OS" = "macos" ]; then
            curl -sSL https://install.python-poetry.org | python3 -
        elif [ "$OS" = "linux" ]; then
            curl -sSL https://install.python-poetry.org | python3 -
        else
            print_warning "Please install Poetry manually: https://python-poetry.org/docs/#installation"
            return 1
        fi

        # Add Poetry to PATH
        if [ -f "$HOME/.local/bin/poetry" ]; then
            export PATH="$HOME/.local/bin:$PATH"
            print_success "Poetry installed successfully"
        else
            print_error "Poetry installation failed"
            return 1
        fi
    fi
}

# Install system dependencies
install_system_deps() {
    print_status "Installing system dependencies..."

    if [ "$OS" = "macos" ]; then
        if command -v brew &> /dev/null; then
            print_status "Installing dependencies via Homebrew..."
            brew install git curl wget
        else
            print_warning "Homebrew not found. Please install it manually: https://brew.sh/"
        fi
    elif [ "$OS" = "linux" ]; then
        if command -v apt-get &> /dev/null; then
            print_status "Installing dependencies via apt..."
            sudo apt-get update
            sudo apt-get install -y git curl wget build-essential
        elif command -v yum &> /dev/null; then
            print_status "Installing dependencies via yum..."
            sudo yum install -y git curl wget gcc
        else
            print_warning "Package manager not detected. Please install git, curl, and wget manually."
        fi
    fi
}

# Setup project
setup_project() {
    print_status "Setting up project..."

    # Create necessary directories
    mkdir -p data/raw data/processed dashboards/tableau logs cache tests/data tests/output

    # Copy environment file
    if [ ! -f .env ]; then
        if [ -f env.example ]; then
            cp env.example .env
            print_success "Environment file created from template"
            print_warning "Please edit .env file with your API keys and configuration"
        else
            print_warning "env.example not found. Please create .env file manually"
        fi
    else
        print_success "Environment file already exists"
    fi

    # Install Python dependencies
    print_status "Installing Python dependencies..."
    poetry install

    # Pre-commit hooks are disabled for this project
    print_status "Pre-commit hooks are disabled for this project"

    print_success "Project setup completed!"
}

# Setup development tools
setup_dev_tools() {
    print_status "Setting up development tools..."

    # Install additional development dependencies
    poetry add --group dev black isort flake8 mypy bandit pytest pytest-cov

    # Create .gitignore if it doesn't exist
    if [ ! -f .gitignore ]; then
        print_warning ".gitignore not found. Please create it manually."
    fi

    print_success "Development tools setup completed!"
}

# Run initial tests
run_tests() {
    print_status "Running initial tests..."

    if poetry run pytest --version &> /dev/null; then
        poetry run pytest --tb=short -v
        print_success "Initial tests completed!"
    else
        print_warning "Tests not run. Please run 'poetry run pytest' manually."
    fi
}

# Show next steps
show_next_steps() {
    echo ""
    echo "🎉 Setup completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Edit .env file with your API keys and configuration"
    echo "2. Activate the virtual environment: poetry shell"
    echo "3. Run the ETL pipeline: poetry run python -m src.cli quickrun"
    echo "4. Run tests: poetry run pytest"
    echo "5. Format code: poetry run black src/ tests/"
    echo "6. Check code quality: poetry run flake8 src/ tests/"
    echo ""
    echo "Useful commands:"
    echo "- poetry shell          # Activate virtual environment"
    echo "- poetry run pytest     # Run tests"
    echo "- poetry run black      # Format code"
    echo "- poetry run mypy       # Type checking"
    echo ""
    echo "Documentation:"
    echo "- README.md             # Project overview"
    echo "- QUICKSTART.md         # Quick start guide"
    echo "- CONTRIBUTING.md       # Contribution guidelines"
    echo "- PROJECT_STRUCTURE.md  # Project structure"
    echo ""
}

# Main setup function
main() {
    echo "=========================================="
    echo "    DeFi BI-ETL Development Setup"
    echo "=========================================="
    echo ""

    check_os
    check_python
    install_system_deps
    install_poetry
    setup_project
    setup_dev_tools
    run_tests
    show_next_steps

    echo "=========================================="
    echo "    Setup completed successfully!"
    echo "=========================================="
}

# Run main function
main "$@"
