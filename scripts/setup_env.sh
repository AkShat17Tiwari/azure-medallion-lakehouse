#!/usr/bin/env bash
# ==============================================================================
# Lakehouse Environment Setup Script
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=========================================================="
echo " Setting up Lakehouse Medallion Architecture Environment "
echo "=========================================================="

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed or not in PATH."
    exit 1
fi

PYTHON_VERSION=$(python3 --version)
echo "Found: $PYTHON_VERSION"

# 1. Create virtual environment if not present
VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists in $VENV_DIR."
fi

# 2. Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# 3. Upgrade pip and setuptools
echo "Upgrading pip..."
pip install --upgrade pip setuptools wheel

# 4. Install requirements
echo "Installing project requirements from requirements.txt..."
pip install -r requirements.txt

# 5. Prepare .env file
if [ ! -f ".env" ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo ".env created. Please update it with your Azure and PostgreSQL credentials."
else
    echo ".env file already exists. Skipping copy."
fi

echo ""
echo "=========================================================="
echo " Setup Complete! "
echo "=========================================================="
echo "To activate your virtual environment:"
echo "    source .venv/bin/activate"
echo ""
echo "To run tests:"
echo "    pytest"
echo ""
echo "To run the sample pipeline locally:"
echo "    python -m src.pipelines.run_pipeline --stage all"
echo "=========================================================="
