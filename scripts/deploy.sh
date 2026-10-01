#!/usr/bin/env bash
# ==============================================================================
# Automated Deployment Script for Azure Medallion Lakehouse
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================="
echo "🚀 Lakehouse Medallion Deployment Routine"
echo "================================================================="

cd "${PROJECT_ROOT}"

# 1. Environment & Config Verification
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found. Copying .env.example..."
    cp .env.example .env
fi

# 2. Check Python / Virtualenv
if [ -d ".venv" ]; then
    PYTHON_BIN=".venv/bin/python3"
    PYTEST_BIN=".venv/bin/pytest"
else
    PYTHON_BIN="python3"
    PYTEST_BIN="pytest"
fi

echo "✓ Using Python: $("${PYTHON_BIN}" --version)"

# 3. Run Quality Test Suite
echo "🧪 Running quality guardrails & test suite..."
"${PYTEST_BIN}" tests/ -v

# 4. Check for Docker
if command -v docker >/dev/null 2>&1; then
    echo "🐳 Docker detected. Building container..."
    docker build -t lakehouse-observer:latest .
    echo "✓ Docker image built: lakehouse-observer:latest"
    echo "ℹ️  Run 'docker compose up -d' to start the full stack with PostgreSQL."
else
    echo "ℹ️  Docker not detected. Starting local native background server..."
    echo "👉 Starting server on http://localhost:8080"
    "${PYTHON_BIN}" serve.py --port 8080 &
fi

echo "================================================================="
echo "✅ Deployment verification completed successfully!"
echo "================================================================="
