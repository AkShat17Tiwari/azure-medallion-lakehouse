# ==============================================================================
# Dockerfile: Production Container for Lakehouse Medallion Pipeline & Observer
# ==============================================================================
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PORT=8080

WORKDIR /app

# Install OpenJDK (Headless) and build dependencies required for PySpark 3.5
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt requirements-dev.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application source code and configurations
COPY config/ ./config/
COPY src/ ./src/
COPY sql/ ./sql/
COPY templates/ ./templates/
COPY main.py serve.py .env.example README.md ./

# Create data and checkpoint directories with correct permissions
RUN mkdir -p data/raw data/bronze data/silver data/gold data/checkpoints

EXPOSE 8080

# Default command: start Lakehouse Interactive Observer dashboard
CMD ["python", "serve.py", "--port", "8080"]
