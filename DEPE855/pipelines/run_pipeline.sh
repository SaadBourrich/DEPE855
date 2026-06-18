#!/bin/bash
# run_pipeline.sh - Automated Fake News Pipeline Runner
# Runs every 6 hours via cron to process news data
#
# Usage:
#   ./run_pipeline.sh          # Run once
#   crontab -e                # Add to cron for scheduling
#
# Environment variables (set in crontab or here):
#   HDFS_HOST - HDFS NameNode host (optional)
#   HDFS_PORT - HDFS NameNode port, default 8020 (optional)
#   FAKE_NEWS_API_KEY - API key for Fake News Detector (required)
#   VENV_PATH - Path to Python virtual environment, default ./venv

set -euo pipefail

# ==================== CONFIG ====================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${VENV_PATH:-${SCRIPT_DIR}/venv}"
LOG_FILE="${SCRIPT_DIR}/logs/pipeline_$(date +%Y%m%d).log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "${LOG_FILE}")"

# ==================== LOGGING ====================
log() {
    echo "[${TIMESTAMP}] $1" | tee -a "${LOG_FILE}"
}

log "Starting fake news pipeline..."

# ==================== ENVIRONMENT ====================
# Load environment variables from .env file if it exists
if [ -f "${SCRIPT_DIR}/.env" ]; then
    log "Loading environment from .env file"
    set -a
    source "${SCRIPT_DIR}/.env"
    set +a
fi

# Validate required variables
if [ -z "${FAKE_NEWS_API_KEY:-}" ]; then
    log "ERROR: FAKE_NEWS_API_KEY is not set. Please set it in .env or environment."
    exit 1
fi

# ==================== VIRTUAL ENV ====================
if [ -d "${VENV_PATH}" ]; then
    log "Activating virtual environment at ${VENV_PATH}"
    source "${VENV_PATH}/bin/activate"
else
    log "WARNING: Virtual environment not found at ${VENV_PATH}. Using system Python."
fi

# ==================== DEPENDENCIES ====================
log "Checking Python dependencies..."
python -c "import pandas, pyarrow, hdfs3, requests" 2>/dev/null || {
    log "Installing missing dependencies..."
    pip install pandas pyarrow hdfs3 requests 2>&1 | tee -a "${LOG_FILE}"
}

# ==================== RUN PIPELINE ====================
log "Running pipeline.py..."

cd "${SCRIPT_DIR}"

# Run the pipeline with timestamp
python pipeline.py 2>&1 | tee -a "${LOG_FILE}"

# Check exit status
if [ $? -eq 0 ]; then
    log "Pipeline completed successfully"
else
    log "ERROR: Pipeline failed with exit code $?"
    exit 1
fi

log "Pipeline finished"