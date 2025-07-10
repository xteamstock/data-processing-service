#!/bin/bash
# Run tests for data processing service

set -e

echo "Running tests for data processing service..."

# Set up test environment
export PYTHONPATH="$(pwd):$PYTHONPATH"
export GOOGLE_CLOUD_PROJECT="test-project"
export BIGQUERY_DATASET="test_analytics"

# Run unit tests
echo "Running unit tests..."
python -m pytest tests/ -v --tb=short

# Run specific test files
echo "Running text processor tests..."
python -m unittest tests.test_text_processor -v

echo "Running app tests..."
python -m unittest tests.test_app -v

echo "All tests completed!"