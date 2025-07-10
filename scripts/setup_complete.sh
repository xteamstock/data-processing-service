#!/bin/bash
# Complete setup script for data processing service
# Runs all setup steps in the correct order

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}
REGION=${REGION:-"asia-southeast1"}

echo "🚀 Complete setup for Data Processing Service"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""

# Step 1: Enable APIs
echo "Step 1: Enabling required GCP APIs..."
./scripts/enable_apis.sh

echo ""
echo "⏳ Waiting 30 seconds for APIs to be fully enabled..."
sleep 30

# Step 2: Create service account
echo ""
echo "Step 2: Creating service account and permissions..."
./scripts/setup_service_account.sh

# Step 3: Create Pub/Sub topics
echo ""
echo "Step 3: Creating Pub/Sub topics..."
./scripts/create_pubsub_topics.sh

# Step 4: Create BigQuery tables
echo ""
echo "Step 4: Creating BigQuery tables..."
python scripts/create_bigquery_tables.py

# Step 5: Deploy service
echo ""
echo "Step 5: Deploying service to Cloud Run..."
./scripts/deploy.sh

# Step 6: Create push subscriptions
echo ""
echo "Step 6: Creating Pub/Sub push subscriptions..."
./scripts/create_push_subscriptions.sh

echo ""
echo "🎉 Data Processing Service setup complete!"
echo ""
echo "Service Details:"
SERVICE_URL=$(gcloud run services describe data-processing-service --region=$REGION --format="value(status.url)" --project=$PROJECT_ID 2>/dev/null)
echo "- Service URL: $SERVICE_URL"
echo "- Health Check: ${SERVICE_URL}/health"
echo "- Pub/Sub Endpoint: ${SERVICE_URL}/api/v1/events/data-ingestion-completed"
echo "- Test Endpoint: ${SERVICE_URL}/api/v1/test"
echo ""
echo "Testing commands:"
echo "# Health check"
echo "curl ${SERVICE_URL}/health"
echo ""
echo "# Manual test"
echo "curl -X POST ${SERVICE_URL}/api/v1/test -H \"Content-Type: application/json\" -d '{\"test\": \"data\"}'"
echo ""
echo "# Monitor logs"
echo "gcloud run services logs read data-processing-service --region=$REGION --limit=20"
echo ""
echo "Next steps:"
echo "1. Test the service with the commands above"
echo "2. Verify integration with data-ingestion service"
echo "3. Monitor processing events in BigQuery"