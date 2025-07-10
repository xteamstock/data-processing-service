#!/bin/bash
# Enable required GCP APIs for data processing service
# Based on successful data-ingestion service setup

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}

echo "Enabling required GCP APIs for data processing service..."
echo "Project: $PROJECT_ID"

# Enable required APIs
echo "Enabling APIs..."

gcloud services enable run.googleapis.com --project=$PROJECT_ID
gcloud services enable containerregistry.googleapis.com --project=$PROJECT_ID
gcloud services enable secretmanager.googleapis.com --project=$PROJECT_ID
gcloud services enable pubsub.googleapis.com --project=$PROJECT_ID
gcloud services enable bigquery.googleapis.com --project=$PROJECT_ID
gcloud services enable storage.googleapis.com --project=$PROJECT_ID
gcloud services enable cloudbuild.googleapis.com --project=$PROJECT_ID

echo "✅ All required APIs enabled successfully!"
echo ""
echo "Enabled APIs:"
echo "- Cloud Run: run.googleapis.com"
echo "- Container Registry: containerregistry.googleapis.com"  
echo "- Secret Manager: secretmanager.googleapis.com"
echo "- Pub/Sub: pubsub.googleapis.com"
echo "- BigQuery: bigquery.googleapis.com"
echo "- Cloud Storage: storage.googleapis.com"
echo "- Cloud Build: cloudbuild.googleapis.com"
echo ""
echo "Next steps:"
echo "1. Run ./scripts/setup_service_account.sh to create service account"
echo "2. Run ./scripts/create_pubsub_topics.sh to create topics"
echo "3. Run python scripts/create_bigquery_tables.py to create tables"
echo "4. Run ./scripts/deploy.sh to deploy the service"