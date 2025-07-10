#!/bin/bash
# Create service account for data processing service
# Based on successful data-ingestion service account setup

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}
SERVICE_ACCOUNT_NAME="data-processing-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
USER_EMAIL=$(gcloud config get-value account)

echo "Setting up service account for data processing service..."
echo "Project: $PROJECT_ID"
echo "Service Account: $SERVICE_ACCOUNT_EMAIL"
echo "User: $USER_EMAIL"

# Step 1: Create service account
echo "Creating service account..."
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Data Processing Service Account" \
    --description="Service account for data processing microservice" \
    --project=$PROJECT_ID || true

# Step 2: Grant service account permissions
echo "Granting service account permissions..."

# BigQuery permissions (read/write data + execute jobs)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/bigquery.jobUser"

# Cloud Storage permissions (read raw data from data-ingestion service)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/storage.objectViewer"

# Pub/Sub permissions (subscribe to data-ingestion events + publish processing events)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/pubsub.subscriber"

# Cloud Run permissions (for push subscriptions)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/run.invoker"

# Step 3: Grant user permission to act as service account (for deployment)
echo "Granting user permission to act as service account..."
gcloud iam service-accounts add-iam-policy-binding $SERVICE_ACCOUNT_EMAIL \
    --member="user:$USER_EMAIL" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --project=$PROJECT_ID || true

echo "✅ Service account setup complete!"
echo "Service account: $SERVICE_ACCOUNT_EMAIL"
echo ""
echo "Permissions granted:"
echo "- BigQuery: dataEditor + jobUser (read/write data + execute queries)"
echo "- Cloud Storage: objectViewer (read raw data from GCS)"
echo "- Pub/Sub: publisher + subscriber (receive events + publish results)"
echo "- Cloud Run: invoker (for push subscriptions)"
echo "- User: serviceAccountTokenCreator (for deployment)"