#!/bin/bash
# Create Pub/Sub topics and subscriptions for data processing service

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}
REGION=${REGION:-"asia-southeast1"}

echo "Creating Pub/Sub topics for data processing service..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"

# Create topics for data processing events
echo "Creating topics..."

gcloud pubsub topics create social-analytics-data-processing-started --project=$PROJECT_ID || true
gcloud pubsub topics create social-analytics-data-processing-completed --project=$PROJECT_ID || true
gcloud pubsub topics create social-analytics-data-processing-failed --project=$PROJECT_ID || true
gcloud pubsub topics create social-analytics-media-processing-requested --project=$PROJECT_ID || true

echo "Topics created successfully!"

# Note: Push subscriptions will be created after service deployment
echo "Note: Push subscriptions will be created after service deployment using create_push_subscriptions.sh"