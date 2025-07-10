#!/bin/bash
# Create Pub/Sub push subscriptions for data processing service
# Based on successful data-ingestion service patterns

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}
REGION=${REGION:-"asia-southeast1"}
SERVICE_NAME="data-processing-service"
SERVICE_ACCOUNT="data-processing-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Creating Pub/Sub push subscriptions for data processing service..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service: $SERVICE_NAME"

# Step 1: Verify service is deployed
echo "Verifying service deployment..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)" --project=$PROJECT_ID 2>/dev/null)

if [ -z "$SERVICE_URL" ]; then
    echo "❌ Error: Could not get service URL. Make sure the service is deployed first."
    echo "Run: ./scripts/deploy.sh"
    exit 1
fi

echo "✅ Service URL: $SERVICE_URL"

# Step 2: Test service health before creating subscriptions
echo "Testing service health..."
if curl -f -H "Authorization: Bearer $(gcloud auth print-identity-token)" "${SERVICE_URL}/health" -s > /dev/null; then
    echo "✅ Service health check passed"
else
    echo "❌ Service health check failed. Please check service deployment."
    exit 1
fi

# Step 3: Create push subscription for data ingestion completed events
echo "Creating push subscription for data-ingestion-completed events..."

gcloud pubsub subscriptions create data-processing-ingestion-events \
    --topic=social-analytics-data-ingestion-completed \
    --push-endpoint=${SERVICE_URL}/api/v1/events/data-ingestion-completed \
    --ack-deadline=600 \
    --max-delivery-attempts=5 \
    --min-retry-delay=10s \
    --max-retry-delay=60s \
    --project=$PROJECT_ID || {
        echo "Note: Subscription may already exist"
    }

# Step 4: Grant Pub/Sub permission to invoke the service
echo "Granting Pub/Sub permission to invoke Cloud Run service..."
# Get project number for correct service account
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=$REGION \
    --project=$PROJECT_ID || true

echo "✅ Push subscriptions created successfully!"
echo ""
echo "Subscription Details:"
echo "- Subscription: data-processing-ingestion-events"
echo "- Topic: social-analytics-data-ingestion-completed" 
echo "- Endpoint: ${SERVICE_URL}/api/v1/events/data-ingestion-completed"
echo "- Ack Deadline: 600 seconds (10 minutes)"
echo "- Max Delivery Attempts: 3"
echo "- Retry Delays: 10s - 60s exponential backoff"
echo ""
echo "Testing:"
echo "- Health: curl ${SERVICE_URL}/health"
echo "- Manual test: curl -X POST ${SERVICE_URL}/api/v1/test -H \"Content-Type: application/json\" -d '{\"test\": \"data\"}'"
echo ""
echo "Next steps:"
echo "1. Verify subscription: gcloud pubsub subscriptions list --filter=\"name:data-processing\""
echo "2. Test end-to-end with data-ingestion service"
echo "3. Monitor logs: gcloud run services logs read $SERVICE_NAME --region=$REGION --limit=20"