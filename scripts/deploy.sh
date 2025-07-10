#!/bin/bash
# Deploy data processing service to Cloud Run
# Based on successful data-ingestion service deployment patterns

set -e

PROJECT_ID=${GOOGLE_CLOUD_PROJECT:-"competitor-destroyer"}
REGION=${REGION:-"asia-southeast1"}
SERVICE_NAME="data-processing-service"
SERVICE_ACCOUNT="data-processing-sa@${PROJECT_ID}.iam.gserviceaccount.com"
IMAGE_NAME="gcr.io/${PROJECT_ID}/data-processing:latest"
USER_EMAIL=$(gcloud config get-value account)

# Check if we should skip already completed steps
SKIP_BUILD=${SKIP_BUILD:-false}

echo "Deploying data processing service..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service: $SERVICE_NAME"
echo "Image: $IMAGE_NAME"
echo "User: $USER_EMAIL"
echo "Skip build: $SKIP_BUILD"

if [ "$SKIP_BUILD" = "false" ]; then
    # Step 1: Configure Docker for GCR
    echo "Configuring Docker for GCR..."
    gcloud auth configure-docker --quiet

    # Step 2: Build Docker image for AMD64 (Cloud Run requirement)
    echo "Building Docker image for AMD64 architecture..."
    docker build --platform linux/amd64 -t $IMAGE_NAME .

    # Step 3: Push image to GCR
    echo "Pushing image to Google Container Registry..."
    docker push $IMAGE_NAME
else
    echo "⏭️  Skipping Docker build and push (already completed)"
fi

# Step 4: Grant user permission to act as service account (critical for deployment)
echo "Granting user permission to act as service account..."
gcloud iam service-accounts add-iam-policy-binding $SERVICE_ACCOUNT \
    --member="user:$USER_EMAIL" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --project=$PROJECT_ID || true

# Step 5: Deploy to Cloud Run (with authentication required)
echo "Deploying to Cloud Run with authentication required..."
gcloud run deploy $SERVICE_NAME \
    --image=$IMAGE_NAME \
    --service-account=$SERVICE_ACCOUNT \
    --region=$REGION \
    --no-allow-unauthenticated \
    --memory=1Gi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=10 \
    --timeout=300 \
    --concurrency=100 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BIGQUERY_DATASET=social_analytics,PUBSUB_TOPIC_PREFIX=social-analytics,GCS_BUCKET_RAW_DATA=social-analytics-raw-data" \
    --project=$PROJECT_ID

echo "Service deployed successfully!"

# Step 6: Grant Pub/Sub service account permission to invoke service (for push subscriptions)
echo "Granting Pub/Sub service account permission to invoke service..."
PUBSUB_SERVICE_ACCOUNT="service-$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --region=$REGION \
    --member="serviceAccount:$PUBSUB_SERVICE_ACCOUNT" \
    --role="roles/run.invoker" \
    --project=$PROJECT_ID || true

# Step 7: Grant user permission to invoke service (for testing)
echo "Granting user permission to invoke service..."
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --region=$REGION \
    --member="user:$USER_EMAIL" \
    --role="roles/run.invoker" \
    --project=$PROJECT_ID || true

# Step 8: Get service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)" --project=$PROJECT_ID)
echo "Service URL: $SERVICE_URL"

# Step 9: Test authenticated health check
echo "Testing authenticated health check..."
HEALTH_RESPONSE=$(curl -s -X GET "${SERVICE_URL}/health" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -w "%{http_code}")

if [[ "$HEALTH_RESPONSE" == *"200" ]]; then
    echo "✅ Health check passed"
else
    echo "❌ Health check failed: $HEALTH_RESPONSE"
fi

# Step 10: Test authenticated endpoints
echo "Testing authenticated test endpoint..."
TEST_RESPONSE=$(curl -s -X POST "${SERVICE_URL}/api/v1/test" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"test": "deployment_verification"}' \
    -w "%{http_code}")

if [[ "$TEST_RESPONSE" == *"200" ]]; then
    echo "✅ Test endpoint passed"
else
    echo "❌ Test endpoint failed: $TEST_RESPONSE"
fi

# Step 10: Verify service configuration
echo "Verifying service configuration..."
echo "Service details:"
gcloud run services describe $SERVICE_NAME --region=$REGION --format="table(metadata.name,status.url,spec.template.spec.serviceAccountName,spec.template.spec.containers[0].image)" --project=$PROJECT_ID

echo ""
echo "🎉 Secure deployment complete!"
echo "Service URL: $SERVICE_URL"
echo "Health endpoint: ${SERVICE_URL}/health (authentication required)"
echo "Pub/Sub endpoint: ${SERVICE_URL}/api/v1/events/data-ingestion-completed (Pub/Sub service account authorized)"
echo "Test endpoint: ${SERVICE_URL}/api/v1/test (authentication required)"
echo ""
echo "🔒 Security Configuration:"
echo "- Authentication: REQUIRED (no unauthenticated access)"
echo "- Pub/Sub service account: AUTHORIZED to invoke service"
echo "- User access: AUTHORIZED for testing"
echo ""
echo "Testing commands:"
echo "# Health check (authenticated)"
echo "curl -X GET ${SERVICE_URL}/health \\"
echo "  -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\""
echo ""
echo "# Test endpoint (authenticated)"
echo "curl -X POST ${SERVICE_URL}/api/v1/test \\"
echo "  -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\" \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -d '{\"test\": \"data\"}'"
echo ""
echo "Next steps:"
echo "1. Run ./scripts/create_push_subscriptions.sh to create Pub/Sub subscriptions"
echo "2. Test the service with the provided curl commands"
echo "3. Monitor logs with: gcloud run services logs read $SERVICE_NAME --region=$REGION --limit=20"
echo "4. Verify Pub/Sub integration with data-ingestion service"