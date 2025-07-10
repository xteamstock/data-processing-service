#!/bin/bash
# Deploy the reimplemented data-processing service with dual-output jobs
# Based on proven deployment patterns from deployment-results.md

set -e

# Configuration
SERVICE_NAME="data-processing-service"
REGION="asia-southeast1"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-social-analytics-prod}"
IMAGE_TAG="dual-output-$(date +%Y%m%d-%H%M%S)"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Deploying Reimplemented Data Processing Service${NC}"
echo -e "${BLUE}=================================================${NC}"
echo "Service: $SERVICE_NAME"
echo "Region: $REGION"
echo "Project: $PROJECT_ID"
echo "Image Tag: $IMAGE_TAG"
echo ""

# Check if we're in the right directory
if [ ! -f "app.py" ] || [ ! -f "requirements.txt" ]; then
    echo -e "${RED}❌ Error: Must run from data-processing service directory${NC}"
    echo "Expected files: app.py, requirements.txt"
    exit 1
fi

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" > /dev/null 2>&1; then
    echo -e "${RED}❌ Error: Not authenticated with gcloud${NC}"
    echo "Please run: gcloud auth login"
    exit 1
fi

# Set project
echo -e "${YELLOW}📋 Setting GCP project...${NC}"
gcloud config set project "$PROJECT_ID"

# Enable required APIs (if not already enabled)
echo -e "${YELLOW}🔧 Ensuring required APIs are enabled...${NC}"
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com

# Create service account if it doesn't exist
SERVICE_ACCOUNT="data-processing-sa@${PROJECT_ID}.iam.gserviceaccount.com"
echo -e "${YELLOW}👤 Checking service account: $SERVICE_ACCOUNT${NC}"

if ! gcloud iam service-accounts describe "$SERVICE_ACCOUNT" > /dev/null 2>&1; then
    echo -e "${YELLOW}👤 Creating service account...${NC}"
    gcloud iam service-accounts create data-processing-sa \
        --description="Service account for Data Processing Service with dual-output jobs" \
        --display-name="Data Processing SA"
    
    # Grant necessary permissions
    echo -e "${YELLOW}🔑 Granting permissions...${NC}"
    
    # GCS permissions for both raw and processed data buckets
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/storage.objectViewer"
    
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/storage.objectCreator"
    
    # BigQuery permissions
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/bigquery.dataEditor"
    
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/bigquery.jobUser"
    
    # Pub/Sub permissions
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="roles/pubsub.publisher"
    
    echo -e "${GREEN}✅ Service account created and configured${NC}"
else
    echo -e "${GREEN}✅ Service account already exists${NC}"
fi

# Create required GCS buckets if they don't exist
echo -e "${YELLOW}🪣 Checking GCS buckets...${NC}"

RAW_DATA_BUCKET="social-analytics-raw-data"
PROCESSED_DATA_BUCKET="social-analytics-processed-data"

for bucket in "$RAW_DATA_BUCKET" "$PROCESSED_DATA_BUCKET"; do
    if ! gsutil ls -b "gs://$bucket" > /dev/null 2>&1; then
        echo -e "${YELLOW}🪣 Creating bucket: $bucket${NC}"
        gsutil mb -p "$PROJECT_ID" -c STANDARD -l "$REGION" "gs://$bucket"
        echo -e "${GREEN}✅ Bucket created: gs://$bucket${NC}"
    else
        echo -e "${GREEN}✅ Bucket exists: gs://$bucket${NC}"
    fi
done

# Create BigQuery dataset and table if they don't exist
echo -e "${YELLOW}📊 Checking BigQuery resources...${NC}"

DATASET="social_analytics"
TABLE="posts"

if ! bq show "${PROJECT_ID}:${DATASET}" > /dev/null 2>&1; then
    echo -e "${YELLOW}📊 Creating BigQuery dataset: $DATASET${NC}"
    bq mk --location="$REGION" --dataset "${PROJECT_ID}:${DATASET}"
    echo -e "${GREEN}✅ Dataset created: ${PROJECT_ID}:${DATASET}${NC}"
else
    echo -e "${GREEN}✅ Dataset exists: ${PROJECT_ID}:${DATASET}${NC}"
fi

# Create Pub/Sub topics if they don't exist
echo -e "${YELLOW}📡 Checking Pub/Sub topics...${NC}"

TOPICS=(
    "social-analytics-data-ingestion-completed"
    "social-analytics-data-processing-completed"
    "social-analytics-media-processing-requested"
)

for topic in "${TOPICS[@]}"; do
    if ! gcloud pubsub topics describe "$topic" > /dev/null 2>&1; then
        echo -e "${YELLOW}📡 Creating topic: $topic${NC}"
        gcloud pubsub topics create "$topic"
        echo -e "${GREEN}✅ Topic created: $topic${NC}"
    else
        echo -e "${GREEN}✅ Topic exists: $topic${NC}"
    fi
done

# Build and deploy the service
echo -e "${YELLOW}🏗️ Building and deploying service...${NC}"

# Build the container image
echo -e "${YELLOW}🏗️ Building container image...${NC}"
gcloud builds submit --tag "gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}" .

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Container build failed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Container built successfully${NC}"

# Deploy to Cloud Run with dual-output jobs configuration
echo -e "${YELLOW}🚀 Deploying to Cloud Run...${NC}"

gcloud run deploy "$SERVICE_NAME" \
    --image="gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}" \
    --service-account="$SERVICE_ACCOUNT" \
    --region="$REGION" \
    --allow-unauthenticated \
    --memory=2Gi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=20 \
    --timeout=300 \
    --concurrency=100 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BIGQUERY_DATASET=${DATASET},GCS_BUCKET_RAW_DATA=${RAW_DATA_BUCKET},GCS_BUCKET_PROCESSED_DATA=${PROCESSED_DATA_BUCKET},PUBSUB_TOPIC_PREFIX=social-analytics" \
    --port=8080

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Cloud Run deployment failed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Service deployed successfully${NC}"

# Get the service URL
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --region="$REGION" --format="value(status.url)")

# Update push subscription with new service URL
echo -e "${YELLOW}📡 Updating Pub/Sub push subscription...${NC}"

SUBSCRIPTION_NAME="data-processing-ingestion-events"
PUSH_ENDPOINT="${SERVICE_URL}/api/v1/events/data-ingestion-completed"

# Check if subscription exists
if gcloud pubsub subscriptions describe "$SUBSCRIPTION_NAME" > /dev/null 2>&1; then
    echo -e "${YELLOW}📡 Updating existing push subscription...${NC}"
    gcloud pubsub subscriptions update "$SUBSCRIPTION_NAME" \
        --push-endpoint="$PUSH_ENDPOINT"
else
    echo -e "${YELLOW}📡 Creating new push subscription...${NC}"
    gcloud pubsub subscriptions create "$SUBSCRIPTION_NAME" \
        --topic="social-analytics-data-ingestion-completed" \
        --push-endpoint="$PUSH_ENDPOINT" \
        --ack-deadline=600 \
        --max-delivery-attempts=3
fi

echo -e "${GREEN}✅ Push subscription configured${NC}"

# Test the deployment
echo -e "${YELLOW}🧪 Testing deployment...${NC}"

# Test health endpoint
if curl -f -s "$SERVICE_URL/health" > /dev/null; then
    echo -e "${GREEN}✅ Health check passed${NC}"
else
    echo -e "${RED}❌ Health check failed${NC}"
    echo "Service URL: $SERVICE_URL"
    exit 1
fi

# Display deployment summary
echo -e "${GREEN}🎉 DEPLOYMENT SUCCESSFUL${NC}"
echo -e "${GREEN}========================${NC}"
echo "Service Name: $SERVICE_NAME"
echo "Service URL: $SERVICE_URL"
echo "Region: $REGION"
echo "Image: gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}"
echo "Service Account: $SERVICE_ACCOUNT"
echo ""
echo -e "${BLUE}📋 Environment Configuration:${NC}"
echo "  Raw Data Bucket: gs://$RAW_DATA_BUCKET"
echo "  Processed Data Bucket: gs://$PROCESSED_DATA_BUCKET"
echo "  BigQuery Dataset: ${PROJECT_ID}:${DATASET}"
echo "  Push Endpoint: $PUSH_ENDPOINT"
echo ""
echo -e "${BLUE}🧪 Test the deployment:${NC}"
echo "  Health Check: curl $SERVICE_URL/health"
echo "  Integration Test: python tests/test_deployment_integration.py"
echo ""
echo -e "${YELLOW}📝 Next Steps:${NC}"
echo "1. Update DATA_PROCESSING_URL environment variable:"
echo "   export DATA_PROCESSING_URL=$SERVICE_URL"
echo "2. Run integration tests to verify dual-output jobs"
echo "3. Monitor logs: gcloud logs tail projects/$PROJECT_ID/logs/run.googleapis.com%2Fstdout --filter='resource.labels.service_name=\"$SERVICE_NAME\"'"

# Write deployment info to deployment-results.md
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
DEPLOYMENT_ENTRY="
## 🚀 Deployment - $TIMESTAMP

### Dual-Output Jobs Implementation Deployment

**Image**: gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}
**Service URL**: $SERVICE_URL
**Status**: ✅ SUCCESS

### Commands Executed:
\`\`\`bash
gcloud builds submit --tag \"gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}\" .

gcloud run deploy \"$SERVICE_NAME\" \\
    --image=\"gcr.io/${PROJECT_ID}/${SERVICE_NAME}:${IMAGE_TAG}\" \\
    --service-account=\"$SERVICE_ACCOUNT\" \\
    --region=\"$REGION\" \\
    --allow-unauthenticated \\
    --memory=2Gi \\
    --cpu=1 \\
    --min-instances=0 \\
    --max-instances=20 \\
    --timeout=300 \\
    --concurrency=100 \\
    --set-env-vars=\"GOOGLE_CLOUD_PROJECT=${PROJECT_ID},BIGQUERY_DATASET=${DATASET},GCS_BUCKET_RAW_DATA=${RAW_DATA_BUCKET},GCS_BUCKET_PROCESSED_DATA=${PROCESSED_DATA_BUCKET},PUBSUB_TOPIC_PREFIX=social-analytics\" \\
    --port=8080
\`\`\`

### Features Deployed:
- ✅ Job 1: GCS processed data upload
- ✅ Job 2: BigQuery analytics insertion
- ✅ Job 3: Media detection and event publishing
- ✅ Enhanced error handling and logging
- ✅ Schema-driven data transformation

### Environment:
- Raw Data Bucket: gs://$RAW_DATA_BUCKET
- Processed Data Bucket: gs://$PROCESSED_DATA_BUCKET
- BigQuery Dataset: ${PROJECT_ID}:${DATASET}
- Push Endpoint: $PUSH_ENDPOINT

### Verification:
- Health check: ✅ PASS
- Service accessible: ✅ PASS
- Push subscription updated: ✅ PASS

### Next Steps:
1. Run integration tests with: python tests/test_deployment_integration.py
2. Monitor dual-output job performance
3. Verify data flow: Raw Data → Processing → GCS + BigQuery + Media Events
"

echo "$DEPLOYMENT_ENTRY" >> docs/deployment-results.md

echo -e "${GREEN}✅ Deployment logged to docs/deployment-results.md${NC}"