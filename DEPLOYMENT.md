# Data Processing Service - Deployment Guide

This guide provides step-by-step instructions for deploying the Data Processing Service to Google Cloud Run, following the same successful patterns used for the Data Ingestion Service.

## 📋 Prerequisites

- Google Cloud Project with billing enabled
- Docker installed and configured
- gcloud CLI installed and authenticated
- Data Ingestion Service deployed and working (publishes events to this service)

## 🚀 Quick Start (Automated Setup)

For a complete automated setup, run:

```bash
cd /path/to/social-analytics-platform/services/data-processing
export GOOGLE_CLOUD_PROJECT="competitor-destroyer"
export REGION="asia-southeast1"
./scripts/setup_complete.sh
```

This will run all setup steps automatically. For manual setup, follow the detailed steps below.

## 🔧 Manual Setup Steps

### Step 1: Enable Required APIs

```bash
./scripts/enable_apis.sh
```

This enables:
- Cloud Run
- Container Registry
- Secret Manager  
- Pub/Sub
- BigQuery
- Cloud Storage
- Cloud Build

### Step 2: Create Service Account

```bash
./scripts/setup_service_account.sh
```

This creates `data-processing-sa@competitor-destroyer.iam.gserviceaccount.com` with permissions:
- BigQuery: dataEditor + jobUser (read/write data + execute queries)
- Cloud Storage: objectViewer (read raw data from GCS)
- Pub/Sub: publisher + subscriber (receive events + publish results)
- Cloud Run: invoker (for push subscriptions)

### Step 3: Create Pub/Sub Topics

```bash
./scripts/create_pubsub_topics.sh
```

Creates topics:
- `social-analytics-data-processing-started`
- `social-analytics-data-processing-completed`
- `social-analytics-data-processing-failed`
- `social-analytics-media-processing-requested`

### Step 4: Create BigQuery Tables

```bash
python scripts/create_bigquery_tables.py
```

Creates:
- `social_analytics.posts` (main analytics table)
- `social_analytics.processing_events` (monitoring table)

### Step 5: Deploy Service

```bash
./scripts/deploy.sh
```

This will:
1. Configure Docker for Google Container Registry
2. Build Docker image for AMD64 architecture (Cloud Run requirement)
3. Push image to GCR
4. Deploy to Cloud Run with proper configuration
5. Test health endpoint
6. Display service URL and test commands

### Step 6: Create Push Subscriptions

```bash
./scripts/create_push_subscriptions.sh
```

Creates push subscription `data-processing-ingestion-events` that:
- Subscribes to `social-analytics-data-ingestion-completed` topic
- Pushes to `/api/v1/events/data-ingestion-completed` endpoint
- Configured with 10-minute ack deadline and 3 retry attempts

## 🧪 Testing the Deployment

### Health Check
```bash
curl https://data-processing-service-url/health
```

Expected response:
```json
{
  "status": "healthy",
  "service": "data-processing",
  "version": "1.0.0",
  "environment": "competitor-destroyer"
}
```

### Manual Test Endpoint
```bash
curl -X POST https://data-processing-service-url/api/v1/test \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'
```

### Test Pub/Sub Integration
The service automatically receives events from the Data Ingestion Service. To test:

1. Trigger a crawl in the Data Ingestion Service
2. Monitor data processing logs:
```bash
gcloud run services logs read data-processing-service --region=asia-southeast1 --limit=20
```

3. Check BigQuery for processed data:
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`competitor-destroyer.social_analytics.posts\` 
   ORDER BY processed_date DESC 
   LIMIT 5"
```

## 📊 Service Configuration

### Environment Variables
- `GOOGLE_CLOUD_PROJECT`: competitor-destroyer
- `BIGQUERY_DATASET`: social_analytics
- `PUBSUB_TOPIC_PREFIX`: social-analytics
- `GCS_BUCKET_RAW_DATA`: social-analytics-raw-data

### Resource Configuration
- **Memory**: 1GB
- **CPU**: 1 vCPU
- **Scaling**: 0-20 instances
- **Timeout**: 300 seconds (5 minutes)
- **Concurrency**: 100 requests per instance
- **Region**: asia-southeast1

## 🔄 Data Flow

1. **Data Ingestion Service** completes crawl and publishes `data-ingestion-completed` event
2. **Pub/Sub** delivers event to data processing service via push subscription
3. **Data Processing Service** receives event, downloads raw data from GCS
4. **Text Processing** transforms data using schema-driven mapping
5. **BigQuery Insertion** stores processed posts in analytics table
6. **Event Publishing** triggers media processing for posts with attachments

## 📈 Monitoring

### View Logs
```bash
gcloud run services logs read data-processing-service --region=asia-southeast1 --limit=50
```

### Check Service Status
```bash
gcloud run services describe data-processing-service --region=asia-southeast1
```

### Monitor Processing Events
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`competitor-destroyer.social_analytics.processing_events\` 
   WHERE event_type = 'data_processing' 
   ORDER BY event_timestamp DESC 
   LIMIT 10"
```

### Check Pub/Sub Subscriptions
```bash
gcloud pubsub subscriptions list --filter="name:data-processing"
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Architecture Mismatch
**Error**: `Container manifest type must support amd64/linux`
**Solution**: Rebuild with `--platform linux/amd64`:
```bash
docker build --platform linux/amd64 -t gcr.io/competitor-destroyer/data-processing:latest .
```

#### 2. Service Account Permissions
**Error**: `403 Forbidden` accessing BigQuery/GCS
**Solution**: Re-run service account setup:
```bash
./scripts/setup_service_account.sh
```

#### 3. Pub/Sub Push Authentication
**Error**: `401 Unauthorized` on push endpoint
**Solution**: Grant Pub/Sub service account permission:
```bash
gcloud run services add-iam-policy-binding data-processing-service \
  --member="serviceAccount:service-366008494339@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --region=asia-southeast1
```

#### 4. BigQuery Table Not Found
**Error**: `Table not found: social_analytics.posts`
**Solution**: Create tables:
```bash
python scripts/create_bigquery_tables.py
```

#### 5. GCS Access Denied
**Error**: `403 Forbidden` downloading from GCS
**Solution**: Verify service account has `storage.objectViewer` permission:
```bash
gcloud projects add-iam-policy-binding competitor-destroyer \
  --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

### Debug Commands
```bash
# Check service account permissions
gcloud projects get-iam-policy competitor-destroyer \
  --flatten="bindings[].members" \
  --filter="bindings.members:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com"

# Test BigQuery access
bq ls --project_id=competitor-destroyer social_analytics

# Test GCS access
gsutil ls gs://social-analytics-raw-data/

# Check Pub/Sub topics
gcloud pubsub topics list --filter="name:social-analytics"
```

## 🔄 Updating the Service

### Quick Update
```bash
# Build new image
docker build --platform linux/amd64 -t gcr.io/competitor-destroyer/data-processing:latest .

# Deploy update
./scripts/deploy.sh
```

### Controlled Deployment
```bash
# Build with version tag
docker build --platform linux/amd64 -t gcr.io/competitor-destroyer/data-processing:v1.1 .
docker push gcr.io/competitor-destroyer/data-processing:v1.1

# Deploy without traffic
gcloud run deploy data-processing-service \
  --image=gcr.io/competitor-destroyer/data-processing:v1.1 \
  --region=asia-southeast1 \
  --no-traffic

# Test new revision, then route traffic
gcloud run services update-traffic data-processing-service \
  --to-latest \
  --region=asia-southeast1
```

## 📚 Integration Testing

### End-to-End Test
1. Trigger crawl in Data Ingestion Service
2. Monitor data processing logs
3. Verify data appears in BigQuery
4. Check media processing events are published

### Manual Event Test
For testing without Data Ingestion Service:
```bash
# Create test GCS file with sample data
echo '[{"post_id": "test-123", "content": "Test post", "date_posted": "2025-07-09T10:00:00Z"}]' > test-data.json
gsutil cp test-data.json gs://social-analytics-raw-data/test/

# Manually trigger processing (would need to simulate Pub/Sub event)
```

## 🎯 Success Criteria

✅ **Deployment Success**:
- Service deployed to Cloud Run
- Health check responds 200 OK
- Pub/Sub subscription created
- BigQuery tables accessible

✅ **Integration Success**:
- Receives events from Data Ingestion Service
- Processes posts within 30 seconds
- Inserts data to BigQuery successfully
- Publishes media processing events

✅ **Performance Success**:
- Handles 3 posts per workflow efficiently
- Memory usage under 500MB
- Processing time under 30 seconds
- 99.9% success rate

## 🔗 Related Documentation

- [Data Processing Service README](README.md)
- [Data Ingestion Service Deployment](../data-ingestion/docs/deployment.md)
- [BigQuery Schema Documentation](docs/bigquery-schema-mapping.md)
- [Architecture Design](docs/architecture-design.md)

## 📞 Support

For issues:
1. Check service health: `curl https://service-url/health`
2. Review logs: `gcloud run services logs read data-processing-service`
3. Verify BigQuery access: `bq ls social_analytics`
4. Test Pub/Sub: `gcloud pubsub subscriptions list`

The Data Processing Service is now ready to receive events from the Data Ingestion Service and process social media data for analytics!