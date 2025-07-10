# Data Processing Service - Deployment Master Plan

## 🎯 Executive Summary

The Data Processing Service performs **dual processing jobs** on raw BrightData JSON data:
1. **Data Grouping & GCS Storage**: Groups records by date/platform/competitor/brand and stores in `social-analytics-processed-data` bucket
2. **BigQuery Analytics**: Transforms individual records following Facebook post schema and inserts to BigQuery
3. **Media Event Publishing**: Detects media attachments and publishes events to trigger media processing

### Key Design Decisions

1. **Dual Output Strategy**: Both processed GCS files AND BigQuery analytics tables
2. **Legacy Grouping Pattern**: Preserves date-based grouping from backup/legacy-monolith
3. **Schema-Driven Transformation**: Uses facebook_post_schema_v1.json for consistent BigQuery mapping
4. **Media Detection**: Identifies video_url and image URLs to trigger media processing events
5. **Event-Driven Architecture**: Seamless integration with existing microservices

## 🏗️ Architecture Overview

### Data Flow
```
Data Ingestion Service → Pub/Sub (data-ingestion-completed) → Data Processing Service 
                                                                        ↓
                                                      ┌─────────────────────────────────┐
                                                      │  Data Processing Service        │
                                                      │  ┌─────────────────────────────┐ │
                                                      │  │ Job 1: Group & Store to GCS│ │
                                                      │  └─────────────────────────────┘ │
                                                      │  ┌─────────────────────────────┐ │  
                                                      │  │ Job 2: Transform & BigQuery │ │
                                                      │  └─────────────────────────────┘ │
                                                      │  ┌─────────────────────────────┐ │
                                                      │  │ Job 3: Detect Media & Publish│ │
                                                      │  └─────────────────────────────┘ │
                                                      └─────────────────────────────────┘
                                                                        ↓
                                        ┌─────────────────────────────────────────────────────┐
                                        │                     Outputs                         │
                                        ├─────────────────────────────────────────────────────┤
                                        │ 1. GCS: social-analytics-processed-data/           │
                                        │    └─ raw_data/platform=X/competitor=Y/brand=Z/   │
                                        │       category=W/year=YYYY/month=MM/day=DD/        │
                                        │ 2. BigQuery: social_analytics.posts table         │
                                        │ 3. Pub/Sub: media-processing-requested events     │
                                        └─────────────────────────────────────────────────────┘
```

### Service Scope

The Data Processing service handles:
- **Raw JSON Parsing**: Downloads from data-ingestion GCS paths (raw_snapshots/)
- **Data Grouping**: Groups posts by date following legacy pattern from backup/legacy-monolith  
- **GCS Processed Storage**: Uploads grouped data to social-analytics-processed-data bucket
- **Schema Transformation**: Maps Facebook posts using schemas/facebook_post_schema_v1.json
- **BigQuery Analytics**: Direct insertion to social_analytics.posts table
- **Media Detection**: Identifies attachments.video_url (videos) and attachments.url (images)
- **Event Publishing**: Publishes media-processing-requested events when media found
- **Multi-platform Support**: Extensible for Facebook, YouTube, Instagram schemas

### Core Functionality
- **Input**: Raw BrightData JSON from data-ingestion service (raw_snapshots/)
- **Processing**: Data grouping, schema transformation, media detection, sentiment analysis
- **Outputs**: 
  - Processed GCS files (social-analytics-processed-data bucket)
  - BigQuery analytics data (social_analytics.posts table)
  - Media processing events (Pub/Sub)

## 🚀 Implementation Strategy

### Phase 1: Current Implementation Analysis & Fixes
**Status**: Examining existing implementation for JSON parsing issues and missing functionality

**Current Issues Identified**:
1. **Missing GCS Processed Data Upload**: Service only does BigQuery insertion, missing Job 1 (group & store to GCS)
2. **Media Detection Incomplete**: Need to implement proper media attachment detection and event publishing
3. **JSON Parsing Issues**: Previous parsing problems need investigation and fixes
4. **Schema Validation**: Ensure facebook_post_schema_v1.json mapping works with supposed_raw_data.json sample

**Required Fixes**:
1. **Add GCS Grouping Handler**: Extract and implement legacy grouping pattern from backup/legacy-monolith/src/storage/gcs_uploader.py
2. **Add Media Detection**: Implement attachments.video_url and attachments.url detection logic
3. **Fix JSON Parsing**: Resolve Unicode and record preservation issues
4. **Validate Schema Mapping**: Test with sample data to ensure correct BigQuery transformation

### Phase 2: Enhanced Implementation (After fixes)
1. **GCS Integration**: Implement social-analytics-processed-data bucket uploads with proper hierarchical structure
2. **Media Event Publishing**: Implement proper media-processing-requested event publishing
3. **End-to-End Testing**: Validate complete pipeline with sample data
4. **Performance Optimization**: Ensure <30 second processing target for dual output jobs

## 📊 Service Specifications

### Cloud Run Configuration
- **Memory**: 1GB (text processing optimized)
- **CPU**: 1 vCPU
- **Scaling**: 0-20 instances (higher than ingestion for burst processing)
- **Timeout**: 300 seconds (5 minutes - sufficient for text processing)
- **Concurrency**: 100 (can handle multiple events simultaneously)

### Resource Requirements
- **GCS Buckets**: 
  - Read access to `social-analytics-raw-data` (input: raw_snapshots/)
  - Write access to `social-analytics-processed-data` (output: processed grouped data)
- **BigQuery**: `social_analytics.posts` table for analytics data
- **Pub/Sub**: 
  - Subscription to `social-analytics-data-ingestion-completed` 
  - Publish to `social-analytics-media-processing-requested`
- **Service Account**: `data-processing-sa` with proper GCS and BigQuery permissions

## 🛠️ Implementation Tasks

### Task 1: Add GCS Processed Data Handler
**Reference**: `backup/legacy-monolith/src/storage/gcs_uploader.py`

**Required Implementation**:
1. Create `handlers/gcs_processed_handler.py` 
2. Extract legacy `upload_grouped_data()` method
3. Implement hierarchical path structure: `raw_data/platform=X/competitor=Y/brand=Z/category=W/year=YYYY/month=MM/day=DD/`
4. Use proper JSON encoding with `ensure_ascii=False` for Unicode preservation
5. Group data by date using existing `_group_posts_by_date()` logic

### Task 2: Enhance Media Detection Logic
**Reference**: Sample data in `supposed_raw_data.json`

**Required Implementation**:
1. Detect video attachments: `attachments[].video_url` field presence
2. Detect image attachments: `attachments[].url` field presence (type="Photo" or "Image")
3. Create media processing event payload with attachment details
4. Publish to `social-analytics-media-processing-requested` topic

### Task 3: Fix JSON Parsing Issues
**Investigation Focus**:
1. Compare `result_raw_data.json` (2 records) vs `supposed_raw_data.json` (3 records)
2. Identify why records are being lost during processing
3. Fix Unicode encoding issues in content fields
4. Validate schema mapping preserves all Facebook post fields

### Task 4: Update Event Handler Integration
**Required Changes**:
1. Modify `events/event_handler.py` to call both GCS upload and BigQuery insertion
2. Ensure proper error handling for dual-output jobs
3. Update event publishing logic for media processing requests
4. Add processing duration tracking for performance monitoring

## 🔧 Technical Implementation

### Service Structure
```
services/data-processing/
├── app.py                      # Flask app with Pub/Sub push endpoint
├── handlers/
│   ├── text_processor.py       # Core text processing logic
│   ├── bigquery_handler.py     # Direct BigQuery operations
│   └── validation_handler.py   # Data validation
├── events/
│   ├── event_publisher.py      # Event publishing to other services
│   └── event_handler.py        # Incoming event processing
├── models/
│   └── data_models.py          # BigQuery schema models
├── docs/
│   ├── deployment-master-plan.md
│   ├── architecture-design.md
│   └── implementation-guide.md
└── tests/
    └── test_processing.py
```

### Key Components

#### 1. Pub/Sub Push Endpoint
```python
@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    """Process data-ingestion-completed events via Pub/Sub push"""
    # Verify Pub/Sub authentication
    # Extract event data
    # Process raw data from GCS
    # Insert to BigQuery
    # Publish completion events
```

#### 2. Text Processing Pipeline
```python
def process_raw_data(gcs_path, metadata):
    """Process raw BrightData JSON for text analytics"""
    # 1. Download raw JSON from GCS
    # 2. Parse BrightData response format (platform-agnostic)
    # 3. Extract text content, engagement metrics
    # 4. Perform date-based grouping (legacy pattern)
    # 5. Perform sentiment analysis
    # 6. Structure for BigQuery insertion
    # 7. Identify media files for separate processing
    # 8. Publish media processing events
```

#### 3. BigQuery Direct Insertion
```python
def insert_to_bigquery(processed_posts, metadata):
    """Direct insertion optimized for low volume"""
    # 1. Validate against BigQuery schema
    # 2. Insert rows to analytics.posts table
    # 3. Handle insertion errors
    # 4. Log processing statistics
```

## 📋 Deployment Steps

### Prerequisites
- Data Ingestion Service deployed and functional
- BigQuery analytics tables created
- Pub/Sub topics configured
- Service account with proper permissions

### Step 1: Environment Setup
```bash
# Create service account
gcloud iam service-accounts create data-processing-sa \
    --description="Service account for Data Processing Service" \
    --display-name="Data Processing SA"

# Grant permissions
gcloud projects add-iam-policy-binding competitor-destroyer \
    --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding competitor-destroyer \
    --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding competitor-destroyer \
    --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding competitor-destroyer \
    --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"
```

### Step 2: BigQuery Schema Setup
```sql
-- Create analytics posts table
CREATE TABLE `competitor-destroyer.social_analytics.posts` (
  id STRING,
  crawl_id STRING,
  snapshot_id STRING,
  platform STRING,
  competitor STRING,
  brand STRING,
  category STRING,
  post_content STRING,
  engagement_metrics STRUCT<
    likes INT64,
    comments INT64,
    shares INT64,
    reactions INT64
  >,
  posted_date TIMESTAMP,
  crawl_date TIMESTAMP,
  processed_date TIMESTAMP,
  processing_metadata STRUCT<
    text_length INT64,
    language STRING,
    sentiment_score FLOAT64,
    processing_duration_seconds INT64
  >,
  media_metadata STRUCT<
    media_count INT64,
    has_video BOOL,
    has_image BOOL,
    media_processing_requested BOOL
  >
)
PARTITION BY DATE(posted_date)
CLUSTER BY platform, competitor, brand;

-- Create processing events table for monitoring
CREATE TABLE `competitor-destroyer.social_analytics.processing_events` (
  event_id STRING,
  crawl_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  processing_duration_seconds INT64,
  post_count INT64,
  media_count INT64,
  success BOOL,
  error_message STRING
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY event_type, success;
```

### Step 3: Pub/Sub Configuration
```bash
# Create additional topics for data processing
gcloud pubsub topics create social-analytics-data-processing-started
gcloud pubsub topics create social-analytics-data-processing-completed
gcloud pubsub topics create social-analytics-media-processing-requested

# Create push subscription for data processing service
gcloud pubsub subscriptions create data-processing-ingestion-events \
    --topic=social-analytics-data-ingestion-completed \
    --push-endpoint=https://data-processing-service-URL/api/v1/events/data-ingestion-completed \
    --ack-deadline=600 \
    --max-delivery-attempts=3
```

### Step 4: Service Deployment
```bash
# Build and deploy
cd services/data-processing
docker build --platform linux/amd64 -t gcr.io/competitor-destroyer/data-processing:latest .
docker push gcr.io/competitor-destroyer/data-processing:latest

# Deploy to Cloud Run
gcloud run deploy data-processing-service \
    --image=gcr.io/competitor-destroyer/data-processing:latest \
    --service-account=data-processing-sa@competitor-destroyer.iam.gserviceaccount.com \
    --region=asia-southeast1 \
    --allow-unauthenticated \
    --memory=1Gi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=20 \
    --timeout=300 \
    --concurrency=100 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=competitor-destroyer,BIGQUERY_DATASET=social_analytics,GCS_BUCKET_RAW_DATA=social-analytics-raw-data,PUBSUB_TOPIC_PREFIX=social-analytics" \
    --port=8080
```

### Step 5: Update Pub/Sub Subscription
```bash
# Update push subscription with deployed service URL
SERVICE_URL=$(gcloud run services describe data-processing-service --region=asia-southeast1 --format="value(status.url)")

gcloud pubsub subscriptions update data-processing-ingestion-events \
    --push-endpoint=${SERVICE_URL}/api/v1/events/data-ingestion-completed
```

## 🧪 Testing Strategy

### Unit Tests
- Text processing logic validation
- BigQuery insertion formatting
- Event publishing verification
- Error handling scenarios

### Integration Tests
- End-to-end data ingestion → processing pipeline
- Pub/Sub event flow validation
- BigQuery data integrity checks
- Performance benchmarking (<30 second target)

### Load Testing
- Multiple concurrent event processing
- Large dataset handling (50+ posts)
- Memory and CPU utilization monitoring
- Error rate under load

## 📊 Monitoring & Observability

### Key Metrics
- **Processing Time**: Target <30 seconds for text processing
- **Success Rate**: Target >99% for event processing
- **BigQuery Insertion Rate**: Target <5 seconds per batch
- **Event Publishing Latency**: Target <1 second

### Alerting Configuration
```bash
# Create alerting policy for processing failures
gcloud alpha monitoring policies create \
    --policy-from-file=monitoring/processing-alerts.yaml
```

### Logging Strategy
- Structured JSON logging for all processing events
- BigQuery audit trail for data lineage
- Error tracking with context preservation
- Performance metrics collection

## 🔄 CI/CD Pipeline

### Automated Testing
- Unit tests on every commit
- Integration tests on PR
- Performance regression testing
- Security vulnerability scanning

### Deployment Pipeline
```yaml
# .github/workflows/data-processing-deploy.yml
name: Deploy Data Processing Service
on:
  push:
    branches: [main]
    paths: ['services/data-processing/**']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Cloud Run
        run: |
          gcloud run deploy data-processing-service \
            --source=services/data-processing \
            --region=asia-southeast1
```

## 🚨 Risk Mitigation

### Technical Risks
1. **BigQuery Insertion Failures**: Implement retry logic with exponential backoff
2. **Memory Leaks**: Monitor memory usage and implement cleanup
3. **Processing Timeouts**: Optimize for <30 second processing time
4. **Event Ordering**: Handle out-of-order events gracefully

### Operational Risks
1. **Service Downtime**: Implement health checks and auto-scaling
2. **Data Loss**: Ensure event acknowledgment only after successful processing
3. **Cost Overruns**: Monitor BigQuery slot usage and optimize queries
4. **Security**: Implement proper authentication and authorization

## 📈 Performance Optimization

### Text Processing
- Parallel processing for multiple posts
- Efficient JSON parsing with streaming
- Minimal memory footprint
- Language detection optimization

### BigQuery Integration
- Batch insertion for multiple posts
- Proper data type optimization
- Clustering and partitioning utilization
- Query result caching

### Resource Management
- Auto-scaling based on queue depth
- Memory usage monitoring
- CPU utilization optimization
- Connection pooling

## 🔮 Future Enhancements

### Phase 2 Features
1. **Advanced Analytics**: Trend analysis, sentiment tracking
2. **Machine Learning**: Auto-categorization, anomaly detection
3. **Real-time Processing**: Streaming analytics for live data
4. **Data Quality**: Automated data validation and cleansing

### Scalability Improvements
1. **Horizontal Scaling**: Multi-region deployment
2. **Caching Layer**: Redis for frequent queries
3. **Database Optimization**: BigQuery performance tuning
4. **Event Sourcing**: Complete audit trail with event replay

## 📝 Success Criteria

### Technical Metrics
- ✅ Process 3 posts in <30 seconds
- ✅ 99.9% success rate for event processing
- ✅ <1 second event publishing latency
- ✅ <5 seconds BigQuery insertion time

### Business Metrics
- ✅ Complete analytics pipeline operational
- ✅ Real-time data processing capability
- ✅ Cost-effective processing (<$10/day)
- ✅ Scalable to 100+ posts/day

### Operational Metrics
- ✅ Zero-downtime deployments
- ✅ Automated monitoring and alerting
- ✅ Complete error handling and recovery
- ✅ Comprehensive logging and debugging

---

## 🎯 Next Steps

1. **Review and Approve**: Technical review of architecture and deployment plan
2. **Implementation**: Begin Phase 1 development following this plan
3. **Testing**: Comprehensive testing at each phase
4. **Deployment**: Gradual rollout with monitoring
5. **Optimization**: Performance tuning and scalability improvements

This deployment plan provides a comprehensive roadmap for implementing the Data Processing Service as a production-ready microservice that seamlessly integrates with the existing event-driven architecture.