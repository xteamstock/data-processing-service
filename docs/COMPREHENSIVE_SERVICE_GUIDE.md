# Data Processing Service - Comprehensive Service Guide

## 📋 Table of Contents
1. [Service Overview](#service-overview)
2. [Architecture & Design](#architecture--design)
3. [Core Components](#core-components)
4. [Event-Driven Processing](#event-driven-processing)
5. [Data Transformation Pipeline](#data-transformation-pipeline)
6. [Schema-Driven Mapping](#schema-driven-mapping)
7. [Parallel Processing Jobs](#parallel-processing-jobs)
8. [Media Detection & Processing](#media-detection--processing)
9. [BigQuery Integration](#bigquery-integration)
10. [API Endpoints](#api-endpoints)
11. [Error Handling](#error-handling)
12. [Performance Optimization](#performance-optimization)
13. [Monitoring & Observability](#monitoring--observability)
14. [Integration Points](#integration-points)
15. [Security Considerations](#security-considerations)
16. [Configuration Management](#configuration-management)
17. [Best Practices](#best-practices)

## 🎯 Service Overview

The Data Processing Service is the **critical data transformation layer** that converts raw social media data into structured, queryable insights. It serves as the analytics engine between the Data Ingestion Service and BigQuery, performing parallel processing jobs for optimal performance.

### Key Responsibilities
- **Data Transformation**: Convert raw social media data to structured format
- **Schema Mapping**: Apply platform-specific schemas via JSON configuration
- **Analytics Storage**: Insert processed data into BigQuery for real-time queries
- **Processed Data Archive**: Store grouped data in GCS for backup/replay
- **Media Detection**: Identify media content and trigger batch processing events
- **Multi-Platform Support**: Handle Facebook, Instagram, YouTube, TikTok data

### Service Characteristics
- **Event-Driven**: Triggered by Pub/Sub push notifications from Data Ingestion Service
- **Schema-Driven**: Flexible transformation via external JSON configuration files
- **Parallel Processing**: Four concurrent jobs for optimal performance
- **Fault-Tolerant**: Individual job failures don't block other jobs
- **Scalable**: Cloud Run auto-scaling based on event volume

## 🏗️ Architecture & Design

### Component Architecture
```
┌────────────────────────────────────────────────────────────────┐
│                   Data Processing Service                       │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────┐   │
│  │   Flask     │    │    Event     │    │     Text       │   │
│  │   App       │───▶│   Handler    │───▶│   Processor    │   │
│  │  (app.py)   │    │              │    │                │   │
│  └─────────────┘    └──────────────┘    └────────────────┘   │
│         │                   │                     │             │
│         │                   ▼                     ▼             │
│         │           ┌──────────────┐    ┌────────────────┐    │
│         │           │    Schema    │    │     Media      │    │
│         └──────────▶│    Mapper    │    │    Detector    │    │
│                     │              │    │                │    │
│                     └──────────────┘    └────────────────┘    │
│                             │                     │             │
│                    ┌────────┴──────────────┬─────┴────────┐   │
│                    ▼                       ▼              ▼   │
│            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│            │   BigQuery   │  │     GCS      │  │    Batch     │ │
│            │   Handler    │  │   Handler    │  │Media Publisher│ │
│            └──────────────┘  └──────────────┘  └──────────────┘ │
│                    │                  │                  │      │
│                    ▼                  ▼                  ▼      │
│            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│            │   BigQuery   │  │     GCS      │  │    Pub/Sub   │ │
│            │  Analytics   │  │  Processed   │  │    Topics    │ │
│            └──────────────┘  └──────────────┘  └──────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

### Technology Stack
- **Runtime**: Python 3.11 on Cloud Run
- **Framework**: Flask for HTTP endpoints
- **Event System**: Google Cloud Pub/Sub (push model)
- **Analytics Storage**: BigQuery with structured schema
- **File Storage**: Google Cloud Storage for processed data
- **Text Analysis**: TextBlob for sentiment analysis
- **Schema Management**: JSON-based configuration files

### Architecture Patterns

#### 1. Event-Driven Processing
- **Pub/Sub Push Pattern**: Optimized for Cloud Run integration
- **Asynchronous Processing**: Non-blocking event handling
- **Scalable**: Auto-scaling based on event volume

#### 2. Single Source of Truth Pattern
- **Raw Data**: Remains unchanged in GCS for reprocessing capability
- **Processed Data**: Direct BigQuery insertion for analytics
- **Backup**: Processed data stored in GCS for replay

#### 3. Parallel Processing Pattern
- **Four Concurrent Jobs**: GCS upload, BigQuery insert, media detection, batch media
- **Fault Isolation**: Individual job failures don't block others
- **Performance**: Optimal resource utilization

## 🔧 Core Components

### 1. Flask Application (`app.py`)
Main entry point handling Pub/Sub push endpoints and health checks.

**Key Features**:
- Pub/Sub push endpoint for event processing
- Health check endpoint for monitoring
- Test endpoint for manual testing
- Minimal overhead for fast event processing

```python
@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    return event_handler.handle_data_ingestion_completed(request)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'data-processing',
        'version': '1.0.0'
    })
```

### 2. Event Handler (`events/event_handler.py`)
Orchestrates the complete event processing pipeline with four parallel jobs.

**Responsibilities**:
- Extract and validate Pub/Sub messages
- Download raw data from GCS
- Coordinate four parallel processing jobs
- Publish completion/failure events

**Key Methods**:
- `handle_data_ingestion_completed()`: Main event processor
- `_extract_pubsub_event_data()`: Parse Pub/Sub push format
- `_process_data_ingestion_event()`: Orchestrate processing jobs
- `_download_raw_data_from_gcs()`: Retrieve raw data

### 3. Text Processor (`handlers/text_processor.py`)
Transforms raw social media posts into structured data.

**Features**:
- Date-based grouping for analytics optimization
- Multi-platform support via schema mapper
- Data validation and cleaning
- Media detection integration

**Key Methods**:
- `process_posts_for_analytics()`: Main processing pipeline
- `_group_posts_by_date()`: Group posts for optimization
- `get_grouped_data_for_gcs()`: Prepare data for GCS storage

### 4. Schema Mapper (`handlers/schema_mapper.py`)
Provides flexible, configuration-driven data transformation.

**Architecture**:
- External JSON schema files per platform/version
- Field-level transformations and validations
- Computed fields for derived values
- Preprocessing and computation functions

**Key Features**:
- **Field Mappings**: Map source fields to target schema
- **Preprocessing**: Clean, normalize, validate data
- **Computed Fields**: Calculate sentiment, quality scores
- **Multi-Platform**: Separate schemas for each platform

### 5. BigQuery Handler (`handlers/bigquery_handler.py`)
Manages analytics data insertion into BigQuery.

**Features**:
- Direct row insertion (no batching delay)
- Schema validation before insertion
- Processing event logging
- Error handling with detailed logging

**Key Methods**:
- `insert_posts()`: Insert processed posts
- `_validate_posts_schema()`: Ensure schema compliance
- `_ensure_timestamp_format()`: Format dates for BigQuery
- `_log_processing_event()`: Track processing metrics

### 6. GCS Processed Handler (`handlers/gcs_processed_handler.py`)
Stores processed data in GCS with hierarchical structure.

**Features**:
- Date-based directory structure
- UTF-8 encoded JSON storage
- Parallel upload for multiple date groups
- Retry logic with exponential backoff

**Storage Pattern**:
```
processed_data/
└── platform={platform}/
    └── competitor={competitor}/
        └── brand={brand}/
            └── category={category}/
                └── year={year}/
                    └── month={month}/
                        └── day={day}/
                            └── processed_{date}_{timestamp}.json
```

### 7. Media Detector (`handlers/media_detector.py`)
Identifies and prepares media content for processing.

**Features**:
- Attachment type detection (video, image)
- URL validation
- Media metadata extraction
- Batch preparation for media service

**Key Methods**:
- `detect_media_in_posts()`: Scan posts for media
- `extract_media_for_processing_event()`: Prepare event data
- `validate_media_urls()`: Ensure URLs are accessible

### 8. Batch Media Event Publisher (`events/batch_media_event_publisher.py`)
Publishes batch media processing events to downstream services.

**Features**:
- Batch optimization for multiple media items
- Platform-specific media handling
- Structured event format for media service
- Error handling and retry logic

**Event Types**:
- `batch-media-download-requested`: Batch media found for processing
- Media breakdown by type (videos, images, profile images)
- Processing priority and timeout calculation

## 🔄 Event-Driven Processing

### Pub/Sub Push Model
The service uses Pub/Sub push subscriptions for optimal Cloud Run integration.

**Push Endpoint Configuration**:
```yaml
Subscription: data-ingestion-completed-sub
Push Endpoint: https://data-processing-service/api/v1/events/data-ingestion-completed
Authentication: Service account with invoker role
```

### Event Flow
```
Data Ingestion Service → Pub/Sub Topic → Push Subscription → Data Processing Service
                                                                      ↓
                                                              Process Event
                                                                      ↓
                                                    ┌─────────────────┼─────────────────┐
                                                    ▼                 ▼                 ▼
                                            BigQuery Insert    GCS Upload      Media Events
```

### Incoming Event Schema
```json
{
  "message": {
    "data": "base64-encoded-json",
    "messageId": "1234567890",
    "publishTime": "2025-07-11T10:00:00Z"
  }
}
```

**Decoded Event Data**:
```json
{
  "event_type": "data-ingestion-completed",
  "timestamp": "2025-07-11T10:00:00Z",
  "source": "data-ingestion-service",
  "data": {
    "crawl_id": "uuid",
    "snapshot_id": "s_123",
    "gcs_path": "gs://bucket/path/to/raw_data.json",
    "post_count": 150,
    "media_count": 75,
    "platform": "facebook",
    "competitor": "nutifood",
    "brand": "growplus",
    "category": "milk-powder"
  }
}
```

## 🔄 Data Transformation Pipeline

### Processing Steps

#### 1. Raw Data Download
```python
# Download from GCS path in event
raw_data = download_from_gcs("gs://bucket/raw_snapshots/2025/07/11/snapshot_s_123.json")
```

#### 2. Date-Based Grouping
```python
# Group posts by date for optimization
grouped_data = {
  "2025-07-10": [post1, post2, post3],
  "2025-07-11": [post4, post5]
}
```

#### 3. Schema-Driven Transformation
```python
# Apply platform-specific schema
for post in raw_posts:
    transformed_post = schema_mapper.transform_post(
        raw_post=post,
        platform="facebook",
        metadata=crawl_metadata,
        schema_version="1.0.0"
    )
```

#### 4. Media Detection
```python
# Identify posts with media
posts_with_media = media_detector.detect_media_in_posts(processed_posts)
```

### Transformed Data Structure
```json
{
  "id": "post123_crawl456",
  "post_id": "post123",
  "crawl_id": "crawl456",
  "snapshot_id": "s_789",
  "platform": "facebook",
  "competitor": "nutifood",
  "brand": "growplus",
  "category": "milk-powder",
  
  "post_url": "https://facebook.com/post123",
  "post_content": "Check out our new product!",
  "post_type": "Post",
  "date_posted": "2025-07-10T08:00:00Z",
  "crawl_date": "2025-07-11T10:00:00Z",
  "processed_date": "2025-07-11T10:05:00Z",
  "grouped_date": "2025-07-10",
  
  "page_name": "GrowPLUS Official",
  "page_verified": true,
  "page_followers": 50000,
  
  "engagement_metrics": {
    "likes": 1200,
    "comments": 150,
    "shares": 80,
    "reactions": 1350,
    "reactions_by_type": [
      {"type": "LIKE", "count": 1000},
      {"type": "LOVE", "count": 350}
    ]
  },
  
  "content_analysis": {
    "text_length": 25,
    "language": "en",
    "sentiment_score": 0.8,
    "hashtags": ["newproduct", "nutrition"]
  },
  
  "media_metadata": {
    "media_count": 2,
    "has_video": true,
    "has_image": true,
    "attachments": [
      {"type": "video", "url": "https://video.url"},
      {"type": "image", "url": "https://image.url"}
    ],
    "media_processing_requested": true
  },
  
  "processing_metadata": {
    "schema_version": "1.0.0",
    "processing_version": "1.0.0",
    "data_quality_score": 0.95
  }
}
```

## 📊 Schema-Driven Mapping

### External Schema Configuration System
The service uses external JSON files to define data transformation rules, enabling hot updates without code changes.

### Schema File Structure
```
services/data-processing/
├── schemas/
│   ├── facebook_post_schema_v1.json     # Facebook field mappings
│   ├── youtube_video_schema_v1.json     # YouTube field mappings
│   ├── instagram_post_schema_v1.json    # Instagram field mappings
│   └── bigquery_table_schema.json       # BigQuery table structure
└── handlers/
    └── schema_mapper.py                 # Schema processing engine
```

### Field Mapping Types

#### 1. Direct Mapping
```json
{
  "post_id": {
    "source_field": "post_id",
    "target_field": "post_id"
  }
}
```

#### 2. Nested Field Mapping
```json
{
  "page_name": {
    "source_field": "author.name",
    "target_field": "page_name"
  }
}
```

#### 3. Preprocessed Mapping
```json
{
  "post_content": {
    "source_field": "text",
    "target_field": "post_content",
    "preprocessing": ["clean_text"],
    "max_length": 5000
  }
}
```

#### 4. Computed Fields
```json
{
  "sentiment_score": {
    "target_field": "content_analysis.sentiment_score",
    "computation": "calculate_sentiment"
  }
}
```

### Preprocessing Functions
- `clean_text`: Remove special characters, normalize whitespace
- `normalize_hashtags`: Extract and lowercase hashtags
- `parse_iso_timestamp`: Convert to BigQuery timestamp format
- `safe_int/safe_float`: Safe type conversion with defaults
- `parse_reaction_types`: Structure reaction data
- `clean_username`: Normalize user/page names

### Computation Functions
- `calculate_sentiment`: TextBlob sentiment analysis
- `detect_language`: Language detection
- `calculate_data_quality`: Quality score (0-1)
- `sum_reactions_by_type`: Total reactions count
- `check_video/image_attachments`: Media type detection

### Schema Versioning
```json
{
  "schema_version": "1.1.0",
  "previous_version": "1.0.0",
  "changes": [
    "Added new engagement metric: saves",
    "Updated timestamp parsing for timezone handling",
    "Added validation for phone number format"
  ]
}
```

## 🔀 Parallel Processing Jobs

The service performs **four parallel jobs** for each event to optimize performance and enable fault isolation.

### Job 1: GCS Processed Data Upload
**Purpose**: Archive processed data for replay/backup

**Process**:
1. Group processed posts by date
2. Create hierarchical directory structure
3. Upload as UTF-8 JSON files
4. Use parallel uploads for multiple dates

**Benefits**:
- Data backup and recovery
- Historical analysis
- Reprocessing capability
- Audit trail

### Job 2: BigQuery Analytics Insertion
**Purpose**: Enable real-time analytics queries

**Process**:
1. Validate posts against schema
2. Format timestamps and data types
3. Direct row insertion (no batching)
4. Log processing events

**Benefits**:
- Immediate query availability
- No batch processing delays
- Structured analytics
- SQL-based analysis

### Job 3: Media Processing Events (Legacy)
**Purpose**: Individual media processing events

**Process**:
1. Scan posts for media attachments
2. Create individual events per media item
3. Publish to media processing topic

**Note**: Maintained for backward compatibility

### Job 4: Batch Media Processing Events (Primary)
**Purpose**: Efficient batch media processing

**Process**:
1. Collect all media from processed posts
2. Group by type (video, image, profile)
3. Create single batch event
4. Publish to batch media processing topic

**Benefits**:
- Efficient batch processing
- Reduced event overhead
- Optimized resource usage
- Better failure handling

### Job Coordination
```python
# All four jobs run in parallel
job_results = {
    "job1_gcs": gcs_handler.upload_grouped_data(),
    "job2_bigquery": bigquery_handler.insert_posts(),
    "job3_media": event_publisher.publish_media_events(),
    "job4_batch_media": batch_media_publisher.publish_batch_events()
}

# Individual job failures don't block others
if not job_results["job1_gcs"]["success"]:
    logger.error("GCS upload failed, but continuing...")
```

## 🎬 Media Detection & Processing

### Media Types Detected
- **Videos**: Facebook video posts, reels, YouTube videos
- **Images**: Photos, albums, carousel posts
- **Profile Images**: User/page profile pictures
- **Mixed**: Posts with both video and images

### Detection Logic
```python
def detect_media(post):
    attachments = post.get("attachments", [])
    media_info = {
        "has_media": len(attachments) > 0,
        "video_count": sum(1 for a in attachments if a["type"] == "video"),
        "image_count": sum(1 for a in attachments if a["type"] in ["photo", "image"]),
        "media_urls": [a["url"] for a in attachments]
    }
    return media_info
```

### Batch Media Event Schema
```json
{
  "event_type": "batch-media-download-requested",
  "event_id": "crawl123_snap456_batch_media",
  "timestamp": "2025-07-15T06:48:53.534034",
  "version": "2.0",
  "schema_version": "batch-media-v2",
  "data": {
    "batch_summary": {
      "platform": "facebook",
      "total_posts": 50,
      "total_media_items": 125,
      "media_counts": {
        "videos": 25,
        "images": 100
      }
    },
    "media_by_type": {
      "videos": [...],
      "images": [...],
      "profile_images": [...]
    },
    "processing_config": {...},
    "storage_config": {...}
  }
}
```

## 🗂️ BigQuery Integration

### Main Posts Table Schema
```sql
CREATE TABLE `social_analytics.posts` (
  -- Core identifiers
  id STRING,
  post_id STRING,
  crawl_id STRING,
  snapshot_id STRING,
  platform STRING,
  
  -- Post content and metadata
  post_url STRING,
  post_content STRING,
  post_type STRING,
  date_posted TIMESTAMP,
  crawl_date TIMESTAMP,
  processed_date TIMESTAMP,
  grouped_date DATE,
  
  -- User/Page information
  user_url STRING,
  user_username STRING,
  user_profile_id STRING,
  page_name STRING,
  page_category STRING,
  page_verified BOOL,
  page_followers INT64,
  page_likes INT64,
  
  -- Analytics metadata
  competitor STRING,
  brand STRING,
  category STRING,
  
  -- Engagement metrics
  engagement_metrics STRUCT<
    likes INT64,
    comments INT64,
    shares INT64,
    reactions INT64,
    reactions_by_type ARRAY<STRUCT<
      type STRING,
      count INT64
    >>
  >,
  
  -- Content analysis
  content_analysis STRUCT<
    text_length INT64,
    language STRING,
    sentiment_score FLOAT64,
    hashtags ARRAY<STRING>,
    contains_sponsored BOOL
  >,
  
  -- Media metadata
  media_metadata STRUCT<
    media_count INT64,
    has_video BOOL,
    has_image BOOL,
    media_processing_requested BOOL,
    attachments ARRAY<STRUCT<
      id STRING,
      type STRING,
      url STRING,
      attachment_url STRING
    >>
  >,
  
  -- Processing metadata
  processing_metadata STRUCT<
    processing_duration_seconds INT64,
    processing_version STRING,
    source_gcs_path STRING,
    data_quality_score FLOAT64
  >
)
PARTITION BY DATE(date_posted)
CLUSTER BY platform, competitor, brand, page_name;
```

### Data Transformation Features
- **Engagement Metrics**: Structured reactions by type
- **Content Analysis**: Sentiment scoring and language detection
- **Media Metadata**: Attachment tracking for media processing
- **Data Quality**: Quality scoring for analytics confidence
- **Processing Metadata**: Full audit trail

## 📡 API Endpoints

### 1. Health Check
```http
GET /health
```

**Response**:
```json
{
  "status": "healthy",
  "service": "data-processing",
  "version": "1.0.0",
  "environment": "social-analytics-prod"
}
```

### 2. Data Ingestion Event Handler
```http
POST /api/v1/events/data-ingestion-completed
```

**Headers**:
```
Content-Type: application/json
Authorization: Bearer [Pub/Sub JWT token]
```

**Request Body** (Pub/Sub Push):
```json
{
  "message": {
    "data": "eyJldmVudF90eXBlIjoi...",
    "messageId": "1234567890",
    "publishTime": "2025-07-11T10:00:00Z"
  },
  "subscription": "projects/social-analytics-prod/subscriptions/data-ingestion-completed-sub"
}
```

**Response**:
```json
{
  "success": true,
  "processed_posts": 150,
  "processing_duration_seconds": 5.2,
  "jobs_summary": {
    "job1_gcs_upload": {
      "success": true,
      "files_uploaded": 3,
      "total_records": 150
    },
    "job2_bigquery_insert": {
      "success": true,
      "table_id": "social_analytics.posts",
      "rows_inserted": 150
    },
    "job3_media_detection": {
      "posts_with_media": 75,
      "media_event_published": true,
      "total_media_count": 125
    },
    "job4_batch_media": {
      "success": true,
      "media_count": 125,
      "event_id": "crawl123_snap456_batch_media",
      "message_id": "pubsub-message-id-789",
      "media_breakdown": {
        "videos": 25,
        "images": 100,
        "posts_with_media": 75
      }
    }
  }
}
```

### 3. Test Endpoint
```http
POST /api/v1/test
```

**Purpose**: Manual testing without Pub/Sub

## ⚠️ Error Handling

### Error Categories

#### 1. Event Processing Errors
- **Invalid Event Data**: Return 400 with error details
- **Missing Required Fields**: Log and return 400
- **Decoding Errors**: Handle base64/JSON errors

#### 2. Data Access Errors
- **GCS Download Failures**: Retry with backoff
- **Permission Errors**: Log and fail fast
- **Network Timeouts**: Configurable retry logic

#### 3. Processing Errors
- **Schema Validation**: Skip invalid posts, log errors
- **Transformation Errors**: Continue with other posts
- **Computation Errors**: Use default values

#### 4. Storage Errors
- **BigQuery Insertion**: Log errors, don't block
- **GCS Upload**: Retry with exponential backoff
- **Quota Exceeded**: Handle rate limiting

### Error Response Format
```json
{
  "error": "Error description",
  "error_code": "PROCESSING_FAILED",
  "details": {
    "crawl_id": "uuid",
    "stage": "bigquery_insertion",
    "posts_processed": 100,
    "posts_failed": 5
  }
}
```

### Failure Events
Published on critical errors:
```json
{
  "event_type": "processing-failed",
  "data": {
    "crawl_id": "uuid",
    "error": "BigQuery insertion failed",
    "stage": "analytics_storage",
    "partial_success": true,
    "processed_count": 145,
    "failed_count": 5
  }
}
```

## 🚀 Performance Optimization

### Processing Optimizations
1. **Parallel Jobs**: Four jobs run concurrently
2. **Date Grouping**: Optimize storage and queries
3. **Direct Insertion**: No batch delays for BigQuery
4. **Streaming**: Process data as it arrives

### Resource Management
- **Memory**: ~100MB per event processing
- **CPU**: Scales with post count and complexity
- **Network**: Parallel GCS operations
- **Concurrency**: Cloud Run handles multiple events

### Performance Metrics
- **Event Processing**: < 10 seconds for 1000 posts
- **BigQuery Insertion**: ~1000 posts/second
- **GCS Upload**: Parallel uploads for speed
- **Media Detection**: < 1ms per post

### Scaling Characteristics
- **Horizontal**: Cloud Run auto-scaling
- **Event Queue**: Pub/Sub handles backpressure
- **Storage**: BigQuery and GCS scale infinitely
- **Compute**: CPU scales with complexity

## 📈 Monitoring & Observability

### Key Metrics
1. **Processing Metrics**
   - Events processed per minute
   - Average processing duration
   - Success/failure rates
   - Posts processed per event

2. **Job-Specific Metrics**
   - GCS upload success rate
   - BigQuery insertion rate
   - Media events published
   - Job execution times

3. **Error Metrics**
   - Error rate by type
   - Failed post count
   - Retry attempts
   - Timeout occurrences

### Logging Strategy
```python
# Structured logging for monitoring
logger.info({
    "event": "processing_completed",
    "crawl_id": "uuid",
    "duration_seconds": 5.2,
    "posts_processed": 150,
    "jobs": {
        "gcs": "success",
        "bigquery": "success",
        "media": "success",
        "batch_media": "success"
    }
})
```

### Health Indicators
- Service availability
- Pub/Sub subscription health
- BigQuery connectivity
- GCS access status

### Alerting Thresholds
- Processing duration > 30 seconds
- Error rate > 5%
- Failed jobs > 10 per hour
- Memory usage > 80%

## 🔗 Integration Points

### 1. Data Ingestion Service
- **Communication**: Pub/Sub events
- **Trigger**: data-ingestion-completed events
- **Data Transfer**: GCS path reference

### 2. Media Processing Service
- **Communication**: Pub/Sub events
- **Trigger**: batch-media-download-requested events
- **Data Transfer**: Media URLs and metadata

### 3. BigQuery Analytics
- **Direct Integration**: Row insertion
- **Schema**: Structured posts table
- **Query Access**: Real-time analytics

### 4. GCS Storage
- **Raw Data**: Download source data
- **Processed Data**: Upload transformed data
- **Structure**: Hierarchical organization

### 5. Monitoring Service
- **Events**: Processing completed/failed
- **Metrics**: Performance statistics
- **Alerts**: Error notifications

## 🔐 Security Considerations

### Authentication
- **Pub/Sub Push**: JWT token validation
- **Service Account**: Limited permissions
- **IAM Roles**: Least privilege access

### Data Security
- **Encryption at Rest**: GCS and BigQuery
- **Encryption in Transit**: HTTPS only
- **Access Control**: Service-specific accounts

### Compliance
- **Data Retention**: Configurable policies
- **Audit Logging**: All operations logged
- **PII Handling**: No PII processing

## 🎯 Configuration Management

### Environment Variables
```bash
# Required
GOOGLE_CLOUD_PROJECT=social-analytics-prod
BIGQUERY_DATASET=social_analytics
PUBSUB_TOPIC_PREFIX=social-analytics
GCS_BUCKET_RAW_DATA=social-analytics-raw-data

# Optional
SCHEMA_VERSION=1.0.0
BATCH_MEDIA_ENABLED=true
LOG_LEVEL=INFO
```

### Schema Configuration
- **Location**: `schemas/` directory
- **Format**: JSON files per platform
- **Versioning**: Semantic versioning
- **Hot Reload**: Configuration changes without restart

### Service Configuration
- **Memory**: 1GB
- **CPU**: 1 vCPU
- **Instances**: 0-10 (auto-scaling)
- **Timeout**: 300 seconds
- **Concurrency**: 100 requests per instance

## 🎯 Best Practices

### 1. Schema Management
- Version schemas for compatibility
- Document all transformations
- Test schema changes thoroughly
- Maintain backward compatibility

### 2. Error Handling
- Don't let one bad post fail entire batch
- Log detailed error context
- Publish failure events for monitoring
- Implement proper retry logic

### 3. Performance
- Process in parallel where possible
- Use streaming for large datasets
- Monitor resource usage
- Optimize for common cases

### 4. Maintainability
- Keep handlers focused and testable
- Use configuration over code
- Document data flows
- Maintain clear separation of concerns

### 5. Monitoring
- Implement comprehensive logging
- Track all key metrics
- Set up meaningful alerts
- Monitor dependencies

This comprehensive guide provides complete understanding of the Data Processing Service architecture, functionality, and operational characteristics, enabling effective development, deployment, and maintenance of the service.