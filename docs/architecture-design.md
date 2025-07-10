# Data Processing Service - Architecture Design

## 🎯 Service Overview

The Data Processing Service serves as the **critical data transformation layer** between raw social media data and structured analytics. It processes BrightData JSON responses into normalized BigQuery tables optimized for analytics queries.

### Core Responsibilities
1. **Text Processing**: Extract and normalize text content from social media posts
2. **Date-Based Grouping**: Group posts by date for analytics optimization (legacy pattern)
3. **Data Validation**: Ensure data quality and schema compliance
4. **Sentiment Analysis**: Basic sentiment scoring for content analysis
5. **Media Detection**: Identify and catalog media attachments for separate processing
6. **Event Orchestration**: Trigger Media Processing Service for async media handling
7. **Multi-Platform Support**: Unified processing for Facebook, YouTube, Instagram

**Service Boundary**: TEXT processing only - media downloads/ML handled by Media Processing Service

## 🏗️ Architecture Patterns

### 1. Event-Driven Processing
```python
# Pub/Sub Push Pattern for Cloud Run
@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    """
    Handle incoming data-ingestion-completed events from Pub/Sub.
    
    This endpoint receives push notifications from Pub/Sub when
    the Data Ingestion Service completes raw data collection.
    """
    # Verify Pub/Sub authentication
    # Extract event payload
    # Process data asynchronously
    # Return 200 OK immediately
```

### 2. Single Source of Truth Pattern
```python
def process_raw_data(gcs_path: str, metadata: dict):
    """
    Process raw BrightData JSON from GCS (single source of truth).
    
    Raw data remains unchanged in GCS for reprocessing capability.
    Processed data goes directly to BigQuery for analytics.
    """
    # 1. Download raw JSON from GCS
    raw_data = download_from_gcs(gcs_path)
    
    # 2. Parse BrightData response (platform-agnostic)
    posts = parse_brightdata_response(raw_data, metadata['platform'])
    
    # 3. Perform date-based grouping (legacy pattern)
    grouped_posts = group_posts_by_date(posts)
    
    # 4. Process for analytics
    processed_posts = process_posts_for_analytics(grouped_posts, metadata)
    
    # 5. Insert to BigQuery
    insert_to_bigquery(processed_posts)
    
    # 6. Trigger media processing (separate service)
    trigger_media_processing(posts, metadata)
```

### 3. Direct BigQuery Insertion Pattern
```python
def insert_to_bigquery(processed_posts: list, metadata: dict):
    """
    Direct insertion optimized for low volume (3 posts/workflow).
    
    No intermediate storage needed - goes straight to BigQuery
    for immediate analytics availability.
    """
    # Validate against BigQuery schema
    validated_posts = validate_bigquery_schema(processed_posts)
    
    # Insert directly to analytics table
    table_id = f"{project_id}.social_analytics.posts"
    errors = bigquery_client.insert_rows_json(table_id, validated_posts)
    
    if errors:
        raise ProcessingError(f"BigQuery insertion failed: {errors}")
```

## 🔧 Component Design

### 1. Flask Application Structure
```python
# app.py - Main application
from flask import Flask, request, jsonify
from handlers.text_processor import TextProcessor
from handlers.bigquery_handler import BigQueryHandler
from events.event_publisher import EventPublisher
from events.event_handler import EventHandler

app = Flask(__name__)
text_processor = TextProcessor()
bigquery_handler = BigQueryHandler()
event_publisher = EventPublisher()
event_handler = EventHandler()

# Pub/Sub push endpoint
@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    return event_handler.handle_data_ingestion_completed(request)

# Health check
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'data-processing',
        'version': '1.0.0'
    })
```

### 2. Text Processing Handler
```python
# handlers/text_processor.py
import re
from typing import List, Dict, Any
from datetime import datetime
from textblob import TextBlob

class TextProcessor:
    """
    Handles text processing operations for social media content.
    
    Migrated from backup/legacy-monolith/src/core/data_processor.py
    with adaptations for microservices architecture.
    """
    
    def process_posts_for_analytics(self, raw_posts: List[Dict], metadata: Dict) -> List[Dict]:
        """
        Process raw posts for BigQuery analytics table.
        
        Args:
            raw_posts: Raw posts from BrightData
            metadata: Crawl metadata (platform, competitor, etc.)
        
        Returns:
            List of processed posts ready for BigQuery insertion
        """
        processed_posts = []
        
        for post in raw_posts:
            processed_post = {
                'id': post.get('post_id', f"post_{datetime.utcnow().isoformat()}"),
                'crawl_id': metadata.get('crawl_id'),
                'snapshot_id': metadata.get('snapshot_id'),
                'platform': metadata.get('platform'),
                'competitor': metadata.get('competitor'),
                'brand': metadata.get('brand'),
                'category': metadata.get('category'),
                'post_content': self.clean_text(post.get('content', '')),
                'engagement_metrics': self.extract_engagement_metrics(post),
                'posted_date': self.parse_date(post.get('date_posted')),
                'crawl_date': metadata.get('crawl_date'),
                'processed_date': datetime.utcnow().isoformat(),
                'grouped_date': self.extract_grouped_date(post),  # For date-based analytics
                'processing_metadata': self.generate_processing_metadata(post),
                'media_metadata': self.extract_media_metadata(post)
            }
            
            processed_posts.append(processed_post)
        
        return processed_posts
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\.\!\?\,\;\:\-\(\)]', '', text)
        
        return text
    
    def extract_engagement_metrics(self, post: Dict) -> Dict:
        """Extract engagement metrics from post."""
        metrics = {
            'likes': int(post.get('likes', 0)),
            'comments': int(post.get('comments', 0)),
            'shares': int(post.get('shares', 0)),
            'reactions': int(post.get('reactions', 0))
        }
        
        # Add platform-specific metrics
        if metadata.get('platform') == 'youtube':
            metrics['views'] = int(post.get('views', 0))
        
        return metrics
    
    def generate_processing_metadata(self, post: Dict) -> Dict:
        """Generate processing metadata for analytics."""
        content = post.get('content', '')
        
        # Basic sentiment analysis
        sentiment_score = 0.0
        if content:
            try:
                blob = TextBlob(content)
                sentiment_score = blob.sentiment.polarity
            except Exception:
                sentiment_score = 0.0
        
        return {
            'text_length': len(content),
            'language': self.detect_language(content),
            'sentiment_score': sentiment_score,
            'processing_duration_seconds': 0  # Will be calculated
        }
    
    def extract_media_metadata(self, post: Dict) -> Dict:
        """Extract media metadata from post."""
        attachments = post.get('attachments', [])
        
        media_count = len(attachments)
        has_video = any(att.get('type') == 'video' for att in attachments)
        has_image = any(att.get('type') in ['photo', 'image'] for att in attachments)
        
        return {
            'media_count': media_count,
            'has_video': has_video,
            'has_image': has_image,
            'media_processing_requested': media_count > 0
        }
    
    def detect_language(self, text: str) -> str:
        """Detect language of text content."""
        if not text:
            return 'unknown'
        
        try:
            blob = TextBlob(text)
            return blob.detect_language()
        except Exception:
            return 'unknown'
    
    def parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        if not date_str:
            return datetime.utcnow().isoformat()
        
        try:
            # Handle various date formats from BrightData
            # This logic should be migrated from legacy data_processor.py
            if isinstance(date_str, str):
                # Parse common social media date formats
                parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return parsed_date.isoformat()
            return date_str
        except Exception:
            return datetime.utcnow().isoformat()
```

### 3. BigQuery Handler
```python
# handlers/bigquery_handler.py
import logging
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class BigQueryHandler:
    """
    Handles BigQuery operations for analytics data storage.
    
    Optimized for direct insertion of processed social media data.
    """
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    
    def insert_posts(self, processed_posts: List[Dict], metadata: Dict) -> Dict:
        """
        Insert processed posts to BigQuery analytics table.
        
        Args:
            processed_posts: List of processed posts
            metadata: Processing metadata
        
        Returns:
            Dict with insertion results
        """
        table_id = f"{self.project_id}.{self.dataset_id}.posts"
        
        try:
            # Insert rows to BigQuery
            errors = self.client.insert_rows_json(table_id, processed_posts)
            
            if errors:
                logger.error(f"BigQuery insertion errors: {errors}")
                raise BigQueryInsertionError(f"Failed to insert posts: {errors}")
            
            logger.info(f"Successfully inserted {len(processed_posts)} posts to BigQuery")
            
            # Log processing event
            self.log_processing_event(metadata, len(processed_posts), True)
            
            return {
                'success': True,
                'rows_inserted': len(processed_posts),
                'table_id': table_id
            }
            
        except GoogleCloudError as e:
            logger.error(f"BigQuery error: {str(e)}")
            self.log_processing_event(metadata, len(processed_posts), False, str(e))
            raise BigQueryInsertionError(f"BigQuery operation failed: {str(e)}")
    
    def log_processing_event(self, metadata: Dict, post_count: int, success: bool, error_message: str = None):
        """Log processing event for monitoring."""
        event_table_id = f"{self.project_id}.{self.dataset_id}.processing_events"
        
        event_record = {
            'event_id': f"proc_{datetime.utcnow().isoformat()}",
            'crawl_id': metadata.get('crawl_id'),
            'event_type': 'data_processing',
            'event_timestamp': datetime.utcnow().isoformat(),
            'processing_duration_seconds': metadata.get('processing_duration', 0),
            'post_count': post_count,
            'media_count': metadata.get('media_count', 0),
            'success': success,
            'error_message': error_message
        }
        
        try:
            self.client.insert_rows_json(event_table_id, [event_record])
        except Exception as e:
            logger.warning(f"Failed to log processing event: {str(e)}")
    
    def validate_schema(self, processed_posts: List[Dict]) -> List[Dict]:
        """
        Validate processed posts against BigQuery schema.
        
        Args:
            processed_posts: List of processed posts
        
        Returns:
            List of validated posts
        """
        validated_posts = []
        
        for post in processed_posts:
            # Ensure required fields exist
            validated_post = {
                'id': str(post.get('id', '')),
                'crawl_id': str(post.get('crawl_id', '')),
                'snapshot_id': str(post.get('snapshot_id', '')),
                'platform': str(post.get('platform', '')),
                'competitor': str(post.get('competitor', '')),
                'brand': str(post.get('brand', '')),
                'category': str(post.get('category', '')),
                'post_content': str(post.get('post_content', '')),
                'engagement_metrics': post.get('engagement_metrics', {}),
                'posted_date': post.get('posted_date'),
                'crawl_date': post.get('crawl_date'),
                'processed_date': post.get('processed_date'),
                'processing_metadata': post.get('processing_metadata', {}),
                'media_metadata': post.get('media_metadata', {})
            }
            
            validated_posts.append(validated_post)
        
        return validated_posts

class BigQueryInsertionError(Exception):
    """Custom exception for BigQuery insertion errors."""
    pass
```

### 4. Event Handler
```python
# events/event_handler.py
import json
import logging
from typing import Dict, Any
from flask import Request
from handlers.text_processor import TextProcessor
from handlers.bigquery_handler import BigQueryHandler
from events.event_publisher import EventPublisher

logger = logging.getLogger(__name__)

class EventHandler:
    """
    Handles incoming Pub/Sub events for data processing.
    
    Implements Pub/Sub push pattern for Cloud Run integration.
    """
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.bigquery_handler = BigQueryHandler()
        self.event_publisher = EventPublisher()
    
    def handle_data_ingestion_completed(self, request: Request) -> Dict[str, Any]:
        """
        Handle data-ingestion-completed events from Pub/Sub.
        
        Args:
            request: Flask request object with Pub/Sub message
        
        Returns:
            Dict with processing results
        """
        try:
            # Verify Pub/Sub authentication
            if not self.verify_pubsub_auth(request):
                logger.warning("Unauthorized Pub/Sub request")
                return {'error': 'Unauthorized'}, 401
            
            # Extract event data
            event_data = self.extract_event_data(request)
            
            if not event_data:
                logger.error("Invalid event data")
                return {'error': 'Invalid event data'}, 400
            
            # Process the event
            result = self.process_ingestion_event(event_data)
            
            # Return success response
            return {
                'success': True,
                'processed_posts': result['processed_posts'],
                'media_processing_requested': result['media_processing_requested']
            }
            
        except Exception as e:
            logger.error(f"Error processing data ingestion event: {str(e)}")
            return {'error': str(e)}, 500
    
    def verify_pubsub_auth(self, request: Request) -> bool:
        """Verify Pub/Sub authentication headers."""
        # Implement Pub/Sub authentication verification
        # Check for required headers and tokens
        return True  # Simplified for now
    
    def extract_event_data(self, request: Request) -> Dict:
        """Extract event data from Pub/Sub message."""
        try:
            # Parse Pub/Sub message format
            envelope = request.get_json()
            
            if not envelope:
                logger.error("No JSON body in request")
                return None
            
            # Extract message data
            message = envelope.get('message', {})
            data = message.get('data', '')
            
            if not data:
                logger.error("No data in Pub/Sub message")
                return None
            
            # Decode base64 data
            import base64
            decoded_data = base64.b64decode(data).decode('utf-8')
            event_data = json.loads(decoded_data)
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error extracting event data: {str(e)}")
            return None
    
    def process_ingestion_event(self, event_data: Dict) -> Dict:
        """Process data ingestion completed event."""
        start_time = datetime.utcnow()
        
        try:
            # Extract event information
            crawl_id = event_data['data']['crawl_id']
            snapshot_id = event_data['data']['snapshot_id']
            gcs_path = event_data['data']['gcs_path']
            
            logger.info(f"Processing ingestion event for crawl {crawl_id}")
            
            # Download and process raw data
            raw_data = self.download_from_gcs(gcs_path)
            
            # Extract metadata
            metadata = {
                'crawl_id': crawl_id,
                'snapshot_id': snapshot_id,
                'platform': event_data['data'].get('platform'),
                'competitor': event_data['data'].get('competitor'),
                'brand': event_data['data'].get('brand'),
                'category': event_data['data'].get('category'),
                'crawl_date': event_data['timestamp'],
                'gcs_path': gcs_path
            }
            
            # Process posts for analytics
            processed_posts = self.text_processor.process_posts_for_analytics(raw_data, metadata)
            
            # Calculate processing duration
            processing_duration = (datetime.utcnow() - start_time).total_seconds()
            metadata['processing_duration'] = processing_duration
            
            # Insert to BigQuery
            bigquery_result = self.bigquery_handler.insert_posts(processed_posts, metadata)
            
            # Publish data processing completed event
            self.event_publisher.publish_data_processing_completed(
                crawl_id=crawl_id,
                snapshot_id=snapshot_id,
                processed_posts=len(processed_posts),
                bigquery_table=bigquery_result['table_id']
            )
            
            # Check for media processing needs
            media_processing_requested = False
            posts_with_media = [p for p in processed_posts if p['media_metadata']['media_count'] > 0]
            
            if posts_with_media:
                media_processing_requested = True
                self.event_publisher.publish_media_processing_requested(
                    crawl_id=crawl_id,
                    snapshot_id=snapshot_id,
                    posts_with_media=posts_with_media
                )
            
            logger.info(f"Successfully processed {len(processed_posts)} posts for crawl {crawl_id}")
            
            return {
                'processed_posts': len(processed_posts),
                'media_processing_requested': media_processing_requested,
                'processing_duration': processing_duration
            }
            
        except Exception as e:
            logger.error(f"Error processing ingestion event: {str(e)}")
            
            # Publish processing failed event
            self.event_publisher.publish_processing_failed(
                crawl_id=event_data['data'].get('crawl_id'),
                error_message=str(e)
            )
            
            raise
    
    def download_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Download raw data from GCS."""
        try:
            from google.cloud import storage
            
            storage_client = storage.Client()
            
            # Parse GCS path
            bucket_name = gcs_path.split('/')[2]
            object_name = '/'.join(gcs_path.split('/')[3:])
            
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Download and parse JSON
            json_data = blob.download_as_text()
            raw_data = json.loads(json_data)
            
            return raw_data
            
        except Exception as e:
            logger.error(f"Error downloading from GCS: {str(e)}")
            raise
```

### 5. Event Publisher
```python
# events/event_publisher.py
import os
import json
import logging
from typing import Dict, List, Any
from datetime import datetime
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class EventPublisher:
    """
    Publisher for sending events to other microservices.
    
    Adapted from data-ingestion service event publisher.
    """
    
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.topic_prefix = os.getenv('PUBSUB_TOPIC_PREFIX', 'social-analytics')
    
    def publish_data_processing_completed(self, crawl_id: str, snapshot_id: str, 
                                        processed_posts: int, bigquery_table: str) -> bool:
        """Publish data processing completed event."""
        event_data = {
            'crawl_id': crawl_id,
            'snapshot_id': snapshot_id,
            'processed_posts': processed_posts,
            'bigquery_table': bigquery_table,
            'status': 'completed'
        }
        
        return self.publish('data-processing-completed', event_data)
    
    def publish_media_processing_requested(self, crawl_id: str, snapshot_id: str, 
                                         posts_with_media: List[Dict]) -> bool:
        """Publish media processing requested event."""
        event_data = {
            'crawl_id': crawl_id,
            'snapshot_id': snapshot_id,
            'posts_with_media': posts_with_media,
            'media_count': sum(p['media_metadata']['media_count'] for p in posts_with_media),
            'status': 'requested'
        }
        
        return self.publish('media-processing-requested', event_data)
    
    def publish_processing_failed(self, crawl_id: str, error_message: str) -> bool:
        """Publish processing failed event."""
        event_data = {
            'crawl_id': crawl_id,
            'error_message': error_message,
            'status': 'failed'
        }
        
        return self.publish('data-processing-failed', event_data)
    
    def publish(self, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Publish event to Pub/Sub topic."""
        try:
            topic_name = f"{self.topic_prefix}-{event_type}"
            topic_path = self.publisher.topic_path(self.project_id, topic_name)
            
            message = {
                'event_type': event_type,
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'data-processing-service',
                'data': event_data
            }
            
            message_data = json.dumps(message).encode('utf-8')
            
            logger.info(f"Publishing event: {event_type} to {topic_name}")
            
            future = self.publisher.publish(topic_path, message_data)
            message_id = future.result()
            
            logger.info(f"Event published successfully: {message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing event {event_type}: {str(e)}")
            return False
```

## 🔄 Data Processing Flow

### 1. Event Reception
```
Pub/Sub → Push Notification → Flask Endpoint → Event Handler → Processing Pipeline
```

### 2. Data Processing Pipeline
```
GCS Raw Data → Text Processing → Validation → BigQuery Insertion → Event Publishing
```

### 3. Event Publishing
```
Processing Complete → Pub/Sub Events → [Media Processing Service, Analytics Service, Monitoring Service]
```

## 📊 Performance Characteristics

### Processing Targets
- **Text Processing**: <30 seconds for 3 posts
- **BigQuery Insertion**: <5 seconds per batch
- **Event Publishing**: <1 second per event
- **Memory Usage**: <500MB per processing job

### Scalability Features
- **Horizontal Scaling**: 0-20 Cloud Run instances
- **Concurrent Processing**: 100 requests per instance
- **Auto-scaling**: Based on Pub/Sub message queue depth
- **Resource Optimization**: Efficient memory and CPU usage

## 🛡️ Error Handling & Resilience

### Retry Mechanisms
- **Pub/Sub Retries**: 3 attempts with exponential backoff
- **BigQuery Retries**: 2 attempts for transient errors
- **GCS Download Retries**: 3 attempts with backoff

### Circuit Breakers
- **BigQuery Circuit Breaker**: Open on 5 consecutive failures
- **GCS Circuit Breaker**: Open on 3 consecutive failures
- **Event Publishing Circuit Breaker**: Open on 10 consecutive failures

### Graceful Degradation
- **Partial Processing**: Continue processing if some posts fail
- **Error Isolation**: Individual post failures don't stop batch
- **Fallback Mechanisms**: Default values for missing data

## 🔍 Monitoring & Observability

### Metrics Collection
```python
# Processing metrics
processing_duration_histogram = Histogram('processing_duration_seconds')
processing_success_counter = Counter('processing_success_total')
processing_error_counter = Counter('processing_error_total')

# BigQuery metrics
bigquery_insertion_duration = Histogram('bigquery_insertion_duration_seconds')
bigquery_rows_inserted = Counter('bigquery_rows_inserted_total')

# Event publishing metrics
event_publishing_duration = Histogram('event_publishing_duration_seconds')
events_published = Counter('events_published_total')
```

### Health Checks
```python
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'data-processing',
        'version': '1.0.0',
        'dependencies': {
            'bigquery': check_bigquery_health(),
            'gcs': check_gcs_health(),
            'pubsub': check_pubsub_health()
        }
    })
```

## 🔐 Security Considerations

### Authentication
- **Pub/Sub Authentication**: Verify push subscription tokens
- **Service Account**: Minimal permissions for required operations
- **API Security**: Rate limiting and input validation

### Data Protection
- **Encryption**: Data encrypted in transit and at rest
- **Access Control**: Role-based access to BigQuery and GCS
- **Audit Logging**: Complete audit trail for all operations

## 🚀 Deployment Architecture

### Cloud Run Configuration
```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: data-processing-service
  annotations:
    run.googleapis.com/ingress: all
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "0"
        autoscaling.knative.dev/maxScale: "20"
        run.googleapis.com/cpu-throttling: "false"
        run.googleapis.com/memory: "1Gi"
        run.googleapis.com/cpu: "1000m"
    spec:
      containerConcurrency: 100
      containers:
      - image: gcr.io/competitor-destroyer/data-processing:latest
        ports:
        - containerPort: 8080
        env:
        - name: GOOGLE_CLOUD_PROJECT
          value: "competitor-destroyer"
        - name: BIGQUERY_DATASET
          value: "social_analytics"
        - name: PUBSUB_TOPIC_PREFIX
          value: "social-analytics"
        resources:
          limits:
            cpu: "1000m"
            memory: "1Gi"
```

This architecture design provides a robust, scalable, and maintainable foundation for the Data Processing Service that seamlessly integrates with the existing microservices ecosystem.