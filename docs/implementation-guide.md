# Data Processing Service - Implementation Guide

## 🎯 Implementation Strategy

This guide provides step-by-step instructions for implementing the Data Processing Service, including **legacy code migration** from `backup/legacy-monolith/` and **Pub/Sub push configuration** for Cloud Run.

### Key Implementation Principles

1. **Migrate Core Logic**: Extract text processing from `backup/legacy-monolith/src/core/data_processor.py`
2. **Preserve Date Grouping**: Maintain legacy date-based grouping pattern for analytics
3. **Multi-Platform Support**: Unified processing for Facebook, YouTube, Instagram
4. **Clear Service Boundaries**: TEXT processing only - media handled by separate service
5. **Pub/Sub Push Pattern**: Use push subscriptions for serverless Cloud Run integration
6. **Direct BigQuery Insertion**: Optimize for current 3 posts/workflow volume
7. **Event-Driven Architecture**: Seamless integration with existing services

## 📋 Phase 1: Core Implementation (Week 1)

### Step 1: Service Structure Setup
```bash
cd /Users/tranquocbao/crawlerX/social-analytics-platform/services/data-processing

# Create required directories
mkdir -p handlers events models tests

# Create main application files
touch app.py
touch handlers/__init__.py
touch handlers/text_processor.py
touch handlers/bigquery_handler.py
touch handlers/validation_handler.py
touch events/__init__.py
touch events/event_publisher.py
touch events/event_handler.py
touch models/__init__.py
touch models/data_models.py
```

### Step 2: Extract Core Text Processing Logic

#### 2.1 Migrate from Legacy Data Processor
```python
# handlers/text_processor.py
# MIGRATE FROM: backup/legacy-monolith/src/core/data_processor.py

import re
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from textblob import TextBlob

logger = logging.getLogger(__name__)

class TextProcessor:
    """
    Text processing logic migrated from legacy data_processor.py
    
    MIGRATION SOURCE: backup/legacy-monolith/src/core/data_processor.py
    - process_and_group_data() method [TEXT PROCESSING ONLY]
    - Data validation and cleaning methods
    - Date parsing and normalization
    """
    
    def __init__(self):
        self.language_detector = TextBlob
        
    def process_posts_for_analytics(self, raw_data: List[Dict], metadata: Dict) -> List[Dict]:
        """
        Process raw BrightData posts for BigQuery analytics.
        
        MIGRATED FROM: DataProcessor.process_and_group_data()
        CHANGES: 
        - Removed media processing (moved to separate service)
        - Added BigQuery schema formatting
        - Enhanced metadata extraction
        - Added multi-platform support
        - Preserved date-based grouping pattern
        """
        # Step 1: Group posts by date (legacy pattern)
        grouped_data = self._group_posts_by_date(raw_data)
        
        # Step 2: Process each group
        processed_posts = []
        
        for date_group, posts in grouped_data.items():
            for post in posts:
                try:
                    processed_post = self._process_single_post(post, metadata)
                    processed_post['grouped_date'] = date_group  # Add date group for analytics
                    processed_posts.append(processed_post)
                    
                except Exception as e:
                    logger.error(f"Error processing post {post.get('post_id', 'unknown')}: {str(e)}")
                    # Continue processing other posts
                    continue
        
        logger.info(f"Successfully processed {len(processed_posts)} posts")
        return processed_posts
    
    def _group_posts_by_date(self, raw_data: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group posts by date for analytics optimization.
        
        MIGRATED FROM: DataProcessor.process_and_group_data()
        This preserves the legacy date-based grouping pattern.
        """
        grouped_data = {}
        
        for post in raw_data:
            # Extract date from post
            date_posted = post.get('date_posted', '')
            
            # Parse date to get date string (YYYY-MM-DD)
            try:
                if date_posted:
                    if 'T' in date_posted:
                        date_key = date_posted.split('T')[0]  # Extract YYYY-MM-DD
                    else:
                        date_key = str(date_posted)[:10]  # First 10 chars
                else:
                    date_key = datetime.utcnow().strftime('%Y-%m-%d')
            except Exception:
                date_key = datetime.utcnow().strftime('%Y-%m-%d')
            
            # Group by date
            if date_key not in grouped_data:
                grouped_data[date_key] = []
            grouped_data[date_key].append(post)
        
        logger.info(f"Grouped posts into {len(grouped_data)} date groups")
        return grouped_data
    
    def _process_single_post(self, post: Dict, metadata: Dict) -> Dict:
        """Process a single post for BigQuery insertion."""
        
        # Extract and clean content
        content = self._clean_text(post.get('content', ''))
        
        # Generate processing metadata
        processing_metadata = self._generate_processing_metadata(content)
        
        # Extract media metadata
        media_metadata = self._extract_media_metadata(post)
        
        # Build processed post
        processed_post = {
            'id': post.get('post_id', f"post_{datetime.utcnow().timestamp()}"),
            'crawl_id': metadata.get('crawl_id'),
            'snapshot_id': metadata.get('snapshot_id'),
            'platform': metadata.get('platform'),
            'competitor': metadata.get('competitor'),
            'brand': metadata.get('brand'),
            'category': metadata.get('category'),
            'post_content': content,
            'engagement_metrics': self._extract_engagement_metrics(post, metadata),
            'posted_date': self._parse_date(post.get('date_posted')),
            'crawl_date': metadata.get('crawl_date'),
            'processed_date': datetime.utcnow().isoformat(),
            'processing_metadata': processing_metadata,
            'media_metadata': media_metadata
        }
        
        return processed_post
    
    def _clean_text(self, text: str) -> str:
        """
        Clean text content for analytics.
        
        MIGRATED FROM: DataProcessor text cleaning logic
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove problematic characters
        text = re.sub(r'[^\w\s\.\!\?\,\;\:\-\(\)\[\]\{\}\"\'@#]', '', text)
        
        # Limit text length for BigQuery
        if len(text) > 10000:
            text = text[:10000] + "..."
        
        return text
    
    def _extract_engagement_metrics(self, post: Dict, metadata: Dict) -> Dict:
        """
        Extract engagement metrics from post.
        
        MIGRATED FROM: DataProcessor engagement extraction
        ENHANCED: Added platform-specific metrics
        """
        metrics = {
            'likes': self._safe_int(post.get('likes', 0)),
            'comments': self._safe_int(post.get('comments', 0)),
            'shares': self._safe_int(post.get('shares', 0)),
            'reactions': self._safe_int(post.get('reactions', 0))
        }
        
        # Add platform-specific metrics
        platform = metadata.get('platform', '').lower()
        if platform == 'youtube':
            metrics['views'] = self._safe_int(post.get('views', 0))
        elif platform == 'instagram':
            metrics['saves'] = self._safe_int(post.get('saves', 0))
        
        return metrics
    
    def _generate_processing_metadata(self, content: str) -> Dict:
        """
        Generate processing metadata for analytics.
        
        ENHANCED FROM: Legacy processing with sentiment analysis
        """
        # Basic sentiment analysis
        sentiment_score = 0.0
        language = 'unknown'
        
        if content:
            try:
                blob = TextBlob(content)
                sentiment_score = blob.sentiment.polarity
                language = blob.detect_language()
            except Exception as e:
                logger.debug(f"Sentiment analysis failed: {str(e)}")
        
        return {
            'text_length': len(content),
            'language': language,
            'sentiment_score': sentiment_score,
            'processing_duration_seconds': 0  # Will be updated later
        }
    
    def _extract_media_metadata(self, post: Dict) -> Dict:
        """
        Extract media metadata for separate processing.
        
        MIGRATED FROM: DataProcessor media detection
        CHANGES: Only detection, no processing
        """
        attachments = post.get('attachments', [])
        
        if not attachments:
            return {
                'media_count': 0,
                'has_video': False,
                'has_image': False,
                'media_processing_requested': False
            }
        
        media_count = len(attachments)
        has_video = any(self._is_video_attachment(att) for att in attachments)
        has_image = any(self._is_image_attachment(att) for att in attachments)
        
        return {
            'media_count': media_count,
            'has_video': has_video,
            'has_image': has_image,
            'media_processing_requested': True
        }
    
    def _is_video_attachment(self, attachment: Dict) -> bool:
        """Check if attachment is a video."""
        return attachment.get('type') == 'video' or 'video' in attachment.get('url', '').lower()
    
    def _is_image_attachment(self, attachment: Dict) -> bool:
        """Check if attachment is an image."""
        return attachment.get('type') in ['photo', 'image'] or any(
            ext in attachment.get('url', '').lower() 
            for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']
        )
    
    def _parse_date(self, date_str: str) -> str:
        """
        Parse date string to ISO format.
        
        MIGRATED FROM: DataProcessor date parsing logic
        """
        if not date_str:
            return datetime.utcnow().isoformat()
        
        try:
            # Handle various date formats
            if isinstance(date_str, str):
                # Try ISO format first
                if 'T' in date_str:
                    parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                else:
                    # Try parsing as timestamp
                    parsed_date = datetime.fromtimestamp(float(date_str))
                
                return parsed_date.isoformat()
            
            return str(date_str)
            
        except Exception as e:
            logger.warning(f"Date parsing failed for '{date_str}': {str(e)}")
            return datetime.utcnow().isoformat()
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert value to integer."""
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0
```

### Step 3: Implement Pub/Sub Push Handler

#### 3.1 Create Event Handler for Push Subscriptions
```python
# events/event_handler.py
# NEW: Pub/Sub push handler for Cloud Run

import json
import base64
import logging
from typing import Dict, Any, Optional
from flask import Request, jsonify
from datetime import datetime
from google.cloud import storage
from handlers.text_processor import TextProcessor
from handlers.bigquery_handler import BigQueryHandler
from events.event_publisher import EventPublisher

logger = logging.getLogger(__name__)

class EventHandler:
    """
    Handle incoming Pub/Sub push events for data processing.
    
    This class implements the Pub/Sub push pattern optimized for Cloud Run.
    """
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.bigquery_handler = BigQueryHandler()
        self.event_publisher = EventPublisher()
        self.storage_client = storage.Client()
    
    def handle_data_ingestion_completed(self, request: Request) -> tuple:
        """
        Handle data-ingestion-completed events from Pub/Sub push.
        
        Args:
            request: Flask request object with Pub/Sub message
            
        Returns:
            tuple: (response_dict, status_code)
        """
        try:
            # Extract and validate event data
            event_data = self._extract_pubsub_event_data(request)
            
            if not event_data:
                logger.error("Invalid or missing event data")
                return {'error': 'Invalid event data'}, 400
            
            # Process the event
            start_time = datetime.utcnow()
            result = self._process_data_ingestion_event(event_data)
            processing_duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Log successful processing
            logger.info(f"Successfully processed data ingestion event in {processing_duration:.2f}s")
            
            return {
                'success': True,
                'processed_posts': result['processed_posts'],
                'media_processing_requested': result['media_processing_requested'],
                'processing_duration_seconds': processing_duration
            }, 200
            
        except Exception as e:
            logger.error(f"Error processing data ingestion event: {str(e)}")
            
            # Publish failure event if we have crawl_id
            if 'event_data' in locals() and event_data:
                crawl_id = event_data.get('data', {}).get('crawl_id')
                if crawl_id:
                    self.event_publisher.publish_processing_failed(crawl_id, str(e))
            
            return {'error': str(e)}, 500
    
    def _extract_pubsub_event_data(self, request: Request) -> Optional[Dict]:
        """
        Extract event data from Pub/Sub push message.
        
        Pub/Sub push format:
        {
            "message": {
                "data": "<base64-encoded-json>",
                "attributes": {...},
                "messageId": "...",
                "publishTime": "..."
            },
            "subscription": "..."
        }
        """
        try:
            # Get request JSON
            envelope = request.get_json()
            
            if not envelope:
                logger.error("No JSON body in request")
                return None
            
            # Extract message
            message = envelope.get('message', {})
            
            if not message:
                logger.error("No message in Pub/Sub envelope")
                return None
            
            # Extract and decode data
            data = message.get('data', '')
            
            if not data:
                logger.error("No data in Pub/Sub message")
                return None
            
            # Decode base64 data
            try:
                decoded_data = base64.b64decode(data).decode('utf-8')
                event_data = json.loads(decoded_data)
            except Exception as e:
                logger.error(f"Error decoding Pub/Sub data: {str(e)}")
                return None
            
            # Validate event structure
            if not event_data.get('data'):
                logger.error("Missing 'data' field in event")
                return None
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error extracting Pub/Sub event data: {str(e)}")
            return None
    
    def _process_data_ingestion_event(self, event_data: Dict) -> Dict:
        """Process the data ingestion completed event."""
        # Extract event details
        data = event_data['data']
        crawl_id = data['crawl_id']
        snapshot_id = data['snapshot_id']
        gcs_path = data['gcs_path']
        
        logger.info(f"Processing data ingestion event for crawl {crawl_id}")
        
        # Download raw data from GCS
        raw_data = self._download_raw_data_from_gcs(gcs_path)
        
        if not raw_data:
            raise ValueError(f"No data found at GCS path: {gcs_path}")
        
        # Prepare metadata
        metadata = {
            'crawl_id': crawl_id,
            'snapshot_id': snapshot_id,
            'gcs_path': gcs_path,
            'crawl_date': event_data.get('timestamp'),
            'platform': data.get('platform'),
            'competitor': data.get('competitor'),
            'brand': data.get('brand'),
            'category': data.get('category')
        }
        
        # Process posts for analytics
        processed_posts = self.text_processor.process_posts_for_analytics(raw_data, metadata)
        
        if not processed_posts:
            logger.warning(f"No posts processed for crawl {crawl_id}")
            return {
                'processed_posts': 0,
                'media_processing_requested': False
            }
        
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
        posts_with_media = [p for p in processed_posts 
                           if p['media_metadata']['media_processing_requested']]
        
        media_processing_requested = False
        if posts_with_media:
            media_processing_requested = True
            self.event_publisher.publish_media_processing_requested(
                crawl_id=crawl_id,
                snapshot_id=snapshot_id,
                posts_with_media=posts_with_media
            )
            
            logger.info(f"Requested media processing for {len(posts_with_media)} posts")
        
        return {
            'processed_posts': len(processed_posts),
            'media_processing_requested': media_processing_requested
        }
    
    def _download_raw_data_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Download raw data from GCS path."""
        try:
            # Parse GCS path (gs://bucket/path/to/file.json)
            path_parts = gcs_path.replace('gs://', '').split('/')
            bucket_name = path_parts[0]
            object_name = '/'.join(path_parts[1:])
            
            # Download from GCS
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Download and parse JSON
            json_content = blob.download_as_text()
            raw_data = json.loads(json_content)
            
            # Extract posts from BrightData format
            if isinstance(raw_data, list):
                return raw_data
            elif isinstance(raw_data, dict):
                # Handle nested structure
                return raw_data.get('posts', raw_data.get('data', [raw_data]))
            else:
                logger.error(f"Unexpected data format: {type(raw_data)}")
                return []
                
        except Exception as e:
            logger.error(f"Error downloading from GCS {gcs_path}: {str(e)}")
            raise
```

### Step 4: Create Flask Application with Pub/Sub Endpoint

#### 4.1 Main Application
```python
# app.py
# NEW: Flask app with Pub/Sub push endpoint

from flask import Flask, request, jsonify
from events.event_handler import EventHandler
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize event handler
event_handler = EventHandler()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'data-processing',
        'version': '1.0.0',
        'environment': os.getenv('GOOGLE_CLOUD_PROJECT', 'unknown')
    })

@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    """
    Handle data-ingestion-completed events from Pub/Sub push.
    
    This is the main entry point for the data processing service.
    It receives push notifications from Pub/Sub when the Data Ingestion
    Service completes raw data collection.
    """
    try:
        result, status_code = event_handler.handle_data_ingestion_completed(request)
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error in data ingestion handler: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/v1/test', methods=['POST'])
def test_endpoint():
    """Test endpoint for manual testing."""
    try:
        test_data = request.json
        logger.info(f"Test endpoint called with data: {test_data}")
        
        return jsonify({
            'message': 'Test endpoint working',
            'received_data': test_data
        })
        
    except Exception as e:
        logger.error(f"Error in test endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
```

### Step 5: Create BigQuery Handler

#### 5.1 BigQuery Operations
```python
# handlers/bigquery_handler.py
# NEW: BigQuery handler for direct insertion

import os
import logging
from typing import List, Dict, Any
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class BigQueryHandler:
    """
    Handle BigQuery operations for analytics data storage.
    
    Optimized for direct insertion of processed social media data.
    """
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
        self.posts_table = f"{self.project_id}.{self.dataset_id}.posts"
        self.events_table = f"{self.project_id}.{self.dataset_id}.processing_events"
    
    def insert_posts(self, processed_posts: List[Dict], metadata: Dict) -> Dict:
        """
        Insert processed posts to BigQuery analytics table.
        
        Args:
            processed_posts: List of processed posts
            metadata: Processing metadata
            
        Returns:
            Dict with insertion results
        """
        try:
            # Validate posts against BigQuery schema
            validated_posts = self._validate_posts_schema(processed_posts)
            
            # Insert to BigQuery
            errors = self.client.insert_rows_json(self.posts_table, validated_posts)
            
            if errors:
                error_msg = f"BigQuery insertion errors: {errors}"
                logger.error(error_msg)
                self._log_processing_event(metadata, len(processed_posts), False, error_msg)
                raise BigQueryInsertionError(error_msg)
            
            logger.info(f"Successfully inserted {len(processed_posts)} posts to BigQuery")
            
            # Log successful processing
            self._log_processing_event(metadata, len(processed_posts), True)
            
            return {
                'success': True,
                'rows_inserted': len(processed_posts),
                'table_id': self.posts_table
            }
            
        except GoogleCloudError as e:
            error_msg = f"BigQuery operation failed: {str(e)}"
            logger.error(error_msg)
            self._log_processing_event(metadata, len(processed_posts), False, error_msg)
            raise BigQueryInsertionError(error_msg)
    
    def _validate_posts_schema(self, processed_posts: List[Dict]) -> List[Dict]:
        """Validate processed posts against BigQuery schema."""
        validated_posts = []
        
        for post in processed_posts:
            # Ensure all required fields are present and properly typed
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
            
            # Validate timestamp format
            for date_field in ['posted_date', 'crawl_date', 'processed_date']:
                if validated_post[date_field]:
                    validated_post[date_field] = self._ensure_timestamp_format(
                        validated_post[date_field]
                    )
            
            validated_posts.append(validated_post)
        
        return validated_posts
    
    def _ensure_timestamp_format(self, date_value: str) -> str:
        """Ensure date is in proper timestamp format for BigQuery."""
        if not date_value:
            return datetime.utcnow().isoformat()
        
        try:
            # Try parsing as ISO format
            if isinstance(date_value, str):
                if 'T' in date_value:
                    # ISO format - ensure proper timezone
                    return date_value.replace('Z', '+00:00')
                else:
                    # Try as timestamp
                    parsed = datetime.fromtimestamp(float(date_value))
                    return parsed.isoformat()
            
            return str(date_value)
            
        except Exception as e:
            logger.warning(f"Date format validation failed: {str(e)}")
            return datetime.utcnow().isoformat()
    
    def _log_processing_event(self, metadata: Dict, post_count: int, success: bool, error_message: str = None):
        """Log processing event to BigQuery for monitoring."""
        try:
            event_record = {
                'event_id': f"proc_{datetime.utcnow().timestamp()}",
                'crawl_id': metadata.get('crawl_id'),
                'event_type': 'data_processing',
                'event_timestamp': datetime.utcnow().isoformat(),
                'processing_duration_seconds': metadata.get('processing_duration', 0),
                'post_count': post_count,
                'media_count': metadata.get('media_count', 0),
                'success': success,
                'error_message': error_message
            }
            
            # Insert to events table
            errors = self.client.insert_rows_json(self.events_table, [event_record])
            
            if errors:
                logger.warning(f"Failed to log processing event: {errors}")
                
        except Exception as e:
            logger.warning(f"Error logging processing event: {str(e)}")

class BigQueryInsertionError(Exception):
    """Custom exception for BigQuery insertion errors."""
    pass
```

## 📋 Phase 2: Pub/Sub Configuration (Week 2)

### Step 1: Create Pub/Sub Push Subscription

#### 1.1 Configure Push Subscription
```bash
# Create push subscription for data processing service
# This will be run after service deployment

# First, deploy the service to get the URL
SERVICE_URL=$(gcloud run services describe data-processing-service --region=asia-southeast1 --format="value(status.url)")

# Create push subscription
gcloud pubsub subscriptions create data-processing-ingestion-events \
    --topic=social-analytics-data-ingestion-completed \
    --push-endpoint=${SERVICE_URL}/api/v1/events/data-ingestion-completed \
    --ack-deadline=600 \
    --max-delivery-attempts=3 \
    --min-retry-delay=10s \
    --max-retry-delay=60s \
    --project=competitor-destroyer
```

#### 1.2 Create Additional Topics
```bash
# Create topics for data processing events
gcloud pubsub topics create social-analytics-data-processing-started --project=competitor-destroyer
gcloud pubsub topics create social-analytics-data-processing-completed --project=competitor-destroyer
gcloud pubsub topics create social-analytics-data-processing-failed --project=competitor-destroyer
gcloud pubsub topics create social-analytics-media-processing-requested --project=competitor-destroyer
```

### Step 2: Service Deployment

#### 2.1 Create Dockerfile
```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8080

# Run the application
CMD ["python", "app.py"]
```

#### 2.2 Create Requirements
```txt
# requirements.txt
Flask==2.3.3
google-cloud-bigquery==3.11.4
google-cloud-storage==2.10.0
google-cloud-pubsub==2.18.2
textblob==0.17.1
python-dotenv==1.0.0
gunicorn==21.2.0
```

#### 2.3 Deploy Service
```bash
# Deploy data processing service
cd services/data-processing

# Build and deploy
gcloud run deploy data-processing-service \
    --source=. \
    --region=asia-southeast1 \
    --service-account=data-processing-sa@competitor-destroyer.iam.gserviceaccount.com \
    --allow-unauthenticated \
    --memory=1Gi \
    --cpu=1 \
    --min-instances=0 \
    --max-instances=20 \
    --timeout=300 \
    --concurrency=100 \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=competitor-destroyer,BIGQUERY_DATASET=social_analytics,PUBSUB_TOPIC_PREFIX=social-analytics"
```

### Step 3: End-to-End Testing

#### 3.1 Test Pub/Sub Push Integration
```python
# test_pubsub_integration.py
import json
import base64
import requests
from google.cloud import pubsub_v1

def test_pubsub_push():
    """Test Pub/Sub push integration with data processing service."""
    
    # Get service URL
    service_url = "https://data-processing-service-xyz.run.app"
    
    # Create test event data
    test_event = {
        "event_type": "data-ingestion-completed",
        "timestamp": "2025-07-09T10:00:00.000Z",
        "source": "data-ingestion-service",
        "data": {
            "crawl_id": "test-crawl-123",
            "snapshot_id": "s_test123",
            "gcs_path": "gs://social-analytics-raw-data/test/data.json",
            "platform": "facebook",
            "competitor": "test-competitor",
            "brand": "test-brand",
            "category": "test-category",
            "post_count": 3,
            "media_count": 5
        }
    }
    
    # Encode as Pub/Sub message
    message_data = json.dumps(test_event).encode('utf-8')
    encoded_data = base64.b64encode(message_data).decode('utf-8')
    
    # Create Pub/Sub push payload
    push_payload = {
        "message": {
            "data": encoded_data,
            "messageId": "test-message-123",
            "publishTime": "2025-07-09T10:00:00.000Z",
            "attributes": {}
        },
        "subscription": "projects/competitor-destroyer/subscriptions/data-processing-ingestion-events"
    }
    
    # Send to service
    response = requests.post(
        f"{service_url}/api/v1/events/data-ingestion-completed",
        json=push_payload,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    
    return response.status_code == 200

if __name__ == "__main__":
    test_pubsub_push()
```

#### 3.2 Test Complete Pipeline
```bash
# Test complete data ingestion → processing pipeline

# 1. Trigger crawl in data ingestion service
curl -X POST "https://data-ingestion-service-url/api/v1/crawl/trigger" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{
        "dataset_id": "gd_test123",
        "platform": "facebook",
        "competitor": "test-competitor",
        "brand": "test-brand",
        "category": "test-category",
        "url": "https://example.com",
        "num_of_posts": 3
    }'

# 2. Monitor BigQuery for processed data
bq query --use_legacy_sql=false --project_id=competitor-destroyer \
    "SELECT * FROM \`competitor-destroyer.social_analytics.posts\` 
     WHERE competitor = 'test-competitor' 
     ORDER BY processed_date DESC 
     LIMIT 10"

# 3. Check processing events
bq query --use_legacy_sql=false --project_id=competitor-destroyer \
    "SELECT * FROM \`competitor-destroyer.social_analytics.processing_events\` 
     WHERE event_type = 'data_processing' 
     ORDER BY event_timestamp DESC 
     LIMIT 10"
```

## 🔧 Phase 3: Optimization & Monitoring

### Step 1: Performance Monitoring

#### 1.1 Add Metrics Collection
```python
# Add to app.py
from prometheus_client import Counter, Histogram, generate_latest
import time

# Metrics
processing_duration = Histogram('data_processing_duration_seconds', 'Time spent processing data')
processing_success = Counter('data_processing_success_total', 'Successful processing operations')
processing_errors = Counter('data_processing_errors_total', 'Processing errors')

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

# Add metrics to event handler
def handle_data_ingestion_completed(self, request: Request) -> tuple:
    start_time = time.time()
    
    try:
        # ... existing code ...
        processing_success.inc()
        return result, 200
    except Exception as e:
        processing_errors.inc()
        raise
    finally:
        processing_duration.observe(time.time() - start_time)
```

### Step 2: Error Handling Enhancement

#### 2.1 Implement Circuit Breaker
```python
# handlers/circuit_breaker.py
import time
from typing import Callable, Any

class CircuitBreaker:
    """Simple circuit breaker implementation."""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful operation."""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
```

### Step 3: Comprehensive Testing

#### 3.1 Unit Tests
```python
# tests/test_text_processor.py
import unittest
from handlers.text_processor import TextProcessor

class TestTextProcessor(unittest.TestCase):
    
    def setUp(self):
        self.processor = TextProcessor()
    
    def test_process_posts_for_analytics(self):
        """Test post processing for analytics."""
        raw_posts = [
            {
                'post_id': 'test-post-1',
                'content': 'Test post content',
                'likes': 10,
                'comments': 5,
                'shares': 2,
                'date_posted': '2025-07-09T10:00:00.000Z',
                'attachments': [
                    {'type': 'photo', 'url': 'https://example.com/photo.jpg'}
                ]
            }
        ]
        
        metadata = {
            'crawl_id': 'test-crawl-123',
            'platform': 'facebook',
            'competitor': 'test-competitor'
        }
        
        result = self.processor.process_posts_for_analytics(raw_posts, metadata)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], 'test-post-1')
        self.assertEqual(result[0]['platform'], 'facebook')
        self.assertTrue(result[0]['media_metadata']['media_processing_requested'])

if __name__ == '__main__':
    unittest.main()
```

## 🎯 Success Criteria

### Technical Validation
- ✅ Process 3 posts in <30 seconds
- ✅ Pub/Sub push integration working
- ✅ BigQuery direct insertion successful
- ✅ Event publishing to downstream services
- ✅ Error handling and retry mechanisms

### Performance Metrics
- ✅ <5 second BigQuery insertion time
- ✅ <1 second event publishing latency
- ✅ 99.9% success rate for event processing
- ✅ Memory usage <500MB per processing job

### Integration Testing
- ✅ End-to-end data ingestion → processing pipeline
- ✅ Media processing events triggered correctly
- ✅ Monitoring and alerting operational
- ✅ Circuit breaker and resilience features

This implementation guide provides a comprehensive roadmap for building the Data Processing Service with proper legacy code migration, Pub/Sub push integration, and production-ready features.