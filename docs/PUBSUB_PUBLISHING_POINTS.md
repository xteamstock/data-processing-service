# GCP Pub/Sub Publishing Points in Data Processing Service

## 📡 **Overview**

The data processing service fires events to GCP Pub/Sub in **TWO** main locations:

1. **Standard Event Publisher** (`events/event_publisher.py`) - For general service events
2. **Batch Media Event Publisher** (`events/batch_media_event_publisher.py`) - For batch media processing events

## 🎯 **1. Batch Media Event Publisher**

### **Location**: `events/batch_media_event_publisher.py:83-98`

This is the main batch media publishing code that fires events to GCP Pub/Sub:

```python
# Step 4: Publish single batch event
future = self.publisher.publish(
    self.topic_path,
    json.dumps(batch_event).encode('utf-8'),
    # Pub/Sub attributes for filtering/routing
    event_type='batch-media-download',
    platform=platform,
    batch_size=str(batch_result['total_media_items']),
    crawl_id=crawl_metadata.get('crawl_id', ''),
    competitor=crawl_metadata.get('competitor', ''),
    brand=crawl_metadata.get('brand', ''),
    has_videos=str(batch_result['total_videos'] > 0),
    has_images=str(batch_result['total_images'] > 0)
)

# Wait for publish to complete (optional)
message_id = future.result(timeout=10)
```

### **Pub/Sub Client Initialization**: `events/batch_media_event_publisher.py:46-48`

```python
self.publisher = pubsub_v1.PublisherClient()
self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
```

### **Topic Configuration**:
- **Default Topic**: `batch-media-processing-requests`
- **Project ID**: From `GOOGLE_CLOUD_PROJECT` environment variable
- **Topic Path**: `projects/{project_id}/topics/batch-media-processing-requests`

### **Event Data Structure**:
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
      "total_posts": 3,
      "total_media_items": 7,
      "media_counts": {
        "videos": 2,
        "images": 5
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

### **Pub/Sub Message Attributes**:
- `event_type`: "batch-media-download"
- `platform`: "facebook" | "tiktok" | "youtube"
- `batch_size`: Number of media items
- `crawl_id`: Crawl identifier
- `competitor`: Competitor name
- `brand`: Brand name
- `has_videos`: "true" | "false"
- `has_images`: "true" | "false"

## 🔄 **2. Standard Event Publisher**

### **Location**: `events/event_publisher.py:96-120`

This publishes general service events:

```python
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

### **Pub/Sub Client Initialization**: `events/event_publisher.py:18-21`

```python
self.publisher = pubsub_v1.PublisherClient()
self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
self.topic_prefix = os.getenv('PUBSUB_TOPIC_PREFIX', 'social-analytics')
```

### **Events Published**:

#### **A. Data Processing Completed** (`event_publisher.py:23-34`)
```python
def publish_data_processing_completed(self, crawl_id: str, snapshot_id: str, 
                                    processed_posts: int, bigquery_table: str) -> bool:
    event_data = {
        'crawl_id': crawl_id,
        'snapshot_id': snapshot_id,
        'processed_posts': processed_posts,
        'bigquery_table': bigquery_table,
        'status': 'completed'
    }
    
    return self.publish('data-processing-completed', event_data)
```
- **Topic**: `social-analytics-data-processing-completed`

#### **B. Media Processing Requested** (`event_publisher.py:36-84`)
```python
def publish_media_processing_requested(self, crawl_id: str, snapshot_id: str, 
                                     posts_with_media: List[Dict], media_info: Dict[str, Any] = None) -> bool:
    # Individual media processing events (legacy)
    return self.publish('media-processing-requested', event_data)
```
- **Topic**: `social-analytics-media-processing-requested`

#### **C. Processing Failed** (`event_publisher.py:86-94`)
```python
def publish_processing_failed(self, crawl_id: str, error_message: str) -> bool:
    event_data = {
        'crawl_id': crawl_id,
        'error_message': error_message,
        'status': 'failed'
    }
    
    return self.publish('data-processing-failed', event_data)
```
- **Topic**: `social-analytics-data-processing-failed`

## 🚀 **Integration Points in Main Service**

### **1. EventHandler Integration** (`events/event_handler.py`)

The EventHandler uses both publishers:

```python
def __init__(self):
    # Standard event publisher
    self.event_publisher = EventPublisher()
    
    # Batch media event publisher
    try:
        self.batch_media_publisher = BatchMediaEventPublisher()
        self.batch_media_enabled = True
    except Exception as e:
        self.batch_media_publisher = None
        self.batch_media_enabled = False
```

### **2. Publishing Flow in Main Processing** (`events/event_handler.py:218-273`)

```python
# Publish data processing completed event
self.event_publisher.publish_data_processing_completed(
    crawl_id=crawl_id,
    snapshot_id=snapshot_id,
    processed_posts=len(processed_posts),
    bigquery_table=bigquery_result['table_id']
)

# Job 3: Individual media events (legacy)
if posts_with_media:
    media_event_success = self.event_publisher.publish_media_processing_requested(
        crawl_id=crawl_id,
        snapshot_id=snapshot_id,
        posts_with_media=posts_with_media,
        media_info=media_info
    )

# Job 4: Batch media events (new)
batch_media_result = self._process_batch_media_events(
    processed_posts, 
    metadata.get('platform'),
    metadata
)
```

### **3. Batch Media Processing Call** (`events/event_handler.py:364-369`)

```python
result = self.batch_media_publisher.publish_batch_from_raw_file(
    raw_posts=processed_posts,
    platform=platform,
    crawl_metadata=crawl_metadata,
    file_metadata={'source': 'data_processing_pipeline'}
)
```

## 📊 **Summary of Pub/Sub Topics**

| **Event Type** | **Topic Name** | **Publisher** | **Purpose** |
|---|---|---|---|
| Batch Media Processing | `batch-media-processing-requests` | BatchMediaEventPublisher | Batch media download requests |
| Data Processing Completed | `social-analytics-data-processing-completed` | EventPublisher | Processing completion notifications |
| Media Processing Requested | `social-analytics-media-processing-requested` | EventPublisher | Individual media processing (legacy) |
| Processing Failed | `social-analytics-data-processing-failed` | EventPublisher | Error notifications |

## 🔧 **Environment Configuration**

### **Required Environment Variables**:
- `GOOGLE_CLOUD_PROJECT`: GCP project ID
- `PUBSUB_TOPIC_PREFIX`: Topic prefix (default: "social-analytics")

### **Service Account Permissions**:
- `pubsub.publisher` role on all topics
- `pubsub.viewer` role for topic validation

## 🎯 **Key Execution Points**

1. **Main Batch Media Publishing**: `batch_media_event_publisher.py:83-98`
2. **Standard Event Publishing**: `event_publisher.py:113-114`
3. **Integration Call**: `event_handler.py:364-369`
4. **Error Handling**: Both publishers include comprehensive error handling and logging

The service publishes to **4 different Pub/Sub topics** with the batch media event publisher being the primary mechanism for media processing events.