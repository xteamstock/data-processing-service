# Events Package - Data Processing Service

## 📋 Overview

This package provides unified event publishing functionality for the data processing service. It supports both data processing lifecycle events and media processing events with multi-platform support.

## 🏗️ Architecture

```
events/
├── publishers.py           # 🎯 Main: Unified event publishing classes
├── __init__.py            # 📦 Clean imports and convenience functions
├── README.md              # 📚 This documentation
├── MIGRATION_GUIDE.md     # 🔄 Migration from legacy files
├── event_publisher.py     # 📦 Legacy: General event publisher (kept for compatibility)
├── media_event_publisher.py    # 📦 Legacy: Individual media events (kept for compatibility)
└── batch_media_event_publisher.py  # 📦 Legacy: Batch media events (kept for compatibility)
```

## 🚀 Quick Start

### **1. Data Processing Events**
```python
from events import publish_processing_completed

# Publish processing completed event
result = publish_processing_completed(
    crawl_metadata={
        'crawl_id': 'abc123',
        'snapshot_id': 'snap001', 
        'platform': 'youtube',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    },
    processing_stats={
        'processed_posts': 25,
        'bigquery_tables': ['youtube_posts'],
        'gcs_files': ['processed_posts_20250714.json'],
        'duration_seconds': 45.2
    }
)

if result['success']:
    print(f"✅ Event published: {result['message_id']}")
else:
    print(f"❌ Failed: {result['error']}")
```

### **2. Batch Media Events (Recommended)**
```python
from events import publish_batch_media_events

# Process entire raw data file at once
result = publish_batch_media_events(
    raw_posts=raw_posts_from_file,
    platform='facebook',  # facebook, tiktok, youtube
    crawl_metadata={
        'crawl_id': 'abc123',
        'snapshot_id': 'snap001',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    },
    file_metadata={
        'filename': 'gcs-facebook-posts.json',
        'size': 1024000
    }
)

print(f"Published batch with {result['media_count']} media items")
```

### **3. Individual Media Events (Legacy)**
```python
from events import publish_individual_media_events

# Publish individual events for each media URL in a post
result = publish_individual_media_events(
    post_data=single_post,
    platform='tiktok',
    crawl_metadata=metadata
)

print(f"Published {result['events_published']} individual events")
```

## 📊 Event Types Supported

### **Data Processing Events**
- **data-processing-completed**: When post processing finishes successfully
- **data-processing-failed**: When processing fails with errors
- **bigquery-insertion-completed**: When BigQuery operations complete

### **Media Processing Events**
- **media-download-requested**: Individual media URL download request (legacy)
- **batch-media-download-requested**: Batch media download request (recommended)

## 🎯 Multi-Platform Media Support

### **Supported Platforms with Duration Fields**
- **Facebook**: Videos with duration (ms→s), images, page logos from attachments
- **TikTok**: Video URLs with duration (seconds), cover images, author avatars  
- **YouTube**: Video URLs with duration (HH:MM:SS→s), thumbnails, channel avatars/banners

### **Duration Field Support**
```python
# Facebook - video_length in milliseconds converted to seconds
{
    "type": "video",
    "metadata": {
        "duration": 101.573,     # Duration in seconds
        "duration_ms": "101573"  # Original milliseconds
    }
}

# TikTok - duration in videoMeta.duration (seconds)
{
    "type": "video", 
    "metadata": {
        "duration": 77  # Duration in seconds
    }
}

# YouTube - duration in HH:MM:SS format converted to seconds
{
    "type": "video",
    "metadata": {
        "duration": 24,           # Duration in seconds
        "duration_original": "00:00:24"  # Original HH:MM:SS format
    }
}
```

### **Media URL Examples**
```python
# Facebook
"https://video-vie1-1.xx.fbcdn.net/o1/v/t2/f2/m69/VIDEO.mp4"

# TikTok  
"https://www.tiktok.com/@nutifoodgrowplus/video/7525738192612494599"

# YouTube
"https://www.youtube.com/watch?v=vcdIMD2V1UM"
```

## 🔧 Advanced Usage

### **Using Publisher Classes Directly**
```python
from events.publishers import DataProcessingEventPublisher, MediaEventPublisher

# Data processing events
data_publisher = DataProcessingEventPublisher(project_id='my-project')
result = data_publisher.publish_processing_completed(metadata, stats)
data_publisher.close()

# Media events  
media_publisher = MediaEventPublisher(
    project_id='my-project',
    batch_topic='my-batch-topic'
)
result = media_publisher.publish_batch_media_event(posts, platform, metadata)
media_publisher.close()
```

### **Custom Error Handling**
```python
from events.publishers import DataProcessingEventPublisher

publisher = DataProcessingEventPublisher()

try:
    result = publisher.publish_processing_failed(
        crawl_metadata=metadata,
        error_info={
            'error_message': 'BigQuery insertion failed',
            'error_type': 'bigquery_error',
            'failed_step': 'schema_validation',
            'partial_results': {'processed_posts': 15}
        }
    )
finally:
    publisher.close()
```

## 📈 Performance Recommendations

### **Batch vs Individual Events**
- ✅ **Batch**: 1 event for 20 media URLs = 95% reduction in Pub/Sub overhead
- ❌ **Individual**: 20 events for 20 media URLs = Higher latency and cost

### **Example Performance Comparison**
```
Raw File: 10 YouTube videos + 10 thumbnails = 20 media items

Individual Mode:
├── 20 separate Pub/Sub messages
├── 20 separate media-processing service invocations  
└── Higher latency and cost

Batch Mode:  
├── 1 Pub/Sub message with all 20 URLs
├── 1 media-processing service invocation
├── Parallel download of all 20 items
└── 95% reduction in overhead
```

## 🧪 Testing

### **Test Event Publishing**
```bash
# Test import structure and dry-run  
python tests/test_unified_events.py

# Test with actual Pub/Sub publishing
python tests/test_unified_events.py --publish
```

### **Test Media Detection**
```bash
# Test all platforms
python tests/test_batch_media_system.py --platform all

# Test specific platform
python tests/test_batch_media_system.py --platform youtube --publish
```

## 🔗 Integration Points

### **With Data Processing Pipeline**
```python
# In your main processing pipeline
from events import publish_processing_completed, publish_batch_media_events

def process_raw_data_file(raw_posts, platform, crawl_metadata):
    # 1. Process posts for BigQuery
    processed_posts = process_posts_for_analytics(raw_posts, crawl_metadata)
    
    # 2. Insert to BigQuery
    bigquery_result = insert_to_bigquery(processed_posts, platform)
    
    # 3. Publish data processing completed event
    publish_processing_completed(crawl_metadata, {
        'processed_posts': len(processed_posts),
        'bigquery_tables': [f'{platform}_posts']
    })
    
    # 4. Publish batch media event for parallel download
    publish_batch_media_events(raw_posts, platform, crawl_metadata)
```

### **With Error Handling**
```python
from events import DataProcessingEventPublisher

def safe_process_with_events(crawl_metadata):
    publisher = DataProcessingEventPublisher()
    
    try:
        # Your processing logic here
        result = process_data()
        
        # Publish success
        publisher.publish_processing_completed(crawl_metadata, result)
        
    except Exception as e:
        # Publish failure
        publisher.publish_processing_failed(crawl_metadata, {
            'error_message': str(e),
            'error_type': type(e).__name__
        })
        raise
    finally:
        publisher.close()
```

## 📚 Related Documentation

- **Migration Guide**: `MIGRATION_GUIDE.md` - How to migrate from legacy files
- **Multi-Platform Media Detector**: `../handlers/multi_platform_media_detector.py`
- **Test Scripts**: `../tests/test_unified_events.py`, `../tests/test_batch_media_system.py`

## 🎯 Event Payload Schemas

### **Enhanced Batch Media Event Structure**
```json
{
  "event_type": "batch-media-download-requested",
  "data": {
    "batch_summary": {
      "platform": "youtube",
      "total_posts": 10,
      "total_media_items": 40,
      "video_count": 10,
      "image_count": 30,
      "groups": {
        "videos": 10,
        "images": 0,
        "thumbnails": 10,
        "profile_images": 10,
        "banner_images": 10
      }
    },
    "media_groups": {
      "videos": [
        {
          "url": "https://www.youtube.com/watch?v=VIDEO_ID",
          "type": "video",
          "media_id": "VIDEO_ID",
          "metadata": {
            "duration": 45,
            "duration_original": "00:00:45",
            "title": "Video Title"
          }
        }
      ],
      "thumbnails": [/* Thumbnail URLs */],
      "profile_images": [/* Profile image URLs */],
      "banner_images": [/* Banner image URLs */]
    },
    "crawl_metadata": {/* Crawl context */},
    "processing_config": {
      "mode": "batch",
      "parallel_downloads": 10,
      "timeout_seconds": 1260,
      "priority_order": ["videos", "images", "thumbnails", "profile_images", "banner_images"]
    }
  }
}
```

### **Processing Completed Event Structure**
```json
{
  "event_type": "data-processing-completed", 
  "data": {
    "crawl_id": "abc123",
    "platform": "facebook",
    "competitor": "nutifood",
    "processed_posts": 25,
    "bigquery_tables_updated": ["facebook_posts"],
    "processing_duration_seconds": 45.2,
    "status": "completed"
  }
}
```

## ✅ Best Practices

1. **Use batch media events** for better performance and organized processing
2. **Include rich metadata** (platform, competitor, brand, category)
3. **Leverage media grouping** for efficient parallel downloads
4. **Handle return values** and check for success/failure
5. **Use duration fields** for video processing optimization
6. **Close publishers** when done (or use convenience functions)
7. **Test in development** before deploying to production
8. **Monitor Pub/Sub topics** for event delivery and processing

## 🎯 Enhanced Features (Latest)

### **Duration Support for All Platforms**
- ✅ **Facebook**: Video duration from `video_length` (ms → seconds)
- ✅ **TikTok**: Video duration from `videoMeta.duration` (seconds)
- ✅ **YouTube**: Video duration from `duration` (HH:MM:SS → seconds)

### **Organized Media Groups**
- ✅ **Videos**: Prioritized for download with duration metadata
- ✅ **Images**: General images and photos
- ✅ **Thumbnails**: Video cover images and previews
- ✅ **Profile Images**: User/channel avatars
- ✅ **Banner Images**: Channel banners and headers

### **Batch Processing Benefits**
- 📦 **95% fewer events**: One batch event vs individual media events
- ⚡ **Organized processing**: Media grouped by type for efficient downloads
- 🎯 **Priority ordering**: Videos processed first, then images, thumbnails, etc.
- 📊 **Rich statistics**: Detailed breakdown of media types in each batch
- ⏱️ **Duration-aware**: Video download timeouts based on actual duration