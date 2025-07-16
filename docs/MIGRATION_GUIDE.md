# Events Package Migration Guide

## 📋 Overview

The events package has been refactored to provide a clean, organized structure for all event publishing functionality. This document explains the changes and how to migrate existing code.

## 🔄 Before and After

### **BEFORE (Fragmented)**
```
events/
├── event_publisher.py              # General events
├── media_event_publisher.py        # Individual media events  
├── batch_media_event_publisher.py  # Batch media events
└── __init__.py                     # Minimal
```

### **AFTER (Unified)**
```
events/
├── publishers.py                   # 🆕 Unified event publishing
├── __init__.py                     # 🆕 Clean imports & docs
├── MIGRATION_GUIDE.md             # 🆕 This guide
├── event_publisher.py             # 📦 Legacy (kept for compatibility)
├── media_event_publisher.py       # 📦 Legacy (kept for compatibility)
└── batch_media_event_publisher.py # 📦 Legacy (kept for compatibility)
```

## 🏗️ New Architecture

### **1. BaseEventPublisher**
- Common functionality for all publishers
- Handles Pub/Sub client initialization
- Standardized event publishing with attributes
- Error handling and logging

### **2. DataProcessingEventPublisher**
- Data processing lifecycle events
- Processing completed/failed events
- BigQuery operation events

### **3. MediaEventPublisher**
- Unified media event publishing
- Supports both individual and batch modes
- Multi-platform media detection
- Backward compatible with legacy APIs

## 📝 Migration Examples

### **Data Processing Events**

#### Before:
```python
from events.event_publisher import EventPublisher

publisher = EventPublisher()
publisher.publish_data_processing_completed(
    crawl_id, snapshot_id, processed_posts, bigquery_table
)
```

#### After (Recommended):
```python
from events import publish_processing_completed

result = publish_processing_completed(
    crawl_metadata={
        'crawl_id': crawl_id,
        'snapshot_id': snapshot_id,
        'platform': platform,
        'competitor': competitor,
        'brand': brand,
        'category': category
    },
    processing_stats={
        'processed_posts': processed_posts,
        'bigquery_tables': [bigquery_table],
        'duration_seconds': 45.2
    }
)

if result['success']:
    logger.info(f"Published event: {result['message_id']}")
```

#### After (Class Usage):
```python
from events.publishers import DataProcessingEventPublisher

publisher = DataProcessingEventPublisher()
result = publisher.publish_processing_completed(crawl_metadata, stats)
publisher.close()
```

### **Media Events**

#### Before (Individual):
```python
from events.media_event_publisher import MediaEventPublisher

publisher = MediaEventPublisher()
count = publisher.publish_media_events(post_data, platform, crawl_metadata)
```

#### Before (Batch):
```python
from events.batch_media_event_publisher import BatchMediaEventPublisher

publisher = BatchMediaEventPublisher()
result = publisher.publish_batch_from_raw_file(raw_posts, platform, metadata)
```

#### After (Unified - Batch Recommended):
```python
from events import publish_batch_media_events

result = publish_batch_media_events(
    raw_posts=raw_posts,
    platform=platform,
    crawl_metadata=crawl_metadata,
    file_metadata={'filename': 'gcs-facebook-posts.json'}
)

if result['success']:
    logger.info(f"Published batch with {result['media_count']} media items")
```

#### After (Individual for Legacy Support):
```python
from events import publish_individual_media_events

result = publish_individual_media_events(post_data, platform, crawl_metadata)
```

#### After (Class Usage):
```python
from events.publishers import MediaEventPublisher

publisher = MediaEventPublisher()

# Batch mode (recommended)
result = publisher.publish_batch_media_event(raw_posts, platform, metadata)

# Individual mode (legacy)
result = publisher.publish_individual_media_events(post_data, platform, metadata)

publisher.close()
```

## 🎯 Key Benefits

### **1. Organization**
- All event publishing in one file (`publishers.py`)
- Clear separation of concerns
- Consistent APIs across event types

### **2. Unified Media Handling**
- Single class handles both individual and batch media events
- Multi-platform support (Facebook, TikTok, YouTube)
- Includes actual video URLs for all platforms

### **3. Better Developer Experience**
- Clean imports: `from events import publish_batch_media_events`
- Comprehensive documentation in `__init__.py`
- Consistent return values and error handling

### **4. Backward Compatibility**
- Legacy files preserved for existing code
- Gradual migration possible
- No breaking changes to existing APIs

## 📊 Usage Recommendations

### **For New Code:**
1. ✅ **Use convenience functions**: `from events import publish_*`
2. ✅ **Use batch media events**: More efficient than individual events
3. ✅ **Include rich metadata**: platform, competitor, brand, category
4. ✅ **Handle return values**: Check `result['success']` and log errors

### **For Existing Code:**
1. 🔄 **Gradual migration**: Replace imports one at a time
2. 🔄 **Test thoroughly**: Ensure event payloads are compatible
3. 🔄 **Update metadata**: Add new fields like platform, competitor, etc.
4. 🔄 **Switch to batch mode**: For better performance

## 🧪 Testing

### **Test the New System:**
```bash
# Test import structure and dry-run
python tests/test_unified_events.py

# Test with actual Pub/Sub publishing
python tests/test_unified_events.py --publish
```

### **Test Media Detection:**
```bash
# Test multi-platform media detection
python tests/test_batch_media_system.py --platform all

# Test specific platform
python tests/test_batch_media_system.py --platform youtube
```

## 🚨 Breaking Changes

### **None for Existing Code**
All legacy files are preserved and functional. The new system is additive.

### **For New Features**
- Media events now include platform-specific fields
- Batch events have different payload structure
- YouTube video URLs are now included (previously skipped)

## 📚 Event Payload Examples

### **Data Processing Completed (New Format)**
```json
{
  "event_type": "data-processing-completed",
  "timestamp": "2025-07-14T10:00:00Z",
  "source": "data-processing-service",
  "version": "2.0",
  "data": {
    "crawl_id": "abc123",
    "platform": "facebook",
    "competitor": "nutifood",
    "brand": "growplus-nutifood",
    "processed_posts": 25,
    "bigquery_tables_updated": ["facebook_posts"],
    "processing_duration_seconds": 45.2,
    "status": "completed"
  }
}
```

### **Batch Media Event (New Format)**
```json
{
  "event_type": "batch-media-download-requested",
  "timestamp": "2025-07-14T10:00:00Z",
  "source": "data-processing-service", 
  "version": "2.0",
  "data": {
    "batch_summary": {
      "platform": "youtube",
      "total_posts": 10,
      "total_media_items": 20,
      "video_count": 10,
      "image_count": 10
    },
    "media_batch": [
      {
        "url": "https://www.youtube.com/watch?v=VIDEO_ID",
        "type": "video",
        "media_id": "VIDEO_ID",
        "post_id": "POST_ID",
        "metadata": {"duration": "PT3M45S", "title": "Video Title"}
      }
    ],
    "crawl_metadata": {
      "crawl_id": "abc123",
      "platform": "youtube",
      "competitor": "nutifood"
    },
    "processing_config": {
      "mode": "batch",
      "parallel_downloads": 10,
      "timeout_seconds": 630
    }
  }
}
```

## 🔗 Related Files

- **Multi-Platform Media Detector**: `handlers/multi_platform_media_detector.py`
- **Test Scripts**: `tests/test_unified_events.py`, `tests/test_batch_media_system.py`
- **Legacy Files**: `events/event_publisher.py`, `events/media_event_publisher.py`, `events/batch_media_event_publisher.py`

## ✅ Migration Checklist

### **For Each Service/Module:**
- [ ] Update imports to use new events package
- [ ] Replace individual media events with batch events
- [ ] Add platform field to crawl metadata
- [ ] Add competitor/brand/category fields to crawl metadata  
- [ ] Update error handling to check `result['success']`
- [ ] Test event publishing in development environment
- [ ] Update documentation/README files
- [ ] Remove unused legacy imports (optional, for cleanup)

### **System-Wide:**
- [ ] Update Pub/Sub topic subscriptions if needed
- [ ] Update media-processing service to handle batch events
- [ ] Update monitoring/alerting for new event types
- [ ] Update deployment scripts with new topic names