"""
Events Package for Data Processing Service.

This package provides unified event publishing functionality:
- DataProcessingEventPublisher: Lifecycle events (completed, failed, etc.)
- MediaEventPublisher: Media processing events (individual and batch modes)
- Convenience functions for easy integration

Usage Examples:
    # Data processing events
    from events import publish_processing_completed
    result = publish_processing_completed(crawl_metadata, stats)
    
    # Batch media events (recommended)
    from events import publish_batch_media_events  
    result = publish_batch_media_events(raw_posts, platform, metadata)
    
    # Individual media events (legacy)
    from events import publish_individual_media_events
    result = publish_individual_media_events(post_data, platform, metadata)
    
    # Direct class usage
    from events.publishers import DataProcessingEventPublisher, MediaEventPublisher
    publisher = MediaEventPublisher()
    result = publisher.publish_batch_media_event(...)
"""

# Import main classes
from .publishers import (
    BaseEventPublisher,
    DataProcessingEventPublisher, 
    MediaEventPublisher
)

# Import convenience functions
from .publishers import (
    publish_processing_completed,
    publish_batch_media_events,
    publish_individual_media_events
)

# Legacy imports for backward compatibility
try:
    from .event_publisher import EventPublisher as LegacyEventPublisher
except ImportError:
    LegacyEventPublisher = None

__all__ = [
    # Main classes
    'BaseEventPublisher',
    'DataProcessingEventPublisher',
    'MediaEventPublisher',
    
    # Convenience functions
    'publish_processing_completed',
    'publish_batch_media_events', 
    'publish_individual_media_events',
    
    # Legacy
    'LegacyEventPublisher'
]