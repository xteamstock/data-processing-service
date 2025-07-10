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
                                         posts_with_media: List[Dict], media_info: Dict[str, Any] = None) -> bool:
        """
        Publish media processing requested event with enhanced media information.
        
        Args:
            crawl_id: Crawl identifier
            snapshot_id: Snapshot identifier  
            posts_with_media: List of posts containing media
            media_info: Enhanced media information from MediaDetector
        """
        # Use enhanced media info if provided, otherwise extract from posts
        if media_info:
            event_data = {
                'crawl_id': crawl_id,
                'snapshot_id': snapshot_id,
                'posts_with_media_count': len(posts_with_media),
                'total_media_count': media_info['total_media_count'],
                'video_count': media_info['video_count'],
                'image_count': media_info['image_count'],
                'video_urls': media_info['video_urls'],
                'image_urls': media_info['image_urls'],
                'post_media_mapping': media_info['post_media_mapping'],
                'status': 'requested'
            }
        else:
            # Fallback to legacy extraction
            media_files = []
            for post in posts_with_media:
                attachments = post.get('media_metadata', {}).get('attachments', [])
                for attachment in attachments:
                    media_files.append({
                        'post_id': post.get('post_id'),
                        'attachment_id': attachment.get('id'),
                        'type': attachment.get('type'),
                        'url': attachment.get('url'),
                        'attachment_url': attachment.get('attachment_url')
                    })
            
            event_data = {
                'crawl_id': crawl_id,
                'snapshot_id': snapshot_id,
                'posts_with_media': len(posts_with_media),
                'media_files': media_files,
                'total_media_count': len(media_files),
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