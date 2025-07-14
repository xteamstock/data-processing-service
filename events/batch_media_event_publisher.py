"""
Batch Media Event Publisher for efficient media processing.

Sends all media URLs from a raw data file as a single batch event
instead of individual events per media URL.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import pubsub_v1

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from handlers.multi_platform_media_detector import MultiPlatformMediaDetector

logger = logging.getLogger(__name__)


class BatchMediaEventPublisher:
    """
    Publisher for batch media processing events.
    
    Instead of publishing individual events per media URL, this publisher
    groups all media URLs from a raw data file and sends them as a single
    batch event for more efficient processing.
    """
    
    def __init__(self, project_id: Optional[str] = None, topic_name: str = "batch-media-processing-requests"):
        """
        Initialize batch media event publisher.
        
        Args:
            project_id: Google Cloud project ID
            topic_name: Pub/Sub topic name for batch media processing events
        """
        self.project_id = project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
        self.topic_name = topic_name
        
        if not self.project_id:
            raise ValueError("Google Cloud project ID must be provided")
        
        try:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
            self.media_detector = MultiPlatformMediaDetector()
            logger.info(f"Initialized BatchMediaEventPublisher for topic: {self.topic_path}")
        except Exception as e:
            logger.error(f"Failed to initialize Batch Media Event Publisher: {e}")
            raise
    
    def publish_batch_from_raw_file(self, raw_posts: List[Dict], platform: str, 
                                  crawl_metadata: Dict, file_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Process a raw data file and publish a single batch event for all media.
        
        Args:
            raw_posts: List of raw posts from fixture/GCS file
            platform: Platform name (facebook, tiktok, youtube)
            crawl_metadata: Crawl context (crawl_id, snapshot_id, competitor, etc.)
            file_metadata: Optional file metadata (filename, size, etc.)
            
        Returns:
            Publishing result with stats
        """
        try:
            # Step 1: Detect all media in the batch
            logger.info(f"Detecting media in {len(raw_posts)} {platform} posts")
            batch_result = self.media_detector.detect_media_batch(raw_posts, platform)
            
            # Step 2: Check if there's any media to process
            if batch_result['total_media_items'] == 0:
                logger.info(f"No media found in {platform} batch, skipping event publication")
                return self._create_result(success=True, message="No media to process", stats=batch_result)
            
            # Step 3: Prepare batch event
            batch_event = self._create_batch_event(batch_result, crawl_metadata, file_metadata)
            
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
            
            logger.info(f"✅ Published batch media event for {platform}: "
                       f"{batch_result['total_media_items']} media items "
                       f"({batch_result['total_videos']} videos, {batch_result['total_images']} images)")
            
            return self._create_result(
                success=True,
                message=f"Published batch event with {batch_result['total_media_items']} media items",
                stats=batch_result,
                event_id=batch_event['event_id'],
                message_id=message_id
            )
            
        except Exception as e:
            error_msg = f"Failed to publish batch media event for {platform}: {str(e)}"
            logger.error(error_msg)
            return self._create_result(success=False, message=error_msg, error=str(e))
    
    def _create_batch_event(self, batch_result: Dict[str, Any], crawl_metadata: Dict[str, Any], 
                           file_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Create batch media processing event."""
        event_id = f"{crawl_metadata.get('crawl_id')}_{crawl_metadata.get('snapshot_id')}_batch_media"
        
        # Enhanced metadata including duration and file info
        enhanced_metadata = crawl_metadata.copy()
        if file_metadata:
            enhanced_metadata.update({
                'source_file': file_metadata.get('filename'),
                'file_size': file_metadata.get('size'),
                'processing_timestamp': datetime.utcnow().isoformat()
            })
        
        return {
            'event_type': 'batch-media-download-requested',
            'timestamp': datetime.utcnow().isoformat(),
            'event_id': event_id,
            'version': '2.0',
            'schema_version': 'batch-media-v2',
            'data': {
                # Batch summary
                'batch_summary': {
                    'platform': batch_result['platform'],
                    'total_posts': batch_result['total_posts'],
                    'posts_with_media': batch_result['posts_with_media'],
                    'total_media_items': batch_result['total_media_items'],
                    'media_counts': {
                        'videos': batch_result['total_videos'],
                        'images': batch_result['total_images'],
                        'thumbnails': len(batch_result['media_breakdown']['images']),
                        'profile_images': len(batch_result['media_breakdown']['profile_images'])
                    }
                },
                
                # All media URLs with context
                'media_batch': batch_result['all_media_urls'],
                
                # Organized by type for efficient processing
                'media_by_type': {
                    'videos': self._enrich_media_items(batch_result['media_breakdown']['videos'], 'video'),
                    'images': self._enrich_media_items(batch_result['media_breakdown']['images'], 'image'),
                    'profile_images': self._enrich_media_items(batch_result['media_breakdown']['profile_images'], 'profile')
                },
                
                # Crawl context with enhanced metadata
                'crawl_metadata': enhanced_metadata,
                
                # Processing instructions
                'processing_config': {
                    'priority': self._determine_priority(batch_result),
                    'max_retries': 3,
                    'timeout_seconds': self._calculate_timeout(batch_result['total_media_items']),
                    'parallel_downloads': min(10, batch_result['total_media_items']),
                    'preserve_quality': True,
                    'generate_thumbnails': True
                },
                
                # Storage configuration
                'storage_config': {
                    'bucket_name': os.environ.get('MEDIA_STORAGE_BUCKET', 'social-analytics-media'),
                    'base_path': self._generate_batch_storage_path(batch_result, enhanced_metadata),
                    'use_hierarchical_structure': True,
                    'compress_images': False,
                    'video_format_preference': ['mp4', 'webm', 'mov']
                },
                
                # Quality control
                'validation_config': {
                    'check_file_integrity': True,
                    'virus_scan': False,
                    'max_file_size': '500MB',
                    'allowed_video_formats': ['.mp4', '.webm', '.mov', '.avi'],
                    'allowed_image_formats': ['.jpg', '.png', '.gif', '.webp']
                },
                
                # Post-processing actions
                'post_processing': {
                    'update_bigquery': True,
                    'publish_completion_event': True,
                    'notify_on_failure': True,
                    'cleanup_temp_files': True
                }
            }
        }
    
    def _enrich_media_items(self, media_items: List[Dict], media_category: str) -> List[Dict]:
        """Enrich media items with additional processing metadata."""
        enriched = []
        for item in media_items:
            enriched_item = item.copy()
            enriched_item.update({
                'category': media_category,
                'priority': 'high' if media_category == 'video' else 'normal',
                'expected_size_mb': self._estimate_file_size(item),
                'processing_order': len(enriched) + 1
            })
            enriched.append(enriched_item)
        return enriched
    
    def _estimate_file_size(self, media_item: Dict) -> float:
        """Estimate file size based on media type and metadata."""
        media_type = media_item.get('type', 'image')
        metadata = media_item.get('metadata', {})
        
        if media_type == 'video':
            duration = metadata.get('duration', 30)  # seconds
            # Rough estimate: 1MB per second for typical social media video
            return float(duration) * 1.0
        else:
            # Estimate 500KB for images/thumbnails
            return 0.5
    
    def _determine_priority(self, batch_result: Dict) -> str:
        """Determine processing priority based on batch content."""
        video_ratio = batch_result['total_videos'] / max(1, batch_result['total_media_items'])
        
        if video_ratio > 0.5:
            return 'high'  # Video-heavy batches get priority
        elif batch_result['total_media_items'] > 50:
            return 'high'  # Large batches get priority
        else:
            return 'normal'
    
    def _calculate_timeout(self, media_count: int) -> int:
        """Calculate appropriate timeout based on media count."""
        # Base timeout: 60 seconds + 30 seconds per media item
        base_timeout = 60
        per_item_timeout = 30
        max_timeout = 1800  # 30 minutes max
        
        calculated = base_timeout + (media_count * per_item_timeout)
        return min(calculated, max_timeout)
    
    def _generate_batch_storage_path(self, batch_result: Dict, metadata: Dict) -> str:
        """Generate base storage path for the batch."""
        platform = batch_result['platform']
        competitor = metadata.get('competitor', 'unknown')
        brand = metadata.get('brand', 'unknown')
        category = metadata.get('category', 'unknown')
        
        # Use current date for organization
        now = datetime.utcnow()
        
        return (
            f"batch_media/{platform}/"
            f"competitor={competitor}/"
            f"brand={brand}/"
            f"category={category}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        )
    
    def _create_result(self, success: bool, message: str, stats: Optional[Dict] = None, 
                      event_id: Optional[str] = None, message_id: Optional[str] = None, 
                      error: Optional[str] = None) -> Dict[str, Any]:
        """Create standardized result dictionary."""
        result = {
            'success': success,
            'message': message,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if stats:
            result['stats'] = stats
        if event_id:
            result['event_id'] = event_id
        if message_id:
            result['message_id'] = message_id
        if error:
            result['error'] = error
            
        return result
    
    def close(self):
        """Close the publisher client."""
        if hasattr(self, 'publisher'):
            logger.info("BatchMediaEventPublisher closed")


# Convenience function for easy integration
def publish_batch_media_from_file(raw_posts: List[Dict], platform: str, crawl_metadata: Dict,
                                file_metadata: Optional[Dict] = None, project_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to publish batch media events from a raw data file.
    
    Args:
        raw_posts: List of raw posts from fixture/GCS file
        platform: Platform name (facebook, tiktok, youtube)
        crawl_metadata: Crawl context
        file_metadata: Optional file metadata
        project_id: Google Cloud project ID
        
    Returns:
        Publishing result
    """
    publisher = BatchMediaEventPublisher(project_id)
    try:
        return publisher.publish_batch_from_raw_file(raw_posts, platform, crawl_metadata, file_metadata)
    finally:
        publisher.close()