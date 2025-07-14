"""
Unified Event Publishers for Data Processing Service.

This module consolidates all event publishing functionality into organized classes:
- DataProcessingEventPublisher: For data processing lifecycle events
- MediaEventPublisher: For media processing events (both individual and batch)
- BaseEventPublisher: Common functionality shared across publishers
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from google.cloud import pubsub_v1

# Import multi-platform media detector
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from handlers.multi_platform_media_detector import MultiPlatformMediaDetector

logger = logging.getLogger(__name__)


class BaseEventPublisher(ABC):
    """
    Base class for all event publishers with common functionality.
    """
    
    def __init__(self, project_id: Optional[str] = None, topic_prefix: str = "social-analytics"):
        """
        Initialize base event publisher.
        
        Args:
            project_id: Google Cloud project ID
            topic_prefix: Prefix for Pub/Sub topic names
        """
        self.project_id = project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
        self.topic_prefix = topic_prefix
        
        if not self.project_id:
            raise ValueError("Google Cloud project ID must be provided")
        
        try:
            self.publisher = pubsub_v1.PublisherClient()
            logger.info(f"Initialized {self.__class__.__name__} for project: {self.project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Pub/Sub publisher: {e}")
            raise
    
    def publish_event(self, event_type: str, event_data: Dict[str, Any], 
                     topic_name: Optional[str] = None, **attributes) -> Dict[str, Any]:
        """
        Publish event to Pub/Sub topic.
        
        Args:
            event_type: Type of event
            event_data: Event payload data
            topic_name: Custom topic name (defaults to prefixed event_type)
            **attributes: Additional message attributes
            
        Returns:
            Publishing result dictionary
        """
        try:
            # Determine topic
            topic = topic_name or f"{self.topic_prefix}-{event_type}"
            topic_path = self.publisher.topic_path(self.project_id, topic)
            
            # Create message
            message = {
                'event_type': event_type,
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'data-processing-service',
                'version': '2.0',
                'data': event_data
            }
            
            # Add attributes
            message_attributes = {
                'event_type': event_type,
                'source': 'data-processing-service',
                **attributes
            }
            
            # Publish
            message_data = json.dumps(message, ensure_ascii=False).encode('utf-8')
            future = self.publisher.publish(topic_path, message_data, **message_attributes)
            message_id = future.result(timeout=10)
            
            logger.info(f"✅ Published {event_type} event to {topic} (ID: {message_id})")
            
            return {
                'success': True,
                'message_id': message_id,
                'topic': topic,
                'event_type': event_type,
                'data_size': len(message_data)
            }
            
        except Exception as e:
            error_msg = f"Failed to publish {event_type} event: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'event_type': event_type
            }
    
    def close(self):
        """Close the publisher client."""
        if hasattr(self, 'publisher'):
            logger.info(f"{self.__class__.__name__} closed")


class DataProcessingEventPublisher(BaseEventPublisher):
    """
    Publisher for data processing lifecycle events.
    
    Handles events like:
    - Data processing completed
    - Data processing failed  
    - BigQuery insertion completed
    """
    
    def publish_processing_completed(self, crawl_metadata: Dict[str, Any], 
                                   processing_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish data processing completed event.
        
        Args:
            crawl_metadata: Crawl context (crawl_id, snapshot_id, platform, etc.)
            processing_stats: Processing statistics (post_count, tables_updated, etc.)
        """
        event_data = {
            # Crawl context
            'crawl_id': crawl_metadata.get('crawl_id'),
            'snapshot_id': crawl_metadata.get('snapshot_id'),
            'platform': crawl_metadata.get('platform'),
            'competitor': crawl_metadata.get('competitor'),
            'brand': crawl_metadata.get('brand'),
            'category': crawl_metadata.get('category'),
            
            # Processing results
            'processed_posts': processing_stats.get('processed_posts', 0),
            'bigquery_tables_updated': processing_stats.get('bigquery_tables', []),
            'gcs_files_created': processing_stats.get('gcs_files', []),
            'processing_duration_seconds': processing_stats.get('duration_seconds'),
            
            # Status
            'status': 'completed',
            'processing_timestamp': datetime.utcnow().isoformat()
        }
        
        return self.publish_event(
            'data-processing-completed',
            event_data,
            crawl_id=crawl_metadata.get('crawl_id', ''),
            platform=crawl_metadata.get('platform', ''),
            competitor=crawl_metadata.get('competitor', '')
        )
    
    def publish_processing_failed(self, crawl_metadata: Dict[str, Any], 
                                error_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish data processing failed event.
        
        Args:
            crawl_metadata: Crawl context
            error_info: Error details (error_message, error_type, failed_step, etc.)
        """
        event_data = {
            # Crawl context
            'crawl_id': crawl_metadata.get('crawl_id'),
            'snapshot_id': crawl_metadata.get('snapshot_id'),
            'platform': crawl_metadata.get('platform'),
            
            # Error details
            'error_message': error_info.get('error_message'),
            'error_type': error_info.get('error_type', 'processing_error'),
            'failed_step': error_info.get('failed_step'),
            'partial_results': error_info.get('partial_results', {}),
            
            # Status
            'status': 'failed',
            'failure_timestamp': datetime.utcnow().isoformat()
        }
        
        return self.publish_event(
            'data-processing-failed',
            event_data,
            crawl_id=crawl_metadata.get('crawl_id', ''),
            platform=crawl_metadata.get('platform', ''),
            error_type=error_info.get('error_type', 'unknown')
        )
    
    def publish_bigquery_completed(self, crawl_metadata: Dict[str, Any], 
                                  bigquery_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish BigQuery insertion completed event.
        
        Args:
            crawl_metadata: Crawl context
            bigquery_stats: BigQuery operation statistics
        """
        event_data = {
            'crawl_id': crawl_metadata.get('crawl_id'),
            'platform': crawl_metadata.get('platform'),
            'table_name': bigquery_stats.get('table_name'),
            'rows_inserted': bigquery_stats.get('rows_inserted', 0),
            'insertion_duration_seconds': bigquery_stats.get('duration_seconds'),
            'status': 'completed'
        }
        
        return self.publish_event(
            'bigquery-insertion-completed',
            event_data,
            table_name=bigquery_stats.get('table_name', ''),
            platform=crawl_metadata.get('platform', '')
        )


class MediaEventPublisher(BaseEventPublisher):
    """
    Publisher for media processing events.
    
    Supports both individual and batch media event publishing modes:
    - Individual: One event per media URL (legacy)
    - Batch: One event per raw data file (recommended)
    """
    
    def __init__(self, project_id: Optional[str] = None, 
                 individual_topic: str = "media-processing-requests",
                 batch_topic: str = "batch-media-processing-requests"):
        """
        Initialize media event publisher.
        
        Args:
            project_id: Google Cloud project ID
            individual_topic: Topic for individual media events
            batch_topic: Topic for batch media events
        """
        super().__init__(project_id, topic_prefix="")  # No prefix for media topics
        self.individual_topic = individual_topic
        self.batch_topic = batch_topic
        self.media_detector = MultiPlatformMediaDetector()
    
    def publish_individual_media_events(self, post_data: Dict, platform: str, 
                                      crawl_metadata: Dict) -> Dict[str, Any]:
        """
        Publish individual events for each media URL in a post (legacy mode).
        
        Args:
            post_data: Single post data
            platform: Platform name
            crawl_metadata: Crawl context
            
        Returns:
            Publishing results
        """
        try:
            media_urls = self._extract_platform_media_urls(post_data, platform)
            
            if not media_urls:
                return {
                    'success': True,
                    'message': 'No media URLs found',
                    'events_published': 0
                }
            
            published_count = 0
            failed_count = 0
            
            for media_url in media_urls:
                event_data = self._create_individual_media_event(
                    media_url, platform, post_data, crawl_metadata
                )
                
                result = self.publish_event(
                    'media-download-requested',
                    event_data,
                    topic_name=self.individual_topic,
                    platform=platform,
                    media_type=media_url['type'],
                    crawl_id=crawl_metadata.get('crawl_id', '')
                )
                
                if result['success']:
                    published_count += 1
                else:
                    failed_count += 1
            
            return {
                'success': failed_count == 0,
                'events_published': published_count,
                'events_failed': failed_count,
                'total_media_urls': len(media_urls)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'events_published': 0
            }
    
    def publish_batch_media_event(self, raw_posts: List[Dict], platform: str, 
                                crawl_metadata: Dict, file_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Publish a single batch event for all media URLs in a file (recommended mode).
        
        Args:
            raw_posts: List of raw posts from fixture/GCS file
            platform: Platform name
            crawl_metadata: Crawl context
            file_metadata: Optional file metadata
            
        Returns:
            Publishing result
        """
        try:
            # Detect all media in the batch
            batch_result = self.media_detector.detect_media_batch(raw_posts, platform)
            
            if batch_result['total_media_items'] == 0:
                return {
                    'success': True,
                    'message': 'No media found in batch',
                    'media_count': 0
                }
            
            # Create batch event
            event_data = self._create_batch_media_event(batch_result, crawl_metadata, file_metadata)
            
            # Publish single batch event
            result = self.publish_event(
                'batch-media-download-requested',
                event_data,
                topic_name=self.batch_topic,
                platform=platform,
                batch_size=str(batch_result['total_media_items']),
                crawl_id=crawl_metadata.get('crawl_id', ''),
                has_videos=str(batch_result['total_videos'] > 0)
            )
            
            if result['success']:
                result.update({
                    'media_count': batch_result['total_media_items'],
                    'video_count': batch_result['total_videos'],
                    'image_count': batch_result['total_images'],
                    'batch_mode': True
                })
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'media_count': 0
            }
    
    def _extract_platform_media_urls(self, post_data: Dict, platform: str) -> List[Dict]:
        """Extract media URLs using platform-specific logic."""
        if platform == "facebook":
            return self._extract_facebook_media_urls(post_data)
        elif platform == "tiktok":
            return self._extract_tiktok_media_urls(post_data)
        elif platform == "youtube":
            return self._extract_youtube_media_urls(post_data)
        else:
            logger.warning(f"Unknown platform: {platform}")
            return []
    
    def _extract_facebook_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract Facebook media URLs."""
        media_urls = []
        
        # Process attachments
        for attachment in post_data.get('attachments', []):
            if not isinstance(attachment, dict):
                continue
            
            # Videos
            if attachment.get('video_url'):
                # Convert video_length from milliseconds to seconds
                duration_ms = attachment.get('video_length')
                duration_seconds = int(duration_ms) / 1000 if duration_ms else None
                
                media_urls.append({
                    'url': attachment['video_url'],
                    'type': 'video',
                    'media_id': attachment.get('id'),
                    'thumbnail_url': attachment.get('url'),
                    'metadata': {
                        'duration': duration_seconds,  # Duration in seconds (converted from milliseconds)
                        'duration_ms': attachment.get('video_length'),  # Original duration in milliseconds
                        'width': attachment.get('width'),
                        'height': attachment.get('height'),
                        'attachment_url': attachment.get('attachment_url')
                    }
                })
            
            # Images
            elif attachment.get('url') and attachment.get('type', '').lower() in ['photo', 'image']:
                media_urls.append({
                    'url': attachment['url'],
                    'type': 'image',
                    'media_id': attachment.get('id'),
                    'metadata': {
                        'width': attachment.get('width'),
                        'height': attachment.get('height')
                    }
                })
        
        # Profile images
        if post_data.get('page_logo'):
            media_urls.append({
                'url': post_data['page_logo'],
                'type': 'profile_image',
                'media_id': 'page_logo'
            })
        
        return media_urls
    
    def _extract_tiktok_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract TikTok media URLs."""
        media_urls = []
        
        video_meta = post_data.get('videoMeta', {})
        
        # Video URL
        if post_data.get('webVideoUrl'):
            media_urls.append({
                'url': post_data['webVideoUrl'],
                'type': 'video',
                'media_id': post_data.get('id'),
                'metadata': {
                    'duration': video_meta.get('duration'),  # Duration in seconds
                    'width': video_meta.get('width'),
                    'height': video_meta.get('height'),
                    'format': video_meta.get('format'),
                    'definition': video_meta.get('definition')
                }
            })
        
        # Cover image
        if video_meta.get('coverUrl'):
            media_urls.append({
                'url': video_meta['coverUrl'],
                'type': 'thumbnail',
                'media_id': f"{post_data.get('id')}_cover",
                'metadata': {
                    'width': video_meta.get('width'),
                    'height': video_meta.get('height')
                }
            })
        
        # Author avatar
        author_meta = post_data.get('authorMeta', {})
        if author_meta.get('avatar'):
            media_urls.append({
                'url': author_meta['avatar'],
                'type': 'profile_image',
                'media_id': f"{author_meta.get('id')}_avatar"
            })
        
        return media_urls
    
    def _extract_youtube_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract YouTube media URLs."""
        media_urls = []
        
        # Video URL - ALWAYS include
        video_url = post_data.get('url') or post_data.get('video_url')
        if video_url:
            media_urls.append({
                'url': video_url,
                'type': 'video',
                'media_id': post_data.get('id'),
                'metadata': {
                    'duration': self._parse_youtube_duration(post_data.get('duration')),  # Duration in seconds
                    'duration_original': post_data.get('duration'),  # Original format: "HH:MM:SS"
                    'title': post_data.get('title'),
                    'view_count': post_data.get('viewCount'),
                    'likes': post_data.get('likes')
                }
            })
        
        # Thumbnail
        if post_data.get('thumbnailUrl'):
            media_urls.append({
                'url': post_data['thumbnailUrl'],
                'type': 'thumbnail',
                'media_id': f"{post_data.get('id')}_thumbnail"
            })
        
        # Channel avatar
        if post_data.get('channelAvatarUrl'):
            media_urls.append({
                'url': post_data['channelAvatarUrl'],
                'type': 'profile_image',
                'media_id': f"{post_data.get('channelId')}_avatar"
            })
        
        # Channel banner
        if post_data.get('channelBannerUrl'):
            media_urls.append({
                'url': post_data['channelBannerUrl'],
                'type': 'banner_image',
                'media_id': f"{post_data.get('channelId')}_banner"
            })
        
        return media_urls
    
    def _parse_youtube_duration(self, duration_str: str) -> Optional[int]:
        """Parse YouTube duration string (HH:MM:SS) to seconds."""
        if not duration_str:
            return None
        
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = map(int, parts)
                return hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:  # MM:SS
                minutes, seconds = map(int, parts)
                return minutes * 60 + seconds
            elif len(parts) == 1:  # SS
                return int(parts[0])
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse YouTube duration: {duration_str}")
        
        return None
    
    def _organize_media_by_groups(self, media_urls: List[Dict]) -> Dict[str, List[Dict]]:
        """Organize media URLs by type groups for better batch processing."""
        groups = {
            'videos': [],
            'images': [],
            'thumbnails': [],
            'profile_images': [],
            'banner_images': []
        }
        
        for media in media_urls:
            media_type = media.get('type', 'unknown')
            
            if media_type == 'video':
                groups['videos'].append(media)
            elif media_type == 'image':
                groups['images'].append(media)
            elif media_type == 'thumbnail':
                groups['thumbnails'].append(media)
            elif media_type == 'profile_image':
                groups['profile_images'].append(media)
            elif media_type == 'banner_image':
                groups['banner_images'].append(media)
            else:
                # Default to images for unknown types
                groups['images'].append(media)
        
        return groups
    
    def _create_individual_media_event(self, media_url: Dict, platform: str, 
                                     post_data: Dict, crawl_metadata: Dict) -> Dict[str, Any]:
        """Create individual media event data."""
        return {
            'media_url': media_url['url'],
            'media_type': media_url['type'],
            'media_id': media_url.get('media_id'),
            'platform': platform,
            'post_id': post_data.get('id') or post_data.get('post_id'),
            'crawl_id': crawl_metadata.get('crawl_id'),
            'snapshot_id': crawl_metadata.get('snapshot_id'),
            'competitor': crawl_metadata.get('competitor'),
            'brand': crawl_metadata.get('brand'),
            'category': crawl_metadata.get('category'),
            'processing_mode': 'individual'
        }
    
    def _create_batch_media_event(self, batch_result: Dict, crawl_metadata: Dict, 
                                file_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Create batch media event data with organized media groups."""
        # Organize media by type groups
        media_groups = self._organize_media_by_groups(batch_result['all_media_urls'])
        
        return {
            'batch_summary': {
                'platform': batch_result['platform'],
                'total_posts': batch_result['total_posts'],
                'total_media_items': batch_result['total_media_items'],
                'video_count': batch_result['total_videos'],
                'image_count': batch_result['total_images'],
                'groups': {
                    'videos': len(media_groups['videos']),
                    'images': len(media_groups['images']),
                    'thumbnails': len(media_groups['thumbnails']),
                    'profile_images': len(media_groups['profile_images']),
                    'banner_images': len(media_groups['banner_images'])
                }
            },
            'media_groups': media_groups,
            'crawl_metadata': crawl_metadata,
            'file_metadata': file_metadata or {},
            'processing_config': {
                'mode': 'batch',
                'parallel_downloads': min(10, batch_result['total_media_items']),
                'timeout_seconds': 60 + (batch_result['total_media_items'] * 30),
                'priority_order': ['videos', 'images', 'thumbnails', 'profile_images', 'banner_images']
            }
        }


# Convenience functions for backward compatibility
def publish_processing_completed(crawl_metadata: Dict, processing_stats: Dict, 
                               project_id: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to publish processing completed event."""
    publisher = DataProcessingEventPublisher(project_id)
    try:
        return publisher.publish_processing_completed(crawl_metadata, processing_stats)
    finally:
        publisher.close()


def publish_batch_media_events(raw_posts: List[Dict], platform: str, crawl_metadata: Dict,
                              file_metadata: Optional[Dict] = None, 
                              project_id: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to publish batch media events."""
    publisher = MediaEventPublisher(project_id)
    try:
        return publisher.publish_batch_media_event(raw_posts, platform, crawl_metadata, file_metadata)
    finally:
        publisher.close()


def publish_individual_media_events(post_data: Dict, platform: str, crawl_metadata: Dict,
                                   project_id: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to publish individual media events."""
    publisher = MediaEventPublisher(project_id)
    try:
        return publisher.publish_individual_media_events(post_data, platform, crawl_metadata)
    finally:
        publisher.close()