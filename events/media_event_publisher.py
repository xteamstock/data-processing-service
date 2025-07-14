"""
Media Event Publisher for multi-platform media processing requests.

This module handles publishing events to trigger media processing for
social media posts that contain downloadable media (videos, images).
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class MediaEventPublisher:
    """Publisher for media processing events."""
    
    def __init__(self, project_id: Optional[str] = None, topic_name: str = "media-processing-requests"):
        """
        Initialize media event publisher.
        
        Args:
            project_id: Google Cloud project ID
            topic_name: Pub/Sub topic name for media processing events
        """
        self.project_id = project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
        self.topic_name = topic_name
        
        if not self.project_id:
            raise ValueError("Google Cloud project ID must be provided via parameter or GOOGLE_CLOUD_PROJECT environment variable")
        
        try:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
            logger.info(f"Initialized MediaEventPublisher for topic: {self.topic_path}")
        except Exception as e:
            logger.error(f"Failed to initialize Pub/Sub publisher: {e}")
            raise
    
    def publish_media_events(self, post_data: Dict, platform: str, crawl_metadata: Dict) -> int:
        """
        Publish media download events for each media URL found in post.
        
        Args:
            post_data: Raw or processed post data from platform
            platform: Platform name (facebook, tiktok, youtube)
            crawl_metadata: Crawl context (crawl_id, competitor, brand, etc.)
            
        Returns:
            Number of media events published
        """
        try:
            media_urls = self._extract_platform_media_urls(post_data, platform)
            
            if not media_urls:
                logger.debug(f"No media URLs found for {platform} post {post_data.get('id', 'unknown')}")
                return 0
            
            published_count = 0
            
            for media_url in media_urls:
                try:
                    event = self._create_media_event(media_url, platform, post_data, crawl_metadata)
                    
                    # Publish event to Pub/Sub
                    future = self.publisher.publish(
                        self.topic_path,
                        json.dumps(event).encode('utf-8'),
                        platform=platform,
                        media_type=media_url['type'],
                        crawl_id=crawl_metadata.get('crawl_id', ''),
                        competitor=crawl_metadata.get('competitor', ''),
                        brand=crawl_metadata.get('brand', '')
                    )
                    
                    published_count += 1
                    logger.debug(f"Published media event for {platform} {media_url['type']}: {media_url['url']}")
                    
                except Exception as e:
                    logger.error(f"Failed to publish media event for {media_url.get('url', 'unknown')}: {e}")
            
            if published_count > 0:
                logger.info(f"Published {published_count} media events for {platform} post {post_data.get('id', 'unknown')}")
            
            return published_count
            
        except Exception as e:
            logger.error(f"Error publishing media events for {platform} post: {e}")
            return 0
    
    def _extract_platform_media_urls(self, post_data: Dict, platform: str) -> List[Dict]:
        """Extract media URLs based on platform."""
        if platform == "facebook":
            return self._extract_facebook_media_urls(post_data)
        elif platform == "tiktok":
            return self._extract_tiktok_media_urls(post_data)
        elif platform == "youtube":
            return self._extract_youtube_media_urls(post_data)
        else:
            logger.warning(f"Unknown platform for media extraction: {platform}")
            return []
    
    def _extract_facebook_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract Facebook media URLs for download."""
        media_urls = []
        
        # Extract from attachments
        for attachment in post_data.get('attachments', []):
            if not isinstance(attachment, dict):
                continue
                
            attachment_id = attachment.get('id')
            attachment_type = attachment.get('type', '').lower()
            
            # Video attachments
            if attachment.get('video_url'):
                media_urls.append({
                    'url': attachment['video_url'],
                    'type': 'video',
                    'attachment_id': attachment_id,
                    'thumbnail_url': attachment.get('url')
                })
            # Image/photo attachments
            elif attachment.get('url') and attachment_type in ['photo', 'image']:
                media_urls.append({
                    'url': attachment['url'], 
                    'type': 'image',
                    'attachment_id': attachment_id
                })
        
        # Additional media from post metadata
        if post_data.get('page_logo'):
            media_urls.append({
                'url': post_data['page_logo'],
                'type': 'profile_image',
                'attachment_id': 'page_logo'
            })
        
        if post_data.get('post_image'):
            media_urls.append({
                'url': post_data['post_image'],
                'type': 'post_image',
                'attachment_id': 'post_image'
            })
        
        return media_urls
    
    def _extract_tiktok_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract TikTok media URLs for download."""
        media_urls = []
        
        # Main video URL
        if post_data.get('webVideoUrl'):
            media_urls.append({
                'url': post_data['webVideoUrl'],
                'type': 'video',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        # Cover/thumbnail image
        video_meta = post_data.get('videoMeta', {})
        if video_meta.get('coverUrl'):
            media_urls.append({
                'url': video_meta['coverUrl'],
                'type': 'thumbnail',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        # Author avatar
        author_meta = post_data.get('authorMeta', {})
        if author_meta.get('avatar'):
            media_urls.append({
                'url': author_meta['avatar'],
                'type': 'profile_image',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        return media_urls
    
    def _extract_youtube_media_urls(self, post_data: Dict) -> List[Dict]:
        """Extract YouTube media URLs for download."""
        media_urls = []
        
        # Main video URL - ALWAYS include as requested
        video_url = post_data.get('url') or post_data.get('video_url')
        if video_url:
            media_urls.append({
                'url': video_url,
                'type': 'video',
                'video_id': post_data.get('id') or post_data.get('video_id'),
                'platform_specific': True,
                'duration': post_data.get('duration'),
                'note': 'YouTube video URL for reference/analysis'
            })
        
        # Video thumbnail
        if post_data.get('thumbnailUrl'):
            media_urls.append({
                'url': post_data['thumbnailUrl'],
                'type': 'thumbnail',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        # Channel avatar
        channel_metadata = post_data.get('channel_metadata', {})
        avatar_url = channel_metadata.get('avatar_url') or post_data.get('channelAvatarUrl')
        if avatar_url:
            media_urls.append({
                'url': avatar_url,
                'type': 'profile_image',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        # Channel banner
        banner_url = channel_metadata.get('banner_url') or post_data.get('channelBannerUrl')
        if banner_url:
            media_urls.append({
                'url': banner_url,
                'type': 'banner_image',
                'video_id': post_data.get('id'),
                'platform_specific': True
            })
        
        return media_urls
    
    def _create_media_event(self, media_url: Dict, platform: str, post_data: Dict, crawl_metadata: Dict) -> Dict:
        """Create media processing event."""
        event_id = self._generate_event_id(media_url, crawl_metadata, post_data)
        
        return {
            "event_type": "media-download-requested",
            "timestamp": datetime.utcnow().isoformat(),
            "event_id": event_id,
            "version": "1.0",
            "data": {
                # Media details
                "media_url": media_url['url'],
                "media_type": media_url['type'],  # video, image, thumbnail, profile_image, banner_image
                "media_id": media_url.get('attachment_id', media_url.get('video_id')),
                
                # Platform context
                "platform": platform,
                "post_id": post_data.get('id', post_data.get('post_id')),
                "post_url": self._get_post_url(post_data, platform),
                
                # Crawl context
                "crawl_id": crawl_metadata.get('crawl_id'),
                "snapshot_id": crawl_metadata.get('snapshot_id'),
                "competitor": crawl_metadata.get('competitor'),
                "brand": crawl_metadata.get('brand'),
                "category": crawl_metadata.get('category'),
                
                # Storage configuration
                "storage_path": self._generate_media_storage_path(
                    media_url, platform, crawl_metadata, post_data
                ),
                "bucket_name": os.environ.get('MEDIA_STORAGE_BUCKET', 'social-analytics-media'),
                
                # Processing configuration
                "retry_count": 0,
                "max_retries": int(os.environ.get('MEDIA_MAX_RETRIES', '3')),
                "priority": media_url.get('priority', 'normal'),  # normal, high, low
                "timeout_seconds": int(os.environ.get('MEDIA_DOWNLOAD_TIMEOUT', '60')),
                
                # Additional metadata
                "thumbnail_url": media_url.get('thumbnail_url'),
                "platform_specific": media_url.get('platform_specific', False),
                "file_size_limit": os.environ.get('MEDIA_MAX_FILE_SIZE', '100MB')
            }
        }
    
    def _generate_event_id(self, media_url: Dict, crawl_metadata: Dict, post_data: Dict) -> str:
        """Generate unique event ID for media processing."""
        crawl_id = crawl_metadata.get('crawl_id', 'unknown')
        post_id = post_data.get('id', post_data.get('post_id', 'unknown'))
        media_id = media_url.get('attachment_id', media_url.get('video_id', 'unknown'))
        
        return f"{crawl_id}_{post_id}_{media_id}_{media_url['type']}"
    
    def _get_post_url(self, post_data: Dict, platform: str) -> str:
        """Get platform-specific post URL."""
        if platform == "facebook":
            return post_data.get('url', '')
        elif platform == "tiktok":
            return post_data.get('webVideoUrl', '')
        elif platform == "youtube":
            return post_data.get('url', '')
        else:
            return post_data.get('url', '')
    
    def _generate_media_storage_path(self, media_url: Dict, platform: str, crawl_metadata: Dict, post_data: Dict) -> str:
        """Generate GCS storage path for downloaded media."""
        # Extract date for partitioning
        date_posted = post_data.get('date_posted', post_data.get('createTimeISO', datetime.utcnow().isoformat()))
        try:
            if isinstance(date_posted, str):
                date_obj = datetime.fromisoformat(date_posted.replace('Z', '+00:00'))
            else:
                date_obj = datetime.utcnow()
        except (ValueError, TypeError):
            date_obj = datetime.utcnow()
        
        # Generate file extension
        media_type = media_url['type']
        media_id = media_url.get('attachment_id', media_url.get('video_id', 'unknown'))
        file_extension = self._get_file_extension(media_url['url'], media_type)
        
        # Create hierarchical path
        return (
            f"media/{platform}/"
            f"competitor={crawl_metadata.get('competitor', 'unknown')}/"
            f"brand={crawl_metadata.get('brand', 'unknown')}/"
            f"category={crawl_metadata.get('category', 'unknown')}/"
            f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}/"
            f"{media_type}s/"
            f"{media_id}{file_extension}"
        )
    
    def _get_file_extension(self, url: str, media_type: str) -> str:
        """Determine file extension from URL or media type."""
        url_lower = url.lower()
        
        # Check URL for explicit extension
        if '.mp4' in url_lower:
            return '.mp4'
        elif '.jpg' in url_lower or '.jpeg' in url_lower:
            return '.jpg'
        elif '.png' in url_lower:
            return '.png'
        elif '.gif' in url_lower:
            return '.gif'
        elif '.webp' in url_lower:
            return '.webp'
        
        # Fallback based on media type
        if media_type == 'video':
            return '.mp4'
        else:
            return '.jpg'  # Default for images
    
    def publish_batch_media_events(self, posts_batch: List[Dict], platform: str, crawl_metadata: Dict) -> int:
        """
        Publish media events for a batch of posts.
        
        Args:
            posts_batch: List of posts to process
            platform: Platform name
            crawl_metadata: Crawl context
            
        Returns:
            Total number of media events published
        """
        total_published = 0
        
        for post in posts_batch:
            try:
                published_count = self.publish_media_events(post, platform, crawl_metadata)
                total_published += published_count
            except Exception as e:
                logger.error(f"Failed to publish media events for post {post.get('id', 'unknown')}: {e}")
        
        logger.info(f"Published {total_published} total media events for {len(posts_batch)} posts")
        return total_published
    
    def close(self):
        """Close the publisher client."""
        if hasattr(self, 'publisher'):
            # Pub/Sub publisher doesn't need explicit closing in current version
            logger.info("MediaEventPublisher closed")


# Convenience function for backward compatibility
def publish_media_processing_events(posts: List[Dict], platform: str, crawl_metadata: Dict,
                                  project_id: Optional[str] = None) -> int:
    """
    Convenience function to publish media processing events.
    
    Args:
        posts: List of posts with media
        platform: Platform name
        crawl_metadata: Crawl metadata
        project_id: Google Cloud project ID
        
    Returns:
        Number of events published
    """
    publisher = MediaEventPublisher(project_id)
    return publisher.publish_batch_media_events(posts, platform, crawl_metadata)