# handlers/multi_platform_media_detector.py
# Enhanced media detection for Facebook, TikTok, and YouTube

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class MultiPlatformMediaDetector:
    """
    Detect and extract media URLs from social media posts across multiple platforms.
    
    Supports:
    - Facebook: attachments with videos/images
    - TikTok: video URLs and cover images
    - YouTube: video URLs and thumbnails
    """
    
    def __init__(self):
        """Initialize multi-platform media detector."""
        self.platform_extractors = {
            'facebook': self._extract_facebook_media,
            'tiktok': self._extract_tiktok_media,
            'youtube': self._extract_youtube_media
        }
    
    def detect_media_batch(self, posts: List[Dict[str, Any]], platform: str) -> Dict[str, Any]:
        """
        Detect all media in a batch of posts and prepare for batch event publishing.
        
        Args:
            posts: List of posts from a platform
            platform: Platform name (facebook, tiktok, youtube)
            
        Returns:
            Batch media detection result with all URLs grouped
        """
        if platform not in self.platform_extractors:
            logger.warning(f"Unknown platform: {platform}")
            return self._empty_batch_result()
        
        # Extract media from all posts
        all_media_urls = []
        posts_with_media = []
        total_videos = 0
        total_images = 0
        
        extractor = self.platform_extractors[platform]
        
        for post in posts:
            try:
                media_items = extractor(post)
                
                if media_items:
                    posts_with_media.append({
                        'post_id': self._get_post_id(post, platform),
                        'media_count': len(media_items),
                        'media_items': media_items
                    })
                    
                    for item in media_items:
                        # Add post context to each media item
                        item['post_id'] = self._get_post_id(post, platform)
                        item['post_url'] = self._get_post_url(post, platform)
                        item['date_posted'] = self._get_post_date(post, platform)
                        
                        all_media_urls.append(item)
                        
                        if item['type'] == 'video':
                            total_videos += 1
                        elif item['type'] in ['image', 'thumbnail', 'profile_image']:
                            total_images += 1
                            
            except Exception as e:
                logger.error(f"Error extracting media from {platform} post: {str(e)}")
                continue
        
        # Group media by type for batch processing
        video_urls = [m for m in all_media_urls if m['type'] == 'video']
        image_urls = [m for m in all_media_urls if m['type'] in ['image', 'thumbnail']]
        profile_images = [m for m in all_media_urls if m['type'] == 'profile_image']
        
        return {
            'platform': platform,
            'total_posts': len(posts),
            'posts_with_media': len(posts_with_media),
            'total_media_items': len(all_media_urls),
            'total_videos': total_videos,
            'total_images': total_images,
            'media_breakdown': {
                'videos': video_urls,
                'images': image_urls,
                'profile_images': profile_images
            },
            'all_media_urls': all_media_urls,
            'posts_media_mapping': posts_with_media,
            'batch_metadata': {
                'detection_timestamp': datetime.utcnow().isoformat(),
                'platform': platform
            }
        }
    
    def _extract_facebook_media(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract media from Facebook post."""
        media_items = []
        
        # Process attachments
        attachments = post.get('attachments', [])
        for idx, attachment in enumerate(attachments):
            if not isinstance(attachment, dict):
                continue
            
            attachment_id = attachment.get('id', f"fb_attach_{idx}")
            attachment_type = attachment.get('type', '').lower()
            
            # Video attachments
            video_url = attachment.get('video_url')
            if video_url:
                media_items.append({
                    'url': video_url,
                    'type': 'video',
                    'media_id': attachment_id,
                    'thumbnail_url': attachment.get('url'),
                    'metadata': {
                        'video_length': attachment.get('video_length'),
                        'media_type': attachment.get('media_type')
                    }
                })
            
            # Image attachments
            elif attachment_type in ['photo', 'image'] and attachment.get('url'):
                media_items.append({
                    'url': attachment['url'],
                    'type': 'image',
                    'media_id': attachment_id,
                    'metadata': {
                        'media_type': attachment.get('media_type')
                    }
                })
        
        # Page logo/profile image
        if post.get('page_logo'):
            media_items.append({
                'url': post['page_logo'],
                'type': 'profile_image',
                'media_id': 'page_logo'
            })
        
        # Post image (featured image)
        if post.get('post_image'):
            media_items.append({
                'url': post['post_image'],
                'type': 'image',
                'media_id': 'post_featured_image'
            })
        
        return media_items
    
    def _extract_tiktok_media(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract media from TikTok post."""
        media_items = []
        
        # Main video URL
        video_url = post.get('webVideoUrl') or post.get('video_url')
        if video_url:
            video_meta = post.get('videoMeta', {})
            media_items.append({
                'url': video_url,
                'type': 'video',
                'media_id': post.get('id', 'unknown'),
                'metadata': {
                    'duration': video_meta.get('duration'),
                    'height': video_meta.get('height'),
                    'width': video_meta.get('width'),
                    'format': video_meta.get('format')
                }
            })
        
        # Video cover/thumbnail
        video_meta = post.get('videoMeta', {})
        cover_url = video_meta.get('coverUrl') or video_meta.get('cover')
        if cover_url:
            media_items.append({
                'url': cover_url,
                'type': 'thumbnail',
                'media_id': f"{post.get('id', 'unknown')}_cover"
            })
        
        # Author avatar
        author_meta = post.get('authorMeta', {})
        avatar_url = author_meta.get('avatar')
        if avatar_url:
            media_items.append({
                'url': avatar_url,
                'type': 'profile_image',
                'media_id': f"{author_meta.get('id', 'unknown')}_avatar"
            })
        
        return media_items
    
    def _extract_youtube_media(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract media from YouTube post."""
        media_items = []
        
        # Main video URL - ALWAYS include
        video_url = post.get('url') or post.get('video_url')
        video_id = post.get('id') or post.get('video_id')
        
        if video_url:
            media_items.append({
                'url': video_url,
                'type': 'video',
                'media_id': video_id,
                'metadata': {
                    'duration': post.get('duration'),
                    'view_count': post.get('viewCount'),
                    'title': post.get('title')
                }
            })
        
        # Video thumbnail
        thumbnail_url = post.get('thumbnailUrl') or post.get('thumbnail')
        if thumbnail_url:
            media_items.append({
                'url': thumbnail_url,
                'type': 'thumbnail',
                'media_id': f"{video_id}_thumbnail"
            })
        
        # Channel avatar
        channel_metadata = post.get('channel_metadata', {})
        avatar_url = channel_metadata.get('avatar_url')
        if avatar_url:
            media_items.append({
                'url': avatar_url,
                'type': 'profile_image',
                'media_id': f"{post.get('channel_id', 'unknown')}_avatar"
            })
        
        # Channel banner
        banner_url = channel_metadata.get('banner_url')
        if banner_url:
            media_items.append({
                'url': banner_url,
                'type': 'banner_image',
                'media_id': f"{post.get('channel_id', 'unknown')}_banner"
            })
        
        return media_items
    
    def _get_post_id(self, post: Dict[str, Any], platform: str) -> str:
        """Get post ID based on platform."""
        if platform == 'facebook':
            return post.get('post_id') or post.get('id', 'unknown')
        elif platform == 'tiktok':
            return post.get('id', 'unknown')
        elif platform == 'youtube':
            return post.get('id') or post.get('video_id', 'unknown')
        return 'unknown'
    
    def _get_post_url(self, post: Dict[str, Any], platform: str) -> str:
        """Get post URL based on platform."""
        if platform == 'facebook':
            return post.get('post_url') or post.get('url', '')
        elif platform == 'tiktok':
            return post.get('webVideoUrl', '')
        elif platform == 'youtube':
            return post.get('url') or post.get('video_url', '')
        return ''
    
    def _get_post_date(self, post: Dict[str, Any], platform: str) -> str:
        """Get post date based on platform."""
        if platform == 'facebook':
            return post.get('date_posted', '')
        elif platform == 'tiktok':
            return post.get('createTimeISO', '')
        elif platform == 'youtube':
            return post.get('date') or post.get('publishedAt', '')
        return ''
    
    def _empty_batch_result(self) -> Dict[str, Any]:
        """Return empty batch result structure."""
        return {
            'platform': 'unknown',
            'total_posts': 0,
            'posts_with_media': 0,
            'total_media_items': 0,
            'total_videos': 0,
            'total_images': 0,
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            },
            'all_media_urls': [],
            'posts_media_mapping': [],
            'batch_metadata': {
                'detection_timestamp': datetime.utcnow().isoformat(),
                'platform': 'unknown'
            }
        }
    
    def prepare_batch_event(self, batch_result: Dict[str, Any], crawl_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a single batch event containing all media URLs.
        
        Args:
            batch_result: Result from detect_media_batch
            crawl_metadata: Crawl context (crawl_id, snapshot_id, competitor, etc.)
            
        Returns:
            Event payload for batch media processing
        """
        return {
            'event_type': 'batch-media-download-requested',
            'timestamp': datetime.utcnow().isoformat(),
            'event_id': f"{crawl_metadata.get('crawl_id')}_{crawl_metadata.get('snapshot_id')}_batch",
            'version': '2.0',
            'data': {
                # Batch information
                'batch_size': batch_result['total_media_items'],
                'platform': batch_result['platform'],
                
                # Media URLs grouped by type
                'media_urls': batch_result['all_media_urls'],
                'media_breakdown': batch_result['media_breakdown'],
                
                # Crawl context
                'crawl_metadata': {
                    'crawl_id': crawl_metadata.get('crawl_id'),
                    'snapshot_id': crawl_metadata.get('snapshot_id'),
                    'competitor': crawl_metadata.get('competitor'),
                    'brand': crawl_metadata.get('brand'),
                    'category': crawl_metadata.get('category'),
                    'crawl_date': crawl_metadata.get('crawl_date')
                },
                
                # Processing configuration
                'processing_config': {
                    'priority': 'normal',
                    'max_retries': 3,
                    'timeout_seconds': 300,  # 5 minutes for batch
                    'parallel_downloads': 10
                },
                
                # Storage configuration
                'storage_config': {
                    'bucket_name': 'social-analytics-media',
                    'use_hierarchical_path': True,
                    'preserve_filenames': False
                },
                
                # Batch metadata
                'batch_metadata': batch_result['batch_metadata'],
                'posts_count': batch_result['total_posts'],
                'posts_with_media_count': batch_result['posts_with_media']
            }
        }