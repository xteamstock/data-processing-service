# handlers/media_detector.py
# NEW: Media detection logic for enhanced data processing service

import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class MediaDetector:
    """
    Detect and analyze media attachments in social media posts.
    
    This class implements media detection logic for the data processing service
    to identify video and image attachments that require separate processing.
    """
    
    def __init__(self):
        """Initialize media detector."""
        pass
    
    def detect_media_in_posts(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect media attachments in a list of posts and enhance with media metadata.
        
        Args:
            posts: List of processed posts
            
        Returns:
            List of posts enhanced with media metadata
        """
        enhanced_posts = []
        
        for post in posts:
            try:
                # Detect media in this post
                media_metadata = self.detect_media_in_post(post)
                
                # Add media metadata to post
                post['media_metadata'] = media_metadata
                
                enhanced_posts.append(post)
                
            except Exception as e:
                logger.error(f"Error detecting media in post {post.get('post_id', 'unknown')}: {str(e)}")
                # Add empty media metadata for failed detection
                post['media_metadata'] = self._get_empty_media_metadata()
                enhanced_posts.append(post)
        
        return enhanced_posts
    
    def detect_media_in_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect media attachments in a single post.
        
        Args:
            post: Single post data (could be raw or processed)
            
        Returns:
            Media metadata dictionary
        """
        # Get attachments from post
        attachments = post.get('attachments', [])
        
        if not attachments:
            return self._get_empty_media_metadata()
        
        # Initialize media counts and lists
        video_count = 0
        image_count = 0
        total_media_count = 0
        video_attachments = []
        image_attachments = []
        
        # Process each attachment
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
                
            attachment_type = attachment.get('type', '').lower()
            attachment_id = attachment.get('id')
            attachment_url = attachment.get('url')
            video_url = attachment.get('video_url')
            
            # Detect videos
            if (attachment_type == 'video' or 
                video_url or 
                'video' in attachment.get('attachment_url', '').lower()):
                
                video_count += 1
                total_media_count += 1
                
                video_info = {
                    'id': attachment_id,
                    'type': 'video',
                    'thumbnail_url': attachment_url,
                    'video_url': video_url,
                    'attachment_url': attachment.get('attachment_url'),
                    'video_length': attachment.get('video_length'),
                    'requires_processing': True
                }
                video_attachments.append(video_info)
                
                logger.debug(f"Detected video: {attachment_id}")
            
            # Detect images/photos
            elif (attachment_type in ['photo', 'image'] or 
                  attachment_url and self._is_image_url(attachment_url)):
                
                image_count += 1
                total_media_count += 1
                
                image_info = {
                    'id': attachment_id,
                    'type': 'image',
                    'url': attachment_url,
                    'attachment_url': attachment.get('attachment_url'),
                    'requires_processing': True
                }
                image_attachments.append(image_info)
                
                logger.debug(f"Detected image: {attachment_id}")
        
        # Combine video and image attachments into single attachments array for BigQuery schema
        all_attachments = []
        primary_image_url = None
        
        # Add video attachments
        for video_info in video_attachments:
            attachment = {
                'id': video_info['id'],
                'type': 'Video',
                'url': video_info.get('video_url') or video_info.get('thumbnail_url'),
                'attachment_url': video_info.get('attachment_url')
            }
            all_attachments.append(attachment)
        
        # Add image attachments
        for image_info in image_attachments:
            attachment = {
                'id': image_info['id'],
                'type': 'Photo',
                'url': image_info['url'],
                'attachment_url': image_info.get('attachment_url')
            }
            all_attachments.append(attachment)
            
            # Set primary image URL (first image found)
            if not primary_image_url and image_info.get('url'):
                primary_image_url = image_info['url']
        
        # Create media metadata matching BigQuery schema
        media_metadata = {
            'media_count': total_media_count,
            'has_video': video_count > 0,
            'has_image': image_count > 0,
            'media_processing_requested': total_media_count > 0,
            'primary_image_url': primary_image_url,
            'attachments': all_attachments
        }
        
        if total_media_count > 0:
            logger.info(f"Detected {total_media_count} media attachments in post {post.get('post_id', 'unknown')} (videos: {video_count}, images: {image_count})")
        
        return media_metadata
    
    def _is_image_url(self, url: str) -> bool:
        """Check if URL appears to be an image based on common patterns."""
        if not url:
            return False
            
        url_lower = url.lower()
        
        # Check for common image file extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
        for ext in image_extensions:
            if ext in url_lower:
                return True
        
        # Check for Facebook image URL patterns
        facebook_image_patterns = [
            'scontent.',
            'fbcdn.net',
            '/photos/',
            '/profile_image/',
            '/cover_photo/',
            '/photo.php'
        ]
        
        for pattern in facebook_image_patterns:
            if pattern in url_lower:
                return True
        
        return False
    
    def _get_empty_media_metadata(self) -> Dict[str, Any]:
        """Get empty media metadata for posts without media."""
        return {
            'media_count': 0,
            'has_video': False,
            'has_image': False,
            'media_processing_requested': False,
            'primary_image_url': None,
            'attachments': []
        }
    
    def extract_media_for_processing_event(self, posts_with_media: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract media information for media processing event payload.
        
        Args:
            posts_with_media: List of posts that have media attachments
            
        Returns:
            Media processing event payload
        """
        total_videos = 0
        total_images = 0
        video_urls = []
        image_urls = []
        post_media_mapping = []
        
        for post in posts_with_media:
            media_metadata = post.get('media_metadata', {})
            post_id = post.get('post_id') or post.get('id')
            
            if not media_metadata.get('media_processing_requested', False):
                continue
            
            # Extract media information from attachments
            for attachment in media_metadata.get('attachments', []):
                attachment_type = attachment.get('type', '').lower()
                attachment_id = attachment.get('id')
                attachment_url = attachment.get('url')
                
                if attachment_type == 'video':
                    total_videos += 1
                    video_urls.append(attachment_url)
                    
                    post_media_mapping.append({
                        'post_id': post_id,
                        'media_type': 'video',
                        'media_id': attachment_id,
                        'media_url': attachment_url,
                        'attachment_url': attachment.get('attachment_url')
                    })
                
                elif attachment_type == 'photo':
                    total_images += 1
                    image_urls.append(attachment_url)
                    
                    post_media_mapping.append({
                        'post_id': post_id,
                        'media_type': 'image',
                        'media_id': attachment_id,
                        'media_url': attachment_url,
                        'attachment_url': attachment.get('attachment_url')
                    })
        
        return {
            'total_media_count': total_videos + total_images,
            'video_count': total_videos,
            'image_count': total_images,
            'video_urls': [url for url in video_urls if url],
            'image_urls': [url for url in image_urls if url],
            'post_media_mapping': post_media_mapping,
            'posts_with_media_count': len(posts_with_media)
        }
    
    def validate_media_urls(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate media URLs to ensure they are accessible.
        
        Args:
            media_info: Media information dictionary
            
        Returns:
            Validation results
        """
        validation_results = {
            'valid_videos': 0,
            'invalid_videos': 0,
            'valid_images': 0,
            'invalid_images': 0,
            'validation_errors': []
        }
        
        # Validate video URLs
        for video_url in media_info.get('video_urls', []):
            if video_url and video_url.startswith('http'):
                validation_results['valid_videos'] += 1
            else:
                validation_results['invalid_videos'] += 1
                validation_results['validation_errors'].append(f"Invalid video URL: {video_url}")
        
        # Validate image URLs
        for image_url in media_info.get('image_urls', []):
            if image_url and image_url.startswith('http'):
                validation_results['valid_images'] += 1
            else:
                validation_results['invalid_images'] += 1
                validation_results['validation_errors'].append(f"Invalid image URL: {image_url}")
        
        validation_results['is_valid'] = (
            validation_results['invalid_videos'] == 0 and 
            validation_results['invalid_images'] == 0
        )
        
        return validation_results