# handlers/bigquery_handler.py
# NEW: BigQuery handler for direct insertion

import os
import logging
from typing import List, Dict, Any
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

class BigQueryHandler:
    """
    Handle BigQuery operations for analytics data storage.
    
    Optimized for direct insertion of processed social media data.
    """
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
        self.posts_table = f"{self.dataset_id}.posts"
        self.events_table = f"{self.dataset_id}.processing_events"
    
    def insert_post(self, processed_post: Dict, platform: str) -> bool:
        """
        Insert a single processed post to platform-specific BigQuery table.
        
        Args:
            processed_post: Single processed post
            platform: Platform name (facebook, tiktok, youtube)
            
        Returns:
            bool: Success status
        """
        return self.insert_posts([processed_post], platform=platform)['success']
    
    def insert_posts(self, processed_posts: List[Dict], metadata: Dict = None, platform: str = None) -> Dict:
        """
        Insert processed posts to BigQuery analytics table.
        
        Args:
            processed_posts: List of processed posts
            metadata: Processing metadata
            platform: Platform name for platform-specific table (facebook, tiktok, youtube)
            
        Returns:
            Dict with insertion results
        """
        if not processed_posts:
            logger.warning("No posts to insert")
            target_table = self._get_platform_table(platform) if platform else self.posts_table
            return {'success': True, 'rows_inserted': 0, 'table_id': target_table}
        
        try:
            # Determine target table
            target_table = self._get_platform_table(platform) if platform else self.posts_table
            
            # Validate posts against BigQuery schema
            validated_posts = self._validate_posts_schema(processed_posts)
            
            # Insert to BigQuery
            errors = self.client.insert_rows_json(target_table, validated_posts)
            
            if errors:
                error_msg = f"BigQuery insertion errors: {errors}"
                logger.error(error_msg)
                if metadata:
                    self._log_processing_event(metadata, len(processed_posts), False, error_msg)
                raise BigQueryInsertionError(error_msg)
            
            logger.info(f"Successfully inserted {len(processed_posts)} posts to BigQuery table {target_table}")
            
            # Log successful processing
            if metadata:
                self._log_processing_event(metadata, len(processed_posts), True)
            
            return {
                'success': True,
                'rows_inserted': len(processed_posts),
                'table_id': target_table
            }
            
        except GoogleCloudError as e:
            error_msg = f"BigQuery operation failed: {str(e)}"
            logger.error(error_msg)
            if metadata:
                self._log_processing_event(metadata, len(processed_posts), False, error_msg)
            raise BigQueryInsertionError(error_msg)
    
    def _validate_posts_schema(self, processed_posts: List[Dict]) -> List[Dict]:
        """Validate processed posts against platform-specific BigQuery schema."""
        validated_posts = []
        
        for post in processed_posts:
            platform = post.get('platform', '').lower()
            
            # Start with common fields
            validated_post = {
                'id': str(post.get('id', '')),
                'crawl_id': str(post.get('crawl_id', '')),
                'snapshot_id': str(post.get('snapshot_id', '')),
                'platform': str(post.get('platform', '')),
                'competitor': str(post.get('competitor', '')),
                'brand': str(post.get('brand', '')),
                'category': str(post.get('category', '')),
                'date_posted': post.get('date_posted'),
                'crawl_date': post.get('crawl_date'),
                'processed_date': post.get('processed_date'),
                'grouped_date': post.get('grouped_date'),
                'user_url': str(post.get('user_url', '')),
                'user_username': str(post.get('user_username', '')),
                'user_profile_id': str(post.get('user_profile_id', ''))
            }
            
            # Add platform-specific fields
            if platform == 'facebook':
                validated_post.update({
                    'post_id': str(post.get('post_id', '')),
                    'post_url': str(post.get('post_url', '')),
                    'post_content': str(post.get('post_content', '')),
                    'post_type': str(post.get('post_type', 'Post')),
                    'page_name': str(post.get('page_name', '')),
                    'page_category': str(post.get('page_category', '')),
                    'page_verified': bool(post.get('page_verified', False)),
                    'page_followers': self._safe_int(post.get('page_followers', 0)),
                    'page_likes': self._safe_int(post.get('page_likes', 0)),
                    'engagement_metrics': self._ensure_json_field(post.get('engagement_metrics', {})),
                    'content_analysis': self._ensure_json_field(post.get('content_analysis', {})),
                    'media_metadata': self._ensure_json_field(post.get('media_metadata', {})),
                    'page_metadata': self._ensure_json_field(post.get('page_metadata', {})),
                    'processing_metadata': self._ensure_json_field(post.get('processing_metadata', {}))
                })
            
            elif platform == 'tiktok':
                validated_post.update({
                    'video_id': str(post.get('video_id', '')),
                    'video_url': str(post.get('video_url', '')),
                    'description': str(post.get('description', '')),
                    'author_name': str(post.get('author_name', '')),
                    'author_verified': bool(post.get('author_verified', False)),
                    'author_follower_count': self._safe_int(post.get('author_follower_count', 0)),
                    'play_count': self._safe_int(post.get('play_count', 0)),
                    'digg_count': self._safe_int(post.get('digg_count', 0)),
                    'share_count': self._safe_int(post.get('share_count', 0)),
                    'comment_count': self._safe_int(post.get('comment_count', 0)),
                    'engagement_metrics': self._ensure_json_field(post.get('engagement_metrics', {})),
                    'content_analysis': self._ensure_json_field(post.get('content_analysis', {})),
                    'video_metadata': self._ensure_json_field(post.get('video_metadata', {})),
                    'author_metadata': self._ensure_json_field(post.get('author_metadata', {})),
                    'processing_metadata': self._ensure_json_field(post.get('processing_metadata', {}))
                })
            
            elif platform == 'youtube':
                validated_post.update({
                    'video_id': str(post.get('video_id', '')),
                    'video_url': str(post.get('video_url', '')),
                    'title': str(post.get('title', '')),
                    'description': str(post.get('description', '')),
                    'channel_id': str(post.get('channel_id', '')),
                    'channel_name': str(post.get('channel_name', '')),
                    'channel_verified': bool(post.get('channel_verified', False)),
                    'channel_subscriber_count': self._safe_int(post.get('channel_subscriber_count', 0)),
                    'view_count': self._safe_int(post.get('view_count', 0)),
                    'like_count': self._safe_int(post.get('like_count', 0)),
                    'comment_count': self._safe_int(post.get('comment_count', 0)),
                    'published_at': post.get('published_at'),
                    'engagement_metrics': self._ensure_json_field(post.get('engagement_metrics', {})),
                    'content_analysis': self._ensure_json_field(post.get('content_analysis', {})),
                    'video_metadata': self._ensure_json_field(post.get('video_metadata', {})),
                    'channel_metadata': self._ensure_json_field(post.get('channel_metadata', {})),
                    'processing_metadata': self._ensure_json_field(post.get('processing_metadata', {}))
                })
            else:
                # Default/unknown platform - use minimal fields
                validated_post.update({
                    'engagement_metrics': self._ensure_json_field(post.get('engagement_metrics', {})),
                    'content_analysis': self._ensure_json_field(post.get('content_analysis', {})),
                    'processing_metadata': self._ensure_json_field(post.get('processing_metadata', {}))
                })
            
            # Validate timestamp format
            for date_field in ['date_posted', 'crawl_date', 'processed_date']:
                if validated_post.get(date_field):
                    validated_post[date_field] = self._ensure_timestamp_format(
                        validated_post[date_field]
                    )
            
            # Validate grouped_date as date string
            if validated_post.get('grouped_date'):
                validated_post['grouped_date'] = str(validated_post['grouped_date'])[:10]
            
            # Handle YouTube specific published_at field
            if platform == 'youtube' and validated_post.get('published_at'):
                validated_post['published_at'] = self._ensure_timestamp_format(
                    validated_post['published_at']
                )
            
            validated_posts.append(validated_post)
        
        return validated_posts
    
    def _ensure_timestamp_format(self, date_value: str) -> str:
        """Ensure date is in proper timestamp format for BigQuery."""
        if not date_value:
            return datetime.utcnow().isoformat()
        
        try:
            # Try parsing as ISO format
            if isinstance(date_value, str):
                if 'T' in date_value:
                    # ISO format - ensure proper timezone
                    return date_value.replace('Z', '+00:00')
                else:
                    # Try as timestamp
                    parsed = datetime.fromtimestamp(float(date_value))
                    return parsed.isoformat()
            
            return str(date_value)
            
        except Exception as e:
            logger.warning(f"Date format validation failed: {str(e)}")
            return datetime.utcnow().isoformat()
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert value to integer."""
        try:
            if value is None:
                return 0
            return int(value)
        except (ValueError, TypeError):
            return 0
    
    def _ensure_json_field(self, value: Any) -> Any:
        """Ensure field is JSON-compatible for BigQuery JSON type."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            # For BigQuery JSON type, serialize to JSON string
            try:
                import json
                return json.dumps(value) if value else None
            except (TypeError, ValueError):
                return None
        # If it's already a string (maybe JSON), validate and return
        if isinstance(value, str):
            try:
                import json
                # Parse and re-serialize to ensure valid JSON
                parsed = json.loads(value)
                return json.dumps(parsed)
            except (json.JSONDecodeError, ValueError):
                return None
        return None
    
    def _get_platform_table(self, platform: str) -> str:
        """Get platform-specific table name."""
        if platform:
            platform_table_map = {
                'facebook': f"{self.project_id}.{self.dataset_id}.facebook_posts",
                'tiktok': f"{self.project_id}.{self.dataset_id}.tiktok_posts", 
                'youtube': f"{self.project_id}.{self.dataset_id}.youtube_posts"
            }
            return platform_table_map.get(platform.lower(), f"{self.project_id}.{self.dataset_id}.posts")
        return f"{self.project_id}.{self.dataset_id}.posts"
    
    def _log_processing_event(self, metadata: Dict, post_count: int, success: bool, error_message: str = None):
        """Log processing event to BigQuery for monitoring."""
        try:
            event_record = {
                'event_id': f"proc_{datetime.utcnow().timestamp()}",
                'crawl_id': metadata.get('crawl_id'),
                'event_type': 'data_processing',
                'event_timestamp': datetime.utcnow().isoformat(),
                'processing_duration_seconds': metadata.get('processing_duration', 0),
                'post_count': post_count,
                'media_count': metadata.get('media_count', 0),
                'success': success,
                'error_message': error_message
            }
            
            # Insert to events table
            errors = self.client.insert_rows_json(self.events_table, [event_record])
            
            if errors:
                logger.warning(f"Failed to log processing event: {errors}")
                
        except Exception as e:
            logger.warning(f"Error logging processing event: {str(e)}")

class BigQueryInsertionError(Exception):
    """Custom exception for BigQuery insertion errors."""
    pass