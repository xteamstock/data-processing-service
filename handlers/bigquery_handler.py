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
        self.posts_table = f"{self.project_id}.{self.dataset_id}.posts"
        self.events_table = f"{self.project_id}.{self.dataset_id}.processing_events"
    
    def insert_posts(self, processed_posts: List[Dict], metadata: Dict = None) -> Dict:
        """
        Insert processed posts to BigQuery analytics table.
        
        Args:
            processed_posts: List of processed posts
            metadata: Processing metadata
            
        Returns:
            Dict with insertion results
        """
        if not processed_posts:
            logger.warning("No posts to insert")
            return {'success': True, 'rows_inserted': 0, 'table_id': self.posts_table}
        
        try:
            # Validate posts against BigQuery schema
            validated_posts = self._validate_posts_schema(processed_posts)
            
            # Insert to BigQuery
            errors = self.client.insert_rows_json(self.posts_table, validated_posts)
            
            if errors:
                error_msg = f"BigQuery insertion errors: {errors}"
                logger.error(error_msg)
                if metadata:
                    self._log_processing_event(metadata, len(processed_posts), False, error_msg)
                raise BigQueryInsertionError(error_msg)
            
            logger.info(f"Successfully inserted {len(processed_posts)} posts to BigQuery")
            
            # Log successful processing
            if metadata:
                self._log_processing_event(metadata, len(processed_posts), True)
            
            return {
                'success': True,
                'rows_inserted': len(processed_posts),
                'table_id': self.posts_table
            }
            
        except GoogleCloudError as e:
            error_msg = f"BigQuery operation failed: {str(e)}"
            logger.error(error_msg)
            if metadata:
                self._log_processing_event(metadata, len(processed_posts), False, error_msg)
            raise BigQueryInsertionError(error_msg)
    
    def _validate_posts_schema(self, processed_posts: List[Dict]) -> List[Dict]:
        """Validate processed posts against BigQuery schema."""
        validated_posts = []
        
        for post in processed_posts:
            # Ensure all required fields are present and properly typed
            validated_post = {
                'id': str(post.get('id', '')),
                'post_id': str(post.get('post_id', '')),
                'crawl_id': str(post.get('crawl_id', '')),
                'snapshot_id': str(post.get('snapshot_id', '')),
                'platform': str(post.get('platform', '')),
                'competitor': str(post.get('competitor', '')),
                'brand': str(post.get('brand', '')),
                'category': str(post.get('category', '')),
                'post_url': str(post.get('post_url', '')),
                'post_content': str(post.get('post_content', '')),
                'post_type': str(post.get('post_type', 'Post')),
                'date_posted': post.get('date_posted'),
                'crawl_date': post.get('crawl_date'),
                'processed_date': post.get('processed_date'),
                'grouped_date': post.get('grouped_date'),
                'user_url': str(post.get('user_url', '')),
                'user_username': str(post.get('user_username', '')),
                'user_profile_id': str(post.get('user_profile_id', '')),
                'page_name': str(post.get('page_name', '')),
                'page_category': str(post.get('page_category', '')),
                'page_verified': bool(post.get('page_verified', False)),
                'page_followers': int(post.get('page_followers', 0)),
                'page_likes': int(post.get('page_likes', 0)),
                'engagement_metrics': post.get('engagement_metrics', {}),
                'content_analysis': post.get('content_analysis', {}),
                'media_metadata': post.get('media_metadata', {}),
                'page_metadata': post.get('page_metadata', {}),
                'processing_metadata': post.get('processing_metadata', {})
            }
            
            # Validate timestamp format
            for date_field in ['date_posted', 'crawl_date', 'processed_date']:
                if validated_post[date_field]:
                    validated_post[date_field] = self._ensure_timestamp_format(
                        validated_post[date_field]
                    )
            
            # Validate grouped_date as date string
            if validated_post['grouped_date']:
                validated_post['grouped_date'] = str(validated_post['grouped_date'])[:10]
            
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