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
        
        # Deduplication configuration
        self.deduplication_enabled = os.getenv('BIGQUERY_DEDUPLICATION_ENABLED', 'false').lower() == 'true'
        self.deduplication_batch_size = int(os.getenv('BIGQUERY_DEDUPLICATION_BATCH_SIZE', '1000'))
        self.deduplication_fallback_on_error = os.getenv('BIGQUERY_DEDUPLICATION_FALLBACK_ON_ERROR', 'true').lower() == 'true'
    
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
            # Apply deduplication if enabled
            if self.deduplication_enabled and platform:
                processed_posts = self._filter_duplicates_batched(processed_posts, platform)
                # If all posts were duplicates, return early
                if not processed_posts:
                    logger.info("All posts were duplicates, nothing to insert")
                    target_table = self._get_platform_table(platform)
                    return {'success': True, 'rows_inserted': 0, 'table_id': target_table}
            
            # Determine target table
            target_table = self._get_platform_table(platform) if platform else self.posts_table
            
            # Trust schema mapper - posts are already transformed correctly
            # Apply the same logic that works in test_youtube_only.py
            cleaned_posts = []
            for post in processed_posts:
                # Flatten processing metadata (like test_youtube_only.py does)
                if 'processing_metadata' in post:
                    processing_meta = post.pop('processing_metadata')
                    post['schema_version'] = processing_meta.get('schema_version')
                    post['processing_version'] = processing_meta.get('processing_version')
                    post['data_quality_score'] = processing_meta.get('data_quality_score')
                
                # Remove nested objects (like test_youtube_only.py does)
                cleaned_post = {}
                for key, value in post.items():
                    if not isinstance(value, dict):
                        cleaned_post[key] = value
                
                # Platform-specific field filtering
                if platform == 'youtube':
                    # Remove date_posted for YouTube since table uses published_at
                    cleaned_post.pop('date_posted', None)
                
                cleaned_posts.append(cleaned_post)
            
            # Insert to BigQuery with cleaned posts
            logger.info(f"Attempting BigQuery insertion to {target_table} with {len(cleaned_posts)} posts")
            logger.info(f"Sample post keys: {list(cleaned_posts[0].keys()) if cleaned_posts else 'None'}")
            errors = self.client.insert_rows_json(target_table, cleaned_posts)
            
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
                    # Add all the fields that working test includes
                    'likes': self._safe_int(post.get('likes', 0)),
                    'comments': self._safe_int(post.get('comments', 0)),
                    'shares': self._safe_int(post.get('shares', 0)),
                    'total_reactions': self._safe_int(post.get('total_reactions', 0)),
                    'video_views': self._safe_int(post.get('video_views', 0)),
                    'hashtags': post.get('hashtags', []),
                    'likes_breakdown': str(post.get('likes_breakdown', '')),
                    'reactions_by_type': str(post.get('reactions_by_type', '')),
                    'attachments': str(post.get('attachments', '')),
                    'page_intro': str(post.get('page_intro', '')),
                    'page_creation_date': post.get('page_creation_date'),
                    'page_address': str(post.get('page_address', '')),
                    'page_reviews_score': float(post.get('page_reviews_score', 0.0)),
                    'page_reviewers_count': self._safe_int(post.get('page_reviewers_count', 0)),
                    'privacy_legal_info': str(post.get('privacy_legal_info', '')),
                    'about_sections': str(post.get('about_sections', '')),
                    'link_description': str(post.get('link_description', '')),
                    'profile_handle': str(post.get('profile_handle', '')),
                    'crawl_timestamp': post.get('crawl_timestamp'),
                    'original_input': str(post.get('original_input', '')),
                    'post_limit': self._safe_int(post.get('post_limit', 0)),
                    'media_count': self._safe_int(post.get('media_count', 0)),
                    'has_video': bool(post.get('has_video', False)),
                    'has_image': bool(post.get('has_image', False)),
                    'text_length': self._safe_int(post.get('text_length', 0)),
                    'language': str(post.get('language', '')),
                    'sentiment_score': float(post.get('sentiment_score', 0.0)),
                    'data_quality_score': float(post.get('data_quality_score') or 0.0)
                })
                
                # Note: processing_metadata is excluded for schema-driven tables
                # as the table schema doesn't include these flattened fields
            
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
                    # Note: Nested objects (engagement_metrics, content_analysis, video_metadata, author_metadata)
                    # are removed to match schema-driven table structure
                })
                
                # Note: processing_metadata is excluded for schema-driven tables
                # as the table schema doesn't include these flattened fields
            
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
                    # Note: Nested objects (engagement_metrics, content_analysis, video_metadata, channel_metadata)
                    # are removed to match schema-driven table structure
                })
                
                # Note: processing_metadata is excluded for schema-driven tables
                # as the table schema doesn't include these flattened fields
            else:
                # Default/unknown platform - use minimal fields
                # Note: Nested objects removed to match schema-driven table structure
                pass
                
                # Note: processing_metadata is excluded for schema-driven tables
                # as the table schema doesn't include these flattened fields
            
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
                'facebook': f"{self.project_id}.{self.dataset_id}.facebook_posts_schema_driven",
                'tiktok': f"{self.project_id}.{self.dataset_id}.tiktok_posts_schema_driven", 
                'youtube': f"{self.project_id}.{self.dataset_id}.youtube_videos_schema_driven"
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
    
    def _extract_post_ids(self, posts: List[Dict]) -> List[str]:
        """
        Extract post_id values from posts list.
        
        Args:
            posts: List of posts to extract IDs from
            
        Returns:
            List of valid post IDs for duplicate checking
        """
        post_ids = []
        for post in posts:
            # Check for post_id field (Facebook) or video_id field (TikTok/YouTube)
            post_id = post.get('post_id') or post.get('video_id')
            if post_id:
                post_ids.append(str(post_id))
        return post_ids
    
    def _get_existing_post_ids(self, post_ids: List[str], table_id: str) -> set:
        """
        Query BigQuery to find existing post_id values using parameterized queries.
        
        Args:
            post_ids: List of post IDs to check
            table_id: Target BigQuery table ID
            
        Returns:
            Set of existing post IDs
        """
        if not post_ids:
            return set()
        
        try:
            # Determine the ID column based on table type
            if 'facebook' in table_id:
                id_column = 'post_id'
            elif 'tiktok' in table_id or 'youtube' in table_id:
                id_column = 'video_id'
            else:
                # Default fallback - try both columns
                id_column = 'COALESCE(post_id, video_id)'
            
            # Use parameterized query for safety
            query = f"""
                SELECT DISTINCT 
                    {id_column} as post_id
                FROM `{table_id}` 
                WHERE {id_column} IN UNNEST(@post_ids)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("post_ids", "STRING", post_ids)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            return {row.post_id for row in results if row.post_id}
            
        except Exception as e:
            logger.error(f"Error checking for existing post_ids: {str(e)}")
            if self.deduplication_fallback_on_error:
                logger.warning("Falling back to assume no duplicates due to error")
                return set()
            raise
    
    def _filter_duplicates(self, posts: List[Dict], platform: str) -> List[Dict]:
        """
        Remove posts that already exist in the target table.
        
        Args:
            posts: List of posts to check
            platform: Platform name for table selection
            
        Returns:
            Filtered list of new posts only
        """
        if not posts:
            return posts
        
        # Extract post IDs
        post_ids = self._extract_post_ids(posts)
        if not post_ids:
            logger.info("No post IDs found, skipping deduplication")
            return posts
        
        # Get target table
        table_id = self._get_platform_table(platform)
        
        # Find existing post IDs
        existing_ids = self._get_existing_post_ids(post_ids, table_id)
        
        # Filter out duplicates
        new_posts = []
        for post in posts:
            post_id = post.get('post_id') or post.get('video_id')
            if post_id and str(post_id) not in existing_ids:
                new_posts.append(post)
        
        # Log results
        duplicates_found = len(posts) - len(new_posts)
        if duplicates_found > 0:
            logger.info(f"Filtered {duplicates_found} duplicate posts, inserting {len(new_posts)} new posts")
        else:
            logger.info(f"No duplicates found, inserting all {len(posts)} posts")
        
        return new_posts
    
    def _filter_duplicates_batched(self, posts: List[Dict], platform: str) -> List[Dict]:
        """
        Handle large datasets by processing duplicate checking in batches.
        
        Args:
            posts: List of posts to check
            platform: Platform name for table selection
            
        Returns:
            Filtered list of new posts only
        """
        if len(posts) <= self.deduplication_batch_size:
            return self._filter_duplicates(posts, platform)
        
        logger.info(f"Processing {len(posts)} posts in batches of {self.deduplication_batch_size}")
        filtered_posts = []
        
        for i in range(0, len(posts), self.deduplication_batch_size):
            batch = posts[i:i + self.deduplication_batch_size]
            filtered_batch = self._filter_duplicates(batch, platform)
            filtered_posts.extend(filtered_batch)
            logger.info(f"Processed batch {i // self.deduplication_batch_size + 1}, kept {len(filtered_batch)} of {len(batch)} posts")
        
        total_duplicates = len(posts) - len(filtered_posts)
        logger.info(f"Batch processing complete: filtered {total_duplicates} duplicates, keeping {len(filtered_posts)} new posts")
        
        return filtered_posts

class BigQueryInsertionError(Exception):
    """Custom exception for BigQuery insertion errors."""
    pass