"""
Media Tracking Handler for monitoring media processing pipeline status and statistics.

This handler manages the media_tracking BigQuery table to track:
- Media detection and processing status
- Processing statistics and analytics
- Stalled media detection and recovery
- Pipeline performance monitoring
"""

import os
import json
import logging
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound

logger = logging.getLogger(__name__)

class MediaTrackingHandler:
    """
    Handle media tracking operations for monitoring media processing pipeline.
    
    This handler provides comprehensive tracking of media items through the entire
    processing pipeline, from detection to completion, with analytics and monitoring.
    """
    
    def __init__(self):
        """Initialize MediaTrackingHandler with BigQuery client and configuration."""
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
        self.table_name = 'media_tracking'
        self.table_id = f"{self.project_id}.{self.dataset_id}.{self.table_name}"
        
        # Configuration
        self.tracking_enabled = os.getenv('MEDIA_TRACKING_ENABLED', 'true').lower() == 'true'
        self.batch_size = int(os.getenv('MEDIA_TRACKING_BATCH_SIZE', '100'))
        self.stall_threshold_minutes = int(os.getenv('MEDIA_STALL_THRESHOLD_MINUTES', '30'))
        
        # Initialize table reference
        self._verify_table_exists()
        
        logger.info(f"MediaTrackingHandler initialized for table {self.table_id}")
        logger.info(f"Tracking enabled: {self.tracking_enabled}, batch size: {self.batch_size}")
    
    def _verify_table_exists(self):
        """Verify that the media tracking table exists."""
        try:
            self.client.get_table(self.table_id)
            logger.info(f"Media tracking table {self.table_id} verified")
        except NotFound:
            logger.error(f"Media tracking table {self.table_id} not found")
            logger.error("Run: python scripts/create_media_tracking_table.py")
            raise
    
    def _generate_media_id(self, crawl_id: str, post_id: str, media_type: str, media_url: str) -> str:
        """
        Generate unique media ID from crawl_id, post_id, media_type, and URL hash.
        
        Args:
            crawl_id: Crawl session identifier
            post_id: Post identifier
            media_type: Type of media (image, video, audio, document)
            media_url: Original media URL
            
        Returns:
            Unique media ID
        """
        # Create URL hash for uniqueness
        url_hash = hashlib.md5(media_url.encode()).hexdigest()[:8]
        
        # Format: crawl_id_post_id_media_type_url_hash
        media_id = f"{crawl_id}_{post_id}_{media_type}_{url_hash}"
        
        return media_id
    
    def insert_detected_media(self, media_items: List[Dict[str, Any]], 
                            batch_metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Insert newly detected media records with 'detected' status.
        
        Args:
            media_items: List of media items to track
            batch_metadata: Optional metadata about the batch
            
        Returns:
            Dict with insertion results
        """
        if not self.tracking_enabled:
            logger.info("Media tracking disabled, skipping insertion")
            return {'success': True, 'rows_inserted': 0, 'tracking_disabled': True}
        
        if not media_items:
            logger.warning("No media items to insert")
            return {'success': True, 'rows_inserted': 0}
        
        try:
            # Convert media items to tracking records
            tracking_records = []
            current_time = datetime.utcnow()
            current_time_str = current_time.isoformat()
            
            for media_item in media_items:
                # Extract required fields
                crawl_id = media_item.get('crawl_id')
                post_id = media_item.get('post_id')
                media_url = media_item.get('url')
                media_type = media_item.get('type', 'unknown')
                platform = media_item.get('platform')
                competitor = media_item.get('competitor')
                
                # Validate required fields
                if not all([crawl_id, post_id, media_url, platform, competitor]):
                    logger.warning(f"Missing required fields in media item: {media_item}")
                    continue
                
                # Generate media ID
                media_id = self._generate_media_id(crawl_id, post_id, media_type, media_url)
                
                # Create tracking record
                tracking_record = {
                    'media_id': media_id,
                    'media_url': media_url,
                    'post_id': post_id,
                    'crawl_id': crawl_id,
                    'status': 'detected',
                    'media_type': media_type,
                    'platform': platform,
                    'competitor': competitor,
                    'brand': media_item.get('brand'),
                    'category': media_item.get('category'),
                    'detection_timestamp': current_time_str,
                    'last_status_update': current_time_str,
                    'retry_count': 0,
                    'media_metadata': {
                        'attachment_id': media_item.get('id'),
                        'attachment_url': media_item.get('attachment_url'),
                        'content_type': media_item.get('content_type'),
                        'platform_metadata': json.dumps(media_item.get('platform_metadata', {}))
                    },
                    'tracking_metadata': {
                        'created_by': 'data-processing-service',
                        'tracking_version': '1.0.0',
                        'priority': 'medium',
                        'processing_queue': f"{platform}_media_processing"
                    }
                }
                
                tracking_records.append(tracking_record)
            
            # Insert records in batches
            total_inserted = 0
            for i in range(0, len(tracking_records), self.batch_size):
                batch = tracking_records[i:i + self.batch_size]
                
                errors = self.client.insert_rows_json(self.table_id, batch)
                
                if errors:
                    error_msg = f"BigQuery insertion errors: {errors}"
                    logger.error(error_msg)
                    raise MediaTrackingError(error_msg)
                
                total_inserted += len(batch)
                logger.info(f"Inserted batch of {len(batch)} media tracking records")
            
            logger.info(f"Successfully inserted {total_inserted} media tracking records")
            
            return {
                'success': True,
                'rows_inserted': total_inserted,
                'table_id': self.table_id,
                'batch_metadata': batch_metadata
            }
            
        except Exception as e:
            logger.error(f"Error inserting media tracking records: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'rows_inserted': 0
            }
    
    def update_media_status(self, media_id: str, status: str, 
                          processing_metadata: Optional[Dict] = None,
                          error_message: Optional[str] = None) -> bool:
        """
        Update processing status for existing media record.
        
        Args:
            media_id: Unique media identifier
            status: New status (processing, completed, failed)
            processing_metadata: Optional processing results
            error_message: Optional error message for failed status
            
        Returns:
            bool: Success status
        """
        if not self.tracking_enabled:
            logger.info("Media tracking disabled, skipping status update")
            return True
        
        try:
            current_time = datetime.utcnow()
            
            # Build update query
            update_fields = [
                f"status = '{status}'",
                f"last_status_update = '{current_time.isoformat()}'",
            ]
            
            # Add status-specific fields
            if status == 'processing':
                update_fields.append(f"processing_start_timestamp = '{current_time.isoformat()}'")
            elif status in ['completed', 'failed']:
                update_fields.append(f"processing_end_timestamp = '{current_time.isoformat()}'")
            
            # Add retry count increment for failed status
            if status == 'failed':
                update_fields.append("retry_count = COALESCE(retry_count, 0) + 1")
                if error_message:
                    update_fields.append(f"error_message = '{error_message}'")
            
            # Add processing metadata
            if processing_metadata:
                # Convert to JSON string for BigQuery
                import json
                metadata_json = json.dumps(processing_metadata)
                update_fields.append(f"processing_metadata = PARSE_JSON('{metadata_json}')")
            
            # Execute update
            update_query = f"""
            UPDATE `{self.table_id}`
            SET {', '.join(update_fields)}
            WHERE media_id = '{media_id}'
            """
            
            query_job = self.client.query(update_query)
            query_job.result()  # Wait for completion
            
            logger.info(f"Updated media {media_id} status to {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating media status for {media_id}: {str(e)}")
            return False
    
    def get_processing_statistics(self, platform: Optional[str] = None,
                                competitor: Optional[str] = None,
                                hours_back: int = 24) -> Dict[str, Any]:
        """
        Get processing statistics for the media pipeline.
        
        Args:
            platform: Optional platform filter
            competitor: Optional competitor filter
            hours_back: Number of hours to look back
            
        Returns:
            Dict with processing statistics
        """
        try:
            # Build query with filters
            time_filter = f"detection_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)"
            
            filters = [time_filter]
            if platform:
                filters.append(f"platform = '{platform}'")
            if competitor:
                filters.append(f"competitor = '{competitor}'")
            
            where_clause = " AND ".join(filters)
            
            query = f"""
            SELECT 
                platform,
                status,
                COUNT(*) as count,
                AVG(CASE 
                    WHEN processing_start_timestamp IS NOT NULL AND processing_end_timestamp IS NOT NULL 
                    THEN TIMESTAMP_DIFF(processing_end_timestamp, processing_start_timestamp, SECOND)
                    ELSE NULL 
                END) as avg_processing_duration_seconds,
                SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as retry_count,
                COUNT(DISTINCT media_type) as media_types_count
            FROM `{self.table_id}`
            WHERE {where_clause}
            GROUP BY platform, status
            ORDER BY platform, status
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Process results
            statistics = {
                'total_media': 0,
                'by_platform': {},
                'by_status': {},
                'overall_stats': {
                    'avg_processing_duration_seconds': 0,
                    'total_retries': 0,
                    'unique_media_types': 0
                }
            }
            
            total_duration = 0
            total_with_duration = 0
            
            for row in results:
                platform_name = row.platform
                status = row.status
                count = row.count
                avg_duration = row.avg_processing_duration_seconds
                retry_count = row.retry_count
                
                statistics['total_media'] += count
                
                # By platform
                if platform_name not in statistics['by_platform']:
                    statistics['by_platform'][platform_name] = {}
                statistics['by_platform'][platform_name][status] = {
                    'count': count,
                    'avg_processing_duration_seconds': avg_duration,
                    'retry_count': retry_count
                }
                
                # By status
                if status not in statistics['by_status']:
                    statistics['by_status'][status] = 0
                statistics['by_status'][status] += count
                
                # Overall stats
                if avg_duration:
                    total_duration += avg_duration * count
                    total_with_duration += count
                
                statistics['overall_stats']['total_retries'] += retry_count
            
            # Calculate overall average duration
            if total_with_duration > 0:
                statistics['overall_stats']['avg_processing_duration_seconds'] = total_duration / total_with_duration
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error getting processing statistics: {str(e)}")
            return {'error': str(e)}
    
    def get_stalled_media(self, threshold_minutes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get media items that are stalled in processing status.
        
        Args:
            threshold_minutes: Optional threshold override
            
        Returns:
            List of stalled media items
        """
        try:
            threshold = threshold_minutes or self.stall_threshold_minutes
            
            query = f"""
            SELECT 
                media_id,
                media_url,
                post_id,
                crawl_id,
                platform,
                competitor,
                status,
                processing_start_timestamp,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), processing_start_timestamp, MINUTE) as stalled_minutes,
                retry_count,
                error_message
            FROM `{self.table_id}`
            WHERE status = 'processing' 
            AND processing_start_timestamp IS NOT NULL
            AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), processing_start_timestamp, MINUTE) >= {threshold}
            ORDER BY stalled_minutes DESC
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            stalled_media = []
            for row in results:
                stalled_media.append({
                    'media_id': row.media_id,
                    'media_url': row.media_url,
                    'post_id': row.post_id,
                    'crawl_id': row.crawl_id,
                    'platform': row.platform,
                    'competitor': row.competitor,
                    'status': row.status,
                    'processing_start_timestamp': row.processing_start_timestamp,
                    'stalled_minutes': row.stalled_minutes,
                    'retry_count': row.retry_count,
                    'error_message': row.error_message
                })
            
            logger.info(f"Found {len(stalled_media)} stalled media items")
            return stalled_media
            
        except Exception as e:
            logger.error(f"Error getting stalled media: {str(e)}")
            return []
    
    def get_media_by_crawl_id(self, crawl_id: str) -> List[Dict[str, Any]]:
        """
        Get all media items for a specific crawl ID.
        
        Args:
            crawl_id: Crawl session identifier
            
        Returns:
            List of media items
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.table_id}`
            WHERE crawl_id = '{crawl_id}'
            ORDER BY detection_timestamp DESC
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            media_items = []
            for row in results:
                media_items.append(dict(row))
            
            return media_items
            
        except Exception as e:
            logger.error(f"Error getting media by crawl ID {crawl_id}: {str(e)}")
            return []

class MediaTrackingError(Exception):
    """Exception raised for media tracking errors."""
    pass