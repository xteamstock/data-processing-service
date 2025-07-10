# events/event_handler.py
# NEW: Pub/Sub push handler for Cloud Run

import json
import base64
import logging
from typing import Dict, Any, Optional, List
from flask import Request, jsonify
from datetime import datetime
from google.cloud import storage
from handlers.text_processor import TextProcessor
from handlers.bigquery_handler import BigQueryHandler
from handlers.gcs_processed_handler import GCSProcessedHandler
from handlers.media_detector import MediaDetector
from events.event_publisher import EventPublisher

logger = logging.getLogger(__name__)

class EventHandler:
    """
    Handle incoming Pub/Sub push events for data processing.
    
    This class implements the Pub/Sub push pattern optimized for Cloud Run.
    """
    
    def __init__(self):
        self.text_processor = TextProcessor()
        self.bigquery_handler = BigQueryHandler()
        self.gcs_processed_handler = GCSProcessedHandler()
        self.media_detector = MediaDetector()
        self.event_publisher = EventPublisher()
        self.storage_client = storage.Client()
    
    def handle_data_ingestion_completed(self, request: Request) -> tuple:
        """
        Handle data-ingestion-completed events from Pub/Sub push.
        
        Args:
            request: Flask request object with Pub/Sub message
            
        Returns:
            tuple: (response_dict, status_code)
        """
        try:
            # Extract and validate event data
            event_data = self._extract_pubsub_event_data(request)
            
            if not event_data:
                logger.error("Invalid or missing event data")
                return {'error': 'Invalid event data'}, 400
            
            # Process the event
            start_time = datetime.utcnow()
            result = self._process_data_ingestion_event(event_data)
            processing_duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Log successful processing
            logger.info(f"Successfully processed data ingestion event in {processing_duration:.2f}s")
            
            return {
                'success': True,
                'processed_posts': result['processed_posts'],
                'media_processing_requested': result['media_processing_requested'],
                'processing_duration_seconds': processing_duration
            }, 200
            
        except Exception as e:
            logger.error(f"Error processing data ingestion event: {str(e)}")
            
            # Publish failure event if we have crawl_id
            if 'event_data' in locals() and event_data:
                crawl_id = event_data.get('data', {}).get('crawl_id')
                if crawl_id:
                    self.event_publisher.publish_processing_failed(crawl_id, str(e))
            
            return {'error': str(e)}, 500
    
    def _extract_pubsub_event_data(self, request: Request) -> Optional[Dict]:
        """
        Extract event data from Pub/Sub push message.
        
        Pub/Sub push format:
        {
            "message": {
                "data": "<base64-encoded-json>",
                "attributes": {...},
                "messageId": "...",
                "publishTime": "..."
            },
            "subscription": "..."
        }
        """
        try:
            # Get request JSON
            envelope = request.get_json()
            
            if not envelope:
                logger.error("No JSON body in request")
                return None
            
            # Extract message
            message = envelope.get('message', {})
            
            if not message:
                logger.error("No message in Pub/Sub envelope")
                return None
            
            # Extract and decode data
            data = message.get('data', '')
            
            if not data:
                logger.error("No data in Pub/Sub message")
                return None
            
            # Decode base64 data
            try:
                decoded_data = base64.b64decode(data).decode('utf-8')
                event_data = json.loads(decoded_data)
            except Exception as e:
                logger.error(f"Error decoding Pub/Sub data: {str(e)}")
                return None
            
            # Validate event structure
            if not event_data.get('data'):
                logger.error("Missing 'data' field in event")
                return None
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error extracting Pub/Sub event data: {str(e)}")
            return None
    
    def _process_data_ingestion_event(self, event_data: Dict) -> Dict:
        """Process the data ingestion completed event."""
        # Extract event details
        data = event_data['data']
        crawl_id = data['crawl_id']
        snapshot_id = data['snapshot_id']
        gcs_path = data['gcs_path']
        
        logger.info(f"Processing data ingestion event for crawl {crawl_id}")
        
        # Download raw data from GCS
        raw_data = self._download_raw_data_from_gcs(gcs_path)
        
        if not raw_data:
            raise ValueError(f"No data found at GCS path: {gcs_path}")
        
        # Prepare metadata
        metadata = {
            'crawl_id': crawl_id,
            'snapshot_id': snapshot_id,
            'gcs_path': gcs_path,
            'crawl_date': event_data.get('timestamp'),
            'platform': data.get('platform'),
            'competitor': data.get('competitor'),
            'brand': data.get('brand'),
            'category': data.get('category')
        }
        
        # Process posts for analytics
        processed_posts = self.text_processor.process_posts_for_analytics(raw_data, metadata)
        
        if not processed_posts:
            logger.warning(f"No posts processed for crawl {crawl_id}")
            return {
                'processed_posts': 0,
                'media_processing_requested': False,
                'gcs_upload_completed': False,
                'bigquery_insert_completed': False
            }
        
        # Job 1: Group and upload processed data to GCS
        logger.info(f"Starting Job 1: GCS processed data upload for crawl {crawl_id}")
        grouped_data = self.text_processor.get_grouped_data_for_gcs(processed_posts)
        gcs_success, gcs_error, gcs_stats = self.gcs_processed_handler.upload_grouped_data(
            grouped_data, metadata
        )
        
        if not gcs_success:
            logger.error(f"GCS upload failed for crawl {crawl_id}: {gcs_error}")
        else:
            logger.info(f"GCS upload completed for crawl {crawl_id}: {gcs_stats['successful_uploads']} files uploaded")
        
        # Job 2: Insert to BigQuery
        logger.info(f"Starting Job 2: BigQuery analytics insert for crawl {crawl_id}")
        bigquery_result = self.bigquery_handler.insert_posts(processed_posts, metadata)
        
        # Publish data processing completed event
        self.event_publisher.publish_data_processing_completed(
            crawl_id=crawl_id,
            snapshot_id=snapshot_id,
            processed_posts=len(processed_posts),
            bigquery_table=bigquery_result['table_id']
        )
        
        # Job 3: Detect media and publish processing events
        logger.info(f"Starting Job 3: Media detection and event publishing for crawl {crawl_id}")
        posts_with_media = []
        for post in processed_posts:
            media_metadata = post.get('media_metadata', {})
            if media_metadata.get('media_processing_requested', False):
                posts_with_media.append(post)
        
        media_processing_requested = False
        media_event_success = True
        
        if posts_with_media:
            # Extract media information for event
            media_info = self.media_detector.extract_media_for_processing_event(posts_with_media)
            
            # Validate media URLs
            validation_results = self.media_detector.validate_media_urls(media_info)
            
            if validation_results['is_valid']:
                media_processing_requested = True
                media_event_success = self.event_publisher.publish_media_processing_requested(
                    crawl_id=crawl_id,
                    snapshot_id=snapshot_id,
                    posts_with_media=posts_with_media,
                    media_info=media_info
                )
                
                logger.info(f"Requested media processing for {len(posts_with_media)} posts ({media_info['video_count']} videos, {media_info['image_count']} images)")
            else:
                logger.warning(f"Media validation failed for crawl {crawl_id}: {validation_results['validation_errors'][:3]}")
        
        # Create comprehensive result
        result = {
            'processed_posts': len(processed_posts),
            'media_processing_requested': media_processing_requested,
            'gcs_upload_completed': gcs_success,
            'bigquery_insert_completed': bigquery_result.get('success', False),
            'media_event_published': media_event_success and media_processing_requested,
            'jobs_summary': {
                'job1_gcs_upload': {
                    'success': gcs_success,
                    'files_uploaded': gcs_stats.get('successful_uploads', 0) if gcs_success else 0,
                    'total_records': gcs_stats.get('total_records', 0) if gcs_success else 0,
                    'error': gcs_error if not gcs_success else None
                },
                'job2_bigquery_insert': {
                    'success': bigquery_result.get('success', False),
                    'table_id': bigquery_result.get('table_id'),
                    'rows_inserted': len(processed_posts)
                },
                'job3_media_detection': {
                    'posts_with_media': len(posts_with_media),
                    'media_event_published': media_event_success and media_processing_requested,
                    'total_media_count': sum(post.get('media_metadata', {}).get('media_count', 0) for post in posts_with_media)
                }
            }
        }
        
        logger.info(f"Dual-output jobs completed for crawl {crawl_id}: GCS={gcs_success}, BigQuery={bigquery_result.get('success', False)}, Media={media_processing_requested}")
        return result
    
    def _download_raw_data_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Download raw data from GCS path."""
        try:
            # Parse GCS path (gs://bucket/path/to/file.json)
            path_parts = gcs_path.replace('gs://', '').split('/')
            bucket_name = path_parts[0]
            object_name = '/'.join(path_parts[1:])
            
            # Download from GCS
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Download and parse JSON
            json_content = blob.download_as_text()
            raw_data = json.loads(json_content)
            
            # Extract posts from BrightData format
            if isinstance(raw_data, list):
                # Filter out non-dictionary items (malformed JSON parsing artifacts)
                valid_posts = [item for item in raw_data if isinstance(item, dict)]
                if len(valid_posts) != len(raw_data):
                    logger.warning(f"Filtered out {len(raw_data) - len(valid_posts)} non-dictionary items from raw data")
                return valid_posts
            elif isinstance(raw_data, dict):
                # Handle nested structure
                return raw_data.get('posts', raw_data.get('data', [raw_data]))
            else:
                logger.error(f"Unexpected data format: {type(raw_data)}")
                return []
                
        except Exception as e:
            logger.error(f"Error downloading from GCS {gcs_path}: {str(e)}")
            raise