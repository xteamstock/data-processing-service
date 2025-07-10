# handlers/gcs_processed_handler.py
# MIGRATED FROM: backup/legacy-monolith/src/storage/gcs_uploader.py

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from google.cloud import storage

logger = logging.getLogger(__name__)

class GCSProcessedHandler:
    """
    Handler for uploading processed data to GCS in hierarchical structure.
    
    MIGRATED FROM: backup/legacy-monolith/src/storage/gcs_uploader.py
    CHANGES:
    - Adapted for microservices architecture
    - Uses social-analytics-processed-data bucket
    - Simplified for data processing service only
    - Enhanced error handling and logging
    """
    
    def __init__(self, bucket_name: str = None):
        """Initialize GCS processed data handler."""
        self.bucket_name = bucket_name or os.getenv('GCS_BUCKET_PROCESSED_DATA', 'social-analytics-processed-data')
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
    
    def upload_grouped_data(
        self, 
        grouped_data: Dict[str, List[Dict[str, Any]]], 
        metadata: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Upload grouped data to GCS following hierarchical structure.
        
        Args:
            grouped_data: Data grouped by date (format: {"2025-01-01": [posts...]})
            metadata: Contains platform, competitor, brand, category, crawl_id, etc.
            
        Returns:
            (success, error_message, upload_stats)
        """
        try:
            # Extract metadata
            platform = metadata.get('platform', 'unknown')
            competitor = metadata.get('competitor', 'unknown')
            brand = metadata.get('brand', 'unknown')
            category = metadata.get('category', 'unknown')
            crawl_id = metadata.get('crawl_id', 'unknown')
            data_type = 'processed_posts'
            
            logger.info(f"Starting GCS processed data upload for {len(grouped_data)} date groups")
            logger.info(f"Target: gs://{self.bucket_name}/raw_data/platform={platform}/competitor={competitor}/brand={brand}/category={category}/")
            
            upload_stats = {
                'total_files': 0,
                'total_records': 0,
                'successful_uploads': 0,
                'failed_uploads': 0,
                'uploaded_files': [],
                'failed_files': [],
                'crawl_id': crawl_id
            }
            
            for date_key, posts in grouped_data.items():
                success, file_path, record_count = self._upload_date_group(
                    date_key, posts, platform, competitor, brand, category, data_type, crawl_id
                )
                
                upload_stats['total_files'] += 1
                upload_stats['total_records'] += record_count
                
                if success:
                    upload_stats['successful_uploads'] += 1
                    upload_stats['uploaded_files'].append({
                        'date': date_key,
                        'file_path': file_path,
                        'record_count': record_count
                    })
                else:
                    upload_stats['failed_uploads'] += 1
                    upload_stats['failed_files'].append({
                        'date': date_key,
                        'file_path': file_path,
                        'record_count': record_count
                    })
            
            if upload_stats['failed_uploads'] == 0:
                logger.info(f"Successfully uploaded all {upload_stats['total_files']} files to GCS")
                return True, None, upload_stats
            else:
                error_msg = f"Upload partially failed: {upload_stats['failed_uploads']}/{upload_stats['total_files']} files failed"
                logger.error(error_msg)
                return False, error_msg, upload_stats
                
        except Exception as e:
            error_msg = f"GCS processed data upload failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, {'error': str(e)}
    
    def _upload_date_group(
        self, 
        date_key: str, 
        posts: List[Dict[str, Any]], 
        platform: str, 
        competitor: str, 
        brand: str, 
        category: str, 
        data_type: str,
        crawl_id: str
    ) -> Tuple[bool, str, int]:
        """Upload a single date group to GCS."""
        try:
            # Parse date for folder structure
            if date_key == "unknown":
                yyyy, mm, dd = "unknown", "unknown", "unknown"
            else:
                try:
                    date_obj = datetime.strptime(date_key, "%Y-%m-%d")
                    yyyy = date_obj.strftime("%Y")
                    mm = date_obj.strftime("%m")
                    dd = date_obj.strftime("%d")
                except ValueError:
                    yyyy, mm, dd = "unknown", "unknown", "unknown"
            
            # Create folder path following hierarchical structure
            folder_path = f"raw_data/platform={platform}/competitor={competitor}/brand={brand}/category={category}/year={yyyy}/month={mm}/day={dd}/"
            
            # Create file name with timestamp and crawl_id
            timestamp = datetime.now().strftime("%H%M%S")
            file_name = f"{data_type}_{platform}_{competitor}_{brand}_{category}_{yyyy}{mm}{dd}_{timestamp}_{crawl_id}.jsonl"
            
            gcs_blob_path = os.path.join(folder_path, file_name)
            
            # Create JSONL content with Unicode preservation
            file_content = "\n".join(json.dumps(post, ensure_ascii=False) for post in posts)
            
            # Upload to GCS
            blob = self.bucket.blob(gcs_blob_path)
            blob.upload_from_string(file_content, content_type="application/jsonl")
            
            logger.info(f"Uploaded processed data to gs://{self.bucket_name}/{gcs_blob_path} ({len(posts)} records)")
            return True, gcs_blob_path, len(posts)
            
        except Exception as e:
            error_msg = f"Failed to upload processed date group {date_key}: {str(e)}"
            logger.error(error_msg)
            return False, f"failed_{date_key}", len(posts)
    
    def get_upload_path_preview(self, metadata: Dict[str, Any], sample_date: str = "2025-01-01") -> str:
        """Get a preview of where processed files would be uploaded."""
        platform = metadata.get('platform', 'unknown')
        competitor = metadata.get('competitor', 'unknown')
        brand = metadata.get('brand', 'unknown')
        category = metadata.get('category', 'unknown')
        crawl_id = metadata.get('crawl_id', 'example_crawl')
        data_type = 'processed_posts'
        
        try:
            date_obj = datetime.strptime(sample_date, "%Y-%m-%d")
            yyyy = date_obj.strftime("%Y")
            mm = date_obj.strftime("%m")
            dd = date_obj.strftime("%d")
        except ValueError:
            yyyy, mm, dd = "unknown", "unknown", "unknown"
        
        folder_path = f"raw_data/platform={platform}/competitor={competitor}/brand={brand}/category={category}/year={yyyy}/month={mm}/day={dd}/"
        file_name = f"{data_type}_{platform}_{competitor}_{brand}_{category}_{yyyy}{mm}{dd}_HHMMSS_{crawl_id}.jsonl"
        
        return f"gs://{self.bucket_name}/{folder_path}{file_name}"
    
    def test_bucket_access(self) -> Tuple[bool, str]:
        """Test if processed data bucket is accessible."""
        try:
            # Try to list one object to test access
            blobs = list(self.client.list_blobs(self.bucket, max_results=1))
            return True, f"Successfully accessed bucket gs://{self.bucket_name}"
        except Exception as e:
            return False, f"Cannot access bucket gs://{self.bucket_name}: {str(e)}"