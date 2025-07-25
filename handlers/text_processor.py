# handlers/text_processor.py
# MIGRATED FROM: backup/legacy-monolith/src/core/data_processor.py

import re
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from textblob import TextBlob
from handlers.schema_mapper import SchemaMapper
from handlers.media_detector import MediaDetector
from handlers.platform_date_grouper import PlatformDateGrouper

logger = logging.getLogger(__name__)

class TextProcessor:
    """
    Text processing logic migrated from legacy data_processor.py
    
    MIGRATION SOURCE: backup/legacy-monolith/src/core/data_processor.py
    - process_and_group_data() method [TEXT PROCESSING ONLY]
    - Data validation and cleaning methods
    - Date parsing and normalization
    """
    
    def __init__(self):
        self.schema_mapper = SchemaMapper()
        self.media_detector = MediaDetector()
        self.platform_date_grouper = PlatformDateGrouper()
        
    def process_posts_for_analytics(self, raw_data: List[Dict], metadata: Dict) -> List[Dict]:
        """
        Process raw BrightData posts for BigQuery analytics.
        
        MIGRATED FROM: DataProcessor.process_and_group_data()
        CHANGES: 
        - Removed media processing (moved to separate service)
        - Added BigQuery schema formatting via SchemaMapper
        - Enhanced metadata extraction
        - Added multi-platform support
        - Preserved date-based grouping pattern
        """
        logger.info(f"Processing {len(raw_data)} posts for analytics")
        
        # Step 1: Group posts by date (legacy pattern preserved)
        grouped_data = self._group_posts_by_date(raw_data)
        logger.info(f"Grouped posts into {len(grouped_data)} date groups")
        
        # Step 2: Process each group using schema-driven transformation
        processed_posts = []
        
        for date_group, posts in grouped_data.items():
            for post in posts:
                try:
                    # Use schema mapper for transformation
                    processed_post = self.schema_mapper.transform_post(
                        raw_post=post,
                        platform=metadata.get('platform', 'facebook'),
                        metadata=metadata,
                        schema_version="1.0.0"
                    )
                    
                    # Add date group for analytics (legacy pattern)
                    processed_post['grouped_date'] = date_group
                    
                    processed_posts.append(processed_post)
                    
                except Exception as e:
                    logger.error(f"Error processing post {post.get('post_id', 'unknown')}: {str(e)}")
                    # Continue processing other posts
                    continue
        
        # Step 3: Enhance with media detection
        processed_posts_with_media = self.media_detector.detect_media_in_posts(processed_posts)
        
        logger.info(f"Successfully processed {len(processed_posts_with_media)} posts")
        return processed_posts_with_media
    
    def _group_posts_by_date(self, raw_data: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group posts by date for analytics optimization.
        
        MIGRATED FROM: DataProcessor.process_and_group_data()
        This preserves the legacy date-based grouping pattern.
        """
        grouped_data = {}
        
        for post in raw_data:
            # Extract date from post
            date_posted = post.get('date_posted', '')
            
            # Parse date to get date string (YYYY-MM-DD)
            try:
                if date_posted:
                    if 'T' in date_posted:
                        date_key = date_posted.split('T')[0]  # Extract YYYY-MM-DD
                    else:
                        date_key = str(date_posted)[:10]  # First 10 chars
                else:
                    date_key = datetime.utcnow().strftime('%Y-%m-%d')
            except Exception:
                date_key = datetime.utcnow().strftime('%Y-%m-%d')
            
            # Group by date
            if date_key not in grouped_data:
                grouped_data[date_key] = []
            grouped_data[date_key].append(post)
        
        return grouped_data
    
    def process_posts(self, event_data: Dict) -> List[Dict]:
        """
        Legacy interface for backward compatibility.
        """
        raw_data = event_data.get('posts', [])
        metadata = {
            'crawl_id': event_data.get('crawl_id'),
            'snapshot_id': event_data.get('snapshot_id'),
            'platform': event_data.get('platform', 'facebook'),
            'competitor': event_data.get('competitor'),
            'brand': event_data.get('brand'),
            'category': event_data.get('category'),
            'crawl_date': event_data.get('timestamp')
        }
        
        return self.process_posts_for_analytics(raw_data, metadata)
    
    def get_grouped_data_for_gcs(self, processed_posts: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group processed posts by their actual upload date for GCS upload.
        
        Uses platform-aware date extraction to ensure posts are grouped
        by their original upload date from each platform, not crawl date.
        
        Args:
            processed_posts: List of processed posts with platform info
            
        Returns:
            Dict with upload date keys (YYYY-MM-DD) and lists of posts
        """
        grouped_data = {}
        
        for post in processed_posts:
            # Extract the actual upload date using platform-aware logic
            platform = post.get('platform', 'unknown')
            
            # Use the date_posted field that was set by SchemaMapper
            # This contains the actual upload date from the platform
            upload_date = post.get('date_posted', '')
            
            if upload_date:
                # Parse timestamp to date string
                date_key = self.platform_date_grouper._parse_date_to_string(upload_date)
            else:
                logger.warning(f"Post {post.get('id', 'unknown')} missing date_posted field")
                date_key = 'unknown'
            
            # Group by upload date
            if date_key not in grouped_data:
                grouped_data[date_key] = []
            grouped_data[date_key].append(post)
        
        logger.info(f"Grouped {len(processed_posts)} posts into {len(grouped_data)} upload date groups for GCS upload")
        
        # Log summary of date distribution
        summary = self.platform_date_grouper.get_upload_date_summary(
            {k: [{'platform': p.get('platform')} for p in v] for k, v in grouped_data.items()}
        )
        logger.info(f"Upload date range: {summary.get('earliest_upload_date')} to {summary.get('latest_upload_date')}")
        logger.info(f"Platform distribution: {summary.get('platform_distribution', {})}")
        
        return grouped_data