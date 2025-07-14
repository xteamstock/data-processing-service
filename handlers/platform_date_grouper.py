#!/usr/bin/env python3
"""
Platform-specific date grouper for multi-platform social media data.

Groups posts by their actual upload date from each platform, handling
platform-specific date field variations for hierarchical GCS storage.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

class PlatformDateGrouper:
    """
    Groups posts by upload date with platform-specific date field handling.
    
    Supports:
    - Facebook: 'date_posted' field
    - TikTok: 'createTimeISO' field  
    - YouTube: 'date' field
    """
    
    # Platform-specific date field mapping
    PLATFORM_DATE_FIELDS = {
        'facebook': 'date_posted',
        'tiktok': 'createTimeISO',
        'youtube': 'date'
    }
    
    def __init__(self):
        """Initialize platform date grouper."""
        pass
    
    def extract_upload_date(self, raw_post: Dict[str, Any], platform: str) -> str:
        """
        Extract upload date from platform-specific raw post data.
        
        Args:
            raw_post: Raw post data from platform
            platform: Platform name (facebook, tiktok, youtube)
            
        Returns:
            Date string in YYYY-MM-DD format or 'unknown' if extraction fails
        """
        date_field = self.get_date_field(platform)
        
        if not date_field:
            logger.warning(f"Unknown platform: {platform}")
            return 'unknown'
        
        # Extract date value from raw post
        date_value = raw_post.get(date_field)
        
        if not date_value:
            logger.warning(f"Missing {date_field} field in {platform} post {raw_post.get('id', 'unknown')}")
            return 'unknown'
        
        return self._parse_date_to_string(date_value)
    
    def get_date_field(self, platform: str) -> Optional[str]:
        """
        Get the date field name for a specific platform.
        
        Args:
            platform: Platform name
            
        Returns:
            Date field name or None for unknown platforms
        """
        return self.PLATFORM_DATE_FIELDS.get(platform.lower())
    
    def group_by_upload_date(self, posts_with_platform: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group posts by their upload date, handling multiple platforms.
        
        Args:
            posts_with_platform: List of dicts with 'platform' and 'raw_data' keys
            
        Returns:
            Dict mapping date strings (YYYY-MM-DD) to lists of posts
        """
        grouped_data = defaultdict(list)
        
        for post_info in posts_with_platform:
            platform = post_info.get('platform', 'unknown')
            raw_data = post_info.get('raw_data', {})
            
            # Extract upload date for this platform
            upload_date = self.extract_upload_date(raw_data, platform)
            
            # Add to grouped data
            grouped_data[upload_date].append(post_info)
        
        logger.info(f"Grouped {len(posts_with_platform)} posts into {len(grouped_data)} date groups")
        for date_key, posts in grouped_data.items():
            platforms = [p.get('platform', 'unknown') for p in posts]
            platform_counts = {p: platforms.count(p) for p in set(platforms)}
            logger.info(f"  {date_key}: {len(posts)} posts ({platform_counts})")
        
        return dict(grouped_data)
    
    def create_gcs_paths(self, grouped_data: Dict[str, List[Dict]], metadata: Dict[str, Any]) -> Dict[str, str]:
        """
        Create GCS path structure for each date group.
        
        Args:
            grouped_data: Posts grouped by date
            metadata: Contains platform, competitor, brand, category info
            
        Returns:
            Dict mapping date keys to GCS path prefixes
        """
        platform = metadata.get('platform', 'unknown')
        competitor = metadata.get('competitor', 'unknown')
        brand = metadata.get('brand', 'unknown')
        category = metadata.get('category', 'unknown')
        
        gcs_paths = {}
        
        for date_key, posts in grouped_data.items():
            if date_key == 'unknown':
                # Use current date for unknown dates
                current_date = datetime.now()
                yyyy, mm, dd = current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d")
            else:
                try:
                    date_obj = datetime.strptime(date_key, "%Y-%m-%d")
                    yyyy, mm, dd = date_obj.strftime("%Y"), date_obj.strftime("%m"), date_obj.strftime("%d")
                except ValueError:
                    # Fallback for invalid dates
                    current_date = datetime.now()
                    yyyy, mm, dd = current_date.strftime("%Y"), current_date.strftime("%m"), current_date.strftime("%d")
            
            # Create hierarchical path
            path_prefix = f"social-analytics-processed-data/raw_data/platform={platform}/competitor={competitor}/brand={brand}/category={category}/year={yyyy}/month={mm}/day={dd}"
            gcs_paths[date_key] = path_prefix
        
        return gcs_paths
    
    def _parse_date_to_string(self, date_value: Any) -> str:
        """
        Parse various date formats to YYYY-MM-DD string.
        
        Args:
            date_value: Date value (string, timestamp, etc.)
            
        Returns:
            Date string in YYYY-MM-DD format or 'unknown' if parsing fails
        """
        if not date_value:
            return 'unknown'
        
        try:
            date_str = str(date_value)
            
            # Handle ISO timestamp format (most common)
            if 'T' in date_str:
                # Format: "2024-12-24T13:30:14.000Z" -> "2024-12-24"
                date_part = date_str.split('T')[0]
            else:
                # Handle date-only format or take first 10 chars
                date_part = date_str[:10]
            
            # Validate date format
            datetime.strptime(date_part, '%Y-%m-%d')
            return date_part
            
        except (ValueError, IndexError, AttributeError) as e:
            logger.warning(f"Failed to parse date value '{date_value}': {str(e)}")
            return 'unknown'
    
    def get_upload_date_summary(self, grouped_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Get summary statistics for upload date distribution.
        
        Args:
            grouped_data: Posts grouped by upload date
            
        Returns:
            Summary statistics
        """
        if not grouped_data:
            return {'error': 'No data provided'}
        
        valid_dates = [date_key for date_key in grouped_data.keys() if date_key != 'unknown']
        
        total_posts = sum(len(posts) for posts in grouped_data.values())
        unknown_posts = len(grouped_data.get('unknown', []))
        
        summary = {
            'total_date_groups': len(grouped_data),
            'valid_dates': len(valid_dates),
            'unknown_dates': unknown_posts,
            'total_posts': total_posts,
            'platform_distribution': self._get_platform_distribution(grouped_data)
        }
        
        if valid_dates:
            valid_dates.sort()
            summary.update({
                'earliest_upload_date': valid_dates[0],
                'latest_upload_date': valid_dates[-1],
                'date_range_days': (datetime.strptime(valid_dates[-1], '%Y-%m-%d') - 
                                   datetime.strptime(valid_dates[0], '%Y-%m-%d')).days + 1
            })
        else:
            summary.update({
                'earliest_upload_date': None,
                'latest_upload_date': None,
                'date_range_days': 0
            })
        
        return summary
    
    def _get_platform_distribution(self, grouped_data: Dict[str, List[Dict]]) -> Dict[str, int]:
        """Get distribution of posts by platform."""
        platform_counts = defaultdict(int)
        
        for posts in grouped_data.values():
            for post in posts:
                platform = post.get('platform', 'unknown')
                platform_counts[platform] += 1
        
        return dict(platform_counts)