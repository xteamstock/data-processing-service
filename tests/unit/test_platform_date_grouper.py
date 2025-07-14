#!/usr/bin/env python3
"""
TDD tests for platform-specific date grouping functionality.

Tests extracting upload dates from different platforms and grouping
for hierarchical GCS storage.
"""

import unittest
import json
from pathlib import Path

class TestPlatformDateGrouper(unittest.TestCase):
    """Test platform-specific date extraction and grouping using TDD."""
    
    def setUp(self):
        """Set up test fixtures with real platform data."""
        # Load actual fixture data for testing
        fixtures_dir = Path(__file__).parent.parent.parent / 'fixtures'
        
        with open(fixtures_dir / 'gcs-facebook-posts.json', 'r') as f:
            self.facebook_data = json.load(f)
        
        with open(fixtures_dir / 'gcs-tiktok-posts.json', 'r') as f:
            self.tiktok_data = json.load(f)
            
        with open(fixtures_dir / 'gcs-youtube-posts.json', 'r') as f:
            self.youtube_data = json.load(f)
    
    def test_extract_upload_date_facebook(self):
        """Test extracting upload date from Facebook posts."""
        # Facebook uses 'date_posted' field
        facebook_post = self.facebook_data[0]
        expected_date = "2024-12-24"  # From "2024-12-24T13:30:14.000Z"
        
        # This will fail until we implement PlatformDateGrouper
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        extracted_date = grouper.extract_upload_date(facebook_post, 'facebook')
        self.assertEqual(extracted_date, expected_date)
    
    def test_extract_upload_date_tiktok(self):
        """Test extracting upload date from TikTok posts."""
        # TikTok uses 'createTimeISO' field
        tiktok_post = self.tiktok_data[0]
        expected_date = "2025-07-11"  # From "2025-07-11T08:27:53.000Z"
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        extracted_date = grouper.extract_upload_date(tiktok_post, 'tiktok')
        self.assertEqual(extracted_date, expected_date)
    
    def test_extract_upload_date_youtube(self):
        """Test extracting upload date from YouTube videos."""
        # YouTube uses 'date' field
        youtube_post = self.youtube_data[0]
        expected_date = "2025-07-08"  # From "2025-07-08T10:41:45.000Z"
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        extracted_date = grouper.extract_upload_date(youtube_post, 'youtube')
        self.assertEqual(extracted_date, expected_date)
    
    def test_group_mixed_platform_posts_by_upload_date(self):
        """Test grouping posts from multiple platforms by their upload dates."""
        # Mix posts from all platforms
        mixed_posts = [
            {'platform': 'facebook', 'raw_data': self.facebook_data[0]},
            {'platform': 'tiktok', 'raw_data': self.tiktok_data[0]},
            {'platform': 'youtube', 'raw_data': self.youtube_data[0]}
        ]
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        grouped = grouper.group_by_upload_date(mixed_posts)
        
        # Should have 3 different date groups
        expected_dates = ["2024-12-24", "2025-07-11", "2025-07-08"]
        self.assertEqual(len(grouped), 3)
        
        for expected_date in expected_dates:
            self.assertIn(expected_date, grouped)
            self.assertEqual(len(grouped[expected_date]), 1)
    
    def test_handle_missing_date_fields(self):
        """Test handling posts with missing or invalid date fields."""
        # Posts with missing/invalid dates
        problematic_posts = [
            {'platform': 'facebook', 'raw_data': {'post_id': '123'}},  # No date_posted
            {'platform': 'tiktok', 'raw_data': {'id': '456', 'createTimeISO': None}},  # Null date
            {'platform': 'youtube', 'raw_data': {'id': '789', 'date': 'invalid-date'}},  # Invalid format
            {'platform': 'unknown', 'raw_data': {'id': '000'}},  # Unknown platform
        ]
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        grouped = grouper.group_by_upload_date(problematic_posts)
        
        # All should go to 'unknown' date group
        self.assertIn('unknown', grouped)
        self.assertEqual(len(grouped['unknown']), 4)
    
    def test_get_platform_date_field_mapping(self):
        """Test platform-specific date field mapping."""
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        # Test correct field mapping
        self.assertEqual(grouper.get_date_field('facebook'), 'date_posted')
        self.assertEqual(grouper.get_date_field('tiktok'), 'createTimeISO')
        self.assertEqual(grouper.get_date_field('youtube'), 'date')
        
        # Test unknown platform
        self.assertIsNone(grouper.get_date_field('unknown'))
    
    def test_create_gcs_path_structure(self):
        """Test creating correct GCS path structure from grouped data."""
        # Sample grouped data with metadata
        grouped_data = {
            "2025-07-12": [
                {'platform': 'facebook', 'competitor': 'nutifood', 'brand': 'growplus-nutifood', 'category': 'sua-bot-tre-em'}
            ]
        }
        
        metadata = {
            'platform': 'facebook',
            'competitor': 'nutifood', 
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_id': 'test_crawl_123'
        }
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        gcs_paths = grouper.create_gcs_paths(grouped_data, metadata)
        
        expected_path = "social-analytics-processed-data/raw_data/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12"
        
        self.assertIn('2025-07-12', gcs_paths)
        self.assertTrue(gcs_paths['2025-07-12'].startswith(expected_path))
    
    def test_integration_with_real_fixture_data(self):
        """Integration test using all real fixture data."""
        # Process all fixture data as if coming from data-ingestion service
        all_posts = []
        
        # Add Facebook posts
        for post in self.facebook_data[:2]:  # First 2 posts
            all_posts.append({'platform': 'facebook', 'raw_data': post})
        
        # Add TikTok posts  
        for post in self.tiktok_data[:2]:  # First 2 posts
            all_posts.append({'platform': 'tiktok', 'raw_data': post})
            
        # Add YouTube posts
        for post in self.youtube_data[:2]:  # First 2 posts
            all_posts.append({'platform': 'youtube', 'raw_data': post})
        
        from handlers.platform_date_grouper import PlatformDateGrouper
        grouper = PlatformDateGrouper()
        
        # Group by upload date
        grouped = grouper.group_by_upload_date(all_posts)
        
        # Verify grouping worked
        self.assertGreater(len(grouped), 0)
        
        # Each group should contain posts from the same date
        for date_key, posts in grouped.items():
            if date_key != 'unknown':
                # All posts in this group should have the same upload date
                for post in posts:
                    extracted_date = grouper.extract_upload_date(
                        post['raw_data'], 
                        post['platform']
                    )
                    self.assertEqual(extracted_date, date_key)

if __name__ == '__main__':
    unittest.main()