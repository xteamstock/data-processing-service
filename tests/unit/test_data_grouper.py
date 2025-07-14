#!/usr/bin/env python3
"""
Unit tests for data grouping functionality - TDD approach.

Tests grouping of processed posts by their upload date (date_posted) 
rather than crawl date for hierarchical GCS storage.
"""

import unittest
from datetime import datetime

class TestDataGrouper(unittest.TestCase):
    """Test data grouping functionality using TDD approach."""
    
    def setUp(self):
        """Set up test fixtures."""
        # These tests will fail initially until we implement DataGrouper
        pass
    
    def test_group_posts_by_upload_date_basic(self):
        """Test basic grouping of posts by their upload date."""
        # Sample processed posts with different upload dates
        processed_posts = [
            {
                'id': 'post1_crawl123',
                'platform': 'facebook',
                'date_posted': '2025-07-12T10:30:00Z',
                'competitor': 'nutifood',
                'brand': 'growplus-nutifood'
            },
            {
                'id': 'post2_crawl123', 
                'platform': 'facebook',
                'date_posted': '2025-07-12T15:45:00Z',
                'competitor': 'nutifood',
                'brand': 'growplus-nutifood'
            },
            {
                'id': 'post3_crawl123',
                'platform': 'facebook', 
                'date_posted': '2025-07-13T09:15:00Z',
                'competitor': 'nutifood',
                'brand': 'growplus-nutifood'
            }
        ]
        
        # This will fail until we implement DataGrouper
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        grouped = grouper.group_posts_by_date(processed_posts)
        
        # Should have 2 date groups
        self.assertEqual(len(grouped), 2)
        self.assertIn('2025-07-12', grouped)
        self.assertIn('2025-07-13', grouped)
        
        # July 12 should have 2 posts
        self.assertEqual(len(grouped['2025-07-12']), 2)
        # July 13 should have 1 post
        self.assertEqual(len(grouped['2025-07-13']), 1)
    
    def test_group_posts_handles_missing_date(self):
        """Test grouping handles posts with missing date_posted."""
        processed_posts = [
            {
                'id': 'post1_crawl123',
                'platform': 'facebook',
                'date_posted': '2025-07-12T10:30:00Z',
                'competitor': 'nutifood'
            },
            {
                'id': 'post2_crawl123',
                'platform': 'facebook',
                'date_posted': None,  # Missing date
                'competitor': 'nutifood'
            },
            {
                'id': 'post3_crawl123',
                'platform': 'facebook',
                # No date_posted field at all
                'competitor': 'nutifood'
            }
        ]
        
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        grouped = grouper.group_posts_by_date(processed_posts)
        
        # Should have 2 groups: valid date and 'unknown'
        self.assertEqual(len(grouped), 2)
        self.assertIn('2025-07-12', grouped)
        self.assertIn('unknown', grouped)
        
        # Valid date group should have 1 post
        self.assertEqual(len(grouped['2025-07-12']), 1)
        # Unknown date group should have 2 posts
        self.assertEqual(len(grouped['unknown']), 2)
    
    def test_group_posts_different_timestamp_formats(self):
        """Test grouping handles different timestamp formats."""
        processed_posts = [
            {
                'id': 'post1',
                'date_posted': '2025-07-12T10:30:00Z',  # ISO with Z
                'platform': 'facebook'
            },
            {
                'id': 'post2', 
                'date_posted': '2025-07-12T15:45:00+00:00',  # ISO with timezone
                'platform': 'tiktok'
            },
            {
                'id': 'post3',
                'date_posted': '2025-07-13',  # Date only
                'platform': 'youtube'
            },
            {
                'id': 'post4',
                'date_posted': 'invalid-date',  # Invalid format
                'platform': 'facebook'
            }
        ]
        
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        grouped = grouper.group_posts_by_date(processed_posts)
        
        # Should group correctly despite different formats
        self.assertEqual(len(grouped), 3)  # 2025-07-12, 2025-07-13, unknown
        self.assertIn('2025-07-12', grouped)
        self.assertIn('2025-07-13', grouped)
        self.assertIn('unknown', grouped)
        
        # July 12 should have 2 posts (different timestamp formats)
        self.assertEqual(len(grouped['2025-07-12']), 2)
        # July 13 should have 1 post
        self.assertEqual(len(grouped['2025-07-13']), 1)
        # Unknown should have 1 post (invalid date)
        self.assertEqual(len(grouped['unknown']), 1)
    
    def test_extract_date_from_timestamp(self):
        """Test date extraction from various timestamp formats."""
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        # Test various valid formats
        self.assertEqual(grouper._extract_date_from_timestamp('2025-07-12T10:30:00Z'), '2025-07-12')
        self.assertEqual(grouper._extract_date_from_timestamp('2025-07-12T10:30:00+00:00'), '2025-07-12')
        self.assertEqual(grouper._extract_date_from_timestamp('2025-07-12'), '2025-07-12')
        self.assertEqual(grouper._extract_date_from_timestamp('2025-12-25T23:59:59.000Z'), '2025-12-25')
        
        # Test invalid formats
        self.assertEqual(grouper._extract_date_from_timestamp('invalid'), 'unknown')
        self.assertEqual(grouper._extract_date_from_timestamp(''), 'unknown')
        self.assertEqual(grouper._extract_date_from_timestamp(None), 'unknown')
        self.assertEqual(grouper._extract_date_from_timestamp('2025-13-45'), 'unknown')  # Invalid date
    
    def test_get_date_range_summary(self):
        """Test date range summary statistics."""
        grouped_data = {
            '2025-07-10': [{'id': 'post1'}],
            '2025-07-12': [{'id': 'post2'}, {'id': 'post3'}],
            '2025-07-15': [{'id': 'post4'}],
            'unknown': [{'id': 'post5'}]
        }
        
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        summary = grouper.get_date_range_summary(grouped_data)
        
        self.assertEqual(summary['total_date_groups'], 4)
        self.assertEqual(summary['valid_dates'], 3)
        self.assertEqual(summary['unknown_dates'], 1)
        self.assertEqual(summary['earliest_date'], '2025-07-10')
        self.assertEqual(summary['latest_date'], '2025-07-15')
        self.assertEqual(summary['total_posts'], 5)
        self.assertEqual(summary['date_range_days'], 6)  # 10th to 15th = 6 days
    
    def test_validate_grouped_data(self):
        """Test validation of grouped data structure."""
        # Valid grouped data
        valid_data = {
            '2025-07-12': [
                {'id': 'post1', 'platform': 'facebook', 'date_posted': '2025-07-12T10:00:00Z'},
                {'id': 'post2', 'platform': 'tiktok', 'date_posted': '2025-07-12T11:00:00Z'}
            ],
            '2025-07-13': [
                {'id': 'post3', 'platform': 'youtube', 'date_posted': '2025-07-13T12:00:00Z'}
            ]
        }
        
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        validation = grouper.validate_grouped_data(valid_data)
        
        self.assertTrue(validation['valid'])
        self.assertEqual(validation['total_posts'], 3)
        self.assertEqual(validation['date_groups'], 2)
        self.assertEqual(len(validation['issues']), 0)
    
    def test_validate_grouped_data_with_issues(self):
        """Test validation catches data issues."""
        # Invalid grouped data
        invalid_data = {
            'invalid-date-format': [{'id': 'post1'}],  # Bad date format
            '2025-07-12': [
                'not-a-dict',  # Post should be dict
                {'id': 'post2'},  # Missing required fields
                {'id': 'post3', 'platform': 'facebook', 'date_posted': '2025-07-12T10:00:00Z'}  # Valid
            ]
        }
        
        from handlers.data_grouper import DataGrouper
        grouper = DataGrouper()
        
        validation = grouper.validate_grouped_data(invalid_data)
        
        self.assertFalse(validation['valid'])
        self.assertGreater(len(validation['issues']), 0)
        self.assertGreater(len(validation['warnings']), 0)

if __name__ == '__main__':
    unittest.main()