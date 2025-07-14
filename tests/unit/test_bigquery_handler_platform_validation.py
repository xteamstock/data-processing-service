#!/usr/bin/env python3
"""
Test platform-aware validation in BigQuery handler.

This test verifies that the BigQueryHandler correctly validates
platform-specific data without forcing all platforms into a unified schema.
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os
from datetime import datetime

# Add the handlers directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'handlers'))

from bigquery_handler import BigQueryHandler

class TestBigQueryHandlerPlatformValidation(unittest.TestCase):
    """Test platform-aware validation in BigQueryHandler."""
    
    def setUp(self):
        """Set up test fixtures."""
        with patch('bigquery_handler.bigquery.Client'):
            self.handler = BigQueryHandler()
    
    def test_facebook_validation(self):
        """Test Facebook post validation."""
        facebook_post = {
            'id': 'test_123',
            'crawl_id': 'crawl_456',
            'platform': 'facebook',
            'competitor': 'test_competitor',
            'brand': 'test_brand',
            'category': 'test_category',
            'date_posted': '2024-01-01T12:00:00Z',
            'processed_date': '2024-01-01T12:30:00Z',
            'grouped_date': '2024-01-01',
            'post_id': 'fb_post_123',
            'post_url': 'https://facebook.com/post/123',
            'post_content': 'Test Facebook post content',
            'page_name': 'Test Page',
            'page_category': 'Business',
            'page_verified': True,
            'page_followers': 1000,
            'page_likes': 500,
            'engagement_metrics': {'likes': 10, 'comments': 5},
            'content_analysis': {'language': 'en'},
            'media_metadata': {'has_image': True},
            'page_metadata': {'created_date': '2020-01-01'},
            'processing_metadata': {'version': '1.0'}
        }
        
        validated_posts = self.handler._validate_posts_schema([facebook_post])
        validated_post = validated_posts[0]
        
        # Verify platform-specific fields are present
        self.assertEqual(validated_post['platform'], 'facebook')
        self.assertEqual(validated_post['post_id'], 'fb_post_123')
        self.assertEqual(validated_post['post_url'], 'https://facebook.com/post/123')
        self.assertEqual(validated_post['page_name'], 'Test Page')
        self.assertEqual(validated_post['page_category'], 'Business')
        self.assertTrue(validated_post['page_verified'])
        self.assertEqual(validated_post['page_followers'], 1000)
        
        # Verify JSON fields are preserved
        self.assertIsInstance(validated_post['engagement_metrics'], dict)
        self.assertEqual(validated_post['engagement_metrics']['likes'], 10)
    
    def test_tiktok_validation(self):
        """Test TikTok post validation."""
        tiktok_post = {
            'id': 'test_tiktok_123',
            'crawl_id': 'crawl_456',
            'platform': 'tiktok',
            'competitor': 'test_competitor',
            'brand': 'test_brand',
            'category': 'test_category',
            'date_posted': '2024-01-01T12:00:00Z',
            'processed_date': '2024-01-01T12:30:00Z',
            'grouped_date': '2024-01-01',
            'video_id': 'tiktok_video_123',
            'video_url': 'https://tiktok.com/video/123',
            'description': 'Test TikTok video description',
            'author_name': 'Test Author',
            'author_verified': True,
            'author_follower_count': 5000,
            'play_count': 10000,
            'digg_count': 100,
            'share_count': 50,
            'comment_count': 25,
            'engagement_metrics': {'total_engagement': 175},
            'content_analysis': {'hashtag_count': 3},
            'video_metadata': {'duration': 30, 'has_music': True},
            'author_metadata': {'bio': 'Test bio'},
            'processing_metadata': {'version': '1.0'}
        }
        
        validated_posts = self.handler._validate_posts_schema([tiktok_post])
        validated_post = validated_posts[0]
        
        # Verify platform-specific fields are present
        self.assertEqual(validated_post['platform'], 'tiktok')
        self.assertEqual(validated_post['video_id'], 'tiktok_video_123')
        self.assertEqual(validated_post['video_url'], 'https://tiktok.com/video/123')
        self.assertEqual(validated_post['description'], 'Test TikTok video description')
        self.assertEqual(validated_post['author_name'], 'Test Author')
        self.assertTrue(validated_post['author_verified'])
        self.assertEqual(validated_post['author_follower_count'], 5000)
        self.assertEqual(validated_post['play_count'], 10000)
        self.assertEqual(validated_post['digg_count'], 100)
        
        # Verify TikTok doesn't have Facebook fields
        self.assertNotIn('post_id', validated_post)
        self.assertNotIn('page_name', validated_post)
        self.assertNotIn('page_category', validated_post)
        
        # Verify JSON fields are preserved
        self.assertIsInstance(validated_post['video_metadata'], dict)
        self.assertTrue(validated_post['video_metadata']['has_music'])
    
    def test_youtube_validation(self):
        """Test YouTube post validation."""
        youtube_post = {
            'id': 'test_youtube_123',
            'crawl_id': 'crawl_456',
            'platform': 'youtube',
            'competitor': 'test_competitor',
            'brand': 'test_brand',
            'category': 'test_category',
            'date_posted': '2024-01-01T12:00:00Z',
            'processed_date': '2024-01-01T12:30:00Z',
            'grouped_date': '2024-01-01',
            'video_id': 'youtube_video_123',
            'video_url': 'https://youtube.com/watch?v=123',
            'title': 'Test YouTube Video Title',
            'description': 'Test YouTube video description',
            'channel_id': 'channel_123',
            'channel_name': 'Test Channel',
            'channel_verified': True,
            'channel_subscriber_count': 50000,
            'view_count': 100000,
            'like_count': 1000,
            'comment_count': 200,
            'published_at': '2024-01-01T10:00:00Z',
            'engagement_metrics': {'engagement_rate': 0.012},
            'content_analysis': {'title_length': 25},
            'video_metadata': {'duration': 300, 'is_short': False},
            'channel_metadata': {'subscriber_count': 50000},
            'processing_metadata': {'version': '1.0'}
        }
        
        validated_posts = self.handler._validate_posts_schema([youtube_post])
        validated_post = validated_posts[0]
        
        # Verify platform-specific fields are present
        self.assertEqual(validated_post['platform'], 'youtube')
        self.assertEqual(validated_post['video_id'], 'youtube_video_123')
        self.assertEqual(validated_post['video_url'], 'https://youtube.com/watch?v=123')
        self.assertEqual(validated_post['title'], 'Test YouTube Video Title')
        self.assertEqual(validated_post['description'], 'Test YouTube video description')
        self.assertEqual(validated_post['channel_id'], 'channel_123')
        self.assertEqual(validated_post['channel_name'], 'Test Channel')
        self.assertTrue(validated_post['channel_verified'])
        self.assertEqual(validated_post['channel_subscriber_count'], 50000)
        self.assertEqual(validated_post['view_count'], 100000)
        
        # Verify YouTube doesn't have Facebook or TikTok fields
        self.assertNotIn('post_id', validated_post)
        self.assertNotIn('page_name', validated_post)
        self.assertNotIn('play_count', validated_post)
        self.assertNotIn('digg_count', validated_post)
        
        # Verify published_at timestamp is handled
        self.assertIn('published_at', validated_post)
        
        # Verify JSON fields are preserved
        self.assertIsInstance(validated_post['video_metadata'], dict)
        self.assertFalse(validated_post['video_metadata']['is_short'])
    
    def test_platform_table_routing(self):
        """Test platform-specific table routing."""
        # Test Facebook routing
        fb_table = self.handler._get_platform_table('facebook')
        self.assertEqual(fb_table, 'social_analytics.facebook_posts')
        
        # Test TikTok routing
        tiktok_table = self.handler._get_platform_table('tiktok')
        self.assertEqual(tiktok_table, 'social_analytics.tiktok_posts')
        
        # Test YouTube routing
        youtube_table = self.handler._get_platform_table('youtube')
        self.assertEqual(youtube_table, 'social_analytics.youtube_posts')
        
        # Test unknown platform fallback
        unknown_table = self.handler._get_platform_table('unknown')
        self.assertEqual(unknown_table, 'social_analytics.posts')
    
    def test_safe_int_conversion(self):
        """Test safe integer conversion."""
        # Test valid integers
        self.assertEqual(self.handler._safe_int(100), 100)
        self.assertEqual(self.handler._safe_int('200'), 200)
        self.assertEqual(self.handler._safe_int(300.5), 300)
        
        # Test None and invalid values
        self.assertEqual(self.handler._safe_int(None), 0)
        self.assertEqual(self.handler._safe_int('invalid'), 0)
        self.assertEqual(self.handler._safe_int([]), 0)
    
    def test_json_field_validation(self):
        """Test JSON field validation."""
        import json
        
        # Test dict serialization
        test_dict = {'key': 'value', 'count': 5}
        result = self.handler._ensure_json_field(test_dict)
        self.assertEqual(result, json.dumps(test_dict))
        
        # Test list serialization
        test_list = [1, 2, 3]
        result = self.handler._ensure_json_field(test_list)
        self.assertEqual(result, json.dumps(test_list))
        
        # Test None handling
        self.assertIsNone(self.handler._ensure_json_field(None))
        
        # Test JSON string parsing and re-serialization
        json_string = '{"parsed": true}'
        result = self.handler._ensure_json_field(json_string)
        self.assertEqual(result, '{"parsed": true}')
        
        # Test invalid JSON string
        invalid_json = 'not json'
        self.assertIsNone(self.handler._ensure_json_field(invalid_json))
        
        # Test empty dict/list
        self.assertIsNone(self.handler._ensure_json_field({}))
        self.assertIsNone(self.handler._ensure_json_field([]))

if __name__ == '__main__':
    unittest.main()