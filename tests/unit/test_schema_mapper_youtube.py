"""
Unit tests for YouTube schema mapping and transformation.

Tests the SchemaMapper's ability to transform YouTube posts from Apify
JSON format to BigQuery format using the YouTube schema configuration.
"""

import unittest
import json
import os
from datetime import datetime
from pathlib import Path

from handlers.schema_mapper import SchemaMapper


class TestYouTubeSchemaMapper(unittest.TestCase):
    """Test YouTube-specific schema mapping functionality."""
    
    def setUp(self):
        """Set up test fixtures and schema mapper."""
        # Initialize schema mapper
        schema_dir = Path(__file__).parent.parent.parent / "schemas"
        self.mapper = SchemaMapper(str(schema_dir))
        
        # Load YouTube test fixture
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "gcs-youtube-posts.json"
        with open(fixture_path, 'r', encoding='utf-8') as f:
            self.youtube_posts = json.load(f)
        
        # Test metadata
        self.test_metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-12T16:06:22.177Z'
        }
    
    def test_youtube_schema_loaded(self):
        """Test that YouTube schema is properly loaded."""
        schema = self.mapper.get_schema('youtube', '1.0.0')
        self.assertIsNotNone(schema)
        self.assertEqual(schema['platform'], 'youtube')
        self.assertEqual(schema['schema_version'], '1.0.0')
    
    def test_transform_youtube_post_basic_fields(self):
        """Test transformation of basic YouTube post fields."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Core identifiers
        self.assertEqual(transformed['platform'], 'youtube')
        self.assertEqual(transformed['crawl_id'], 'test_crawl_123')
        self.assertEqual(transformed['snapshot_id'], 'test_snapshot_456')
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        
        # YouTube-specific fields
        self.assertEqual(transformed['video_id'], raw_post['id'])
        self.assertEqual(transformed['video_url'], raw_post['url'])
        self.assertEqual(transformed['title'], raw_post['title'])
        # Description maps from 'text' field and may be cleaned/preprocessed
        self.assertIsNotNone(transformed.get('description'))
        self.assertIn('Värna Diabetes', transformed.get('description', ''))
    
    def test_transform_channel_metadata(self):
        """Test transformation of YouTube channel metadata."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Channel fields
        self.assertEqual(transformed['channel_url'], raw_post['channelUrl'])
        self.assertEqual(transformed['channel_id'], raw_post['channelId'])
        self.assertEqual(transformed['channel_name'], raw_post['channelName'])
        self.assertEqual(transformed['channel_verified'], raw_post['isChannelVerified'])
        self.assertEqual(transformed['channel_subscriber_count'], raw_post['numberOfSubscribers'])
        
        # Channel metadata nested fields
        if 'channelDescription' in raw_post:
            # Description may be cleaned/preprocessed, so check it contains expected content
            self.assertIsNotNone(transformed['channel_metadata']['description'])
            self.assertIn('Giải pháp dinh dưỡng', transformed['channel_metadata']['description'])
        if 'channelTotalViews' in raw_post:
            self.assertEqual(transformed['channel_metadata']['total_views'], raw_post['channelTotalViews'])
    
    def test_transform_engagement_metrics(self):
        """Test transformation of YouTube engagement metrics."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Direct engagement fields
        self.assertEqual(transformed['view_count'], raw_post['viewCount'])
        self.assertEqual(transformed['like_count'], raw_post['likes'])
        self.assertEqual(transformed['comment_count'], raw_post.get('numberOfComments', 0))
        
        # Computed engagement metrics
        expected_total = raw_post['likes'] + raw_post.get('numberOfComments', 0)
        self.assertEqual(transformed['engagement_metrics']['total_engagement'], expected_total)
        
        if raw_post['viewCount'] > 0:
            expected_rate = expected_total / raw_post['viewCount']
            self.assertAlmostEqual(transformed['engagement_metrics']['engagement_rate'], expected_rate, places=6)
        else:
            self.assertEqual(transformed['engagement_metrics']['engagement_rate'], 0.0)
    
    def test_transform_video_metadata(self):
        """Test transformation of YouTube video metadata."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Video metadata fields
        self.assertEqual(transformed['video_metadata']['thumbnail_url'], raw_post['thumbnailUrl'])
        if 'duration' in raw_post:
            self.assertEqual(transformed['video_metadata']['duration'], raw_post['duration'])
        
        # Computed video fields
        if 'duration' in raw_post:
            duration_seconds = transformed['video_metadata']['duration_seconds']
            self.assertIsInstance(duration_seconds, int)
            self.assertGreaterEqual(duration_seconds, 0)
            
            # Test YouTube Short detection (videos <= 60 seconds)
            is_short = transformed['video_metadata']['is_short']
            self.assertEqual(is_short, duration_seconds <= 60)
        
    
    def test_transform_content_analysis(self):
        """Test transformation of YouTube content analysis fields."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        
        if 'location' in raw_post and raw_post['location']:
            self.assertEqual(transformed['content_analysis']['location'], raw_post['location'])
        else:
            # Location can be null/empty in YouTube data
            self.assertEqual(transformed['content_analysis']['location'], '')
        
        # Computed content fields
        self.assertEqual(transformed['content_analysis']['title_length'], len(raw_post['title']))
        
        # Text length is calculated from the transformed description
        if 'text' in raw_post and raw_post['text']:
            # Check that text_length matches the transformed description length
            description_length = len(transformed.get('description', ''))
            self.assertEqual(transformed['content_analysis']['text_length'], description_length)
            # Verify we have actual content
            self.assertGreater(transformed['content_analysis']['text_length'], 0)
        else:
            self.assertEqual(transformed['content_analysis']['text_length'], 0)
        
        # Language and sentiment
        self.assertIn(transformed['content_analysis']['language'], ['vi', 'en', 'unknown'])
        self.assertIsInstance(transformed['content_analysis']['sentiment_score'], float)
        self.assertGreaterEqual(transformed['content_analysis']['sentiment_score'], -1.0)
        self.assertLessEqual(transformed['content_analysis']['sentiment_score'], 1.0)
    
    def test_transform_temporal_fields(self):
        """Test transformation of YouTube temporal fields."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Temporal fields (timestamps may be normalized)
        expected_published_at = raw_post['date'].replace('Z', '+00:00')
        self.assertEqual(transformed['published_at'], expected_published_at)
        self.assertEqual(transformed['grouped_date'], raw_post['date'].split('T')[0])
        
        # Processing metadata temporal fields
        self.assertIsNotNone(transformed['processed_date'])
        self.assertIsNotNone(transformed['crawl_date'])
    
    def test_data_quality_calculation(self):
        """Test YouTube-specific data quality score calculation."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        quality_score = transformed['processing_metadata']['data_quality_score']
        self.assertIsInstance(quality_score, float)
        self.assertGreaterEqual(quality_score, 0.0)
        self.assertLessEqual(quality_score, 1.0)
        
        # Should have good quality for complete posts
        self.assertGreater(quality_score, 0.5)  # Has title, engagement, video metadata, channel, date
    
    def test_youtube_computation_functions(self):
        """Test YouTube-specific computation functions."""
        # Test engagement calculation
        test_data = {
            'like_count': 100,
            'comment_count': 20
        }
        
        total_engagement = self.mapper._sum_youtube_engagement({}, test_data)
        self.assertEqual(total_engagement, 120)
        
        # Test engagement rate calculation
        test_data['view_count'] = 1000
        engagement_rate = self.mapper._calculate_youtube_engagement_rate({}, test_data)
        self.assertEqual(engagement_rate, 0.12)
        
        # Test duration parsing
        test_cases = [
            ("PT4M13S", 253),  # 4 minutes 13 seconds
            ("PT1M30S", 90),   # 1 minute 30 seconds
            ("PT45S", 45),     # 45 seconds
            ("4:13", 253),     # 4:13 format
            ("1:30", 90),      # 1:30 format
            ("1:23:45", 5025), # 1 hour 23 minutes 45 seconds
        ]
        
        for duration_str, expected_seconds in test_cases:
            with self.subTest(duration=duration_str):
                test_video_data = {
                    'video_metadata': {
                        'duration': duration_str
                    }
                }
                result = self.mapper._parse_youtube_duration({}, test_video_data)
                self.assertEqual(result, expected_seconds)
        
        # Test YouTube Short detection (needs duration field, not duration_seconds)
        short_duration_data = {'video_metadata': {'duration': 'PT45S'}}
        long_duration_data = {'video_metadata': {'duration': 'PT5M'}}
        
        self.assertTrue(self.mapper._check_is_youtube_short({}, short_duration_data))
        self.assertFalse(self.mapper._check_is_youtube_short({}, long_duration_data))
    
    def test_schema_validation(self):
        """Test YouTube schema validation rules."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Required fields should be present
        self.assertIsNotNone(transformed.get('video_id'))
        self.assertIsNotNone(transformed.get('video_url'))
        self.assertIsNotNone(transformed.get('title'))
        self.assertIsNotNone(transformed.get('channel_id'))
        self.assertIsNotNone(transformed.get('channel_name'))
        self.assertIsNotNone(transformed.get('published_at'))
        
        # URL validation
        self.assertTrue(transformed['video_url'].startswith('https://'))
        self.assertTrue(transformed['channel_url'].startswith('https://'))
        self.assertTrue(transformed['video_metadata']['thumbnail_url'].startswith('https://'))
    
    def test_missing_fields_handling(self):
        """Test handling of missing or null fields in YouTube posts."""
        # Create minimal post with missing optional fields
        minimal_post = {
            'id': 'test_video_id',
            'url': 'https://youtube.com/watch?v=test',
            'title': 'Test Video Title',
            'channelId': 'test_channel_id',
            'channelName': 'Test Channel',
            'channelUrl': 'https://youtube.com/channel/test',
            'date': '2025-07-12T10:00:00.000Z',
            'thumbnailUrl': 'https://example.com/thumb.jpg',
            'viewCount': 100,
            'likes': 5,
            'isChannelVerified': False,
            'numberOfSubscribers': 1000
        }
        
        transformed = self.mapper.transform_post(minimal_post, 'youtube', self.test_metadata)
        
        # Should handle missing fields gracefully with defaults
        self.assertEqual(transformed['comment_count'], 0)
        self.assertEqual(transformed['content_analysis']['text_length'], 0)
    
    def test_multiple_posts_transformation(self):
        """Test transformation of multiple YouTube posts."""
        for i, raw_post in enumerate(self.youtube_posts[:3]):  # Test first 3 posts
            with self.subTest(post_index=i):
                transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
                
                # Verify core structure
                self.assertIn('video_id', transformed)
                self.assertIn('video_url', transformed)
                self.assertIn('title', transformed)
                self.assertIn('channel_id', transformed)
                self.assertIn('channel_name', transformed)
                self.assertIn('published_at', transformed)
                self.assertIn('engagement_metrics', transformed)
                self.assertIn('video_metadata', transformed)
                self.assertIn('content_analysis', transformed)
                self.assertIn('channel_metadata', transformed)
                self.assertIn('processing_metadata', transformed)
                
                # Verify data quality
                quality_score = transformed['processing_metadata']['data_quality_score']
                self.assertGreater(quality_score, 0.0)
    
    def test_business_context_preservation(self):
        """Test that business context is preserved across transformation."""
        raw_post = self.youtube_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Business context should be preserved
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        self.assertEqual(transformed['platform'], 'youtube')
        
        # ID should include crawl context
        expected_id = f"{raw_post['id']}_test_crawl_123"
        self.assertEqual(transformed['id'], expected_id)
    
    def test_unicode_text_handling(self):
        """Test handling of Unicode text in YouTube titles and descriptions."""
        raw_post = self.youtube_posts[0].copy()
        
        # YouTube posts often contain Vietnamese text and special characters
        unicode_title = "VÄRNA DIABETES - ỔN ĐỊNH ĐƯỜNG HUYẾT, CẢ NHÀ AN TÂM!"
        unicode_desc = "Với slogan \"Giải pháp dinh dưỡng của chuyên gia\" 💗"
        
        raw_post['title'] = unicode_title
        raw_post['text'] = unicode_desc
        
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Unicode text should be preserved
        self.assertIn('VÄRNA DIABETES', transformed['title'])
        self.assertIn('ỔN ĐỊNH ĐƯỜNG HUYẾT', transformed['title'])
        self.assertIn('Giải pháp dinh dưỡng', transformed['description'])
        
        # Text length should be calculated correctly from transformed content
        self.assertEqual(transformed['content_analysis']['title_length'], len(unicode_title))
        # Text length is calculated from the cleaned/processed description
        description_length = len(transformed.get('description', ''))
        self.assertEqual(transformed['content_analysis']['text_length'], description_length)
    
    def test_timestamp_parsing(self):
        """Test various timestamp formats in YouTube posts."""
        raw_post = self.youtube_posts[0].copy()
        
        # Test ISO timestamp parsing
        iso_timestamp = "2025-07-08T10:41:45.000Z"
        raw_post['date'] = iso_timestamp
        
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Should normalize ISO format for BigQuery
        expected_timestamp = iso_timestamp.replace('Z', '+00:00')
        self.assertEqual(transformed['published_at'], expected_timestamp)
        self.assertEqual(transformed['grouped_date'], "2025-07-08")
    
    def test_duration_parsing_edge_cases(self):
        """Test edge cases in YouTube duration parsing."""
        test_cases = [
            ("", 0),              # Empty string
            ("invalid", 0),       # Invalid format
            ("PT0S", 0),         # Zero seconds
            ("PT1H", 3600),      # 1 hour only
            ("PT1M", 60),        # 1 minute only
            ("0:00", 0),         # Zero in HH:MM format
            ("0:30", 30),        # 30 seconds
        ]
        
        for duration_str, expected_seconds in test_cases:
            with self.subTest(duration=duration_str):
                test_video_data = {
                    'video_metadata': {
                        'duration': duration_str
                    }
                }
                result = self.mapper._parse_youtube_duration({}, test_video_data)
                self.assertEqual(result, expected_seconds)
    
    def test_engagement_rate_edge_cases(self):
        """Test edge cases in YouTube engagement rate calculation."""
        # Test zero views (avoid division by zero)
        test_data = {
            'like_count': 10,
            'comment_count': 5,
            'view_count': 0
        }
        
        engagement_rate = self.mapper._calculate_youtube_engagement_rate({}, test_data)
        self.assertEqual(engagement_rate, 0.0)
        
        # Test zero engagement
        test_data = {
            'like_count': 0,
            'comment_count': 0,
            'view_count': 1000
        }
        
        engagement_rate = self.mapper._calculate_youtube_engagement_rate({}, test_data)
        self.assertEqual(engagement_rate, 0.0)
    
    
    def test_monetization_fields(self):
        """Test YouTube monetization field transformation."""
        raw_post = self.youtube_posts[0].copy()
        
        # Add monetization fields
        raw_post['isMonetized'] = True
        
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Should transform monetization fields
        self.assertTrue(transformed['content_analysis']['is_monetized'])
    
    def test_channel_creation_date_handling(self):
        """Test handling of channel creation date."""
        raw_post = self.youtube_posts[0].copy()
        
        # Add channel creation date
        creation_date = "Jun 12, 2013"
        raw_post['channelJoinedDate'] = creation_date
        
        # Note: This field might need custom parsing since it's in a different format
        # For now, we test that it doesn't break the transformation
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Should not break transformation
        self.assertIsInstance(transformed, dict)
        self.assertIn('channel_id', transformed)
    
    def test_edge_cases(self):
        """Test edge cases in YouTube data transformation."""
        raw_post = self.youtube_posts[0].copy()
        
        # Test zero engagement
        raw_post['viewCount'] = 0
        raw_post['likes'] = 0
        raw_post['numberOfComments'] = 0
        
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        
        # Should handle zero values gracefully
        self.assertEqual(transformed['engagement_metrics']['total_engagement'], 0)
        self.assertEqual(transformed['engagement_metrics']['engagement_rate'], 0.0)
        
        # Test empty title (should not happen in practice but test resilience)
        raw_post['title'] = ""
        transformed = self.mapper.transform_post(raw_post, 'youtube', self.test_metadata)
        self.assertEqual(transformed['title'], "")
        self.assertEqual(transformed['content_analysis']['title_length'], 0)
    
    def test_preprocessing_functions(self):
        """Test YouTube-specific preprocessing functions."""
        # Test text cleaning
        dirty_text = "  Extra   spaces   here  "
        cleaned = self.mapper._clean_text(dirty_text)
        self.assertEqual(cleaned, "Extra spaces here")
        
        # Test safe int conversion
        self.assertEqual(self.mapper._safe_int("123"), 123)
        self.assertEqual(self.mapper._safe_int("invalid"), 0)
        self.assertEqual(self.mapper._safe_int(None), 0)
        
        # Test ISO timestamp parsing (Z gets normalized to +00:00)
        iso_timestamp = "2025-07-08T10:41:45.000Z"
        parsed = self.mapper._parse_iso_timestamp(iso_timestamp)
        expected = iso_timestamp.replace('Z', '+00:00')
        self.assertEqual(parsed, expected)


if __name__ == '__main__':
    unittest.main()