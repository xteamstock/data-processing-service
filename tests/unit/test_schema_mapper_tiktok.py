"""
Unit tests for TikTok schema mapping and transformation.

Tests the SchemaMapper's ability to transform TikTok posts from Apify
JSON format to BigQuery format using the TikTok schema configuration.
"""

import unittest
import json
import os
from datetime import datetime
from pathlib import Path

from handlers.schema_mapper import SchemaMapper


class TestTikTokSchemaMapper(unittest.TestCase):
    """Test TikTok-specific schema mapping functionality."""
    
    def setUp(self):
        """Set up test fixtures and schema mapper."""
        # Initialize schema mapper
        schema_dir = Path(__file__).parent.parent.parent / "schemas"
        self.mapper = SchemaMapper(str(schema_dir))
        
        # Load TikTok test fixture
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "gcs-tiktok-posts.json"
        with open(fixture_path, 'r', encoding='utf-8') as f:
            self.tiktok_posts = json.load(f)
        
        # Test metadata
        self.test_metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-12T16:06:22.177Z'
        }
    
    def test_tiktok_schema_loaded(self):
        """Test that TikTok schema is properly loaded."""
        schema = self.mapper.get_schema('tiktok', '1.0.0')
        self.assertIsNotNone(schema)
        self.assertEqual(schema['platform'], 'tiktok')
        self.assertEqual(schema['schema_version'], '1.0.0')
    
    def test_transform_tiktok_post_basic_fields(self):
        """Test transformation of basic TikTok post fields."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Core identifiers
        self.assertEqual(transformed['platform'], 'tiktok')
        self.assertEqual(transformed['crawl_id'], 'test_crawl_123')
        self.assertEqual(transformed['snapshot_id'], 'test_snapshot_456')
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        
        # TikTok-specific fields
        self.assertEqual(transformed['video_id'], raw_post['id'])
        self.assertEqual(transformed['video_url'], raw_post['webVideoUrl'])
        # Description may be cleaned/preprocessed, so check it contains expected content
        self.assertIsNotNone(transformed['description'])
        self.assertIn('NUTIFOOD GROWPLUS', transformed['description'])
    
    def test_transform_author_metadata(self):
        """Test transformation of TikTok author metadata."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Author fields
        self.assertEqual(transformed['user_url'], raw_post['authorMeta']['profileUrl'])
        self.assertEqual(transformed['user_username'], raw_post['authorMeta']['name'])
        self.assertEqual(transformed['author_name'], raw_post['authorMeta']['nickName'])
        self.assertEqual(transformed['user_profile_id'], raw_post['authorMeta']['id'])
        self.assertEqual(transformed['author_verified'], raw_post['authorMeta']['verified'])
        self.assertEqual(transformed['author_follower_count'], raw_post['authorMeta']['fans'])
        
        # Author metadata nested fields
        self.assertEqual(transformed['author_metadata']['following_count'], raw_post['authorMeta']['following'])
        self.assertEqual(transformed['author_metadata']['video_count'], raw_post['authorMeta']['video'])
        # Signature may be cleaned/preprocessed
        self.assertIsNotNone(transformed['author_metadata']['signature'])
        self.assertIn('Nutifood GrowPLUS', transformed['author_metadata']['signature'])
        self.assertEqual(transformed['author_metadata']['avatar_url'], raw_post['authorMeta']['avatar'])
    
    def test_transform_engagement_metrics(self):
        """Test transformation of TikTok engagement metrics."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Direct engagement fields
        self.assertEqual(transformed['play_count'], raw_post['playCount'])
        self.assertEqual(transformed['digg_count'], raw_post['diggCount'])
        self.assertEqual(transformed['comment_count'], raw_post['commentCount'])
        self.assertEqual(transformed['share_count'], raw_post['shareCount'])
        
        # Computed engagement metrics
        expected_total = raw_post['diggCount'] + raw_post['commentCount'] + raw_post['shareCount']
        self.assertEqual(transformed['engagement_metrics']['total_engagement'], expected_total)
        
        expected_rate = expected_total / raw_post['playCount']
        self.assertAlmostEqual(transformed['engagement_metrics']['engagement_rate'], expected_rate, places=6)
        
        # Additional engagement field
        self.assertEqual(transformed['engagement_metrics']['collect_count'], raw_post['collectCount'])
    
    def test_transform_video_metadata(self):
        """Test transformation of TikTok video metadata."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Video metadata fields
        self.assertEqual(transformed['video_metadata']['video_url'], raw_post['webVideoUrl'])
        self.assertEqual(transformed['video_metadata']['cover_image_url'], raw_post['videoMeta']['coverUrl'])
        self.assertEqual(transformed['video_metadata']['duration_seconds'], raw_post['videoMeta']['duration'])
        self.assertEqual(transformed['video_metadata']['width'], raw_post['videoMeta']['width'])
        self.assertEqual(transformed['video_metadata']['height'], raw_post['videoMeta']['height'])
        
        # Computed video fields
        self.assertTrue(transformed['video_metadata']['has_music'])  # Should detect music
        self.assertEqual(transformed['video_metadata']['aspect_ratio'], "9:16")  # 576x1024 is 9:16
    
    def test_transform_music_metadata(self):
        """Test transformation of TikTok music metadata."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Music fields
        self.assertEqual(transformed['video_metadata']['music_id'], raw_post['musicMeta']['musicId'])
        self.assertEqual(transformed['video_metadata']['music_title'], raw_post['musicMeta']['musicName'])
        self.assertEqual(transformed['video_metadata']['music_author'], raw_post['musicMeta']['musicAuthor'])
        self.assertEqual(transformed['video_metadata']['is_original_sound'], raw_post['musicMeta']['musicOriginal'])
    
    def test_transform_content_analysis(self):
        """Test transformation of TikTok content analysis fields."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Content analysis fields
        expected_hashtags = [hashtag['name'] for hashtag in raw_post['hashtags']]
        self.assertEqual(transformed['content_analysis']['hashtags'], expected_hashtags)
        
        self.assertEqual(transformed['content_analysis']['mentions'], raw_post['mentions'])
        self.assertEqual(transformed['content_analysis']['is_ad'], raw_post['isAd'])
        
        # Computed content fields - text length is calculated from cleaned description
        description_length = len(transformed.get('description', ''))
        self.assertEqual(transformed['content_analysis']['text_length'], description_length)
        self.assertEqual(transformed['content_analysis']['hashtag_count'], len(raw_post['hashtags']))
        self.assertIn(transformed['content_analysis']['language'], ['vi', 'unknown'])  # Vietnamese content
        self.assertIsInstance(transformed['content_analysis']['sentiment_score'], float)
        self.assertGreaterEqual(transformed['content_analysis']['sentiment_score'], -1.0)
        self.assertLessEqual(transformed['content_analysis']['sentiment_score'], 1.0)
    
    def test_transform_temporal_fields(self):
        """Test transformation of TikTok temporal fields."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Temporal fields (timestamps may be normalized)
        expected_date_posted = raw_post['createTimeISO'].replace('Z', '+00:00')
        self.assertEqual(transformed['date_posted'], expected_date_posted)
        self.assertEqual(transformed['grouped_date'], raw_post['createTimeISO'].split('T')[0])
        
        # Processing metadata temporal fields
        self.assertIsNotNone(transformed['processed_date'])
        self.assertIsNotNone(transformed['crawl_date'])
    
    def test_data_quality_calculation(self):
        """Test TikTok-specific data quality score calculation."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        quality_score = transformed['processing_metadata']['data_quality_score']
        self.assertIsInstance(quality_score, float)
        self.assertGreaterEqual(quality_score, 0.0)
        self.assertLessEqual(quality_score, 1.0)
        
        # Should have high quality for complete posts
        self.assertGreater(quality_score, 0.7)  # Has description, engagement, video metadata, author, date
    
    def test_preprocessing_functions(self):
        """Test TikTok-specific preprocessing functions."""
        # Test hashtag extraction
        hashtags = [{'name': 'nutifoodgrowplus'}, {'name': 'lactoferrin'}]
        extracted = self.mapper._extract_hashtag_names(hashtags)
        self.assertEqual(extracted, ['nutifoodgrowplus', 'lactoferrin'])
        
        # Test text cleaning
        dirty_text = "  Extra   spaces   here  "
        cleaned = self.mapper._remove_extra_whitespace(dirty_text)
        self.assertEqual(cleaned, "Extra spaces here")
        
        # Test safe int conversion
        self.assertEqual(self.mapper._safe_int("123"), 123)
        self.assertEqual(self.mapper._safe_int("invalid"), 0)
        self.assertEqual(self.mapper._safe_int(None), 0)
    
    def test_tiktok_computation_functions(self):
        """Test TikTok-specific computation functions."""
        # Test engagement calculation
        test_data = {
            'digg_count': 100,
            'comment_count': 20,
            'share_count': 5
        }
        
        total_engagement = self.mapper._sum_tiktok_engagement({}, test_data)
        self.assertEqual(total_engagement, 125)
        
        # Test engagement rate calculation
        test_data['play_count'] = 1000
        engagement_rate = self.mapper._calculate_tiktok_engagement_rate({}, test_data)
        self.assertEqual(engagement_rate, 0.125)
        
        # Test aspect ratio calculation
        test_video_data = {
            'video_metadata': {
                'width': 576,
                'height': 1024
            }
        }
        aspect_ratio = self.mapper._calculate_aspect_ratio({}, test_video_data)
        self.assertEqual(aspect_ratio, "9:16")
    
    def test_schema_validation(self):
        """Test TikTok schema validation rules."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Required fields should be present
        self.assertIsNotNone(transformed.get('video_id'))
        self.assertIsNotNone(transformed.get('video_url'))
        self.assertIsNotNone(transformed.get('date_posted'))
        
        # URL validation
        self.assertTrue(transformed['video_url'].startswith('https://'))
        self.assertTrue(transformed['user_url'].startswith('https://'))
    
    def test_missing_fields_handling(self):
        """Test handling of missing or null fields in TikTok posts."""
        # Create minimal post with missing optional fields
        minimal_post = {
            'id': 'test_video_id',
            'webVideoUrl': 'https://example.com/video',
            'createTimeISO': '2025-07-12T10:00:00.000Z',
            'text': 'Test description',
            'authorMeta': {
                'name': 'testuser'
            },
            'diggCount': 0,
            'commentCount': 0,
            'shareCount': 0,
            'playCount': 100,
            'hashtags': [],
            'mentions': []
        }
        
        transformed = self.mapper.transform_post(minimal_post, 'tiktok', self.test_metadata)
        
        # Should handle missing fields gracefully with defaults
        self.assertEqual(transformed['author_follower_count'], 0)
        self.assertEqual(transformed['engagement_metrics']['collect_count'], 0)
        self.assertFalse(transformed['video_metadata']['has_music'])
        self.assertEqual(transformed['content_analysis']['hashtags'], [])
        self.assertEqual(transformed['content_analysis']['hashtag_count'], 0)
    
    def test_multiple_posts_transformation(self):
        """Test transformation of multiple TikTok posts."""
        for i, raw_post in enumerate(self.tiktok_posts[:3]):  # Test first 3 posts
            with self.subTest(post_index=i):
                transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
                
                # Verify core structure
                self.assertIn('video_id', transformed)
                self.assertIn('video_url', transformed)
                self.assertIn('description', transformed)
                self.assertIn('date_posted', transformed)
                self.assertIn('engagement_metrics', transformed)
                self.assertIn('video_metadata', transformed)
                self.assertIn('content_analysis', transformed)
                self.assertIn('author_metadata', transformed)
                self.assertIn('processing_metadata', transformed)
                
                # Verify data quality
                quality_score = transformed['processing_metadata']['data_quality_score']
                self.assertGreater(quality_score, 0.0)
    
    def test_business_context_preservation(self):
        """Test that business context is preserved across transformation."""
        raw_post = self.tiktok_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Business context should be preserved
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        self.assertEqual(transformed['platform'], 'tiktok')
        
        # ID should include crawl context
        expected_id = f"{raw_post['id']}_test_crawl_123"
        self.assertEqual(transformed['id'], expected_id)
    
    def test_unicode_text_handling(self):
        """Test handling of Unicode text in TikTok descriptions."""
        raw_post = self.tiktok_posts[0].copy()
        
        # TikTok posts often contain Vietnamese text and emojis
        unicode_text = "VỀ KỂ KHÔNG AI TIN ??? - MẸ THU HƯƠNG 🔎💗 #NutifoodGrowPLUS"
        raw_post['text'] = unicode_text
        
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Unicode text should be preserved
        self.assertIn('VỀ KỂ KHÔNG AI TIN', transformed['description'])
        self.assertIn('MẸ THU HƯƠNG', transformed['description'])
        
        # Text length should be calculated correctly from cleaned text
        description_length = len(transformed.get('description', ''))
        self.assertEqual(transformed['content_analysis']['text_length'], description_length)
    
    def test_timestamp_parsing(self):
        """Test various timestamp formats in TikTok posts."""
        raw_post = self.tiktok_posts[0].copy()
        
        # Test ISO timestamp parsing
        iso_timestamp = "2025-07-11T08:27:53.000Z"
        raw_post['createTimeISO'] = iso_timestamp
        
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Should normalize ISO format for BigQuery
        expected_timestamp = iso_timestamp.replace('Z', '+00:00')
        self.assertEqual(transformed['date_posted'], expected_timestamp)
        self.assertEqual(transformed['grouped_date'], "2025-07-11")
    
    def test_edge_cases(self):
        """Test edge cases in TikTok data transformation."""
        raw_post = self.tiktok_posts[0].copy()
        
        # Test zero engagement
        raw_post['diggCount'] = 0
        raw_post['commentCount'] = 0
        raw_post['shareCount'] = 0
        raw_post['playCount'] = 0
        
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        
        # Should handle zero values gracefully
        self.assertEqual(transformed['engagement_metrics']['total_engagement'], 0)
        self.assertEqual(transformed['engagement_metrics']['engagement_rate'], 0.0)  # Avoid division by zero
        
        # Test empty text
        raw_post['text'] = ""
        transformed = self.mapper.transform_post(raw_post, 'tiktok', self.test_metadata)
        self.assertEqual(transformed['description'], "")
        self.assertEqual(transformed['content_analysis']['text_length'], 0)


if __name__ == '__main__':
    unittest.main()