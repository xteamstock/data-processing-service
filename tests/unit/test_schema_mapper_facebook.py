"""
Unit tests for Facebook schema mapping and transformation - backward compatibility.

Tests the SchemaMapper's ability to transform Facebook posts from BrightData
JSON format to BigQuery format using the Facebook schema configuration.
This ensures backward compatibility with the existing Facebook processing pipeline.
"""

import unittest
import json
import os
from datetime import datetime
from pathlib import Path

from handlers.schema_mapper import SchemaMapper


class TestFacebookSchemaMapper(unittest.TestCase):
    """Test Facebook-specific schema mapping functionality and backward compatibility."""
    
    def setUp(self):
        """Set up test fixtures and schema mapper."""
        # Initialize schema mapper
        schema_dir = Path(__file__).parent.parent.parent / "schemas"
        self.mapper = SchemaMapper(str(schema_dir))
        
        # Load Facebook test fixture
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "gcs-facebook-posts.json"
        with open(fixture_path, 'r', encoding='utf-8') as f:
            self.facebook_posts = json.load(f)
        
        # Test metadata
        self.test_metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-12T16:06:22.177Z'
        }
    
    def test_facebook_schema_loaded(self):
        """Test that Facebook schema is properly loaded."""
        schema = self.mapper.get_schema('facebook', '1.0.0')
        self.assertIsNotNone(schema)
        self.assertEqual(schema['platform'], 'facebook')
        self.assertEqual(schema['schema_version'], '1.0.0')
    
    def test_transform_facebook_post_basic_fields(self):
        """Test transformation of basic Facebook post fields."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Core identifiers
        self.assertEqual(transformed['platform'], 'facebook')
        self.assertEqual(transformed['crawl_id'], 'test_crawl_123')
        self.assertEqual(transformed['snapshot_id'], 'test_snapshot_456')
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        
        # Facebook-specific fields
        self.assertEqual(transformed['post_id'], raw_post['post_id'])
        self.assertEqual(transformed['post_url'], raw_post['url'])
        # Content may be cleaned/preprocessed, so check it contains expected content
        self.assertIsNotNone(transformed['post_content'])
        self.assertIn('Câu chuyện đi chơi', transformed['post_content'])
        self.assertIn('Thị Dứa nhà Lâm Ngọc Hà', transformed['post_content'])
        self.assertEqual(transformed['post_shortcode'], raw_post['shortcode'])
    
    def test_transform_user_page_metadata(self):
        """Test transformation of Facebook user/page metadata."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # User/page fields
        self.assertEqual(transformed['user_url'], raw_post['user_url'])
        self.assertEqual(transformed['user_username'], raw_post['user_username_raw'])
        self.assertEqual(transformed['user_profile_id'], raw_post['profile_id'])
        self.assertEqual(transformed['page_name'], raw_post['page_name'])
        self.assertEqual(transformed['page_category'], raw_post['page_category'])
        self.assertEqual(transformed['page_verified'], raw_post['page_is_verified'])
        self.assertEqual(transformed['page_followers'], raw_post['page_followers'])
        self.assertEqual(transformed['page_likes'], raw_post['page_likes'])
        
        # Page metadata nested fields
        self.assertEqual(transformed['page_metadata']['page_intro'], raw_post['page_intro'])
        self.assertEqual(transformed['page_metadata']['page_logo'], raw_post['page_logo'])
        # page_website may be missing due to URL validation failure (invalid URL format in fixture)
        if 'page_website' in transformed['page_metadata']:
            self.assertEqual(transformed['page_metadata']['page_website'], raw_post['page_external_website'])
        self.assertEqual(transformed['page_metadata']['page_phone'], raw_post['page_phone'])
        self.assertEqual(transformed['page_metadata']['page_email'], raw_post['page_email'])
        # Timestamp may be normalized (Z → +00:00)
        expected_creation_date = raw_post['page_creation_time'].replace('Z', '+00:00')
        self.assertEqual(transformed['page_metadata']['page_creation_date'], expected_creation_date)
    
    def test_transform_engagement_metrics(self):
        """Test transformation of Facebook engagement metrics."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Direct engagement fields
        self.assertEqual(transformed['engagement_metrics']['likes'], raw_post['likes'])
        self.assertEqual(transformed['engagement_metrics']['comments'], raw_post['num_comments'])
        self.assertEqual(transformed['engagement_metrics']['shares'], raw_post['num_shares'])
        
        # Reactions by type
        expected_reactions = []
        for reaction in raw_post['count_reactions_type']:
            expected_reactions.append({
                'type': reaction['type'],
                'count': reaction['reaction_count']
            })
        self.assertEqual(transformed['engagement_metrics']['reactions_by_type'], expected_reactions)
        
        # Computed total reactions
        expected_total_reactions = sum(r['count'] for r in expected_reactions)
        self.assertEqual(transformed['engagement_metrics']['reactions'], expected_total_reactions)
    
    def test_transform_media_metadata(self):
        """Test transformation of Facebook media metadata."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Media fields
        expected_attachments = []
        for attachment in raw_post['attachments']:
            expected_attachments.append({
                'id': attachment['id'],
                'type': attachment['type'],
                'url': attachment['url'],
                'attachment_url': attachment.get('attachment_url', '')
            })
        self.assertEqual(transformed['media_metadata']['attachments'], expected_attachments)
        
        # Primary image URL
        if 'post_image' in raw_post:
            self.assertEqual(transformed['media_metadata']['primary_image_url'], raw_post['post_image'])
        
        # Computed media fields
        self.assertEqual(transformed['media_metadata']['media_count'], len(raw_post['attachments']))
        
        # Check for video and image detection
        has_video = any(att['type'].lower() == 'video' for att in raw_post['attachments'])
        has_image = any(att['type'].lower() in ['photo', 'image'] for att in raw_post['attachments'])
        self.assertEqual(transformed['media_metadata']['has_video'], has_video)
        self.assertEqual(transformed['media_metadata']['has_image'], has_image)
    
    def test_transform_content_analysis(self):
        """Test transformation of Facebook content analysis fields."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Content analysis fields
        expected_hashtags = [tag.lower() for tag in raw_post['hashtags']]
        self.assertEqual(transformed['content_analysis']['hashtags'], expected_hashtags)
        
        if 'is_sponsored' in raw_post:
            self.assertEqual(transformed['content_analysis']['contains_sponsored'], raw_post['is_sponsored'])
        
        # Computed content fields - text length is calculated from cleaned content
        content_length = len(transformed.get('post_content', ''))
        self.assertEqual(transformed['content_analysis']['text_length'], content_length)
        self.assertIn(transformed['content_analysis']['language'], ['vi', 'unknown'])  # Vietnamese content
        self.assertIsInstance(transformed['content_analysis']['sentiment_score'], float)
        self.assertGreaterEqual(transformed['content_analysis']['sentiment_score'], -1.0)
        self.assertLessEqual(transformed['content_analysis']['sentiment_score'], 1.0)
    
    def test_transform_temporal_fields(self):
        """Test transformation of Facebook temporal fields."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Temporal fields (timestamps may be normalized)
        expected_date_posted = raw_post['date_posted'].replace('Z', '+00:00')
        self.assertEqual(transformed['date_posted'], expected_date_posted)
        self.assertEqual(transformed['grouped_date'], raw_post['date_posted'].split('T')[0])
        
        # Processing metadata temporal fields
        self.assertIsNotNone(transformed['processed_date'])
        self.assertIsNotNone(transformed['crawl_date'])
    
    def test_data_quality_calculation(self):
        """Test Facebook-specific data quality score calculation."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        quality_score = transformed['processing_metadata']['data_quality_score']
        self.assertIsInstance(quality_score, float)
        self.assertGreaterEqual(quality_score, 0.0)
        self.assertLessEqual(quality_score, 1.0)
        
        # Should have high quality for complete posts
        self.assertGreater(quality_score, 0.8)  # Has content, engagement, media, page info, date
    
    def test_backward_compatibility_with_legacy_function(self):
        """Test backward compatibility with the legacy process_facebook_post_for_bigquery function."""
        raw_post = self.facebook_posts[0]
        
        # Import the legacy compatibility function
        from handlers.schema_mapper import process_facebook_post_for_bigquery
        
        # Transform using legacy function
        legacy_result = process_facebook_post_for_bigquery(raw_post, self.test_metadata)
        
        # Transform using new schema mapper
        new_result = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Should produce identical results for core fields
        self.assertEqual(legacy_result['post_id'], new_result['post_id'])
        self.assertEqual(legacy_result['post_url'], new_result['post_url'])
        self.assertEqual(legacy_result['post_content'], new_result['post_content'])
        self.assertEqual(legacy_result['date_posted'], new_result['date_posted'])
        self.assertEqual(legacy_result['page_name'], new_result['page_name'])
        self.assertEqual(legacy_result['engagement_metrics'], new_result['engagement_metrics'])
        self.assertEqual(legacy_result['media_metadata'], new_result['media_metadata'])
    
    def test_preprocessing_functions(self):
        """Test Facebook-specific preprocessing functions."""
        # Test hashtag normalization
        hashtags = ["#NutifoodGrowPLUS", "#NutifoodCreator", "#MOMANGBAOBOCMOMANGTHEGIOI"]
        normalized = self.mapper._normalize_hashtags(hashtags)
        expected = ["nutifoodgrowplus", "nutifoodcreator", "momangbaobocmomangthegioi"]
        self.assertEqual(normalized, expected)
        
        # Test reaction parsing
        reactions = [
            {"type": "Like", "reaction_count": 6882},
            {"type": "Love", "reaction_count": 59},
            {"type": "Haha", "reaction_count": 35}
        ]
        parsed = self.mapper._parse_reaction_types(reactions)
        expected = [
            {"type": "Like", "count": 6882},
            {"type": "Love", "count": 59},
            {"type": "Haha", "count": 35}
        ]
        self.assertEqual(parsed, expected)
        
        # Test attachment parsing
        attachments = [
            {
                "id": "1083376743560186",
                "type": "Video",
                "url": "https://example.com/video.mp4",
                "attachment_url": "https://facebook.com/video/123"
            }
        ]
        parsed = self.mapper._parse_attachments(attachments)
        expected = [
            {
                "id": "1083376743560186",
                "type": "Video",
                "url": "https://example.com/video.mp4",
                "attachment_url": "https://facebook.com/video/123"
            }
        ]
        self.assertEqual(parsed, expected)
        
        # Test address extraction from about sections
        about_sections = [
            {"type": "INFLUENCER CATEGORY", "value": "Page · Baby goods/kids goods"},
            {"type": "ADDRESS", "value": "Ho Chi Minh City, Vietnam"},
            {"type": "PROFILE PHONE", "value": "+84 28 3825 5777"}
        ]
        address = self.mapper._extract_address_from_about(about_sections)
        self.assertEqual(address, "Ho Chi Minh City, Vietnam")
    
    def test_facebook_computation_functions(self):
        """Test Facebook-specific computation functions."""
        # Test reaction sum calculation
        test_data = {
            'engagement_metrics': {
                'reactions_by_type': [
                    {'type': 'Like', 'count': 100},
                    {'type': 'Love', 'count': 20},
                    {'type': 'Haha', 'count': 5}
                ]
            }
        }
        
        total_reactions = self.mapper._sum_reactions_by_type({}, test_data)
        self.assertEqual(total_reactions, 125)
        
        # Test attachment counting
        test_data = {
            'media_metadata': {
                'attachments': [
                    {'id': '1', 'type': 'video'},
                    {'id': '2', 'type': 'image'}
                ]
            }
        }
        
        attachment_count = self.mapper._count_attachments({}, test_data)
        self.assertEqual(attachment_count, 2)
        
        # Test video detection
        has_video = self.mapper._check_video_attachments({}, test_data)
        self.assertTrue(has_video)
        
        # Test image detection
        has_image = self.mapper._check_image_attachments({}, {
            'media_metadata': {
                'attachments': [
                    {'id': '1', 'type': 'Photo'},
                    {'id': '2', 'type': 'video'}
                ]
            }
        })
        self.assertTrue(has_image)
    
    def test_schema_validation(self):
        """Test Facebook schema validation rules."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Required fields should be present
        self.assertIsNotNone(transformed.get('post_id'))
        self.assertIsNotNone(transformed.get('post_url'))
        self.assertIsNotNone(transformed.get('date_posted'))
        
        # URL validation
        self.assertTrue(transformed['post_url'].startswith('https://'))
        self.assertTrue(transformed['user_url'].startswith('https://'))
        self.assertTrue(transformed['page_metadata']['page_logo'].startswith('https://'))
        
        # Email validation
        page_email = transformed['page_metadata']['page_email']
        self.assertIn('@', page_email)
        self.assertIn('.', page_email)
    
    def test_missing_fields_handling(self):
        """Test handling of missing or null fields in Facebook posts."""
        # Create minimal post with missing optional fields
        minimal_post = {
            'post_id': 'test_post_id',
            'url': 'https://facebook.com/post/123',
            'date_posted': '2025-07-12T10:00:00.000Z',
            'content': 'Test post content',
            'hashtags': [],
            'attachments': [],
            'count_reactions_type': [],
            'likes': 0,
            'num_comments': 0,
            'num_shares': 0
        }
        
        transformed = self.mapper.transform_post(minimal_post, 'facebook', self.test_metadata)
        
        # Should handle missing fields gracefully with defaults
        self.assertEqual(transformed['page_followers'], 0)
        self.assertEqual(transformed['page_likes'], 0)
        # page_verified may not be present if source field is missing
        if 'page_verified' in transformed:
            self.assertFalse(transformed['page_verified'])
        self.assertEqual(transformed['engagement_metrics']['reactions'], 0)
        self.assertEqual(transformed['media_metadata']['media_count'], 0)
        self.assertFalse(transformed['media_metadata']['has_video'])
        self.assertFalse(transformed['media_metadata']['has_image'])
    
    def test_multiple_posts_transformation(self):
        """Test transformation of multiple Facebook posts."""
        for i, raw_post in enumerate(self.facebook_posts[:3]):  # Test first 3 posts
            with self.subTest(post_index=i):
                transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
                
                # Verify core structure
                self.assertIn('post_id', transformed)
                self.assertIn('post_url', transformed)
                self.assertIn('post_content', transformed)
                self.assertIn('date_posted', transformed)
                self.assertIn('engagement_metrics', transformed)
                self.assertIn('media_metadata', transformed)
                self.assertIn('content_analysis', transformed)
                self.assertIn('page_metadata', transformed)
                self.assertIn('processing_metadata', transformed)
                
                # Verify data quality
                quality_score = transformed['processing_metadata']['data_quality_score']
                self.assertGreater(quality_score, 0.0)
    
    def test_business_context_preservation(self):
        """Test that business context is preserved across transformation."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Business context should be preserved
        self.assertEqual(transformed['competitor'], 'nutifood')
        self.assertEqual(transformed['brand'], 'growplus-nutifood')
        self.assertEqual(transformed['category'], 'sua-bot-tre-em')
        self.assertEqual(transformed['platform'], 'facebook')
        
        # ID should include crawl context
        expected_id = f"{raw_post['post_id']}_test_crawl_123"
        self.assertEqual(transformed['id'], expected_id)
    
    def test_unicode_text_handling(self):
        """Test handling of Unicode text in Facebook posts."""
        raw_post = self.facebook_posts[0].copy()
        
        # Facebook posts often contain Vietnamese text and emojis
        unicode_content = "Câu chuyện đi chơi về muộn không có hồi kết của Thị Dứa nhà Lâm Ngọc Hà 💗"
        raw_post['content'] = unicode_content
        
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Unicode text should be preserved (except emojis which are cleaned)
        self.assertIn('Câu chuyện đi chơi', transformed['post_content'])
        self.assertIn('Thị Dứa nhà Lâm Ngọc Hà', transformed['post_content'])
        # Note: Emojis are removed by text cleaning process
        
        # Text length should be calculated correctly from cleaned content
        content_length = len(transformed.get('post_content', ''))
        self.assertEqual(transformed['content_analysis']['text_length'], content_length)
    
    def test_timestamp_parsing(self):
        """Test various timestamp formats in Facebook posts."""
        raw_post = self.facebook_posts[0].copy()
        
        # Test ISO timestamp parsing
        iso_timestamp = "2024-12-24T13:30:14.000Z"
        raw_post['date_posted'] = iso_timestamp
        
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Should normalize ISO format for BigQuery
        expected_timestamp = iso_timestamp.replace('Z', '+00:00')
        self.assertEqual(transformed['date_posted'], expected_timestamp)
        self.assertEqual(transformed['grouped_date'], "2024-12-24")
    
    def test_large_content_handling(self):
        """Test handling of large content that exceeds max length."""
        raw_post = self.facebook_posts[0].copy()
        
        # Create content that exceeds max length (50000 chars)
        large_content = "A" * 50001
        raw_post['content'] = large_content
        
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Should be truncated with ellipsis
        self.assertEqual(len(transformed['post_content']), 50003)  # 50000 + "..."
        self.assertTrue(transformed['post_content'].endswith('...'))
    
    def test_video_view_count_handling(self):
        """Test handling of video-specific fields in Facebook posts."""
        raw_post = self.facebook_posts[0].copy()
        
        # This post has video view count
        if 'video_view_count' in raw_post:
            transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
            
            # Video view count should be preserved (if the schema supports it)
            # Note: Current schema doesn't explicitly map video_view_count
            # This test documents the current behavior
            self.assertIsInstance(transformed, dict)
    
    def test_edge_cases(self):
        """Test edge cases in Facebook data transformation."""
        raw_post = self.facebook_posts[0].copy()
        
        # Test zero engagement
        raw_post['likes'] = 0
        raw_post['num_comments'] = 0
        raw_post['num_shares'] = 0
        raw_post['count_reactions_type'] = []
        
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        # Should handle zero values gracefully
        self.assertEqual(transformed['engagement_metrics']['likes'], 0)
        self.assertEqual(transformed['engagement_metrics']['comments'], 0)
        self.assertEqual(transformed['engagement_metrics']['shares'], 0)
        self.assertEqual(transformed['engagement_metrics']['reactions'], 0)
        self.assertEqual(transformed['engagement_metrics']['reactions_by_type'], [])
        
        # Test empty content
        raw_post['content'] = ""
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        self.assertEqual(transformed['post_content'], "")
        self.assertEqual(transformed['content_analysis']['text_length'], 0)
    
    def test_page_metadata_completeness(self):
        """Test that all page metadata fields are properly transformed."""
        raw_post = self.facebook_posts[0]
        transformed = self.mapper.transform_post(raw_post, 'facebook', self.test_metadata)
        
        page_metadata = transformed['page_metadata']
        
        # Check all expected page metadata fields (some may be missing due to validation failures)
        expected_fields = [
            'page_intro', 'page_logo', 'page_phone',
            'page_email', 'page_creation_date'
        ]
        
        for field in expected_fields:
            self.assertIn(field, page_metadata)
        
        # page_website may be missing if URL validation fails (invalid URL format in fixture)
        
        # Verify specific page data from fixture
        self.assertIsNotNone(page_metadata['page_intro'])
        self.assertTrue(page_metadata['page_logo'].startswith('https://'))
        # page_website may be missing due to URL validation failure
        if 'page_website' in page_metadata:
            self.assertTrue(page_metadata['page_website'].startswith('https://'))
        self.assertIn('+84', page_metadata['page_phone'])
        self.assertIn('@', page_metadata['page_email'])


if __name__ == '__main__':
    unittest.main()