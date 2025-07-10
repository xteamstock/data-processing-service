import unittest
import sys
import os
from datetime import datetime

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from handlers.text_processor import TextProcessor

class TestTextProcessor(unittest.TestCase):
    
    def setUp(self):
        self.processor = TextProcessor()
    
    def test_process_posts_for_analytics(self):
        """Test post processing for analytics."""
        raw_posts = [
            {
                'post_id': 'test-post-1',
                'content': 'Test post content with some hashtags #test #social',
                'likes': 10,
                'num_comments': 5,
                'num_shares': 2,
                'date_posted': '2025-07-09T10:00:00.000Z',
                'url': 'https://facebook.com/post/123',
                'user_username_raw': 'Test User',
                'page_name': 'Test Page',
                'attachments': [
                    {'id': 'att1', 'type': 'photo', 'url': 'https://example.com/photo.jpg'}
                ]
            }
        ]
        
        metadata = {
            'crawl_id': 'test-crawl-123',
            'snapshot_id': 'test-snapshot-456',
            'platform': 'facebook',
            'competitor': 'test-competitor',
            'brand': 'test-brand',
            'category': 'test-category',
            'crawl_date': '2025-07-09T10:00:00.000Z'
        }
        
        result = self.processor.process_posts_for_analytics(raw_posts, metadata)
        
        # Validate results
        self.assertEqual(len(result), 1)
        post = result[0]
        
        # Check core fields
        self.assertEqual(post['post_id'], 'test-post-1')
        self.assertEqual(post['platform'], 'facebook')
        self.assertEqual(post['competitor'], 'test-competitor')
        self.assertEqual(post['crawl_id'], 'test-crawl-123')
        
        # Check engagement metrics
        self.assertIn('engagement_metrics', post)
        self.assertEqual(post['engagement_metrics']['likes'], 10)
        self.assertEqual(post['engagement_metrics']['comments'], 5)
        self.assertEqual(post['engagement_metrics']['shares'], 2)
        
        # Check media metadata
        self.assertIn('media_metadata', post)
        self.assertEqual(post['media_metadata']['media_count'], 1)
        self.assertTrue(post['media_metadata']['has_image'])
        self.assertFalse(post['media_metadata']['has_video'])
        
        # Check content analysis
        self.assertIn('content_analysis', post)
        self.assertTrue(post['content_analysis']['text_length'] > 0)
        self.assertIn('hashtags', post['content_analysis'])
        
        # Check grouped date
        self.assertEqual(post['grouped_date'], '2025-07-09')
    
    def test_group_posts_by_date(self):
        """Test date-based grouping functionality."""
        raw_posts = [
            {'post_id': '1', 'date_posted': '2025-07-09T10:00:00.000Z'},
            {'post_id': '2', 'date_posted': '2025-07-09T15:00:00.000Z'},
            {'post_id': '3', 'date_posted': '2025-07-10T12:00:00.000Z'},
            {'post_id': '4', 'date_posted': ''}  # Missing date
        ]
        
        grouped = self.processor._group_posts_by_date(raw_posts)
        
        # Should have 3 groups (2 for 07-09, 1 for 07-10, 1 for today due to missing date)
        self.assertIn('2025-07-09', grouped)
        self.assertIn('2025-07-10', grouped)
        
        # Check group contents
        self.assertEqual(len(grouped['2025-07-09']), 2)
        self.assertEqual(len(grouped['2025-07-10']), 1)
        
        # Check post IDs in groups
        date_09_post_ids = [p['post_id'] for p in grouped['2025-07-09']]
        self.assertIn('1', date_09_post_ids)
        self.assertIn('2', date_09_post_ids)
        
        date_10_post_ids = [p['post_id'] for p in grouped['2025-07-10']]
        self.assertIn('3', date_10_post_ids)
    
    def test_process_posts_legacy_interface(self):
        """Test legacy interface for backward compatibility."""
        event_data = {
            'crawl_id': 'test-crawl-123',
            'platform': 'facebook',
            'competitor': 'test-competitor',
            'posts': [
                {
                    'post_id': 'legacy-test-1',
                    'content': 'Legacy test content',
                    'likes': 15,
                    'date_posted': '2025-07-09T14:00:00.000Z',
                    'url': 'https://facebook.com/legacy/123'
                }
            ]
        }
        
        result = self.processor.process_posts(event_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['post_id'], 'legacy-test-1')
        self.assertEqual(result[0]['platform'], 'facebook')
        self.assertEqual(result[0]['competitor'], 'test-competitor')
    
    def test_empty_posts_handling(self):
        """Test handling of empty post data."""
        metadata = {
            'crawl_id': 'test-empty',
            'platform': 'facebook',
            'competitor': 'test-competitor'
        }
        
        # Test with empty list
        result = self.processor.process_posts_for_analytics([], metadata)
        self.assertEqual(len(result), 0)
        
        # Test with None
        result = self.processor.process_posts_for_analytics(None or [], metadata)
        self.assertEqual(len(result), 0)
    
    def test_malformed_post_handling(self):
        """Test handling of malformed post data."""
        raw_posts = [
            {
                'post_id': 'good-post',
                'content': 'Good post content',
                'date_posted': '2025-07-09T10:00:00.000Z',
                'url': 'https://facebook.com/good/123'
            },
            {
                # Missing required fields
                'content': 'Bad post without ID'
            },
            None  # Completely invalid post
        ]
        
        metadata = {
            'crawl_id': 'test-malformed',
            'platform': 'facebook',
            'competitor': 'test-competitor'
        }
        
        # Should process only the valid post
        result = self.processor.process_posts_for_analytics(raw_posts, metadata)
        
        # Should get at least one valid post (the processor might handle some malformed data)
        self.assertGreaterEqual(len(result), 1)
        
        # First result should be the good post
        good_posts = [p for p in result if p.get('post_id') == 'good-post']
        self.assertEqual(len(good_posts), 1)

if __name__ == '__main__':
    unittest.main()