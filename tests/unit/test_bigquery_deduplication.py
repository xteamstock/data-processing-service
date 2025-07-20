# tests/unit/test_bigquery_deduplication.py
# Unit tests for BigQuery deduplication functionality

import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest
from handlers.bigquery_handler import BigQueryHandler
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


class TestBigQueryDeduplication(unittest.TestCase):
    """Test suite for BigQuery deduplication functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        with patch('handlers.bigquery_handler.bigquery.Client'):
            self.handler = BigQueryHandler()
            # Enable deduplication for testing
            self.handler.deduplication_enabled = True
            self.handler.deduplication_batch_size = 1000
            self.handler.deduplication_fallback_on_error = True
    
    def test_extract_post_ids_facebook(self):
        """Test _extract_post_ids with Facebook posts."""
        posts = [
            {'post_id': '123', 'platform': 'facebook'},
            {'post_id': '456', 'platform': 'facebook'},
            {'post_id': '', 'platform': 'facebook'},  # Empty ID
            {'platform': 'facebook'}  # Missing post_id
        ]
        
        result = self.handler._extract_post_ids(posts)
        
        self.assertEqual(result, ['123', '456'])
    
    def test_extract_post_ids_tiktok(self):
        """Test _extract_post_ids with TikTok posts."""
        posts = [
            {'video_id': 'video123', 'platform': 'tiktok'},
            {'video_id': 'video456', 'platform': 'tiktok'},
            {'video_id': None, 'platform': 'tiktok'},  # None ID
            {'platform': 'tiktok'}  # Missing video_id
        ]
        
        result = self.handler._extract_post_ids(posts)
        
        self.assertEqual(result, ['video123', 'video456'])
    
    def test_extract_post_ids_youtube(self):
        """Test _extract_post_ids with YouTube posts."""
        posts = [
            {'video_id': 'yt123', 'platform': 'youtube'},
            {'video_id': 'yt456', 'platform': 'youtube'},
        ]
        
        result = self.handler._extract_post_ids(posts)
        
        self.assertEqual(result, ['yt123', 'yt456'])
    
    def test_extract_post_ids_mixed_platforms(self):
        """Test _extract_post_ids with mixed platform posts."""
        posts = [
            {'post_id': '123', 'platform': 'facebook'},
            {'video_id': 'video456', 'platform': 'tiktok'},
            {'video_id': 'yt789', 'platform': 'youtube'},
        ]
        
        result = self.handler._extract_post_ids(posts)
        
        self.assertEqual(result, ['123', 'video456', 'yt789'])
    
    def test_extract_post_ids_empty_list(self):
        """Test _extract_post_ids with empty posts list."""
        result = self.handler._extract_post_ids([])
        
        self.assertEqual(result, [])
    
    @patch('handlers.bigquery_handler.bigquery.Client')
    def test_get_existing_post_ids_success(self, mock_client_class):
        """Test _get_existing_post_ids with successful query."""
        # Mock the client and query results
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        mock_row1 = Mock()
        mock_row1.post_id = '123'
        mock_row2 = Mock()
        mock_row2.post_id = '456'
        
        mock_query_job = Mock()
        mock_query_job.result.return_value = [mock_row1, mock_row2]
        mock_client.query.return_value = mock_query_job
        
        # Set up handler with mocked client
        handler = BigQueryHandler()
        handler.client = mock_client
        
        post_ids = ['123', '456', '789']
        table_id = 'test_project.test_dataset.test_table'
        
        result = handler._get_existing_post_ids(post_ids, table_id)
        
        self.assertEqual(result, {'123', '456'})
        
        # Verify query was called with correct parameters
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        self.assertIn('UNNEST(@post_ids)', call_args[0][0])
    
    @patch('handlers.bigquery_handler.bigquery.Client')
    def test_get_existing_post_ids_empty_input(self, mock_client_class):
        """Test _get_existing_post_ids with empty post_ids."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        handler = BigQueryHandler()
        handler.client = mock_client
        
        result = handler._get_existing_post_ids([], 'test_table')
        
        self.assertEqual(result, set())
        # Verify query was not called
        mock_client.query.assert_not_called()
    
    @patch('handlers.bigquery_handler.bigquery.Client')
    def test_get_existing_post_ids_error_with_fallback(self, mock_client_class):
        """Test _get_existing_post_ids with error and fallback enabled."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.query.side_effect = GoogleCloudError("BigQuery error")
        
        handler = BigQueryHandler()
        handler.client = mock_client
        handler.deduplication_fallback_on_error = True
        
        post_ids = ['123', '456']
        table_id = 'test_project.test_dataset.test_table'
        
        result = handler._get_existing_post_ids(post_ids, table_id)
        
        self.assertEqual(result, set())
    
    @patch('handlers.bigquery_handler.bigquery.Client')
    def test_get_existing_post_ids_error_without_fallback(self, mock_client_class):
        """Test _get_existing_post_ids with error and fallback disabled."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.query.side_effect = GoogleCloudError("BigQuery error")
        
        handler = BigQueryHandler()
        handler.client = mock_client
        handler.deduplication_fallback_on_error = False
        
        post_ids = ['123', '456']
        table_id = 'test_project.test_dataset.test_table'
        
        with self.assertRaises(GoogleCloudError):
            handler._get_existing_post_ids(post_ids, table_id)
    
    @patch.object(BigQueryHandler, '_get_existing_post_ids')
    @patch.object(BigQueryHandler, '_get_platform_table')
    def test_filter_duplicates_with_duplicates(self, mock_get_table, mock_get_existing):
        """Test _filter_duplicates with some duplicates found."""
        mock_get_table.return_value = 'test_table'
        mock_get_existing.return_value = {'123', '456'}
        
        posts = [
            {'post_id': '123', 'platform': 'facebook'},  # Duplicate
            {'post_id': '456', 'platform': 'facebook'},  # Duplicate
            {'post_id': '789', 'platform': 'facebook'},  # New
        ]
        
        result = self.handler._filter_duplicates(posts, 'facebook')
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['post_id'], '789')
    
    @patch.object(BigQueryHandler, '_get_existing_post_ids')
    @patch.object(BigQueryHandler, '_get_platform_table')
    def test_filter_duplicates_no_duplicates(self, mock_get_table, mock_get_existing):
        """Test _filter_duplicates with no duplicates found."""
        mock_get_table.return_value = 'test_table'
        mock_get_existing.return_value = set()
        
        posts = [
            {'post_id': '123', 'platform': 'facebook'},
            {'post_id': '456', 'platform': 'facebook'},
        ]
        
        result = self.handler._filter_duplicates(posts, 'facebook')
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result, posts)
    
    @patch.object(BigQueryHandler, '_get_existing_post_ids')
    @patch.object(BigQueryHandler, '_get_platform_table')
    def test_filter_duplicates_empty_posts(self, mock_get_table, mock_get_existing):
        """Test _filter_duplicates with empty posts list."""
        result = self.handler._filter_duplicates([], 'facebook')
        
        self.assertEqual(result, [])
        # Verify methods were not called
        mock_get_table.assert_not_called()
        mock_get_existing.assert_not_called()
    
    @patch.object(BigQueryHandler, '_get_existing_post_ids')
    @patch.object(BigQueryHandler, '_get_platform_table')
    def test_filter_duplicates_no_post_ids(self, mock_get_table, mock_get_existing):
        """Test _filter_duplicates with posts missing post_id fields."""
        posts = [
            {'platform': 'facebook'},  # Missing post_id
            {'post_id': '', 'platform': 'facebook'},  # Empty post_id
        ]
        
        result = self.handler._filter_duplicates(posts, 'facebook')
        
        self.assertEqual(result, posts)
        # Verify BigQuery query was not called
        mock_get_existing.assert_not_called()
    
    @patch.object(BigQueryHandler, '_filter_duplicates')
    def test_filter_duplicates_batched_small_dataset(self, mock_filter):
        """Test _filter_duplicates_batched with small dataset."""
        self.handler.deduplication_batch_size = 1000
        posts = [{'post_id': '123'}, {'post_id': '456'}]
        
        mock_filter.return_value = posts
        
        result = self.handler._filter_duplicates_batched(posts, 'facebook')
        
        # Should call _filter_duplicates directly for small datasets
        mock_filter.assert_called_once_with(posts, 'facebook')
        self.assertEqual(result, posts)
    
    @patch.object(BigQueryHandler, '_filter_duplicates')
    def test_filter_duplicates_batched_large_dataset(self, mock_filter):
        """Test _filter_duplicates_batched with large dataset."""
        self.handler.deduplication_batch_size = 2
        posts = [
            {'post_id': '1'}, {'post_id': '2'}, 
            {'post_id': '3'}, {'post_id': '4'}
        ]
        
        # Mock filter to return input unchanged
        mock_filter.side_effect = lambda x, y: x
        
        result = self.handler._filter_duplicates_batched(posts, 'facebook')
        
        # Should call _filter_duplicates twice (2 batches)
        self.assertEqual(mock_filter.call_count, 2)
        self.assertEqual(result, posts)
    
    @patch.object(BigQueryHandler, '_filter_duplicates_batched')
    def test_insert_posts_with_deduplication_enabled(self, mock_filter_batched):
        """Test insert_posts with deduplication enabled."""
        self.handler.deduplication_enabled = True
        
        posts = [{'post_id': '123'}, {'post_id': '456'}]
        filtered_posts = [{'post_id': '456'}]  # One duplicate filtered out
        
        mock_filter_batched.return_value = filtered_posts
        
        with patch.object(self.handler, 'client') as mock_client:
            mock_client.insert_rows_json.return_value = []  # No errors
            
            result = self.handler.insert_posts(posts, platform='facebook')
            
            # Verify deduplication was called
            mock_filter_batched.assert_called_once_with(posts, 'facebook')
            # Verify insertion was called with filtered posts
            mock_client.insert_rows_json.assert_called_once()
            self.assertEqual(result['rows_inserted'], 1)
    
    @patch.object(BigQueryHandler, '_filter_duplicates_batched')
    def test_insert_posts_with_deduplication_disabled(self, mock_filter_batched):
        """Test insert_posts with deduplication disabled."""
        self.handler.deduplication_enabled = False
        
        posts = [{'post_id': '123'}, {'post_id': '456'}]
        
        with patch.object(self.handler, 'client') as mock_client:
            mock_client.insert_rows_json.return_value = []  # No errors
            
            result = self.handler.insert_posts(posts, platform='facebook')
            
            # Verify deduplication was not called
            mock_filter_batched.assert_not_called()
            # Verify insertion was called with original posts
            mock_client.insert_rows_json.assert_called_once()
            self.assertEqual(result['rows_inserted'], 2)
    
    @patch.object(BigQueryHandler, '_filter_duplicates_batched')
    def test_insert_posts_all_duplicates(self, mock_filter_batched):
        """Test insert_posts when all posts are duplicates."""
        self.handler.deduplication_enabled = True
        
        posts = [{'post_id': '123'}, {'post_id': '456'}]
        mock_filter_batched.return_value = []  # All filtered out
        
        with patch.object(self.handler, 'client') as mock_client:
            result = self.handler.insert_posts(posts, platform='facebook')
            
            # Verify deduplication was called
            mock_filter_batched.assert_called_once_with(posts, 'facebook')
            # Verify insertion was not called
            mock_client.insert_rows_json.assert_not_called()
            self.assertEqual(result['rows_inserted'], 0)
            self.assertTrue(result['success'])


if __name__ == '__main__':
    unittest.main()