"""
Unit tests for MediaTrackingHandler.

Tests cover:
- Basic CRUD operations
- Error handling and edge cases
- Query and analytics functionality
- BigQuery integration with mocks
"""

import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from google.cloud.exceptions import NotFound

# Add the parent directory to the path to import the handler
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from handlers.media_tracking_handler import MediaTrackingHandler, MediaTrackingError

class TestMediaTrackingHandler:
    """Test suite for MediaTrackingHandler."""

    @pytest.fixture
    def mock_bigquery_client(self):
        """Mock BigQuery client."""
        with patch('handlers.media_tracking_handler.bigquery.Client') as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def handler(self, mock_bigquery_client):
        """Create MediaTrackingHandler instance with mocked BigQuery client."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'BIGQUERY_DATASET': 'test_dataset',
            'MEDIA_TRACKING_ENABLED': 'true'
        }):
            handler = MediaTrackingHandler()
            handler.client = mock_bigquery_client
            return handler

    @pytest.fixture
    def sample_media_items(self):
        """Sample media items for testing."""
        return [
            {
                'crawl_id': 'crawl_123',
                'post_id': 'post_456',
                'url': 'https://example.com/image1.jpg',
                'type': 'image',
                'platform': 'facebook',
                'competitor': 'test_competitor',
                'brand': 'test_brand',
                'category': 'test_category',
                'id': 'img_123',
                'attachment_url': 'https://facebook.com/attachment/123',
                'content_type': 'image/jpeg',
                'platform_metadata': {'width': 1920, 'height': 1080}
            },
            {
                'crawl_id': 'crawl_123',
                'post_id': 'post_789',
                'url': 'https://example.com/video1.mp4',
                'type': 'video',
                'platform': 'facebook',
                'competitor': 'test_competitor',
                'brand': 'test_brand',
                'category': 'test_category',
                'id': 'vid_456',
                'attachment_url': 'https://facebook.com/attachment/456',
                'content_type': 'video/mp4',
                'platform_metadata': {'duration': 30, 'quality': 'HD'}
            }
        ]

    def test_init_successful(self, mock_bigquery_client):
        """Test successful initialization of MediaTrackingHandler."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'BIGQUERY_DATASET': 'test_dataset',
            'MEDIA_TRACKING_ENABLED': 'true'
        }):
            # Mock table exists
            mock_bigquery_client.get_table.return_value = Mock()
            
            handler = MediaTrackingHandler()
            
            assert handler.project_id == 'test-project'
            assert handler.dataset_id == 'test_dataset'
            assert handler.table_name == 'media_tracking'
            assert handler.tracking_enabled == True
            assert handler.batch_size == 100
            assert handler.stall_threshold_minutes == 30

    def test_init_table_not_found(self, mock_bigquery_client):
        """Test initialization when table doesn't exist."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'BIGQUERY_DATASET': 'test_dataset'
        }):
            # Mock table not found
            mock_bigquery_client.get_table.side_effect = NotFound('Table not found')
            
            with pytest.raises(NotFound):
                MediaTrackingHandler()

    def test_generate_media_id(self, handler):
        """Test media ID generation logic."""
        media_id = handler._generate_media_id(
            crawl_id='crawl_123',
            post_id='post_456',
            media_type='image',
            media_url='https://example.com/image1.jpg'
        )
        
        assert media_id.startswith('crawl_123_post_456_image_')
        assert len(media_id.split('_')) == 4
        assert len(media_id.split('_')[3]) == 8  # Hash should be 8 characters

    def test_insert_detected_media_success(self, handler, sample_media_items):
        """Test successful media insertion."""
        # Mock successful BigQuery insertion
        handler.client.insert_rows_json.return_value = []  # No errors
        
        result = handler.insert_detected_media(sample_media_items)
        
        assert result['success'] == True
        assert result['rows_inserted'] == 2
        assert 'table_id' in result
        
        # Verify BigQuery insert was called
        handler.client.insert_rows_json.assert_called_once()
        
        # Verify the tracking records structure
        call_args = handler.client.insert_rows_json.call_args
        tracking_records = call_args[0][1]  # Second argument is the records
        
        assert len(tracking_records) == 2
        assert tracking_records[0]['status'] == 'detected'
        assert tracking_records[0]['media_type'] == 'image'
        assert tracking_records[1]['media_type'] == 'video'

    def test_insert_detected_media_disabled(self, handler, sample_media_items):
        """Test media insertion when tracking is disabled."""
        handler.tracking_enabled = False
        
        result = handler.insert_detected_media(sample_media_items)
        
        assert result['success'] == True
        assert result['rows_inserted'] == 0
        assert result['tracking_disabled'] == True
        
        # Verify BigQuery was not called
        handler.client.insert_rows_json.assert_not_called()

    def test_insert_detected_media_empty_list(self, handler):
        """Test media insertion with empty list."""
        result = handler.insert_detected_media([])
        
        assert result['success'] == True
        assert result['rows_inserted'] == 0
        
        # Verify BigQuery was not called
        handler.client.insert_rows_json.assert_not_called()

    def test_insert_detected_media_bigquery_error(self, handler, sample_media_items):
        """Test media insertion with BigQuery errors."""
        # Mock BigQuery insertion error
        handler.client.insert_rows_json.return_value = [
            {'index': 0, 'errors': [{'reason': 'invalid', 'message': 'Invalid data'}]}
        ]
        
        result = handler.insert_detected_media(sample_media_items)
        
        assert result['success'] == False
        assert result['rows_inserted'] == 0
        assert 'error' in result

    def test_insert_detected_media_missing_fields(self, handler):
        """Test media insertion with missing required fields."""
        incomplete_media = [
            {
                'crawl_id': 'crawl_123',
                # Missing post_id, url, platform, competitor
                'type': 'image',
            }
        ]
        
        result = handler.insert_detected_media(incomplete_media)
        
        assert result['success'] == True
        assert result['rows_inserted'] == 0  # No valid items to insert

    def test_insert_detected_media_batch_processing(self, handler, sample_media_items):
        """Test batch processing of media items."""
        # Set small batch size
        handler.batch_size = 1
        
        # Mock successful BigQuery insertion
        handler.client.insert_rows_json.return_value = []
        
        result = handler.insert_detected_media(sample_media_items)
        
        assert result['success'] == True
        assert result['rows_inserted'] == 2
        
        # Verify BigQuery was called twice (once per batch)
        assert handler.client.insert_rows_json.call_count == 2

    def test_update_media_status_success(self, handler):
        """Test successful media status update."""
        # Mock successful query execution
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        handler.client.query.return_value = mock_query_job
        
        result = handler.update_media_status(
            media_id='test_media_id',
            status='processing',
            processing_metadata={'file_size': 1024}
        )
        
        assert result == True
        
        # Verify query was called
        handler.client.query.assert_called_once()
        
        # Verify query contains correct fields
        query_call = handler.client.query.call_args[0][0]
        assert 'status = \'processing\'' in query_call
        assert 'processing_start_timestamp' in query_call
        assert 'media_id = \'test_media_id\'' in query_call

    def test_update_media_status_disabled(self, handler):
        """Test media status update when tracking is disabled."""
        handler.tracking_enabled = False
        
        result = handler.update_media_status('test_media_id', 'processing')
        
        assert result == True
        
        # Verify query was not called
        handler.client.query.assert_not_called()

    def test_update_media_status_failed_with_error(self, handler):
        """Test media status update for failed status with error message."""
        # Mock successful query execution
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        handler.client.query.return_value = mock_query_job
        
        result = handler.update_media_status(
            media_id='test_media_id',
            status='failed',
            error_message='Download failed'
        )
        
        assert result == True
        
        # Verify query contains error handling
        query_call = handler.client.query.call_args[0][0]
        assert 'status = \'failed\'' in query_call
        assert 'retry_count = COALESCE(retry_count, 0) + 1' in query_call
        assert 'error_message = \'Download failed\'' in query_call

    def test_update_media_status_completed(self, handler):
        """Test media status update for completed status."""
        # Mock successful query execution
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        handler.client.query.return_value = mock_query_job
        
        result = handler.update_media_status(
            media_id='test_media_id',
            status='completed',
            processing_metadata={'file_size': 2048, 'duration': 30}
        )
        
        assert result == True
        
        # Verify query contains completion fields
        query_call = handler.client.query.call_args[0][0]
        assert 'status = \'completed\'' in query_call
        assert 'processing_end_timestamp' in query_call

    def test_update_media_status_query_error(self, handler):
        """Test media status update with query error."""
        # Mock query execution error
        handler.client.query.side_effect = Exception('Query failed')
        
        result = handler.update_media_status('test_media_id', 'processing')
        
        assert result == False

    def test_get_processing_statistics_success(self, handler):
        """Test successful retrieval of processing statistics."""
        # Mock query results
        mock_row_1 = Mock()
        mock_row_1.platform = 'facebook'
        mock_row_1.status = 'completed'
        mock_row_1.count = 50
        mock_row_1.avg_processing_duration_seconds = 120.5
        mock_row_1.retry_count = 5
        
        mock_row_2 = Mock()
        mock_row_2.platform = 'facebook'
        mock_row_2.status = 'processing'
        mock_row_2.count = 10
        mock_row_2.avg_processing_duration_seconds = None
        mock_row_2.retry_count = 0
        
        mock_query_job = Mock()
        mock_query_job.result.return_value = [mock_row_1, mock_row_2]
        handler.client.query.return_value = mock_query_job
        
        result = handler.get_processing_statistics()
        
        assert result['total_media'] == 60
        assert 'facebook' in result['by_platform']
        assert result['by_platform']['facebook']['completed']['count'] == 50
        assert result['by_platform']['facebook']['processing']['count'] == 10
        assert result['by_status']['completed'] == 50
        assert result['by_status']['processing'] == 10
        assert result['overall_stats']['total_retries'] == 5

    def test_get_processing_statistics_with_filters(self, handler):
        """Test processing statistics with platform and competitor filters."""
        # Mock query results
        mock_query_job = Mock()
        mock_query_job.result.return_value = []
        handler.client.query.return_value = mock_query_job
        
        handler.get_processing_statistics(
            platform='facebook',
            competitor='test_competitor',
            hours_back=12
        )
        
        # Verify query was called with correct filters
        query_call = handler.client.query.call_args[0][0]
        assert 'platform = \'facebook\'' in query_call
        assert 'competitor = \'test_competitor\'' in query_call
        assert 'INTERVAL 12 HOUR' in query_call

    def test_get_processing_statistics_error(self, handler):
        """Test processing statistics with query error."""
        # Mock query error
        handler.client.query.side_effect = Exception('Query failed')
        
        result = handler.get_processing_statistics()
        
        assert 'error' in result
        assert result['error'] == 'Query failed'

    def test_get_stalled_media_success(self, handler):
        """Test successful retrieval of stalled media."""
        # Mock query results
        mock_row = Mock()
        mock_row.media_id = 'stalled_media_123'
        mock_row.media_url = 'https://example.com/stalled.jpg'
        mock_row.post_id = 'post_123'
        mock_row.crawl_id = 'crawl_123'
        mock_row.platform = 'facebook'
        mock_row.competitor = 'test_competitor'
        mock_row.status = 'processing'
        mock_row.processing_start_timestamp = datetime.utcnow() - timedelta(hours=1)
        mock_row.stalled_minutes = 60
        mock_row.retry_count = 2
        mock_row.error_message = 'Timeout'
        
        mock_query_job = Mock()
        mock_query_job.result.return_value = [mock_row]
        handler.client.query.return_value = mock_query_job
        
        result = handler.get_stalled_media()
        
        assert len(result) == 1
        assert result[0]['media_id'] == 'stalled_media_123'
        assert result[0]['stalled_minutes'] == 60
        assert result[0]['retry_count'] == 2

    def test_get_stalled_media_custom_threshold(self, handler):
        """Test stalled media with custom threshold."""
        # Mock query results
        mock_query_job = Mock()
        mock_query_job.result.return_value = []
        handler.client.query.return_value = mock_query_job
        
        handler.get_stalled_media(threshold_minutes=60)
        
        # Verify query was called with custom threshold
        query_call = handler.client.query.call_args[0][0]
        assert '>= 60' in query_call

    def test_get_stalled_media_error(self, handler):
        """Test stalled media with query error."""
        # Mock query error
        handler.client.query.side_effect = Exception('Query failed')
        
        result = handler.get_stalled_media()
        
        assert result == []

    def test_get_media_by_crawl_id_success(self, handler):
        """Test successful retrieval of media by crawl ID."""
        # Mock query results
        mock_row = Mock()
        mock_row.__dict__ = {
            'media_id': 'media_123',
            'crawl_id': 'crawl_123',
            'status': 'completed'
        }
        
        mock_query_job = Mock()
        mock_query_job.result.return_value = [mock_row]
        handler.client.query.return_value = mock_query_job
        
        result = handler.get_media_by_crawl_id('crawl_123')
        
        assert len(result) == 1
        assert result[0]['media_id'] == 'media_123'
        assert result[0]['crawl_id'] == 'crawl_123'

    def test_get_media_by_crawl_id_error(self, handler):
        """Test media by crawl ID with query error."""
        # Mock query error
        handler.client.query.side_effect = Exception('Query failed')
        
        result = handler.get_media_by_crawl_id('crawl_123')
        
        assert result == []

    def test_media_tracking_error_exception(self):
        """Test MediaTrackingError exception."""
        error = MediaTrackingError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_environment_variable_defaults(self, mock_bigquery_client):
        """Test default environment variable values."""
        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            # Set required variables
            os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project'
            
            # Mock table exists
            mock_bigquery_client.get_table.return_value = Mock()
            
            handler = MediaTrackingHandler()
            
            assert handler.project_id == 'test-project'
            assert handler.dataset_id == 'social_analytics'
            assert handler.tracking_enabled == True
            assert handler.batch_size == 100
            assert handler.stall_threshold_minutes == 30

    def test_environment_variable_overrides(self, mock_bigquery_client):
        """Test environment variable overrides."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'custom-project',
            'BIGQUERY_DATASET': 'custom_dataset',
            'MEDIA_TRACKING_ENABLED': 'false',
            'MEDIA_TRACKING_BATCH_SIZE': '50',
            'MEDIA_STALL_THRESHOLD_MINUTES': '45'
        }):
            # Mock table exists
            mock_bigquery_client.get_table.return_value = Mock()
            
            handler = MediaTrackingHandler()
            
            assert handler.project_id == 'custom-project'
            assert handler.dataset_id == 'custom_dataset'
            assert handler.tracking_enabled == False
            assert handler.batch_size == 50
            assert handler.stall_threshold_minutes == 45

if __name__ == '__main__':
    pytest.main([__file__, '-v'])