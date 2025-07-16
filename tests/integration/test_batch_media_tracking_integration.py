"""
Integration tests for batch media publisher with media tracking.

Tests cover:
- Complete flow from media detection to tracking record insertion
- Integration with different platforms (Facebook, TikTok, YouTube)
- Error scenarios and graceful degradation
- Tracking with different media types and configurations
"""

import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add the parent directory to the path to import modules
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from events.batch_media_event_publisher import BatchMediaEventPublisher
from handlers.media_tracking_handler import MediaTrackingHandler

class TestBatchMediaTrackingIntegration:
    """Integration test suite for batch media tracking."""

    @pytest.fixture
    def mock_pubsub_client(self):
        """Mock Pub/Sub client."""
        with patch('events.batch_media_event_publisher.pubsub_v1.PublisherClient') as mock_client:
            mock_instance = Mock()
            mock_future = Mock()
            mock_future.result.return_value = 'test_message_id'
            mock_instance.publish.return_value = mock_future
            mock_client.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_bigquery_client(self):
        """Mock BigQuery client."""
        with patch('handlers.media_tracking_handler.bigquery.Client') as mock_client:
            mock_instance = Mock()
            mock_instance.get_table.return_value = Mock()  # Table exists
            mock_instance.insert_rows_json.return_value = []  # No errors
            mock_client.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def mock_media_detector(self):
        """Mock MultiPlatformMediaDetector."""
        with patch('events.batch_media_event_publisher.MultiPlatformMediaDetector') as mock_detector:
            mock_instance = Mock()
            mock_detector.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def publisher(self, mock_pubsub_client, mock_bigquery_client, mock_media_detector):
        """Create BatchMediaEventPublisher with mocked dependencies."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'MEDIA_TRACKING_ENABLED': 'true'
        }):
            publisher = BatchMediaEventPublisher(
                project_id='test-project',
                topic_name='test-topic'
            )
            return publisher

    @pytest.fixture
    def sample_facebook_posts(self):
        """Sample Facebook posts with media."""
        return [
            {
                'id': 'post_123',
                'message': 'Test post with image',
                'attachments': {
                    'data': [
                        {
                            'type': 'photo',
                            'media': {
                                'image': {
                                    'src': 'https://facebook.com/image123.jpg'
                                }
                            }
                        }
                    ]
                }
            },
            {
                'id': 'post_456',
                'message': 'Test post with video',
                'attachments': {
                    'data': [
                        {
                            'type': 'video_inline',
                            'media': {
                                'source': 'https://facebook.com/video456.mp4'
                            }
                        }
                    ]
                }
            }
        ]

    @pytest.fixture
    def sample_youtube_videos(self):
        """Sample YouTube videos with media."""
        return [
            {
                'id': 'video_123',
                'snippet': {
                    'title': 'Test Video',
                    'description': 'Test video description',
                    'thumbnails': {
                        'high': {
                            'url': 'https://youtube.com/thumbnail123.jpg'
                        }
                    }
                },
                'contentDetails': {
                    'duration': 'PT5M30S'
                }
            }
        ]

    @pytest.fixture
    def sample_tiktok_videos(self):
        """Sample TikTok videos with media."""
        return [
            {
                'id': 'tiktok_123',
                'video': {
                    'playAddr': 'https://tiktok.com/video123.mp4',
                    'cover': 'https://tiktok.com/cover123.jpg'
                },
                'desc': 'Test TikTok video'
            }
        ]

    @pytest.fixture
    def crawl_metadata(self):
        """Sample crawl metadata."""
        return {
            'crawl_id': 'crawl_test_123',
            'snapshot_id': 'snapshot_456',
            'competitor': 'test_competitor',
            'brand': 'test_brand',
            'category': 'test_category',
            'timestamp': datetime.utcnow().isoformat()
        }

    def test_facebook_media_tracking_integration(self, publisher, mock_media_detector, 
                                               sample_facebook_posts, crawl_metadata):
        """Test complete Facebook media tracking integration."""
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 2,
            'posts_with_media': 2,
            'total_media_items': 2,
            'total_videos': 1,
            'total_images': 1,
            'all_media_urls': [
                {
                    'post_id': 'post_123',
                    'url': 'https://facebook.com/image123.jpg',
                    'type': 'image',
                    'id': 'img_123',
                    'attachment_url': 'https://facebook.com/attachment/123'
                },
                {
                    'post_id': 'post_456',
                    'url': 'https://facebook.com/video456.mp4',
                    'type': 'video',
                    'id': 'vid_456',
                    'attachment_url': 'https://facebook.com/attachment/456'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=sample_facebook_posts,
            platform='facebook',
            crawl_metadata=crawl_metadata
        )
        
        # Verify overall success
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 2
        
        # Verify tracking integration
        assert 'tracking_result' in result
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert tracking_result['rows_inserted'] == 2
        
        # Verify media tracking handler was called
        assert publisher.media_tracking_handler.client.insert_rows_json.called
        
        # Verify tracking records structure
        call_args = publisher.media_tracking_handler.client.insert_rows_json.call_args
        tracking_records = call_args[0][1]
        
        assert len(tracking_records) == 2
        assert tracking_records[0]['platform'] == 'facebook'
        assert tracking_records[0]['status'] == 'detected'
        assert tracking_records[0]['competitor'] == 'test_competitor'
        assert tracking_records[1]['media_type'] == 'video'

    def test_youtube_media_tracking_integration(self, publisher, mock_media_detector,
                                              sample_youtube_videos, crawl_metadata):
        """Test complete YouTube media tracking integration."""
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'youtube',
            'total_posts': 1,
            'posts_with_media': 1,
            'total_media_items': 2,  # Video + thumbnail
            'total_videos': 1,
            'total_images': 1,
            'all_media_urls': [
                {
                    'post_id': 'video_123',
                    'url': 'https://youtube.com/video123.mp4',
                    'type': 'video',
                    'id': 'vid_123'
                },
                {
                    'post_id': 'video_123',
                    'url': 'https://youtube.com/thumbnail123.jpg',
                    'type': 'image',
                    'id': 'thumb_123'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=sample_youtube_videos,
            platform='youtube',
            crawl_metadata=crawl_metadata
        )
        
        # Verify success
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 2
        
        # Verify tracking integration
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert tracking_result['rows_inserted'] == 2
        
        # Verify YouTube-specific tracking
        call_args = publisher.media_tracking_handler.client.insert_rows_json.call_args
        tracking_records = call_args[0][1]
        
        assert tracking_records[0]['platform'] == 'youtube'
        assert any(record['media_type'] == 'video' for record in tracking_records)
        assert any(record['media_type'] == 'image' for record in tracking_records)

    def test_tiktok_media_tracking_integration(self, publisher, mock_media_detector,
                                             sample_tiktok_videos, crawl_metadata):
        """Test complete TikTok media tracking integration."""
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'tiktok',
            'total_posts': 1,
            'posts_with_media': 1,
            'total_media_items': 2,  # Video + cover
            'total_videos': 1,
            'total_images': 1,
            'all_media_urls': [
                {
                    'post_id': 'tiktok_123',
                    'url': 'https://tiktok.com/video123.mp4',
                    'type': 'video',
                    'id': 'vid_123'
                },
                {
                    'post_id': 'tiktok_123',
                    'url': 'https://tiktok.com/cover123.jpg',
                    'type': 'image',
                    'id': 'cover_123'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=sample_tiktok_videos,
            platform='tiktok',
            crawl_metadata=crawl_metadata
        )
        
        # Verify success
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 2
        
        # Verify tracking integration
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert tracking_result['rows_inserted'] == 2
        
        # Verify TikTok-specific tracking
        call_args = publisher.media_tracking_handler.client.insert_rows_json.call_args
        tracking_records = call_args[0][1]
        
        assert tracking_records[0]['platform'] == 'tiktok'

    def test_no_media_found_scenario(self, publisher, mock_media_detector, crawl_metadata):
        """Test scenario when no media is found in posts."""
        # Mock media detection result with no media
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 1,
            'posts_with_media': 0,
            'total_media_items': 0,
            'total_videos': 0,
            'total_images': 0,
            'all_media_urls': [],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=[{'id': 'post_123', 'message': 'Text only post'}],
            platform='facebook',
            crawl_metadata=crawl_metadata
        )
        
        # Verify success but no media processing
        assert result['success'] == True
        assert result['message'] == 'No media to process'
        assert result['stats']['total_media_items'] == 0
        
        # Verify tracking was not called
        assert not publisher.media_tracking_handler.client.insert_rows_json.called

    def test_tracking_disabled_scenario(self, mock_pubsub_client, mock_bigquery_client, 
                                       mock_media_detector, crawl_metadata):
        """Test scenario when media tracking is disabled."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'MEDIA_TRACKING_ENABLED': 'false'
        }):
            publisher = BatchMediaEventPublisher(
                project_id='test-project',
                topic_name='test-topic'
            )
            
            # Mock media detection result
            mock_media_detector.detect_media_batch.return_value = {
                'platform': 'facebook',
                'total_posts': 1,
                'posts_with_media': 1,
                'total_media_items': 1,
                'total_videos': 0,
                'total_images': 1,
                'all_media_urls': [
                    {
                        'post_id': 'post_123',
                        'url': 'https://facebook.com/image123.jpg',
                        'type': 'image',
                        'id': 'img_123'
                    }
                ],
                'media_breakdown': {
                    'videos': [],
                    'images': [],
                    'profile_images': []
                }
            }
            
            # Execute integration
            result = publisher.publish_batch_from_raw_file(
                raw_posts=[{'id': 'post_123', 'message': 'Post with image'}],
                platform='facebook',
                crawl_metadata=crawl_metadata
            )
            
            # Verify success
            assert result['success'] == True
            assert result['stats']['total_media_items'] == 1
            
            # Verify tracking was disabled
            tracking_result = result['tracking_result']
            assert tracking_result['success'] == True
            assert tracking_result['rows_inserted'] == 0
            assert tracking_result['tracking_disabled'] == True

    def test_tracking_handler_initialization_failure(self, mock_pubsub_client, 
                                                    mock_media_detector, crawl_metadata):
        """Test scenario when tracking handler initialization fails."""
        with patch.dict(os.environ, {
            'GOOGLE_CLOUD_PROJECT': 'test-project',
            'MEDIA_TRACKING_ENABLED': 'true'
        }):
            with patch('events.batch_media_event_publisher.MediaTrackingHandler') as mock_handler:
                # Mock handler initialization failure
                mock_handler.side_effect = Exception('BigQuery connection failed')
                
                publisher = BatchMediaEventPublisher(
                    project_id='test-project',
                    topic_name='test-topic'
                )
                
                # Verify tracking is disabled due to initialization failure
                assert publisher.media_tracking_enabled == False
                assert publisher.media_tracking_handler is None

    def test_tracking_insertion_failure_graceful_degradation(self, publisher, mock_media_detector,
                                                           sample_facebook_posts, crawl_metadata):
        """Test graceful degradation when tracking insertion fails."""
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 1,
            'posts_with_media': 1,
            'total_media_items': 1,
            'total_videos': 0,
            'total_images': 1,
            'all_media_urls': [
                {
                    'post_id': 'post_123',
                    'url': 'https://facebook.com/image123.jpg',
                    'type': 'image',
                    'id': 'img_123'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Mock tracking insertion failure
        publisher.media_tracking_handler.client.insert_rows_json.return_value = [
            {'index': 0, 'errors': [{'reason': 'invalid', 'message': 'Invalid data'}]}
        ]
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=sample_facebook_posts,
            platform='facebook',
            crawl_metadata=crawl_metadata
        )
        
        # Verify overall success despite tracking failure
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 1
        
        # Verify tracking failure was captured
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == False
        assert 'error' in tracking_result
        assert tracking_result['rows_inserted'] == 0

    def test_media_tracking_with_batch_metadata(self, publisher, mock_media_detector,
                                              sample_facebook_posts, crawl_metadata):
        """Test media tracking with batch metadata."""
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 1,
            'posts_with_media': 1,
            'total_media_items': 1,
            'total_videos': 0,
            'total_images': 1,
            'all_media_urls': [
                {
                    'post_id': 'post_123',
                    'url': 'https://facebook.com/image123.jpg',
                    'type': 'image',
                    'id': 'img_123'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        file_metadata = {
            'filename': 'test_file.json',
            'size': 1024,
            'upload_timestamp': datetime.utcnow().isoformat()
        }
        
        # Execute integration with file metadata
        result = publisher.publish_batch_from_raw_file(
            raw_posts=sample_facebook_posts,
            platform='facebook',
            crawl_metadata=crawl_metadata,
            file_metadata=file_metadata
        )
        
        # Verify success
        assert result['success'] == True
        
        # Verify tracking was called with batch metadata
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert 'batch_metadata' in tracking_result

    def test_large_batch_processing(self, publisher, mock_media_detector, crawl_metadata):
        """Test processing of large media batches."""
        # Create large batch
        large_batch = []
        all_media_urls = []
        
        for i in range(150):  # Larger than default batch size
            large_batch.append({
                'id': f'post_{i}',
                'message': f'Post {i} with media'
            })
            all_media_urls.append({
                'post_id': f'post_{i}',
                'url': f'https://example.com/image{i}.jpg',
                'type': 'image',
                'id': f'img_{i}'
            })
        
        # Mock media detection result
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 150,
            'posts_with_media': 150,
            'total_media_items': 150,
            'total_videos': 0,
            'total_images': 150,
            'all_media_urls': all_media_urls,
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=large_batch,
            platform='facebook',
            crawl_metadata=crawl_metadata
        )
        
        # Verify success
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 150
        
        # Verify tracking handled batch size correctly
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert tracking_result['rows_inserted'] == 150
        
        # Verify multiple batch inserts were made (due to batch_size limit)
        assert publisher.media_tracking_handler.client.insert_rows_json.call_count >= 2

    def test_mixed_media_types_tracking(self, publisher, mock_media_detector, crawl_metadata):
        """Test tracking with mixed media types."""
        # Mock media detection result with various media types
        mock_media_detector.detect_media_batch.return_value = {
            'platform': 'facebook',
            'total_posts': 3,
            'posts_with_media': 3,
            'total_media_items': 4,
            'total_videos': 1,
            'total_images': 3,
            'all_media_urls': [
                {
                    'post_id': 'post_1',
                    'url': 'https://facebook.com/image1.jpg',
                    'type': 'image',
                    'id': 'img_1'
                },
                {
                    'post_id': 'post_2',
                    'url': 'https://facebook.com/video1.mp4',
                    'type': 'video',
                    'id': 'vid_1'
                },
                {
                    'post_id': 'post_3',
                    'url': 'https://facebook.com/profile.jpg',
                    'type': 'profile',
                    'id': 'prof_1'
                },
                {
                    'post_id': 'post_3',
                    'url': 'https://facebook.com/thumbnail.jpg',
                    'type': 'image',
                    'id': 'thumb_1'
                }
            ],
            'media_breakdown': {
                'videos': [],
                'images': [],
                'profile_images': []
            }
        }
        
        # Execute integration
        result = publisher.publish_batch_from_raw_file(
            raw_posts=[
                {'id': 'post_1', 'message': 'Post with image'},
                {'id': 'post_2', 'message': 'Post with video'},
                {'id': 'post_3', 'message': 'Post with profile and thumbnail'}
            ],
            platform='facebook',
            crawl_metadata=crawl_metadata
        )
        
        # Verify success
        assert result['success'] == True
        assert result['stats']['total_media_items'] == 4
        
        # Verify tracking captured all media types
        tracking_result = result['tracking_result']
        assert tracking_result['success'] == True
        assert tracking_result['rows_inserted'] == 4
        
        # Verify different media types in tracking records
        call_args = publisher.media_tracking_handler.client.insert_rows_json.call_args
        tracking_records = call_args[0][1]
        
        media_types = [record['media_type'] for record in tracking_records]
        assert 'image' in media_types
        assert 'video' in media_types
        assert 'profile' in media_types

if __name__ == '__main__':
    pytest.main([__file__, '-v'])