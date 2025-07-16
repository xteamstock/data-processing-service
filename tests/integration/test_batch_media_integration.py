#!/usr/bin/env python3
"""
Integration tests for batch media processing in the data processing pipeline.

Tests the complete flow from Pub/Sub event to batch media event publishing
across multiple platforms (Facebook, TikTok, YouTube).
"""

import json
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import base64

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from events.event_handler import EventHandler
from events.batch_media_event_publisher import BatchMediaEventPublisher


class TestBatchMediaIntegration:
    """Integration tests for batch media processing pipeline."""
    
    @pytest.fixture
    def event_handler(self):
        """Create EventHandler instance with mocked dependencies."""
        # Set required environment variable
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project'
        
        with patch('events.event_handler.storage.Client'):
            with patch('events.event_handler.BigQueryHandler'):
                with patch('events.event_handler.GCSProcessedHandler'):
                    with patch('events.event_handler.EventPublisher'):
                        with patch('events.event_handler.BatchMediaEventPublisher') as mock_batch_publisher:
                            # Create a mock instance
                            mock_instance = Mock()
                            mock_instance.publish_batch_from_raw_file = Mock()
                            mock_batch_publisher.return_value = mock_instance
                            
                            handler = EventHandler()
                            # Ensure batch media is enabled
                            handler.batch_media_enabled = True
                            handler.batch_media_publisher = mock_instance
                            return handler
    
    @pytest.fixture
    def mock_request(self):
        """Create mock Flask request object."""
        request = Mock()
        return request
    
    @pytest.fixture
    def facebook_fixture_data(self):
        """Load Facebook test data from fixtures."""
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 
            '../../fixtures/gcs-facebook-posts.json'
        )
        with open(fixtures_path, 'r') as f:
            return json.load(f)
    
    @pytest.fixture
    def tiktok_fixture_data(self):
        """Load TikTok test data from fixtures."""
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 
            '../../fixtures/gcs-tiktok-posts.json'
        )
        with open(fixtures_path, 'r') as f:
            return json.load(f)
    
    @pytest.fixture
    def youtube_fixture_data(self):
        """Load YouTube test data from fixtures."""
        fixtures_path = os.path.join(
            os.path.dirname(__file__), 
            '../../fixtures/gcs-youtube-posts.json'
        )
        with open(fixtures_path, 'r') as f:
            return json.load(f)
    
    def create_pubsub_message(self, event_data):
        """Create a Pub/Sub push message with base64 encoded data."""
        encoded_data = base64.b64encode(
            json.dumps(event_data).encode('utf-8')
        ).decode('utf-8')
        
        return {
            "message": {
                "data": encoded_data,
                "attributes": {
                    "event_type": "data-ingestion-completed"
                },
                "messageId": "test-message-123",
                "publishTime": datetime.now(timezone.utc).isoformat()
            },
            "subscription": "test-subscription"
        }
    
    def test_facebook_batch_media_processing(self, event_handler, mock_request, facebook_fixture_data):
        """Test batch media processing for Facebook posts with video attachments."""
        # Prepare test event data
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "test-crawl-123",
                "snapshot_id": "test-snapshot-456",
                "gcs_path": "gs://test-bucket/facebook-posts.json",
                "platform": "facebook",
                "competitor": "nutifood",
                "brand": "growplus",
                "category": "milk-powder"
            }
        }
        
        # Mock request
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock GCS download to return Facebook fixture data
        event_handler._download_raw_data_from_gcs = Mock(return_value=facebook_fixture_data)
        
        # Mock text processor to return processed posts
        processed_posts = []
        for post in facebook_fixture_data[:5]:  # Test with first 5 posts
            processed_post = {
                "post_id": post.get("id", ""),
                "platform": "facebook",
                "attachments": post.get("attachments", []),
                "media_metadata": {
                    "media_processing_requested": bool(post.get("attachments", [])),
                    "media_count": len(post.get("attachments", []))
                }
            }
            processed_posts.append(processed_post)
        
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=processed_posts
        )
        
        # Mock successful BigQuery and GCS operations
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "facebook_posts"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 5})
        )
        
        # Mock batch media publisher
        batch_media_result = {
            "success": True,
            "message": "Published batch event with 3 media items",
            "stats": {
                "total_media_items": 3,
                "total_videos": 2,
                "total_images": 1,
                "posts_with_media": 3
            },
            "event_id": "test-crawl-123_test-snapshot-456_batch_media",
            "message_id": "pub-sub-msg-789"
        }
        event_handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            return_value=batch_media_result
        )
        
        # Execute the handler
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions
        assert status_code == 200
        assert response["success"] is True
        assert "jobs_summary" in response
        assert "job4_batch_media" in response["jobs_summary"]
        
        # Verify batch media job results
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is True
        assert batch_media_job["media_count"] == 3
        assert batch_media_job["event_id"] == "test-crawl-123_test-snapshot-456_batch_media"
        assert batch_media_job["media_breakdown"]["videos"] == 2
        assert batch_media_job["media_breakdown"]["images"] == 1
        
        # Verify batch media publisher was called with correct arguments
        event_handler.batch_media_publisher.publish_batch_from_raw_file.assert_called_once_with(
            raw_posts=processed_posts,
            platform="facebook",
            crawl_metadata={
                'crawl_id': 'test-crawl-123',
                'snapshot_id': 'test-snapshot-456',
                'gcs_path': 'gs://test-bucket/facebook-posts.json',
                'crawl_date': event_data['timestamp'],
                'platform': 'facebook',
                'competitor': 'nutifood',
                'brand': 'growplus',
                'category': 'milk-powder'
            },
            file_metadata={'source': 'data_processing_pipeline'}
        )
    
    def test_tiktok_batch_media_processing(self, event_handler, mock_request, tiktok_fixture_data):
        """Test batch media processing for TikTok posts with video URLs and cover images."""
        # Prepare test event data
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "tiktok-crawl-789",
                "snapshot_id": "tiktok-snap-101",
                "gcs_path": "gs://test-bucket/tiktok-posts.json",
                "platform": "tiktok",
                "competitor": "vinamilk",
                "brand": "optimum",
                "category": "nutrition"
            }
        }
        
        # Mock request
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock GCS download
        event_handler._download_raw_data_from_gcs = Mock(return_value=tiktok_fixture_data)
        
        # Mock text processor
        processed_posts = []
        for post in tiktok_fixture_data[:5]:
            processed_post = {
                "post_id": post.get("id", ""),
                "platform": "tiktok",
                "video_url": post.get("video", {}).get("playAddr", ""),
                "cover_image_url": post.get("video", {}).get("cover", ""),
                "media_metadata": {
                    "media_processing_requested": True,
                    "media_count": 2  # video + cover
                }
            }
            processed_posts.append(processed_post)
        
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=processed_posts
        )
        
        # Mock operations
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "tiktok_posts"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 5})
        )
        
        # Mock batch media publisher with TikTok-specific results
        batch_media_result = {
            "success": True,
            "stats": {
                "total_media_items": 10,
                "total_videos": 5,
                "total_images": 5,  # cover images
                "posts_with_media": 5
            },
            "event_id": "tiktok-crawl-789_tiktok-snap-101_batch_media",
            "message_id": "tiktok-pubsub-msg-202"
        }
        event_handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            return_value=batch_media_result
        )
        
        # Execute
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions
        assert status_code == 200
        assert response["success"] is True
        
        # Verify TikTok batch media results
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is True
        assert batch_media_job["media_count"] == 10
        assert batch_media_job["media_breakdown"]["videos"] == 5
        assert batch_media_job["media_breakdown"]["images"] == 5
    
    def test_youtube_batch_media_processing(self, event_handler, mock_request, youtube_fixture_data):
        """Test batch media processing for YouTube posts with video URLs and thumbnails."""
        # Prepare test event data
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "youtube-crawl-333",
                "snapshot_id": "youtube-snap-444",
                "gcs_path": "gs://test-bucket/youtube-posts.json",
                "platform": "youtube",
                "competitor": "abbott",
                "brand": "ensure",
                "category": "health-drink"
            }
        }
        
        # Mock request
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock GCS download
        event_handler._download_raw_data_from_gcs = Mock(return_value=youtube_fixture_data)
        
        # Mock text processor
        processed_posts = []
        for post in youtube_fixture_data[:3]:
            processed_post = {
                "post_id": post.get("id", ""),
                "platform": "youtube",
                "video_url": f"https://www.youtube.com/watch?v={post.get('id', '')}",
                "thumbnail_url": post.get("snippet", {}).get("thumbnails", {}).get("high", {}).get("url", ""),
                "media_metadata": {
                    "media_processing_requested": True,
                    "media_count": 2  # video + thumbnail
                }
            }
            processed_posts.append(processed_post)
        
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=processed_posts
        )
        
        # Mock operations
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "youtube_videos"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 3})
        )
        
        # Mock batch media publisher
        batch_media_result = {
            "success": True,
            "stats": {
                "total_media_items": 6,
                "total_videos": 3,
                "total_images": 3,  # thumbnails
                "posts_with_media": 3
            },
            "event_id": "youtube-crawl-333_youtube-snap-444_batch_media",
            "message_id": "youtube-pubsub-msg-555"
        }
        event_handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            return_value=batch_media_result
        )
        
        # Execute
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions
        assert status_code == 200
        assert response["success"] is True
        
        # Verify YouTube batch media results
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is True
        assert batch_media_job["media_count"] == 6
        assert batch_media_job["media_breakdown"]["videos"] == 3
        assert batch_media_job["media_breakdown"]["images"] == 3
    
    def test_batch_media_processing_with_no_media(self, event_handler, mock_request):
        """Test batch media processing when posts have no media URLs."""
        # Create posts with no media
        posts_without_media = [
            {
                "post_id": "text-only-1",
                "platform": "facebook",
                "text": "This is a text-only post",
                "media_metadata": {
                    "media_processing_requested": False,
                    "media_count": 0
                }
            },
            {
                "post_id": "text-only-2",
                "platform": "facebook",
                "text": "Another text post",
                "media_metadata": {
                    "media_processing_requested": False,
                    "media_count": 0
                }
            }
        ]
        
        # Setup event
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "no-media-crawl",
                "snapshot_id": "no-media-snap",
                "gcs_path": "gs://test-bucket/no-media.json",
                "platform": "facebook",
                "competitor": "test",
                "brand": "test",
                "category": "test"
            }
        }
        
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock operations - return raw posts that will be processed
        raw_posts_without_media = [
            {"id": "text-only-1", "message": "This is a text-only post"},
            {"id": "text-only-2", "message": "Another text post"}
        ]
        event_handler._download_raw_data_from_gcs = Mock(return_value=raw_posts_without_media)
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=posts_without_media
        )
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "facebook_posts"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 2})
        )
        
        # Mock batch media publisher to return no media found
        batch_media_result = {
            "success": True,
            "message": "No media to process",
            "stats": {
                "total_media_items": 0,
                "total_videos": 0,
                "total_images": 0,
                "posts_with_media": 0
            }
        }
        event_handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            return_value=batch_media_result
        )
        
        # Execute
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions
        assert status_code == 200
        assert response["success"] is True
        
        # Verify batch media job handled no media case correctly
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is True
        assert batch_media_job["media_count"] == 0
        assert batch_media_job["media_breakdown"]["videos"] == 0
        assert batch_media_job["media_breakdown"]["images"] == 0
    
    def test_batch_media_processing_failure_handling(self, event_handler, mock_request):
        """Test that batch media failures don't block other jobs."""
        # Setup event
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "fail-test-crawl",
                "snapshot_id": "fail-test-snap",
                "gcs_path": "gs://test-bucket/test.json",
                "platform": "facebook",
                "competitor": "test",
                "brand": "test",
                "category": "test"
            }
        }
        
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock successful operations for other jobs
        event_handler._download_raw_data_from_gcs = Mock(return_value=[{"id": "1"}])
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=[{"post_id": "1", "media_metadata": {"media_processing_requested": True}}]
        )
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "facebook_posts"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 1})
        )
        
        # Make batch media publisher fail
        event_handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            side_effect=Exception("Pub/Sub topic not found")
        )
        
        # Execute
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions - should still succeed overall
        assert status_code == 200
        assert response["success"] is True
        assert response["gcs_upload_completed"] is True
        assert response["bigquery_insert_completed"] is True
        
        # Verify batch media job shows failure but didn't block pipeline
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is False
        assert "error" in batch_media_job
        assert "Pub/Sub topic not found" in batch_media_job["error"]
        assert batch_media_job["media_count"] == 0
    
    def test_batch_media_publisher_not_initialized(self, event_handler, mock_request):
        """Test graceful handling when batch media publisher is not initialized."""
        # Disable batch media publisher
        event_handler.batch_media_enabled = False
        event_handler.batch_media_publisher = None
        
        # Setup basic event
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "crawl_id": "test-crawl",
                "snapshot_id": "test-snap",
                "gcs_path": "gs://test-bucket/test.json",
                "platform": "facebook",
                "competitor": "test",
                "brand": "test",
                "category": "test"
            }
        }
        
        pubsub_message = self.create_pubsub_message(event_data)
        mock_request.get_json.return_value = pubsub_message
        
        # Mock operations
        event_handler._download_raw_data_from_gcs = Mock(return_value=[{"id": "1"}])
        event_handler.text_processor.process_posts_for_analytics = Mock(
            return_value=[{"post_id": "1"}]
        )
        event_handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "facebook_posts"}
        )
        event_handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 1})
        )
        
        # Execute
        response, status_code = event_handler.handle_data_ingestion_completed(mock_request)
        
        # Assertions
        assert status_code == 200
        assert response["success"] is True
        
        # Verify batch media job shows it was skipped
        batch_media_job = response["jobs_summary"]["job4_batch_media"]
        assert batch_media_job["success"] is False
        assert batch_media_job["error"] == "Batch media publisher not initialized"
        assert batch_media_job["media_count"] == 0


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])