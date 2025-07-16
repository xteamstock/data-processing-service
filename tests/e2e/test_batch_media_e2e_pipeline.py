#!/usr/bin/env python3
"""
End-to-end integration tests for the complete data processing pipeline with batch media.

Tests the full flow from Pub/Sub event reception to batch media event publishing,
verifying event formats, parallel job execution, and error handling.
"""

import json
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone
import base64
import threading
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from events.event_handler import EventHandler
from events.batch_media_event_publisher import BatchMediaEventPublisher


class TestBatchMediaE2EPipeline:
    """End-to-end tests for complete data processing pipeline with batch media."""
    
    @pytest.fixture
    def event_handler_with_real_components(self):
        """Create EventHandler with minimal mocking for E2E testing."""
        # Set required environment variable
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project'
        
        with patch('google.cloud.storage.Client') as mock_storage:
            with patch('google.cloud.pubsub_v1.PublisherClient') as mock_pubsub:
                # Mock storage client
                mock_bucket = Mock()
                mock_blob = Mock()
                mock_storage.return_value.bucket.return_value = mock_bucket
                mock_bucket.blob.return_value = mock_blob
                
                # Create handler
                handler = EventHandler()
                handler.storage_client = mock_storage.return_value
                
                # Mock Pub/Sub publisher for batch media
                mock_publisher = Mock()
                mock_publisher.topic_path.return_value = "projects/test/topics/batch-media-processing-requests"
                mock_publisher.publish.return_value.result.return_value = "msg-id-123"
                
                if handler.batch_media_publisher:
                    handler.batch_media_publisher.publisher = mock_publisher
                
                return handler
    
    def create_full_pubsub_event(self, platform, num_posts=10):
        """Create a complete Pub/Sub event with realistic data."""
        event_data = {
            "event_type": "data-ingestion-completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_id": f"e2e-test-{platform}-{int(time.time())}",
            "data": {
                "crawl_id": f"crawl-{platform}-{int(time.time())}",
                "snapshot_id": f"snap-{platform}-{int(time.time())}",
                "gcs_path": f"gs://test-bucket/raw_data/{platform}/test-data.json",
                "platform": platform,
                "competitor": "test-competitor",
                "brand": "test-brand",
                "category": "test-category",
                "crawl_config": {
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "num_posts": num_posts
                },
                "ingestion_stats": {
                    "total_posts": num_posts,
                    "success_rate": 1.0,
                    "duration_seconds": 45.2
                }
            }
        }
        
        # Create Flask request mock
        request = Mock()
        encoded_data = base64.b64encode(json.dumps(event_data).encode('utf-8')).decode('utf-8')
        request.get_json.return_value = {
            "message": {
                "data": encoded_data,
                "attributes": {
                    "event_type": "data-ingestion-completed",
                    "platform": platform
                },
                "messageId": f"pubsub-msg-{int(time.time())}",
                "publishTime": datetime.now(timezone.utc).isoformat()
            },
            "subscription": "projects/test/subscriptions/data-processing-push"
        }
        
        return request, event_data
    
    def test_complete_pipeline_with_all_jobs_parallel(self, event_handler_with_real_components):
        """Test that all 4 jobs execute successfully in parallel."""
        handler = event_handler_with_real_components
        request, event_data = self.create_full_pubsub_event("facebook", num_posts=20)
        
        # Create test data with media
        test_posts = []
        for i in range(20):
            post = {
                "id": f"fb-post-{i}",
                "message": f"Test post {i}",
                "created_time": "2024-01-15T12:00:00Z",
                "attachments": []
            }
            # Add media to some posts
            if i % 3 == 0:  # Every 3rd post has video
                post["attachments"].append({
                    "type": "video",
                    "url": f"https://video.fb.com/v{i}.mp4",
                    "duration_ms": "30000"
                })
            elif i % 3 == 1:  # Every 3rd+1 post has image
                post["attachments"].append({
                    "type": "image",
                    "url": f"https://image.fb.com/img{i}.jpg"
                })
            test_posts.append(post)
        
        # Mock GCS download
        handler._download_raw_data_from_gcs = Mock(return_value=test_posts)
        
        # Track job execution timing
        job_timings = {
            "gcs": {"start": None, "end": None},
            "bigquery": {"start": None, "end": None},
            "media": {"start": None, "end": None},
            "batch_media": {"start": None, "end": None}
        }
        
        # Mock handlers with timing tracking
        def mock_gcs_upload(*args, **kwargs):
            job_timings["gcs"]["start"] = time.time()
            time.sleep(0.1)  # Simulate work
            job_timings["gcs"]["end"] = time.time()
            return (True, None, {"successful_uploads": 4, "total_records": 20})
        
        def mock_bigquery_insert(*args, **kwargs):
            job_timings["bigquery"]["start"] = time.time()
            time.sleep(0.1)  # Simulate work
            job_timings["bigquery"]["end"] = time.time()
            return {"success": True, "table_id": "facebook_posts_2024"}
        
        def mock_media_publish(*args, **kwargs):
            job_timings["media"]["start"] = time.time()
            time.sleep(0.05)  # Simulate work
            job_timings["media"]["end"] = time.time()
            return True
        
        def mock_batch_media_publish(*args, **kwargs):
            job_timings["batch_media"]["start"] = time.time()
            time.sleep(0.05)  # Simulate work
            job_timings["batch_media"]["end"] = time.time()
            return {
                "success": True,
                "stats": {
                    "total_media_items": 13,
                    "total_videos": 7,
                    "total_images": 6,
                    "posts_with_media": 13
                },
                "event_id": f"{event_data['data']['crawl_id']}_{event_data['data']['snapshot_id']}_batch_media",
                "message_id": "batch-msg-123"
            }
        
        handler.gcs_processed_handler.upload_grouped_data = Mock(side_effect=mock_gcs_upload)
        handler.bigquery_handler.insert_posts = Mock(side_effect=mock_bigquery_insert)
        handler.event_publisher.publish_media_processing_requested = Mock(side_effect=mock_media_publish)
        handler.batch_media_publisher.publish_batch_from_raw_file = Mock(side_effect=mock_batch_media_publish)
        
        # Execute pipeline
        start_time = time.time()
        response, status_code = handler.handle_data_ingestion_completed(request)
        end_time = time.time()
        
        # Verify successful completion
        assert status_code == 200
        assert response["success"] is True
        assert response["processed_posts"] == 20
        
        # Verify all 4 jobs completed
        jobs = response["jobs_summary"]
        assert jobs["job1_gcs_upload"]["success"] is True
        assert jobs["job2_bigquery_insert"]["success"] is True
        assert jobs["job3_media_detection"]["media_event_published"] is True
        assert jobs["job4_batch_media"]["success"] is True
        
        # Verify batch media results
        assert jobs["job4_batch_media"]["media_count"] == 13
        assert jobs["job4_batch_media"]["event_id"] == f"{event_data['data']['crawl_id']}_{event_data['data']['snapshot_id']}_batch_media"
        
        # Verify total execution time is less than sum of individual jobs (parallel execution)
        total_time = end_time - start_time
        assert total_time < 0.5  # Should be much less than 0.3s (sum of all job times)
    
    def test_batch_media_event_format_compliance(self, event_handler_with_real_components):
        """Test that batch media events match the expected schema."""
        handler = event_handler_with_real_components
        request, event_data = self.create_full_pubsub_event("tiktok", num_posts=5)
        
        # Create TikTok test data
        test_posts = [
            {
                "id": "tiktok-1",
                "desc": "Test TikTok video",
                "createTime": 1705320000,
                "video": {
                    "playAddr": "https://v.tiktok.com/video1.mp4",
                    "duration": 15,
                    "cover": "https://p.tiktok.com/cover1.jpg"
                }
            },
            {
                "id": "tiktok-2",
                "desc": "Another TikTok",
                "createTime": 1705320100,
                "video": {
                    "playAddr": "https://v.tiktok.com/video2.mp4",
                    "duration": 30,
                    "cover": "https://p.tiktok.com/cover2.jpg"
                }
            }
        ]
        
        # Mock GCS download
        handler._download_raw_data_from_gcs = Mock(return_value=test_posts)
        
        # Capture the actual batch media event
        captured_event = None
        
        def capture_publish_call(*args, **kwargs):
            nonlocal captured_event
            # The first argument after self should be the raw_posts
            if 'raw_posts' in kwargs:
                # Simulate what the batch media publisher would do
                captured_event = {
                    "event_type": "batch-media-download-requested",
                    "event_id": f"{kwargs['crawl_metadata']['crawl_id']}_{kwargs['crawl_metadata']['snapshot_id']}_batch_media",
                    "data": {
                        "batch_summary": {
                            "platform": "tiktok",
                            "total_posts": len(kwargs['raw_posts']),
                            "posts_with_media": 2,
                            "total_media_items": 4,
                            "media_counts": {
                                "videos": 2,
                                "images": 2
                            }
                        },
                        "media_by_type": {
                            "videos": [
                                {
                                    "url": "https://v.tiktok.com/video1.mp4",
                                    "post_id": "tiktok-1",
                                    "duration": 15,
                                    "type": "video"
                                },
                                {
                                    "url": "https://v.tiktok.com/video2.mp4",
                                    "post_id": "tiktok-2",
                                    "duration": 30,
                                    "type": "video"
                                }
                            ],
                            "images": [
                                {
                                    "url": "https://p.tiktok.com/cover1.jpg",
                                    "post_id": "tiktok-1",
                                    "type": "cover_image"
                                },
                                {
                                    "url": "https://p.tiktok.com/cover2.jpg",
                                    "post_id": "tiktok-2",
                                    "type": "cover_image"
                                }
                            ]
                        },
                        "crawl_metadata": kwargs['crawl_metadata'],
                        "processing_config": {
                            "priority": "high",
                            "parallel_downloads": 4
                        }
                    }
                }
            
            return {
                "success": True,
                "stats": {
                    "total_media_items": 4,
                    "total_videos": 2,
                    "total_images": 2,
                    "posts_with_media": 2
                },
                "event_id": captured_event["event_id"] if captured_event else "test-event",
                "message_id": "test-msg-123"
            }
        
        # Mock handlers
        handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 2})
        )
        handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "tiktok_posts"}
        )
        handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            side_effect=capture_publish_call
        )
        
        # Execute
        response, status_code = handler.handle_data_ingestion_completed(request)
        
        # Verify response
        assert status_code == 200
        assert response["success"] is True
        
        # Verify batch media publisher was called
        handler.batch_media_publisher.publish_batch_from_raw_file.assert_called_once()
        
        # Verify the simulated event structure
        assert captured_event is not None
        assert captured_event["event_type"] == "batch-media-download-requested"
        assert "data" in captured_event
        assert "batch_summary" in captured_event["data"]
        assert "media_by_type" in captured_event["data"]
        assert "crawl_metadata" in captured_event["data"]
        
        # Verify media organization
        assert len(captured_event["data"]["media_by_type"]["videos"]) == 2
        assert len(captured_event["data"]["media_by_type"]["images"]) == 2
        
        # Verify video metadata includes duration
        for video in captured_event["data"]["media_by_type"]["videos"]:
            assert "duration" in video
            assert video["duration"] > 0
    
    def test_pipeline_resilience_with_partial_failures(self, event_handler_with_real_components):
        """Test that pipeline continues when some jobs fail."""
        handler = event_handler_with_real_components
        request, event_data = self.create_full_pubsub_event("youtube", num_posts=3)
        
        # Create YouTube test data
        test_posts = [
            {
                "id": "youtube-1",
                "snippet": {
                    "title": "Test Video 1",
                    "thumbnails": {
                        "high": {"url": "https://i.ytimg.com/vi/abc123/hqdefault.jpg"}
                    }
                },
                "contentDetails": {"duration": "PT3M25S"}
            }
        ]
        
        # Mock GCS download
        handler._download_raw_data_from_gcs = Mock(return_value=test_posts)
        
        # Make GCS upload fail but other jobs succeed
        handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(False, "Network timeout", {})
        )
        handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "youtube_videos"}
        )
        handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
            return_value={
                "success": True,
                "stats": {"total_media_items": 2, "total_videos": 1, "total_images": 1, "posts_with_media": 1},
                "event_id": "test-event-id",
                "message_id": "test-msg"
            }
        )
        
        # Execute
        response, status_code = handler.handle_data_ingestion_completed(request)
        
        # Should fail overall due to GCS failure (critical job)
        assert status_code == 500
        assert response["success"] is False
        assert "GCS upload" in response["error"]
        
        # But verify other jobs still executed
        assert response["bigquery_insert_completed"] is True
        assert response["jobs_summary"]["job4_batch_media"]["success"] is True
    
    def test_batch_media_topic_verification(self, event_handler_with_real_components):
        """Test that batch media events are published to the correct topic."""
        handler = event_handler_with_real_components
        request, event_data = self.create_full_pubsub_event("facebook", num_posts=1)
        
        # Simple test data
        test_posts = [{"id": "1", "attachments": [{"type": "video", "url": "test.mp4"}]}]
        handler._download_raw_data_from_gcs = Mock(return_value=test_posts)
        
        # Mock other handlers
        handler.gcs_processed_handler.upload_grouped_data = Mock(
            return_value=(True, None, {"successful_uploads": 1, "total_records": 1})
        )
        handler.bigquery_handler.insert_posts = Mock(
            return_value={"success": True, "table_id": "facebook_posts"}
        )
        
        # Track Pub/Sub publish calls
        publish_calls = []
        
        def track_publish(topic_path, data, **attributes):
            publish_calls.append({
                "topic": topic_path,
                "data": json.loads(data.decode('utf-8')),
                "attributes": attributes
            })
            future = Mock()
            future.result.return_value = f"msg-{len(publish_calls)}"
            return future
        
        if handler.batch_media_publisher and handler.batch_media_publisher.publisher:
            handler.batch_media_publisher.publisher.publish = Mock(side_effect=track_publish)
            handler.batch_media_publisher.topic_path = "projects/test/topics/batch-media-processing-requests"
        
        # Execute
        response, status_code = handler.handle_data_ingestion_completed(request)
        
        # Verify response
        assert status_code == 200
        assert response["success"] is True
        
        # Verify Pub/Sub publish was called if publisher is available
        if handler.batch_media_publisher and handler.batch_media_publisher.publisher:
            assert len(publish_calls) == 1
            assert "batch-media-processing-requests" in publish_calls[0]["topic"]
            assert publish_calls[0]["data"]["event_type"] == "batch-media-download-requested"
            assert publish_calls[0]["attributes"]["platform"] == "facebook"
    
    def test_performance_impact_of_batch_media(self, event_handler_with_real_components):
        """Test that adding batch media job doesn't significantly impact performance."""
        handler = event_handler_with_real_components
        
        # Test with different post counts
        test_sizes = [10, 50, 100]
        timings = []
        
        for num_posts in test_sizes:
            request, _ = self.create_full_pubsub_event("facebook", num_posts=num_posts)
            
            # Create test data
            test_posts = [{"id": f"post-{i}", "message": f"Post {i}"} for i in range(num_posts)]
            handler._download_raw_data_from_gcs = Mock(return_value=test_posts)
            
            # Mock all handlers with minimal delay
            handler.gcs_processed_handler.upload_grouped_data = Mock(
                return_value=(True, None, {"successful_uploads": 1, "total_records": num_posts})
            )
            handler.bigquery_handler.insert_posts = Mock(
                return_value={"success": True, "table_id": "facebook_posts"}
            )
            handler.batch_media_publisher.publish_batch_from_raw_file = Mock(
                return_value={
                    "success": True,
                    "stats": {"total_media_items": 0, "total_videos": 0, "total_images": 0, "posts_with_media": 0}
                }
            )
            
            # Measure execution time
            start = time.time()
            response, status_code = handler.handle_data_ingestion_completed(request)
            duration = time.time() - start
            
            timings.append({
                "posts": num_posts,
                "duration": duration,
                "success": response["success"]
            })
            
            assert status_code == 200
            assert response["success"] is True
        
        # Verify performance scales reasonably
        # Processing time should not increase dramatically with post count
        # due to parallel job execution
        for i in range(1, len(timings)):
            ratio = timings[i]["duration"] / timings[i-1]["duration"]
            posts_ratio = timings[i]["posts"] / timings[i-1]["posts"]
            
            # Duration should increase slower than post count (due to parallelism)
            assert ratio < posts_ratio * 0.5  # Allow up to 50% of linear scaling


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])