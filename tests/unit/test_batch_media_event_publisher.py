#!/usr/bin/env python3
"""
Unit tests for BatchMediaEventPublisher with real fixture data.

This test file demonstrates the actual event structure that gets created
for each platform using the real fixture data.
"""

import json
import pytest
import os
import sys
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from events.batch_media_event_publisher import BatchMediaEventPublisher
from handlers.multi_platform_media_detector import MultiPlatformMediaDetector


class TestBatchMediaEventPublisher:
    """Unit tests for BatchMediaEventPublisher using real fixture data."""
    
    @pytest.fixture
    def publisher(self):
        """Create a BatchMediaEventPublisher instance."""
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project'
        
        with patch('google.cloud.pubsub_v1.PublisherClient') as mock_publisher_client:
            mock_publisher = Mock()
            mock_publisher.topic_path.return_value = "projects/test-project/topics/batch-media-processing-requests"
            mock_publisher.publish.return_value.result.return_value = "mock-message-id-123"
            mock_publisher_client.return_value = mock_publisher
            
            publisher = BatchMediaEventPublisher()
            publisher.publisher = mock_publisher
            return publisher
    
    @pytest.fixture
    def crawl_metadata(self):
        """Sample crawl metadata for testing."""
        return {
            'crawl_id': 'test-crawl-12345',
            'snapshot_id': 'test-snapshot-67890',
            'platform': 'facebook',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': datetime.now(timezone.utc).isoformat()
        }
    
    @pytest.fixture
    def facebook_fixture_data(self):
        """Load Facebook fixture data."""
        fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-facebook-posts.json'
        with open(fixture_path, 'r') as f:
            return json.load(f)
    
    @pytest.fixture
    def tiktok_fixture_data(self):
        """Load TikTok fixture data."""
        fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-tiktok-posts.json'
        with open(fixture_path, 'r') as f:
            return json.load(f)
    
    @pytest.fixture
    def youtube_fixture_data(self):
        """Load YouTube fixture data."""
        fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-youtube-posts.json'
        with open(fixture_path, 'r') as f:
            return json.load(f)
    
    def test_facebook_batch_event_structure(self, publisher, crawl_metadata, facebook_fixture_data):
        """Test the structure of Facebook batch media events."""
        print("\n" + "="*80)
        print("FACEBOOK BATCH MEDIA EVENT STRUCTURE")
        print("="*80)
        
        # Update metadata for Facebook
        crawl_metadata['platform'] = 'facebook'
        
        # Publish batch event
        result = publisher.publish_batch_from_raw_file(
            raw_posts=facebook_fixture_data,
            platform='facebook',
            crawl_metadata=crawl_metadata,
            file_metadata={'filename': 'gcs-facebook-posts.json', 'size': 12345}
        )
        
        # Verify result
        assert result['success'] is True
        assert 'event_id' in result
        assert 'message_id' in result
        assert 'stats' in result
        
        # Print stats
        stats = result['stats']
        print(f"📊 FACEBOOK STATS:")
        print(f"  Total posts: {stats['total_posts']}")
        print(f"  Posts with media: {stats['posts_with_media']}")
        print(f"  Total media items: {stats['total_media_items']}")
        print(f"  Videos: {stats['total_videos']}")
        print(f"  Images: {stats['total_images']}")
        
        # Get the actual event that would be published
        detector = MultiPlatformMediaDetector()
        batch_result = detector.detect_media_batch(facebook_fixture_data, 'facebook')
        event = publisher._create_batch_event(batch_result, crawl_metadata, {'filename': 'gcs-facebook-posts.json'})
        
        # Print event structure
        print(f"\n📤 FACEBOOK EVENT STRUCTURE:")
        print(f"  Event Type: {event['event_type']}")
        print(f"  Event ID: {event['event_id']}")
        print(f"  Schema Version: {event['schema_version']}")
        print(f"  Event Size: {len(json.dumps(event))} bytes")
        
        # Print batch summary
        batch_summary = event['data']['batch_summary']
        print(f"\n📋 BATCH SUMMARY:")
        print(f"  Platform: {batch_summary['platform']}")
        print(f"  Total Posts: {batch_summary['total_posts']}")
        print(f"  Posts with Media: {batch_summary['posts_with_media']}")
        print(f"  Total Media Items: {batch_summary['total_media_items']}")
        print(f"  Media Counts: {batch_summary['media_counts']}")
        
        # Print media types
        media_by_type = event['data']['media_by_type']
        print(f"\n🎬 MEDIA BY TYPE:")
        for media_type, items in media_by_type.items():
            if items:
                print(f"  {media_type.capitalize()}: {len(items)} items")
                for i, item in enumerate(items[:2]):  # Show first 2
                    url_preview = item['url'][:60] + "..." if len(item['url']) > 60 else item['url']
                    duration = f" ({item.get('duration', 'N/A')}s)" if media_type == 'videos' else ""
                    print(f"    {i+1}. {url_preview}{duration}")
        
        # Print processing config
        processing_config = event['data']['processing_config']
        print(f"\n⚙️  PROCESSING CONFIG:")
        print(f"  Priority: {processing_config['priority']}")
        print(f"  Parallel Downloads: {processing_config['parallel_downloads']}")
        print(f"  Timeout: {processing_config['timeout_seconds']}s")
        
        # Verify event structure
        assert event['event_type'] == 'batch-media-download-requested'
        assert 'data' in event
        assert 'batch_summary' in event['data']
        assert 'media_by_type' in event['data']
        assert 'processing_config' in event['data']
        assert 'storage_config' in event['data']
        
        return event
    
    def test_tiktok_batch_event_structure(self, publisher, crawl_metadata, tiktok_fixture_data):
        """Test the structure of TikTok batch media events."""
        print("\n" + "="*80)
        print("TIKTOK BATCH MEDIA EVENT STRUCTURE")
        print("="*80)
        
        # Update metadata for TikTok
        crawl_metadata['platform'] = 'tiktok'
        
        # Publish batch event
        result = publisher.publish_batch_from_raw_file(
            raw_posts=tiktok_fixture_data,
            platform='tiktok',
            crawl_metadata=crawl_metadata,
            file_metadata={'filename': 'gcs-tiktok-posts.json', 'size': 67890}
        )
        
        # Verify result
        assert result['success'] is True
        
        # Print stats
        stats = result['stats']
        print(f"📊 TIKTOK STATS:")
        print(f"  Total posts: {stats['total_posts']}")
        print(f"  Posts with media: {stats['posts_with_media']}")
        print(f"  Total media items: {stats['total_media_items']}")
        print(f"  Videos: {stats['total_videos']}")
        print(f"  Images: {stats['total_images']}")
        
        # Get the actual event structure
        detector = MultiPlatformMediaDetector()
        batch_result = detector.detect_media_batch(tiktok_fixture_data, 'tiktok')
        event = publisher._create_batch_event(batch_result, crawl_metadata, {'filename': 'gcs-tiktok-posts.json'})
        
        # Print TikTok-specific media structure
        media_by_type = event['data']['media_by_type']
        print(f"\n🎬 TIKTOK MEDIA STRUCTURE:")
        
        # TikTok videos
        videos = media_by_type.get('videos', [])
        if videos:
            print(f"  Videos ({len(videos)}):")
            for i, video in enumerate(videos[:3]):
                print(f"    {i+1}. URL: {video['url'][:50]}...")
                print(f"        Duration: {video.get('duration', 'N/A')}s")
                print(f"        Post ID: {video.get('post_id', 'N/A')}")
        
        # TikTok cover images
        images = media_by_type.get('images', [])
        if images:
            print(f"  Cover Images ({len(images)}):")
            for i, image in enumerate(images[:3]):
                print(f"    {i+1}. URL: {image['url'][:50]}...")
                print(f"        Type: {image.get('type', 'N/A')}")
                print(f"        Post ID: {image.get('post_id', 'N/A')}")
        
        # Storage config for TikTok
        storage_config = event['data']['storage_config']
        print(f"\n📁 STORAGE CONFIG:")
        print(f"  Base Path: {storage_config['base_path']}")
        print(f"  Video Format Preference: {storage_config['video_format_preference']}")
        
        assert event['data']['batch_summary']['platform'] == 'tiktok'
        assert len(videos) > 0  # TikTok should have videos
        assert len(images) > 0  # TikTok should have cover images
        
        return event
    
    def test_youtube_batch_event_structure(self, publisher, crawl_metadata, youtube_fixture_data):
        """Test the structure of YouTube batch media events."""
        print("\n" + "="*80)
        print("YOUTUBE BATCH MEDIA EVENT STRUCTURE")
        print("="*80)
        
        # Update metadata for YouTube
        crawl_metadata['platform'] = 'youtube'
        
        # Publish batch event
        result = publisher.publish_batch_from_raw_file(
            raw_posts=youtube_fixture_data,
            platform='youtube',
            crawl_metadata=crawl_metadata,
            file_metadata={'filename': 'gcs-youtube-posts.json', 'size': 54321}
        )
        
        # Verify result
        assert result['success'] is True
        
        # Print stats
        stats = result['stats']
        print(f"📊 YOUTUBE STATS:")
        print(f"  Total posts: {stats['total_posts']}")
        print(f"  Posts with media: {stats['posts_with_media']}")
        print(f"  Total media items: {stats['total_media_items']}")
        print(f"  Videos: {stats['total_videos']}")
        print(f"  Images: {stats['total_images']}")
        
        # Get the actual event structure
        detector = MultiPlatformMediaDetector()
        batch_result = detector.detect_media_batch(youtube_fixture_data, 'youtube')
        event = publisher._create_batch_event(batch_result, crawl_metadata, {'filename': 'gcs-youtube-posts.json'})
        
        # Print YouTube-specific media structure
        media_by_type = event['data']['media_by_type']
        print(f"\n🎬 YOUTUBE MEDIA STRUCTURE:")
        
        # YouTube videos
        videos = media_by_type.get('videos', [])
        if videos:
            print(f"  Videos ({len(videos)}):")
            for i, video in enumerate(videos[:3]):
                print(f"    {i+1}. URL: {video['url']}")
                print(f"        Duration: {video.get('duration', 'N/A')}s")
                print(f"        Video ID: {video.get('video_id', 'N/A')}")
        
        # YouTube thumbnails
        images = media_by_type.get('images', [])
        if images:
            print(f"  Thumbnails ({len(images)}):")
            for i, image in enumerate(images[:3]):
                print(f"    {i+1}. URL: {image['url']}")
                print(f"        Type: {image.get('type', 'N/A')}")
                print(f"        Video ID: {image.get('video_id', 'N/A')}")
        
        # Priority calculation
        processing_config = event['data']['processing_config']
        print(f"\n⚙️  PROCESSING PRIORITY:")
        print(f"  Priority: {processing_config['priority']}")
        print(f"  Reason: {len(videos)} videos out of {stats['total_media_items']} media items")
        
        assert event['data']['batch_summary']['platform'] == 'youtube'
        assert len(videos) > 0  # YouTube should have videos
        assert len(images) > 0  # YouTube should have thumbnails
        
        return event
    
    def test_batch_event_size_and_performance(self, publisher, crawl_metadata, facebook_fixture_data, tiktok_fixture_data, youtube_fixture_data):
        """Test the size and performance characteristics of batch events."""
        print("\n" + "="*80)
        print("BATCH EVENT SIZE AND PERFORMANCE ANALYSIS")
        print("="*80)
        
        platforms_data = {
            'facebook': facebook_fixture_data,
            'tiktok': tiktok_fixture_data,
            'youtube': youtube_fixture_data
        }
        
        results = {}
        total_size = 0
        
        for platform, data in platforms_data.items():
            # Update metadata
            crawl_metadata['platform'] = platform
            
            # Create event
            detector = MultiPlatformMediaDetector()
            batch_result = detector.detect_media_batch(data, platform)
            event = publisher._create_batch_event(batch_result, crawl_metadata, {'filename': f'gcs-{platform}-posts.json'})
            
            # Calculate size
            event_json = json.dumps(event)
            event_size = len(event_json)
            
            results[platform] = {
                'posts': len(data),
                'media_items': batch_result['total_media_items'],
                'event_size_bytes': event_size,
                'event_size_kb': round(event_size / 1024, 2),
                'avg_bytes_per_media': round(event_size / max(1, batch_result['total_media_items']), 2)
            }
            
            total_size += event_size
            
            print(f"{platform.upper()}:")
            print(f"  Posts: {results[platform]['posts']}")
            print(f"  Media Items: {results[platform]['media_items']}")
            print(f"  Event Size: {results[platform]['event_size_kb']} KB")
            print(f"  Bytes per Media Item: {results[platform]['avg_bytes_per_media']}")
            print()
        
        print(f"TOTAL EVENT SIZE: {round(total_size / 1024, 2)} KB")
        print(f"AVERAGE EVENT SIZE: {round(total_size / len(platforms_data) / 1024, 2)} KB")
        
        # Verify reasonable event sizes (should be manageable for Pub/Sub)
        for platform, result in results.items():
            assert result['event_size_kb'] < 100, f"{platform} event too large: {result['event_size_kb']} KB"
            assert result['avg_bytes_per_media'] < 1000, f"{platform} too many bytes per media item"
        
        return results
    
    def test_batch_event_validation(self, publisher, crawl_metadata, facebook_fixture_data):
        """Test that batch events contain all required fields."""
        print("\n" + "="*80)
        print("BATCH EVENT VALIDATION")
        print("="*80)
        
        # Create event
        detector = MultiPlatformMediaDetector()
        batch_result = detector.detect_media_batch(facebook_fixture_data, 'facebook')
        event = publisher._create_batch_event(batch_result, crawl_metadata, {'filename': 'test.json'})
        
        # Required top-level fields
        required_fields = ['event_type', 'event_id', 'timestamp', 'version', 'schema_version', 'data']
        for field in required_fields:
            assert field in event, f"Missing required field: {field}"
            print(f"✅ {field}: {event[field]}")
        
        # Required data fields
        data = event['data']
        required_data_fields = ['batch_summary', 'media_by_type', 'crawl_metadata', 'processing_config', 'storage_config']
        for field in required_data_fields:
            assert field in data, f"Missing required data field: {field}"
            print(f"✅ data.{field}: Present")
        
        # Batch summary validation
        batch_summary = data['batch_summary']
        required_summary_fields = ['platform', 'total_posts', 'posts_with_media', 'total_media_items', 'media_counts']
        for field in required_summary_fields:
            assert field in batch_summary, f"Missing batch summary field: {field}"
        
        # Media types validation
        media_by_type = data['media_by_type']
        required_media_types = ['videos', 'images', 'profile_images']
        for media_type in required_media_types:
            assert media_type in media_by_type, f"Missing media type: {media_type}"
            assert isinstance(media_by_type[media_type], list), f"Media type {media_type} should be a list"
        
        # Processing config validation
        processing_config = data['processing_config']
        required_processing_fields = ['priority', 'max_retries', 'timeout_seconds', 'parallel_downloads']
        for field in required_processing_fields:
            assert field in processing_config, f"Missing processing config field: {field}"
        
        print("\n✅ All validation checks passed!")
        return True
    
    def test_no_media_handling(self, publisher, crawl_metadata):
        """Test handling when posts have no media."""
        print("\n" + "="*80)
        print("NO MEDIA HANDLING TEST")
        print("="*80)
        
        # Create posts with no media
        posts_without_media = [
            {"id": "1", "message": "Text only post 1"},
            {"id": "2", "message": "Text only post 2"},
            {"id": "3", "message": "Text only post 3"}
        ]
        
        result = publisher.publish_batch_from_raw_file(
            raw_posts=posts_without_media,
            platform='facebook',
            crawl_metadata=crawl_metadata,
            file_metadata={'filename': 'no-media.json'}
        )
        
        print(f"📊 NO MEDIA RESULT:")
        print(f"  Success: {result['success']}")
        print(f"  Message: {result['message']}")
        print(f"  Stats: {result.get('stats', {})}")
        
        # Verify no media handling
        assert result['success'] is True
        assert 'No media' in result['message']
        assert result['stats']['total_media_items'] == 0
        assert result['stats']['total_videos'] == 0
        assert result['stats']['total_images'] == 0
        
        print("\n✅ No media handling works correctly!")
        return result


if __name__ == "__main__":
    # Run with pytest for detailed output
    pytest.main([__file__, "-v", "-s"])