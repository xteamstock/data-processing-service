"""
Unit tests for MediaEventPublisher - multi-platform media processing event publishing.

Tests the MediaEventPublisher's ability to publish Pub/Sub events for media
processing requests across different social media platforms (Facebook, TikTok, YouTube).
"""

import unittest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from events.media_event_publisher import MediaEventPublisher, publish_media_processing_events


class TestMediaEventPublisher(unittest.TestCase):
    """Test MediaEventPublisher functionality for multi-platform media processing."""
    
    def setUp(self):
        """Set up test fixtures and mocks."""
        # Mock environment variables
        self.test_project_id = "test-social-analytics"
        self.test_topic_name = "test-media-processing-requests"
        
        # Mock Pub/Sub client
        self.mock_publisher = Mock()
        self.mock_topic_path = f"projects/{self.test_project_id}/topics/{self.test_topic_name}"
        
        # Load test fixtures for all platforms
        fixture_dir = Path(__file__).parent.parent.parent / "fixtures"
        
        with open(fixture_dir / "gcs-facebook-posts.json", 'r', encoding='utf-8') as f:
            self.facebook_posts = json.load(f)
        
        with open(fixture_dir / "gcs-tiktok-posts.json", 'r', encoding='utf-8') as f:
            self.tiktok_posts = json.load(f)
        
        with open(fixture_dir / "gcs-youtube-posts.json", 'r', encoding='utf-8') as f:
            self.youtube_posts = json.load(f)
        
        # Test crawl metadata
        self.test_metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em'
        }
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    @patch.dict(os.environ, {'GOOGLE_CLOUD_PROJECT': 'test-social-analytics'})
    def test_publisher_initialization(self, mock_publisher_class):
        """Test MediaEventPublisher initialization."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        publisher = MediaEventPublisher()
        
        # Verify initialization
        self.assertEqual(publisher.project_id, 'test-social-analytics')
        self.assertEqual(publisher.topic_name, 'media-processing-requests')
        mock_publisher_class.assert_called_once()
        mock_client.topic_path.assert_called_once_with('test-social-analytics', 'media-processing-requests')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publisher_initialization_with_params(self, mock_publisher_class):
        """Test MediaEventPublisher initialization with custom parameters."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(
            project_id="custom-project",
            topic_name="custom-topic"
        )
        
        # Verify custom parameters
        self.assertEqual(publisher.project_id, 'custom-project')
        self.assertEqual(publisher.topic_name, 'custom-topic')
        mock_client.topic_path.assert_called_once_with('custom-project', 'custom-topic')
    
    def test_publisher_initialization_no_project_id(self):
        """Test MediaEventPublisher initialization fails without project ID."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as context:
                MediaEventPublisher()
            
            self.assertIn("Google Cloud project ID must be provided", str(context.exception))
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_facebook_media_extraction(self, mock_publisher_class):
        """Test extraction of Facebook media URLs."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Use Facebook post with video attachment from fixture
        facebook_post = self.facebook_posts[0]
        
        media_urls = publisher._extract_facebook_media_urls(facebook_post)
        
        # Verify video attachment extraction
        self.assertGreater(len(media_urls), 0)
        
        # Check for video media
        video_media = [m for m in media_urls if m['type'] == 'video']
        self.assertGreater(len(video_media), 0)
        
        # Verify video media structure
        video = video_media[0]
        self.assertIn('url', video)
        self.assertIn('type', video)
        self.assertEqual(video['type'], 'video')
        self.assertIn('attachment_id', video)
        
        # Check for additional Facebook media (page logo, post image)
        if 'page_logo' in facebook_post:
            profile_media = [m for m in media_urls if m['type'] == 'profile_image']
            self.assertGreater(len(profile_media), 0)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_tiktok_media_extraction(self, mock_publisher_class):
        """Test extraction of TikTok media URLs."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Use TikTok post from fixture
        tiktok_post = self.tiktok_posts[0]
        
        media_urls = publisher._extract_tiktok_media_urls(tiktok_post)
        
        # Verify TikTok media extraction
        self.assertGreater(len(media_urls), 0)
        
        # Check for video media
        video_media = [m for m in media_urls if m['type'] == 'video']
        self.assertEqual(len(video_media), 1)
        
        video = video_media[0]
        self.assertEqual(video['url'], tiktok_post['webVideoUrl'])
        self.assertEqual(video['video_id'], tiktok_post['id'])
        self.assertTrue(video['platform_specific'])
        
        # Check for thumbnail media
        thumbnail_media = [m for m in media_urls if m['type'] == 'thumbnail']
        if tiktok_post.get('videoMeta', {}).get('coverUrl'):
            self.assertGreater(len(thumbnail_media), 0)
            thumbnail = thumbnail_media[0]
            self.assertEqual(thumbnail['url'], tiktok_post['videoMeta']['coverUrl'])
        
        # Check for author avatar
        avatar_media = [m for m in media_urls if m['type'] == 'profile_image']
        if tiktok_post.get('authorMeta', {}).get('avatar'):
            self.assertGreater(len(avatar_media), 0)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_youtube_media_extraction(self, mock_publisher_class):
        """Test extraction of YouTube media URLs."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Use YouTube post from fixture
        youtube_post = self.youtube_posts[0]
        
        media_urls = publisher._extract_youtube_media_urls(youtube_post)
        
        # Verify YouTube media extraction (thumbnails and channel images only)
        self.assertGreater(len(media_urls), 0)
        
        # Check for thumbnail media
        thumbnail_media = [m for m in media_urls if m['type'] == 'thumbnail']
        if youtube_post.get('thumbnailUrl'):
            self.assertGreater(len(thumbnail_media), 0)
            thumbnail = thumbnail_media[0]
            self.assertEqual(thumbnail['url'], youtube_post['thumbnailUrl'])
            self.assertEqual(thumbnail['video_id'], youtube_post['id'])
            self.assertTrue(thumbnail['platform_specific'])
        
        # Check for channel avatar
        avatar_media = [m for m in media_urls if m['type'] == 'profile_image']
        if youtube_post.get('channelAvatarUrl'):
            self.assertGreater(len(avatar_media), 0)
        
        # Check for channel banner
        banner_media = [m for m in media_urls if m['type'] == 'banner_image']
        if youtube_post.get('channelBannerUrl'):
            self.assertGreater(len(banner_media), 0)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_event_creation_structure(self, mock_publisher_class):
        """Test structure of created media processing events."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Create test media URL
        test_media_url = {
            'url': 'https://example.com/video.mp4',
            'type': 'video',
            'attachment_id': 'test_video_123'
        }
        
        test_post_data = {
            'id': 'test_post_456',
            'url': 'https://facebook.com/post/456'
        }
        
        event = publisher._create_media_event(
            test_media_url, 'facebook', test_post_data, self.test_metadata
        )
        
        # Verify event structure
        self.assertEqual(event['event_type'], 'media-download-requested')
        self.assertEqual(event['version'], '1.0')
        self.assertIn('timestamp', event)
        self.assertIn('event_id', event)
        self.assertIn('data', event)
        
        # Verify event data
        data = event['data']
        self.assertEqual(data['media_url'], 'https://example.com/video.mp4')
        self.assertEqual(data['media_type'], 'video')
        self.assertEqual(data['media_id'], 'test_video_123')
        self.assertEqual(data['platform'], 'facebook')
        self.assertEqual(data['post_id'], 'test_post_456')
        self.assertEqual(data['crawl_id'], 'test_crawl_123')
        self.assertEqual(data['competitor'], 'nutifood')
        self.assertEqual(data['brand'], 'growplus-nutifood')
        self.assertEqual(data['category'], 'sua-bot-tre-em')
        
        # Verify storage configuration
        self.assertIn('storage_path', data)
        self.assertIn('bucket_name', data)
        
        # Verify processing configuration
        self.assertEqual(data['retry_count'], 0)
        self.assertIn('max_retries', data)
        self.assertIn('timeout_seconds', data)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_storage_path_generation(self, mock_publisher_class):
        """Test generation of GCS storage paths for media."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_media_url = {
            'url': 'https://example.com/video.mp4',
            'type': 'video',
            'attachment_id': 'test_video_123'
        }
        
        test_post_data = {
            'id': 'test_post_456',
            'date_posted': '2024-12-24T13:30:14.000Z'
        }
        
        storage_path = publisher._generate_media_storage_path(
            test_media_url, 'facebook', self.test_metadata, test_post_data
        )
        
        # Verify hierarchical path structure
        expected_parts = [
            'media/facebook/',
            'competitor=nutifood/',
            'brand=growplus-nutifood/',
            'category=sua-bot-tre-em/',
            'year=2024/month=12/day=24/',
            'videos/',
            'test_video_123.mp4'
        ]
        
        for part in expected_parts:
            self.assertIn(part, storage_path)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_file_extension_detection(self, mock_publisher_class):
        """Test file extension detection from URLs and media types."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_cases = [
            ('https://example.com/video.mp4', 'video', '.mp4'),
            ('https://example.com/image.jpg', 'image', '.jpg'),
            ('https://example.com/photo.jpeg', 'image', '.jpg'),
            ('https://example.com/picture.png', 'image', '.png'),
            ('https://example.com/animation.gif', 'image', '.gif'),
            ('https://example.com/modern.webp', 'image', '.webp'),
            ('https://example.com/unknown', 'video', '.mp4'),  # Fallback for video
            ('https://example.com/unknown', 'image', '.jpg'),  # Fallback for image
        ]
        
        for url, media_type, expected_ext in test_cases:
            with self.subTest(url=url, media_type=media_type):
                result = publisher._get_file_extension(url, media_type)
                self.assertEqual(result, expected_ext)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_event_id_generation(self, mock_publisher_class):
        """Test generation of unique event IDs."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_media_url = {
            'url': 'https://example.com/video.mp4',
            'type': 'video',
            'attachment_id': 'test_video_123'
        }
        
        test_post_data = {'id': 'test_post_456'}
        
        event_id = publisher._generate_event_id(
            test_media_url, self.test_metadata, test_post_data
        )
        
        # Verify event ID format
        expected_id = "test_crawl_123_test_post_456_test_video_123_video"
        self.assertEqual(event_id, expected_id)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_platform_specific_post_url_extraction(self, mock_publisher_class):
        """Test platform-specific post URL extraction."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_cases = [
            ('facebook', {'url': 'https://facebook.com/post/123'}, 'https://facebook.com/post/123'),
            ('tiktok', {'webVideoUrl': 'https://tiktok.com/video/456'}, 'https://tiktok.com/video/456'),
            ('youtube', {'url': 'https://youtube.com/watch?v=789'}, 'https://youtube.com/watch?v=789'),
            ('unknown', {'url': 'https://example.com/post'}, 'https://example.com/post'),
        ]
        
        for platform, post_data, expected_url in test_cases:
            with self.subTest(platform=platform):
                result = publisher._get_post_url(post_data, platform)
                self.assertEqual(result, expected_url)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publish_media_events_facebook(self, mock_publisher_class):
        """Test publishing media events for Facebook posts."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish
        mock_future = Mock()
        mock_client.publish.return_value = mock_future
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use Facebook post with media
        facebook_post = self.facebook_posts[0]
        
        published_count = publisher.publish_media_events(
            facebook_post, 'facebook', self.test_metadata
        )
        
        # Verify events were published
        self.assertGreater(published_count, 0)
        self.assertTrue(mock_client.publish.called)
        
        # Verify publish call parameters
        call_args = mock_client.publish.call_args
        self.assertEqual(call_args[0][0], self.mock_topic_path)  # topic_path
        
        # Verify message attributes
        attributes = call_args[1]
        self.assertEqual(attributes['platform'], 'facebook')
        self.assertEqual(attributes['crawl_id'], 'test_crawl_123')
        self.assertEqual(attributes['competitor'], 'nutifood')
        self.assertEqual(attributes['brand'], 'growplus-nutifood')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publish_media_events_tiktok(self, mock_publisher_class):
        """Test publishing media events for TikTok posts."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish
        mock_future = Mock()
        mock_client.publish.return_value = mock_future
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use TikTok post with media
        tiktok_post = self.tiktok_posts[0]
        
        published_count = publisher.publish_media_events(
            tiktok_post, 'tiktok', self.test_metadata
        )
        
        # Verify events were published
        self.assertGreater(published_count, 0)
        self.assertTrue(mock_client.publish.called)
        
        # Verify platform in attributes
        call_args = mock_client.publish.call_args
        attributes = call_args[1]
        self.assertEqual(attributes['platform'], 'tiktok')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publish_media_events_youtube(self, mock_publisher_class):
        """Test publishing media events for YouTube posts."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish
        mock_future = Mock()
        mock_client.publish.return_value = mock_future
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use YouTube post with media
        youtube_post = self.youtube_posts[0]
        
        published_count = publisher.publish_media_events(
            youtube_post, 'youtube', self.test_metadata
        )
        
        # Verify events were published (thumbnails and channel images)
        self.assertGreater(published_count, 0)
        self.assertTrue(mock_client.publish.called)
        
        # Verify platform in attributes
        call_args = mock_client.publish.call_args
        attributes = call_args[1]
        self.assertEqual(attributes['platform'], 'youtube')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publish_no_media_post(self, mock_publisher_class):
        """Test publishing events for posts without media."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Create post without media
        no_media_post = {
            'id': 'text_only_post',
            'content': 'This is a text-only post'
        }
        
        published_count = publisher.publish_media_events(
            no_media_post, 'facebook', self.test_metadata
        )
        
        # Should not publish any events
        self.assertEqual(published_count, 0)
        self.assertFalse(mock_client.publish.called)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publish_batch_media_events(self, mock_publisher_class):
        """Test batch publishing of media events."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish
        mock_future = Mock()
        mock_client.publish.return_value = mock_future
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use multiple posts from different platforms
        posts_batch = [
            self.facebook_posts[0],  # Has media
            self.tiktok_posts[0],    # Has media
            {'id': 'text_only', 'content': 'No media'}  # No media
        ]
        
        total_published = publisher.publish_batch_media_events(
            posts_batch, 'facebook', self.test_metadata
        )
        
        # Should publish events for posts with media
        self.assertGreater(total_published, 0)
        self.assertTrue(mock_client.publish.called)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_error_handling_during_publish(self, mock_publisher_class):
        """Test error handling during event publishing."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock publish failure
        mock_client.publish.side_effect = Exception("Pub/Sub error")
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use Facebook post with media
        facebook_post = self.facebook_posts[0]
        
        # Should handle error gracefully
        published_count = publisher.publish_media_events(
            facebook_post, 'facebook', self.test_metadata
        )
        
        # Should return 0 due to error
        self.assertEqual(published_count, 0)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_unknown_platform_handling(self, mock_publisher_class):
        """Test handling of unknown platforms."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_post = {
            'id': 'test_post',
            'attachments': [{'id': 'att1', 'type': 'video', 'url': 'https://example.com/video.mp4'}]
        }
        
        published_count = publisher.publish_media_events(
            test_post, 'unknown_platform', self.test_metadata
        )
        
        # Should return 0 for unknown platform
        self.assertEqual(published_count, 0)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_environment_variable_configuration(self, mock_publisher_class):
        """Test configuration through environment variables."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        test_env_vars = {
            'MEDIA_STORAGE_BUCKET': 'custom-media-bucket',
            'MEDIA_MAX_RETRIES': '5',
            'MEDIA_DOWNLOAD_TIMEOUT': '120',
            'MEDIA_MAX_FILE_SIZE': '200MB'
        }
        
        with patch.dict(os.environ, test_env_vars):
            publisher = MediaEventPublisher(project_id=self.test_project_id)
            
            test_media_url = {
                'url': 'https://example.com/video.mp4',
                'type': 'video',
                'attachment_id': 'test_video'
            }
            
            test_post_data = {'id': 'test_post'}
            
            event = publisher._create_media_event(
                test_media_url, 'facebook', test_post_data, self.test_metadata
            )
            
            data = event['data']
            self.assertEqual(data['bucket_name'], 'custom-media-bucket')
            self.assertEqual(data['max_retries'], 5)
            self.assertEqual(data['timeout_seconds'], 120)
            self.assertEqual(data['file_size_limit'], '200MB')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_convenience_function(self, mock_publisher_class):
        """Test the convenience function for publishing media events."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish
        mock_future = Mock()
        mock_client.publish.return_value = mock_future
        
        posts = [self.facebook_posts[0]]
        
        # Use convenience function
        published_count = publish_media_processing_events(
            posts, 'facebook', self.test_metadata, self.test_project_id
        )
        
        # Should create publisher and publish events
        self.assertGreater(published_count, 0)
        mock_publisher_class.assert_called_once()
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_publisher_close(self, mock_publisher_class):
        """Test publisher close functionality."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Should not raise exception
        publisher.close()
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_malformed_media_data_handling(self, mock_publisher_class):
        """Test handling of malformed media data."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Test malformed Facebook attachments
        malformed_facebook_post = {
            'id': 'malformed_post',
            'attachments': [
                {},  # Empty attachment
                {'type': 'video'},  # Missing URL
                {'url': 'https://example.com/video.mp4'},  # Missing type
                None,  # Null attachment
                'invalid_attachment'  # String instead of dict
            ]
        }
        
        media_urls = publisher._extract_facebook_media_urls(malformed_facebook_post)
        
        # Should handle malformed data gracefully
        self.assertIsInstance(media_urls, list)
        # Only valid attachments should be extracted
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_timestamp_handling_edge_cases(self, mock_publisher_class):
        """Test handling of various timestamp formats."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        test_media_url = {
            'url': 'https://example.com/video.mp4',
            'type': 'video',
            'attachment_id': 'test_video'
        }
        
        # Test various timestamp formats
        timestamp_test_cases = [
            '2024-12-24T13:30:14.000Z',  # ISO with Z
            '2024-12-24T13:30:14+00:00',  # ISO with timezone
            '2024-12-24T13:30:14',  # ISO without timezone
            'invalid_timestamp',  # Invalid format
            None,  # None value
            ''  # Empty string
        ]
        
        for timestamp in timestamp_test_cases:
            with self.subTest(timestamp=timestamp):
                test_post_data = {
                    'id': 'test_post',
                    'date_posted': timestamp
                }
                
                # Should not raise exception
                storage_path = publisher._generate_media_storage_path(
                    test_media_url, 'facebook', self.test_metadata, test_post_data
                )
                
                # Should generate valid path
                self.assertIsInstance(storage_path, str)
                self.assertIn('media/facebook/', storage_path)
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_media_priority_handling(self, mock_publisher_class):
        """Test handling of media priority in events."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        
        # Test media with priority
        high_priority_media = {
            'url': 'https://example.com/important_video.mp4',
            'type': 'video',
            'attachment_id': 'important_video',
            'priority': 'high'
        }
        
        test_post_data = {'id': 'test_post'}
        
        event = publisher._create_media_event(
            high_priority_media, 'facebook', test_post_data, self.test_metadata
        )
        
        # Verify priority is preserved
        self.assertEqual(event['data']['priority'], 'high')
        
        # Test default priority
        normal_media = {
            'url': 'https://example.com/normal_video.mp4',
            'type': 'video',
            'attachment_id': 'normal_video'
        }
        
        event = publisher._create_media_event(
            normal_media, 'facebook', test_post_data, self.test_metadata
        )
        
        # Should default to 'normal'
        self.assertEqual(event['data']['priority'], 'normal')
    
    @patch('events.media_event_publisher.pubsub_v1.PublisherClient')
    def test_message_serialization(self, mock_publisher_class):
        """Test JSON serialization of event messages."""
        mock_client = Mock()
        mock_publisher_class.return_value = mock_client
        mock_client.topic_path.return_value = self.mock_topic_path
        
        # Mock successful publish to capture message
        published_messages = []
        def capture_publish(topic_path, message_data, **attributes):
            published_messages.append({
                'topic_path': topic_path,
                'message_data': message_data,
                'attributes': attributes
            })
            return Mock()
        
        mock_client.publish.side_effect = capture_publish
        
        publisher = MediaEventPublisher(project_id=self.test_project_id)
        publisher.publisher = mock_client
        
        # Use Facebook post with media
        facebook_post = self.facebook_posts[0]
        
        published_count = publisher.publish_media_events(
            facebook_post, 'facebook', self.test_metadata
        )
        
        # Verify messages were captured
        self.assertGreater(len(published_messages), 0)
        
        # Verify message serialization
        message = published_messages[0]
        message_data = message['message_data']
        
        # Should be valid JSON bytes
        self.assertIsInstance(message_data, bytes)
        
        # Should deserialize correctly
        event_data = json.loads(message_data.decode('utf-8'))
        self.assertIn('event_type', event_data)
        self.assertIn('timestamp', event_data)
        self.assertIn('data', event_data)


if __name__ == '__main__':
    unittest.main()