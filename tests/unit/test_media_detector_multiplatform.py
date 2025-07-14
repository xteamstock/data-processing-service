"""
Unit tests for multi-platform media detection functionality.

Tests the MediaDetector's ability to detect and analyze media attachments
across different social media platforms (Facebook, TikTok, YouTube).
"""

import unittest
import json
from pathlib import Path
from typing import Dict, List, Any

from handlers.media_detector import MediaDetector


class TestMediaDetectorMultiPlatform(unittest.TestCase):
    """Test multi-platform media detection functionality."""
    
    def setUp(self):
        """Set up test fixtures and media detector."""
        self.detector = MediaDetector()
        
        # Load test fixtures for all platforms
        fixture_dir = Path(__file__).parent.parent.parent / "fixtures"
        
        with open(fixture_dir / "gcs-facebook-posts.json", 'r', encoding='utf-8') as f:
            self.facebook_posts = json.load(f)
        
        with open(fixture_dir / "gcs-tiktok-posts.json", 'r', encoding='utf-8') as f:
            self.tiktok_posts = json.load(f)
        
        with open(fixture_dir / "gcs-youtube-posts.json", 'r', encoding='utf-8') as f:
            self.youtube_posts = json.load(f)
    
    def test_facebook_media_detection(self):
        """Test media detection in Facebook posts."""
        facebook_post = self.facebook_posts[0]  # This post has video attachment
        
        media_metadata = self.detector.detect_media_in_post(facebook_post)
        
        # Verify basic media detection
        self.assertGreater(media_metadata['media_count'], 0)
        self.assertTrue(media_metadata['has_video'])
        self.assertTrue(media_metadata['media_processing_requested'])
        
        # Verify attachments structure
        attachments = media_metadata['attachments']
        self.assertIsInstance(attachments, list)
        self.assertGreater(len(attachments), 0)
        
        # Check video attachment
        video_attachment = attachments[0]
        self.assertEqual(video_attachment['type'], 'Video')
        self.assertIsNotNone(video_attachment['id'])
        self.assertIsNotNone(video_attachment['url'])
    
    def test_facebook_image_detection(self):
        """Test image detection in Facebook posts."""
        facebook_post = self.facebook_posts[1]  # This post has image attachment
        
        media_metadata = self.detector.detect_media_in_post(facebook_post)
        
        # Verify image detection
        self.assertGreater(media_metadata['media_count'], 0)
        self.assertTrue(media_metadata['has_image'])
        self.assertIsNotNone(media_metadata['primary_image_url'])
        
        # Check image attachment
        attachments = media_metadata['attachments']
        image_attachment = next((att for att in attachments if att['type'] == 'Photo'), None)
        self.assertIsNotNone(image_attachment)
        self.assertIsNotNone(image_attachment['url'])
    
    def test_tiktok_media_detection(self):
        """Test media detection in TikTok posts."""
        # Create TikTok-style post data based on the fixture structure
        tiktok_post = {
            'id': self.tiktok_posts[0]['id'],
            'webVideoUrl': self.tiktok_posts[0]['webVideoUrl'],
            'videoMeta': self.tiktok_posts[0]['videoMeta'],
            'attachments': [
                {
                    'id': self.tiktok_posts[0]['id'],
                    'type': 'video',
                    'url': self.tiktok_posts[0]['videoMeta']['coverUrl'],
                    'video_url': self.tiktok_posts[0]['webVideoUrl'],
                    'video_length': self.tiktok_posts[0]['videoMeta']['duration']
                }
            ]
        }
        
        media_metadata = self.detector.detect_media_in_post(tiktok_post)
        
        # Verify TikTok video detection
        self.assertGreater(media_metadata['media_count'], 0)
        self.assertTrue(media_metadata['has_video'])
        self.assertTrue(media_metadata['media_processing_requested'])
        
        # Check video attachment
        attachments = media_metadata['attachments']
        video_attachment = attachments[0]
        self.assertEqual(video_attachment['type'], 'Video')
        self.assertIn('tiktok', video_attachment['url'])
    
    def test_youtube_media_detection(self):
        """Test media detection in YouTube posts."""
        # Create YouTube-style post data based on the fixture structure  
        youtube_post = {
            'id': self.youtube_posts[0]['id'],
            'url': self.youtube_posts[0]['url'],
            'thumbnailUrl': self.youtube_posts[0]['thumbnailUrl'],
            'attachments': [
                {
                    'id': self.youtube_posts[0]['id'],
                    'type': 'video',
                    'url': self.youtube_posts[0]['thumbnailUrl'],
                    'video_url': self.youtube_posts[0]['url'],
                    'attachment_url': self.youtube_posts[0]['url']
                }
            ]
        }
        
        media_metadata = self.detector.detect_media_in_post(youtube_post)
        
        # Verify YouTube video detection
        self.assertGreater(media_metadata['media_count'], 0)
        self.assertTrue(media_metadata['has_video'])
        self.assertTrue(media_metadata['media_processing_requested'])
        
        # Check video attachment
        attachments = media_metadata['attachments']
        video_attachment = attachments[0]
        self.assertEqual(video_attachment['type'], 'Video')
        self.assertIn('youtube', video_attachment['url'])
    
    def test_platform_specific_media_urls(self):
        """Test detection of platform-specific media URL patterns."""
        test_cases = [
            # Facebook URLs
            {
                'url': 'https://scontent-vie1-1.xx.fbcdn.net/v/photo.jpg',
                'expected_is_image': True,
                'platform': 'Facebook'
            },
            {
                'url': 'https://video-vie1-1.xx.fbcdn.net/o1/v/video.mp4',
                'expected_is_image': False,
                'platform': 'Facebook'
            },
            # TikTok URLs
            {
                'url': 'https://p16-sign-sg.tiktokcdn.com/cover.image',
                'expected_is_image': True,
                'platform': 'TikTok'
            },
            {
                'url': 'https://www.tiktok.com/@user/video/123',
                'expected_is_image': False,
                'platform': 'TikTok'
            },
            # YouTube URLs
            {
                'url': 'https://i.ytimg.com/vi/video_id/hqdefault.jpg',
                'expected_is_image': True,
                'platform': 'YouTube'
            },
            {
                'url': 'https://www.youtube.com/watch?v=video_id',
                'expected_is_image': False,
                'platform': 'YouTube'
            }
        ]
        
        for test_case in test_cases:
            with self.subTest(platform=test_case['platform'], url=test_case['url']):
                result = self.detector._is_image_url(test_case['url'])
                self.assertEqual(result, test_case['expected_is_image'])
    
    def test_mixed_media_post(self):
        """Test detection in posts with both videos and images."""
        mixed_post = {
            'post_id': 'test_mixed_123',
            'attachments': [
                {
                    'id': 'video_1',
                    'type': 'video',
                    'url': 'https://example.com/video1.mp4',
                    'video_url': 'https://example.com/video1.mp4',
                    'video_length': '120'
                },
                {
                    'id': 'image_1',
                    'type': 'photo',
                    'url': 'https://example.com/image1.jpg',
                    'attachment_url': 'https://example.com/post/image1'
                },
                {
                    'id': 'image_2',
                    'type': 'image',
                    'url': 'https://example.com/image2.png',
                    'attachment_url': 'https://example.com/post/image2'
                }
            ]
        }
        
        media_metadata = self.detector.detect_media_in_post(mixed_post)
        
        # Verify mixed media detection
        self.assertEqual(media_metadata['media_count'], 3)
        self.assertTrue(media_metadata['has_video'])
        self.assertTrue(media_metadata['has_image'])
        self.assertTrue(media_metadata['media_processing_requested'])
        
        # Verify primary image URL is set
        self.assertIsNotNone(media_metadata['primary_image_url'])
        self.assertIn('image1.jpg', media_metadata['primary_image_url'])
        
        # Verify all attachments are present
        attachments = media_metadata['attachments']
        self.assertEqual(len(attachments), 3)
        
        # Check attachment types
        video_attachments = [att for att in attachments if att['type'] == 'Video']
        image_attachments = [att for att in attachments if att['type'] == 'Photo']
        
        self.assertEqual(len(video_attachments), 1)
        self.assertEqual(len(image_attachments), 2)
    
    def test_no_media_post(self):
        """Test detection in posts without media attachments."""
        no_media_post = {
            'post_id': 'test_text_only',
            'content': 'This is a text-only post',
            'attachments': []
        }
        
        media_metadata = self.detector.detect_media_in_post(no_media_post)
        
        # Verify no media detected
        self.assertEqual(media_metadata['media_count'], 0)
        self.assertFalse(media_metadata['has_video'])
        self.assertFalse(media_metadata['has_image'])
        self.assertFalse(media_metadata['media_processing_requested'])
        self.assertIsNone(media_metadata['primary_image_url'])
        self.assertEqual(len(media_metadata['attachments']), 0)
    
    def test_malformed_attachments_handling(self):
        """Test handling of malformed or invalid attachment data."""
        malformed_post = {
            'post_id': 'test_malformed',
            'attachments': [
                # Valid attachment
                {
                    'id': 'valid_1',
                    'type': 'photo',
                    'url': 'https://example.com/valid.jpg'
                },
                # Missing required fields
                {
                    'type': 'video'
                    # Missing id and url
                },
                # Invalid type (not dict)
                "invalid_string_attachment",
                # Null attachment
                None,
                # Empty attachment
                {},
                # Valid video
                {
                    'id': 'valid_2',
                    'type': 'video',
                    'url': 'https://example.com/thumbnail.jpg',
                    'video_url': 'https://example.com/video.mp4'
                }
            ]
        }
        
        # Should not raise exception
        media_metadata = self.detector.detect_media_in_post(malformed_post)
        
        # Should detect only valid attachments
        self.assertEqual(media_metadata['media_count'], 2)  # 1 photo + 1 video
        self.assertTrue(media_metadata['has_video'])
        self.assertTrue(media_metadata['has_image'])
        
        # Should have 2 valid attachments in the output
        attachments = media_metadata['attachments']
        self.assertEqual(len(attachments), 2)
    
    def test_batch_media_detection(self):
        """Test media detection on multiple posts."""
        test_posts = [
            {
                'post_id': 'post_1',
                'attachments': [
                    {
                        'id': 'video_1',
                        'type': 'video',
                        'url': 'https://example.com/video1.mp4',
                        'video_url': 'https://example.com/video1.mp4'
                    }
                ]
            },
            {
                'post_id': 'post_2',
                'attachments': [
                    {
                        'id': 'image_1',
                        'type': 'photo',
                        'url': 'https://example.com/image1.jpg'
                    }
                ]
            },
            {
                'post_id': 'post_3',
                'attachments': []  # No media
            }
        ]
        
        enhanced_posts = self.detector.detect_media_in_posts(test_posts)
        
        # Verify all posts are processed
        self.assertEqual(len(enhanced_posts), 3)
        
        # Verify each post has media_metadata
        for post in enhanced_posts:
            self.assertIn('media_metadata', post)
            self.assertIsInstance(post['media_metadata'], dict)
        
        # Verify specific detections
        self.assertTrue(enhanced_posts[0]['media_metadata']['has_video'])
        self.assertTrue(enhanced_posts[1]['media_metadata']['has_image'])
        self.assertEqual(enhanced_posts[2]['media_metadata']['media_count'], 0)
    
    def test_media_processing_event_extraction(self):
        """Test extraction of media information for processing events."""
        posts_with_media = [
            {
                'post_id': 'post_1',
                'media_metadata': {
                    'media_count': 2,
                    'media_processing_requested': True,
                    'attachments': [
                        {
                            'id': 'video_1',
                            'type': 'video',
                            'url': 'https://example.com/video1.mp4',
                            'attachment_url': 'https://facebook.com/video/123'
                        },
                        {
                            'id': 'image_1',
                            'type': 'photo',
                            'url': 'https://example.com/image1.jpg',
                            'attachment_url': 'https://facebook.com/photo/456'
                        }
                    ]
                }
            },
            {
                'post_id': 'post_2',
                'media_metadata': {
                    'media_count': 1,
                    'media_processing_requested': True,
                    'attachments': [
                        {
                            'id': 'video_2',
                            'type': 'video',
                            'url': 'https://example.com/video2.mp4'
                        }
                    ]
                }
            }
        ]
        
        event_payload = self.detector.extract_media_for_processing_event(posts_with_media)
        
        # Verify totals
        self.assertEqual(event_payload['total_media_count'], 3)
        self.assertEqual(event_payload['video_count'], 2)
        self.assertEqual(event_payload['image_count'], 1)
        self.assertEqual(event_payload['posts_with_media_count'], 2)
        
        # Verify URLs are collected
        self.assertEqual(len(event_payload['video_urls']), 2)
        self.assertEqual(len(event_payload['image_urls']), 1)
        
        # Verify post-media mapping
        self.assertEqual(len(event_payload['post_media_mapping']), 3)
        
        # Check mapping structure
        mapping = event_payload['post_media_mapping'][0]
        self.assertIn('post_id', mapping)
        self.assertIn('media_type', mapping)
        self.assertIn('media_id', mapping)
        self.assertIn('media_url', mapping)
    
    def test_media_url_validation(self):
        """Test validation of media URLs."""
        media_info = {
            'video_urls': [
                'https://example.com/valid_video.mp4',
                'invalid_video_url',
                'ftp://invalid_protocol/video.mp4',
                'https://valid-domain.com/video2.mp4'
            ],
            'image_urls': [
                'https://example.com/valid_image.jpg',
                '',  # Empty URL
                'https://valid-domain.com/image2.png',
                'not_a_url'
            ]
        }
        
        validation_results = self.detector.validate_media_urls(media_info)
        
        # Verify validation counts
        self.assertEqual(validation_results['valid_videos'], 2)
        self.assertEqual(validation_results['invalid_videos'], 2)
        self.assertEqual(validation_results['valid_images'], 2)
        self.assertEqual(validation_results['invalid_images'], 2)
        
        # Overall validation should fail due to invalid URLs
        self.assertFalse(validation_results['is_valid'])
        
        # Check error messages
        self.assertGreater(len(validation_results['validation_errors']), 0)
    
    def test_platform_specific_url_patterns(self):
        """Test detection of platform-specific URL patterns."""
        platform_url_tests = [
            # Facebook image patterns
            ('https://scontent-vie1-1.xx.fbcdn.net/v/t39.30808-6/image.jpg', True),
            ('https://scontent.xx.fbcdn.net/v/photo.jpg', True),
            ('https://facebook.com/photos/image.jpg', True),
            
            # TikTok patterns
            ('https://p16-sign-sg.tiktokcdn.com/cover.image', True),
            ('https://v77.tiktokcdn-eu.com/video.mp4', False),
            
            # YouTube patterns
            ('https://i.ytimg.com/vi/video_id/hqdefault.jpg', True),
            ('https://yt3.googleusercontent.com/avatar.jpg', True),
            
            # Generic patterns
            ('https://example.com/image.png', True),
            ('https://example.com/photo.jpeg', True),
            ('https://example.com/picture.gif', True),
            ('https://example.com/video.mp4', False),
            ('https://example.com/document.pdf', False),
        ]
        
        for url, expected_is_image in platform_url_tests:
            with self.subTest(url=url):
                result = self.detector._is_image_url(url)
                self.assertEqual(result, expected_is_image, f"Failed for URL: {url}")
    
    def test_empty_media_metadata_structure(self):
        """Test that empty media metadata has correct structure."""
        empty_metadata = self.detector._get_empty_media_metadata()
        
        expected_keys = [
            'media_count', 'has_video', 'has_image',
            'media_processing_requested', 'primary_image_url', 'attachments'
        ]
        
        for key in expected_keys:
            self.assertIn(key, empty_metadata)
        
        # Verify default values
        self.assertEqual(empty_metadata['media_count'], 0)
        self.assertFalse(empty_metadata['has_video'])
        self.assertFalse(empty_metadata['has_image'])
        self.assertFalse(empty_metadata['media_processing_requested'])
        self.assertIsNone(empty_metadata['primary_image_url'])
        self.assertEqual(len(empty_metadata['attachments']), 0)
    
    def test_error_handling_in_batch_processing(self):
        """Test error handling during batch media detection."""
        problematic_posts = [
            {
                'post_id': 'valid_post',
                'attachments': [
                    {
                        'id': 'image_1',
                        'type': 'photo',
                        'url': 'https://example.com/image.jpg'
                    }
                ]
            },
            {
                'post_id': 'problematic_post',
                'attachments': None  # This could cause an error
            },
            {
                # Missing post_id
                'attachments': [
                    {
                        'id': 'image_2',
                        'type': 'photo',
                        'url': 'https://example.com/image2.jpg'
                    }
                ]
            }
        ]
        
        # Should not raise exception
        enhanced_posts = self.detector.detect_media_in_posts(problematic_posts)
        
        # All posts should be processed
        self.assertEqual(len(enhanced_posts), 3)
        
        # All posts should have media_metadata
        for post in enhanced_posts:
            self.assertIn('media_metadata', post)
        
        # Valid post should have correct detection
        self.assertTrue(enhanced_posts[0]['media_metadata']['has_image'])
        
        # Problematic posts should have empty media metadata
        self.assertEqual(enhanced_posts[1]['media_metadata']['media_count'], 0)
        self.assertEqual(enhanced_posts[2]['media_metadata']['media_count'], 1)  # Should still work
    
    def test_video_metadata_extraction(self):
        """Test extraction of video-specific metadata."""
        video_post = {
            'post_id': 'video_test',
            'attachments': [
                {
                    'id': 'video_123',
                    'type': 'video',
                    'url': 'https://example.com/thumbnail.jpg',
                    'video_url': 'https://example.com/video.mp4',
                    'attachment_url': 'https://facebook.com/video/123',
                    'video_length': '120000'  # milliseconds
                }
            ]
        }
        
        media_metadata = self.detector.detect_media_in_post(video_post)
        
        # Verify video detection
        self.assertTrue(media_metadata['has_video'])
        self.assertEqual(media_metadata['media_count'], 1)
        
        # Check video attachment structure
        video_attachment = media_metadata['attachments'][0]
        self.assertEqual(video_attachment['type'], 'Video')
        self.assertEqual(video_attachment['id'], 'video_123')
        self.assertEqual(video_attachment['url'], 'https://example.com/video.mp4')  # Should prefer video_url
        self.assertEqual(video_attachment['attachment_url'], 'https://facebook.com/video/123')
    
    def test_facebook_specific_patterns(self):
        """Test Facebook-specific media URL pattern detection."""
        facebook_urls = [
            'https://scontent-vie1-1.xx.fbcdn.net/v/t39.30808-6/image.jpg',
            'https://video-vie1-1.xx.fbcdn.net/o1/v/t2/f2/m69/video.mp4',
            'https://www.facebook.com/photos/profile_image.jpg',
            'https://facebook.com/photo.php?id=123',
            'https://scontent.xx.fbcdn.net/v/cover_photo.jpg'
        ]
        
        for url in facebook_urls:
            if 'video' in url and '.mp4' in url:
                self.assertFalse(self.detector._is_image_url(url), f"Should not detect video as image: {url}")
            else:
                self.assertTrue(self.detector._is_image_url(url), f"Should detect Facebook image: {url}")


if __name__ == '__main__':
    unittest.main()