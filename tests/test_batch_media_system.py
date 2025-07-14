#!/usr/bin/env python3
"""
Test script for the new batch media detection and publishing system.

This script demonstrates how the multi-platform media detector and batch
event publisher work with fixture data files.

USAGE:
    python test_batch_media_system.py [--platform facebook|tiktok|youtube] [--publish]
    
    --platform: Test specific platform (default: all)
    --publish: Actually publish to Pub/Sub (default: dry-run)
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
import argparse

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from handlers.multi_platform_media_detector import MultiPlatformMediaDetector
from events.batch_media_event_publisher import BatchMediaEventPublisher


def test_platform_media_detection(platform: str, publish_events: bool = False):
    """Test media detection and event publishing for a specific platform."""
    print(f"\n🔍 Testing {platform.upper()} Media Detection")
    print("=" * 60)
    
    # Load fixture data
    fixtures_dir = Path(__file__).parent.parent / 'fixtures'
    fixture_file = fixtures_dir / f'gcs-{platform}-posts.json'
    
    if not fixture_file.exists():
        print(f"❌ Fixture file not found: {fixture_file}")
        return
    
    with open(fixture_file, 'r') as f:
        raw_posts = json.load(f)
    
    print(f"📁 Loaded {len(raw_posts)} posts from {fixture_file.name}")
    
    # Initialize media detector
    detector = MultiPlatformMediaDetector()
    
    # Detect media in batch
    batch_result = detector.detect_media_batch(raw_posts, platform)
    
    # Display results
    print(f"\n📊 Detection Results:")
    print(f"  Total posts: {batch_result['total_posts']}")
    print(f"  Posts with media: {batch_result['posts_with_media']}")
    print(f"  Total media items: {batch_result['total_media_items']}")
    print(f"  Videos: {batch_result['total_videos']}")
    print(f"  Images: {batch_result['total_images']}")
    
    # Show media breakdown
    if batch_result['total_media_items'] > 0:
        print(f"\n🎬 Media Breakdown:")
        
        # Videos
        videos = batch_result['media_breakdown']['videos']
        if videos:
            print(f"  Videos ({len(videos)}):")
            for i, video in enumerate(videos[:3]):  # Show first 3
                url_preview = video['url'][:80] + "..." if len(video['url']) > 80 else video['url']
                print(f"    {i+1}. {url_preview}")
            if len(videos) > 3:
                print(f"    ... and {len(videos) - 3} more videos")
        
        # Images
        images = batch_result['media_breakdown']['images']
        if images:
            print(f"  Images ({len(images)}):")
            for i, image in enumerate(images[:3]):  # Show first 3
                url_preview = image['url'][:80] + "..." if len(image['url']) > 80 else image['url']
                print(f"    {i+1}. {url_preview}")
            if len(images) > 3:
                print(f"    ... and {len(images) - 3} more images")
        
        # Profile images
        profiles = batch_result['media_breakdown']['profile_images']
        if profiles:
            print(f"  Profile Images ({len(profiles)}):")
            for i, profile in enumerate(profiles[:2]):  # Show first 2
                url_preview = profile['url'][:80] + "..." if len(profile['url']) > 80 else profile['url']
                print(f"    {i+1}. {url_preview}")
    
    # Test batch event creation
    if batch_result['total_media_items'] > 0:
        print(f"\n📤 Testing Batch Event Creation...")
        
        # Sample crawl metadata
        crawl_metadata = {
            'crawl_id': f'test_batch_{platform}_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'snapshot_id': f'test_snapshot_{platform}_001',
            'platform': platform,
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': datetime.utcnow().isoformat()
        }
        
        file_metadata = {
            'filename': fixture_file.name,
            'size': fixture_file.stat().st_size
        }
        
        if publish_events:
            try:
                # Actually publish the batch event
                publisher = BatchMediaEventPublisher()
                result = publisher.publish_batch_from_raw_file(
                    raw_posts, platform, crawl_metadata, file_metadata
                )
                
                if result['success']:
                    print(f"✅ Successfully published batch event!")
                    print(f"   Event ID: {result.get('event_id')}")
                    print(f"   Message ID: {result.get('message_id')}")
                    print(f"   Message: {result['message']}")
                else:
                    print(f"❌ Failed to publish batch event: {result['message']}")
                    
            except Exception as e:
                print(f"❌ Error publishing batch event: {str(e)}")
        else:
            # Just create the event structure (dry run)
            batch_event = detector.prepare_batch_event(batch_result, crawl_metadata)
            
            print(f"✅ Batch event structure created:")
            print(f"   Event Type: {batch_event['event_type']}")
            print(f"   Event ID: {batch_event['event_id']}")
            print(f"   Media Items: {batch_event['data']['batch_size']}")
            print(f"   Platform: {batch_event['data']['platform']}")
            print(f"   Event Size: {len(json.dumps(batch_event))} bytes")
    
    else:
        print(f"\n⚠️  No media found in {platform} posts - no events to publish")
    
    return batch_result


def test_all_platforms(publish_events: bool = False):
    """Test media detection for all platforms."""
    print("🚀 Testing Multi-Platform Batch Media Detection System")
    print("=" * 80)
    
    platforms = ['facebook', 'tiktok', 'youtube']
    results = {}
    
    for platform in platforms:
        try:
            result = test_platform_media_detection(platform, publish_events)
            results[platform] = result
        except Exception as e:
            print(f"❌ Error testing {platform}: {str(e)}")
            results[platform] = None
    
    # Summary
    print(f"\n" + "=" * 80)
    print("📊 MULTI-PLATFORM SUMMARY")
    print("=" * 80)
    
    total_posts = 0
    total_media = 0
    total_videos = 0
    total_images = 0
    
    for platform, result in results.items():
        if result:
            platform_posts = result['total_posts']
            platform_media = result['total_media_items']
            platform_videos = result['total_videos']
            platform_images = result['total_images']
            
            total_posts += platform_posts
            total_media += platform_media
            total_videos += platform_videos
            total_images += platform_images
            
            print(f"{platform.upper()}: {platform_posts} posts, {platform_media} media "
                  f"({platform_videos} videos, {platform_images} images)")
        else:
            print(f"{platform.upper()}: ❌ Failed")
    
    print(f"\nTOTALS: {total_posts} posts, {total_media} media items "
          f"({total_videos} videos, {total_images} images)")
    
    if publish_events:
        print(f"\n✅ All batch events published to Pub/Sub topic: batch-media-processing-requests")
    else:
        print(f"\n💡 This was a dry-run. Use --publish to actually send events to Pub/Sub.")
    
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Test batch media detection and publishing system'
    )
    parser.add_argument(
        '--platform',
        choices=['facebook', 'tiktok', 'youtube', 'all'],
        default='all',
        help='Platform to test (default: all)'
    )
    parser.add_argument(
        '--publish',
        action='store_true',
        help='Actually publish events to Pub/Sub (default: dry-run)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.platform == 'all':
            results = test_all_platforms(args.publish)
        else:
            results = test_platform_media_detection(args.platform, args.publish)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())