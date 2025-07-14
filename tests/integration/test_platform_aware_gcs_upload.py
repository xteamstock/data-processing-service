#!/usr/bin/env python3
"""
Integration test for platform-aware GCS upload by actual upload date.

Tests the complete flow:
Raw data -> Schema transformation -> Platform-aware date grouping -> GCS upload path structure
"""

import json
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from handlers.text_processor import TextProcessor
from handlers.gcs_processed_handler import GCSProcessedHandler


def test_platform_aware_date_grouping():
    """Test platform-aware date grouping with real fixture data."""
    print("🧪 Testing platform-aware date grouping with real fixture data...")
    
    # Load real fixture data
    fixtures_dir = Path(__file__).parent.parent.parent / 'fixtures'
    
    with open(fixtures_dir / 'gcs-facebook-posts.json', 'r') as f:
        facebook_data = json.load(f)
    
    with open(fixtures_dir / 'gcs-tiktok-posts.json', 'r') as f:
        tiktok_data = json.load(f)
        
    with open(fixtures_dir / 'gcs-youtube-posts.json', 'r') as f:
        youtube_data = json.load(f)
    
    # Test each platform separately
    platforms_data = [
        ('facebook', facebook_data[:2]),
        ('tiktok', tiktok_data[:2]), 
        ('youtube', youtube_data[:2])
    ]
    
    for platform, raw_data in platforms_data:
        print(f"\n📋 Testing {platform.upper()} platform...")
        
        # Simulate metadata from data-ingestion service
        metadata = {
            'crawl_id': f'test_{platform}_crawl_123',
            'snapshot_id': f'test_{platform}_snapshot_456',
            'platform': platform,
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-13T10:00:00Z'
        }
        
        # Step 1: Process posts through TextProcessor
        processor = TextProcessor()
        processed_posts = processor.process_posts_for_analytics(raw_data, metadata)
        
        print(f"  ✅ Processed {len(processed_posts)} {platform} posts")
        
        # Step 2: Group by upload date (not crawl date)
        grouped_data = processor.get_grouped_data_for_gcs(processed_posts)
        
        print(f"  ✅ Grouped into {len(grouped_data)} date groups:")
        for date_key, posts in grouped_data.items():
            print(f"    {date_key}: {len(posts)} posts")
        
        # Step 3: Verify grouping is by upload date, not crawl date
        for date_key, posts in grouped_data.items():
            if date_key != 'unknown':
                for post in posts:
                    upload_date = post.get('date_posted', '')
                    if upload_date:
                        # Extract date part from upload timestamp
                        upload_date_only = upload_date.split('T')[0] if 'T' in upload_date else upload_date[:10]
                        assert upload_date_only == date_key, f"Post upload date {upload_date_only} doesn't match group {date_key}"
                        print(f"    ✅ Post {post.get('id', 'unknown')} correctly grouped by upload date {date_key}")
        
        # Step 4: Test GCS path generation
        handler = GCSProcessedHandler()
        preview_path = handler.get_upload_path_preview(metadata, list(grouped_data.keys())[0])
        
        expected_pattern = f"social-analytics-processed-data/raw_data/platform={platform}/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year="
        
        assert expected_pattern in preview_path, f"GCS path doesn't match expected pattern: {preview_path}"
        print(f"  ✅ GCS path structure: {preview_path}")


def test_mixed_platform_date_grouping():
    """Test grouping mixed platform data by upload dates."""
    print("\n🧪 Testing mixed platform date grouping...")
    
    # Load mixed platform data
    fixtures_dir = Path(__file__).parent.parent.parent / 'fixtures'
    
    with open(fixtures_dir / 'gcs-facebook-posts.json', 'r') as f:
        facebook_data = json.load(f)[:1]  # 1 post
    
    with open(fixtures_dir / 'gcs-tiktok-posts.json', 'r') as f:
        tiktok_data = json.load(f)[:1]  # 1 post
        
    with open(fixtures_dir / 'gcs-youtube-posts.json', 'r') as f:
        youtube_data = json.load(f)[:1]  # 1 post
    
    # Process each platform and combine results
    all_processed_posts = []
    
    platforms_data = [
        ('facebook', facebook_data),
        ('tiktok', tiktok_data),
        ('youtube', youtube_data)
    ]
    
    processor = TextProcessor()
    
    for platform, raw_data in platforms_data:
        metadata = {
            'crawl_id': 'mixed_test_crawl_789',
            'platform': platform,
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em'
        }
        
        processed_posts = processor.process_posts_for_analytics(raw_data, metadata)
        all_processed_posts.extend(processed_posts)
    
    print(f"  ✅ Combined {len(all_processed_posts)} posts from all platforms")
    
    # Group all posts by upload date
    grouped_data = processor.get_grouped_data_for_gcs(all_processed_posts)
    
    print(f"  ✅ Grouped into {len(grouped_data)} date groups:")
    for date_key, posts in grouped_data.items():
        platforms = [p.get('platform') for p in posts]
        platform_counts = {p: platforms.count(p) for p in set(platforms)}
        print(f"    {date_key}: {len(posts)} posts {platform_counts}")
    
    # Verify each group contains posts from the correct upload date
    for date_key, posts in grouped_data.items():
        if date_key != 'unknown':
            for post in posts:
                upload_date = post.get('date_posted', '')
                if upload_date:
                    upload_date_only = upload_date.split('T')[0] if 'T' in upload_date else upload_date[:10]
                    assert upload_date_only == date_key, f"Mixed platform grouping error: {upload_date_only} != {date_key}"
    
    print("  ✅ All posts correctly grouped by upload date across platforms")


def test_gcs_upload_path_structure():
    """Test the complete GCS upload path structure."""
    print("\n🧪 Testing GCS upload path structure...")
    
    # Test different date scenarios
    test_scenarios = [
        {
            'platform': 'facebook',
            'date': '2024-12-24',
            'expected_path': 'social-analytics-processed-data/raw_data/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2024/month=12/day=24'
        },
        {
            'platform': 'tiktok', 
            'date': '2025-07-11',
            'expected_path': 'social-analytics-processed-data/raw_data/platform=tiktok/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=11'
        },
        {
            'platform': 'youtube',
            'date': '2025-07-08', 
            'expected_path': 'social-analytics-processed-data/raw_data/platform=youtube/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=08'
        }
    ]
    
    handler = GCSProcessedHandler()
    
    for scenario in test_scenarios:
        metadata = {
            'platform': scenario['platform'],
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood', 
            'category': 'sua-bot-tre-em',
            'crawl_id': 'path_test_123'
        }
        
        preview_path = handler.get_upload_path_preview(metadata, scenario['date'])
        
        assert scenario['expected_path'] in preview_path, f"Path mismatch for {scenario['platform']}: {preview_path}"
        print(f"  ✅ {scenario['platform']}: {preview_path}")


def main():
    """Run all platform-aware GCS upload tests."""
    print("🚀 Testing platform-aware GCS upload by actual upload date...")
    
    try:
        test_platform_aware_date_grouping()
        test_mixed_platform_date_grouping()
        test_gcs_upload_path_structure()
        
        print("\n🎉 All platform-aware GCS upload tests passed!")
        print("✅ Posts are correctly grouped by actual upload date from each platform")
        print("✅ GCS paths follow hierarchical structure based on upload date")
        print("✅ Multi-platform date handling working correctly")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Platform-aware GCS upload test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())