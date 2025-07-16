#!/usr/bin/env python3
"""
Test script to verify BigQuery insertion for all platforms.

This script tests the complete data flow:
1. Load test fixtures for each platform
2. Transform data using SchemaMapper
3. Insert into platform-specific BigQuery tables
4. Verify insertion success
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from handlers.schema_mapper import SchemaMapper
from handlers.bigquery_handler import BigQueryHandler


def test_platform_insertion(platform, fixture_file, metadata):
    """Test insertion for a specific platform."""
    print(f"\n🧪 Testing {platform.upper()} insertion...")
    
    # Load fixture data
    fixture_path = Path(__file__).parent / "fixtures" / fixture_file
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    # Initialize components
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    bigquery_handler = BigQueryHandler()
    
    # Test with first post from fixture
    raw_post = posts[0]
    print(f"  📄 Processing post: {raw_post.get('id', 'unknown')}")
    
    # Transform the post
    try:
        transformed_post = schema_mapper.transform_post(raw_post, platform, metadata)
        print(f"  ✅ Schema transformation successful")
        print(f"     - Post ID: {transformed_post.get('id')}")
        print(f"     - Platform: {transformed_post.get('platform')}")
        print(f"     - Date Posted: {transformed_post.get('date_posted')}")
        print(f"     - Content Preview: {transformed_post.get('description', transformed_post.get('post_content', ''))[:100]}...")
    except Exception as e:
        print(f"  ❌ Schema transformation failed: {str(e)}")
        return False
    
    # Insert into BigQuery
    try:
        result = bigquery_handler.insert_post(transformed_post, platform)
        if result:
            print(f"  ✅ BigQuery insertion successful")
            return True
        else:
            print(f"  ❌ BigQuery insertion failed")
            return False
    except Exception as e:
        print(f"  ❌ BigQuery insertion error: {str(e)}")
        return False


def main():
    """Main test function."""
    print("🚀 Testing BigQuery insertion for all platforms")
    
    # Test metadata
    test_metadata = {
        'crawl_id': f'test_crawl_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'snapshot_id': 'test_snapshot_integration',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.now().isoformat()
    }
    
    # Test configurations
    platforms = [
        {
            'name': 'facebook',
            'fixture': 'gcs-facebook-posts.json'
        },
        {
            'name': 'tiktok', 
            'fixture': 'gcs-tiktok-posts.json'
        },
        {
            'name': 'youtube',
            'fixture': 'gcs-youtube-posts.json'
        }
    ]
    
    # Run tests for each platform
    results = {}
    for platform_config in platforms:
        platform = platform_config['name']
        fixture = platform_config['fixture']
        
        try:
            success = test_platform_insertion(platform, fixture, test_metadata)
            results[platform] = success
        except Exception as e:
            print(f"  ❌ Platform {platform} test failed with exception: {str(e)}")
            results[platform] = False
    
    # Summary
    print(f"\n📊 Test Results Summary:")
    all_passed = True
    for platform, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {platform.upper()}: {status}")
        if not success:
            all_passed = False
    
    if all_passed:
        print(f"\n🎉 All platform BigQuery insertions successful!")
        print(f"   - Facebook, TikTok, and YouTube data can be inserted into BigQuery")
        print(f"   - Schema transformations working correctly")
        print(f"   - Platform-specific tables accessible")
        return 0
    else:
        print(f"\n❌ Some platform tests failed")
        print(f"   - Check BigQuery permissions and table schemas")
        print(f"   - Verify service account has BigQuery Editor role")
        return 1


if __name__ == "__main__":
    sys.exit(main())