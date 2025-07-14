#!/usr/bin/env python3
"""
Integration test for platform-specific BigQuery data flow.

This test verifies the complete flow from SchemaMapper transformation
to BigQuery insertion for all platforms (Facebook, TikTok, YouTube).
"""

import json
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'handlers'))

from schema_mapper import SchemaMapper
from bigquery_handler import BigQueryHandler


def test_facebook_complete_flow():
    """Test complete Facebook data flow."""
    print("🧪 Testing Facebook complete flow...")
    
    # Load Facebook test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-facebook-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Extract first post for testing
    raw_facebook_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': 'test_crawl_123',
        'snapshot_id': 'test_snapshot_456',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': '2024-01-01T12:00:00Z'
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_facebook_post, 'facebook', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Post ID: {transformed_post.get('post_id')}")
    print(f"   Content length: {len(transformed_post.get('post_content', ''))}")
    
    # Step 2: Validate with BigQueryHandler
    with patch('bigquery_handler.bigquery.Client'):
        handler = BigQueryHandler()
        
        # Test platform table routing
        table_name = handler._get_platform_table('facebook')
        print(f"   Target table: {table_name}")
        
        # Test validation
        validated_posts = handler._validate_posts_schema([transformed_post])
        validated_post = validated_posts[0]
        
        print(f"✅ BigQuery validation successful")
        print(f"   Validated platform: {validated_post.get('platform')}")
        print(f"   Post ID: {validated_post.get('post_id')}")
        print(f"   Page name: {validated_post.get('page_name')}")
        print(f"   Page category: {validated_post.get('page_category')}")
        
        # Verify Facebook-specific fields are present
        facebook_fields = ['post_id', 'post_url', 'post_content', 'page_name', 'page_category']
        for field in facebook_fields:
            assert field in validated_post, f"Missing Facebook field: {field}"
        
        # Verify TikTok fields are NOT present
        tiktok_fields = ['video_id', 'digg_count', 'play_count']
        for field in tiktok_fields:
            assert field not in validated_post, f"TikTok field should not be present: {field}"


def test_tiktok_complete_flow():
    """Test complete TikTok data flow."""
    print("\n🧪 Testing TikTok complete flow...")
    
    # Load TikTok test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-tiktok-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Extract first post for testing
    raw_tiktok_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': 'test_crawl_456',
        'snapshot_id': 'test_snapshot_789',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': '2024-01-01T12:00:00Z'
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_tiktok_post, 'tiktok', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Video ID: {transformed_post.get('video_id')}")
    print(f"   Description length: {len(transformed_post.get('description', ''))}")
    
    # Step 2: Validate with BigQueryHandler
    with patch('bigquery_handler.bigquery.Client'):
        handler = BigQueryHandler()
        
        # Test platform table routing
        table_name = handler._get_platform_table('tiktok')
        print(f"   Target table: {table_name}")
        
        # Test validation
        validated_posts = handler._validate_posts_schema([transformed_post])
        validated_post = validated_posts[0]
        
        print(f"✅ BigQuery validation successful")
        print(f"   Validated platform: {validated_post.get('platform')}")
        print(f"   Video ID: {validated_post.get('video_id')}")
        print(f"   Author name: {validated_post.get('author_name')}")
        print(f"   Play count: {validated_post.get('play_count')}")
        
        # Verify TikTok-specific fields are present
        tiktok_fields = ['video_id', 'video_url', 'description', 'author_name', 'play_count', 'digg_count']
        for field in tiktok_fields:
            assert field in validated_post, f"Missing TikTok field: {field}"
        
        # Verify Facebook fields are NOT present
        facebook_fields = ['post_id', 'page_name', 'page_category']
        for field in facebook_fields:
            assert field not in validated_post, f"Facebook field should not be present: {field}"


def test_youtube_complete_flow():
    """Test complete YouTube data flow."""
    print("\n🧪 Testing YouTube complete flow...")
    
    # Load YouTube test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-youtube-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Extract first post for testing
    raw_youtube_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': 'test_crawl_789',
        'snapshot_id': 'test_snapshot_101',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': '2024-01-01T12:00:00Z'
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_youtube_post, 'youtube', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Video ID: {transformed_post.get('video_id')}")
    print(f"   Title: {transformed_post.get('title', '')[:50]}...")
    
    # Step 2: Validate with BigQueryHandler
    with patch('bigquery_handler.bigquery.Client'):
        handler = BigQueryHandler()
        
        # Test platform table routing
        table_name = handler._get_platform_table('youtube')
        print(f"   Target table: {table_name}")
        
        # Test validation
        validated_posts = handler._validate_posts_schema([transformed_post])
        validated_post = validated_posts[0]
        
        print(f"✅ BigQuery validation successful")
        print(f"   Validated platform: {validated_post.get('platform')}")
        print(f"   Video ID: {validated_post.get('video_id')}")
        print(f"   Channel name: {validated_post.get('channel_name')}")
        print(f"   View count: {validated_post.get('view_count')}")
        
        # Verify YouTube-specific fields are present
        youtube_fields = ['video_id', 'video_url', 'title', 'channel_name', 'view_count', 'like_count']
        for field in youtube_fields:
            assert field in validated_post, f"Missing YouTube field: {field}"
        
        # Verify other platform fields are NOT present
        other_fields = ['post_id', 'page_name', 'digg_count', 'play_count']
        for field in other_fields:
            assert field not in validated_post, f"Other platform field should not be present: {field}"


def main():
    """Run all integration tests."""
    print("🚀 Starting platform-specific BigQuery integration tests...")
    
    try:
        test_facebook_complete_flow()
        test_tiktok_complete_flow()
        test_youtube_complete_flow()
        
        print("\n✅ All platform-specific BigQuery integration tests passed!")
        print("🎉 SchemaMapper + BigQueryHandler integration working correctly")
        return 0
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())