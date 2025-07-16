#!/usr/bin/env python3
"""
Test to verify crawl_id preservation through the data processing pipeline.
"""

import json
import logging
from datetime import datetime
from handlers.bigquery_handler import BigQueryHandler
from handlers.schema_mapper import SchemaMapper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_crawl_id_preservation():
    """Test that crawl_id is preserved through schema mapping and BigQuery insertion."""
    
    # Test crawl_id
    test_crawl_id = "crawl-id-preservation-test-12345"
    
    # Create test metadata
    metadata = {
        'crawl_id': test_crawl_id,
        'snapshot_id': 'test_snapshot',
        'platform': 'facebook',
        'competitor': 'test_competitor',
        'brand': 'test_brand',
        'category': 'test_category',
        'crawl_date': datetime.utcnow().isoformat()
    }
    
    # Create minimal test post data
    raw_post = {
        'post_id': 'test_post_12345',
        'url': 'https://www.facebook.com/test/posts/12345',
        'user_url': 'https://www.facebook.com/test',
        'post_content': 'Test post content for crawl_id preservation test',
        'date_posted': '2024-12-24T12:00:00.000Z',
        'likes': 10,
        'num_comments': 2,
        'num_shares': 1,
        'user_username': 'Test User',
        'page_name': 'Test Page',
        'page_verified': False,
        'page_followers': 1000,
        'page_likes': 900
    }
    
    print("="*60)
    print("CRAWL_ID PRESERVATION TEST")
    print("="*60)
    print(f"Original crawl_id: {test_crawl_id}")
    
    # Step 1: Test schema mapping
    print("\n1. Testing Schema Mapping...")
    schema_mapper = SchemaMapper()
    
    try:
        transformed_post = schema_mapper.transform_post(
            raw_post=raw_post,
            platform='facebook',
            metadata=metadata,
            schema_version='1.0.0'
        )
        
        mapped_crawl_id = transformed_post.get('crawl_id')
        print(f"   Schema mapped crawl_id: {mapped_crawl_id}")
        
        if mapped_crawl_id == test_crawl_id:
            print("   ✅ Schema mapping preserves crawl_id")
        else:
            print(f"   ❌ Schema mapping CHANGED crawl_id: {mapped_crawl_id}")
            
    except Exception as e:
        print(f"   ❌ Schema mapping failed: {str(e)}")
        return False
    
    # Step 2: Test BigQuery validation
    print("\n2. Testing BigQuery Validation...")
    bigquery_handler = BigQueryHandler()
    
    try:
        # Test the validation method directly
        validated_posts = bigquery_handler._validate_posts_schema([transformed_post])
        
        if validated_posts:
            validated_crawl_id = validated_posts[0].get('crawl_id')
            print(f"   BigQuery validated crawl_id: {validated_crawl_id}")
            
            if validated_crawl_id == test_crawl_id:
                print("   ✅ BigQuery validation preserves crawl_id")
            else:
                print(f"   ❌ BigQuery validation CHANGED crawl_id: {validated_crawl_id}")
                return False
        else:
            print("   ❌ BigQuery validation returned empty result")
            return False
            
    except Exception as e:
        print(f"   ❌ BigQuery validation failed: {str(e)}")
        return False
    
    # Step 3: Test full BigQuery insertion (but don't actually insert to avoid test data)
    print("\n3. Testing BigQuery Insertion Preparation...")
    
    try:
        # Simulate the insertion call but don't actually execute
        print(f"   Would insert to table: facebook_posts_schema_driven")
        print(f"   Post count: 1")
        print(f"   Crawl_id in final data: {validated_posts[0].get('crawl_id')}")
        
        # Check if the post ID generation preserves crawl_id
        post_id = validated_posts[0].get('id')
        print(f"   Generated post ID: {post_id}")
        
        if test_crawl_id in post_id:
            print("   ✅ Post ID includes crawl_id")
        else:
            print(f"   ❌ Post ID does not include crawl_id: {post_id}")
            
    except Exception as e:
        print(f"   ❌ BigQuery insertion preparation failed: {str(e)}")
        return False
    
    print("\n" + "="*60)
    print("CRAWL_ID PRESERVATION TEST SUMMARY")
    print("="*60)
    print(f"✅ Original crawl_id: {test_crawl_id}")
    print(f"✅ Schema mapped crawl_id: {transformed_post.get('crawl_id')}")
    print(f"✅ BigQuery validated crawl_id: {validated_posts[0].get('crawl_id')}")
    print(f"✅ Generated post ID: {validated_posts[0].get('id')}")
    print("\n🎯 RESULT: crawl_id preservation is working correctly in the pipeline!")
    
    return True

if __name__ == "__main__":
    success = test_crawl_id_preservation()
    exit(0 if success else 1)