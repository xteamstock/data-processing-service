#!/usr/bin/env python3
"""
Debug script to compare BigQuery handler vs direct insertion.
"""

import json
import logging
from datetime import datetime
from handlers.bigquery_handler import BigQueryHandler
from handlers.schema_mapper import SchemaMapper
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_direct_insertion():
    """Test direct BigQuery insertion like the working test."""
    print("="*60)
    print("TESTING DIRECT BIGQUERY INSERTION (WORKING TEST METHOD)")
    print("="*60)
    
    metadata = {
        'crawl_id': f'debug-direct-{datetime.now().strftime("%H%M%S")}',
        'platform': 'facebook',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    }
    
    raw_post = {
        'post_id': 'debug_direct_123',
        'url': 'https://www.facebook.com/test/posts/123',
        'user_url': 'https://www.facebook.com/test',
        'post_content': 'Debug direct test post',
        'date_posted': '2024-12-24T12:00:00.000Z',
        'likes': 100,
        'num_comments': 20,
        'num_shares': 10,
        'user_username': 'Direct Test User',
        'page_name': 'Direct Test Page',
        'page_verified': False,
        'page_followers': 1000,
        'page_likes': 900
    }
    
    # Schema transformation
    schema_mapper = SchemaMapper()
    transformed_post = schema_mapper.transform_post(raw_post, 'facebook', metadata)
    
    # Flatten processing metadata (like working test)
    if 'processing_metadata' in transformed_post:
        processing_meta = transformed_post.pop('processing_metadata')
        transformed_post['schema_version'] = processing_meta.get('schema_version')
        transformed_post['processing_version'] = processing_meta.get('processing_version')
        transformed_post['data_quality_score'] = processing_meta.get('data_quality_score')
    
    # Clean for BigQuery (like working test)
    cleaned = {}
    for key, value in transformed_post.items():
        if not isinstance(value, dict):
            cleaned[key] = value
    
    print(f"1. Transformed and cleaned data:")
    print(f"   - crawl_id: {cleaned.get('crawl_id')}")
    print(f"   - likes: {cleaned.get('likes')}")
    print(f"   - comments: {cleaned.get('comments')}")
    print(f"   - Total fields: {len(cleaned)}")
    
    # Direct BigQuery insertion
    client = bigquery.Client()
    table_id = f"{client.project}.social_analytics.facebook_posts_schema_driven"
    
    print(f"2. Inserting to table: {table_id}")
    
    try:
        errors = client.insert_rows_json(table_id, [cleaned])
        
        if errors:
            print(f"❌ Direct insertion errors: {errors}")
            return False, None
        else:
            print(f"✅ Direct insertion SUCCESS!")
            return True, cleaned.get('crawl_id')
            
    except Exception as e:
        print(f"❌ Direct insertion exception: {str(e)}")
        return False, None

def test_handler_insertion():
    """Test BigQuery insertion through our handler."""
    print("\n" + "="*60)
    print("TESTING BIGQUERY HANDLER INSERTION (INTEGRATION TEST METHOD)")
    print("="*60)
    
    metadata = {
        'crawl_id': f'debug-handler-{datetime.now().strftime("%H%M%S")}',
        'platform': 'facebook',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    }
    
    raw_post = {
        'post_id': 'debug_handler_123',
        'url': 'https://www.facebook.com/test/posts/123',
        'user_url': 'https://www.facebook.com/test',
        'post_content': 'Debug handler test post',
        'date_posted': '2024-12-24T12:00:00.000Z',
        'likes': 200,
        'num_comments': 30,
        'num_shares': 15,
        'user_username': 'Handler Test User',
        'page_name': 'Handler Test Page',
        'page_verified': False,
        'page_followers': 2000,
        'page_likes': 1800
    }
    
    # Schema transformation
    schema_mapper = SchemaMapper()
    transformed_post = schema_mapper.transform_post(raw_post, 'facebook', metadata)
    
    print(f"1. Transformed data:")
    print(f"   - crawl_id: {transformed_post.get('crawl_id')}")
    print(f"   - likes: {transformed_post.get('likes')}")
    print(f"   - comments: {transformed_post.get('comments')}")
    print(f"   - Total fields: {len(transformed_post)}")
    
    # BigQuery handler insertion
    bigquery_handler = BigQueryHandler()
    
    print(f"2. Using BigQuery handler...")
    
    try:
        result = bigquery_handler.insert_posts([transformed_post], metadata)
        
        print(f"3. Handler result: {result}")
        
        if result.get('success', False):
            print(f"✅ Handler insertion reports SUCCESS!")
            print(f"   - Table: {result.get('table_id')}")
            print(f"   - Rows inserted: {result.get('rows_inserted')}")
            return True, metadata.get('crawl_id')
        else:
            print(f"❌ Handler insertion reports FAILURE!")
            return False, None
            
    except Exception as e:
        print(f"❌ Handler insertion exception: {str(e)}")
        return False, None

def verify_insertions(direct_crawl_id, handler_crawl_id):
    """Verify if the insertions actually made it to BigQuery."""
    print("\n" + "="*60)
    print("VERIFYING BIGQUERY INSERTIONS")
    print("="*60)
    
    client = bigquery.Client()
    
    # Check direct insertion
    if direct_crawl_id:
        query = f"""
        SELECT crawl_id, post_id, likes, comments 
        FROM `social_analytics.facebook_posts_schema_driven` 
        WHERE crawl_id = '{direct_crawl_id}'
        """
        try:
            results = list(client.query(query))
            print(f"Direct insertion verification:")
            if results:
                row = results[0]
                print(f"   ✅ Found in BigQuery: {row.crawl_id} (likes: {row.likes}, comments: {row.comments})")
            else:
                print(f"   ❌ NOT found in BigQuery: {direct_crawl_id}")
        except Exception as e:
            print(f"   ❌ Query error: {str(e)}")
    
    # Check handler insertion
    if handler_crawl_id:
        query = f"""
        SELECT crawl_id, post_id, likes, comments 
        FROM `social_analytics.facebook_posts_schema_driven` 
        WHERE crawl_id = '{handler_crawl_id}'
        """
        try:
            results = list(client.query(query))
            print(f"Handler insertion verification:")
            if results:
                row = results[0]
                print(f"   ✅ Found in BigQuery: {row.crawl_id} (likes: {row.likes}, comments: {row.comments})")
            else:
                print(f"   ❌ NOT found in BigQuery: {handler_crawl_id}")
        except Exception as e:
            print(f"   ❌ Query error: {str(e)}")

def main():
    """Run the diagnostic comparison."""
    print("🔍 BIGQUERY HANDLER VS DIRECT INSERTION COMPARISON")
    
    # Test both methods
    direct_success, direct_crawl_id = test_direct_insertion()
    handler_success, handler_crawl_id = test_handler_insertion()
    
    # Wait a moment for BigQuery consistency
    import time
    print("\n⏳ Waiting 10 seconds for BigQuery consistency...")
    time.sleep(10)
    
    # Verify both insertions
    verify_insertions(direct_crawl_id, handler_crawl_id)
    
    # Summary
    print("\n" + "="*60)
    print("DIAGNOSTIC SUMMARY")
    print("="*60)
    print(f"Direct insertion: {'✅ SUCCESS' if direct_success else '❌ FAILED'}")
    print(f"Handler insertion: {'✅ SUCCESS' if handler_success else '❌ FAILED'}")
    
    if direct_success and not handler_success:
        print("\n🎯 CONCLUSION: BigQuery handler has a bug - direct insertion works but handler fails")
    elif not direct_success and handler_success:
        print("\n🎯 CONCLUSION: Direct method has issues - handler works correctly")
    elif direct_success and handler_success:
        print("\n🎯 CONCLUSION: Both methods work - the issue is elsewhere")
    else:
        print("\n🎯 CONCLUSION: Both methods fail - broader BigQuery or auth issue")

if __name__ == "__main__":
    main()