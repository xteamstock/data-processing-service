#!/usr/bin/env python3
"""
Debug script to compare data formats between working test and integration test.
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

def simulate_working_test_data_format():
    """Simulate how the working test prepares data."""
    print("="*60)
    print("SIMULATING WORKING TEST DATA FORMAT")
    print("="*60)
    
    # Create test data similar to working test
    metadata = {
        'crawl_id': 'debug-working-format',
        'platform': 'facebook',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    }
    
    raw_post = {
        'post_id': 'debug_post_123',
        'url': 'https://www.facebook.com/test/posts/123',
        'user_url': 'https://www.facebook.com/test',
        'post_content': 'Debug test post',
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
    
    # Step 1: Schema transformation
    schema_mapper = SchemaMapper()
    transformed_post = schema_mapper.transform_post(raw_post, 'facebook', metadata)
    
    print(f"1. After schema transformation:")
    print(f"   - Keys: {list(transformed_post.keys())}")
    print(f"   - crawl_id: {transformed_post.get('crawl_id')}")
    print(f"   - processing_metadata present: {'processing_metadata' in transformed_post}")
    
    # Step 2: Flatten processing metadata (like working test)
    if 'processing_metadata' in transformed_post:
        processing_meta = transformed_post.pop('processing_metadata')
        transformed_post['schema_version'] = processing_meta.get('schema_version')
        transformed_post['processing_version'] = processing_meta.get('processing_version')
        transformed_post['data_quality_score'] = processing_meta.get('data_quality_score')
    
    print(f"2. After flattening processing_metadata:")
    print(f"   - Keys: {list(transformed_post.keys())}")
    print(f"   - schema_version: {transformed_post.get('schema_version')}")
    print(f"   - data_quality_score: {transformed_post.get('data_quality_score')}")
    
    # Step 3: Remove nested objects (like working test)
    cleaned = {}
    nested_objects_removed = []
    for key, value in transformed_post.items():
        if not isinstance(value, dict):
            cleaned[key] = value
        else:
            nested_objects_removed.append(key)
    
    print(f"3. After removing nested objects:")
    print(f"   - Final keys: {list(cleaned.keys())}")
    print(f"   - Nested objects removed: {nested_objects_removed}")
    print(f"   - Final crawl_id: {cleaned.get('crawl_id')}")
    
    return [cleaned]

def simulate_integration_test_data_format():
    """Simulate how the integration test prepares data."""
    print("\n" + "="*60)
    print("SIMULATING INTEGRATION TEST DATA FORMAT")
    print("="*60)
    
    # Create identical test data
    metadata = {
        'crawl_id': 'debug-integration-format',
        'platform': 'facebook',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em'
    }
    
    raw_post = {
        'post_id': 'debug_post_123',
        'url': 'https://www.facebook.com/test/posts/123',
        'user_url': 'https://www.facebook.com/test',
        'post_content': 'Debug test post',
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
    
    # Step 1: Schema transformation
    schema_mapper = SchemaMapper()
    transformed_post = schema_mapper.transform_post(raw_post, 'facebook', metadata)
    
    print(f"1. After schema transformation:")
    print(f"   - Keys: {list(transformed_post.keys())}")
    print(f"   - crawl_id: {transformed_post.get('crawl_id')}")
    print(f"   - processing_metadata present: {'processing_metadata' in transformed_post}")
    
    # Step 2: BigQuery handler validation (our current approach)
    bigquery_handler = BigQueryHandler()
    validated_posts = bigquery_handler._validate_posts_schema([transformed_post])
    
    print(f"2. After BigQuery validation:")
    print(f"   - Keys: {list(validated_posts[0].keys()) if validated_posts else 'EMPTY'}")
    print(f"   - crawl_id: {validated_posts[0].get('crawl_id') if validated_posts else 'MISSING'}")
    print(f"   - processing_metadata present: {'processing_metadata' in validated_posts[0] if validated_posts else 'N/A'}")
    
    return validated_posts

def test_both_approaches():
    """Test both data preparation approaches."""
    print("🔍 BIGQUERY DATA FORMAT COMPARISON DEBUG")
    
    # Simulate both approaches
    working_test_data = simulate_working_test_data_format()
    integration_test_data = simulate_integration_test_data_format()
    
    print("\n" + "="*60)
    print("COMPARISON SUMMARY")
    print("="*60)
    
    if working_test_data and integration_test_data:
        working_keys = set(working_test_data[0].keys())
        integration_keys = set(integration_test_data[0].keys())
        
        print(f"Working test keys count: {len(working_keys)}")
        print(f"Integration test keys count: {len(integration_keys)}")
        
        only_in_working = working_keys - integration_keys
        only_in_integration = integration_keys - working_keys
        common_keys = working_keys & integration_keys
        
        print(f"\n📊 Key Differences:")
        print(f"   - Only in working test: {only_in_working}")
        print(f"   - Only in integration test: {only_in_integration}")
        print(f"   - Common keys: {len(common_keys)}")
        
        print(f"\n🔑 crawl_id comparison:")
        print(f"   - Working test: {working_test_data[0].get('crawl_id')}")
        print(f"   - Integration test: {integration_test_data[0].get('crawl_id')}")
        
        # Check for specific problematic fields
        problematic_fields = []
        for key in common_keys:
            working_val = working_test_data[0].get(key)
            integration_val = integration_test_data[0].get(key)
            
            if type(working_val) != type(integration_val):
                problematic_fields.append(f"{key}: {type(working_val)} vs {type(integration_val)}")
        
        if problematic_fields:
            print(f"\n⚠️  Type mismatches:")
            for field in problematic_fields:
                print(f"   - {field}")
        else:
            print(f"\n✅ No type mismatches found")
            
    else:
        print("❌ Failed to generate data for comparison")
    
    print("\n" + "="*60)
    print("RECOMMENDATION")
    print("="*60)
    
    if only_in_working:
        print("🎯 The working test has fields that integration test is missing.")
        print("   Consider adding these fields to BigQuery validation.")
    
    if only_in_integration:
        print("🎯 The integration test has extra fields that working test removes.")
        print("   These might be causing BigQuery insertion to fail.")
    
    return True

if __name__ == "__main__":
    test_both_approaches()