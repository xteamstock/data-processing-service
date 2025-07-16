#!/usr/bin/env python3
"""
Debug different BigQuery JSON insertion methods.
"""

import os
import json
from datetime import datetime
from google.cloud import bigquery

def test_different_insertion_methods():
    """Test different ways to insert JSON data to BigQuery."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    table_id = f"{project_id}.{dataset_id}.facebook_posts"
    
    # Test data with JSON fields
    base_record = {
        'id': 'json_test_123',
        'crawl_id': 'json_test_456',
        'platform': 'facebook',
        'competitor': 'test_competitor',
        'date_posted': datetime.now().isoformat(),
        'processed_date': datetime.now().isoformat(),
        'grouped_date': datetime.now().strftime('%Y-%m-%d'),
        'post_id': 'test_post_123',
        'post_url': 'https://facebook.com/test',
    }
    
    # Method 1: Dict objects (current approach)
    print("🧪 Method 1: Dict objects with insert_rows_json()")
    test_record_1 = base_record.copy()
    test_record_1.update({
        'engagement_metrics': {'likes': 100},
        'content_analysis': {'language': 'en'}
    })
    
    try:
        errors = client.insert_rows_json(table_id, [test_record_1])
        if errors:
            print(f"❌ Errors: {errors}")
        else:
            print("✅ Success!")
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
    
    # Method 2: JSON strings
    print("\n🧪 Method 2: JSON strings with insert_rows_json()")
    test_record_2 = base_record.copy()
    test_record_2['id'] = 'json_test_124'
    test_record_2.update({
        'engagement_metrics': json.dumps({'likes': 100}),
        'content_analysis': json.dumps({'language': 'en'})
    })
    
    try:
        errors = client.insert_rows_json(table_id, [test_record_2])
        if errors:
            print(f"❌ Errors: {errors}")
        else:
            print("✅ Success!")
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
    
    # Method 3: Using insert_rows with table schema
    print("\n🧪 Method 3: Dict objects with insert_rows()")
    test_record_3 = base_record.copy()
    test_record_3['id'] = 'json_test_125'
    test_record_3.update({
        'engagement_metrics': {'likes': 100},
        'content_analysis': {'language': 'en'}
    })
    
    try:
        table = client.get_table(table_id)
        errors = client.insert_rows(table, [test_record_3])
        if errors:
            print(f"❌ Errors: {errors}")
        else:
            print("✅ Success!")
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        
    # Method 4: Minimal with only required fields
    print("\n🧪 Method 4: Only required fields")
    test_record_4 = {
        'id': 'json_test_126',
        'crawl_id': 'json_test_456',
        'platform': 'facebook',
        'competitor': 'test_competitor',
        'date_posted': datetime.now().isoformat(),
        'processed_date': datetime.now().isoformat(),
        'grouped_date': datetime.now().strftime('%Y-%m-%d'),
        'post_id': 'test_post_123',
        'post_url': 'https://facebook.com/test',
    }
    
    try:
        errors = client.insert_rows_json(table_id, [test_record_4])
        if errors:
            print(f"❌ Errors: {errors}")
        else:
            print("✅ Success! (Required fields only)")
    except Exception as e:
        print(f"❌ Exception: {str(e)}")

if __name__ == '__main__':
    test_different_insertion_methods()