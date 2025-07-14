#!/usr/bin/env python3
"""
Debug BigQuery insertion with minimal test data.
"""

import os
import sys
from datetime import datetime
from google.cloud import bigquery

def test_minimal_insertion():
    """Test minimal data insertion to understand the issue."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    
    # Test Facebook table with minimal data
    table_id = f"{project_id}.{dataset_id}.facebook_posts"
    
    # Minimal test record - exclude empty JSON fields
    test_record = {
        'id': 'debug_test_123',
        'crawl_id': 'debug_crawl_456',
        'platform': 'facebook',
        'competitor': 'test_competitor',
        'date_posted': datetime.now().isoformat(),
        'processed_date': datetime.now().isoformat(),
        'grouped_date': datetime.now().strftime('%Y-%m-%d'),
        'post_id': 'test_post_123',
        'post_url': 'https://facebook.com/test',
        'engagement_metrics': {'likes': 100, 'comments': 10},  # Simple dict
        'content_analysis': {'language': 'en'},  # Simple dict
        'processing_metadata': {'version': '1.0'}  # Simple dict
        # Skip empty dicts: media_metadata, page_metadata
    }
    
    print("🧪 Testing minimal BigQuery insertion...")
    print(f"Table: {table_id}")
    print(f"Record: {test_record}")
    
    try:
        errors = client.insert_rows_json(table_id, [test_record])
        
        if errors:
            print(f"❌ Insertion errors: {errors}")
            
            # Try to understand the specific error
            for error in errors:
                if 'errors' in error:
                    for err in error['errors']:
                        print(f"  Error: {err}")
                        
        else:
            print("✅ Minimal insertion successful!")
            
    except Exception as e:
        print(f"❌ Insertion exception: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_minimal_insertion()