#!/usr/bin/env python3
"""
Real BigQuery insertion test using fixture data.

This test actually pushes fixture data to BigQuery tables to verify
the complete end-to-end data flow works correctly.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'handlers'))

from schema_mapper import SchemaMapper
from bigquery_handler import BigQueryHandler


def test_facebook_real_insertion():
    """Test real Facebook data insertion to BigQuery."""
    print("🧪 Testing Facebook real BigQuery insertion...")
    
    # Load Facebook test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-facebook-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Use first post for testing
    raw_facebook_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': f'test_fb_{int(datetime.now().timestamp())}',
        'snapshot_id': 'test_snapshot_fb_456',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.utcnow().isoformat()
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_facebook_post, 'facebook', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Post ID: {transformed_post.get('post_id')}")
    print(f"   Page name: {transformed_post.get('page_name')}")
    
    # Step 2: Insert to BigQuery
    handler = BigQueryHandler()
    
    try:
        result = handler.insert_posts([transformed_post], metadata, platform='facebook')
        
        if result['success']:
            print(f"✅ BigQuery insertion successful!")
            print(f"   Rows inserted: {result['rows_inserted']}")
            print(f"   Table: {result['table_id']}")
            return True
        else:
            print(f"❌ BigQuery insertion failed")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery insertion error: {str(e)}")
        return False


def test_tiktok_real_insertion():
    """Test real TikTok data insertion to BigQuery."""
    print("\n🧪 Testing TikTok real BigQuery insertion...")
    
    # Load TikTok test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-tiktok-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Use first post for testing
    raw_tiktok_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': f'test_tt_{int(datetime.now().timestamp())}',
        'snapshot_id': 'test_snapshot_tt_789',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.utcnow().isoformat()
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_tiktok_post, 'tiktok', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Video ID: {transformed_post.get('video_id')}")
    print(f"   Author: {transformed_post.get('author_name')}")
    
    # Step 2: Insert to BigQuery
    handler = BigQueryHandler()
    
    try:
        result = handler.insert_posts([transformed_post], metadata, platform='tiktok')
        
        if result['success']:
            print(f"✅ BigQuery insertion successful!")
            print(f"   Rows inserted: {result['rows_inserted']}")
            print(f"   Table: {result['table_id']}")
            return True
        else:
            print(f"❌ BigQuery insertion failed")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery insertion error: {str(e)}")
        return False


def test_youtube_real_insertion():
    """Test real YouTube data insertion to BigQuery."""
    print("\n🧪 Testing YouTube real BigQuery insertion...")
    
    # Load YouTube test fixture
    fixture_path = Path(__file__).parent.parent.parent / 'fixtures' / 'gcs-youtube-posts.json'
    with open(fixture_path, 'r') as f:
        test_data = json.load(f)
    
    # Use first post for testing
    raw_youtube_post = test_data[0]
    
    # Simulate crawl metadata
    metadata = {
        'crawl_id': f'test_yt_{int(datetime.now().timestamp())}',
        'snapshot_id': 'test_snapshot_yt_101',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.utcnow().isoformat()
    }
    
    # Step 1: Transform with SchemaMapper
    mapper = SchemaMapper()
    transformed_post = mapper.transform_post(raw_youtube_post, 'youtube', metadata)
    
    print(f"✅ SchemaMapper transformation successful")
    print(f"   Platform: {transformed_post.get('platform')}")
    print(f"   Video ID: {transformed_post.get('video_id')}")
    print(f"   Channel: {transformed_post.get('channel_name')}")
    
    # Step 2: Insert to BigQuery
    handler = BigQueryHandler()
    
    try:
        result = handler.insert_posts([transformed_post], metadata, platform='youtube')
        
        if result['success']:
            print(f"✅ BigQuery insertion successful!")
            print(f"   Rows inserted: {result['rows_inserted']}")
            print(f"   Table: {result['table_id']}")
            return True
        else:
            print(f"❌ BigQuery insertion failed")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery insertion error: {str(e)}")
        return False


def verify_bigquery_data():
    """Query BigQuery to verify inserted data."""
    print("\n🔍 Verifying data in BigQuery tables...")
    
    handler = BigQueryHandler()
    
    # Check each platform table
    platforms = ['facebook', 'tiktok', 'youtube']
    
    for platform in platforms:
        table_name = handler._get_platform_table(platform)
        
        try:
            # Simple count query
            query = f"""
            SELECT COUNT(*) as row_count, 
                   MIN(processed_date) as oldest_record,
                   MAX(processed_date) as newest_record
            FROM `{table_name}`
            WHERE crawl_id LIKE 'test_%'
            """
            
            results = handler.client.query(query).to_dataframe()
            
            if not results.empty:
                row_count = results.iloc[0]['row_count']
                oldest = results.iloc[0]['oldest_record']
                newest = results.iloc[0]['newest_record']
                
                print(f"✅ {platform.capitalize()} table verification:")
                print(f"   Rows: {row_count}")
                print(f"   Date range: {oldest} to {newest}")
            else:
                print(f"⚠️  {platform.capitalize()} table: No test data found")
                
        except Exception as e:
            print(f"❌ {platform.capitalize()} table verification failed: {str(e)}")


def main():
    """Run all real BigQuery insertion tests."""
    print("🚀 Starting REAL BigQuery insertion tests...")
    print("⚠️  This will actually insert data to BigQuery tables!")
    
    # Verify we have proper authentication
    try:
        handler = BigQueryHandler()
        print(f"✅ BigQuery client initialized")
        print(f"   Project: {handler.project_id}")
        print(f"   Dataset: {handler.dataset_id}")
    except Exception as e:
        print(f"❌ BigQuery authentication failed: {str(e)}")
        print("💡 Make sure you're authenticated with gcloud auth login")
        return 1
    
    successes = 0
    total_tests = 3
    
    try:
        # Test Facebook insertion
        if test_facebook_real_insertion():
            successes += 1
        
        # Test TikTok insertion
        if test_tiktok_real_insertion():
            successes += 1
        
        # Test YouTube insertion  
        if test_youtube_real_insertion():
            successes += 1
        
        # Verify the data was inserted
        verify_bigquery_data()
        
        print(f"\n📊 Results: {successes}/{total_tests} platforms inserted successfully")
        
        if successes == total_tests:
            print("🎉 All platform BigQuery insertions successful!")
            print("✅ Multi-platform schema system working perfectly!")
            return 0
        else:
            print("⚠️  Some insertions failed. Check errors above.")
            return 1
            
    except Exception as e:
        print(f"\n❌ Test execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())