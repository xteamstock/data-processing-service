#!/usr/bin/env python3
"""
End-to-end test for data-processing service:
1. Simulate receiving a Pub/Sub message from data-ingestion service
2. Download raw snapshot from GCS
3. Transform data using schema mapper
4. Upload grouped data to GCS
5. Insert transformed data into BigQuery
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from google.cloud import storage, bigquery
import traceback

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from handlers.schema_mapper import SchemaMapper
from handlers.gcs_processed_handler import GCSProcessedHandler
from handlers.bigquery_handler import BigQueryHandler

def simulate_pubsub_message():
    """Simulate a Pub/Sub message from data-ingestion service."""
    # This is what data-ingestion service publishes
    return {
        "event_type": "data.ingestion.completed",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "snapshot_id": "snapshot_s_md0frwedjgcpd3405",
            "gcs_path": "gs://social-analytics-raw-data/raw_snapshots/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12/snapshot_s_md0frwedjgcpd3405.json",
            "platform": "facebook",
            "competitor": "nutifood",
            "brand": "growplus-nutifood",
            "category": "sua-bot-tre-em",
            "crawl_metadata": {
                "crawl_id": "facebook_e2e_test_20250114",
                "crawl_date": "2025-01-14T10:30:00Z",
                "num_posts": 5,
                "dataset_id": "gd_lkaxegm826bjpoo9m5"
            }
        }
    }

def download_from_gcs(gcs_path):
    """Download raw snapshot from GCS."""
    print(f"\n📥 Downloading from GCS: {gcs_path}")
    
    # Parse GCS path
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    
    path_parts = gcs_path[5:].split("/", 1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1]
    
    print(f"   - Bucket: {bucket_name}")
    print(f"   - Blob: {blob_name}")
    
    # Download from GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        raise FileNotFoundError(f"Blob not found: {blob_name}")
    
    # Download as JSON
    content = blob.download_as_text()
    data = json.loads(content)
    
    print(f"✅ Downloaded snapshot: {len(data)} posts")
    return data

def process_posts(posts, platform, metadata):
    """Transform posts using schema mapper."""
    print(f"\n🔄 Processing {len(posts)} {platform} posts...")
    
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    transformed_posts = []
    failed_count = 0
    
    for i, raw_post in enumerate(posts):
        try:
            # Add unique ID for each post
            transform_metadata = {
                **metadata,
                'crawl_id': f"{metadata['crawl_id']}_{i}"
            }
            
            transformed_post = schema_mapper.transform_post(raw_post, platform, transform_metadata)
            
            # Flatten processing metadata
            if 'processing_metadata' in transformed_post:
                processing_meta = transformed_post.pop('processing_metadata')
                transformed_post['schema_version'] = processing_meta.get('schema_version')
                transformed_post['processing_version'] = processing_meta.get('processing_version')
                transformed_post['data_quality_score'] = processing_meta.get('data_quality_score')
            
            # Clean for BigQuery (remove nested objects)
            cleaned = {}
            for key, value in transformed_post.items():
                if not isinstance(value, dict):
                    cleaned[key] = value
            
            transformed_posts.append(cleaned)
            
        except Exception as e:
            print(f"⚠️  Failed to transform post {i}: {e}")
            failed_count += 1
    
    print(f"✅ Transformed: {len(transformed_posts)} posts")
    if failed_count > 0:
        print(f"⚠️  Failed: {failed_count} posts")
    
    return transformed_posts

def upload_to_gcs_grouped(transformed_posts, platform, metadata):
    """Upload grouped data to GCS."""
    print(f"\n📤 Uploading grouped data to GCS...")
    
    gcs_handler = GCSProcessedHandler()
    
    # Group by date
    grouped_data = {}
    for post in transformed_posts:
        date_key = post.get('grouped_date', 'unknown')
        if date_key not in grouped_data:
            grouped_data[date_key] = []
        grouped_data[date_key].append(post)
    
    # Prepare metadata for upload
    upload_metadata = {
        'platform': platform,
        'competitor': metadata['competitor'],
        'brand': metadata['brand'],
        'category': metadata['category'],
        'crawl_id': metadata['crawl_id'],
        'snapshot_id': metadata['snapshot_id']
    }
    
    # Upload using GCS handler
    success, error, stats = gcs_handler.upload_grouped_data(grouped_data, upload_metadata)
    
    if success:
        print(f"✅ Upload successful!")
        print(f"   - Total files: {stats['total_files']}")
        print(f"   - Total records: {stats['total_records']}")
        print(f"   - Successful uploads: {stats['successful_uploads']}")
        for file_info in stats['uploaded_files']:
            print(f"   - {file_info['date']}: {file_info['record_count']} posts → {file_info['file_path']}")
    else:
        print(f"❌ Upload failed: {error}")
        if 'failed_files' in stats:
            for file_info in stats['failed_files']:
                print(f"   - Failed: {file_info['date']}")
    
    return stats.get('uploaded_files', [])

def insert_to_bigquery(transformed_posts, platform, metadata):
    """Insert transformed data to BigQuery."""
    print(f"\n💾 Inserting to BigQuery...")
    
    bq_handler = BigQueryHandler()
    
    # Insert to BigQuery using the platform parameter
    # The handler will determine the appropriate table
    try:
        result = bq_handler.insert_posts(transformed_posts, metadata=metadata, platform=platform)
        
        if result['success']:
            print(f"✅ Successfully inserted {result['rows_inserted']} posts to {result['table_id']}")
            
            # Verify with query
            client = bigquery.Client()
            query = f"""
            SELECT 
                COUNT(*) as count,
                MIN(date_posted) as earliest,
                MAX(date_posted) as latest,
                AVG(data_quality_score) as avg_quality
            FROM `{result['table_id']}`
            WHERE crawl_id LIKE '{metadata['crawl_id']}%'
            """
            
            results = list(client.query(query))
            if results:
                row = results[0]
                print(f"\n📊 Verification:")
                print(f"   - Posts in BigQuery: {row.count}")
                print(f"   - Date range: {row.earliest} to {row.latest}")
                print(f"   - Avg quality score: {row.avg_quality:.3f}")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Failed to insert to BigQuery: {e}")
        import traceback
        traceback.print_exc()
        
        # Print first transformed post for debugging
        if transformed_posts:
            print(f"\n🔍 Sample transformed post:")
            first_post = transformed_posts[0]
            for key, value in first_post.items():
                print(f"   - {key}: {type(value).__name__} = {value}")
        
        return False

def main():
    """Main end-to-end test function."""
    print("🚀 END-TO-END DATA PROCESSING TEST")
    print("=" * 70)
    
    try:
        # 1. Simulate receiving Pub/Sub message
        print("\n1️⃣ Simulating Pub/Sub message from data-ingestion service...")
        message = simulate_pubsub_message()
        print(f"   - Event: {message['event_type']}")
        print(f"   - Platform: {message['data']['platform']}")
        print(f"   - Snapshot: {message['data']['snapshot_id']}")
        print(f"   - GCS Path: {message['data']['gcs_path']}")
        
        # Extract metadata
        data = message['data']
        metadata = {
            'snapshot_id': data['snapshot_id'],
            'competitor': data['competitor'],
            'brand': data['brand'],
            'category': data['category'],
            'crawl_id': data['crawl_metadata']['crawl_id'],
            'crawl_date': data['crawl_metadata']['crawl_date']
        }
        
        # 2. Download raw snapshot from GCS
        print("\n2️⃣ Downloading raw snapshot from GCS...")
        try:
            raw_posts = download_from_gcs(data['gcs_path'])
        except FileNotFoundError:
            print("⚠️  Snapshot not found in GCS. Using local fixture as fallback...")
            # Fallback to local fixture for testing
            fixture_path = Path(__file__).parent / "fixtures" / f"gcs-{data['platform']}-posts.json"
            with open(fixture_path, 'r', encoding='utf-8') as f:
                raw_posts = json.load(f)
            print(f"   ✅ Loaded {len(raw_posts)} posts from local fixture")
        
        # 3. Transform posts
        print("\n3️⃣ Transforming posts with schema mapper...")
        transformed_posts = process_posts(raw_posts, data['platform'], metadata)
        
        if not transformed_posts:
            print("❌ No posts were successfully transformed")
            return
        
        # 4. Upload grouped data to GCS
        print("\n4️⃣ Uploading grouped data to GCS...")
        uploaded_paths = upload_to_gcs_grouped(transformed_posts, data['platform'], metadata)
        print(f"   ✅ Uploaded to {len(uploaded_paths)} date groups")
        
        # 5. Insert to BigQuery
        print("\n5️⃣ Inserting to BigQuery...")
        success = insert_to_bigquery(transformed_posts, data['platform'], metadata)
        
        # Summary
        print("\n" + "=" * 70)
        print("🎯 END-TO-END TEST COMPLETE!")
        print("=" * 70)
        print(f"✅ Downloaded: {len(raw_posts)} raw posts")
        print(f"✅ Transformed: {len(transformed_posts)} posts")
        print(f"✅ Uploaded: {len(uploaded_paths)} grouped files to GCS")
        print(f"✅ Inserted: {'Success' if success else 'Failed'} to BigQuery")
        print("\n📋 This simulates the complete data-processing flow:")
        print("   1. Receive Pub/Sub message from data-ingestion")
        print("   2. Download raw snapshot from GCS")
        print("   3. Transform with schema mapper (compute fields, etc.)")
        print("   4. Upload grouped data to GCS")
        print("   5. Insert flattened data to BigQuery")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()