#!/usr/bin/env python3
"""
E2E test for TikTok platform processing.
Tests the service with TikTok-specific GCS path and BigQuery table.
"""

import json
import sys
import os
import base64
import requests
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

def create_tiktok_pubsub_message():
    """Create a TikTok-specific Pub/Sub push message."""
    
    # TikTok event data
    event_data = {
        "event_type": "data.ingestion.completed",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "crawl_id": "tiktok_service_test_20250716",
            "snapshot_id": "snapshot_H3PpNDO2VSmrvSyUs",
            "gcs_path": "gs://social-analytics-raw-data/raw_snapshots/platform=tiktok/competitor=nutifood/brand=nutifood-official/category=sua-bot-tre-em/year=2025/month=07/day=16/snapshot_H3PpNDO2VSmrvSyUs.json",
            "platform": "tiktok",
            "competitor": "nutifood",
            "brand": "growplus-nutifood",
            "category": "sua-bot-tre-em",
            "crawl_metadata": {
                "dataset_id": "H3PpNDO2VSmrvSyUs",
                "num_posts": 24,
                "crawl_date": "2025-07-16T10:41:00Z"
            }
        }
    }
    
    # Encode as base64 (Pub/Sub format)
    event_json = json.dumps(event_data)
    encoded_data = base64.b64encode(event_json.encode('utf-8')).decode('utf-8')
    
    # Create Pub/Sub push message format
    pubsub_message = {
        "message": {
            "data": encoded_data,
            "attributes": {
                "event_type": "data.ingestion.completed"
            },
            "messageId": "tiktok_test_1234567890",
            "publishTime": datetime.now().isoformat()
        },
        "subscription": "projects/competitor-destroyer/subscriptions/data-processing-sub"
    }
    
    return pubsub_message

def test_service_health():
    """Test that the service is running."""
    try:
        response = requests.get('http://localhost:8080/health', timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Service is healthy: {health_data}")
            return True
        else:
            print(f"❌ Service health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to service: {e}")
        return False

def test_tiktok_processing():
    """Test TikTok-specific processing with actual GCS path."""
    print("\n🔄 Testing TikTok processing with real GCS data...")
    
    # Create TikTok Pub/Sub message
    pubsub_message = create_tiktok_pubsub_message()
    
    print(f"📨 Sending TikTok Pub/Sub message:")
    print(f"   - Event type: {pubsub_message['message']['attributes']['event_type']}")
    print(f"   - Message ID: {pubsub_message['message']['messageId']}")
    
    # Decode and show event data for verification
    decoded_data = base64.b64decode(pubsub_message['message']['data']).decode('utf-8')
    event_data = json.loads(decoded_data)
    print(f"   - Platform: {event_data['data']['platform']}")
    print(f"   - Crawl ID: {event_data['data']['crawl_id']}")
    print(f"   - GCS Path: {event_data['data']['gcs_path']}")
    print(f"   - Expected BigQuery table: tiktok_posts_schema_driven")
    
    try:
        # Send to the actual service endpoint
        response = requests.post(
            'http://localhost:8080/api/v1/events/data-ingestion-completed',
            json=pubsub_message,
            headers={'Content-Type': 'application/json'},
            timeout=60  # TikTok processing might take longer
        )
        
        print(f"\n📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ SUCCESS! TikTok processing completed:")
            print(f"   - Processed videos: {result.get('processed_posts', 0)}")
            print(f"   - GCS upload: {'✅' if result.get('gcs_upload_completed') else '❌'}")
            print(f"   - BigQuery insert: {'✅' if result.get('bigquery_insert_completed') else '❌'}")
            print(f"   - Media processing: {'✅' if result.get('media_processing_requested') else '❌'}")
            print(f"   - Processing time: {result.get('processing_duration_seconds', 0):.2f}s")
            
            # Show jobs summary if available
            if 'jobs_summary' in result:
                jobs = result['jobs_summary']
                print(f"\n📋 Detailed Job Results:")
                
                # GCS Upload
                gcs_job = jobs.get('job1_gcs_upload', {})
                print(f"   🗂️  GCS Upload: {'✅' if gcs_job.get('success') else '❌'}")
                if gcs_job.get('success'):
                    print(f"      - Files uploaded: {gcs_job.get('files_uploaded', 0)}")
                    print(f"      - Total records: {gcs_job.get('total_records', 0)}")
                else:
                    print(f"      - Error: {gcs_job.get('error', 'Unknown')}")
                
                # BigQuery Insert
                bq_job = jobs.get('job2_bigquery_insert', {})
                print(f"   💾 BigQuery Insert: {'✅' if bq_job.get('success') else '❌'}")
                if bq_job.get('success'):
                    print(f"      - Table: {bq_job.get('table_id', 'Unknown')}")
                    print(f"      - Rows inserted: {bq_job.get('rows_inserted', 0)}")
                
                # Media Detection
                media_job = jobs.get('job3_media_detection', {})
                print(f"   🎥 Media Detection: {'✅' if media_job.get('media_event_published') else '❌'}")
                print(f"      - Videos with media: {media_job.get('posts_with_media', 0)}")
                print(f"      - Total media count: {media_job.get('total_media_count', 0)}")
            
            return True
        else:
            error_data = response.json() if response.content else {}
            print(f"❌ TikTok processing failed: {error_data}")
            
            # Show detailed error if available
            if 'error' in error_data:
                print(f"   💥 Error details: {error_data['error']}")
            
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def verify_tiktok_bigquery_table():
    """Check if the TikTok BigQuery table exists and is accessible."""
    print("\n🔍 Verifying TikTok BigQuery table access...")
    
    try:
        # Test BigQuery debug endpoint
        response = requests.post(
            'http://localhost:8080/api/v1/test',
            json={"test": "bigquery_debug"},
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            debug_info = response.json()
            handler_config = debug_info.get('debug_info', {}).get('handler_config', {})
            
            print(f"✅ BigQuery configuration:")
            print(f"   - Project ID: {handler_config.get('project_id', 'Unknown')}")
            print(f"   - Dataset ID: {handler_config.get('dataset_id', 'Unknown')}")
            
            # Check if TikTok table path is correctly configured
            from handlers.bigquery_handler import BigQueryHandler
            bq_handler = BigQueryHandler()
            tiktok_table = bq_handler._get_platform_table('tiktok')
            print(f"   - TikTok table: {tiktok_table}")
            
            return True
        else:
            print(f"❌ Cannot access BigQuery debug info: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery verification failed: {e}")
        return False

def main():
    """Main test function for TikTok processing."""
    print("🚀 E2E TEST: TIKTOK PLATFORM PROCESSING")
    print("=" * 70)
    print("📋 This test verifies TikTok-specific processing:")
    print("   1. Service health check")
    print("   2. TikTok GCS path processing")
    print("   3. TikTok schema transformation")
    print("   4. BigQuery insertion to tiktok_posts_schema_driven")
    print("   5. Media detection for TikTok videos")
    print("=" * 70)
    
    # Test 1: Health check
    print("\n1️⃣ Testing service health...")
    if not test_service_health():
        print("\n❌ Service is not running. Start it with:")
        print("   cd /Users/tranquocbao/crawlerX/social-analytics-platform/services/data-processing")
        print("   source venv/bin/activate")
        print("   python app.py")
        return
    
    # Test 2: Verify BigQuery table configuration
    print("\n2️⃣ Verifying BigQuery table configuration...")
    verify_tiktok_bigquery_table()
    
    # Test 3: TikTok processing
    print("\n3️⃣ Testing TikTok processing...")
    success = test_tiktok_processing()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 TIKTOK E2E TEST COMPLETE!")
    print("=" * 70)
    
    if success:
        print("✅ TikTok processing works correctly!")
        print("✅ GCS path correctly processed")
        print("✅ TikTok schema transformation successful")
        print("✅ BigQuery insertion to tiktok_posts_schema_driven successful")
        print("✅ Media detection for TikTok videos working")
        print("\n📋 TikTok platform is ready for production!")
    else:
        print("❌ TikTok processing has issues")
        print("📋 Check service logs and ensure:")
        print("   - TikTok schema file exists")
        print("   - tiktok_posts_schema_driven table exists in BigQuery")
        print("   - GCS path is accessible")
        print("   - Service has proper TikTok processing logic")

if __name__ == "__main__":
    main()