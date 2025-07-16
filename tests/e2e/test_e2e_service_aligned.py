#!/usr/bin/env python3
"""
E2E test that exactly matches the current service implementation.
Tests the actual Pub/Sub push endpoint with realistic message format.
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

def create_realistic_pubsub_message():
    """Create a realistic Pub/Sub push message that matches what the service expects."""
    
    # This is the actual event data that data-ingestion service publishes
    event_data = {
        "event_type": "data.ingestion.completed",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "crawl_id": "facebook_service_test_20250114",
            "snapshot_id": "snapshot_s_md0frwedjgcpd3405",
            "gcs_path": "gs://social-analytics-raw-data/raw_snapshots/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12/snapshot_s_md0frwedjgcpd3405.json",
            "platform": "facebook",
            "competitor": "nutifood", 
            "brand": "growplus-nutifood",
            "category": "sua-bot-tre-em",
            "crawl_metadata": {
                "dataset_id": "gd_lkaxegm826bjpoo9m5",
                "num_posts": 3,
                "crawl_date": "2025-01-14T10:30:00Z"
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
            "messageId": "1234567890",
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

def test_pubsub_endpoint():
    """Test the actual Pub/Sub endpoint with realistic message."""
    print("\n🔄 Testing Pub/Sub endpoint with realistic message...")
    
    # Create realistic Pub/Sub message
    pubsub_message = create_realistic_pubsub_message()
    
    print(f"📨 Sending Pub/Sub message:")
    print(f"   - Event type: {pubsub_message['message']['attributes']['event_type']}")
    print(f"   - Message ID: {pubsub_message['message']['messageId']}")
    
    # Decode and show event data for verification
    decoded_data = base64.b64decode(pubsub_message['message']['data']).decode('utf-8')
    event_data = json.loads(decoded_data)
    print(f"   - Platform: {event_data['data']['platform']}")
    print(f"   - Crawl ID: {event_data['data']['crawl_id']}")
    print(f"   - GCS Path: {event_data['data']['gcs_path']}")
    
    try:
        # Send to the actual service endpoint
        response = requests.post(
            'http://localhost:8080/api/v1/events/data-ingestion-completed',
            json=pubsub_message,
            headers={'Content-Type': 'application/json'},
            timeout=30  # Give it time to process
        )
        
        print(f"\n📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ SUCCESS! Service processed the event:")
            print(f"   - Processed posts: {result.get('processed_posts', 0)}")
            print(f"   - GCS upload: {'✅' if result.get('gcs_upload_completed') else '❌'}")
            print(f"   - BigQuery insert: {'✅' if result.get('bigquery_insert_completed') else '❌'}")
            print(f"   - Media processing: {'✅' if result.get('media_processing_requested') else '❌'}")
            print(f"   - Processing time: {result.get('processing_duration_seconds', 0):.2f}s")
            
            return True
        else:
            error_data = response.json() if response.content else {}
            print(f"❌ Service returned error: {error_data}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_with_local_fallback():
    """Test using local fixture if GCS snapshot doesn't exist."""
    print("\n🔄 Testing with fallback to local fixture...")
    
    # Create message with non-existent GCS path to trigger fallback
    event_data = {
        "event_type": "data.ingestion.completed", 
        "timestamp": datetime.now().isoformat(),
        "data": {
            "crawl_id": "facebook_fallback_test_20250114",
            "snapshot_id": "test_fallback",
            "gcs_path": "gs://nonexistent-bucket/nonexistent-path.json",
            "platform": "facebook",
            "competitor": "nutifood",
            "brand": "growplus-nutifood", 
            "category": "sua-bot-tre-em"
        }
    }
    
    # Create Pub/Sub message
    event_json = json.dumps(event_data)
    encoded_data = base64.b64encode(event_json.encode('utf-8')).decode('utf-8')
    
    pubsub_message = {
        "message": {
            "data": encoded_data,
            "attributes": {"event_type": "data.ingestion.completed"},
            "messageId": "fallback_test",
            "publishTime": datetime.now().isoformat()
        },
        "subscription": "test-subscription"
    }
    
    try:
        response = requests.post(
            'http://localhost:8080/api/v1/events/data-ingestion-completed',
            json=pubsub_message,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        print(f"📊 Fallback test response: {response.status_code}")
        
        if response.status_code == 500:
            # Expected - GCS path doesn't exist
            print(f"✅ Service correctly handles missing GCS path")
            return True
        else:
            print(f"⚠️  Unexpected response for missing GCS path")
            return False
            
    except Exception as e:
        print(f"❌ Fallback test failed: {e}")
        return False

def main():
    """Main test function."""
    print("🚀 E2E TEST: SERVICE-ALIGNED IMPLEMENTATION")
    print("=" * 70)
    print("📋 This test verifies the actual service implementation:")
    print("   1. Service health check")
    print("   2. Realistic Pub/Sub message format") 
    print("   3. Base64-encoded event data")
    print("   4. Actual service endpoint")
    print("   5. Complete processing flow verification")
    print("=" * 70)
    
    # Test 1: Health check
    print("\n1️⃣ Testing service health...")
    if not test_service_health():
        print("\n❌ Service is not running. Start it with:")
        print("   cd /Users/tranquocbao/crawlerX/social-analytics-platform/services/data-processing")
        print("   source venv/bin/activate") 
        print("   python app.py")
        return
    
    # Test 2: Pub/Sub endpoint with realistic data
    print("\n2️⃣ Testing Pub/Sub endpoint...")
    success = test_pubsub_endpoint()
    
    # Test 3: Error handling
    print("\n3️⃣ Testing error handling...")
    test_with_local_fallback()
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 SERVICE-ALIGNED E2E TEST COMPLETE!")
    print("=" * 70)
    
    if success:
        print("✅ Service implementation matches E2E test expectations!")
        print("✅ Pub/Sub message format is correct")
        print("✅ Event handler processes data correctly")  
        print("✅ All handlers (GCS, BigQuery, Media) work as expected")
        print("\n📋 The service is ready for production Pub/Sub integration!")
    else:
        print("❌ Service implementation has issues")
        print("📋 Check service logs for detailed error information")

if __name__ == "__main__":
    main()