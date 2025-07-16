#!/usr/bin/env python3
"""
Integration test for data-processing service with real GCS data.

This test mocks a data-ingestion-completed event using actual GCS data
to test the complete data processing pipeline.
"""

import json
import base64
import requests
import logging
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_mock_data_ingestion_completed_event() -> Dict[str, Any]:
    """
    Create a mock data-ingestion-completed event that matches the format
    published by the data-ingestion service.
    
    Based on event_publisher.py:publish_data_ingestion_completed()
    """
    
    # Use real GCS path from our bucket
    real_gcs_path = "gs://social-analytics-raw-data/raw_snapshots/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12/snapshot_s_md0frwedjgcpd3405.json"
    
    # Create event data matching the data-ingestion service format
    event_data = {
        'event_type': 'data-ingestion-completed',
        'timestamp': datetime.utcnow().isoformat(),
        'source': 'data-ingestion-service',
        'data': {
            'crawl_id': 'test-crawl-integration-001',
            'snapshot_id': 's_md0frwedjgcpd3405',
            'gcs_path': real_gcs_path,
            'post_count': 5,  # We'll verify this after processing
            'media_count': 2,  # We'll verify this after processing
            'status': 'completed',
            # Include metadata that data-processing expects
            'platform': 'facebook',
            'competitor': 'nutifood', 
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em'
        }
    }
    
    return event_data

def create_pubsub_push_message(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a Pub/Sub push message format that Cloud Run receives.
    
    This matches the format that Pub/Sub sends to push endpoints.
    """
    
    # Convert event data to JSON and encode as base64
    message_json = json.dumps(event_data)
    message_b64 = base64.b64encode(message_json.encode('utf-8')).decode('utf-8')
    
    # Create Pub/Sub push message format
    pubsub_message = {
        "message": {
            "data": message_b64,
            "attributes": {
                "eventType": "data-ingestion-completed",
                "source": "data-ingestion-service"
            },
            "messageId": f"test-message-{datetime.utcnow().timestamp()}",
            "publishTime": datetime.utcnow().isoformat() + "Z"
        },
        "subscription": "projects/social-analytics-prod/subscriptions/data-processing-ingestion-events"
    }
    
    return pubsub_message

def test_local_data_processing_service():
    """
    Test the data-processing service locally by sending a mock event.
    """
    logger.info("=== Starting Data Processing Service Integration Test ===")
    
    # Create mock event
    event_data = create_mock_data_ingestion_completed_event()
    pubsub_message = create_pubsub_push_message(event_data)
    
    logger.info(f"Created mock event for crawl_id: {event_data['data']['crawl_id']}")
    logger.info(f"Using real GCS path: {event_data['data']['gcs_path']}")
    
    # Test locally (assuming service is running on localhost:8080)
    local_url = "http://localhost:8080/api/v1/events/data-ingestion-completed"
    
    try:
        logger.info("Sending mock event to local data-processing service...")
        
        response = requests.post(
            local_url,
            json=pubsub_message,
            headers={'Content-Type': 'application/json'},
            timeout=300  # 5 minutes timeout for processing
        )
        
        logger.info(f"Response Status: {response.status_code}")
        logger.info(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            logger.info("✅ SUCCESS: Data processing completed successfully!")
            logger.info(f"Processing Results: {json.dumps(result, indent=2)}")
            
            # Validate expected results
            if result.get('success', False):
                logger.info(f"📊 Processed Posts: {result.get('processed_posts', 0)}")
                logger.info(f"📁 Media Processing Requested: {result.get('media_processing_requested', False)}")
                logger.info(f"⏱️  Processing Duration: {result.get('processing_duration_seconds', 0):.2f}s")
                
                return True
            else:
                logger.error(f"❌ Processing failed: {result}")
                return False
                
        else:
            logger.error(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        logger.error("❌ Connection failed: Is the data-processing service running locally?")
        logger.info("💡 Start the service with: python app.py")
        return False
        
    except requests.exceptions.Timeout:
        logger.error("❌ Request timeout: Processing took longer than 5 minutes")
        return False
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        return False


def verify_gcs_data_exists():
    """
    Verify that the GCS data file exists and can be accessed.
    """
    logger.info("=== Verifying GCS Data Availability ===")
    
    from google.cloud import storage
    
    try:
        storage_client = storage.Client()
        
        # Parse GCS path
        gcs_path = "gs://social-analytics-raw-data/raw_snapshots/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12/snapshot_s_md0frwedjgcpd3405.json"
        bucket_name = "social-analytics-raw-data"
        blob_name = "raw_snapshots/platform=facebook/competitor=nutifood/brand=growplus-nutifood/category=sua-bot-tre-em/year=2025/month=07/day=12/snapshot_s_md0frwedjgcpd3405.json"
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if blob.exists():
            # Reload blob to get metadata
            blob.reload()
            blob_size = blob.size or 0
            logger.info(f"✅ GCS file exists: {gcs_path}")
            logger.info(f"📁 File size: {blob_size} bytes ({blob_size/1024:.1f} KB)")
            
            # Try to read a small portion to verify format
            try:
                content_preview = blob.download_as_text(start=0, end=min(500, blob_size))
                logger.info(f"📄 Content preview: {content_preview[:200]}...")
            except Exception as e:
                logger.warning(f"Could not preview content: {str(e)}")
            
            return True
        else:
            logger.error(f"❌ GCS file does not exist: {gcs_path}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error verifying GCS data: {str(e)}")
        return False

def main():
    """
    Run the local integration test suite.
    """
    logger.info("🚀 Starting Local Data Processing Service Integration Tests")
    
    print("\n" + "="*60)
    print("DATA PROCESSING SERVICE - LOCAL INTEGRATION TEST")
    print("="*60)
    
    # Step 1: Verify GCS data exists
    print("\n1. Verifying GCS Data Availability...")
    if not verify_gcs_data_exists():
        print("❌ FAILED: GCS data verification failed")
        return False
    
    # Step 2: Test local service
    print("\n2. Testing Local Data Processing Service...")
    local_success = test_local_data_processing_service()
    
    # Summary
    print("\n" + "="*60)
    print("LOCAL INTEGRATION TEST SUMMARY")
    print("="*60)
    print(f"✅ GCS Data Available: True")
    print(f"{'✅' if local_success else '❌'} Local Service: {'PASSED' if local_success else 'FAILED'}")
    
    if local_success:
        print("\n🎉 Local integration test completed successfully!")
        print("💡 Ready for deployment testing when needed")
        return True
    else:
        print("\n💥 Local integration test failed!")
        print("🔧 Fix local issues before considering deployment")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)