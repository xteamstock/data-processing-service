#!/usr/bin/env python3
"""
Test script to verify Pub/Sub integration between data-ingestion and data-processing services.

This script simulates the data-ingestion service publishing an event to test 
the data-processing service's ability to receive and process events.
"""

import os
import json
import time
import logging
from datetime import datetime
from google.cloud import pubsub_v1

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PubSubIntegrationTest:
    """Test class for Pub/Sub integration between services."""
    
    def __init__(self):
        """Initialize the test."""
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.topic_name = 'social-analytics-data-ingestion-completed'
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
    
    def publish_test_event(self):
        """Publish a test data-ingestion-completed event."""
        try:
            # Create test event data
            event_data = {
                'event_type': 'data-ingestion-completed',
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'data-ingestion-service',
                'data': {
                    'crawl_id': 'test-crawl-123',
                    'snapshot_id': 'test-snapshot-456',
                    'gcs_path': 'gs://social-analytics-raw-data/test-data/crawl-123.json',
                    'post_count': 5,
                    'media_count': 3,
                    'status': 'completed',
                    'platform': 'facebook',
                    'competitor': 'test-competitor',
                    'brand': 'test-brand',
                    'category': 'test-category'
                }
            }
            
            # Convert to JSON bytes
            message_data = json.dumps(event_data).encode('utf-8')
            
            logger.info(f"Publishing test event to topic: {self.topic_name}")
            logger.info(f"Event data: {json.dumps(event_data, indent=2)}")
            
            # Publish the message
            future = self.publisher.publish(self.topic_path, message_data)
            message_id = future.result()  # Wait for publish to complete
            
            logger.info(f"✅ Test event published successfully with message ID: {message_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error publishing test event: {str(e)}")
            return False
    
    def verify_subscription_exists(self):
        """Verify that the push subscription exists."""
        try:
            subscriber = pubsub_v1.SubscriberClient()
            subscription_name = 'data-processing-ingestion-events'
            subscription_path = subscriber.subscription_path(self.project_id, subscription_name)
            
            # Try to get the subscription
            subscription = subscriber.get_subscription(request={"subscription": subscription_path})
            logger.info(f"✅ Subscription exists: {subscription.name}")
            logger.info(f"   Push endpoint: {subscription.push_config.push_endpoint}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error verifying subscription: {str(e)}")
            return False
    
    def check_service_health(self):
        """Check if the data-processing service is healthy."""
        try:
            import requests
            
            # Get auth token
            import subprocess
            result = subprocess.run(['gcloud', 'auth', 'print-identity-token'], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.error("Failed to get auth token")
                return False
            
            token = result.stdout.strip()
            
            # Check service health
            url = 'https://data-processing-service-ud5pi5bwfq-as.a.run.app/health'
            headers = {'Authorization': f'Bearer {token}'}
            
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Data-processing service is healthy: {response.json()}")
                return True
            else:
                logger.error(f"❌ Service health check failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error checking service health: {str(e)}")
            return False

def main():
    """Run the integration test."""
    logger.info("🚀 Starting Pub/Sub integration test...")
    
    test = PubSubIntegrationTest()
    
    # Step 1: Verify subscription exists
    logger.info("Step 1: Verifying push subscription exists...")
    if not test.verify_subscription_exists():
        logger.error("❌ Push subscription verification failed")
        return 1
    
    # Step 2: Check service health
    logger.info("Step 2: Checking data-processing service health...")
    if not test.check_service_health():
        logger.error("❌ Service health check failed")
        return 1
    
    # Step 3: Publish test event
    logger.info("Step 3: Publishing test event...")
    if not test.publish_test_event():
        logger.error("❌ Test event publishing failed")
        return 1
    
    # Step 4: Wait and provide monitoring instructions
    logger.info("Step 4: Event published successfully!")
    logger.info("📊 To monitor the event processing:")
    logger.info("   1. Check Cloud Run logs: gcloud run services logs read data-processing-service --region=asia-southeast1 --limit=20")
    logger.info("   2. Check BigQuery for processed data: SELECT * FROM `competitor-destroyer.social_analytics.processing_events` ORDER BY event_timestamp DESC LIMIT 10")
    logger.info("   3. Monitor Pub/Sub subscription: gcloud pubsub subscriptions describe data-processing-ingestion-events")
    
    logger.info("✅ Integration test completed successfully!")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())