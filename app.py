# app.py
# NEW: Flask app with Pub/Sub push endpoint

from flask import Flask, request, jsonify
from events.event_handler import EventHandler
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize event handler
event_handler = EventHandler()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'data-processing',
        'version': '1.0.0',
        'environment': os.getenv('GOOGLE_CLOUD_PROJECT', 'unknown')
    })

@app.route('/api/v1/events/data-ingestion-completed', methods=['POST'])
def handle_data_ingestion_completed():
    """
    Handle data-ingestion-completed events from Pub/Sub push.
    
    This is the main entry point for the data processing service.
    It receives push notifications from Pub/Sub when the Data Ingestion
    Service completes raw data collection.
    """
    try:
        result, status_code = event_handler.handle_data_ingestion_completed(request)
        return jsonify(result), status_code
        
    except Exception as e:
        logger.error(f"Error in data ingestion handler: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/v1/test', methods=['POST'])
def test_endpoint():
    """Test endpoint for manual testing."""
    try:
        test_data = request.json
        logger.info(f"Test endpoint called with data: {test_data}")
        
        return jsonify({
            'message': 'Test endpoint working',
            'received_data': test_data
        })
        
    except Exception as e:
        logger.error(f"Error in test endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)