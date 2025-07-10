# Data Processing Service

Social media data processing microservice with dual-output jobs (GCS + BigQuery + Media Events).

## 🚀 Auto-Deployment Setup

This service auto-deploys to Cloud Run when you push to the main branch.

### Cloud Run Configuration
- **Service**: data-processing-service  
- **Region**: asia-southeast1
- **Project**: competitor-destroyer

### Environment Variables (Auto-configured)
- `GOOGLE_CLOUD_PROJECT=competitor-destroyer`
- `BIGQUERY_DATASET=social_analytics`
- `PUBSUB_TOPIC_PREFIX=social-analytics`
- `GCS_BUCKET_RAW_DATA=social-analytics-raw-data`

### Dual-Output Jobs
1. **Job 1**: GCS processed data upload (grouped by date)
2. **Job 2**: BigQuery analytics insertion (schema-driven)
3. **Job 3**: Media detection and event publishing

## 📋 API Endpoints

- `GET /health` - Health check
- `POST /api/v1/test` - Test endpoint
- `POST /api/v1/events/data-ingestion-completed` - Process ingestion events

## 🔧 Local Development

```bash
pip install -r requirements.txt
python app.py
```