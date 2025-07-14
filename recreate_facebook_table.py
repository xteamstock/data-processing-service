#!/usr/bin/env python3
"""
Recreate Facebook table with correct JSON schema.
"""

import os
from google.cloud import bigquery

def recreate_facebook_table():
    """Drop and recreate Facebook table with explicit JSON fields."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    table_id = f"{project_id}.{dataset_id}.facebook_posts"
    
    # Drop existing table
    try:
        client.delete_table(table_id)
        print(f"✅ Dropped existing table: {table_id}")
    except Exception as e:
        print(f"⚠️  Table might not exist: {str(e)}")
    
    # Create schema with explicit JSON fields
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("user_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_profile_id", "STRING", mode="NULLABLE"),
        
        # Facebook-specific fields
        bigquery.SchemaField("post_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_verified", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("page_followers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("page_likes", "INT64", mode="NULLABLE"),
        
        # JSON fields - explicitly specified
        bigquery.SchemaField("engagement_metrics", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("content_analysis", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("media_metadata", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("page_metadata", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("processing_metadata", "JSON", mode="NULLABLE"),
    ]
    
    # Create table
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Facebook posts analytics table with JSON fields"
    
    # Set partitioning
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date_posted"
    )
    
    # Set clustering
    table.clustering_fields = ["competitor", "brand", "page_name"]
    
    try:
        table = client.create_table(table)
        print(f"✅ Created table: {table_id}")
        
        # Verify the schema
        print("\n=== VERIFIED SCHEMA ===")
        for field in table.schema:
            if field.name in ['engagement_metrics', 'content_analysis', 'media_metadata', 'page_metadata', 'processing_metadata']:
                print(f"  {field.name}: {field.field_type} ({field.mode})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {str(e)}")
        return False

if __name__ == '__main__':
    recreate_facebook_table()