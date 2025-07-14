#!/usr/bin/env python3
"""
Recreate all platform tables with correct JSON schema.
"""

import os
from google.cloud import bigquery

def recreate_all_tables():
    """Recreate all platform tables with explicit JSON fields."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    
    # Common core fields
    core_fields = [
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
    ]
    
    platforms = [
        {
            'name': 'tiktok',
            'table_name': 'tiktok_posts',
            'description': 'TikTok posts analytics table with JSON fields',
            'cluster_fields': ['competitor', 'brand', 'user_username'],
            'specific_fields': [
                bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("video_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("author_verified", "BOOL", mode="NULLABLE"),
                bigquery.SchemaField("author_follower_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("play_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("digg_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("share_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("comment_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("engagement_metrics", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("content_analysis", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("video_metadata", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("author_metadata", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("processing_metadata", "JSON", mode="NULLABLE"),
            ]
        },
        {
            'name': 'youtube',
            'table_name': 'youtube_posts',
            'description': 'YouTube videos analytics table with JSON fields',
            'cluster_fields': ['competitor', 'brand', 'channel_name'],
            'specific_fields': [
                bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("video_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("channel_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("channel_verified", "BOOL", mode="NULLABLE"),
                bigquery.SchemaField("channel_subscriber_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("view_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("like_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("comment_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("published_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("engagement_metrics", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("content_analysis", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("video_metadata", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("channel_metadata", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("processing_metadata", "JSON", mode="NULLABLE"),
            ]
        }
    ]
    
    for platform_config in platforms:
        platform_name = platform_config['name']
        table_name = platform_config['table_name']
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        # Drop existing table
        try:
            client.delete_table(table_id)
            print(f"✅ Dropped existing {platform_name} table: {table_id}")
        except Exception as e:
            print(f"⚠️  {platform_name} table might not exist: {str(e)}")
        
        # Create schema
        schema = core_fields + platform_config['specific_fields']
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table.description = platform_config['description']
        
        # Set partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_posted"
        )
        
        # Set clustering
        table.clustering_fields = platform_config['cluster_fields']
        
        try:
            table = client.create_table(table)
            print(f"✅ Created {platform_name} table: {table_id}")
            
            # Verify JSON fields
            json_fields = [f.name for f in table.schema if f.field_type == 'JSON']
            print(f"   JSON fields: {json_fields}")
            
        except Exception as e:
            print(f"❌ Error creating {platform_name} table: {str(e)}")

if __name__ == '__main__':
    recreate_all_tables()