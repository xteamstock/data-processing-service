#!/usr/bin/env python3
"""
Debug script to check actual BigQuery table schemas.
"""

import os
import sys
from google.cloud import bigquery

def check_table_schema():
    """Check the actual schema of our BigQuery tables."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    
    platforms = ['facebook', 'tiktok', 'youtube']
    
    for platform in platforms:
        table_name = f"{platform}_posts"
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        try:
            table = client.get_table(table_id)
            print(f"\n=== {platform.upper()} TABLE SCHEMA ===")
            print(f"Table: {table_id}")
            
            # Focus on the JSON/complex fields
            complex_fields = [
                'engagement_metrics', 'content_analysis', 'media_metadata', 
                'page_metadata', 'processing_metadata', 'video_metadata', 
                'author_metadata', 'channel_metadata'
            ]
            
            for field in table.schema:
                if field.name in complex_fields:
                    print(f"  {field.name}:")
                    print(f"    Type: {field.field_type}")
                    print(f"    Mode: {field.mode}")
                    if field.fields:
                        print(f"    Nested fields: {len(field.fields)}")
                        for nested in field.fields[:3]:  # Show first 3 nested fields
                            print(f"      - {nested.name}: {nested.field_type}")
                    print()
                    
        except Exception as e:
            print(f"❌ Error checking {platform} table: {str(e)}")

if __name__ == '__main__':
    check_table_schema()