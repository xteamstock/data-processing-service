#!/usr/bin/env python3
"""
Script to recreate BigQuery tables with updated schemas.
"""

import os
from google.cloud import bigquery

def recreate_tables():
    """Drop and recreate all platform tables."""
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = 'social_analytics'
    
    tables_to_recreate = [
        'facebook_posts',
        'tiktok_posts', 
        'youtube_posts'
    ]
    
    for table_name in tables_to_recreate:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        try:
            # Drop existing table
            client.delete_table(table_id, not_found_ok=True)
            print(f"Dropped table {table_id}")
        except Exception as e:
            print(f"Error dropping {table_id}: {e}")
    
    print("Tables dropped. Now run create_bigquery_tables.py to recreate with updated schemas.")

if __name__ == "__main__":
    recreate_tables()