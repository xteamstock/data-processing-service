#!/usr/bin/env python3
"""
Recreate BigQuery tables with flattened schema for easier querying.
All fields become individual columns instead of nested JSON.
"""

import os
from google.cloud import bigquery

def recreate_flattened_tables():
    """Recreate tables with flattened schema - all fields as individual columns."""
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
    
    # TikTok table with flattened schema
    tiktok_table_config = {
        'name': 'tiktok',
        'table_name': 'tiktok_posts_flattened',
        'description': 'TikTok posts with completely flattened schema - no JSON nesting',
        'cluster_fields': ['competitor', 'brand', 'user_username'],
        'specific_fields': [
            # Core TikTok fields
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("video_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_verified", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("author_follower_count", "INT64", mode="NULLABLE"),
            
            # Engagement metrics (flattened)
            bigquery.SchemaField("play_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("digg_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("share_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("comment_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("collect_count", "INT64", mode="NULLABLE"),
            
            # Content analysis (flattened)
            bigquery.SchemaField("text_language", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),  # ARRAY<STRING>
            bigquery.SchemaField("mentions", "STRING", mode="REPEATED"),  # ARRAY<STRING>
            bigquery.SchemaField("is_ad", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("is_sponsored", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("is_slideshow", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("is_pinned", "BOOL", mode="NULLABLE"),
            
            # Video metadata (flattened)
            bigquery.SchemaField("video_cover_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("duration_seconds", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("video_width", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("video_height", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("video_bitrate", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("video_definition", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("video_format", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("original_cover_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("effect_stickers", "STRING", mode="REPEATED"),  # ARRAY<STRING>
            bigquery.SchemaField("media_urls", "STRING", mode="REPEATED"),      # ARRAY<STRING>
            
            # Author metadata (flattened)
            bigquery.SchemaField("author_following_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("author_video_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("author_heart_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("author_region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_friends_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("is_commerce_user", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("business_category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_tiktok_seller", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("author_signature", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_avatar_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author_original_avatar_url", "STRING", mode="NULLABLE"),
            
            # Music metadata (flattened)
            bigquery.SchemaField("music_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("music_title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("music_author", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_original_sound", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("music_play_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("music_cover_url", "STRING", mode="NULLABLE"),
            
            # Additional metadata (flattened)
            bigquery.SchemaField("create_time_unix", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("from_profile_section", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("crawl_input_data", "STRING", mode="NULLABLE"),
            
            # Computed fields (flattened)
            bigquery.SchemaField("total_engagement", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("engagement_rate", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("has_music", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("video_aspect_ratio", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("text_length", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("hashtag_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("detected_language", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sentiment_score", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
            
            # Complex objects kept as JSON (when unavoidable)
            bigquery.SchemaField("detailed_mentions", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("subtitle_links", "JSON", mode="NULLABLE"),
        ]
    }
    
    platforms = [tiktok_table_config]
    
    print("🚀 RECREATING FLATTENED BIGQUERY TABLES")
    print("=" * 60)
    print("🎯 BENEFIT: All fields as individual columns for easy SQL queries")
    print("📊 NO MORE JSON UNNESTING REQUIRED!")
    
    for platform_config in platforms:
        platform_name = platform_config['name']
        table_name = platform_config['table_name']
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        print(f"\n📋 {platform_name.upper()} FLATTENED TABLE: {table_id}")
        
        # Drop existing table
        try:
            client.delete_table(table_id)
            print(f"  ✅ Dropped existing table")
        except Exception as e:
            print(f"  ⚠️  Table might not exist: {str(e)}")
        
        # Create schema
        schema = core_fields + platform_config['specific_fields']
        total_fields = len(schema)
        json_fields = [f.name for f in schema if f.field_type == 'JSON']
        array_fields = [f.name for f in schema if f.mode == 'REPEATED']
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table.description = platform_config['description']
        
        # Set partitioning by date_posted for performance
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_posted"
        )
        
        # Set clustering for analytics queries
        table.clustering_fields = platform_config['cluster_fields']
        
        try:
            table = client.create_table(table)
            print(f"  ✅ Created flattened table with {total_fields} individual columns")
            print(f"  📊 Array fields: {len(array_fields)} → {array_fields}")
            print(f"  📦 JSON fields: {len(json_fields)} → {json_fields}")
            
        except Exception as e:
            print(f"  ❌ Error creating table: {str(e)}")
    
    print(f"\n🎯 FLATTENED SCHEMA BENEFITS:")
    print("✅ Simple SQL queries - no JSON extraction needed")
    print("✅ Better query performance - no UNNEST operations")
    print("✅ Easy filtering and aggregation on all fields")
    print("✅ Direct access to arrays (hashtags, mentions, effects)")
    print("✅ Analytics-friendly structure for BI tools")
    
    print(f"\n📝 EXAMPLE SIMPLE QUERIES:")
    print("SELECT video_id, hashtags, is_ad, duration_seconds FROM tiktok_posts_flattened")
    print("SELECT author_region, AVG(play_count) FROM tiktok_posts_flattened GROUP BY author_region")
    print("SELECT * FROM tiktok_posts_flattened WHERE is_commerce_user = true")
    
    print(f"\n🚀 Ready for flattened schema testing!")

if __name__ == '__main__':
    recreate_flattened_tables()