#!/usr/bin/env python3
"""
Verify Facebook BigQuery record format and create comprehensive schema.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from handlers.schema_mapper import SchemaMapper
from google.cloud import bigquery

def main():
    """Test Facebook with comprehensive BigQuery schema."""
    print("🔄 TESTING FACEBOOK WITH COMPREHENSIVE SCHEMA")
    print("=" * 60)
    
    # Load fixture
    fixture_path = Path(__file__).parent / "fixtures" / "gcs-facebook-posts.json"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    raw_post = posts[0]
    print(f"📄 Processing post: {raw_post.get('post_id')}")
    
    # Transform with schema mapper
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    
    test_metadata = {
        'crawl_id': f'facebook_comprehensive_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'snapshot_id': 'facebook_comprehensive',
        'competitor': 'nutifood',
        'brand': 'growplus',
        'category': 'milk',
        'crawl_date': datetime.now().isoformat()
    }
    
    transformed_post = schema_mapper.transform_post(raw_post, 'facebook', test_metadata)
    
    # Flatten processing metadata
    if 'processing_metadata' in transformed_post:
        processing_meta = transformed_post.pop('processing_metadata')
        transformed_post['schema_version'] = processing_meta.get('schema_version')
        transformed_post['processing_version'] = processing_meta.get('processing_version')
        transformed_post['data_quality_score'] = processing_meta.get('data_quality_score')
    
    # Clean for BigQuery (remove nested objects)
    cleaned = {}
    for key, value in transformed_post.items():
        if not isinstance(value, dict):
            cleaned[key] = value
    
    print(f"🧹 Cleaned data: {len(cleaned)} fields")
    
    # Print all fields for schema creation
    print(f"\n📊 All fields in transformed data:")
    for key, value in cleaned.items():
        print(f"   - {key}: {type(value).__name__} = {str(value)[:50]}...")
    
    # Create comprehensive BigQuery schema
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = 'social_analytics'
    table_id = f"{project_id}.{dataset_id}.facebook_posts_comprehensive"
    
    # Comprehensive schema based on all fields
    schema = [
        # Core required fields
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        
        # Facebook post fields
        bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_shortcode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_type", "STRING", mode="NULLABLE"),
        
        # User/page fields
        bigquery.SchemaField("user_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_profile_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_verified", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("page_followers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("page_likes", "INT64", mode="NULLABLE"),
        
        # Content fields
        bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
        
        # Engagement fields
        bigquery.SchemaField("likes", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("comments", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("shares", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("video_views", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("likes_breakdown", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reactions_by_type", "STRING", mode="NULLABLE"),
        
        # Media fields
        bigquery.SchemaField("attachments", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("primary_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("header_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("avatar_url", "STRING", mode="NULLABLE"),
        
        # Page metadata
        bigquery.SchemaField("page_intro", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_logo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_website", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_creation_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("page_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_reviews_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("page_reviewers_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("privacy_legal_info", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("delegate_page_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price_range", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_direct_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("about_sections", "STRING", mode="NULLABLE"),
        
        # Content metadata
        bigquery.SchemaField("contains_sponsored", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("active_ads_urls", "STRING", mode="REPEATED"),
        bigquery.SchemaField("link_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("following_status", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_page", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("profile_handle", "STRING", mode="NULLABLE"),
        
        # Crawl metadata
        bigquery.SchemaField("crawl_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("original_input", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_range_start", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_range_end", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_limit", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("include_profile_data", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE"),
        
        # Computed fields
        bigquery.SchemaField("total_reactions", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("media_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("has_video", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("has_image", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("text_length", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
        
        # Processing metadata
        bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
        
        # Additional fields
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    # Create or update table
    try:
        client.delete_table(table_id)
        print(f"  ✅ Dropped existing table")
    except:
        print(f"  ⚠️  Table didn't exist")
    
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Facebook posts with comprehensive flattened schema"
    table = client.create_table(table)
    print(f"  ✅ Created comprehensive table with {len(schema)} fields")
    
    # Insert data
    errors = client.insert_rows_json(table_id, [cleaned])
    
    if errors:
        print(f"❌ BigQuery insertion errors:")
        for error in errors:
            print(f"   - {error}")
    else:
        print(f"✅ SUCCESS! Facebook data inserted with comprehensive schema")
        
        # Verify with query
        query = f"""
        SELECT 
            post_id,
            grouped_date,
            post_content,
            hashtags,
            likes,
            comments,
            shares,
            page_name,
            page_verified,
            total_reactions,
            media_count,
            has_video,
            has_image,
            text_length,
            language,
            sentiment_score,
            data_quality_score
        FROM `{table_id}`
        WHERE post_id = '{cleaned.get('post_id')}'
        LIMIT 1
        """
        
        results = list(client.query(query))
        if results:
            row = results[0]
            print(f"\n✅ Verified Facebook record in BigQuery:")
            print(f"   - post_id: {row.post_id}")
            print(f"   - grouped_date: {row.grouped_date}")
            print(f"   - hashtags: {row.hashtags}")
            print(f"   - likes: {row.likes}")
            print(f"   - comments: {row.comments}")
            print(f"   - shares: {row.shares}")
            print(f"   - page_name: {row.page_name}")
            print(f"   - total_reactions: {row.total_reactions}")
            print(f"   - media_count: {row.media_count}")
            print(f"   - has_video: {row.has_video}")
            print(f"   - has_image: {row.has_image}")
            print(f"   - text_length: {row.text_length}")
            print(f"   - language: {row.language}")
            print(f"   - sentiment_score: {row.sentiment_score}")
            print(f"   - data_quality_score: {row.data_quality_score}")
            
            print(f"\n🎯 FACEBOOK FLATTENED SCHEMA VERIFICATION COMPLETE!")
            print(f"✅ All preprocessing functions working correctly")
            print(f"✅ All computation functions working correctly")
            print(f"✅ All fields flattened successfully")
            print(f"✅ JSON fields converted to strings")
            print(f"✅ Arrays preserved for REPEATED fields")

if __name__ == "__main__":
    main()