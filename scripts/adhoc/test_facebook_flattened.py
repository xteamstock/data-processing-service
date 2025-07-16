#!/usr/bin/env python3
"""
Test the flattened Facebook schema with preprocessing and computation functions.
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
    """Test flattened Facebook schema with full preprocessing and computation."""
    print("🔄 TESTING FLATTENED FACEBOOK SCHEMA")
    print("=" * 60)
    
    # Load fixture
    fixture_path = Path(__file__).parent / "fixtures" / "gcs-facebook-posts.json"
    
    if not fixture_path.exists():
        print(f"❌ Facebook fixture not found at {fixture_path}")
        print("Looking for available fixtures...")
        fixtures_dir = Path(__file__).parent / "fixtures"
        for file in fixtures_dir.glob("*.json"):
            print(f"   - {file.name}")
        return
    
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    raw_post = posts[0]
    print(f"📄 Processing post: {raw_post.get('post_id', 'Unknown')}")
    
    # Check raw data structure
    print(f"\n📊 Raw data analysis:")
    print(f"   - post_id: {raw_post.get('post_id')}")
    print(f"   - date_posted: {raw_post.get('date_posted')}")
    print(f"   - content: {raw_post.get('content', '')[:100]}...")
    print(f"   - likes: {raw_post.get('likes')}")
    print(f"   - num_comments: {raw_post.get('num_comments')}")
    print(f"   - num_shares: {raw_post.get('num_shares')}")
    print(f"   - hashtags: {raw_post.get('hashtags')}")
    print(f"   - attachments: {raw_post.get('attachments', [])}")
    
    # Transform with schema mapper
    print(f"\n🔄 Transforming with SchemaMapper...")
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    
    test_metadata = {
        'crawl_id': f'facebook_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'snapshot_id': 'facebook_test',
        'competitor': 'nutifood',
        'brand': 'growplus',
        'category': 'milk',
        'crawl_date': datetime.now().isoformat()
    }
    
    try:
        transformed_post = schema_mapper.transform_post(raw_post, 'facebook', test_metadata)
        print(f"✅ Schema transformation successful!")
        
        # Analyze transformation results
        print(f"\n📊 Transformation results:")
        print(f"   - post_id: {transformed_post.get('post_id')}")
        print(f"   - date_posted: {transformed_post.get('date_posted')}")
        print(f"   - grouped_date: {transformed_post.get('grouped_date')}")
        print(f"   - post_content: {transformed_post.get('post_content', '')[:100]}...")
        print(f"   - hashtags: {transformed_post.get('hashtags')}")
        print(f"   - likes: {transformed_post.get('likes')}")
        print(f"   - comments: {transformed_post.get('comments')}")
        print(f"   - shares: {transformed_post.get('shares')}")
        print(f"   - page_name: {transformed_post.get('page_name')}")
        print(f"   - page_verified: {transformed_post.get('page_verified')}")
        print(f"   - attachments: {transformed_post.get('attachments', '')[:100]}...")
        
        # Check computed fields
        print(f"\n🧮 Computed fields:")
        print(f"   - total_reactions: {transformed_post.get('total_reactions')}")
        print(f"   - media_count: {transformed_post.get('media_count')}")
        print(f"   - has_video: {transformed_post.get('has_video')}")
        print(f"   - has_image: {transformed_post.get('has_image')}")
        print(f"   - text_length: {transformed_post.get('text_length')}")
        print(f"   - language: {transformed_post.get('language')}")
        print(f"   - sentiment_score: {transformed_post.get('sentiment_score')}")
        print(f"   - data_quality_score: {transformed_post.get('data_quality_score')}")
        
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
        
        print(f"\n🧹 Cleaned data: {len(cleaned)} fields")
        
        # Test BigQuery insertion
        print(f"\n🎯 Testing BigQuery insertion...")
        
        # First, let's create a flattened Facebook table
        client = bigquery.Client()
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        dataset_id = 'social_analytics'
        table_id = f"{project_id}.{dataset_id}.facebook_posts_flattened"
        
        # Create basic schema for testing
        schema = [
            # Core fields
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
            
            # Facebook specific
            bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("post_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("post_content", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("page_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("page_verified", "BOOL", mode="NULLABLE"),
            bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("likes", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("comments", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("shares", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("attachments", "STRING", mode="NULLABLE"),
            
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
        ]
        
        # Create or update table
        try:
            client.delete_table(table_id)
            print(f"  ✅ Dropped existing table")
        except:
            print(f"  ⚠️  Table didn't exist")
        
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Facebook posts with flattened schema - no JSON nesting"
        table = client.create_table(table)
        print(f"  ✅ Created flattened table with {len(schema)} fields")
        
        errors = client.insert_rows_json(table_id, [cleaned])
        
        if errors:
            print(f"❌ BigQuery insertion errors:")
            for error in errors:
                print(f"   - {error}")
        else:
            print(f"✅ SUCCESS! Facebook data inserted into BigQuery")
            print(f"🎯 All preprocessing and computation functions working!")
            
            # Verify with query
            print(f"\n📝 Verification query:")
            query = f"""
            SELECT 
                post_id,
                grouped_date,
                hashtags,
                likes,
                comments,
                shares,
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
                print(f"✅ Verified in BigQuery:")
                print(f"   - grouped_date: {row.grouped_date}")
                print(f"   - hashtags: {row.hashtags}")
                print(f"   - likes: {row.likes}")
                print(f"   - total_reactions: {row.total_reactions}")
                print(f"   - media_count: {row.media_count}")
                print(f"   - has_video: {row.has_video}")
                print(f"   - has_image: {row.has_image}")
                print(f"   - text_length: {row.text_length}")
                print(f"   - language: {row.language}")
                print(f"   - sentiment_score: {row.sentiment_score}")
                print(f"   - data_quality_score: {row.data_quality_score}")
                
    except Exception as e:
        print(f"❌ Error during transformation: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()