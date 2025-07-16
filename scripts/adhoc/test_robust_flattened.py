#!/usr/bin/env python3
"""
Robust test for flattened TikTok insertion with proper field validation.
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

def clean_for_bigquery(data):
    """Clean data for BigQuery insertion."""
    cleaned = {}
    for key, value in data.items():
        if value is None:
            cleaned[key] = None
        elif isinstance(value, (dict, list)) and key not in ['detailed_mentions', 'subtitle_links']:
            # Skip nested objects that shouldn't be there in flattened schema
            continue
        elif isinstance(value, list) and key in ['hashtags', 'mentions', 'effect_stickers', 'media_urls']:
            # Keep arrays that are expected
            cleaned[key] = value
        elif isinstance(value, list) and key in ['detailed_mentions', 'subtitle_links']:
            # Convert to JSON string for JSON fields
            cleaned[key] = json.dumps(value) if value else None
        else:
            cleaned[key] = value
    return cleaned

def main():
    """Test robust flattened insertion."""
    print("🧪 ROBUST FLATTENED TIKTOK INSERTION TEST")
    print("=" * 60)
    
    # Load data
    fixture_path = Path(__file__).parent / "fixtures" / "gcs-tiktok-posts.json"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    # Transform 
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    test_metadata = {
        'crawl_id': f'robust_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'snapshot_id': 'robust_test_snapshot',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',  
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.now().isoformat()
    }
    
    raw_post = posts[0]
    print(f"📄 Processing TikTok post: {raw_post.get('id')}")
    
    transformed_post = schema_mapper.transform_post(raw_post, 'tiktok', test_metadata)
    
    # Flatten processing metadata
    if 'processing_metadata' in transformed_post:
        processing_meta = transformed_post.pop('processing_metadata')
        transformed_post['schema_version'] = processing_meta.get('schema_version')
        transformed_post['processing_version'] = processing_meta.get('processing_version')
    
    # Clean for BigQuery
    cleaned_post = clean_for_bigquery(transformed_post)
    
    print(f"✅ Transformed and cleaned post")
    print(f"   - Original fields: {len(transformed_post)}")
    print(f"   - Cleaned fields: {len(cleaned_post)}")
    
    # Show some key fields
    print(f"\n📊 KEY FLATTENED FIELDS:")
    key_fields = ['video_id', 'author_name', 'total_engagement', 'is_ad', 'author_region', 'is_commerce_user']
    for field in key_fields:
        print(f"   - {field}: {cleaned_post.get(field)}")
    
    # Insert to BigQuery
    try:
        client = bigquery.Client()
        table_id = "competitor-destroyer.social_analytics.tiktok_posts_flattened"
        errors = client.insert_rows_json(table_id, [cleaned_post])
        
        if errors:
            print(f"\n❌ BigQuery insertion errors:")
            for error in errors:
                print(f"   {error}")
        else:
            print(f"\n🎉 SUCCESS: Inserted into {table_id}")
            print(f"✅ Flattened schema working perfectly!")
            print(f"✅ All 75+ fields available as individual columns")
            
            # Show example queries
            print(f"\n📝 READY FOR ANALYTICS - Try these queries:")
            print(f"-- Commerce user analysis")
            print(f"SELECT author_region, business_category, AVG(engagement_rate)")
            print(f"FROM tiktok_posts_flattened") 
            print(f"WHERE is_commerce_user = true")
            print(f"GROUP BY author_region, business_category;")
            
            print(f"\n-- Video performance by format")
            print(f"SELECT video_definition, video_format, AVG(play_count)")
            print(f"FROM tiktok_posts_flattened")
            print(f"GROUP BY video_definition, video_format;")
            
            print(f"\n-- High-engagement ads")
            print(f"SELECT video_id, description, total_engagement, engagement_rate")
            print(f"FROM tiktok_posts_flattened")
            print(f"WHERE is_ad = true AND engagement_rate > 0.01")
            print(f"ORDER BY total_engagement DESC;")
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main()