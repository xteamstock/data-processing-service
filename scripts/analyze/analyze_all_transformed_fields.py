#!/usr/bin/env python3
"""
Analyze all transformed fields from all platforms to create accurate schemas.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from handlers.schema_mapper import SchemaMapper

def analyze_platform_fields(platform_name, fixture_file):
    """Analyze transformed fields for a platform."""
    print(f"\n🔍 ANALYZING {platform_name.upper()} TRANSFORMED FIELDS")
    print("=" * 60)
    
    # Load fixture
    fixture_path = Path(__file__).parent / "fixtures" / fixture_file
    if not fixture_path.exists():
        print(f"❌ Fixture file not found: {fixture_file}")
        return set()
        
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    print(f"📊 Found {len(posts)} posts in {fixture_file}")
    
    # Initialize schema mapper
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    
    # Collect all fields from all transformed posts
    all_fields = set()
    field_types = {}
    
    for i, raw_post in enumerate(posts[:3]):  # Sample first 3 posts
        try:
            test_metadata = {
                'crawl_id': f'{platform_name}_analysis_{i}',
                'snapshot_id': f'{platform_name}_analysis',
                'competitor': 'nutifood',
                'brand': 'growplus',
                'category': 'milk',
                'crawl_date': datetime.now().isoformat()
            }
            
            transformed_post = schema_mapper.transform_post(raw_post, platform_name, test_metadata)
            
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
                    all_fields.add(key)
                    
                    # Track field types
                    if key not in field_types:
                        field_types[key] = set()
                    field_types[key].add(type(value).__name__)
            
        except Exception as e:
            print(f"⚠️  Failed to transform post {i}: {e}")
    
    print(f"📋 Found {len(all_fields)} unique fields after transformation")
    
    # Print all fields with their types
    print(f"\n📊 All fields and types:")
    for field in sorted(all_fields):
        types = list(field_types[field])
        print(f"   - {field}: {'/'.join(types)}")
    
    return all_fields, field_types

def create_bigquery_schema_from_fields(field_types, platform_name):
    """Create BigQuery schema from analyzed fields."""
    schema_fields = []
    
    # Core required fields (always present)
    core_fields = {'id', 'crawl_id', 'platform', 'competitor', 'date_posted', 'grouped_date', 'processed_date'}
    
    for field in sorted(field_types.keys()):
        types = field_types[field]
        
        # Determine BigQuery type
        if field in core_fields:
            mode = "REQUIRED"
        else:
            mode = "NULLABLE"
            
        if 'list' in types:
            if field == 'hashtags' or field == 'mentions' or field == 'active_ads_urls':
                bq_type = "STRING"
                mode = "REPEATED"
            else:
                bq_type = "STRING"  # Convert lists to JSON strings
        elif 'int' in types:
            bq_type = "INT64"
        elif 'float' in types:
            bq_type = "FLOAT64"
        elif 'bool' in types:
            bq_type = "BOOL"
        elif field == 'date_posted' or field == 'published_at' or field == 'crawl_date' or field == 'processed_date' or field == 'page_creation_date' or field == 'crawl_timestamp':
            bq_type = "TIMESTAMP"
        elif field == 'grouped_date':
            bq_type = "DATE"
        else:
            bq_type = "STRING"
        
        schema_fields.append(f'        bigquery.SchemaField("{field}", "{bq_type}", mode="{mode}"),')
    
    return schema_fields

def main():
    """Analyze all platforms and generate accurate schemas."""
    print("🔍 ANALYZING ALL PLATFORM TRANSFORMED FIELDS")
    print("=" * 70)
    
    platforms = [
        ('tiktok', 'gcs-tiktok-posts.json'),
        ('facebook', 'gcs-facebook-posts.json'),
        ('youtube', 'gcs-youtube-posts.json')
    ]
    
    all_schemas = {}
    
    for platform_name, fixture_file in platforms:
        fields, field_types = analyze_platform_fields(platform_name, fixture_file)
        schema_lines = create_bigquery_schema_from_fields(field_types, platform_name)
        all_schemas[platform_name] = schema_lines
    
    # Generate schema functions
    print(f"\n🏗️  GENERATED BIGQUERY SCHEMA FUNCTIONS")
    print("=" * 70)
    
    for platform_name, schema_lines in all_schemas.items():
        print(f"\ndef create_{platform_name}_schema():")
        print(f'    """Create {platform_name.title()} BigQuery schema based on actual transformed fields."""')
        print(f"    return [")
        for line in schema_lines:
            print(line)
        print(f"    ]")

if __name__ == "__main__":
    main()