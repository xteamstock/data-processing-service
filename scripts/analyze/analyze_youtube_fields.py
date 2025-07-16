#!/usr/bin/env python3
"""
Analyze YouTube fixture data to identify all fields and compare with schema.
"""

import json
from pathlib import Path

def analyze_youtube_fields():
    """Analyze all fields in YouTube fixture data."""
    print("🔍 ANALYZING YOUTUBE FIXTURE DATA")
    print("=" * 60)
    
    # Load fixture
    fixture_path = Path(__file__).parent / "fixtures" / "gcs-youtube-posts.json"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    print(f"📊 Total posts: {len(posts)}")
    
    # Get all unique fields from all posts
    all_fields = set()
    for post in posts:
        all_fields.update(post.keys())
    
    print(f"📝 Total unique fields: {len(all_fields)}")
    print(f"\n🔍 All fields found in fixture data:")
    for field in sorted(all_fields):
        sample_value = None
        sample_type = None
        
        # Get a sample value
        for post in posts:
            if field in post and post[field] is not None:
                sample_value = post[field]
                sample_type = type(sample_value).__name__
                break
        
        if sample_value is not None:
            if isinstance(sample_value, (list, dict)):
                print(f"   - {field}: {sample_type} = {str(sample_value)[:100]}...")
            else:
                print(f"   - {field}: {sample_type} = {str(sample_value)[:50]}...")
        else:
            print(f"   - {field}: null/empty")
    
    # Load current schema
    schema_path = Path(__file__).parent / "schemas" / "youtube_video_schema_v1.json"
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    # Extract all source fields from schema
    schema_fields = set()
    field_mappings = schema.get('field_mappings', {})
    
    for category, fields in field_mappings.items():
        for field_name, field_config in fields.items():
            source_field = field_config.get('source_field')
            if source_field:
                schema_fields.add(source_field)
    
    print(f"\n📋 Current schema maps {len(schema_fields)} fields")
    print(f"📊 Coverage: {len(schema_fields.intersection(all_fields))}/{len(all_fields)} = {len(schema_fields.intersection(all_fields))/len(all_fields)*100:.1f}%")
    
    # Find missing fields
    missing_fields = all_fields - schema_fields
    print(f"\n❌ Missing fields in schema ({len(missing_fields)}):")
    for field in sorted(missing_fields):
        sample_value = None
        sample_type = None
        
        for post in posts:
            if field in post and post[field] is not None:
                sample_value = post[field]
                sample_type = type(sample_value).__name__
                break
        
        if sample_value is not None:
            if isinstance(sample_value, (list, dict)):
                print(f"   - {field}: {sample_type} = {str(sample_value)[:100]}...")
            else:
                print(f"   - {field}: {sample_type} = {str(sample_value)[:50]}...")
        else:
            print(f"   - {field}: null/empty")
    
    # Find extra schema fields
    extra_fields = schema_fields - all_fields
    print(f"\n⚠️  Extra schema fields not in fixture ({len(extra_fields)}):")
    for field in sorted(extra_fields):
        print(f"   - {field}")
    
    return all_fields, missing_fields, schema_fields

if __name__ == "__main__":
    analyze_youtube_fields()