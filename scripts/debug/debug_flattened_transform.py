#!/usr/bin/env python3
"""
Debug what the schema mapper produces for flattened schema.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from handlers.schema_mapper import SchemaMapper

def debug_transform():
    """Debug the transformation output."""
    print("🔍 DEBUGGING FLATTENED SCHEMA TRANSFORMATION")
    print("=" * 60)
    
    # Load TikTok fixture data
    fixture_path = Path(__file__).parent / "fixtures" / "gcs-tiktok-posts.json"
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    # Test metadata
    test_metadata = {
        'crawl_id': 'debug_test',
        'snapshot_id': 'debug_snapshot',
        'competitor': 'nutifood',
        'brand': 'growplus-nutifood',
        'category': 'sua-bot-tre-em',
        'crawl_date': datetime.now().isoformat()
    }
    
    # Initialize schema mapper
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    
    # Transform the first post
    raw_post = posts[0]
    transformed_post = schema_mapper.transform_post(raw_post, 'tiktok', test_metadata)
    
    print(f"📊 TRANSFORMED POST STRUCTURE:")
    print(f"Total fields: {len(transformed_post)}")
    
    # Show all top-level fields
    for key, value in transformed_post.items():
        if isinstance(value, dict):
            print(f"  {key}: DICT with {len(value)} fields → {list(value.keys())}")
        elif isinstance(value, list):
            print(f"  {key}: ARRAY with {len(value)} items")
        else:
            print(f"  {key}: {type(value).__name__} = {str(value)[:50]}...")
    
    # Show problematic nested fields
    print(f"\n🚨 NESTED FIELDS THAT NEED FLATTENING:")
    nested_fields = {k: v for k, v in transformed_post.items() if isinstance(v, dict)}
    for field_name, field_value in nested_fields.items():
        print(f"  {field_name}:")
        for sub_key, sub_value in field_value.items():
            print(f"    - {sub_key}: {type(sub_value).__name__}")

if __name__ == "__main__":
    debug_transform()