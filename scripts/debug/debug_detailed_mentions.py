#!/usr/bin/env python3
"""Debug the detailed_mentions field issue."""

import json
from pathlib import Path
from handlers.schema_mapper import SchemaMapper
from datetime import datetime

# Load fixture
fixture_path = Path(__file__).parent / "fixtures" / "gcs-tiktok-posts.json"
with open(fixture_path, 'r', encoding='utf-8') as f:
    posts = json.load(f)

raw_post = posts[0]
print(f"Raw detailedMentions: {raw_post.get('detailedMentions')}")
print(f"Type: {type(raw_post.get('detailedMentions'))}")

# Transform
schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
test_metadata = {
    'crawl_id': f'debug_{datetime.now().strftime("%H%M%S")}',
    'snapshot_id': 'debug',
    'competitor': 'nutifood',
    'brand': 'growplus',
    'category': 'milk',
    'crawl_date': datetime.now().isoformat()
}

transformed_post = schema_mapper.transform_post(raw_post, 'tiktok', test_metadata)
print(f"Transformed detailed_mentions: {transformed_post.get('detailed_mentions')}")
print(f"Type: {type(transformed_post.get('detailed_mentions'))}")

# Check all fields that might be arrays
for key, value in transformed_post.items():
    if isinstance(value, list):
        print(f"Array field found: {key} = {value} (type: {type(value)})")