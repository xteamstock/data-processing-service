#!/usr/bin/env python3
"""
TikTok Field Coverage Analysis
Analyzes the fixture data to identify missing fields that could improve coverage to 90%+
"""

import json
import sys
from collections import defaultdict
from typing import Dict, List, Set, Any

def flatten_dict(data: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Recursively flatten a nested dictionary."""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            # For arrays of objects, flatten the first object to see structure
            items.extend(flatten_dict(v[0], f"{new_key}[0]", sep=sep).items())
            items.append((new_key, v))  # Also keep the array itself
        else:
            items.append((new_key, v))
    return dict(items)

def extract_all_field_paths(posts: List[Dict[str, Any]]) -> Set[str]:
    """Extract all unique field paths from the posts."""
    all_paths = set()
    
    for post in posts:
        flattened = flatten_dict(post)
        all_paths.update(flattened.keys())
    
    return all_paths

def get_currently_mapped_fields() -> Set[str]:
    """Extract all source fields currently mapped in the schema."""
    try:
        with open('/Users/tranquocbao/crawlerX/social-analytics-platform/services/data-processing/schemas/tiktok_post_schema_v1.json', 'r') as f:
            schema = json.load(f)
        
        mapped_fields = set()
        
        # Extract source fields from all mapping categories
        for category, mappings in schema.get("field_mappings", {}).items():
            for field_name, mapping in mappings.items():
                if "source_field" in mapping:
                    mapped_fields.add(mapping["source_field"])
        
        return mapped_fields
    except FileNotFoundError:
        print("Schema file not found!")
        return set()

def analyze_field_value_coverage(posts: List[Dict[str, Any]], field_path: str) -> Dict[str, Any]:
    """Analyze coverage and sample values for a field path."""
    values = []
    non_null_count = 0
    
    for post in posts:
        value = get_nested_value(post, field_path)
        if value is not None and value != "" and value != []:
            non_null_count += 1
            if len(values) < 3:  # Keep sample values
                values.append(value)
    
    coverage = non_null_count / len(posts) * 100 if posts else 0
    
    return {
        "coverage_percentage": coverage,
        "non_null_count": non_null_count,
        "total_posts": len(posts),
        "sample_values": values
    }

def get_nested_value(data: Dict[str, Any], path: str) -> Any:
    """Get value from nested dictionary using dot notation."""
    keys = path.split('.')
    current = data
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    
    return current

def suggest_bigquery_type(sample_values: List[Any]) -> str:
    """Suggest BigQuery type based on sample values."""
    if not sample_values:
        return "STRING"
    
    first_val = sample_values[0]
    
    if isinstance(first_val, bool):
        return "BOOL"
    elif isinstance(first_val, int):
        return "INT64"
    elif isinstance(first_val, float):
        return "FLOAT64"
    elif isinstance(first_val, list):
        if first_val and isinstance(first_val[0], str):
            return "ARRAY<STRING>"
        elif first_val and isinstance(first_val[0], dict):
            return "ARRAY<STRUCT>"
        else:
            return "ARRAY<STRING>"
    elif isinstance(first_val, dict):
        return "STRUCT"
    else:
        return "STRING"

def categorize_field(field_path: str) -> str:
    """Categorize field based on its path."""
    if field_path.startswith("authorMeta."):
        return "author_metadata"
    elif field_path.startswith("videoMeta."):
        return "video_metadata"
    elif field_path.startswith("musicMeta."):
        return "music_metadata"
    elif field_path.startswith("commerceUserInfo."):
        return "commerce_info"
    elif field_path.startswith("subtitleLinks."):
        return "subtitle_info"
    elif field_path in ["diggCount", "shareCount", "playCount", "collectCount", "commentCount"]:
        return "engagement_metrics"
    elif field_path in ["isAd", "isSponsored", "isPinned", "isSlideshow"]:
        return "content_flags"
    else:
        return "other"

def main():
    # Load TikTok fixture data
    try:
        with open('/Users/tranquocbao/crawlerX/social-analytics-platform/services/data-processing/fixtures/gcs-tiktok-posts.json', 'r') as f:
            posts = json.load(f)
    except FileNotFoundError:
        print("Fixture file not found!")
        sys.exit(1)
    
    print(f"Analyzing {len(posts)} TikTok posts...")
    print("=" * 80)
    
    # Extract all available field paths
    all_field_paths = extract_all_field_paths(posts)
    print(f"Total unique field paths found: {len(all_field_paths)}")
    
    # Get currently mapped fields
    mapped_fields = get_currently_mapped_fields()
    print(f"Currently mapped fields: {len(mapped_fields)}")
    
    # Find unmapped fields
    unmapped_fields = all_field_paths - mapped_fields
    print(f"Unmapped fields available: {len(unmapped_fields)}")
    
    print("\n" + "=" * 80)
    print("CURRENT COVERAGE ANALYSIS")
    print("=" * 80)
    
    current_coverage = len(mapped_fields) / len(all_field_paths) * 100
    target_coverage = 90.0
    fields_needed_for_90 = int((target_coverage / 100) * len(all_field_paths)) - len(mapped_fields)
    
    print(f"Current coverage: {len(mapped_fields)}/{len(all_field_paths)} = {current_coverage:.1f}%")
    print(f"Target coverage: {target_coverage}%")
    print(f"Fields needed for 90%: {fields_needed_for_90}")
    
    print("\n" + "=" * 80)
    print("TOP MISSING FIELDS ANALYSIS")
    print("=" * 80)
    
    # Analyze each unmapped field
    field_analysis = []
    
    for field_path in unmapped_fields:
        analysis = analyze_field_value_coverage(posts, field_path)
        analysis["field_path"] = field_path
        analysis["category"] = categorize_field(field_path)
        analysis["suggested_type"] = suggest_bigquery_type(analysis["sample_values"])
        field_analysis.append(analysis)
    
    # Sort by coverage percentage (descending) and non-null count
    field_analysis.sort(key=lambda x: (x["coverage_percentage"], x["non_null_count"]), reverse=True)
    
    print(f"Top {min(20, len(field_analysis))} unmapped fields by data coverage:")
    print()
    print(f"{'Field Path':<40} {'Category':<15} {'Coverage':<10} {'Type':<15} {'Sample Value'}")
    print("-" * 120)
    
    high_value_fields = []
    
    for i, analysis in enumerate(field_analysis[:20]):
        field_path = analysis["field_path"]
        category = analysis["category"]
        coverage = analysis["coverage_percentage"]
        suggested_type = analysis["suggested_type"]
        sample_value = str(analysis["sample_values"][0])[:50] if analysis["sample_values"] else "N/A"
        
        print(f"{field_path:<40} {category:<15} {coverage:>7.1f}% {suggested_type:<15} {sample_value}")
        
        # Collect high-value fields for recommendations
        if coverage > 50 or category in ["engagement_metrics", "author_metadata", "video_metadata", "music_metadata"]:
            high_value_fields.append(analysis)
    
    print("\n" + "=" * 80)
    print("RECOMMENDED FIELD MAPPINGS FOR 90%+ COVERAGE")
    print("=" * 80)
    
    # Select top fields to reach 90% coverage
    recommended_fields = field_analysis[:fields_needed_for_90 + 5]  # Add a few extra for buffer
    
    print(f"Recommending top {len(recommended_fields)} fields to reach 90%+ coverage:")
    print()
    
    # Group by category for better organization
    by_category = defaultdict(list)
    for field in recommended_fields:
        by_category[field["category"]].append(field)
    
    field_mappings = []
    
    for category, fields in by_category.items():
        if not fields:
            continue
            
        print(f"\n## {category.upper()} FIELDS")
        print("-" * 50)
        
        for field in fields:
            field_path = field["field_path"]
            suggested_type = field["suggested_type"]
            coverage = field["coverage_percentage"]
            
            # Generate target field name
            target_field = generate_target_field_name(field_path, category)
            
            print(f"Field: {field_path}")
            print(f"  Target: {target_field}")
            print(f"  Type: {suggested_type}")
            print(f"  Coverage: {coverage:.1f}%")
            if field["sample_values"]:
                print(f"  Sample: {field['sample_values'][0]}")
            print()
            
            # Prepare mapping entry
            mapping_entry = {
                "source_field": field_path,
                "target_field": target_field,
                "target_type": suggested_type,
                "coverage_percent": f"{coverage:.1f}%",
                "category": category
            }
            field_mappings.append(mapping_entry)
    
    print("\n" + "=" * 80)
    print("JSON MAPPING ENTRIES (Ready to add to schema)")
    print("=" * 80)
    
    for mapping in field_mappings[:fields_needed_for_90]:
        field_name = mapping["source_field"].split(".")[-1].lower()
        
        json_entry = {
            mapping["source_field"].replace(".", "_"): {
                "source_field": mapping["source_field"],
                "target_field": mapping["target_field"],
                "target_type": mapping["target_type"],
                "required": False,
                "description": f"Auto-detected field from {mapping['category']} ({mapping['coverage_percent']} coverage)"
            }
        }
        
        if mapping["target_type"] == "INT64":
            json_entry[list(json_entry.keys())[0]]["preprocessing"] = ["safe_int"]
            json_entry[list(json_entry.keys())[0]]["default_value"] = 0
        elif mapping["target_type"] == "BOOL":
            json_entry[list(json_entry.keys())[0]]["default_value"] = False
        elif mapping["target_type"] == "STRING":
            json_entry[list(json_entry.keys())[0]]["preprocessing"] = ["clean_text"]
        
        print(json.dumps(json_entry, indent=2))
        print(",")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"• Current mapping: {len(mapped_fields)} fields ({current_coverage:.1f}% coverage)")
    print(f"• Available unmapped: {len(unmapped_fields)} fields")
    print(f"• Recommended additions: {min(fields_needed_for_90, len(recommended_fields))} fields")
    print(f"• Projected coverage: {((len(mapped_fields) + fields_needed_for_90) / len(all_field_paths) * 100):.1f}%")

def generate_target_field_name(field_path: str, category: str) -> str:
    """Generate appropriate target field name based on path and category."""
    parts = field_path.split(".")
    
    if category == "author_metadata":
        if len(parts) > 1:
            return f"author_metadata.{parts[-1]}"
        return f"author_metadata.{parts[0]}"
    
    elif category == "video_metadata":
        if len(parts) > 1:
            return f"video_metadata.{parts[-1]}"
        return f"video_metadata.{parts[0]}"
    
    elif category == "music_metadata":
        if len(parts) > 1:
            return f"video_metadata.{parts[-1]}"  # Music goes under video_metadata
        return f"video_metadata.{parts[0]}"
    
    elif category == "engagement_metrics":
        return f"engagement_metrics.{parts[-1]}"
    
    elif category == "content_flags":
        return f"content_analysis.{parts[-1]}"
    
    elif category == "commerce_info":
        return f"author_metadata.commerce_{parts[-1]}"
    
    elif category == "subtitle_info":
        return f"video_metadata.{parts[-1]}"
    
    else:
        return f"additional_metadata.{parts[-1]}"

if __name__ == "__main__":
    main()