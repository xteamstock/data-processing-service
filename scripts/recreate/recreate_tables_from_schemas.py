#!/usr/bin/env python3
"""
Recreate BigQuery tables directly from schema JSON files.
This is the proper approach - automatically generate tables from the actual schema definitions.
"""

import json
import os
from pathlib import Path
from google.cloud import bigquery

def load_schema_config(schema_file):
    """Load schema configuration from JSON file."""
    with open(schema_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def convert_to_bigquery_type(target_type):
    """Convert schema target_type to BigQuery type."""
    type_mapping = {
        'STRING': 'STRING',
        'INT64': 'INT64', 
        'FLOAT64': 'FLOAT64',
        'BOOL': 'BOOL',
        'TIMESTAMP': 'TIMESTAMP',
        'DATE': 'DATE',
        'JSON': 'JSON',
        'ARRAY<STRING>': 'STRING',  # Arrays become REPEATED STRING
        'ARRAY<INT64>': 'INT64',    # Arrays become REPEATED INT64
    }
    return type_mapping.get(target_type, 'STRING')

def convert_to_bigquery_mode(target_type, required=False):
    """Convert schema type to BigQuery mode."""
    if target_type.startswith('ARRAY<'):
        return 'REPEATED'
    elif required:
        return 'REQUIRED'
    else:
        return 'NULLABLE'

def create_bigquery_schema_from_json(schema_config):
    """Create BigQuery schema from JSON schema configuration."""
    fields = []
    
    # Add core metadata fields first (these are always added by schema mapper)
    core_fields = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        # Processing metadata fields (automatically added by schema mapper)
        bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
    ]
    fields.extend(core_fields)
    
    # Process field mappings
    field_mappings = schema_config.get('field_mappings', {})
    
    for category, category_fields in field_mappings.items():
        for field_name, field_config in category_fields.items():
            target_field = field_config.get('target_field')
            target_type = field_config.get('target_type', 'STRING')
            required = field_config.get('required', False)
            
            if target_field:
                bq_type = convert_to_bigquery_type(target_type)
                bq_mode = convert_to_bigquery_mode(target_type, required)
                
                # Skip if already added (avoid duplicates)
                if not any(f.name == target_field for f in fields):
                    fields.append(bigquery.SchemaField(target_field, bq_type, mode=bq_mode))
    
    # Process computed fields
    computed_fields = schema_config.get('computed_fields', {})
    
    for field_name, field_config in computed_fields.items():
        target_field = field_config.get('target_field')
        target_type = field_config.get('target_type', 'STRING')
        
        if target_field:
            bq_type = convert_to_bigquery_type(target_type)
            bq_mode = convert_to_bigquery_mode(target_type, False)
            
            # Skip if already added
            if not any(f.name == target_field for f in fields):
                fields.append(bigquery.SchemaField(target_field, bq_type, mode=bq_mode))
    
    return fields

def recreate_table_from_schema(schema_file):
    """Recreate a BigQuery table from schema JSON file."""
    schema_config = load_schema_config(schema_file)
    platform = schema_config.get('platform')
    
    if not platform:
        print(f"❌ No platform specified in {schema_file}")
        return False
    
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
    
    # Determine table name based on platform
    if platform == 'tiktok':
        table_name = 'tiktok_posts_schema_driven'
    elif platform == 'facebook':
        table_name = 'facebook_posts_schema_driven'
    elif platform == 'youtube':
        table_name = 'youtube_videos_schema_driven'
    else:
        table_name = f'{platform}_posts_schema_driven'
    
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    print(f"\n📋 {platform.upper()} TABLE: {table_id}")
    print(f"   Schema source: {schema_file.name}")
    
    # Drop existing table
    try:
        client.delete_table(table_id)
        print(f"  ✅ Dropped existing table")
    except Exception:
        print(f"  ⚠️  Table didn't exist")
    
    # Create BigQuery schema from JSON schema
    schema_fields = create_bigquery_schema_from_json(schema_config)
    
    # Create table
    table = bigquery.Table(table_id, schema=schema_fields)
    table.description = f"{platform.title()} posts table generated from schema JSON file"
    
    # Set partitioning by appropriate date field for performance
    if any(f.name == "date_posted" for f in schema_fields):
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date_posted"
        )
    elif any(f.name == "published_at" for f in schema_fields):
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="published_at"
        )
    else:
        # Fallback to processed_date
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="processed_date"
        )
    
    # Set clustering for analytics queries
    if platform == 'tiktok':
        table.clustering_fields = ['competitor', 'brand', 'author_name']
    elif platform == 'facebook':
        table.clustering_fields = ['competitor', 'brand', 'page_name']
    elif platform == 'youtube':
        table.clustering_fields = ['competitor', 'brand', 'channel_name']
    
    try:
        table = client.create_table(table)
        
        # Analyze created schema
        total_fields = len(table.schema)
        repeated_fields = [f.name for f in table.schema if f.mode == 'REPEATED']
        required_fields = [f.name for f in table.schema if f.mode == 'REQUIRED']
        
        print(f"  ✅ Created table with {total_fields} fields")
        print(f"  📊 Required fields: {len(required_fields)} → {required_fields}")
        print(f"  📦 Array fields: {len(repeated_fields)} → {repeated_fields}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating table: {str(e)}")
        return False

def recreate_all_tables_from_schemas():
    """Recreate all platform tables from their schema JSON files."""
    print("🚀 RECREATING BIGQUERY TABLES FROM SCHEMA JSON FILES")
    print("=" * 70)
    print("🎯 APPROACH: Automatically generate tables from actual schema definitions")
    print("✅ NO HARDCODED SCHEMAS - Direct from JSON files")
    print("=" * 70)
    
    schema_dir = Path(__file__).parent / "schemas"
    schema_files = list(schema_dir.glob("*_schema_v*.json"))
    
    if not schema_files:
        print(f"❌ No schema files found in {schema_dir}")
        return
    
    print(f"📁 Found {len(schema_files)} schema files:")
    for schema_file in schema_files:
        print(f"   - {schema_file.name}")
    
    # Process each schema file
    successful = 0
    failed = 0
    
    for schema_file in schema_files:
        try:
            success = recreate_table_from_schema(schema_file)
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Error processing {schema_file.name}: {e}")
            failed += 1
    
    # Summary
    print(f"\n🎯 SCHEMA-DRIVEN TABLE RECREATION COMPLETE!")
    print("=" * 70)
    print(f"✅ Successfully created: {successful} tables")
    if failed > 0:
        print(f"❌ Failed to create: {failed} tables")
    
    print(f"\n📊 BENEFITS OF SCHEMA-DRIVEN APPROACH:")
    print("✅ Tables automatically match schema definitions")
    print("✅ No manual schema maintenance required")
    print("✅ Schema changes automatically reflected in tables")
    print("✅ Consistent field types and constraints")
    print("✅ Direct mapping from schema JSON to BigQuery")
    
    print(f"\n📝 NEXT STEPS:")
    print("1. Test data insertion with schema mapper")
    print("2. Verify all preprocessing and computation functions")
    print("3. Load fixture data to validate complete pipeline")

if __name__ == '__main__':
    recreate_all_tables_from_schemas()