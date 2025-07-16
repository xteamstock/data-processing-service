#!/usr/bin/env python3
"""
Script to create BigQuery media tracking table for monitoring media processing pipeline.

This script creates the media_tracking table based on the schema definition
in schemas/media_tracking_table_schema.json.
"""

import os
import json
import logging
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound, Conflict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MediaTrackingTableCreator:
    """Create BigQuery media tracking table from schema definition."""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
        self.schemas_dir = Path(__file__).parent.parent / 'schemas'
        self.schema_file = self.schemas_dir / 'media_tracking_table_schema.json'
    
    def create_dataset_if_not_exists(self):
        """Create dataset if it doesn't exist."""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
            return True
        except NotFound:
            # Dataset doesn't exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"  # Match Cloud Run region
            dataset.description = "Social media analytics data with media tracking"
            
            try:
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {self.dataset_id}")
                return True
            except Conflict:
                logger.info(f"Dataset {self.dataset_id} already exists (race condition)")
                return True
            except Exception as e:
                logger.error(f"Error creating dataset: {str(e)}")
                return False
    
    def load_schema_definition(self):
        """Load schema definition from JSON file."""
        try:
            with open(self.schema_file, 'r') as f:
                schema_definition = json.load(f)
            
            logger.info(f"Loaded schema definition from {self.schema_file}")
            return schema_definition
        except FileNotFoundError:
            logger.error(f"Schema file not found: {self.schema_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in schema file: {str(e)}")
            raise
    
    def create_media_tracking_table(self, force_recreate=False):
        """Create media tracking table with proper partitioning and clustering."""
        schema_definition = self.load_schema_definition()
        
        table_name = schema_definition['table_name']
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # Check if table exists
        try:
            existing_table = self.client.get_table(table_id)
            if not force_recreate:
                logger.info(f"Table {table_id} already exists")
                return True
            else:
                logger.info(f"Deleting existing table {table_id}")
                self.client.delete_table(table_id)
        except NotFound:
            pass  # Table doesn't exist, which is expected
        
        try:
            # Convert schema fields to BigQuery schema
            schema_fields = self._convert_schema_to_bigquery(schema_definition['fields'])
            
            # Create table
            table = bigquery.Table(table_id, schema=schema_fields)
            table.description = schema_definition['description']
            
            # Set partitioning
            if 'partitioning' in schema_definition:
                partition_config = schema_definition['partitioning']
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=getattr(bigquery.TimePartitioningType, partition_config['granularity']),
                    field=partition_config['field']
                )
                logger.info(f"Set partitioning on {partition_config['field']} by {partition_config['granularity']}")
            
            # Set clustering
            if 'clustering' in schema_definition:
                table.clustering_fields = schema_definition['clustering']['fields']
                logger.info(f"Set clustering on: {', '.join(table.clustering_fields)}")
            
            # Create the table
            created_table = self.client.create_table(table)
            logger.info(f"Created media tracking table {table_id}")
            
            # Verify table structure
            self._verify_table_structure(created_table, schema_definition)
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating media tracking table: {str(e)}")
            return False
    
    def _convert_schema_to_bigquery(self, schema_fields):
        """Convert schema definition to BigQuery schema fields."""
        bigquery_fields = []
        
        for field in schema_fields:
            field_type = field['type']
            field_mode = field.get('mode', 'NULLABLE')
            field_name = field['name']
            field_description = field.get('description', '')
            
            if field_type == 'RECORD':
                # Handle nested records
                nested_fields = self._convert_schema_to_bigquery(field.get('fields', []))
                bigquery_field = bigquery.SchemaField(
                    field_name, 
                    field_type, 
                    mode=field_mode, 
                    description=field_description,
                    fields=nested_fields
                )
            else:
                bigquery_field = bigquery.SchemaField(
                    field_name, 
                    field_type, 
                    mode=field_mode, 
                    description=field_description
                )
            
            bigquery_fields.append(bigquery_field)
        
        return bigquery_fields
    
    def _verify_table_structure(self, table, schema_definition):
        """Verify the created table has the correct structure."""
        logger.info("Verifying table structure...")
        
        # Verify partitioning
        if table.time_partitioning:
            partition_config = schema_definition['partitioning']
            expected_field = partition_config['field']
            if table.time_partitioning.field == expected_field:
                logger.info(f"✅ Partitioning verified: {expected_field}")
            else:
                logger.warning(f"❌ Partitioning mismatch: expected {expected_field}, got {table.time_partitioning.field}")
        
        # Verify clustering
        if table.clustering_fields:
            expected_fields = schema_definition['clustering']['fields']
            if set(table.clustering_fields) == set(expected_fields):
                logger.info(f"✅ Clustering verified: {', '.join(expected_fields)}")
            else:
                logger.warning(f"❌ Clustering mismatch: expected {expected_fields}, got {table.clustering_fields}")
        
        # Verify field count
        expected_field_count = len(schema_definition['fields'])
        actual_field_count = len(table.schema)
        if actual_field_count == expected_field_count:
            logger.info(f"✅ Field count verified: {actual_field_count} fields")
        else:
            logger.warning(f"❌ Field count mismatch: expected {expected_field_count}, got {actual_field_count}")
        
        logger.info("Table structure verification complete")
    
    def create_table_with_validation(self, force_recreate=False):
        """Create table with full validation and error handling."""
        logger.info("Creating media tracking table with validation...")
        
        # Step 1: Create dataset if needed
        if not self.create_dataset_if_not_exists():
            logger.error("Failed to create or verify dataset")
            return False
        
        # Step 2: Validate schema file exists and is valid
        try:
            schema_definition = self.load_schema_definition()
            logger.info(f"Schema validation passed for table '{schema_definition['table_name']}'")
        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            return False
        
        # Step 3: Create table
        if not self.create_media_tracking_table(force_recreate=force_recreate):
            logger.error("Failed to create media tracking table")
            return False
        
        # Step 4: Test basic table operations
        if not self._test_table_operations():
            logger.error("Table operations test failed")
            return False
        
        logger.info("✅ Media tracking table created successfully with validation")
        return True
    
    def _test_table_operations(self):
        """Test basic table operations to ensure it's working correctly."""
        try:
            schema_definition = self.load_schema_definition()
            table_name = schema_definition['table_name']
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Test 1: Can we get the table?
            table = self.client.get_table(table_id)
            logger.info("✅ Table query test passed")
            
            # Test 2: Can we run a simple query?
            query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
            query_job = self.client.query(query)
            result = list(query_job)
            logger.info(f"✅ Table query test passed: {result[0].row_count} rows")
            
            # Test 3: Can we describe the table?
            schema_query = f"SELECT * FROM `{table_id}` LIMIT 0"
            schema_job = self.client.query(schema_query)
            schema_result = schema_job.result()
            logger.info(f"✅ Schema query test passed: {len(schema_result.schema)} fields")
            
            return True
            
        except Exception as e:
            logger.error(f"Table operations test failed: {str(e)}")
            return False

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create BigQuery media tracking table')
    parser.add_argument('--force-recreate', action='store_true', 
                       help='Force recreation of table if it already exists')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate schema without creating table')
    
    args = parser.parse_args()
    
    creator = MediaTrackingTableCreator()
    
    if args.validate_only:
        try:
            schema_definition = creator.load_schema_definition()
            print(f"✅ Schema validation passed for table '{schema_definition['table_name']}'")
            return 0
        except Exception as e:
            print(f"❌ Schema validation failed: {str(e)}")
            return 1
    
    success = creator.create_table_with_validation(force_recreate=args.force_recreate)
    
    if success:
        print("✅ Media tracking table created successfully")
        return 0
    else:
        print("❌ Failed to create media tracking table")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())