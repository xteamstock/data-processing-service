#!/usr/bin/env python3
"""
Script to create BigQuery tables for data processing service.

This script creates the necessary BigQuery tables based on the schema definitions
in the schemas/ directory.
"""

import os
import json
import logging
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryTableCreator:
    """Create BigQuery tables from schema definitions."""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'social_analytics')
        self.schemas_dir = Path(__file__).parent.parent / 'schemas'
    
    def create_dataset_if_not_exists(self):
        """Create dataset if it doesn't exist."""
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            # Dataset doesn't exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set location
            dataset.description = "Social media analytics data"
            
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def create_posts_table(self):
        """Create posts table from schema definition."""
        schema_file = self.schemas_dir / 'bigquery_table_schema.json'
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        try:
            with open(schema_file, 'r') as f:
                schema_config = json.load(f)
            
            # Convert schema to BigQuery format
            schema_fields = self._convert_schema_to_bigquery(schema_config['fields'])
            
            # Create table reference
            table_id = f"{self.project_id}.{self.dataset_id}.{schema_config['table_name']}"
            
            # Check if table exists
            try:
                table = self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                return True
            except Exception:
                pass  # Table doesn't exist
            
            # Create table
            table = bigquery.Table(table_id, schema=schema_fields)
            table.description = schema_config.get('description', '')
            
            # Set partitioning
            partitioning = schema_config.get('partitioning')
            if partitioning and partitioning['type'] == 'TIME':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partitioning['field']
                )
            
            # Set clustering
            clustering = schema_config.get('clustering')
            if clustering:
                table.clustering_fields = clustering['fields']
            
            # Create the table
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating posts table: {str(e)}")
            return False
    
    def create_processing_events_table(self):
        """Create processing events table for monitoring."""
        table_id = f"{self.project_id}.{self.dataset_id}.processing_events"
        
        try:
            # Check if table exists
            try:
                table = self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                return True
            except Exception:
                pass  # Table doesn't exist
            
            # Define schema for processing events
            schema = [
                bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("crawl_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("processing_duration_seconds", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("post_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("media_count", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("success", "BOOL", mode="REQUIRED"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE")
            ]
            
            # Create table
            table = bigquery.Table(table_id, schema=schema)
            table.description = "Processing events for monitoring data processing operations"
            
            # Partition by event_timestamp
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_timestamp"
            )
            
            # Cluster by event_type
            table.clustering_fields = ["event_type", "success"]
            
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating processing events table: {str(e)}")
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
    
    def create_all_tables(self):
        """Create all required tables."""
        logger.info("Creating BigQuery tables for data processing service")
        
        # Create dataset
        self.create_dataset_if_not_exists()
        
        # Create tables
        success = True
        
        if not self.create_posts_table():
            success = False
        
        if not self.create_processing_events_table():
            success = False
        
        if success:
            logger.info("All tables created successfully")
        else:
            logger.error("Some tables failed to create")
        
        return success

def main():
    """Main function."""
    creator = BigQueryTableCreator()
    success = creator.create_all_tables()
    
    if success:
        print("✅ BigQuery tables created successfully")
        return 0
    else:
        print("❌ Failed to create some BigQuery tables")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())