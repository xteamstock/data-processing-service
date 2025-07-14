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
    
    def create_platform_specific_tables(self):
        """Create separate platform-specific tables for optimal performance."""
        platforms = [
            {
                'name': 'facebook',
                'table_name': 'facebook_posts',
                'description': 'Facebook posts analytics table',
                'cluster_fields': ['competitor', 'brand', 'page_name']
            },
            {
                'name': 'tiktok', 
                'table_name': 'tiktok_posts',
                'description': 'TikTok posts analytics table',
                'cluster_fields': ['competitor', 'brand', 'user_username']
            },
            {
                'name': 'youtube',
                'table_name': 'youtube_posts', 
                'description': 'YouTube videos analytics table',
                'cluster_fields': ['competitor', 'brand', 'channel_name']
            }
        ]
        
        success = True
        for platform in platforms:
            if not self._create_platform_table(platform):
                success = False
        
        return success
    
    def _create_platform_table(self, platform_config):
        """Create a platform-specific table with its own optimized schema."""
        platform_name = platform_config['name']
        table_name = platform_config['table_name']
        
        try:
            # Get platform-specific schema directly
            schema_fields = self._get_platform_schema_fields(platform_name)
            
            # Create table reference
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Check if table exists
            try:
                table = self.client.get_table(table_id)
                logger.info(f"Table {table_id} already exists")
                return True
            except Exception:
                pass  # Table doesn't exist
            
            # Create table
            table = bigquery.Table(table_id, schema=schema_fields)
            table.description = platform_config['description']
            
            # Set partitioning by date_posted
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date_posted"
            )
            
            # Set platform-specific clustering
            table.clustering_fields = platform_config['cluster_fields']
            
            # Create the table
            table = self.client.create_table(table)
            logger.info(f"Created platform table {table_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating {platform_name} table: {str(e)}")
            return False
    
    def _get_platform_schema_fields(self, platform_name):
        """Get complete platform-specific BigQuery schema fields."""
        
        # Common core fields for all platforms
        core_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED", "description": "Unique identifier"},
            {"name": "crawl_id", "type": "STRING", "mode": "REQUIRED", "description": "Crawl session identifier"},
            {"name": "snapshot_id", "type": "STRING", "mode": "NULLABLE", "description": "Source snapshot identifier"},
            {"name": "platform", "type": "STRING", "mode": "REQUIRED", "description": "Platform name"},
            {"name": "competitor", "type": "STRING", "mode": "REQUIRED", "description": "Competitor identifier"},
            {"name": "brand", "type": "STRING", "mode": "NULLABLE", "description": "Brand identifier"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE", "description": "Category identifier"},
            {"name": "date_posted", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Post publish date"},
            {"name": "crawl_date", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Crawl date"},
            {"name": "processed_date", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Processing date"},
            {"name": "grouped_date", "type": "DATE", "mode": "REQUIRED", "description": "Date for grouping"},
            {"name": "user_url", "type": "STRING", "mode": "NULLABLE", "description": "User profile URL"},
            {"name": "user_username", "type": "STRING", "mode": "NULLABLE", "description": "Username"},
            {"name": "user_profile_id", "type": "STRING", "mode": "NULLABLE", "description": "User profile ID"}
        ]
        
        # Platform-specific schemas
        if platform_name == 'facebook':
            return self._convert_schema_to_bigquery(core_fields + [
                {"name": "post_id", "type": "STRING", "mode": "REQUIRED", "description": "Facebook post ID"},
                {"name": "post_url", "type": "STRING", "mode": "REQUIRED", "description": "Facebook post URL"},
                {"name": "post_content", "type": "STRING", "mode": "NULLABLE", "description": "Post text content"},
                {"name": "post_type", "type": "STRING", "mode": "NULLABLE", "description": "Post type"},
                {"name": "page_name", "type": "STRING", "mode": "NULLABLE", "description": "Facebook page name"},
                {"name": "page_category", "type": "STRING", "mode": "NULLABLE", "description": "Page category"},
                {"name": "page_verified", "type": "BOOL", "mode": "NULLABLE", "description": "Page verification status"},
                {"name": "page_followers", "type": "INT64", "mode": "NULLABLE", "description": "Page followers"},
                {"name": "page_likes", "type": "INT64", "mode": "NULLABLE", "description": "Page likes"},
                {"name": "engagement_metrics", "type": "JSON", "mode": "NULLABLE", "description": "Engagement data"},
                {"name": "content_analysis", "type": "JSON", "mode": "NULLABLE", "description": "Content analysis"},
                {"name": "media_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Media attachments"},
                {"name": "page_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Page information"},
                {"name": "processing_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Processing info"}
            ])
        
        elif platform_name == 'tiktok':
            return self._convert_schema_to_bigquery(core_fields + [
                {"name": "video_id", "type": "STRING", "mode": "REQUIRED", "description": "TikTok video ID"},
                {"name": "video_url", "type": "STRING", "mode": "REQUIRED", "description": "TikTok video URL"},
                {"name": "description", "type": "STRING", "mode": "NULLABLE", "description": "Video description"},
                {"name": "author_name", "type": "STRING", "mode": "NULLABLE", "description": "Author display name"},
                {"name": "author_verified", "type": "BOOL", "mode": "NULLABLE", "description": "Author verification"},
                {"name": "author_follower_count", "type": "INT64", "mode": "NULLABLE", "description": "Author followers"},
                {"name": "play_count", "type": "INT64", "mode": "NULLABLE", "description": "Video plays"},
                {"name": "digg_count", "type": "INT64", "mode": "NULLABLE", "description": "Video likes"},
                {"name": "share_count", "type": "INT64", "mode": "NULLABLE", "description": "Video shares"},
                {"name": "comment_count", "type": "INT64", "mode": "NULLABLE", "description": "Video comments"},
                {"name": "engagement_metrics", "type": "JSON", "mode": "NULLABLE", "description": "Engagement data"},
                {"name": "content_analysis", "type": "JSON", "mode": "NULLABLE", "description": "Content analysis"},
                {"name": "video_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Video information"},
                {"name": "author_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Author information"},
                {"name": "processing_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Processing info"}
            ])
        
        elif platform_name == 'youtube':
            return self._convert_schema_to_bigquery(core_fields + [
                {"name": "video_id", "type": "STRING", "mode": "REQUIRED", "description": "YouTube video ID"},
                {"name": "video_url", "type": "STRING", "mode": "REQUIRED", "description": "YouTube video URL"},
                {"name": "title", "type": "STRING", "mode": "NULLABLE", "description": "Video title"},
                {"name": "description", "type": "STRING", "mode": "NULLABLE", "description": "Video description"},
                {"name": "channel_id", "type": "STRING", "mode": "NULLABLE", "description": "YouTube channel ID"},
                {"name": "channel_name", "type": "STRING", "mode": "NULLABLE", "description": "Channel name"},
                {"name": "channel_verified", "type": "BOOL", "mode": "NULLABLE", "description": "Channel verification"},
                {"name": "channel_subscriber_count", "type": "INT64", "mode": "NULLABLE", "description": "Channel subscribers"},
                {"name": "view_count", "type": "INT64", "mode": "NULLABLE", "description": "Video views"},
                {"name": "like_count", "type": "INT64", "mode": "NULLABLE", "description": "Video likes"},
                {"name": "comment_count", "type": "INT64", "mode": "NULLABLE", "description": "Video comments"},
                {"name": "published_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Video publish date"},
                {"name": "engagement_metrics", "type": "JSON", "mode": "NULLABLE", "description": "Engagement data"},
                {"name": "content_analysis", "type": "JSON", "mode": "NULLABLE", "description": "Content analysis"},
                {"name": "video_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Video information"},
                {"name": "channel_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Channel information"},
                {"name": "processing_metadata", "type": "JSON", "mode": "NULLABLE", "description": "Processing info"}
            ])
        
        else:
            raise ValueError(f"Unknown platform: {platform_name}")
    
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
        
        # Create platform-specific tables (Facebook, TikTok, YouTube)
        if not self.create_platform_specific_tables():
            success = False
        
        # Create monitoring table
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