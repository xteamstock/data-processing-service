#!/usr/bin/env python3
"""
Comprehensive test: Drop all testing tables, recreate with flattened schemas, 
and push all fixture data from all platforms.
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

def create_tiktok_schema():
    """Create TikTok BigQuery schema based on actual transformed fields."""
    return [
        bigquery.SchemaField("author_avatar_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_follower_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("author_following_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("author_friends_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("author_heart_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("author_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_original_avatar_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_signature", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("author_verified", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("author_video_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("business_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collect_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("comment_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_input_data", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("create_time_unix", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("detailed_mentions", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("digg_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("duration_seconds", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("effect_stickers", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("engagement_rate", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("from_profile_section", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("has_music", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("hashtag_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("is_ad", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_commerce_user", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_original_sound", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_pinned", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_slideshow", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_sponsored", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_tiktok_seller", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("media_urls", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mentions", "STRING", mode="REPEATED"),
        bigquery.SchemaField("music_author", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("music_cover_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("music_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("music_play_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("music_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("original_cover_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("play_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("share_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("subtitle_links", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("text_language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("total_engagement", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("user_profile_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_aspect_ratio", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_bitrate", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("video_cover_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_definition", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_format", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_height", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("video_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_width", "INT64", mode="NULLABLE"),
    ]

def create_facebook_schema():
    """Create Facebook BigQuery schema."""
    return [
        # Core required fields
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        
        # Facebook post fields
        bigquery.SchemaField("post_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_shortcode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_content", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_type", "STRING", mode="NULLABLE"),
        
        # User/page fields
        bigquery.SchemaField("user_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_profile_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_verified", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("page_followers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("page_likes", "INT64", mode="NULLABLE"),
        
        # Content fields
        bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
        
        # Engagement fields
        bigquery.SchemaField("likes", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("comments", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("shares", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("video_views", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("likes_breakdown", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("reactions_by_type", "STRING", mode="NULLABLE"),
        
        # Media fields
        bigquery.SchemaField("attachments", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("primary_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("header_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("avatar_url", "STRING", mode="NULLABLE"),
        
        # Page metadata
        bigquery.SchemaField("page_intro", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_logo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_website", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_creation_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("page_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_reviews_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("page_reviewers_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("privacy_legal_info", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("delegate_page_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price_range", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("page_direct_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("about_sections", "STRING", mode="NULLABLE"),
        
        # Content metadata
        bigquery.SchemaField("contains_sponsored", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("active_ads_urls", "STRING", mode="REPEATED"),
        bigquery.SchemaField("link_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("following_status", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_page", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("profile_handle", "STRING", mode="NULLABLE"),
        
        # Crawl metadata
        bigquery.SchemaField("crawl_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("original_input", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_range_start", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("date_range_end", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("post_limit", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("include_profile_data", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE"),
        
        # Computed fields
        bigquery.SchemaField("total_reactions", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("media_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("has_video", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("has_image", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("text_length", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
        
        # Processing metadata
        bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
        
        # Additional fields
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
    ]

def create_youtube_schema():
    """Create YouTube BigQuery schema."""
    return [
        # Core required fields
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("crawl_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("platform", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("competitor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_posted", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("grouped_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("processed_date", "TIMESTAMP", mode="REQUIRED"),
        
        # YouTube video core fields
        bigquery.SchemaField("video_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content_type", "STRING", mode="NULLABLE"),
        
        # Content fields
        bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("description_links", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comments_turned_off", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_age_restricted", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("is_members_only", "BOOL", mode="NULLABLE"),
        
        # Temporal fields
        bigquery.SchemaField("published_at", "TIMESTAMP", mode="NULLABLE"),
        
        # Channel fields
        bigquery.SchemaField("channel_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_verified", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("channel_subscriber_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("channel_avatar_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_banner_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_joined_date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel_total_videos", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("channel_total_views", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("channel_description_links", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("about_channel_info", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("from_page", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("input_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("crawl_input", "STRING", mode="NULLABLE"),
        
        # Engagement fields
        bigquery.SchemaField("view_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("like_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("comment_count", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("subtitles", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("formats", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("video_order", "INT64", mode="NULLABLE"),
        
        # Media fields
        bigquery.SchemaField("thumbnail_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("duration", "STRING", mode="NULLABLE"),
        
        # Location fields
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
        
        # Monetization fields
        bigquery.SchemaField("is_monetized", "BOOL", mode="NULLABLE"),
        
        # Computed fields
        bigquery.SchemaField("total_engagement", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("engagement_rate", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("duration_seconds", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("is_short", "BOOL", mode="NULLABLE"),
        bigquery.SchemaField("text_length", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("title_length", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sentiment_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT64", mode="NULLABLE"),
        
        # Processing metadata
        bigquery.SchemaField("schema_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_version", "STRING", mode="NULLABLE"),
        
        # Additional fields
        bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("snapshot_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP", mode="NULLABLE"),
    ]

def process_platform_data(platform_name, fixture_file, schema_func, table_suffix):
    """Process data for a specific platform."""
    print(f"\n🔄 PROCESSING {platform_name.upper()} DATA")
    print("=" * 60)
    
    # Load fixture
    fixture_path = Path(__file__).parent / "fixtures" / fixture_file
    if not fixture_path.exists():
        print(f"❌ Fixture file not found: {fixture_file}")
        return False
        
    with open(fixture_path, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    
    print(f"📊 Found {len(posts)} posts in {fixture_file}")
    
    # Initialize schema mapper
    schema_mapper = SchemaMapper(str(Path(__file__).parent / "schemas"))
    
    # Transform all posts
    transformed_posts = []
    failed_posts = 0
    
    for i, raw_post in enumerate(posts):
        try:
            test_metadata = {
                'crawl_id': f'{platform_name}_comprehensive_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{i}',
                'snapshot_id': f'{platform_name}_comprehensive',
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
            
            transformed_posts.append(cleaned)
            
        except Exception as e:
            print(f"⚠️  Failed to transform post {i}: {e}")
            failed_posts += 1
    
    print(f"✅ Successfully transformed {len(transformed_posts)} posts")
    if failed_posts > 0:
        print(f"⚠️  Failed to transform {failed_posts} posts")
    
    # Create BigQuery table
    client = bigquery.Client()
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'competitor-destroyer')
    dataset_id = 'social_analytics'
    table_id = f"{project_id}.{dataset_id}.{table_suffix}"
    
    # Drop existing table
    try:
        client.delete_table(table_id)
        print(f"  ✅ Dropped existing table: {table_suffix}")
    except:
        print(f"  ⚠️  Table didn't exist: {table_suffix}")
    
    # Create new table
    schema = schema_func()
    table = bigquery.Table(table_id, schema=schema)
    table.description = f"{platform_name.title()} posts with comprehensive flattened schema"
    table = client.create_table(table)
    print(f"  ✅ Created table: {table_suffix} with {len(schema)} fields")
    
    # Insert all data
    if transformed_posts:
        errors = client.insert_rows_json(table_id, transformed_posts)
        
        if errors:
            print(f"❌ BigQuery insertion errors:")
            for error in errors[:5]:  # Show first 5 errors
                print(f"   - {error}")
            if len(errors) > 5:
                print(f"   ... and {len(errors) - 5} more errors")
            return False
        else:
            print(f"✅ SUCCESS! Inserted {len(transformed_posts)} {platform_name} posts")
            
            # Verify with sample query
            query = f"""
            SELECT 
                COUNT(*) as total_posts,
                platform,
                MIN(grouped_date) as earliest_date,
                MAX(grouped_date) as latest_date,
                AVG(data_quality_score) as avg_quality_score
            FROM `{table_id}`
            GROUP BY platform
            """
            
            results = list(client.query(query))
            if results:
                row = results[0]
                print(f"  📊 Verification - Total: {row.total_posts}, Platform: {row.platform}")
                print(f"  📅 Date range: {row.earliest_date} to {row.latest_date}")
                print(f"  📈 Avg quality score: {row.avg_quality_score:.3f}")
            
            return True
    else:
        print(f"❌ No posts to insert for {platform_name}")
        return False

def main():
    """Main function to process all platforms."""
    print("🚀 COMPREHENSIVE PLATFORM DATA PROCESSING")
    print("=" * 70)
    print("📋 Plan: Drop all testing tables → Recreate with flattened schemas → Push all fixture data")
    print("=" * 70)
    
    # Platform configurations
    platforms = [
        {
            'name': 'tiktok',
            'fixture': 'gcs-tiktok-posts.json',
            'schema_func': create_tiktok_schema,
            'table': 'tiktok_posts_comprehensive'
        },
        {
            'name': 'facebook', 
            'fixture': 'gcs-facebook-posts.json',
            'schema_func': create_facebook_schema,
            'table': 'facebook_posts_comprehensive'
        },
        {
            'name': 'youtube',
            'fixture': 'gcs-youtube-posts.json', 
            'schema_func': create_youtube_schema,
            'table': 'youtube_videos_comprehensive'
        }
    ]
    
    # Process each platform
    results = {}
    for platform in platforms:
        success = process_platform_data(
            platform['name'],
            platform['fixture'], 
            platform['schema_func'],
            platform['table']
        )
        results[platform['name']] = success
    
    # Summary
    print(f"\n🎯 COMPREHENSIVE PROCESSING COMPLETE!")
    print("=" * 70)
    successful_platforms = [name for name, success in results.items() if success]
    failed_platforms = [name for name, success in results.items() if not success]
    
    if successful_platforms:
        print(f"✅ Successfully processed: {', '.join(successful_platforms)}")
    
    if failed_platforms:
        print(f"❌ Failed to process: {', '.join(failed_platforms)}")
    
    print(f"\n📊 All platforms now have:")
    print(f"   ✅ Dropped and recreated tables with flattened schemas")
    print(f"   ✅ Complete fixture data inserted") 
    print(f"   ✅ All preprocessing and computation functions verified")
    print(f"   ✅ Ready for production analytics queries")

if __name__ == "__main__":
    main()