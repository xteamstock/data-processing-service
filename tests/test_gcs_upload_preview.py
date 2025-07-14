#!/usr/bin/env python3
"""
Preview GCS upload paths and grouping for fixture data.

Shows exactly how fixture data would be uploaded to GCS with
platform-aware date grouping, without requiring GCS credentials.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from handlers.text_processor import TextProcessor
from handlers.gcs_processed_handler import GCSProcessedHandler


def preview_gcs_upload_structure():
    """Preview how fixture data would be uploaded to GCS."""
    print("🔍 Previewing GCS Upload Structure for Fixture Data")
    print("=" * 80)
    
    # Load all fixture data
    fixtures_dir = Path(__file__).parent.parent / 'fixtures'
    
    platforms_data = {
        'facebook': 'gcs-facebook-posts.json',
        'tiktok': 'gcs-tiktok-posts.json',
        'youtube': 'gcs-youtube-posts.json'
    }
    
    # Initialize handlers
    processor = TextProcessor()
    gcs_handler = GCSProcessedHandler()
    
    # Track all uploads for summary
    all_uploads = defaultdict(list)
    total_posts_by_platform = defaultdict(int)
    
    for platform, fixture_file in platforms_data.items():
        print(f"\n📋 {platform.upper()} Platform")
        print("-" * 40)
        
        # Load fixture data
        with open(fixtures_dir / fixture_file, 'r') as f:
            raw_data = json.load(f)
        
        print(f"Total posts in fixture: {len(raw_data)}")
        
        # Create metadata
        metadata = {
            'crawl_id': f'preview_{platform}_crawl',
            'snapshot_id': f'preview_{platform}_snapshot',
            'platform': platform,
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-13T10:00:00Z'
        }
        
        # Process posts
        processed_posts = processor.process_posts_for_analytics(raw_data, metadata)
        total_posts_by_platform[platform] = len(processed_posts)
        
        # Group by upload date
        grouped_data = processor.get_grouped_data_for_gcs(processed_posts)
        
        print(f"\nDate Grouping Results:")
        for date_key, posts in sorted(grouped_data.items()):
            if date_key == 'unknown':
                print(f"  ⚠️  {date_key}: {len(posts)} posts (missing dates)")
            else:
                print(f"  📅 {date_key}: {len(posts)} posts")
                
                # Show sample post details
                for i, post in enumerate(posts[:2]):  # First 2 posts
                    print(f"      Post {i+1}: {post.get('id', 'unknown')[:30]}...")
                    if 'text' in post:
                        preview_text = post['text'][:50].replace('\n', ' ')
                        print(f"      Text: \"{preview_text}...\"")
                    if 'media_urls' in post and post['media_urls']:
                        print(f"      Media: {len(post['media_urls'])} items")
                
                if len(posts) > 2:
                    print(f"      ... and {len(posts) - 2} more posts")
        
        # Generate GCS paths
        print(f"\nGCS Upload Paths:")
        for date_key, posts in sorted(grouped_data.items()):
            if date_key == 'unknown':
                continue
                
            upload_path = gcs_handler.get_upload_path_preview(metadata, date_key)
            print(f"  📁 {upload_path}")
            
            # Parse path to show hierarchy
            path_parts = upload_path.split('/')
            if len(path_parts) >= 8:
                year = path_parts[-3].split('=')[1]
                month = path_parts[-2].split('=')[1]
                day = path_parts[-1].split('=')[1]
                print(f"     └─ Date breakdown: {year}/{month}/{day}")
            
            # Track for summary
            all_uploads[date_key].append({
                'platform': platform,
                'post_count': len(posts),
                'path': upload_path
            })
    
    # Print cross-platform summary
    print("\n" + "=" * 80)
    print("📊 CROSS-PLATFORM UPLOAD SUMMARY")
    print("=" * 80)
    
    print(f"\nTotal Posts by Platform:")
    for platform, count in total_posts_by_platform.items():
        print(f"  {platform}: {count} posts")
    
    print(f"\nUnique Upload Dates: {len(all_uploads)}")
    
    print(f"\nUpload Date Distribution:")
    for date_key in sorted(all_uploads.keys()):
        uploads = all_uploads[date_key]
        total_posts = sum(u['post_count'] for u in uploads)
        platforms = [u['platform'] for u in uploads]
        
        print(f"\n  📅 {date_key}: {total_posts} posts total")
        for upload in uploads:
            print(f"     {upload['platform']}: {upload['post_count']} posts")
    
    # Show sample GCS structure
    print("\n" + "=" * 80)
    print("🗂️  FULL GCS DIRECTORY STRUCTURE")
    print("=" * 80)
    
    print("\nsocial-analytics-processed-data/")
    print("└── raw_data/")
    
    # Group paths by platform
    paths_by_platform = defaultdict(list)
    for uploads in all_uploads.values():
        for upload in uploads:
            paths_by_platform[upload['platform']].append(upload['path'])
    
    for platform in sorted(paths_by_platform.keys()):
        print(f"    └── platform={platform}/")
        print(f"        └── competitor=nutifood/")
        print(f"            └── brand=growplus-nutifood/")
        print(f"                └── category=sua-bot-tre-em/")
        
        # Extract unique years
        years = set()
        for path in paths_by_platform[platform]:
            parts = path.split('/')
            for part in parts:
                if part.startswith('year='):
                    years.add(part.split('=')[1])
        
        for year in sorted(years):
            print(f"                    └── year={year}/")
            
            # Get months for this year
            months = set()
            for path in paths_by_platform[platform]:
                if f"year={year}" in path:
                    parts = path.split('/')
                    for i, part in enumerate(parts):
                        if part.startswith('month='):
                            months.add(part.split('=')[1])
            
            for month in sorted(months):
                print(f"                        └── month={month}/")
                
                # Get days for this month
                days = []
                for path in paths_by_platform[platform]:
                    if f"year={year}" in path and f"month={month}" in path:
                        parts = path.split('/')
                        for part in parts:
                            if part.startswith('day='):
                                day = part.split('=')[1]
                                # Count posts for this day
                                post_count = 0
                                for uploads in all_uploads.values():
                                    for upload in uploads:
                                        if upload['path'] == path:
                                            post_count = upload['post_count']
                                days.append((day, post_count))
                
                for day, count in sorted(set(days)):
                    print(f"                            └── day={day}/")
                    print(f"                                └── grouped_posts_*.json ({count} posts)")
    
    print("\n" + "=" * 80)
    print("✅ Preview complete! This shows exactly how fixture data would be organized in GCS.")
    print("   Each JSON file contains posts from the same upload date, not crawl date.")


def main():
    """Run the preview."""
    try:
        preview_gcs_upload_structure()
        return 0
    except Exception as e:
        print(f"\n❌ Preview failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())