#!/usr/bin/env python3
"""
Test script for actual GCS upload with fixture data.

This script uploads fixture data to real GCS buckets to verify
production behavior of platform-aware date grouping.

USAGE:
    python test_actual_gcs_upload.py [--dry-run]
    
    --dry-run: Preview upload paths without actual upload
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from google.cloud import storage
import argparse

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from handlers.text_processor import TextProcessor
from handlers.gcs_processed_handler import GCSProcessedHandler


def test_actual_gcs_upload(dry_run=False):
    """Upload fixture data to actual GCS with platform-aware date grouping."""
    print("🚀 Testing actual GCS upload with platform-aware date grouping...")
    print(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL UPLOAD'}")
    print("=" * 60)
    
    # Load fixture data
    fixtures_dir = Path(__file__).parent.parent / 'fixtures'
    
    platforms_data = {
        'facebook': 'gcs-facebook-posts.json',
        'tiktok': 'gcs-tiktok-posts.json',
        'youtube': 'gcs-youtube-posts.json'
    }
    
    # Initialize handlers
    processor = TextProcessor()
    gcs_handler = GCSProcessedHandler()
    
    # Process each platform
    upload_summary = {}
    
    for platform, fixture_file in platforms_data.items():
        print(f"\n📋 Processing {platform.upper()} data...")
        
        # Load fixture data
        with open(fixtures_dir / fixture_file, 'r') as f:
            raw_data = json.load(f)
        
        print(f"  Loaded {len(raw_data)} {platform} posts")
        
        # Simulate metadata from data-ingestion service
        metadata = {
            'crawl_id': f'test_gcs_upload_{platform}_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'snapshot_id': f'test_snapshot_{platform}_001',
            'platform': platform,
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': datetime.utcnow().isoformat() + 'Z'
        }
        
        # Step 1: Process posts through TextProcessor
        processed_posts = processor.process_posts_for_analytics(raw_data, metadata)
        print(f"  ✅ Processed {len(processed_posts)} posts")
        
        # Step 2: Group by upload date (not crawl date)
        grouped_data = processor.get_grouped_data_for_gcs(processed_posts)
        print(f"  ✅ Grouped into {len(grouped_data)} date groups")
        
        # Display grouping results
        for date_key, posts in grouped_data.items():
            print(f"    {date_key}: {len(posts)} posts")
        
        # Step 3: Upload to GCS or preview paths
        upload_summary[platform] = []
        
        for date_key, posts in grouped_data.items():
            if date_key == 'unknown':
                print(f"    ⚠️  Skipping {len(posts)} posts with unknown dates")
                continue
            
            # Generate upload path
            upload_path = gcs_handler.get_upload_path_preview(metadata, date_key)
            
            if dry_run:
                # Just show what would be uploaded
                print(f"\n  📁 Would upload to: {upload_path}")
                print(f"     File: grouped_posts_{metadata['crawl_id']}.json")
                print(f"     Size: {len(json.dumps(posts))} bytes")
                print(f"     Posts: {len(posts)}")
                
                # Show sample post IDs
                sample_ids = [p.get('id', 'unknown')[:20] for p in posts[:3]]
                if sample_ids:
                    print(f"     Sample IDs: {', '.join(sample_ids)}...")
            else:
                # Actually upload to GCS
                try:
                    print(f"\n  📤 Uploading to: {upload_path}")
                    
                    # Extract just the directory path part (remove gs://bucket-name/ prefix and filename)
                    bucket_prefix = f"gs://{gcs_handler.bucket_name}/"
                    if upload_path.startswith(bucket_prefix):
                        clean_path = upload_path[len(bucket_prefix):]
                    else:
                        clean_path = upload_path
                    
                    # Extract directory path only (remove the filename)
                    if '/' in clean_path:
                        directory_path = '/'.join(clean_path.split('/')[:-1])  # Remove last part (filename)
                    else:
                        directory_path = clean_path
                    
                    # Create clean blob name with just the directory path + our filename
                    blob_name = f"{directory_path}/grouped_posts_{metadata['crawl_id']}.json"
                    
                    # Create upload data with metadata
                    upload_data = {
                        'metadata': {
                            'crawl_id': metadata['crawl_id'],
                            'snapshot_id': metadata['snapshot_id'],
                            'platform': platform,
                            'upload_date': date_key,
                            'post_count': len(posts),
                            'upload_timestamp': datetime.utcnow().isoformat() + 'Z'
                        },
                        'posts': posts
                    }
                    
                    # Get storage client and bucket
                    storage_client = storage.Client()
                    bucket_name = 'social-analytics-processed-data'
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)
                    
                    # Upload with content type and proper Unicode handling
                    blob.upload_from_string(
                        json.dumps(upload_data, indent=2, ensure_ascii=False),
                        content_type='application/json; charset=utf-8'
                    )
                    
                    print(f"     ✅ Uploaded successfully!")
                    print(f"     File: {blob_name}")
                    print(f"     Size: {blob.size} bytes")
                    print(f"     Posts: {len(posts)}")
                    
                    # Track successful upload
                    upload_summary[platform].append({
                        'date': date_key,
                        'path': blob_name,
                        'post_count': len(posts),
                        'size': blob.size
                    })
                    
                except Exception as e:
                    print(f"     ❌ Upload failed: {str(e)}")
                    upload_summary[platform].append({
                        'date': date_key,
                        'error': str(e)
                    })
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 UPLOAD SUMMARY")
    print("=" * 60)
    
    for platform, uploads in upload_summary.items():
        print(f"\n{platform.upper()}:")
        if not uploads:
            print("  No uploads performed")
        else:
            for upload in uploads:
                if 'error' in upload:
                    print(f"  ❌ {upload['date']}: {upload['error']}")
                else:
                    print(f"  ✅ {upload['date']}: {upload['post_count']} posts uploaded")
                    if not dry_run:
                        print(f"     Path: {upload['path']}")
                        print(f"     Size: {upload['size']} bytes")
    
    # Show example GCS commands to verify uploads
    if not dry_run and any(upload_summary.values()):
        print("\n" + "=" * 60)
        print("🔍 VERIFY UPLOADS WITH THESE COMMANDS:")
        print("=" * 60)
        
        print("\n# List all uploaded files:")
        print("gsutil ls -la gs://social-analytics-processed-data/raw_data/")
        
        print("\n# View a specific uploaded file:")
        for platform, uploads in upload_summary.items():
            for upload in uploads:
                if 'path' in upload:
                    print(f"gsutil cat gs://social-analytics-processed-data/{upload['path']} | jq .")
                    break
            if uploads:
                break
        
        print("\n# Download all uploaded files:")
        print("gsutil -m cp -r gs://social-analytics-processed-data/raw_data/ ./downloaded_data/")
    
    return upload_summary


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Test actual GCS upload with platform-aware date grouping'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview upload paths without actual upload'
    )
    
    args = parser.parse_args()
    
    try:
        # Check if running in GCP environment
        if not args.dry_run:
            try:
                from google.cloud import storage
                storage.Client()
                print("✅ GCS credentials detected\n")
            except Exception as e:
                print("⚠️  WARNING: GCS credentials not configured properly")
                print(f"   Error: {str(e)}")
                print("   Switching to dry-run mode...")
                args.dry_run = True
        
        # Run the upload test
        summary = test_actual_gcs_upload(dry_run=args.dry_run)
        
        # Return success if any uploads succeeded (or dry run)
        if args.dry_run:
            return 0
        else:
            # Check if any uploads succeeded
            for platform_uploads in summary.values():
                for upload in platform_uploads:
                    if 'path' in upload:
                        return 0
            return 1
            
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())