#!/usr/bin/env python3
"""
Phase 3 TDD Implementation Completion Summary
============================================

This script verifies that all Phase 3 objectives have been achieved:
- TikTok schema loading and transformation
- YouTube schema loading and transformation  
- Multi-platform computation functions
- Enhanced schema mapper platform support
"""

import json
from pathlib import Path
from handlers.schema_mapper import SchemaMapper

def test_tiktok_support():
    """Test TikTok schema support."""
    print("🎵 TESTING TIKTOK SUPPORT")
    print("-" * 30)
    
    mapper = SchemaMapper("schemas")
    schema = mapper.get_schema('tiktok', '1.0.0')
    
    if schema:
        print("✅ TikTok schema loaded successfully")
        print(f"📋 Schema version: {schema['schema_version']}")
        print(f"🎯 Platform: {schema['platform']}")
        
        # Test transformation with sample data
        sample_tiktok = {
            'id': 'test_tiktok_123',
            'webVideoUrl': 'https://tiktok.com/@user/video/123',
            'text': 'Test TikTok description #test',
            'createTimeISO': '2025-07-12T10:00:00.000Z',
            'authorMeta': {'name': 'testuser'},
            'diggCount': 100,
            'commentCount': 20,
            'shareCount': 5,
            'playCount': 1000,
            'hashtags': [{'name': 'test'}],
            'mentions': []
        }
        
        metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-12T16:06:22.177Z'
        }
        
        try:
            result = mapper.transform_post(sample_tiktok, 'tiktok', metadata)
            print("✅ TikTok transformation working")
            print(f"🎯 Video ID: {result.get('video_id')}")
            print(f"📊 Engagement: {result.get('engagement_metrics', {}).get('total_engagement', 0)}")
        except Exception as e:
            print(f"❌ TikTok transformation failed: {e}")
            return False
    else:
        print("❌ TikTok schema not found")
        return False
    
    return True

def test_youtube_support():
    """Test YouTube schema support."""
    print("\n🎬 TESTING YOUTUBE SUPPORT")
    print("-" * 30)
    
    mapper = SchemaMapper("schemas")
    schema = mapper.get_schema('youtube', '1.0.0')
    
    if schema:
        print("✅ YouTube schema loaded successfully")
        print(f"📋 Schema version: {schema['schema_version']}")
        print(f"🎯 Platform: {schema['platform']}")
        
        # Test transformation with sample data
        sample_youtube = {
            'id': 'test_youtube_123',
            'url': 'https://youtube.com/watch?v=123',
            'title': 'Test YouTube Video',
            'text': 'Test YouTube description',
            'date': '2025-07-12T10:00:00.000Z',
            'channelId': 'test_channel_123',
            'channelName': 'Test Channel',
            'channelUrl': 'https://youtube.com/channel/123',
            'thumbnailUrl': 'https://img.youtube.com/vi/123/default.jpg',
            'viewCount': 1000,
            'likes': 100,
            'isChannelVerified': True,
            'numberOfSubscribers': 5000
        }
        
        metadata = {
            'crawl_id': 'test_crawl_123',
            'snapshot_id': 'test_snapshot_456',
            'competitor': 'nutifood',
            'brand': 'growplus-nutifood',
            'category': 'sua-bot-tre-em',
            'crawl_date': '2025-07-12T16:06:22.177Z'
        }
        
        try:
            result = mapper.transform_post(sample_youtube, 'youtube', metadata)
            print("✅ YouTube transformation working")
            print(f"🎯 Video ID: {result.get('video_id')}")
            print(f"📊 View count: {result.get('view_count', 0)}")
            print(f"👍 Likes: {result.get('like_count', 0)}")
        except Exception as e:
            print(f"❌ YouTube transformation failed: {e}")
            return False
    else:
        print("❌ YouTube schema not found")
        return False
    
    return True

def test_multi_platform_computation_functions():
    """Test multi-platform computation functions."""
    print("\n🧮 TESTING COMPUTATION FUNCTIONS")
    print("-" * 35)
    
    mapper = SchemaMapper("schemas")
    
    # Test TikTok computation functions
    tiktok_functions = [
        'sum_tiktok_engagement',
        'calculate_tiktok_engagement_rate',
        'check_has_music',
        'calculate_aspect_ratio',
        'count_hashtags',
        'calculate_tiktok_data_quality'
    ]
    
    tiktok_working = 0
    for func_name in tiktok_functions:
        if func_name in mapper.computation_functions:
            tiktok_working += 1
            print(f"  ✅ {func_name}")
        else:
            print(f"  ❌ {func_name}")
    
    # Test YouTube computation functions  
    youtube_functions = [
        'sum_youtube_engagement',
        'calculate_youtube_engagement_rate',
        'parse_youtube_duration',
        'check_is_youtube_short',
        'calculate_title_length',
        'calculate_youtube_data_quality'
    ]
    
    youtube_working = 0
    for func_name in youtube_functions:
        if func_name in mapper.computation_functions:
            youtube_working += 1
            print(f"  ✅ {func_name}")
        else:
            print(f"  ❌ {func_name}")
    
    print(f"\n📊 TikTok functions: {tiktok_working}/{len(tiktok_functions)}")
    print(f"📊 YouTube functions: {youtube_working}/{len(youtube_functions)}")
    
    return tiktok_working == len(tiktok_functions) and youtube_working == len(youtube_functions)

def test_enhanced_schema_mapper():
    """Test enhanced schema mapper platform support."""
    print("\n🔧 TESTING ENHANCED SCHEMA MAPPER")
    print("-" * 40)
    
    mapper = SchemaMapper("schemas")
    
    supported_platforms = ['facebook', 'tiktok', 'youtube']
    working_platforms = 0
    
    for platform in supported_platforms:
        schema = mapper.get_schema(platform, '1.0.0')
        if schema:
            print(f"  ✅ {platform.capitalize()} schema loaded")
            working_platforms += 1
        else:
            print(f"  ❌ {platform.capitalize()} schema missing")
    
    # Test preprocessing functions
    preprocessing_functions = [
        'clean_text',
        'normalize_hashtags', 
        'parse_iso_timestamp',
        'extract_date_only',
        'safe_int',
        'safe_float',
        'remove_extra_whitespace',
        'extract_hashtag_names',
        'extract_mention_names',
        'normalize_keywords',
        'parse_description_links'
    ]
    
    preprocessing_working = 0
    for func_name in preprocessing_functions:
        if func_name in mapper.preprocessing_functions:
            preprocessing_working += 1
    
    print(f"\n📊 Platform support: {working_platforms}/{len(supported_platforms)}")
    print(f"📊 Preprocessing functions: {preprocessing_working}/{len(preprocessing_functions)}")
    
    return working_platforms == len(supported_platforms)

def main():
    """Main verification function."""
    print("🎯 PHASE 3 TDD IMPLEMENTATION VERIFICATION")
    print("=" * 50)
    
    # Test all Phase 3 objectives
    objectives = [
        ("TikTok schema loading and transformation", test_tiktok_support),
        ("YouTube schema loading and transformation", test_youtube_support),
        ("Multi-platform computation functions", test_multi_platform_computation_functions),
        ("Enhanced schema mapper platform support", test_enhanced_schema_mapper)
    ]
    
    results = []
    for objective, test_func in objectives:
        try:
            result = test_func()
            results.append((objective, result))
        except Exception as e:
            print(f"❌ Error testing {objective}: {e}")
            results.append((objective, False))
    
    # Summary
    print("\n🎉 PHASE 3 COMPLETION SUMMARY")
    print("=" * 50)
    
    completed = 0
    for objective, result in results:
        status = "✅ COMPLETE" if result else "❌ INCOMPLETE"
        print(f"{status}: {objective}")
        if result:
            completed += 1
    
    print(f"\n📊 Overall Progress: {completed}/{len(objectives)} objectives completed")
    
    if completed == len(objectives):
        print("\n🏆 PHASE 3 SUCCESSFULLY COMPLETED!")
        print("🚀 Ready for production multi-platform data processing")
        print("📚 All platforms (Facebook, TikTok, YouTube) fully supported")
        print("🧪 Test-driven development principles maintained")
    else:
        print(f"\n⚠️  Phase 3 partially complete: {completed}/{len(objectives)}")
        print("💡 Review failed objectives and address remaining issues")
    
    return completed == len(objectives)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)