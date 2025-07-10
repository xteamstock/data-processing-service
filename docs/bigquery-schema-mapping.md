# BigQuery Schema Mapping for Facebook Posts

## 📋 Overview

This document defines the BigQuery schema and data transformation logic for processing Facebook posts from BrightData JSON format into structured analytics tables.

## 🗂️ BigQuery Schema Design

### Main Posts Table: `social_analytics.posts`

```sql
CREATE TABLE `competitor-destroyer.social_analytics.posts` (
  -- Core identifiers
  id STRING,
  post_id STRING,
  crawl_id STRING,
  snapshot_id STRING,
  platform STRING,
  
  -- Post content and metadata
  post_url STRING,
  post_content STRING,
  post_type STRING,
  date_posted TIMESTAMP,
  crawl_date TIMESTAMP,
  processed_date TIMESTAMP,
  grouped_date DATE,
  
  -- User/Page information
  user_url STRING,
  user_username STRING,
  user_profile_id STRING,
  page_name STRING,
  page_category STRING,
  page_verified BOOL,
  page_followers INT64,
  page_likes INT64,
  
  -- Analytics metadata
  competitor STRING,
  brand STRING,
  category STRING,
  
  -- Engagement metrics
  engagement_metrics STRUCT<
    likes INT64,
    comments INT64,
    shares INT64,
    reactions INT64,
    reactions_by_type ARRAY<STRUCT<
      type STRING,
      count INT64
    >>
  >,
  
  -- Content analysis
  content_analysis STRUCT<
    text_length INT64,
    language STRING,
    sentiment_score FLOAT64,
    hashtags ARRAY<STRING>,
    contains_sponsored BOOL
  >,
  
  -- Media metadata
  media_metadata STRUCT<
    media_count INT64,
    has_video BOOL,
    has_image BOOL,
    media_processing_requested BOOL,
    attachments ARRAY<STRUCT<
      id STRING,
      type STRING,
      url STRING,
      attachment_url STRING
    >>
  >,
  
  -- Page metadata (for competitor analysis)
  page_metadata STRUCT<
    page_intro STRING,
    page_logo STRING,
    page_website STRING,
    page_phone STRING,
    page_email STRING,
    page_creation_date TIMESTAMP,
    page_address STRING,
    page_reviews_score FLOAT64,
    page_reviewers_count INT64
  >,
  
  -- Processing metadata
  processing_metadata STRUCT<
    processing_duration_seconds INT64,
    processing_version STRING,
    source_gcs_path STRING,
    data_quality_score FLOAT64
  >
)
PARTITION BY DATE(date_posted)
CLUSTER BY platform, competitor, brand, page_name;
```

### Media Files Table: `social_analytics.media_files`

```sql
CREATE TABLE `competitor-destroyer.social_analytics.media_files` (
  -- Identifiers
  media_id STRING,
  post_id STRING,
  crawl_id STRING,
  
  -- Media metadata
  media_type STRING, -- Photo, Video, Link
  media_url STRING,
  attachment_url STRING,
  
  -- Processing status
  download_status STRING, -- pending, completed, failed
  processing_status STRING, -- pending, completed, failed
  
  -- File metadata
  file_size INT64,
  file_format STRING,
  gcs_path STRING,
  
  -- ML analysis results (future)
  ml_analysis STRUCT<
    objects_detected ARRAY<STRING>,
    text_extracted STRING,
    sentiment_score FLOAT64,
    brand_logos_detected ARRAY<STRING>
  >,
  
  -- Timestamps
  created_date TIMESTAMP,
  processed_date TIMESTAMP
)
PARTITION BY DATE(created_date)
CLUSTER BY media_type, processing_status;
```

## 🔄 Data Transformation Logic

### Core Data Processor Function

```python
def process_facebook_post_for_bigquery(facebook_post: Dict, metadata: Dict) -> Dict:
    """
    Transform Facebook post JSON from BrightData to BigQuery format.
    
    Args:
        facebook_post: Raw Facebook post from BrightData
        metadata: Crawl metadata (crawl_id, competitor, etc.)
    
    Returns:
        Dict ready for BigQuery insertion
    """
    
    # Extract core post data
    post_data = {
        'id': f"{facebook_post.get('post_id', '')}_{metadata.get('crawl_id', '')}",
        'post_id': facebook_post.get('post_id', ''),
        'crawl_id': metadata.get('crawl_id', ''),
        'snapshot_id': metadata.get('snapshot_id', ''),
        'platform': 'facebook',
        
        # Post content
        'post_url': facebook_post.get('url', ''),
        'post_content': clean_text(facebook_post.get('content', '')),
        'post_type': facebook_post.get('post_type', 'Post'),
        'date_posted': parse_timestamp(facebook_post.get('date_posted')),
        'crawl_date': parse_timestamp(metadata.get('crawl_date')),
        'processed_date': datetime.utcnow().isoformat(),
        'grouped_date': extract_date_for_grouping(facebook_post.get('date_posted')),
        
        # User/Page information
        'user_url': facebook_post.get('user_url', ''),
        'user_username': facebook_post.get('user_username_raw', ''),
        'user_profile_id': facebook_post.get('profile_id', ''),
        'page_name': facebook_post.get('page_name', ''),
        'page_category': facebook_post.get('page_category', ''),
        'page_verified': facebook_post.get('page_is_verified', False),
        'page_followers': safe_int(facebook_post.get('page_followers', 0)),
        'page_likes': safe_int(facebook_post.get('page_likes', 0)),
        
        # Analytics metadata
        'competitor': metadata.get('competitor', ''),
        'brand': metadata.get('brand', ''),
        'category': metadata.get('category', ''),
        
        # Engagement metrics
        'engagement_metrics': extract_engagement_metrics(facebook_post),
        
        # Content analysis
        'content_analysis': analyze_content(facebook_post),
        
        # Media metadata
        'media_metadata': extract_media_metadata(facebook_post),
        
        # Page metadata
        'page_metadata': extract_page_metadata(facebook_post),
        
        # Processing metadata
        'processing_metadata': {
            'processing_duration_seconds': 0,  # To be updated
            'processing_version': '1.0.0',
            'source_gcs_path': metadata.get('gcs_path', ''),
            'data_quality_score': calculate_data_quality_score(facebook_post)
        }
    }
    
    return post_data

def extract_engagement_metrics(facebook_post: Dict) -> Dict:
    """Extract engagement metrics from Facebook post."""
    
    # Parse reactions by type
    reactions_by_type = []
    total_reactions = 0
    
    count_reactions = facebook_post.get('count_reactions_type', [])
    for reaction in count_reactions:
        reaction_data = {
            'type': reaction.get('type', ''),
            'count': safe_int(reaction.get('reaction_count', 0))
        }
        reactions_by_type.append(reaction_data)
        total_reactions += reaction_data['count']
    
    # Handle legacy likes field
    if not total_reactions and facebook_post.get('likes'):
        total_reactions = safe_int(facebook_post.get('likes', 0))
        reactions_by_type.append({
            'type': 'Like',
            'count': total_reactions
        })
    
    return {
        'likes': safe_int(facebook_post.get('likes', 0)),
        'comments': safe_int(facebook_post.get('num_comments', 0)),
        'shares': safe_int(facebook_post.get('num_shares', 0)),
        'reactions': total_reactions,
        'reactions_by_type': reactions_by_type
    }

def analyze_content(facebook_post: Dict) -> Dict:
    """Analyze post content for insights."""
    content = facebook_post.get('content', '')
    hashtags = facebook_post.get('hashtags', [])
    
    # Basic sentiment analysis
    sentiment_score = 0.0
    language = 'unknown'
    
    if content:
        try:
            from textblob import TextBlob
            blob = TextBlob(content)
            sentiment_score = blob.sentiment.polarity
            language = blob.detect_language()
        except Exception:
            pass
    
    return {
        'text_length': len(content),
        'language': language,
        'sentiment_score': sentiment_score,
        'hashtags': hashtags,
        'contains_sponsored': facebook_post.get('is_sponsored', False)
    }

def extract_media_metadata(facebook_post: Dict) -> Dict:
    """Extract media metadata for separate processing."""
    attachments = facebook_post.get('attachments', [])
    
    if not attachments:
        return {
            'media_count': 0,
            'has_video': False,
            'has_image': False,
            'media_processing_requested': False,
            'attachments': []
        }
    
    processed_attachments = []
    has_video = False
    has_image = False
    
    for attachment in attachments:
        attachment_data = {
            'id': attachment.get('id', ''),
            'type': attachment.get('type', ''),
            'url': attachment.get('url', ''),
            'attachment_url': attachment.get('attachment_url', '')
        }
        processed_attachments.append(attachment_data)
        
        # Check media types
        if attachment.get('type', '').lower() in ['video']:
            has_video = True
        elif attachment.get('type', '').lower() in ['photo', 'image']:
            has_image = True
    
    return {
        'media_count': len(attachments),
        'has_video': has_video,
        'has_image': has_image,
        'media_processing_requested': len(attachments) > 0,
        'attachments': processed_attachments
    }

def extract_page_metadata(facebook_post: Dict) -> Dict:
    """Extract page metadata for competitor analysis."""
    about_sections = facebook_post.get('about', [])
    
    # Parse about sections
    page_address = ''
    for section in about_sections:
        if section.get('type') == 'ADDRESS':
            page_address = section.get('value', '')
            break
    
    return {
        'page_intro': facebook_post.get('page_intro', ''),
        'page_logo': facebook_post.get('page_logo', ''),
        'page_website': facebook_post.get('page_external_website', ''),
        'page_phone': facebook_post.get('page_phone', ''),
        'page_email': facebook_post.get('page_email', ''),
        'page_creation_date': parse_timestamp(facebook_post.get('page_creation_time')),
        'page_address': page_address,
        'page_reviews_score': safe_float(facebook_post.get('page_reviews_score')),
        'page_reviewers_count': safe_int(facebook_post.get('page_reviewers_amount', 0))
    }

def calculate_data_quality_score(facebook_post: Dict) -> float:
    """Calculate data quality score based on available fields."""
    score = 0.0
    max_score = 10.0
    
    # Core content (40%)
    if facebook_post.get('content'):
        score += 4.0
    
    # Engagement data (20%)
    if facebook_post.get('likes') or facebook_post.get('num_comments') or facebook_post.get('num_shares'):
        score += 2.0
    
    # Media attachments (20%)
    if facebook_post.get('attachments'):
        score += 2.0
    
    # Page metadata (10%)
    if facebook_post.get('page_name') and facebook_post.get('page_category'):
        score += 1.0
    
    # Date information (10%)
    if facebook_post.get('date_posted'):
        score += 1.0
    
    return score / max_score

# Utility functions
def clean_text(text: str) -> str:
    """Clean text content for BigQuery storage."""
    if not text:
        return ""
    
    # Remove extra whitespace
    import re
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Limit length for BigQuery
    if len(text) > 50000:  # BigQuery STRING limit
        text = text[:50000] + "..."
    
    return text

def parse_timestamp(date_str: str) -> str:
    """Parse timestamp for BigQuery TIMESTAMP format."""
    if not date_str:
        return datetime.utcnow().isoformat()
    
    try:
        if isinstance(date_str, str):
            # Handle ISO format
            if 'T' in date_str:
                return date_str.replace('Z', '+00:00')
            else:
                # Try parsing as timestamp
                from datetime import datetime
                parsed = datetime.fromtimestamp(float(date_str))
                return parsed.isoformat()
        return str(date_str)
    except Exception:
        return datetime.utcnow().isoformat()

def extract_date_for_grouping(date_str: str) -> str:
    """Extract date for grouping (YYYY-MM-DD format)."""
    if not date_str:
        return datetime.utcnow().strftime('%Y-%m-%d')
    
    try:
        if 'T' in date_str:
            return date_str.split('T')[0]
        else:
            return str(date_str)[:10]
    except Exception:
        return datetime.utcnow().strftime('%Y-%m-%d')

def safe_int(value) -> int:
    """Safely convert value to integer."""
    try:
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0

def safe_float(value) -> float:
    """Safely convert value to float."""
    try:
        return float(value) if value is not None else 0.0
    except (ValueError, TypeError):
        return 0.0
```

## 🎯 Media Processing Events

For the Media Processing service, extract media files:

```python
def extract_media_files_for_processing(facebook_post: Dict, metadata: Dict) -> List[Dict]:
    """Extract media files for separate processing service."""
    media_files = []
    
    attachments = facebook_post.get('attachments', [])
    if not attachments:
        return media_files
    
    for attachment in attachments:
        media_file = {
            'media_id': attachment.get('id', ''),
            'post_id': facebook_post.get('post_id', ''),
            'crawl_id': metadata.get('crawl_id', ''),
            'media_type': attachment.get('type', ''),
            'media_url': attachment.get('url', ''),
            'attachment_url': attachment.get('attachment_url', ''),
            'download_status': 'pending',
            'processing_status': 'pending',
            'created_date': datetime.utcnow().isoformat()
        }
        media_files.append(media_file)
    
    return media_files
```

## 🔄 Complete Processing Pipeline

```python
def process_facebook_posts_batch(raw_posts: List[Dict], metadata: Dict) -> tuple:
    """Process batch of Facebook posts for BigQuery and media processing."""
    
    processed_posts = []
    media_files = []
    
    for post in raw_posts:
        try:
            # Process for BigQuery
            processed_post = process_facebook_post_for_bigquery(post, metadata)
            processed_posts.append(processed_post)
            
            # Extract media files
            post_media_files = extract_media_files_for_processing(post, metadata)
            media_files.extend(post_media_files)
            
        except Exception as e:
            logger.error(f"Error processing post {post.get('post_id', 'unknown')}: {str(e)}")
            continue
    
    return processed_posts, media_files
```

This comprehensive schema and transformation logic handles the rich Facebook post data from BrightData, providing structured analytics data for BigQuery while separating media processing concerns.