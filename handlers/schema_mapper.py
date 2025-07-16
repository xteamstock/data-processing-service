"""
Schema-driven data transformation for social media posts.

This module provides schema-driven transformation of social media data
from various platforms to BigQuery format, enabling flexible mapping
without code changes.
"""

import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from textblob import TextBlob

logger = logging.getLogger(__name__)


class SchemaMapper:
    """
    Schema-driven data mapper for social media posts.
    
    Loads external schema configuration and applies field mappings,
    validations, and transformations to convert platform-specific
    data to BigQuery format.
    """
    
    def __init__(self, schema_dir: str = "schemas"):
        """
        Initialize schema mapper.
        
        Args:
            schema_dir: Directory containing schema configuration files
        """
        self.schema_dir = Path(schema_dir)
        self.schemas = {}
        self.preprocessing_functions = {
            'clean_text': self._clean_text,
            'normalize_hashtags': self._normalize_hashtags,
            'parse_iso_timestamp': self._parse_iso_timestamp,
            'extract_date_only': self._extract_date_only,
            'safe_int': self._safe_int,
            'safe_float': self._safe_float,
            'parse_reaction_types': self._parse_reaction_types,
            'parse_attachments': self._parse_attachments,
            'extract_address_from_about': self._extract_address_from_about,
            'clean_username': self._clean_username,
            'remove_extra_whitespace': self._remove_extra_whitespace,
            'extract_hashtag_names': self._extract_hashtag_names,
            'extract_mention_names': self._extract_mention_names,
            'normalize_keywords': self._normalize_keywords,
            'parse_description_links': self._parse_description_links,
            'json_to_string': self._json_to_string
        }
        
        self.computation_functions = {
            'sum_reactions_by_type': self._sum_reactions_by_type,
            'count_attachments': self._count_attachments,
            'check_video_attachments': self._check_video_attachments,
            'check_image_attachments': self._check_image_attachments,
            'calculate_text_length': self._calculate_text_length,
            'detect_language': self._detect_language,
            'calculate_sentiment': self._calculate_sentiment,
            'calculate_data_quality': self._calculate_data_quality,
            'sum_tiktok_engagement': self._sum_tiktok_engagement,
            'calculate_tiktok_engagement_rate': self._calculate_tiktok_engagement_rate,
            'check_has_music': self._check_has_music,
            'calculate_aspect_ratio': self._calculate_aspect_ratio,
            'count_hashtags': self._count_hashtags,
            'calculate_tiktok_data_quality': self._calculate_tiktok_data_quality,
            'sum_youtube_engagement': self._sum_youtube_engagement,
            'calculate_youtube_engagement_rate': self._calculate_youtube_engagement_rate,
            'parse_youtube_duration': self._parse_youtube_duration,
            'check_is_youtube_short': self._check_is_youtube_short,
            'calculate_title_length': self._calculate_title_length,
            'calculate_youtube_data_quality': self._calculate_youtube_data_quality
        }
        
        # Load schemas
        self._load_schemas()
    
    def _load_schemas(self):
        """Load all schema configuration files."""
        try:
            schema_files = list(self.schema_dir.glob("*_schema_v*.json"))
            
            for schema_file in schema_files:
                try:
                    with open(schema_file, 'r', encoding='utf-8') as f:
                        schema_config = json.load(f)
                    
                    platform = schema_config.get('platform')
                    version = schema_config.get('schema_version')
                    
                    if platform and version:
                        key = f"{platform}_v{version}"
                        self.schemas[key] = schema_config
                        logger.info(f"Loaded schema: {key}")
                    else:
                        logger.warning(f"Invalid schema config in {schema_file}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing schema file {schema_file}: {e}")
                except Exception as e:
                    logger.error(f"Error loading schema file {schema_file}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading schemas from {self.schema_dir}: {e}")
    
    def get_schema(self, platform: str, version: str = "1.0.0") -> Optional[Dict]:
        """
        Get schema configuration for platform and version.
        
        Args:
            platform: Platform name (facebook, youtube, instagram)
            version: Schema version
            
        Returns:
            Schema configuration dict or None
        """
        key = f"{platform}_v{version}"
        return self.schemas.get(key)
    
    def transform_post(self, raw_post: Dict, platform: str, metadata: Dict, 
                      schema_version: str = "1.0.0") -> Dict:
        """
        Transform raw post data to BigQuery format using schema mapping.
        
        Args:
            raw_post: Raw post data from platform
            platform: Platform name
            metadata: Crawl metadata
            schema_version: Schema version to use
            
        Returns:
            Transformed post data ready for BigQuery
        """
        schema = self.get_schema(platform, schema_version)
        if not schema:
            raise ValueError(f"Schema not found for {platform} v{schema_version}")
        
        # Start with base structure
        # Get platform-specific post ID
        if platform == 'facebook':
            post_id = raw_post.get('post_id', '')
        elif platform == 'tiktok':
            post_id = raw_post.get('id', '')
        elif platform == 'youtube':
            post_id = raw_post.get('id', '')
        else:
            post_id = raw_post.get('id', raw_post.get('post_id', ''))
        
        transformed_post = {
            # Core identifiers added by service
            'id': f"{post_id}_{metadata.get('crawl_id', '')}",
            'crawl_id': metadata.get('crawl_id', ''),
            'snapshot_id': metadata.get('snapshot_id', ''),
            'platform': platform,
            'competitor': metadata.get('competitor', ''),
            'brand': metadata.get('brand', ''),
            'category': metadata.get('category', ''),
            'crawl_date': metadata.get('crawl_date', ''),
            'processed_date': datetime.utcnow().isoformat()
        }
        
        # Set platform-specific date_posted field
        # Note: All platforms need date_posted for GCS grouping
        if platform == 'facebook':
            # Facebook uses date_posted directly
            date_value = raw_post.get('date_posted')
            if date_value:
                transformed_post['date_posted'] = self._parse_iso_timestamp(date_value)
        elif platform == 'tiktok':
            # TikTok uses createTimeISO
            date_value = raw_post.get('createTimeISO')
            if date_value:
                transformed_post['date_posted'] = self._parse_iso_timestamp(date_value)
        elif platform == 'youtube':
            # YouTube uses date field (maps to published_at in schema, but we need date_posted for grouping)
            date_value = raw_post.get('date')
            if date_value:
                transformed_post['date_posted'] = self._parse_iso_timestamp(date_value)
        
        # Apply field mappings
        field_mappings = schema.get('field_mappings', {})
        
        for category, fields in field_mappings.items():
            for field_name, field_config in fields.items():
                try:
                    value = self._extract_and_transform_field(
                        raw_post, field_config, transformed_post
                    )
                    
                    if value is not None:
                        # Set nested field value
                        self._set_nested_field(transformed_post, field_config['target_field'], value)
                        
                except Exception as e:
                    logger.error(f"Error processing field {field_name}: {e}")
                    
                    # Set default value if specified
                    if 'default_value' in field_config:
                        self._set_nested_field(
                            transformed_post, 
                            field_config['target_field'], 
                            field_config['default_value']
                        )
        
        # Apply computed fields
        computed_fields = schema.get('computed_fields', {})
        for field_name, field_config in computed_fields.items():
            try:
                value = self._compute_field(raw_post, field_config, transformed_post)
                if value is not None:
                    self._set_nested_field(transformed_post, field_config['target_field'], value)
            except Exception as e:
                logger.error(f"Error computing field {field_name}: {e}")
        
        # Add processing metadata
        if 'processing_metadata' not in transformed_post:
            transformed_post['processing_metadata'] = {}
        
        transformed_post['processing_metadata'].update({
            'schema_version': schema_version,
            'processing_version': '1.0.0'
        })
        
        # Validate transformed post
        self._validate_post(transformed_post, schema)
        
        return transformed_post
    
    def _extract_and_transform_field(self, raw_post: Dict, field_config: Dict, 
                                   transformed_post: Dict) -> Any:
        """Extract and transform a single field."""
        source_field = field_config.get('source_field')
        if not source_field:
            return None
        
        # Extract value
        value = self._get_nested_field(raw_post, source_field)
        
        # Apply preprocessing
        preprocessing = field_config.get('preprocessing', [])
        for func_name in preprocessing:
            if func_name in self.preprocessing_functions:
                value = self.preprocessing_functions[func_name](value)
        
        # Apply validation
        validation = field_config.get('validation')
        if validation and not self._validate_field_value(value, validation):
            logger.warning(f"Validation failed for field {source_field}: {validation}")
            return field_config.get('default_value')
        
        # Apply max length
        max_length = field_config.get('max_length')
        if max_length and isinstance(value, str) and len(value) > max_length:
            value = value[:max_length] + "..."
        
        return value
    
    def _compute_field(self, raw_post: Dict, field_config: Dict, 
                      transformed_post: Dict) -> Any:
        """Compute a field value using computation function."""
        computation = field_config.get('computation')
        if not computation or computation not in self.computation_functions:
            return None
        
        return self.computation_functions[computation](raw_post, transformed_post)
    
    def _get_nested_field(self, data: Dict, field_path: str) -> Any:
        """Get nested field value using dot notation."""
        keys = field_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _set_nested_field(self, data: Dict, field_path: str, value: Any):
        """Set nested field value using dot notation."""
        keys = field_path.split('.')
        current = data
        
        # Navigate to parent
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        # Set final value
        current[keys[-1]] = value
    
    def _validate_field_value(self, value: Any, validation: str) -> bool:
        """Validate field value against validation rule."""
        if value is None:
            return False
        
        if validation == 'non_empty_string':
            return isinstance(value, str) and len(value.strip()) > 0
        elif validation == 'url_format':
            return isinstance(value, str) and (value.startswith('http://') or value.startswith('https://'))
        elif validation == 'email_format':
            return isinstance(value, str) and '@' in value and '.' in value
        
        return True
    
    def _validate_post(self, transformed_post: Dict, schema: Dict):
        """Validate transformed post against schema requirements."""
        validation_rules = schema.get('validation_rules', {})
        
        # Check required fields
        required_fields = validation_rules.get('required_fields', [])
        for field in required_fields:
            if not self._get_nested_field(transformed_post, field):
                logger.warning(f"Required field missing: {field}")
        
        # Check data quality
        quality_thresholds = validation_rules.get('data_quality_thresholds', {})
        min_score = quality_thresholds.get('minimum_score', 0.0)
        current_score = self._get_nested_field(transformed_post, 'processing_metadata.data_quality_score') or 0.0
        
        if current_score < min_score:
            logger.warning(f"Data quality score {current_score} below threshold {min_score}")
    
    # Preprocessing functions
    def _clean_text(self, text: str) -> str:
        """Clean text content."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove problematic characters for BigQuery
        text = re.sub(r'[^\w\s\.\!\?\,\;\:\-\(\)\[\]\{\}\"\'@#]', '', text)
        
        return text
    
    def _normalize_hashtags(self, hashtags: List[str]) -> List[str]:
        """Normalize hashtags."""
        if not hashtags:
            return []
        
        normalized = []
        for tag in hashtags:
            if isinstance(tag, str):
                # Remove # prefix and convert to lowercase
                clean_tag = tag.lstrip('#').lower().strip()
                if clean_tag:
                    normalized.append(clean_tag)
        
        return normalized
    
    def _parse_iso_timestamp(self, timestamp: str) -> str:
        """Parse ISO timestamp for BigQuery."""
        if not timestamp:
            return datetime.utcnow().isoformat()
        
        try:
            if isinstance(timestamp, str):
                # Handle ISO format
                if 'T' in timestamp:
                    return timestamp.replace('Z', '+00:00')
                else:
                    # Try parsing as timestamp
                    parsed = datetime.fromtimestamp(float(timestamp))
                    return parsed.isoformat()
            return str(timestamp)
        except Exception:
            return datetime.utcnow().isoformat()
    
    def _extract_date_only(self, timestamp: str) -> str:
        """Extract date only (YYYY-MM-DD)."""
        if not timestamp:
            return datetime.utcnow().strftime('%Y-%m-%d')
        
        try:
            if 'T' in timestamp:
                return timestamp.split('T')[0]
            else:
                return str(timestamp)[:10]
        except Exception:
            return datetime.utcnow().strftime('%Y-%m-%d')
    
    def _safe_int(self, value: Any) -> int:
        """Safely convert to integer."""
        try:
            return int(value) if value is not None else 0
        except (ValueError, TypeError):
            return 0
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert to float."""
        try:
            return float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_reaction_types(self, reactions: List[Dict]) -> List[Dict]:
        """Parse reaction types array."""
        if not reactions:
            return []
        
        parsed_reactions = []
        for reaction in reactions:
            if isinstance(reaction, dict):
                parsed_reactions.append({
                    'type': reaction.get('type', ''),
                    'count': self._safe_int(reaction.get('reaction_count', 0))
                })
        
        return parsed_reactions
    
    def _parse_attachments(self, attachments: List[Dict]) -> List[Dict]:
        """Parse attachments array."""
        if not attachments:
            return []
        
        parsed_attachments = []
        for attachment in attachments:
            if isinstance(attachment, dict):
                parsed_attachments.append({
                    'id': attachment.get('id', ''),
                    'type': attachment.get('type', ''),
                    'url': attachment.get('url', ''),
                    'attachment_url': attachment.get('attachment_url', '')
                })
        
        return parsed_attachments
    
    def _extract_address_from_about(self, about_sections: List[Dict]) -> str:
        """Extract address from about sections."""
        if not about_sections:
            return ""
        
        for section in about_sections:
            if isinstance(section, dict) and section.get('type') == 'ADDRESS':
                return section.get('value', '')
        
        return ""
    
    def _clean_username(self, username: str) -> str:
        """Clean username/display name."""
        if not username:
            return ""
        
        # Remove extra whitespace
        return re.sub(r'\s+', ' ', username.strip())
    
    # Computation functions
    def _sum_reactions_by_type(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Sum total reactions from reactions by type."""
        # For flattened schema, get reactions from likes/reactions fields
        likes = self._get_nested_field(transformed_post, 'likes') or 0
        return likes
    
    def _count_attachments(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Count total attachments."""
        # For flattened schema, try to parse from attachments JSON string
        attachments_str = self._get_nested_field(transformed_post, 'attachments')
        if not attachments_str:
            return 0
        
        try:
            import json
            attachments = json.loads(attachments_str) if isinstance(attachments_str, str) else attachments_str
            return len(attachments) if isinstance(attachments, list) else 0
        except:
            return 0
    
    def _check_video_attachments(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if post has video attachments."""
        # For flattened schema, try to parse from attachments JSON string
        attachments_str = self._get_nested_field(transformed_post, 'attachments')
        if not attachments_str:
            return False
        
        try:
            import json
            attachments = json.loads(attachments_str) if isinstance(attachments_str, str) else attachments_str
            if isinstance(attachments, list):
                return any(att.get('type', '').lower() == 'video' for att in attachments)
            return False
        except:
            return False
    
    def _check_image_attachments(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if post has image attachments."""
        # For flattened schema, try to parse from attachments JSON string
        attachments_str = self._get_nested_field(transformed_post, 'attachments')
        if not attachments_str:
            return False
        
        try:
            import json
            attachments = json.loads(attachments_str) if isinstance(attachments_str, str) else attachments_str
            if isinstance(attachments, list):
                return any(att.get('type', '').lower() in ['photo', 'image'] for att in attachments)
            return False
        except:
            return False
    
    def _calculate_text_length(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Calculate text length."""
        # Try different text content fields based on platform
        content = (
            self._get_nested_field(transformed_post, 'post_content') or 
            self._get_nested_field(transformed_post, 'description') or
            self._get_nested_field(transformed_post, 'text') or
            ""
        )
        return len(content) if content else 0
    
    def _detect_language(self, raw_post: Dict, transformed_post: Dict) -> str:
        """Detect language of content."""
        # Try different text content fields based on platform
        content = (
            self._get_nested_field(transformed_post, 'post_content') or 
            self._get_nested_field(transformed_post, 'description') or
            self._get_nested_field(transformed_post, 'text') or
            ""
        )
        if not content:
            return 'unknown'
        
        try:
            blob = TextBlob(content)
            return blob.detect_language()
        except Exception:
            return 'unknown'
    
    def _calculate_sentiment(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate sentiment score."""
        # Try different text content fields based on platform
        content = (
            self._get_nested_field(transformed_post, 'post_content') or 
            self._get_nested_field(transformed_post, 'description') or
            self._get_nested_field(transformed_post, 'text') or
            ""
        )
        if not content:
            return 0.0
        
        try:
            blob = TextBlob(content)
            return blob.sentiment.polarity
        except Exception:
            return 0.0
    
    def _calculate_data_quality(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate data quality score."""
        score = 0.0
        max_score = 10.0
        
        # Content (40%)
        if self._get_nested_field(transformed_post, 'post_content'):
            score += 4.0
        
        # Engagement (20%)
        likes = self._get_nested_field(transformed_post, 'likes') or 0
        comments = self._get_nested_field(transformed_post, 'comments') or 0
        if likes > 0 or comments > 0:
            score += 2.0
        
        # Media (20%)
        media_count = self._get_nested_field(transformed_post, 'media_count') or 0
        if media_count > 0:
            score += 2.0
        
        # Page info (10%)
        if self._get_nested_field(transformed_post, 'page_name'):
            score += 1.0
        
        # Date (10%)
        if self._get_nested_field(transformed_post, 'date_posted'):
            score += 1.0
        
        return score / max_score

    # Additional preprocessing functions for multi-platform support
    def _remove_extra_whitespace(self, text: str) -> str:
        """Remove extra whitespace from text."""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())

    def _extract_hashtag_names(self, hashtags: List[Dict]) -> List[str]:
        """Extract hashtag names from hashtag objects."""
        if not hashtags:
            return []
        
        extracted = []
        for hashtag in hashtags:
            if isinstance(hashtag, dict):
                name = hashtag.get('name', hashtag.get('text', ''))
                if name:
                    extracted.append(name.lstrip('#').lower())
            elif isinstance(hashtag, str):
                extracted.append(hashtag.lstrip('#').lower())
        
        return extracted

    def _extract_mention_names(self, mentions: List[Dict]) -> List[str]:
        """Extract mention usernames from mention objects."""
        if not mentions:
            return []
        
        extracted = []
        for mention in mentions:
            if isinstance(mention, dict):
                username = mention.get('username', mention.get('name', ''))
                if username:
                    extracted.append(username.lstrip('@'))
            elif isinstance(mention, str):
                extracted.append(mention.lstrip('@'))
        
        return extracted

    def _normalize_keywords(self, keywords: List[str]) -> List[str]:
        """Normalize YouTube keywords."""
        if not keywords:
            return []
        
        normalized = []
        for keyword in keywords:
            if isinstance(keyword, str):
                clean_keyword = keyword.strip().lower()
                if clean_keyword:
                    normalized.append(clean_keyword)
        
        return normalized

    def _parse_description_links(self, links: List[Dict]) -> List[Dict]:
        """Parse description links from array format."""
        if not links:
            return []
        
        parsed = []
        for link in links:
            if isinstance(link, dict):
                url = link.get('url', '')
                text = link.get('text', '')
                if url:
                    parsed.append({
                        'url': url,
                        'text': text or url
                    })
        
        return parsed

    def _json_to_string(self, value: Any) -> str:
        """Convert JSON object/array to string for BigQuery compatibility."""
        if value is None:
            return ""
        
        try:
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            else:
                return str(value)
        except Exception:
            return str(value)

    # TikTok-specific computation functions
    def _sum_tiktok_engagement(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Sum TikTok engagement metrics."""
        likes = self._get_nested_field(transformed_post, 'digg_count') or 0
        comments = self._get_nested_field(transformed_post, 'comment_count') or 0
        shares = self._get_nested_field(transformed_post, 'share_count') or 0
        return likes + comments + shares

    def _calculate_tiktok_engagement_rate(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate TikTok engagement rate."""
        total_engagement = self._sum_tiktok_engagement(raw_post, transformed_post)
        views = self._get_nested_field(transformed_post, 'play_count') or 0
        
        if views > 0:
            return total_engagement / views
        return 0.0

    def _check_has_music(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if TikTok video has background music."""
        music_id = self._get_nested_field(transformed_post, 'music_id')
        music_title = self._get_nested_field(transformed_post, 'music_title')
        return bool(music_id or music_title)

    def _calculate_aspect_ratio(self, raw_post: Dict, transformed_post: Dict) -> str:
        """Calculate video aspect ratio."""
        width = self._get_nested_field(transformed_post, 'video_width') or 0
        height = self._get_nested_field(transformed_post, 'video_height') or 0
        
        if width > 0 and height > 0:
            # Common aspect ratios
            ratio = width / height
            if 0.55 <= ratio <= 0.58:  # ~9:16
                return "9:16"
            elif 1.75 <= ratio <= 1.80:  # ~16:9
                return "16:9"
            elif 0.98 <= ratio <= 1.02:  # ~1:1
                return "1:1"
            else:
                return f"{width}:{height}"
        
        return "unknown"

    def _count_hashtags(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Count number of hashtags."""
        hashtags = self._get_nested_field(transformed_post, 'hashtags')
        return len(hashtags) if hashtags else 0

    def _calculate_tiktok_data_quality(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate TikTok-specific data quality score."""
        score = 0.0
        max_score = 10.0
        
        # Description (30%)
        if self._get_nested_field(transformed_post, 'description'):
            score += 3.0
        
        # Engagement (30%)
        likes = self._get_nested_field(transformed_post, 'digg_count') or 0
        comments = self._get_nested_field(transformed_post, 'comment_count') or 0
        if likes > 0 or comments > 0:
            score += 3.0
        
        # Video metadata (20%)
        video_url = self._get_nested_field(transformed_post, 'video_url')
        duration = self._get_nested_field(transformed_post, 'duration_seconds')
        if video_url and duration:
            score += 2.0
        
        # Author info (10%)
        if self._get_nested_field(transformed_post, 'author_name'):
            score += 1.0
        
        # Date (10%)
        if self._get_nested_field(transformed_post, 'date_posted'):
            score += 1.0
        
        return score / max_score

    # YouTube-specific computation functions
    def _sum_youtube_engagement(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Sum YouTube engagement metrics."""
        likes = self._get_nested_field(transformed_post, 'like_count') or 0
        comments = self._get_nested_field(transformed_post, 'comment_count') or 0
        return likes + comments

    def _calculate_youtube_engagement_rate(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate YouTube engagement rate."""
        total_engagement = self._sum_youtube_engagement(raw_post, transformed_post)
        views = self._get_nested_field(transformed_post, 'view_count') or 0
        
        if views > 0:
            return total_engagement / views
        return 0.0

    def _parse_youtube_duration(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Parse YouTube duration string to seconds."""
        duration_str = self._get_nested_field(transformed_post, 'duration')
        if not duration_str:
            return 0
        
        try:
            # Handle formats like "PT4M13S", "4:13", "1:23:45"
            if duration_str.startswith('PT'):
                # ISO 8601 duration format
                duration_str = duration_str[2:]  # Remove PT
                hours = 0
                minutes = 0
                seconds = 0
                
                if 'H' in duration_str:
                    hours_str, duration_str = duration_str.split('H')
                    hours = int(hours_str)
                
                if 'M' in duration_str:
                    minutes_str, duration_str = duration_str.split('M')
                    minutes = int(minutes_str)
                
                if 'S' in duration_str:
                    seconds_str = duration_str.replace('S', '')
                    seconds = int(seconds_str)
                
                return hours * 3600 + minutes * 60 + seconds
            
            elif ':' in duration_str:
                # Time format like "4:13" or "1:23:45"
                parts = duration_str.split(':')
                if len(parts) == 2:  # MM:SS
                    return int(parts[0]) * 60 + int(parts[1])
                elif len(parts) == 3:  # HH:MM:SS
                    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            
            return 0
        except (ValueError, IndexError):
            return 0

    def _check_is_youtube_short(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if video is a YouTube Short."""
        duration_seconds = self._parse_youtube_duration(raw_post, transformed_post)
        return duration_seconds > 0 and duration_seconds <= 60

    def _calculate_title_length(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Calculate YouTube title length."""
        title = self._get_nested_field(transformed_post, 'title')
        return len(title) if title else 0


    def _calculate_youtube_data_quality(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate YouTube-specific data quality score."""
        score = 0.0
        max_score = 10.0
        
        # Title (20%)
        if self._get_nested_field(transformed_post, 'title'):
            score += 2.0
        
        # Description (20%)
        if self._get_nested_field(transformed_post, 'description'):
            score += 2.0
        
        # Engagement (25%)
        likes = self._get_nested_field(transformed_post, 'like_count') or 0
        views = self._get_nested_field(transformed_post, 'view_count') or 0
        if likes > 0 and views > 0:
            score += 2.5
        
        # Video metadata (15%)
        thumbnail = self._get_nested_field(transformed_post, 'thumbnail_url')
        duration = self._get_nested_field(transformed_post, 'duration')
        if thumbnail and duration:
            score += 1.5
        
        # Channel info (10%)
        if self._get_nested_field(transformed_post, 'channel_name'):
            score += 1.0
        
        # Date (10%)
        if self._get_nested_field(transformed_post, 'published_at'):
            score += 1.0
        
        return score / max_score


# Convenience function for backward compatibility
def process_facebook_post_for_bigquery(facebook_post: Dict, metadata: Dict) -> Dict:
    """
    Process Facebook post using schema-driven transformation.
    
    Args:
        facebook_post: Raw Facebook post from BrightData
        metadata: Crawl metadata
        
    Returns:
        Transformed post ready for BigQuery
    """
    mapper = SchemaMapper()
    return mapper.transform_post(facebook_post, 'facebook', metadata)