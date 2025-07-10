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
            'clean_username': self._clean_username
        }
        
        self.computation_functions = {
            'sum_reactions_by_type': self._sum_reactions_by_type,
            'count_attachments': self._count_attachments,
            'check_video_attachments': self._check_video_attachments,
            'check_image_attachments': self._check_image_attachments,
            'calculate_text_length': self._calculate_text_length,
            'detect_language': self._detect_language,
            'calculate_sentiment': self._calculate_sentiment,
            'calculate_data_quality': self._calculate_data_quality
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
        transformed_post = {
            # Core identifiers added by service
            'id': f"{raw_post.get('post_id', '')}_{metadata.get('crawl_id', '')}",
            'crawl_id': metadata.get('crawl_id', ''),
            'snapshot_id': metadata.get('snapshot_id', ''),
            'platform': platform,
            'competitor': metadata.get('competitor', ''),
            'brand': metadata.get('brand', ''),
            'category': metadata.get('category', ''),
            'crawl_date': metadata.get('crawl_date', ''),
            'processed_date': datetime.utcnow().isoformat()
        }
        
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
        reactions = self._get_nested_field(transformed_post, 'engagement_metrics.reactions_by_type')
        if not reactions:
            return 0
        
        total = 0
        for reaction in reactions:
            total += reaction.get('count', 0)
        
        return total
    
    def _count_attachments(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Count total attachments."""
        attachments = self._get_nested_field(transformed_post, 'media_metadata.attachments')
        return len(attachments) if attachments else 0
    
    def _check_video_attachments(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if post has video attachments."""
        attachments = self._get_nested_field(transformed_post, 'media_metadata.attachments')
        if not attachments:
            return False
        
        return any(att.get('type', '').lower() == 'video' for att in attachments)
    
    def _check_image_attachments(self, raw_post: Dict, transformed_post: Dict) -> bool:
        """Check if post has image attachments."""
        attachments = self._get_nested_field(transformed_post, 'media_metadata.attachments')
        if not attachments:
            return False
        
        return any(att.get('type', '').lower() in ['photo', 'image'] for att in attachments)
    
    def _calculate_text_length(self, raw_post: Dict, transformed_post: Dict) -> int:
        """Calculate text length."""
        content = self._get_nested_field(transformed_post, 'post_content')
        return len(content) if content else 0
    
    def _detect_language(self, raw_post: Dict, transformed_post: Dict) -> str:
        """Detect language of content."""
        content = self._get_nested_field(transformed_post, 'post_content')
        if not content:
            return 'unknown'
        
        try:
            blob = TextBlob(content)
            return blob.detect_language()
        except Exception:
            return 'unknown'
    
    def _calculate_sentiment(self, raw_post: Dict, transformed_post: Dict) -> float:
        """Calculate sentiment score."""
        content = self._get_nested_field(transformed_post, 'post_content')
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
        engagement = self._get_nested_field(transformed_post, 'engagement_metrics')
        if engagement and (engagement.get('likes', 0) > 0 or engagement.get('comments', 0) > 0):
            score += 2.0
        
        # Media (20%)
        media = self._get_nested_field(transformed_post, 'media_metadata')
        if media and media.get('media_count', 0) > 0:
            score += 2.0
        
        # Page info (10%)
        if self._get_nested_field(transformed_post, 'page_name'):
            score += 1.0
        
        # Date (10%)
        if self._get_nested_field(transformed_post, 'date_posted'):
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