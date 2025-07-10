# Schema Configuration Guide - Data Processing Service

## 📋 Overview

This guide explains how to configure and manage **external schema files** for the Data Processing Service, enabling future-proof transformation of BrightData JSON to BigQuery format without code changes.

## 🎯 Why External Schema Configuration?

### Business Requirements
- **BrightData API Changes**: Third-party APIs evolve over time
- **Platform Variations**: Different social media platforms have different data structures
- **Regional Differences**: Data formats may vary by market
- **A/B Testing**: Test new field mappings safely

### Technical Benefits
- **Hot Updates**: Change mappings without service restart
- **Version Control**: Support multiple schema versions simultaneously
- **Rollback Capability**: Instantly revert to previous schema
- **Validation**: Centralized data quality rules

## 🗂️ Schema File Structure

### Directory Layout
```
services/data-processing/
├── schemas/
│   ├── facebook_post_schema_v1.json     # Facebook field mappings
│   ├── youtube_video_schema_v1.json     # YouTube field mappings (future)
│   ├── instagram_post_schema_v1.json    # Instagram field mappings (future)
│   └── bigquery_table_schema.json       # BigQuery table structure
├── handlers/
│   └── schema_mapper.py                 # Schema processing engine
└── docs/
    └── SCHEMA_CONFIGURATION_GUIDE.md    # This guide
```

### Schema File Format
```json
{
  "schema_version": "1.0.0",
  "platform": "facebook",
  "created_date": "2025-07-09",
  "description": "Facebook post schema mapping",
  "field_mappings": { /* ... */ },
  "computed_fields": { /* ... */ },
  "validation_rules": { /* ... */ }
}
```

## 📝 Field Mapping Configuration

### Basic Field Mapping
```json
{
  "field_mappings": {
    "core_identifiers": {
      "post_id": {
        "source_field": "post_id",
        "target_field": "post_id",
        "target_type": "STRING",
        "required": true,
        "validation": "non_empty_string",
        "description": "Primary post identifier"
      }
    }
  }
}
```

### Complex Field Mapping
```json
{
  "content_fields": {
    "content": {
      "source_field": "content",
      "target_field": "post_content",
      "target_type": "STRING",
      "required": false,
      "max_length": 50000,
      "preprocessing": ["clean_text", "remove_extra_whitespace"],
      "description": "Main post content text"
    }
  }
}
```

### Nested Field Mapping
```json
{
  "engagement_fields": {
    "reactions_by_type": {
      "source_field": "count_reactions_type",
      "target_field": "engagement_metrics.reactions_by_type",
      "target_type": "ARRAY<STRUCT<type STRING, count INT64>>",
      "required": false,
      "preprocessing": ["parse_reaction_types"],
      "description": "Detailed reaction breakdown"
    }
  }
}
```

## 🔧 Preprocessing Functions

### Available Functions
```json
{
  "preprocessing_functions": {
    "clean_text": "Remove extra whitespace and limit length",
    "normalize_hashtags": "Convert to lowercase and remove # prefix",
    "parse_iso_timestamp": "Convert ISO timestamp to BigQuery format",
    "extract_date_only": "Extract YYYY-MM-DD from timestamp",
    "safe_int": "Convert to integer with 0 default",
    "safe_float": "Convert to float with 0.0 default",
    "parse_reaction_types": "Convert reactions array to structured format",
    "parse_attachments": "Convert attachments to structured format"
  }
}
```

### Custom Preprocessing
To add new preprocessing functions:

1. **Add to schema JSON**:
```json
{
  "preprocessing_functions": {
    "my_custom_function": "Description of what it does"
  }
}
```

2. **Implement in schema_mapper.py**:
```python
def _my_custom_function(self, value: Any) -> Any:
    """Custom preprocessing function."""
    # Your transformation logic here
    return processed_value

# Register in __init__
self.preprocessing_functions['my_custom_function'] = self._my_custom_function
```

## 🧮 Computed Fields

### Basic Computed Field
```json
{
  "computed_fields": {
    "total_reactions": {
      "target_field": "engagement_metrics.reactions",
      "target_type": "INT64",
      "computation": "sum_reactions_by_type",
      "description": "Total reactions across all types"
    }
  }
}
```

### Advanced Computed Field
```json
{
  "computed_fields": {
    "data_quality_score": {
      "target_field": "processing_metadata.data_quality_score",
      "target_type": "FLOAT64",
      "computation": "calculate_data_quality",
      "description": "Data completeness score (0 to 1)"
    }
  }
}
```

## ✅ Validation Rules

### Field Validation
```json
{
  "validation_rules": {
    "required_fields": [
      "post_id",
      "post_url",
      "date_posted"
    ],
    "format_validations": {
      "url_fields": ["post_url", "user_url"],
      "timestamp_fields": ["date_posted"],
      "integer_fields": ["likes", "comments"],
      "boolean_fields": ["page_verified"]
    }
  }
}
```

### Data Quality Thresholds
```json
{
  "validation_rules": {
    "data_quality_thresholds": {
      "minimum_score": 0.3,
      "content_required": false,
      "engagement_required": false
    }
  }
}
```

## 🔄 Schema Versioning

### Version Management
```json
{
  "schema_version": "1.1.0",
  "previous_version": "1.0.0",
  "changes": [
    "Added new engagement metric: saves",
    "Updated timestamp parsing for timezone handling",
    "Added validation for phone number format"
  ]
}
```

### Using Different Versions
```python
# Use specific version
transformed_post = mapper.transform_post(
    raw_post=facebook_post,
    platform='facebook',
    metadata=metadata,
    schema_version='1.1.0'
)

# Use latest version (default)
transformed_post = mapper.transform_post(
    raw_post=facebook_post,
    platform='facebook',
    metadata=metadata
)
```

## 🚀 Common Use Cases

### 1. Adding New Field from BrightData

**Scenario**: BrightData adds `post_location` field

**Solution**: Update schema without code deployment
```json
{
  "field_mappings": {
    "location_fields": {
      "post_location": {
        "source_field": "post_location",
        "target_field": "post_metadata.location",
        "target_type": "STRING",
        "required": false,
        "description": "Geographic location of post"
      }
    }
  }
}
```

### 2. Changing Field Names

**Scenario**: BrightData renames `user_username_raw` to `user_display_name`

**Solution**: Update source field mapping
```json
{
  "field_mappings": {
    "user_page_fields": {
      "user_username": {
        "source_field": "user_display_name",  // Changed from "user_username_raw"
        "target_field": "user_username",
        "target_type": "STRING",
        "required": false
      }
    }
  }
}
```

### 3. Adding Platform-Specific Fields

**Scenario**: YouTube needs view count field

**Solution**: Create YouTube-specific schema
```json
{
  "platform": "youtube",
  "field_mappings": {
    "engagement_fields": {
      "views": {
        "source_field": "view_count",
        "target_field": "engagement_metrics.views",
        "target_type": "INT64",
        "required": false,
        "preprocessing": ["safe_int"],
        "default_value": 0
      }
    }
  }
}
```

### 4. Handling Nested Data Changes

**Scenario**: BrightData changes reaction structure

**Solution**: Update preprocessing function
```json
{
  "field_mappings": {
    "engagement_fields": {
      "reactions": {
        "source_field": "reactions_new_format",
        "target_field": "engagement_metrics.reactions_by_type",
        "target_type": "ARRAY<STRUCT<type STRING, count INT64>>",
        "preprocessing": ["parse_reactions_v2"],  // New function
        "description": "Updated reaction parsing"
      }
    }
  }
}
```

## 🛠️ Development Workflow

### 1. Schema Development
```bash
# Create new schema version
cp schemas/facebook_post_schema_v1.json schemas/facebook_post_schema_v1.1.json

# Edit new version
vim schemas/facebook_post_schema_v1.1.json

# Test with sample data
python -c "
from handlers.schema_mapper import SchemaMapper
mapper = SchemaMapper()
result = mapper.transform_post(sample_post, 'facebook', metadata, '1.1.0')
print(result)
"
```

### 2. Schema Validation
```bash
# Validate schema syntax
python -c "
import json
with open('schemas/facebook_post_schema_v1.1.json') as f:
    schema = json.load(f)
    print('Schema valid!')
"

# Test transformation
python tests/test_schema_mapping.py
```

### 3. Schema Deployment
```bash
# Deploy new schema files
gsutil cp schemas/*.json gs://data-processing-schemas/

# Update service configuration (if needed)
gcloud run services update data-processing-service \
  --set-env-vars="SCHEMA_VERSION=1.1.0"
```

## 🚨 Troubleshooting

### Common Issues

1. **Schema Loading Fails**
   - Check JSON syntax
   - Verify file permissions
   - Ensure schema_version field exists

2. **Field Mapping Errors**
   - Verify source field exists in raw data
   - Check target field path syntax
   - Validate preprocessing function names

3. **Validation Failures**
   - Check required fields are present
   - Verify data type conversions
   - Review validation rules

### Debug Mode
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Test specific field mapping
mapper = SchemaMapper()
mapper._extract_and_transform_field(raw_post, field_config, {})
```

## 📊 Performance Considerations

### Schema Optimization
- **Minimize preprocessing**: Only use necessary functions
- **Optimize computed fields**: Cache expensive calculations
- **Efficient validation**: Use simple validation rules

### Memory Usage
- **Schema caching**: Schemas are loaded once and cached
- **Batch processing**: Process multiple posts with same schema
- **Cleanup**: Remove unused schema versions

## 🔐 Security Best Practices

### Schema Security
- **Validate input**: Always validate schema files
- **Sanitize data**: Clean text content for BigQuery
- **Access control**: Restrict schema file access
- **Audit logging**: Log schema changes

### Data Protection
- **PII handling**: Detect and handle personal information
- **Content filtering**: Remove sensitive content
- **Encryption**: Encrypt schema files in storage

## 📈 Monitoring Schema Changes

### Metrics to Track
- **Schema version usage**: Which versions are active
- **Processing success rate**: By schema version
- **Field mapping errors**: Failed transformations
- **Data quality scores**: Track over time

### Alerting
- **Schema load failures**: Alert on schema file issues
- **Validation errors**: High error rates
- **Performance degradation**: Slow processing times
- **Data quality drops**: Quality score decreases

## 🎯 Best Practices

### Schema Design
1. **Keep schemas simple**: Avoid complex nested mappings
2. **Use descriptive names**: Clear field descriptions
3. **Version incrementally**: Small, manageable changes
4. **Test thoroughly**: Validate with real data

### Change Management
1. **Document changes**: Clear change descriptions
2. **Gradual rollouts**: Test with small data sets
3. **Monitoring**: Watch for errors after changes
4. **Rollback plan**: Always have previous version ready

### Performance
1. **Cache schemas**: Load once, use many times
2. **Batch processing**: Process multiple posts together
3. **Optimize functions**: Efficient preprocessing
4. **Monitor resources**: Track memory and CPU usage

This schema configuration system provides the flexibility and reliability needed for production social media analytics while maintaining the ability to adapt to changing data sources.