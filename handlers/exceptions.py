"""Enhanced exception hierarchy for data processing handlers.

This module provides a comprehensive exception hierarchy with rich context,
error categorization, and recovery hints for better error handling and debugging.
"""

from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
import json


class ErrorSeverity(Enum):
    """Severity levels for errors."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of errors for better classification."""
    VALIDATION = "validation"
    EXTERNAL_SERVICE = "external_service"
    RESOURCE_LIMIT = "resource_limit"
    DATA_QUALITY = "data_quality"
    CONFIGURATION = "configuration"
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    TIMEOUT = "timeout"
    INTERNAL = "internal"


class HandlerError(Exception):
    """Base exception for all handler errors with rich context.
    
    This base class provides comprehensive error context including:
    - Error categorization and severity
    - Handler and operation context
    - Recovery hints and retry information
    - Structured error data for monitoring
    """
    
    def __init__(self,
                 message: str,
                 error_code: str,
                 handler_name: str,
                 operation: str,
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                 category: ErrorCategory = ErrorCategory.EXTERNAL_SERVICE,
                 context: Dict[str, Any] = None,
                 retry_after: Optional[int] = None,
                 recoverable: bool = True,
                 cause: Optional[Exception] = None):
        """Initialize handler error with context.
        
        Args:
            message: Human-readable error message
            error_code: Machine-readable error code
            handler_name: Name of the handler where error occurred
            operation: Operation being performed when error occurred
            severity: Error severity level
            category: Error category for classification
            context: Additional context data
            retry_after: Seconds to wait before retry (if applicable)
            recoverable: Whether error is recoverable
            cause: Original exception that caused this error
        """
        self.message = message
        self.error_code = error_code
        self.handler_name = handler_name
        self.operation = operation
        self.severity = severity
        self.category = category
        self.context = context or {}
        self.retry_after = retry_after
        self.recoverable = recoverable
        self.cause = cause
        self.timestamp = datetime.utcnow()
        
        # Call parent constructor with message
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with context."""
        parts = [
            f"[{self.handler_name}:{self.operation}]",
            f"[{self.error_code}]",
            f"[{self.severity.value.upper()}]",
            self.message
        ]
        
        if self.retry_after:
            parts.append(f"(retry after {self.retry_after}s)")
        
        if not self.recoverable:
            parts.append("(non-recoverable)")
        
        return " ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/monitoring."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "handler_name": self.handler_name,
            "operation": self.operation,
            "severity": self.severity.value,
            "category": self.category.value,
            "context": self.context,
            "retry_after": self.retry_after,
            "recoverable": self.recoverable,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None
        }
    
    def to_json(self) -> str:
        """Convert error to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


# BigQuery specific errors (extending existing BigQueryInsertionError)
class BigQueryHandlerError(HandlerError):
    """Base class for BigQuery-related errors."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('handler_name', 'BigQueryHandler')
        kwargs.setdefault('category', ErrorCategory.EXTERNAL_SERVICE)
        super().__init__(message, **kwargs)


class BigQueryInsertionError(BigQueryHandlerError):
    """Error during BigQuery data insertion.
    
    Extends the existing BigQueryInsertionError to maintain compatibility
    while adding enhanced error context.
    """
    
    def __init__(self, message: str, table_id: str = None, 
                 row_count: int = None, **kwargs):
        context = kwargs.get('context', {})
        if table_id:
            context['table_id'] = table_id
        if row_count is not None:
            context['row_count'] = row_count
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'BIGQUERY_INSERTION_FAILED')
        kwargs.setdefault('operation', 'insert_rows')
        super().__init__(message, **kwargs)


class BigQueryQuotaExceededError(BigQueryHandlerError):
    """BigQuery quota exceeded error."""
    
    def __init__(self, message: str, quota_type: str = None, **kwargs):
        context = kwargs.get('context', {})
        if quota_type:
            context['quota_type'] = quota_type
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'BIGQUERY_QUOTA_EXCEEDED')
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        kwargs.setdefault('category', ErrorCategory.RESOURCE_LIMIT)
        kwargs.setdefault('retry_after', 300)  # 5 minutes default
        super().__init__(message, **kwargs)


class BigQuerySchemaError(BigQueryHandlerError):
    """BigQuery schema validation error."""
    
    def __init__(self, message: str, field_name: str = None, **kwargs):
        context = kwargs.get('context', {})
        if field_name:
            context['field_name'] = field_name
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'BIGQUERY_SCHEMA_INVALID')
        kwargs.setdefault('category', ErrorCategory.VALIDATION)
        kwargs.setdefault('recoverable', False)
        super().__init__(message, **kwargs)


# GCS specific errors
class GCSHandlerError(HandlerError):
    """Base class for GCS-related errors."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('handler_name', 'GCSProcessedHandler')
        kwargs.setdefault('category', ErrorCategory.EXTERNAL_SERVICE)
        super().__init__(message, **kwargs)


class GCSUploadError(GCSHandlerError):
    """Error during GCS upload operation."""
    
    def __init__(self, message: str, bucket_name: str = None,
                 blob_name: str = None, **kwargs):
        context = kwargs.get('context', {})
        if bucket_name:
            context['bucket_name'] = bucket_name
        if blob_name:
            context['blob_name'] = blob_name
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'GCS_UPLOAD_FAILED')
        kwargs.setdefault('operation', 'upload_blob')
        super().__init__(message, **kwargs)


class GCSAuthenticationError(GCSHandlerError):
    """GCS authentication error."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('error_code', 'GCS_AUTH_FAILED')
        kwargs.setdefault('category', ErrorCategory.AUTHENTICATION)
        kwargs.setdefault('severity', ErrorSeverity.CRITICAL)
        kwargs.setdefault('recoverable', False)
        super().__init__(message, **kwargs)


class GCSQuotaExceededError(GCSHandlerError):
    """GCS storage quota exceeded."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('error_code', 'GCS_QUOTA_EXCEEDED')
        kwargs.setdefault('category', ErrorCategory.RESOURCE_LIMIT)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        super().__init__(message, **kwargs)


# Media tracking specific errors (extending existing MediaTrackingError)
class MediaTrackingError(HandlerError):
    """Base class for media tracking errors.
    
    Extends the existing MediaTrackingError to maintain compatibility
    while adding enhanced error context.
    """
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('handler_name', 'MediaTrackingHandler')
        kwargs.setdefault('error_code', 'MEDIA_TRACKING_ERROR')
        super().__init__(message, **kwargs)


class MediaValidationError(MediaTrackingError):
    """Media validation failed."""
    
    def __init__(self, message: str, media_url: str = None, **kwargs):
        context = kwargs.get('context', {})
        if media_url:
            context['media_url'] = media_url
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'MEDIA_VALIDATION_FAILED')
        kwargs.setdefault('category', ErrorCategory.VALIDATION)
        super().__init__(message, **kwargs)


# Schema mapping errors
class SchemaMapperError(HandlerError):
    """Base class for schema mapping errors."""
    
    def __init__(self, message: str, **kwargs):
        kwargs.setdefault('handler_name', 'SchemaMapper')
        super().__init__(message, **kwargs)


class SchemaLoadError(SchemaMapperError):
    """Error loading schema definition."""
    
    def __init__(self, message: str, schema_path: str = None, **kwargs):
        context = kwargs.get('context', {})
        if schema_path:
            context['schema_path'] = schema_path
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'SCHEMA_LOAD_FAILED')
        kwargs.setdefault('category', ErrorCategory.CONFIGURATION)
        kwargs.setdefault('recoverable', False)
        super().__init__(message, **kwargs)


class DataTransformationError(SchemaMapperError):
    """Error during data transformation."""
    
    def __init__(self, message: str, field_name: str = None, **kwargs):
        context = kwargs.get('context', {})
        if field_name:
            context['field_name'] = field_name
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'DATA_TRANSFORMATION_FAILED')
        kwargs.setdefault('category', ErrorCategory.DATA_QUALITY)
        super().__init__(message, **kwargs)


# General validation errors
class ValidationError(HandlerError):
    """Input validation error."""
    
    def __init__(self, message: str, field: str = None,
                 expected_type: str = None, **kwargs):
        context = kwargs.get('context', {})
        if field:
            context['field'] = field
        if expected_type:
            context['expected_type'] = expected_type
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'VALIDATION_FAILED')
        kwargs.setdefault('category', ErrorCategory.VALIDATION)
        kwargs.setdefault('severity', ErrorSeverity.LOW)
        kwargs.setdefault('recoverable', False)
        super().__init__(message, **kwargs)


# Network and timeout errors
class NetworkTimeoutError(HandlerError):
    """Network timeout error."""
    
    def __init__(self, message: str, timeout_seconds: int = None, **kwargs):
        context = kwargs.get('context', {})
        if timeout_seconds:
            context['timeout_seconds'] = timeout_seconds
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'NETWORK_TIMEOUT')
        kwargs.setdefault('category', ErrorCategory.TIMEOUT)
        kwargs.setdefault('severity', ErrorSeverity.MEDIUM)
        super().__init__(message, **kwargs)


class ExternalServiceError(HandlerError):
    """Generic external service error."""
    
    def __init__(self, message: str, service_name: str = None, **kwargs):
        context = kwargs.get('context', {})
        if service_name:
            context['service_name'] = service_name
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'EXTERNAL_SERVICE_ERROR')
        kwargs.setdefault('category', ErrorCategory.EXTERNAL_SERVICE)
        super().__init__(message, **kwargs)


# Configuration errors
class ConfigurationError(HandlerError):
    """Configuration error."""
    
    def __init__(self, message: str, config_key: str = None, **kwargs):
        context = kwargs.get('context', {})
        if config_key:
            context['config_key'] = config_key
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'CONFIGURATION_ERROR')
        kwargs.setdefault('category', ErrorCategory.CONFIGURATION)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        kwargs.setdefault('recoverable', False)
        super().__init__(message, **kwargs)


# Quota and resource limit errors
class QuotaExceededError(HandlerError):
    """Generic quota exceeded error."""
    
    def __init__(self, message: str, resource_type: str = None,
                 current_usage: int = None, limit: int = None, **kwargs):
        context = kwargs.get('context', {})
        if resource_type:
            context['resource_type'] = resource_type
        if current_usage is not None:
            context['current_usage'] = current_usage
        if limit is not None:
            context['limit'] = limit
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'QUOTA_EXCEEDED')
        kwargs.setdefault('category', ErrorCategory.RESOURCE_LIMIT)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        super().__init__(message, **kwargs)


class DataQualityError(HandlerError):
    """Data quality or integrity error."""
    
    def __init__(self, message: str, field_name: str = None,
                 expected_type: str = None, actual_value: Any = None, **kwargs):
        context = kwargs.get('context', {})
        if field_name:
            context['field_name'] = field_name
        if expected_type:
            context['expected_type'] = expected_type
        if actual_value is not None:
            context['actual_value'] = str(actual_value)
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'DATA_QUALITY_ERROR')
        kwargs.setdefault('category', ErrorCategory.DATA_QUALITY)
        kwargs.setdefault('severity', ErrorSeverity.MEDIUM)
        super().__init__(message, **kwargs)


class ResourceLimitError(HandlerError):
    """Resource limit exceeded error."""
    
    def __init__(self, message: str, resource_name: str = None,
                 current_usage: float = None, limit: float = None, **kwargs):
        context = kwargs.get('context', {})
        if resource_name:
            context['resource_name'] = resource_name
        if current_usage is not None:
            context['current_usage'] = current_usage
        if limit is not None:
            context['limit'] = limit
        
        kwargs['context'] = context
        kwargs.setdefault('error_code', 'RESOURCE_LIMIT_EXCEEDED')
        kwargs.setdefault('category', ErrorCategory.RESOURCE_LIMIT)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        super().__init__(message, **kwargs)