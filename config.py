"""
Arkitekt OpenCRM - Configuration Module

This module handles loading, validating, and providing access to application
configuration from environment variables. It uses python-dotenv to load .env files
and provides a typed configuration object with validation.

Features:
- Automatic loading of .env file
- Validation of required configuration values
- Type conversion and default values
- Singleton pattern for global access
- Comprehensive error messages
- Support for multiple environments (dev, staging, production)

Usage:
    from config import config
    
    # Access configuration values
    meta_token = config.meta_page_access_token
    db_path = config.db_path
    
Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import os
import sys
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class Config:
    """
    Application configuration class that loads and validates environment variables.
    
    This class implements the singleton pattern to ensure consistent configuration
    access throughout the application.
    
    Attributes:
        Environment settings, API credentials, database settings, logging settings,
        and application behavior configuration.
    """
    
    _instance = None
    
    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """
        Initialize configuration by loading environment variables.
        
        Raises:
            ConfigurationError: If required configuration is missing or invalid.
        """
        # Prevent re-initialization in singleton
        if self._initialized:
            return
        
        # Load .env file if it exists
        self._load_env_file()
        
        # Load and validate all configuration
        try:
            self._load_environment_settings()
            self._load_meta_api_config()
            self._load_microsoft_graph_config()
            self._load_email_config()
            self._load_booking_config()
            self._load_application_config()
            self._load_database_config()
            self._load_logging_config()
            self._load_error_handling_config()
            self._load_health_check_config()
            self._load_security_config()
            self._load_rate_limiting_config()
            self._load_notification_config()
            self._load_performance_config()
            self._load_timezone_config()
            self._load_branding_config()
            self._load_llm_config()

            # Validate required fields
            self._validate_required_config()
            
            self._initialized = True
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}") from e
    
    def _load_env_file(self) -> None:
        """
        Load environment variables from .env file.
        
        Searches for .env file in current directory and parent directories.
        """
        # Try to find .env file
        current_dir = Path.cwd()
        env_file = current_dir / '.env'
        
        # Check if .env exists in current directory
        if env_file.exists():
            load_dotenv(env_file, override=True)
            print(f"[CONFIG] Loaded environment from: {env_file}")
        else:
            # Try parent directory (useful for different execution contexts)
            parent_env = current_dir.parent / '.env'
            if parent_env.exists():
                load_dotenv(parent_env, override=True)
                print(f"[CONFIG] Loaded environment from: {parent_env}")
            else:
                print("[CONFIG] No .env file found, using system environment variables")
    
    def _load_environment_settings(self) -> None:
        """Load general environment settings."""
        self.environment = os.getenv('ENVIRONMENT', 'production').lower()
        self.debug = self._get_bool('DEBUG', False)
        self.dry_run = self._get_bool('DRY_RUN', False)
        self.mock_api_enabled = self._get_bool('MOCK_API_ENABLED', False)
    
    def _load_meta_api_config(self) -> None:
        """Load Meta (Facebook) Lead Ads API configuration."""
        self.meta_page_id = os.getenv('META_PAGE_ID', '').strip()
        self.meta_page_access_token = os.getenv('META_PAGE_ACCESS_TOKEN', '').strip()
        self.meta_form_id = os.getenv('META_FORM_ID', '').strip()
        self.meta_api_version = os.getenv('META_API_VERSION', 'v21.0').strip()
        
        # Construct Meta API base URL
        self.meta_api_base_url = f"https://graph.facebook.com/{self.meta_api_version}"
    
    def _load_microsoft_graph_config(self) -> None:
        """Load Microsoft Graph API (Azure AD) configuration."""
        self.ms_tenant_id = os.getenv('MS_TENANT_ID', '').strip()
        self.ms_client_id = os.getenv('MS_CLIENT_ID', '').strip()
        self.ms_client_secret = os.getenv('MS_CLIENT_SECRET', '').strip()
        self.ms_sender_email = os.getenv('MS_SENDER_EMAIL', '').strip()
        
        # Microsoft Graph API endpoints
        self.ms_authority = f"https://login.microsoftonline.com/{self.ms_tenant_id}"
        self.ms_graph_endpoint = "https://graph.microsoft.com/v1.0"
        self.ms_scope = ["https://graph.microsoft.com/.default"]
    
    def _load_email_config(self) -> None:
        """Load email configuration."""
        # Recipient email - defaults to sender if not specified
        recipient_env = os.getenv('MS_RECIPIENT_EMAIL', '').strip()
        if recipient_env:
            # Support comma-separated multiple recipients
            self.ms_recipient_emails = [email.strip() for email in recipient_env.split(',')]
        else:
            self.ms_recipient_emails = [self.ms_sender_email]
        
        # Email subject template
        self.email_subject_template = os.getenv(
            'EMAIL_SUBJECT_TEMPLATE',
            'New Lead: {name}'
        ).strip()
        
        # Advanced email settings
        self.email_template_path = os.getenv('EMAIL_TEMPLATE_PATH', '').strip()
        self.email_include_attachments = self._get_bool('EMAIL_INCLUDE_ATTACHMENTS', False)
        self.email_format = os.getenv('EMAIL_FORMAT', 'html').lower()
        self.email_priority = os.getenv('EMAIL_PRIORITY', 'normal').lower()
    
    def _load_booking_config(self) -> None:
        """Load booking/scheduling configuration."""
        self.booking_url = os.getenv('BOOKING_URL', '').strip()
    
    def _load_application_config(self) -> None:
        """Load general application configuration."""
        self.poll_interval_seconds = self._get_int('POLL_INTERVAL_SECONDS', 900)
        self.batch_size = self._get_int('BATCH_SIZE', 100)
        
        # Ensure batch size doesn't exceed Meta API limit
        if self.batch_size > 500:
            print(f"[CONFIG] Warning: BATCH_SIZE ({self.batch_size}) exceeds Meta API limit (500). Setting to 500.")
            self.batch_size = 500
    
    def _load_database_config(self) -> None:
        """Load database configuration."""
        self.db_path = os.getenv('DB_PATH', '/app/data/leads.db').strip()
        self.db_backup_enabled = self._get_bool('DB_BACKUP_ENABLED', True)
        self.db_backup_interval_hours = self._get_int('DB_BACKUP_INTERVAL_HOURS', 24)
        
        # Ensure database directory exists
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_logging_config(self) -> None:
        """Load logging configuration."""
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.log_file_path = os.getenv('LOG_FILE_PATH', '/app/data/pipeline.log').strip()
        self.log_console_enabled = self._get_bool('LOG_CONSOLE_ENABLED', True)
        self.log_format = os.getenv('LOG_FORMAT', 'text').lower()
        
        # Log rotation settings
        self.log_rotation_enabled = self._get_bool('LOG_ROTATION_ENABLED', True)
        self.log_rotation_max_mb = self._get_int('LOG_ROTATION_MAX_MB', 10)
        self.log_rotation_backup_count = self._get_int('LOG_ROTATION_BACKUP_COUNT', 5)
        
        # Validate log level
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level not in valid_log_levels:
            print(f"[CONFIG] Warning: Invalid LOG_LEVEL '{self.log_level}'. Using 'INFO'.")
            self.log_level = 'INFO'
        
        # Ensure log directory exists
        log_dir = Path(self.log_file_path).parent
        log_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_error_handling_config(self) -> None:
        """Load error handling and retry configuration."""
        self.max_retries = self._get_int('MAX_RETRIES', 3)
        self.retry_delay = self._get_int('RETRY_DELAY', 5)
        self.request_timeout = self._get_int('REQUEST_TIMEOUT', 30)
    
    def _load_health_check_config(self) -> None:
        """Load health check configuration."""
        self.health_check_enabled = self._get_bool('HEALTH_CHECK_ENABLED', True)
        self.health_check_port = self._get_int('HEALTH_CHECK_PORT', 8080)
        self.health_check_path = os.getenv('HEALTH_CHECK_PATH', '/health').strip()
    
    def _load_security_config(self) -> None:
        """Load security configuration."""
        self.ssl_verify = self._get_bool('SSL_VERIFY', True)
        self.token_encryption_enabled = self._get_bool('TOKEN_ENCRYPTION_ENABLED', False)
        self.encryption_key = os.getenv('ENCRYPTION_KEY', '').strip()
        
        # Validate encryption settings
        if self.token_encryption_enabled and not self.encryption_key:
            raise ConfigurationError(
                "TOKEN_ENCRYPTION_ENABLED is true but ENCRYPTION_KEY is not set. "
                "Generate a key with: python -c \"from cryptography.fernet import Fernet; "
                "print(Fernet.generate_key().decode())\""
            )
    
    def _load_rate_limiting_config(self) -> None:
        """Load rate limiting configuration."""
        self.rate_limit_enabled = self._get_bool('RATE_LIMIT_ENABLED', True)
        self.rate_limit_requests = self._get_int('RATE_LIMIT_REQUESTS', 100)
        self.rate_limit_window_seconds = self._get_int('RATE_LIMIT_WINDOW_SECONDS', 3600)
    
    def _load_notification_config(self) -> None:
        """Load notification configuration (Slack, Discord, etc.)."""
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '').strip()
        self.discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL', '').strip()
        self.send_error_notifications = self._get_bool('SEND_ERROR_NOTIFICATIONS', False)
        self.send_success_notifications = self._get_bool('SEND_SUCCESS_NOTIFICATIONS', False)
    
    def _load_performance_config(self) -> None:
        """Load performance and optimization configuration."""
        self.connection_pool_size = self._get_int('CONNECTION_POOL_SIZE', 10)
        self.cache_enabled = self._get_bool('CACHE_ENABLED', True)
        self.cache_ttl_seconds = self._get_int('CACHE_TTL_SECONDS', 300)
    
    def _load_timezone_config(self) -> None:
        """Load timezone configuration."""
        self.timezone = os.getenv('TIMEZONE', 'UTC').strip()

    def _load_branding_config(self) -> None:
        """Load branding and identity configuration."""
        self.sender_name = os.getenv('SENDER_NAME', '').strip()
        self.company_name = os.getenv('COMPANY_NAME', '').strip()
        self.company_description = os.getenv('COMPANY_DESCRIPTION', '').strip()

    def _load_llm_config(self) -> None:
        """Load LLM configuration for AI email generation.

        Supports any OpenAI-compatible API (OpenAI, Ollama, Together, Groq, etc.)
        as well as Anthropic.  Falls back to legacy CLAUDE_* env vars for
        backwards compatibility.
        """
        self.llm_api_key = (
            os.getenv('LLM_API_KEY', '') or os.getenv('CLAUDE_API_KEY', '')
        ).strip()
        self.llm_model = (
            os.getenv('LLM_MODEL', '') or os.getenv('CLAUDE_MODEL', '')
        ).strip() or 'gpt-4o'
        self.llm_base_url = os.getenv('LLM_BASE_URL', '').strip()
        self.llm_provider = os.getenv('LLM_PROVIDER', '').strip().lower()

        # Auto-detect provider from API key if not explicitly set
        if not self.llm_provider:
            if self.llm_api_key.startswith('sk-ant-'):
                self.llm_provider = 'anthropic'
            else:
                self.llm_provider = 'openai'
    
    def _validate_required_config(self) -> None:
        """
        Validate that all required configuration values are present.

        Raises:
            ConfigurationError: If required configuration is missing.
        """
        errors = []

        # Check Meta API credentials
        if not self.meta_page_id or self.meta_page_id == 'YOUR_META_PAGE_ID_HERE':
            errors.append("META_PAGE_ID is required")

        if not self.meta_page_access_token or self.meta_page_access_token == 'YOUR_META_PAGE_ACCESS_TOKEN_HERE':
            errors.append("META_PAGE_ACCESS_TOKEN is required")

        # Check Microsoft Graph API credentials
        if not self.ms_tenant_id or self.ms_tenant_id == 'YOUR_AZURE_TENANT_ID_HERE':
            errors.append("MS_TENANT_ID is required")

        if not self.ms_client_id or self.ms_client_id == 'YOUR_AZURE_CLIENT_ID_HERE':
            errors.append("MS_CLIENT_ID is required")

        if not self.ms_client_secret or self.ms_client_secret == 'YOUR_AZURE_CLIENT_SECRET_HERE':
            errors.append("MS_CLIENT_SECRET is required")

        if not self.ms_sender_email:
            errors.append("MS_SENDER_EMAIL is required")

        # Check booking URL
        if not self.booking_url or self.booking_url == '<REQUIRED>':
            errors.append("BOOKING_URL is required")

        # If there are validation errors, raise exception with all errors
        if errors:
            error_message = (
                "Configuration validation failed. Please check your .env file.\n"
                "Missing or invalid configuration:\n  - " + "\n  - ".join(errors) +
                "\n\nPlease refer to .env.example for required configuration values."
            )
            raise ConfigurationError(error_message)
    
    def _get_bool(self, key: str, default: bool = False) -> bool:
        """
        Get boolean value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Boolean value
        """
        value = os.getenv(key, str(default)).strip().lower()
        return value in ('true', '1', 'yes', 'on', 'enabled')
    
    def _get_int(self, key: str, default: int = 0) -> int:
        """
        Get integer value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Integer value
        """
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            print(f"[CONFIG] Warning: Invalid integer value for {key}. Using default: {default}")
            return default
    
    def _get_float(self, key: str, default: float = 0.0) -> float:
        """
        Get float value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            Float value
        """
        try:
            return float(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            print(f"[CONFIG] Warning: Invalid float value for {key}. Using default: {default}")
            return default
    
    def _get_list(self, key: str, default: Optional[List[str]] = None, separator: str = ',') -> List[str]:
        """
        Get list value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            separator: Character to split on
            
        Returns:
            List of strings
        """
        if default is None:
            default = []
        
        value = os.getenv(key, '').strip()
        if not value:
            return default
        
        return [item.strip() for item in value.split(separator) if item.strip()]
    
    def __repr__(self) -> str:
        """String representation of configuration (excluding sensitive data)."""
        return (
            f"Config(environment={self.environment}, "
            f"meta_api_version={self.meta_api_version}, "
            f"poll_interval={self.poll_interval_seconds}s, "
            f"db_path={self.db_path}, "
            f"log_level={self.log_level})"
        )
    
    def print_config(self, hide_secrets: bool = True) -> None:
        """
        Print configuration summary.
        
        Args:
            hide_secrets: Whether to hide sensitive values (recommended)
        """
        print("\n" + "=" * 80)
        print("Arkitekt OpenCRM - Configuration Summary")
        print("=" * 80)
        
        print("\n[Environment]")
        print(f"  Environment:        {self.environment}")
        print(f"  Debug Mode:         {self.debug}")
        print(f"  Dry Run:            {self.dry_run}")
        
        print("\n[Meta API]")
        print(f"  Page ID:            {self._mask(self.meta_page_id, hide_secrets)}")
        print(f"  Access Token:       {self._mask(self.meta_page_access_token, hide_secrets)}")
        print(f"  API Version:        {self.meta_api_version}")
        print(f"  Form ID:            {self.meta_form_id or 'All forms'}")
        
        print("\n[Microsoft Graph API]")
        print(f"  Tenant ID:          {self._mask(self.ms_tenant_id, hide_secrets)}")
        print(f"  Client ID:          {self._mask(self.ms_client_id, hide_secrets)}")
        print(f"  Client Secret:      {self._mask(self.ms_client_secret, hide_secrets)}")
        print(f"  Sender Email:       {self.ms_sender_email}")
        print(f"  Recipients:         {', '.join(self.ms_recipient_emails)}")
        
        print("\n[Application]")
        print(f"  Poll Interval:      {self.poll_interval_seconds}s ({self.poll_interval_seconds/60:.1f} min)")
        print(f"  Batch Size:         {self.batch_size}")
        print(f"  Booking URL:        {self.booking_url}")
        
        print("\n[Database]")
        print(f"  Database Path:      {self.db_path}")
        print(f"  Backup Enabled:     {self.db_backup_enabled}")
        print(f"  Backup Interval:    {self.db_backup_interval_hours}h")
        
        print("\n[Logging]")
        print(f"  Log Level:          {self.log_level}")
        print(f"  Log File:           {self.log_file_path}")
        print(f"  Console Logging:    {self.log_console_enabled}")
        print(f"  Log Format:         {self.log_format}")
        print(f"  Rotation Enabled:   {self.log_rotation_enabled}")
        
        print("\n[Error Handling]")
        print(f"  Max Retries:        {self.max_retries}")
        print(f"  Retry Delay:        {self.retry_delay}s")
        print(f"  Request Timeout:    {self.request_timeout}s")
        
        print("\n[Health Check]")
        print(f"  Enabled:            {self.health_check_enabled}")
        print(f"  Port:               {self.health_check_port}")
        print(f"  Path:               {self.health_check_path}")
        
        print("\n" + "=" * 80 + "\n")
    
    @staticmethod
    def _mask(value: str, hide: bool, show_chars: int = 4) -> str:
        """
        Mask sensitive value for display.
        
        Args:
            value: Value to mask
            hide: Whether to hide the value
            show_chars: Number of characters to show at end
            
        Returns:
            Masked or original value
        """
        if not hide or not value:
            return value
        
        if len(value) <= show_chars:
            return '*' * len(value)
        
        return '*' * (len(value) - show_chars) + value[-show_chars:]


# Create singleton instance
config = Config()


# Expose for testing or re-initialization
def reload_config() -> Config:
    """
    Reload configuration (useful for testing).
    
    Returns:
        New Config instance
    """
    Config._instance = None
    return Config()


if __name__ == '__main__':
    """
    Test configuration loading when run as script.
    
    Usage:
        python config.py
    """
    try:
        config.print_config(hide_secrets=True)
        print("[SUCCESS] Configuration loaded successfully!")
        sys.exit(0)
    except ConfigurationError as e:
        print(f"[ERROR] Configuration failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        sys.exit(1)
