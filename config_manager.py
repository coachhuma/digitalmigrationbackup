"""
Configuration Manager and Logging System
Centralized configuration management and comprehensive logging for migration and backup systems
"""

import os
import json
import yaml
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum


class LogLevel(Enum):
    """Logging levels enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class OperationType(Enum):
    """Operation types for logging and configuration."""
    MIGRATION = "migration"
    BACKUP = "backup"
    VERIFICATION = "verification"
    ICLOUD_SYNC = "icloud_sync"
    CONVERSATION_BACKUP = "conversation_backup"


@dataclass
class MigrationConfig:
    """Configuration for migration operations."""
    # Source and destination paths
    source_paths: List[str]
    destination_path: str
    
    # File filtering
    file_extensions: List[str]
    exclude_patterns: List[str]
    include_patterns: List[str]
    
    # Size limits
    max_file_size_gb: float
    min_file_size_kb: float
    
    # Processing options
    preserve_structure: bool
    create_date_folders: bool
    overwrite_existing: bool
    verify_copies: bool
    
    # Performance settings
    batch_size: int
    max_concurrent_operations: int
    progress_update_interval: int
    
    # Safety settings
    create_backups: bool
    dry_run_mode: bool
    stop_on_error: bool


@dataclass
class BackupConfig:
    """Configuration for backup operations."""
    # Backup targets
    backup_sources: List[str]
    backup_destinations: List[str]
    
    # Backup types
    full_backup: bool
    incremental_backup: bool
    differential_backup: bool
    
    # Verification settings
    verify_integrity: bool
    hash_verification: bool
    size_verification: bool
    
    # Retention settings
    keep_versions: int
    auto_cleanup: bool
    cleanup_after_days: int
    
    # Compression settings
    compress_backups: bool
    compression_level: int


@dataclass
class iCloudConfig:
    """Configuration for iCloud backup operations."""
    # iCloud settings
    download_originals: bool
    organize_by_date: bool
    preserve_metadata: bool
    
    # Content types
    backup_photos: bool
    backup_videos: bool
    backup_live_photos: bool
    backup_screenshots: bool
    
    # Quality settings
    verify_download_quality: bool
    min_photo_size_mb: float
    min_video_bitrate: float
    
    # Organization
    create_year_folders: bool
    create_month_folders: bool
    separate_by_device: bool


@dataclass
class LoggingConfig:
    """Configuration for logging system."""
    # Log levels
    console_level: LogLevel
    file_level: LogLevel
    
    # File settings
    log_directory: str
    log_filename_pattern: str
    max_log_size_mb: int
    backup_count: int
    
    # Format settings
    console_format: str
    file_format: str
    date_format: str
    
    # Advanced settings
    enable_rotation: bool
    enable_compression: bool
    log_to_syslog: bool


class ConfigurationManager:
    """
    Centralized configuration management system.
    
    Handles loading, saving, and validating configurations for all system components.
    """
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration manager.
        
        Args:
            config_dir: Directory to store configuration files
        """
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
        self.configs = {}
        self.logger = self._setup_basic_logging()
        
        # Load default configurations
        self._load_default_configs()
    
    def _setup_basic_logging(self) -> logging.Logger:
        """Set up basic logging for configuration manager."""
        logger = logging.getLogger('ConfigurationManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _load_default_configs(self):
        """Load default configurations for all components."""
        # Default migration configuration
        self.configs['migration'] = MigrationConfig(
            source_paths=[],
            destination_path="",
            file_extensions=[
                # Documents
                '.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt',
                # Spreadsheets
                '.xls', '.xlsx', '.csv', '.ods',
                # Presentations
                '.ppt', '.pptx', '.odp',
                # Images
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic',
                # Videos
                '.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv',
                # Audio
                '.mp3', '.wav', '.flac', '.aac', '.ogg',
                # Archives
                '.zip', '.rar', '.7z', '.tar', '.gz',
                # Legacy formats
                '.wps', '.wpd', '.123', '.wk1', '.pcx', '.tga'
            ],
            exclude_patterns=[
                '*.tmp', '*.temp', '*.cache', '*.log',
                'Thumbs.db', '.DS_Store', 'desktop.ini',
                '$RECYCLE.BIN', '.Trash*'
            ],
            include_patterns=['*'],
            max_file_size_gb=10.0,
            min_file_size_kb=1.0,
            preserve_structure=True,
            create_date_folders=False,
            overwrite_existing=False,
            verify_copies=True,
            batch_size=100,
            max_concurrent_operations=4,
            progress_update_interval=10,
            create_backups=True,
            dry_run_mode=False,
            stop_on_error=False
        )
        
        # Default backup configuration
        self.configs['backup'] = BackupConfig(
            backup_sources=[],
            backup_destinations=[],
            full_backup=True,
            incremental_backup=False,
            differential_backup=False,
            verify_integrity=True,
            hash_verification=True,
            size_verification=True,
            keep_versions=5,
            auto_cleanup=True,
            cleanup_after_days=30,
            compress_backups=False,
            compression_level=6
        )
        
        # Default iCloud configuration
        self.configs['icloud'] = iCloudConfig(
            download_originals=True,
            organize_by_date=True,
            preserve_metadata=True,
            backup_photos=True,
            backup_videos=True,
            backup_live_photos=True,
            backup_screenshots=True,
            verify_download_quality=True,
            min_photo_size_mb=1.0,
            min_video_bitrate=5.0,
            create_year_folders=True,
            create_month_folders=True,
            separate_by_device=True
        )
        
        # Default logging configuration
        self.configs['logging'] = LoggingConfig(
            console_level=LogLevel.INFO,
            file_level=LogLevel.DEBUG,
            log_directory="logs",
            log_filename_pattern="{operation}_{timestamp}.log",
            max_log_size_mb=10,
            backup_count=5,
            console_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            file_format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
            date_format="%Y-%m-%d %H:%M:%S",
            enable_rotation=True,
            enable_compression=True,
            log_to_syslog=False
        )
    
    def save_config(self, config_name: str, config_data: Union[Dict, Any], format: str = 'json'):
        """
        Save configuration to file.
        
        Args:
            config_name: Name of the configuration
            config_data: Configuration data (dict or dataclass)
            format: File format ('json' or 'yaml')
        """
        try:
            # Convert dataclass to dict if necessary
            if hasattr(config_data, '__dataclass_fields__'):
                data = asdict(config_data)
            else:
                data = config_data
            
            # Handle enum values
            data = self._serialize_enums(data)
            
            # Determine file path
            extension = 'json' if format == 'json' else 'yaml'
            config_path = self.config_dir / f"{config_name}.{extension}"
            
            # Save configuration
            with open(config_path, 'w') as f:
                if format == 'json':
                    json.dump(data, f, indent=2, default=str)
                else:
                    yaml.dump(data, f, default_flow_style=False)
            
            self.logger.info(f"Configuration saved: {config_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving configuration {config_name}: {e}")
            raise
    
    def load_config(self, config_name: str, config_class: type = None, format: str = 'json') -> Union[Dict, Any]:
        """
        Load configuration from file.
        
        Args:
            config_name: Name of the configuration
            config_class: Optional dataclass to instantiate
            format: File format ('json' or 'yaml')
            
        Returns:
            Configuration data (dict or dataclass instance)
        """
        try:
            # Determine file path
            extension = 'json' if format == 'json' else 'yaml'
            config_path = self.config_dir / f"{config_name}.{extension}"
            
            if not config_path.exists():
                self.logger.warning(f"Configuration file not found: {config_path}")
                return self.configs.get(config_name, {})
            
            # Load configuration
            with open(config_path, 'r') as f:
                if format == 'json':
                    data = json.load(f)
                else:
                    data = yaml.safe_load(f)
            
            # Deserialize enums
            data = self._deserialize_enums(data)
            
            # Create dataclass instance if requested
            if config_class and hasattr(config_class, '__dataclass_fields__'):
                return config_class(**data)
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error loading configuration {config_name}: {e}")
            return self.configs.get(config_name, {})
    
    def _serialize_enums(self, data: Any) -> Any:
        """Convert enum values to strings for serialization."""
        if isinstance(data, dict):
            return {key: self._serialize_enums(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._serialize_enums(item) for item in data]
        elif isinstance(data, Enum):
            return data.value
        else:
            return data
    
    def _deserialize_enums(self, data: Any) -> Any:
        """Convert string values back to enums after deserialization."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                # Check for enum fields
                if key.endswith('_level') and isinstance(value, str):
                    try:
                        result[key] = LogLevel(value)
                    except ValueError:
                        result[key] = value
                else:
                    result[key] = self._deserialize_enums(value)
            return result
        elif isinstance(data, list):
            return [self._deserialize_enums(item) for item in data]
        else:
            return data
    
    def get_config(self, config_name: str) -> Any:
        """Get configuration by name."""
        return self.configs.get(config_name)
    
    def set_config(self, config_name: str, config_data: Any):
        """Set configuration by name."""
        self.configs[config_name] = config_data
    
    def validate_config(self, config_name: str, config_data: Any) -> List[str]:
        """
        Validate configuration data.
        
        Args:
            config_name: Name of the configuration
            config_data: Configuration data to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        try:
            if config_name == 'migration':
                errors.extend(self._validate_migration_config(config_data))
            elif config_name == 'backup':
                errors.extend(self._validate_backup_config(config_data))
            elif config_name == 'icloud':
                errors.extend(self._validate_icloud_config(config_data))
            elif config_name == 'logging':
                errors.extend(self._validate_logging_config(config_data))
        
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        return errors
    
    def _validate_migration_config(self, config: MigrationConfig) -> List[str]:
        """Validate migration configuration."""
        errors = []
        
        if not config.destination_path:
            errors.append("Destination path is required")
        
        if config.max_file_size_gb <= 0:
            errors.append("Max file size must be positive")
        
        if config.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if config.max_concurrent_operations <= 0:
            errors.append("Max concurrent operations must be positive")
        
        return errors
    
    def _validate_backup_config(self, config: BackupConfig) -> List[str]:
        """Validate backup configuration."""
        errors = []
        
        if not config.backup_destinations:
            errors.append("At least one backup destination is required")
        
        if config.keep_versions <= 0:
            errors.append("Keep versions must be positive")
        
        if config.cleanup_after_days <= 0:
            errors.append("Cleanup after days must be positive")
        
        return errors
    
    def _validate_icloud_config(self, config: iCloudConfig) -> List[str]:
        """Validate iCloud configuration."""
        errors = []
        
        if config.min_photo_size_mb <= 0:
            errors.append("Min photo size must be positive")
        
        if config.min_video_bitrate <= 0:
            errors.append("Min video bitrate must be positive")
        
        return errors
    
    def _validate_logging_config(self, config: LoggingConfig) -> List[str]:
        """Validate logging configuration."""
        errors = []
        
        if not config.log_directory:
            errors.append("Log directory is required")
        
        if config.max_log_size_mb <= 0:
            errors.append("Max log size must be positive")
        
        if config.backup_count < 0:
            errors.append("Backup count cannot be negative")
        
        return errors
    
    def create_config_template(self, config_name: str) -> str:
        """
        Create a configuration template file.
        
        Args:
            config_name: Name of the configuration
            
        Returns:
            Path to created template file
        """
        template_path = self.config_dir / f"{config_name}_template.json"
        
        template_data = self.configs.get(config_name, {})
        
        # Add comments for template
        if hasattr(template_data, '__dataclass_fields__'):
            template_dict = asdict(template_data)
        else:
            template_dict = template_data
        
        template_dict['_template_info'] = {
            'description': f'Configuration template for {config_name}',
            'created': datetime.now().isoformat(),
            'instructions': 'Modify values as needed and save without the _template_info section'
        }
        
        with open(template_path, 'w') as f:
            json.dump(template_dict, f, indent=2, default=str)
        
        self.logger.info(f"Configuration template created: {template_path}")
        return str(template_path)


class AdvancedLogger:
    """
    Advanced logging system with multiple handlers and custom formatting.
    """
    
    def __init__(self, config: LoggingConfig, operation_type: OperationType):
        """
        Initialize advanced logger.
        
        Args:
            config: Logging configuration
            operation_type: Type of operation being logged
        """
        self.config = config
        self.operation_type = operation_type
        self.log_dir = Path(config.log_directory)
        self.log_dir.mkdir(exist_ok=True)
        
        self.logger = self._setup_logger()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Statistics tracking
        self.stats = {
            'debug_count': 0,
            'info_count': 0,
            'warning_count': 0,
            'error_count': 0,
            'critical_count': 0,
            'start_time': datetime.now(),
            'operations_logged': 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logger with multiple handlers."""
        logger_name = f"{self.operation_type.value}_{self.session_id}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, self.config.console_level.value))
        console_formatter = logging.Formatter(
            self.config.console_format,
            datefmt=self.config.date_format
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler with rotation
        log_filename = self.config.log_filename_pattern.format(
            operation=self.operation_type.value,
            timestamp=self.session_id
        )
        log_path = self.log_dir / log_filename
        
        if self.config.enable_rotation:
            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=self.config.max_log_size_mb * 1024 * 1024,
                backupCount=self.config.backup_count
            )
        else:
            file_handler = logging.FileHandler(log_path)
        
        file_handler.setLevel(getattr(logging, self.config.file_level.value))
        file_formatter = logging.Formatter(
            self.config.file_format,
            datefmt=self.config.date_format
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Syslog handler (if enabled)
        if self.config.log_to_syslog:
            try:
                syslog_handler = logging.handlers.SysLogHandler()
                syslog_handler.setLevel(logging.WARNING)
                syslog_formatter = logging.Formatter(
                    f"{self.operation_type.value}: %(message)s"
                )
                syslog_handler.setFormatter(syslog_formatter)
                logger.addHandler(syslog_handler)
            except Exception as e:
                logger.warning(f"Could not set up syslog handler: {e}")
        
        return logger
    
    def debug(self, message: str, extra_data: Dict = None):
        """Log debug message."""
        self.stats['debug_count'] += 1
        self._log_with_context(logging.DEBUG, message, extra_data)
    
    def info(self, message: str, extra_data: Dict = None):
        """Log info message."""
        self.stats['info_count'] += 1
        self._log_with_context(logging.INFO, message, extra_data)
    
    def warning(self, message: str, extra_data: Dict = None):
        """Log warning message."""
        self.stats['warning_count'] += 1
        self._log_with_context(logging.WARNING, message, extra_data)
    
    def error(self, message: str, extra_data: Dict = None):
        """Log error message."""
        self.stats['error_count'] += 1
        self._log_with_context(logging.ERROR, message, extra_data)
    
    def critical(self, message: str, extra_data: Dict = None):
        """Log critical message."""
        self.stats['critical_count'] += 1
        self._log_with_context(logging.CRITICAL, message, extra_data)
    
    def _log_with_context(self, level: int, message: str, extra_data: Dict = None):
        """Log message with additional context."""
        self.stats['operations_logged'] += 1
        
        # Add context information
        context = {
            'session_id': self.session_id,
            'operation_type': self.operation_type.value,
            'operation_count': self.stats['operations_logged']
        }
        
        if extra_data:
            context.update(extra_data)
        
        # Format message with context
        if extra_data:
            formatted_message = f"{message} | Context: {json.dumps(context, default=str)}"
        else:
            formatted_message = message
        
        self.logger.log(level, formatted_message)
    
    def log_operation_start(self, operation_name: str, details: Dict = None):
        """Log the start of an operation."""
        self.info(f"Starting operation: {operation_name}", details)
    
    def log_operation_end(self, operation_name: str, success: bool, details: Dict = None):
        """Log the end of an operation."""
        status = "SUCCESS" if success else "FAILED"
        level = self.info if success else self.error
        level(f"Operation {status}: {operation_name}", details)
    
    def log_progress(self, current: int, total: int, operation: str = "Processing"):
        """Log progress information."""
        percentage = (current / total) * 100 if total > 0 else 0
        self.info(f"{operation} progress: {current}/{total} ({percentage:.1f}%)")
    
    def get_session_stats(self) -> Dict:
        """Get logging statistics for current session."""
        duration = datetime.now() - self.stats['start_time']
        
        return {
            'session_id': self.session_id,
            'operation_type': self.operation_type.value,
            'duration_seconds': duration.total_seconds(),
            'total_log_entries': self.stats['operations_logged'],
            'log_level_counts': {
                'debug': self.stats['debug_count'],
                'info': self.stats['info_count'],
                'warning': self.stats['warning_count'],
                'error': self.stats['error_count'],
                'critical': self.stats['critical_count']
            },
            'error_rate': (self.stats['error_count'] + self.stats['critical_count']) / max(1, self.stats['operations_logged']),
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': datetime.now().isoformat()
        }


def main():
    """Example usage of configuration manager and logging system."""
    print("⚙️ Configuration Manager and Logging System")
    print("=" * 50)
    
    # Initialize configuration manager
    config_manager = ConfigurationManager()
    
    # Create configuration templates
    print("\n📝 Creating configuration templates...")
    for config_name in ['migration', 'backup', 'icloud', 'logging']:
        template_path = config_manager.create_config_template(config_name)
        print(f"Template created: {template_path}")
    
    # Example: Modify and save a configuration
    print("\n⚙️ Configuring migration settings...")
    migration_config = config_manager.get_config('migration')
    migration_config.destination_path = "/backup/migration_target"
    migration_config.max_file_size_gb = 5.0
    migration_config.batch_size = 50
    
    # Validate configuration
    errors = config_manager.validate_config('migration', migration_config)
    if errors:
        print("❌ Configuration errors:")
        for error in errors:
            print(f"  • {error}")
    else:
        print("✅ Configuration is valid")
        config_manager.save_config('migration', migration_config)
    
    # Set up advanced logging
    print("\n📊 Setting up advanced logging...")
    logging_config = config_manager.get_config('logging')
    logger = AdvancedLogger(logging_config, OperationType.MIGRATION)
    
    # Example logging operations
    logger.log_operation_start("Example Migration", {"source": "/source", "destination": "/dest"})
    
    for i in range(1, 6):
        logger.log_progress(i, 5, "File processing")
        logger.info(f"Processing file {i}")
    
    logger.warning("Example warning message")
    logger.log_operation_end("Example Migration", True, {"files_processed": 5})
    
    # Get session statistics
    stats = logger.get_session_stats()
    print(f"\n📈 Session Statistics:")
    print(f"Duration: {stats['duration_seconds']:.1f} seconds")
    print(f"Log entries: {stats['total_log_entries']}")
    print(f"Error rate: {stats['error_rate']:.2%}")
    
    print(f"\nLog level counts:")
    for level, count in stats['log_level_counts'].items():
        print(f"  {level.upper()}: {count}")


if __name__ == "__main__":
    main()