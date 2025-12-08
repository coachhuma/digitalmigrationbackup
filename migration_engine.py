"""
Digital Migration Engine
Main class for intelligent file migration with legacy format support
"""

import os
import shutil
import hashlib
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set


class MigrationEngine:
    """
    Advanced file migration engine with smart filtering and legacy format support.
    
    Features:
    - Intelligent file categorization
    - Legacy format detection and preservation
    - Smart overwrite logic
    - Progress tracking with time estimation
    - Comprehensive error handling
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize migration engine with configuration."""
        self.config = config or self._default_config()
        self.stats = self._init_stats()
        self.logger = self._setup_logging()
        
    def _default_config(self) -> Dict:
        """Default configuration for migration engine."""
        return {
            'modern_formats': {
                'documents': {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt',
                            '.xls', '.xlsx', '.csv', '.ppt', '.pptx', '.odp'},
                'images': {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif',
                          '.webp', '.svg', '.ico', '.raw', '.cr2', '.nef', '.dng'},
                'videos': {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm',
                          '.m4v', '.3gp', '.mpg', '.mpeg', '.m2v', '.mts'},
                'audio': {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a',
                         '.opus', '.aiff', '.au', '.ra'},
                'archives': {'.zip', '.rar', '.7z', '.tar', '.gz'}
            },
            'legacy_formats': {
                'legacy_documents': {'.wps', '.wpd', '.lwp', '.sxw', '.123', '.wk1', 
                                   '.wk3', '.shw', '.prz', '.mdb', '.dbf'},
                'legacy_images': {'.pcx', '.tga', '.psd', '.cdr', '.wmf', '.emf', 
                                 '.pic', '.pict', '.sgi'},
                'legacy_videos': {'.asf', '.rm', '.rmvb', '.vob', '.dat', '.divx', '.swf'},
                'legacy_audio': {'.mid', '.midi', '.mod', '.s3m', '.voc', '.cda'},
                'legacy_archives': {'.arj', '.lzh', '.cab', '.ace', '.sit'}
            },
            'skip_extensions': {'.exe', '.msi', '.dll', '.sys', '.bat', '.cmd', '.tmp', '.log'},
            'skip_folders': {'windows', 'program files', 'system32', 'temp', 'cache'},
            'size_limits': {
                'documents': 50 * 1024 * 1024,      # 50MB
                'legacy_documents': 25 * 1024 * 1024, # 25MB
                'images': 25 * 1024 * 1024,          # 25MB
                'videos': 500 * 1024 * 1024,         # 500MB
                'audio': 50 * 1024 * 1024,           # 50MB
                'unknown': 10 * 1024 * 1024          # 10MB
            },
            'safety_limits': {
                'max_files_per_folder': 1000,
                'max_processing_time': 3600,  # 1 hour
                'progress_update_interval': 25
            }
        }
    
    def _init_stats(self) -> Dict:
        """Initialize statistics tracking."""
        return {
            'files_processed': 0,
            'files_copied': 0,
            'files_skipped': 0,
            'modern_files': 0,
            'legacy_files': 0,
            'unknown_files': 0,
            'total_size': 0,
            'errors': [],
            'start_time': None,
            'end_time': None
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('MigrationEngine')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def detect_storage_devices(self) -> List[Dict]:
        """
        Detect available storage devices for backup.
        
        Returns:
            List of dictionaries containing device information
        """
        devices = []
        
        # Check common drive letters on Windows
        for letter in 'DEFGHIJKLMNOPQRSTUVWXYZ':
            drive_path = f"{letter}:\\"
            if os.path.exists(drive_path):
                try:
                    # Test write access
                    test_file = os.path.join(drive_path, '.migration_test')
                    with open(test_file, 'w') as f:
                        f.write('test')
                    os.remove(test_file)
                    
                    # Get drive information
                    total, used, free = shutil.disk_usage(drive_path)
                    
                    devices.append({
                        'path': drive_path,
                        'letter': letter,
                        'total_gb': total / (1024**3),
                        'free_gb': free / (1024**3),
                        'used_gb': used / (1024**3)
                    })
                    
                except (PermissionError, OSError):
                    continue
        
        self.logger.info(f"Detected {len(devices)} storage devices")
        return devices
    
    def categorize_file(self, file_path: str) -> Tuple[Optional[str], str]:
        """
        Categorize file based on extension and content.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (category, file_type) where file_type is 'modern', 'legacy', or 'unknown'
        """
        file_ext = Path(file_path).suffix.lower()
        
        # Skip system files
        if file_ext in self.config['skip_extensions']:
            return None, 'system'
        
        # Check modern formats
        for category, extensions in self.config['modern_formats'].items():
            if file_ext in extensions:
                return category, 'modern'
        
        # Check legacy formats
        for category, extensions in self.config['legacy_formats'].items():
            if file_ext in extensions:
                return category, 'legacy'
        
        # Unknown but potentially valuable file
        if file_ext and len(file_ext) <= 5:
            return 'unknown', 'unknown'
        
        return None, 'skip'
    
    def should_skip_folder(self, folder_path: str) -> bool:
        """Check if folder should be skipped based on configuration."""
        folder_lower = folder_path.lower()
        return any(skip_folder in folder_lower for skip_folder in self.config['skip_folders'])
    
    def validate_file_size(self, file_path: str, category: str) -> Tuple[bool, int]:
        """
        Validate file size against category limits.
        
        Args:
            file_path: Path to the file
            category: File category
            
        Returns:
            Tuple of (is_valid, file_size)
        """
        try:
            file_size = os.path.getsize(file_path)
            max_size = self.config['size_limits'].get(category, self.config['size_limits']['unknown'])
            return file_size <= max_size, file_size
        except OSError:
            return False, 0
    
    def calculate_file_hash(self, file_path: str) -> Optional[str]:
        """Calculate MD5 hash of file for integrity verification."""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Could not hash file {file_path}: {e}")
            return None
    
    def create_backup_structure(self, destination: str, backup_name: str = None) -> str:
        """
        Create organized backup folder structure.
        
        Args:
            destination: Base destination path
            backup_name: Optional custom backup name
            
        Returns:
            Path to created backup folder
        """
        if not backup_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"Migration_Backup_{timestamp}"
        
        backup_path = os.path.join(destination, backup_name)
        
        # Create main backup folder
        os.makedirs(backup_path, exist_ok=True)
        
        # Create category folders
        categories = [
            'Documents', 'Legacy_Documents',
            'Images', 'Legacy_Images',
            'Videos', 'Legacy_Videos', 
            'Audio', 'Legacy_Audio',
            'Archives', 'Legacy_Archives',
            'Unknown_Files'
        ]
        
        for category in categories:
            category_path = os.path.join(backup_path, category)
            os.makedirs(category_path, exist_ok=True)
        
        # Create backup manifest
        self._create_backup_manifest(backup_path)
        
        self.logger.info(f"Created backup structure at {backup_path}")
        return backup_path
    
    def _create_backup_manifest(self, backup_path: str):
        """Create backup manifest with metadata."""
        manifest = {
            'created': datetime.now().isoformat(),
            'backup_type': 'digital_migration',
            'engine_version': '1.0.0',
            'configuration': self.config,
            'source_computer': os.getenv('COMPUTERNAME', 'Unknown'),
            'user': os.getlogin(),
            'files': {}
        }
        
        manifest_path = os.path.join(backup_path, 'backup_manifest.json')
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
    
    def migrate_folder(self, source: str, destination: str, folder_name: str) -> Dict:
        """
        Migrate files from source folder to destination with intelligent filtering.
        
        Args:
            source: Source folder path
            destination: Destination backup path
            folder_name: Name of folder being processed
            
        Returns:
            Dictionary with migration statistics
        """
        if not os.path.exists(source):
            self.logger.warning(f"Source folder not found: {source}")
            return {'files_processed': 0, 'files_copied': 0, 'errors': []}
        
        self.logger.info(f"Processing folder: {folder_name}")
        
        folder_stats = {
            'files_processed': 0,
            'files_copied': 0,
            'files_skipped': 0,
            'total_size': 0,
            'categories': {},
            'errors': []
        }
        
        try:
            for root, dirs, files in os.walk(source):
                # Skip system directories
                dirs[:] = [d for d in dirs if not self.should_skip_folder(os.path.join(root, d))]
                
                # Safety limit
                if folder_stats['files_processed'] >= self.config['safety_limits']['max_files_per_folder']:
                    self.logger.warning(f"Reached file limit for {folder_name}")
                    break
                
                for file in files:
                    file_path = os.path.join(root, file)
                    folder_stats['files_processed'] += 1
                    
                    try:
                        # Categorize file
                        category, file_type = self.categorize_file(file_path)
                        
                        if not category:
                            folder_stats['files_skipped'] += 1
                            continue
                        
                        # Validate file size
                        size_valid, file_size = self.validate_file_size(file_path, category)
                        if not size_valid:
                            folder_stats['files_skipped'] += 1
                            continue
                        
                        # Determine destination folder
                        if file_type == 'legacy':
                            dest_category = f"Legacy_{category.title()}"
                        elif file_type == 'unknown':
                            dest_category = "Unknown_Files"
                        else:
                            dest_category = category.title()
                        
                        # Copy file
                        filename = os.path.basename(file_path)
                        dest_folder = os.path.join(destination, dest_category)
                        dest_file = os.path.join(dest_folder, filename)
                        
                        if not os.path.exists(dest_file):
                            shutil.copy2(file_path, dest_file)
                            
                            # Update statistics
                            folder_stats['files_copied'] += 1
                            folder_stats['total_size'] += file_size
                            folder_stats['categories'][dest_category] = folder_stats['categories'].get(dest_category, 0) + 1
                            
                            # Progress update
                            if folder_stats['files_copied'] % self.config['safety_limits']['progress_update_interval'] == 0:
                                self.logger.info(f"  Copied {folder_stats['files_copied']} files from {folder_name}")
                        else:
                            folder_stats['files_skipped'] += 1
                    
                    except Exception as e:
                        error_msg = f"Error processing {file}: {str(e)}"
                        folder_stats['errors'].append(error_msg)
                        self.logger.error(error_msg)
        
        except Exception as e:
            error_msg = f"Error processing folder {folder_name}: {str(e)}"
            folder_stats['errors'].append(error_msg)
            self.logger.error(error_msg)
        
        self.logger.info(f"Completed {folder_name}: {folder_stats['files_copied']} files copied, "
                        f"{folder_stats['total_size'] / (1024**2):.1f} MB")
        
        return folder_stats
    
    def run_migration(self, source_folders: Dict[str, str], destination: str, 
                     backup_name: str = None) -> Dict:
        """
        Run complete migration process.
        
        Args:
            source_folders: Dictionary mapping folder names to paths
            destination: Destination device path
            backup_name: Optional custom backup name
            
        Returns:
            Complete migration statistics
        """
        self.stats['start_time'] = time.time()
        self.logger.info("Starting migration process")
        
        # Create backup structure
        backup_path = self.create_backup_structure(destination, backup_name)
        
        # Process each source folder
        all_folder_stats = {}
        for folder_name, folder_path in source_folders.items():
            folder_stats = self.migrate_folder(folder_path, backup_path, folder_name)
            all_folder_stats[folder_name] = folder_stats
            
            # Update global statistics
            self.stats['files_processed'] += folder_stats['files_processed']
            self.stats['files_copied'] += folder_stats['files_copied']
            self.stats['files_skipped'] += folder_stats['files_skipped']
            self.stats['total_size'] += folder_stats['total_size']
            self.stats['errors'].extend(folder_stats['errors'])
        
        self.stats['end_time'] = time.time()
        
        # Generate final report
        report = self._generate_migration_report(backup_path, all_folder_stats)
        
        self.logger.info("Migration process completed")
        return report
    
    def _generate_migration_report(self, backup_path: str, folder_stats: Dict) -> Dict:
        """Generate comprehensive migration report."""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        report = {
            'backup_location': backup_path,
            'migration_summary': {
                'total_files_processed': self.stats['files_processed'],
                'total_files_copied': self.stats['files_copied'],
                'total_files_skipped': self.stats['files_skipped'],
                'total_size_gb': self.stats['total_size'] / (1024**3),
                'duration_minutes': duration / 60,
                'success_rate': (self.stats['files_copied'] / max(self.stats['files_processed'], 1)) * 100
            },
            'folder_breakdown': folder_stats,
            'errors': self.stats['errors'],
            'timestamp': datetime.now().isoformat()
        }
        
        # Save report
        report_path = os.path.join(backup_path, 'migration_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report


def main():
    """Example usage of MigrationEngine."""
    # Initialize migration engine
    engine = MigrationEngine()
    
    # Detect storage devices
    devices = engine.detect_storage_devices()
    if not devices:
        print("No storage devices detected!")
        return
    
    print("Available storage devices:")
    for i, device in enumerate(devices):
        print(f"  {i+1}. {device['path']} ({device['free_gb']:.1f} GB free)")
    
    # Select device (in real usage, this would be user input)
    selected_device = devices[0]['path']
    
    # Define source folders
    user_profile = os.path.expanduser("~")
    source_folders = {
        'Documents': os.path.join(user_profile, 'Documents'),
        'Pictures': os.path.join(user_profile, 'Pictures'),
        'Videos': os.path.join(user_profile, 'Videos'),
        'Music': os.path.join(user_profile, 'Music'),
        'Downloads': os.path.join(user_profile, 'Downloads'),
        'Desktop': os.path.join(user_profile, 'Desktop')
    }
    
    # Run migration
    report = engine.run_migration(source_folders, selected_device)
    
    # Display results
    print("\nMigration completed!")
    print(f"Files copied: {report['migration_summary']['total_files_copied']:,}")
    print(f"Total size: {report['migration_summary']['total_size_gb']:.2f} GB")
    print(f"Duration: {report['migration_summary']['duration_minutes']:.1f} minutes")
    print(f"Success rate: {report['migration_summary']['success_rate']:.1f}%")
    print(f"Backup location: {report['backup_location']}")


if __name__ == "__main__":
    main()