"""
File Validator Module
Comprehensive file validation, search, and filtering capabilities.

Classes:
    FileValidator: Validates files based on multiple criteria
    FileSearcher: Searches for files with advanced filtering
    FileFilter: Filters files based on various conditions
    FileAnalyzer: Analyzes file properties and metadata
"""

import os
import re
import mimetypes
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union, Callable, Set
from dataclasses import dataclass
from datetime import datetime
import hashlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """Data class to store file information."""
    path: str
    name: str
    extension: str
    size: int
    created_time: float
    modified_time: float
    mime_type: Optional[str]
    is_dir: bool
    is_file: bool
    exists: bool
    readable: bool
    writable: bool


class FileValidator:
    """
    Comprehensive file validation class.
    
    Validates files based on:
    - Existence and accessibility
    - File type and extension
    - Size constraints
    - Path patterns
    - Content validation
    """
    
    def __init__(self, allowed_extensions: Optional[Set[str]] = None,
                 max_file_size: Optional[int] = None,
                 min_file_size: int = 0):
        """
        Initialize FileValidator.
        
        Args:
            allowed_extensions: Set of allowed file extensions (without dots)
            max_file_size: Maximum allowed file size in bytes
            min_file_size: Minimum allowed file size in bytes
        """
        self.allowed_extensions = allowed_extensions or set()
        self.max_file_size = max_file_size
        self.min_file_size = min_file_size
    
    def validate_file_exists(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file exists.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not os.path.exists(file_path):
            return False, f"File does not exist: {file_path}"
        return True, "File exists"
    
    def validate_is_file(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if path points to a file (not directory).
        
        Args:
            file_path: Path to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not os.path.isfile(file_path):
            return False, f"Path is not a file: {file_path}"
        return True, "Path is a valid file"
    
    def validate_extension(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate file extension against allowed extensions.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not self.allowed_extensions:
            return True, "No extension restrictions"
        
        _, ext = os.path.splitext(file_path)
        ext = ext.lstrip('.').lower()
        
        if ext not in self.allowed_extensions:
            return False, f"Extension '{ext}' not allowed. Allowed: {self.allowed_extensions}"
        return True, f"Extension '{ext}' is valid"
    
    def validate_file_size(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate file size constraints.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            file_size = os.path.getsize(file_path)
        except OSError as e:
            return False, f"Cannot read file size: {e}"
        
        if file_size < self.min_file_size:
            return False, f"File size ({file_size} bytes) is below minimum ({self.min_file_size} bytes)"
        
        if self.max_file_size and file_size > self.max_file_size:
            return False, f"File size ({file_size} bytes) exceeds maximum ({self.max_file_size} bytes)"
        
        return True, f"File size ({file_size} bytes) is valid"
    
    def validate_readability(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file is readable.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not os.access(file_path, os.R_OK):
            return False, f"File is not readable: {file_path}"
        return True, "File is readable"
    
    def validate_writable(self, file_path: str) -> Tuple[bool, str]:
        """
        Validate if file is writable.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not os.access(file_path, os.W_OK):
            return False, f"File is not writable: {file_path}"
        return True, "File is writable"
    
    def validate_path_pattern(self, file_path: str, pattern: str) -> Tuple[bool, str]:
        """
        Validate file path against regex pattern.
        
        Args:
            file_path: Path to validate
            pattern: Regex pattern to match
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            if re.match(pattern, file_path):
                return True, f"Path matches pattern: {pattern}"
            return False, f"Path does not match pattern: {pattern}"
        except re.error as e:
            return False, f"Invalid regex pattern: {e}"
    
    def validate_checksum(self, file_path: str, expected_checksum: str,
                         algorithm: str = 'md5') -> Tuple[bool, str]:
        """
        Validate file checksum.
        
        Args:
            file_path: Path to the file
            expected_checksum: Expected checksum value
            algorithm: Hash algorithm (md5, sha1, sha256)
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            hash_obj = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_obj.update(chunk)
            actual_checksum = hash_obj.hexdigest()
            
            if actual_checksum == expected_checksum:
                return True, f"Checksum matches ({algorithm})"
            return False, f"Checksum mismatch. Expected: {expected_checksum}, Got: {actual_checksum}"
        except Exception as e:
            return False, f"Error validating checksum: {e}"
    
    def validate_complete(self, file_path: str, check_readability: bool = True,
                         check_writable: bool = False) -> Dict[str, Tuple[bool, str]]:
        """
        Perform comprehensive validation.
        
        Args:
            file_path: Path to validate
            check_readability: Whether to check readability
            check_writable: Whether to check writeability
            
        Returns:
            Dictionary of validation results
        """
        results = {
            'exists': self.validate_file_exists(file_path),
            'is_file': self.validate_is_file(file_path),
            'extension': self.validate_extension(file_path),
            'size': self.validate_file_size(file_path),
        }
        
        if check_readability:
            results['readable'] = self.validate_readability(file_path)
        if check_writable:
            results['writable'] = self.validate_writable(file_path)
        
        return results


class FileSearcher:
    """
    Advanced file search functionality.
    
    Features:
    - Search by name, extension, size
    - Recursive directory search
    - Pattern matching
    - Content search
    """
    
    def __init__(self, root_path: str):
        """
        Initialize FileSearcher.
        
        Args:
            root_path: Root directory for search operations
        """
        self.root_path = root_path
        if not os.path.isdir(root_path):
            raise ValueError(f"Root path must be a directory: {root_path}")
    
    def search_by_name(self, name_pattern: str, recursive: bool = True) -> List[str]:
        """
        Search files by name pattern.
        
        Args:
            name_pattern: Glob pattern or exact name
            recursive: Whether to search recursively
            
        Returns:
            List of matching file paths
        """
        results = []
        try:
            if recursive:
                results = list(Path(self.root_path).rglob(name_pattern))
            else:
                results = list(Path(self.root_path).glob(name_pattern))
            return [str(p) for p in results if p.is_file()]
        except Exception as e:
            logger.error(f"Error searching by name: {e}")
            return []
    
    def search_by_extension(self, extension: str, recursive: bool = True) -> List[str]:
        """
        Search files by extension.
        
        Args:
            extension: File extension (with or without dot)
            recursive: Whether to search recursively
            
        Returns:
            List of matching file paths
        """
        if not extension.startswith('.'):
            extension = '.' + extension
        
        pattern = f"*{extension}"
        return self.search_by_name(pattern, recursive)
    
    def search_by_size(self, min_size: int = 0, max_size: Optional[int] = None,
                      recursive: bool = True) -> List[str]:
        """
        Search files by size.
        
        Args:
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            recursive: Whether to search recursively
            
        Returns:
            List of matching file paths
        """
        results = []
        try:
            pattern = "**/*" if recursive else "*"
            for file_path in Path(self.root_path).glob(pattern):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    if min_size <= size:
                        if max_size is None or size <= max_size:
                            results.append(str(file_path))
        except Exception as e:
            logger.error(f"Error searching by size: {e}")
        
        return results
    
    def search_by_regex(self, pattern: str, recursive: bool = True) -> List[str]:
        """
        Search files by regex pattern.
        
        Args:
            pattern: Regex pattern to match against file names
            recursive: Whether to search recursively
            
        Returns:
            List of matching file paths
        """
        results = []
        try:
            compiled_pattern = re.compile(pattern)
            glob_pattern = "**/*" if recursive else "*"
            
            for file_path in Path(self.root_path).glob(glob_pattern):
                if file_path.is_file() and compiled_pattern.search(file_path.name):
                    results.append(str(file_path))
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")
        except Exception as e:
            logger.error(f"Error searching by regex: {e}")
        
        return results
    
    def search_by_content(self, content_pattern: str, extensions: Optional[List[str]] = None,
                         recursive: bool = True, case_sensitive: bool = False) -> List[str]:
        """
        Search files by content.
        
        Args:
            content_pattern: Pattern to search in file content
            extensions: List of file extensions to search (None = all)
            recursive: Whether to search recursively
            case_sensitive: Whether search is case-sensitive
            
        Returns:
            List of matching file paths
        """
        results = []
        flags = 0 if case_sensitive else re.IGNORECASE
        
        try:
            compiled_pattern = re.compile(content_pattern, flags)
            glob_pattern = "**/*" if recursive else "*"
            
            for file_path in Path(self.root_path).glob(glob_pattern):
                if not file_path.is_file():
                    continue
                
                if extensions and file_path.suffix.lstrip('.') not in extensions:
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        if compiled_pattern.search(content):
                            results.append(str(file_path))
                except Exception:
                    pass
        
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")
        except Exception as e:
            logger.error(f"Error searching by content: {e}")
        
        return results
    
    def search_modified_after(self, timestamp: float, recursive: bool = True) -> List[str]:
        """
        Search files modified after timestamp.
        
        Args:
            timestamp: Unix timestamp
            recursive: Whether to search recursively
            
        Returns:
            List of matching file paths
        """
        results = []
        try:
            glob_pattern = "**/*" if recursive else "*"
            for file_path in Path(self.root_path).glob(glob_pattern):
                if file_path.is_file() and file_path.stat().st_mtime > timestamp:
                    results.append(str(file_path))
        except Exception as e:
            logger.error(f"Error searching by modification time: {e}")
        
        return results


class FileFilter:
    """
    Advanced file filtering capabilities.
    
    Features:
    - Filter by multiple criteria
    - Custom filter functions
    - Chainable filters
    - Exclude patterns
    """
    
    def __init__(self, files: List[str]):
        """
        Initialize FileFilter.
        
        Args:
            files: List of file paths to filter
        """
        self.original_files = files
        self.files = files.copy()
    
    def filter_by_extension(self, extensions: Union[str, List[str]],
                           exclude: bool = False) -> 'FileFilter':
        """
        Filter files by extension.
        
        Args:
            extensions: Extension(s) to filter
            exclude: Whether to exclude matching files
            
        Returns:
            Self for chaining
        """
        if isinstance(extensions, str):
            extensions = [extensions]
        
        extensions = {ext.lstrip('.').lower() for ext in extensions}
        
        def filter_func(file_path):
            ext = os.path.splitext(file_path)[1].lstrip('.').lower()
            matches = ext in extensions
            return not matches if exclude else matches
        
        self.files = [f for f in self.files if filter_func(f)]
        return self
    
    def filter_by_size(self, min_size: int = 0, max_size: Optional[int] = None) -> 'FileFilter':
        """
        Filter files by size.
        
        Args:
            min_size: Minimum file size in bytes
            max_size: Maximum file size in bytes
            
        Returns:
            Self for chaining
        """
        def filter_func(file_path):
            try:
                size = os.path.getsize(file_path)
                return min_size <= size and (max_size is None or size <= max_size)
            except OSError:
                return False
        
        self.files = [f for f in self.files if filter_func(f)]
        return self
    
    def filter_by_pattern(self, pattern: str, exclude: bool = False) -> 'FileFilter':
        """
        Filter files by regex pattern.
        
        Args:
            pattern: Regex pattern
            exclude: Whether to exclude matching files
            
        Returns:
            Self for chaining
        """
        try:
            compiled = re.compile(pattern)
            
            def filter_func(file_path):
                matches = bool(compiled.search(os.path.basename(file_path)))
                return not matches if exclude else matches
            
            self.files = [f for f in self.files if filter_func(f)]
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")
        
        return self
    
    def filter_by_custom(self, func: Callable[[str], bool]) -> 'FileFilter':
        """
        Filter files using custom function.
        
        Args:
            func: Function that returns True for files to keep
            
        Returns:
            Self for chaining
        """
        self.files = [f for f in self.files if func(f)]
        return self
    
    def filter_readable(self) -> 'FileFilter':
        """
        Filter to only readable files.
        
        Returns:
            Self for chaining
        """
        self.files = [f for f in self.files if os.access(f, os.R_OK)]
        return self
    
    def filter_exists(self) -> 'FileFilter':
        """
        Filter to only existing files.
        
        Returns:
            Self for chaining
        """
        self.files = [f for f in self.files if os.path.exists(f)]
        return self
    
    def reset(self) -> 'FileFilter':
        """
        Reset to original file list.
        
        Returns:
            Self for chaining
        """
        self.files = self.original_files.copy()
        return self
    
    def get_results(self) -> List[str]:
        """
        Get filtered results.
        
        Returns:
            List of filtered file paths
        """
        return self.files.copy()
    
    def count(self) -> int:
        """Get count of filtered files."""
        return len(self.files)


class FileAnalyzer:
    """
    File analysis and metadata extraction.
    
    Features:
    - File information gathering
    - Mime type detection
    - Statistical analysis
    - Batch analysis
    """
    
    @staticmethod
    def get_file_info(file_path: str) -> Optional[FileInfo]:
        """
        Get comprehensive file information.
        
        Args:
            file_path: Path to the file
            
        Returns:
            FileInfo object or None if file doesn't exist
        """
        try:
            if not os.path.exists(file_path):
                return None
            
            stat = os.stat(file_path)
            mime_type, _ = mimetypes.guess_type(file_path)
            
            return FileInfo(
                path=file_path,
                name=os.path.basename(file_path),
                extension=os.path.splitext(file_path)[1].lstrip('.'),
                size=stat.st_size,
                created_time=stat.st_ctime,
                modified_time=stat.st_mtime,
                mime_type=mime_type,
                is_dir=os.path.isdir(file_path),
                is_file=os.path.isfile(file_path),
                exists=True,
                readable=os.access(file_path, os.R_OK),
                writable=os.access(file_path, os.W_OK),
            )
        except Exception as e:
            logger.error(f"Error analyzing file {file_path}: {e}")
            return None
    
    @staticmethod
    def analyze_multiple(file_paths: List[str]) -> List[FileInfo]:
        """
        Analyze multiple files.
        
        Args:
            file_paths: List of file paths
            
        Returns:
            List of FileInfo objects
        """
        results = []
        for file_path in file_paths:
            info = FileAnalyzer.get_file_info(file_path)
            if info:
                results.append(info)
        return results
    
    @staticmethod
    def get_directory_stats(directory: str, recursive: bool = True) -> Dict:
        """
        Get directory statistics.
        
        Args:
            directory: Directory path
            recursive: Whether to analyze recursively
            
        Returns:
            Dictionary with statistics
        """
        if not os.path.isdir(directory):
            return {}
        
        stats = {
            'total_files': 0,
            'total_size': 0,
            'by_extension': {},
            'largest_file': None,
            'largest_file_size': 0,
            'smallest_file': None,
            'smallest_file_size': float('inf'),
        }
        
        try:
            pattern = "**/*" if recursive else "*"
            for file_path in Path(directory).glob(pattern):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    ext = file_path.suffix.lstrip('.')
                    
                    stats['total_files'] += 1
                    stats['total_size'] += size
                    
                    if ext not in stats['by_extension']:
                        stats['by_extension'][ext] = {'count': 0, 'size': 0}
                    stats['by_extension'][ext]['count'] += 1
                    stats['by_extension'][ext]['size'] += size
                    
                    if size > stats['largest_file_size']:
                        stats['largest_file'] = str(file_path)
                        stats['largest_file_size'] = size
                    
                    if size < stats['smallest_file_size']:
                        stats['smallest_file'] = str(file_path)
                        stats['smallest_file_size'] = size
        
        except Exception as e:
            logger.error(f"Error analyzing directory: {e}")
        
        return stats
    
    @staticmethod
    def group_by_extension(file_paths: List[str]) -> Dict[str, List[str]]:
        """
        Group files by extension.
        
        Args:
            file_paths: List of file paths
            
        Returns:
            Dictionary with extensions as keys
        """
        grouped = {}
        for file_path in file_paths:
            ext = os.path.splitext(file_path)[1].lstrip('.')
            if not ext:
                ext = 'no_extension'
            
            if ext not in grouped:
                grouped[ext] = []
            grouped[ext].append(file_path)
        
        return grouped
    
    @staticmethod
    def group_by_size_range(file_paths: List[str],
                           ranges: Optional[List[Tuple[int, int]]] = None) -> Dict[str, List[str]]:
        """
        Group files by size ranges.
        
        Args:
            file_paths: List of file paths
            ranges: List of (min, max) size tuples in bytes
            
        Returns:
            Dictionary with size ranges as keys
        """
        if ranges is None:
            ranges = [
                (0, 1024),                    # 0 - 1 KB
                (1024, 1024 * 100),           # 1 KB - 100 KB
                (1024 * 100, 1024 * 1024),    # 100 KB - 1 MB
                (1024 * 1024, 1024 * 1024 * 100),  # 1 MB - 100 MB
                (1024 * 1024 * 100, float('inf')),  # > 100 MB
            ]
        
        grouped = {}
        for file_path in file_paths:
            try:
                size = os.path.getsize(file_path)
                for min_s, max_s in ranges:
                    if min_s <= size < max_s:
                        key = f"{min_s}-{max_s}"
                        if key not in grouped:
                            grouped[key] = []
                        grouped[key].append(file_path)
                        break
            except OSError:
                pass
        
        return grouped


# Example usage and testing
if __name__ == "__main__":
    # Example: File Validation
    validator = FileValidator(
        allowed_extensions={'txt', 'py', 'md'},
        max_file_size=10 * 1024 * 1024,  # 10 MB
    )
    
    # Validate a file
    test_file = "file_validator.py"
    if os.path.exists(test_file):
        results = validator.validate_complete(test_file)
        for check, (is_valid, message) in results.items():
            status = "✓" if is_valid else "✗"
            print(f"{status} {check}: {message}")
    
    print("\n" + "="*60 + "\n")
    
    # Example: File Analysis
    info = FileAnalyzer.get_file_info(test_file)
    if info:
        print(f"File: {info.name}")
        print(f"Size: {info.size} bytes")
        print(f"Extension: {info.extension}")
        print(f"MIME Type: {info.mime_type}")
        print(f"Readable: {info.readable}")
        print(f"Writable: {info.writable}")
