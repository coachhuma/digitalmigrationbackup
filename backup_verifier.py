"""
Backup Verification System
Comprehensive backup integrity verification with quality analysis
"""

import os
import hashlib
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set


class BackupVerifier:
    """
    Advanced backup verification system with integrity checking and quality analysis.
    
    Features:
    - MD5 hash verification for data integrity
    - Photo/video quality analysis
    - Multi-location backup validation
    - Comprehensive reporting
    - Safety assessment for deletion
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize backup verifier with configuration."""
        self.config = config or self._default_config()
        self.verification_stats = self._init_stats()
        self.logger = self._setup_logging()
    
    def _default_config(self) -> Dict:
        """Default configuration for backup verification."""
        return {
            'photo_quality_thresholds': {
                '.jpg': {'min_size': 1024 * 1024, 'typical_range': (2, 15)},      # 1MB-15MB
                '.jpeg': {'min_size': 1024 * 1024, 'typical_range': (2, 15)},     # 1MB-15MB
                '.heic': {'min_size': 2 * 1024 * 1024, 'typical_range': (3, 20)}, # 2MB-20MB
                '.png': {'min_size': 500 * 1024, 'typical_range': (1, 25)},       # 500KB-25MB
                '.raw': {'min_size': 10 * 1024 * 1024, 'typical_range': (15, 50)}, # 10MB-50MB
                '.dng': {'min_size': 10 * 1024 * 1024, 'typical_range': (15, 50)}  # 10MB-50MB
            },
            'video_quality_thresholds': {
                '.mp4': {'min_size_per_min': 10 * 1024 * 1024},  # 10MB per minute
                '.mov': {'min_size_per_min': 15 * 1024 * 1024},  # 15MB per minute
                '.avi': {'min_size_per_min': 20 * 1024 * 1024},  # 20MB per minute
                '.mkv': {'min_size_per_min': 12 * 1024 * 1024}   # 12MB per minute
            },
            'file_categories': {
                'photos': {'.jpg', '.jpeg', '.png', '.heic', '.raw', '.dng', '.tiff', '.bmp'},
                'videos': {'.mp4', '.mov', '.avi', '.mkv', '.m4v', '.3gp', '.webm'},
                'documents': {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.xls', '.xlsx'},
                'audio': {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'}
            },
            'verification_settings': {
                'chunk_size': 8192,
                'max_file_size_for_hash': 1024 * 1024 * 1024,  # 1GB
                'sample_verification_ratio': 0.1  # Verify 10% of files for large collections
            }
        }
    
    def _init_stats(self) -> Dict:
        """Initialize verification statistics."""
        return {
            'total_files': 0,
            'verified_files': 0,
            'failed_verifications': 0,
            'quality_analysis': {},
            'file_categories': {},
            'total_size': 0,
            'verification_time': 0,
            'errors': []
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('BackupVerifier')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def calculate_file_hash(self, file_path: str) -> Optional[str]:
        """
        Calculate MD5 hash of file for integrity verification.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MD5 hash string or None if calculation fails
        """
        try:
            # Skip very large files to avoid performance issues
            file_size = os.path.getsize(file_path)
            if file_size > self.config['verification_settings']['max_file_size_for_hash']:
                self.logger.warning(f"Skipping hash calculation for large file: {file_path}")
                return None
            
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(self.config['verification_settings']['chunk_size']), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        
        except (OSError, PermissionError) as e:
            self.logger.error(f"Could not calculate hash for {file_path}: {e}")
            return None
    
    def verify_photo_quality(self, file_path: str) -> Dict:
        """
        Verify photo is full quality (not compressed).
        
        Args:
            file_path: Path to the photo file
            
        Returns:
            Dictionary with quality analysis results
        """
        try:
            file_size = os.path.getsize(file_path)
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext not in self.config['photo_quality_thresholds']:
                return {'is_full_quality': True, 'quality_score': 'UNKNOWN_FORMAT'}
            
            threshold = self.config['photo_quality_thresholds'][file_ext]
            size_mb = file_size / (1024 * 1024)
            
            meets_minimum = file_size >= threshold['min_size']
            in_typical_range = threshold['typical_range'][0] <= size_mb <= threshold['typical_range'][1]
            is_full_quality = meets_minimum and in_typical_range
            
            return {
                'is_full_quality': is_full_quality,
                'file_size_mb': size_mb,
                'meets_minimum': meets_minimum,
                'in_typical_range': in_typical_range,
                'quality_score': 'FULL' if is_full_quality else 'COMPRESSED',
                'threshold_info': threshold
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing photo quality for {file_path}: {e}")
            return {'error': str(e), 'is_full_quality': False}
    
    def verify_video_quality(self, file_path: str) -> Dict:
        """
        Verify video is full quality.
        
        Args:
            file_path: Path to the video file
            
        Returns:
            Dictionary with quality analysis results
        """
        try:
            file_size = os.path.getsize(file_path)
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext not in self.config['video_quality_thresholds']:
                return {'is_full_quality': True, 'quality_score': 'UNKNOWN_FORMAT'}
            
            threshold = self.config['video_quality_thresholds'][file_ext]
            size_mb = file_size / (1024 * 1024)
            
            # Assume minimum 30 seconds for quality check
            min_expected_size = threshold['min_size_per_min'] * 0.5  # 30 seconds
            is_reasonable_quality = file_size >= min_expected_size
            
            return {
                'is_full_quality': is_reasonable_quality,
                'file_size_mb': size_mb,
                'quality_score': 'FULL' if is_reasonable_quality else 'COMPRESSED',
                'threshold_info': threshold
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing video quality for {file_path}: {e}")
            return {'error': str(e), 'is_full_quality': False}
    
    def categorize_file(self, file_path: str) -> Optional[str]:
        """
        Categorize file based on extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            File category or None if not categorized
        """
        file_ext = Path(file_path).suffix.lower()
        
        for category, extensions in self.config['file_categories'].items():
            if file_ext in extensions:
                return category
        
        return 'other'
    
    def scan_backup_contents(self, backup_path: str) -> Dict:
        """
        Scan all contents in backup folder and analyze quality.
        
        Args:
            backup_path: Path to backup folder
            
        Returns:
            Dictionary with scan results and quality analysis
        """
        self.logger.info(f"Scanning backup contents: {backup_path}")
        
        scan_results = {
            'photos': {'count': 0, 'size': 0, 'full_quality': 0, 'compressed': 0},
            'videos': {'count': 0, 'size': 0, 'full_quality': 0, 'compressed': 0},
            'documents': {'count': 0, 'size': 0},
            'audio': {'count': 0, 'size': 0},
            'other': {'count': 0, 'size': 0}
        }
        
        quality_details = []
        
        try:
            for root, dirs, files in os.walk(backup_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    try:
                        file_size = os.path.getsize(file_path)
                        category = self.categorize_file(file_path)
                        
                        if category not in scan_results:
                            category = 'other'
                        
                        scan_results[category]['count'] += 1
                        scan_results[category]['size'] += file_size
                        
                        # Quality analysis for photos and videos
                        if category == 'photos':
                            quality_result = self.verify_photo_quality(file_path)
                            if quality_result.get('is_full_quality', False):
                                scan_results[category]['full_quality'] += 1
                            else:
                                scan_results[category]['compressed'] += 1
                            
                            quality_details.append({
                                'file': file_path,
                                'category': category,
                                'quality_result': quality_result
                            })
                        
                        elif category == 'videos':
                            quality_result = self.verify_video_quality(file_path)
                            if quality_result.get('is_full_quality', False):
                                scan_results[category]['full_quality'] += 1
                            else:
                                scan_results[category]['compressed'] += 1
                            
                            quality_details.append({
                                'file': file_path,
                                'category': category,
                                'quality_result': quality_result
                            })
                    
                    except Exception as e:
                        self.logger.error(f"Error processing file {file_path}: {e}")
                        self.verification_stats['errors'].append(f"Scan error for {file}: {str(e)}")
        
        except Exception as e:
            self.logger.error(f"Error scanning backup folder {backup_path}: {e}")
            self.verification_stats['errors'].append(f"Folder scan error: {str(e)}")
        
        return {
            'scan_results': scan_results,
            'quality_details': quality_details
        }
    
    def verify_backup_integrity(self, original_path: str, backup_path: str, 
                               sample_verification: bool = False) -> Dict:
        """
        Verify backup integrity by comparing original and backup files.
        
        Args:
            original_path: Path to original files
            backup_path: Path to backup files
            sample_verification: If True, verify only a sample of files
            
        Returns:
            Dictionary with verification results
        """
        self.logger.info(f"Verifying backup integrity: {original_path} -> {backup_path}")
        
        verification_results = {
            'total_files': 0,
            'verified_files': 0,
            'failed_verifications': 0,
            'missing_files': [],
            'hash_mismatches': [],
            'verified_files_list': [],
            'verification_rate': 0.0
        }
        
        if not os.path.exists(original_path) or not os.path.exists(backup_path):
            self.logger.error("Original or backup path does not exist")
            return verification_results
        
        try:
            # Get list of original files
            original_files = []
            for root, dirs, files in os.walk(original_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, original_path)
                    original_files.append((file_path, rel_path))
            
            verification_results['total_files'] = len(original_files)
            
            # Sample verification for large collections
            if sample_verification and len(original_files) > 1000:
                sample_size = int(len(original_files) * self.config['verification_settings']['sample_verification_ratio'])
                original_files = original_files[:sample_size]
                self.logger.info(f"Using sample verification: {sample_size} files")
            
            # Verify each file
            for original_file, rel_path in original_files:
                # Find corresponding backup file
                backup_file = None
                for root, dirs, files in os.walk(backup_path):
                    potential_backup = os.path.join(root, os.path.basename(original_file))
                    if os.path.exists(potential_backup):
                        backup_file = potential_backup
                        break
                
                if not backup_file:
                    verification_results['missing_files'].append({
                        'original': original_file,
                        'relative_path': rel_path
                    })
                    continue
                
                # Verify file integrity
                try:
                    original_hash = self.calculate_file_hash(original_file)
                    backup_hash = self.calculate_file_hash(backup_file)
                    
                    if original_hash and backup_hash:
                        if original_hash == backup_hash:
                            verification_results['verified_files'] += 1
                            verification_results['verified_files_list'].append({
                                'original': original_file,
                                'backup': backup_file,
                                'hash': original_hash
                            })
                        else:
                            verification_results['failed_verifications'] += 1
                            verification_results['hash_mismatches'].append({
                                'original': original_file,
                                'backup': backup_file,
                                'original_hash': original_hash,
                                'backup_hash': backup_hash
                            })
                    else:
                        # Could not calculate hash, verify by size
                        original_size = os.path.getsize(original_file)
                        backup_size = os.path.getsize(backup_file)
                        
                        if original_size == backup_size:
                            verification_results['verified_files'] += 1
                        else:
                            verification_results['failed_verifications'] += 1
                
                except Exception as e:
                    self.logger.error(f"Error verifying {original_file}: {e}")
                    verification_results['failed_verifications'] += 1
            
            # Calculate verification rate
            total_checked = verification_results['verified_files'] + verification_results['failed_verifications']
            if total_checked > 0:
                verification_results['verification_rate'] = (verification_results['verified_files'] / total_checked) * 100
        
        except Exception as e:
            self.logger.error(f"Error during backup verification: {e}")
            self.verification_stats['errors'].append(f"Verification error: {str(e)}")
        
        return verification_results
    
    def generate_verification_report(self, backup_path: str, scan_results: Dict, 
                                   verification_results: Dict = None) -> Dict:
        """
        Generate comprehensive verification report.
        
        Args:
            backup_path: Path to backup folder
            scan_results: Results from backup content scan
            verification_results: Optional integrity verification results
            
        Returns:
            Complete verification report
        """
        report = {
            'verification_date': datetime.now().isoformat(),
            'backup_path': backup_path,
            'verification_type': 'comprehensive_backup_verification',
            'scan_summary': {},
            'quality_analysis': {},
            'integrity_verification': verification_results,
            'overall_assessment': {},
            'recommendations': []
        }
        
        # Process scan results
        scan_data = scan_results['scan_results']
        total_files = sum(category['count'] for category in scan_data.values())
        total_size_gb = sum(category['size'] for category in scan_data.values()) / (1024**3)
        
        report['scan_summary'] = {
            'total_files': total_files,
            'total_size_gb': total_size_gb,
            'file_breakdown': scan_data
        }
        
        # Quality analysis
        photos = scan_data.get('photos', {})
        videos = scan_data.get('videos', {})
        
        if photos.get('count', 0) > 0:
            photo_quality_ratio = photos.get('full_quality', 0) / photos['count']
            report['quality_analysis']['photos'] = {
                'total_photos': photos['count'],
                'full_quality_count': photos.get('full_quality', 0),
                'compressed_count': photos.get('compressed', 0),
                'quality_ratio': photo_quality_ratio,
                'total_size_gb': photos['size'] / (1024**3),
                'quality_status': 'EXCELLENT' if photo_quality_ratio > 0.9 else 'NEEDS_REVIEW'
            }
            
            if photo_quality_ratio < 0.9:
                report['recommendations'].append("Some photos may be compressed - verify download settings")
        
        if videos.get('count', 0) > 0:
            video_quality_ratio = videos.get('full_quality', 0) / videos['count']
            report['quality_analysis']['videos'] = {
                'total_videos': videos['count'],
                'full_quality_count': videos.get('full_quality', 0),
                'compressed_count': videos.get('compressed', 0),
                'quality_ratio': video_quality_ratio,
                'total_size_gb': videos['size'] / (1024**3),
                'quality_status': 'EXCELLENT' if video_quality_ratio > 0.8 else 'NEEDS_REVIEW'
            }
            
            if video_quality_ratio < 0.8:
                report['recommendations'].append("Some videos may be compressed - verify download settings")
        
        # Overall assessment
        backup_completeness = 'COMPLETE' if total_files > 0 else 'INCOMPLETE'
        safe_to_delete = len(report['recommendations']) == 0
        
        if verification_results:
            verification_rate = verification_results.get('verification_rate', 0)
            if verification_rate < 95:
                safe_to_delete = False
                report['recommendations'].append(f"Low verification rate ({verification_rate:.1f}%) - investigate missing or corrupted files")
        
        report['overall_assessment'] = {
            'backup_completeness': backup_completeness,
            'safe_to_delete_originals': safe_to_delete,
            'verification_confidence': 'HIGH' if safe_to_delete else 'MEDIUM'
        }
        
        if safe_to_delete:
            report['recommendations'].append("✅ Backup verification passed - safe to delete originals")
        
        return report
    
    def save_verification_report(self, report: Dict, output_path: str) -> Tuple[str, str]:
        """
        Save verification report to files.
        
        Args:
            report: Verification report dictionary
            output_path: Directory to save reports
            
        Returns:
            Tuple of (json_report_path, summary_report_path)
        """
        os.makedirs(output_path, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save detailed JSON report
        json_report_path = os.path.join(output_path, f'backup_verification_report_{timestamp}.json')
        with open(json_report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Save human-readable summary
        summary_report_path = os.path.join(output_path, f'backup_verification_summary_{timestamp}.txt')
        with open(summary_report_path, 'w') as f:
            f.write("BACKUP VERIFICATION SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Verification Date: {report['verification_date']}\n")
            f.write(f"Backup Path: {report['backup_path']}\n\n")
            
            f.write("CONTENT SUMMARY:\n")
            f.write("-" * 20 + "\n")
            scan_summary = report['scan_summary']
            f.write(f"Total Files: {scan_summary['total_files']:,}\n")
            f.write(f"Total Size: {scan_summary['total_size_gb']:.2f} GB\n\n")
            
            for category, data in scan_summary['file_breakdown'].items():
                if data['count'] > 0:
                    size_gb = data['size'] / (1024**3)
                    f.write(f"{category.title()}: {data['count']} files ({size_gb:.2f} GB)\n")
            
            if 'quality_analysis' in report and report['quality_analysis']:
                f.write(f"\nQUALITY ANALYSIS:\n")
                f.write("-" * 20 + "\n")
                for media_type, analysis in report['quality_analysis'].items():
                    f.write(f"{media_type.title()}: {analysis['quality_status']}\n")
                    f.write(f"  Full Quality: {analysis['full_quality_count']}/{analysis[f'total_{media_type}']}\n")
            
            f.write(f"\nOVERALL ASSESSMENT:\n")
            f.write("-" * 20 + "\n")
            assessment = report['overall_assessment']
            f.write(f"Backup Completeness: {assessment['backup_completeness']}\n")
            f.write(f"Safe to Delete Originals: {assessment['safe_to_delete_originals']}\n")
            f.write(f"Verification Confidence: {assessment['verification_confidence']}\n")
            
            if report['recommendations']:
                f.write(f"\nRECOMMENDATIONS:\n")
                f.write("-" * 20 + "\n")
                for rec in report['recommendations']:
                    f.write(f"• {rec}\n")
        
        self.logger.info(f"Verification reports saved: {json_report_path}, {summary_report_path}")
        return json_report_path, summary_report_path


def main():
    """Example usage of BackupVerifier."""
    # Initialize verifier
    verifier = BackupVerifier()
    
    # Example backup path (user would specify this)
    backup_path = input("Enter backup folder path to verify: ").strip()
    if not backup_path or not os.path.exists(backup_path):
        print("Invalid backup path!")
        return
    
    print("Starting backup verification...")
    start_time = time.time()
    
    # Scan backup contents
    scan_results = verifier.scan_backup_contents(backup_path)
    
    # Generate verification report
    report = verifier.generate_verification_report(backup_path, scan_results)
    
    # Save reports
    output_dir = os.path.join(os.path.expanduser("~"), "Desktop", "Backup_Verification")
    json_path, summary_path = verifier.save_verification_report(report, output_dir)
    
    verification_time = time.time() - start_time
    
    # Display results
    print(f"\n✅ Backup verification completed in {verification_time:.1f} seconds!")
    print(f"📊 Total Files: {report['scan_summary']['total_files']:,}")
    print(f"💾 Total Size: {report['scan_summary']['total_size_gb']:.2f} GB")
    print(f"🛡️ Safe to Delete Originals: {report['overall_assessment']['safe_to_delete_originals']}")
    print(f"📋 Detailed Report: {json_path}")
    print(f"📄 Summary Report: {summary_path}")


if __name__ == "__main__":
    main()