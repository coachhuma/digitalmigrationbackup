"""
Debug and Utility Tools
Comprehensive debugging and troubleshooting utilities for migration and backup systems
"""

import os
import sys
import json
import time
import psutil
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any


class SystemDiagnostics:
    """
    System diagnostics and performance monitoring for migration operations.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.start_time = time.time()
        self.diagnostics_data = {}
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging for diagnostics."""
        logger = logging.getLogger('SystemDiagnostics')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def check_system_resources(self) -> Dict:
        """
        Check available system resources for migration operations.
        
        Returns:
            Dictionary with system resource information
        """
        try:
            # Memory information
            memory = psutil.virtual_memory()
            
            # Disk information
            disk_usage = {}
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    disk_usage[partition.device] = {
                        'total_gb': usage.total / (1024**3),
                        'used_gb': usage.used / (1024**3),
                        'free_gb': usage.free / (1024**3),
                        'percent_used': (usage.used / usage.total) * 100
                    }
                except PermissionError:
                    continue
            
            # CPU information
            cpu_info = {
                'cpu_count': psutil.cpu_count(),
                'cpu_percent': psutil.cpu_percent(interval=1),
                'load_average': os.getloadavg() if hasattr(os, 'getloadavg') else None
            }
            
            # Process information
            current_process = psutil.Process()
            process_info = {
                'memory_mb': current_process.memory_info().rss / (1024**2),
                'cpu_percent': current_process.cpu_percent(),
                'open_files': len(current_process.open_files()),
                'threads': current_process.num_threads()
            }
            
            system_info = {
                'timestamp': datetime.now().isoformat(),
                'memory': {
                    'total_gb': memory.total / (1024**3),
                    'available_gb': memory.available / (1024**3),
                    'used_gb': memory.used / (1024**3),
                    'percent_used': memory.percent
                },
                'disk': disk_usage,
                'cpu': cpu_info,
                'process': process_info,
                'recommendations': self._generate_resource_recommendations(memory, disk_usage, cpu_info)
            }
            
            self.diagnostics_data['system_resources'] = system_info
            return system_info
            
        except Exception as e:
            self.logger.error(f"Error checking system resources: {e}")
            return {'error': str(e)}
    
    def _generate_resource_recommendations(self, memory, disk_usage, cpu_info) -> List[str]:
        """Generate recommendations based on system resources."""
        recommendations = []
        
        # Memory recommendations
        if memory.percent > 85:
            recommendations.append("⚠️ High memory usage detected. Consider closing other applications.")
        elif memory.available / (1024**3) < 2:
            recommendations.append("⚠️ Low available memory. Migration may be slow.")
        
        # Disk recommendations
        for device, usage in disk_usage.items():
            if usage['percent_used'] > 90:
                recommendations.append(f"⚠️ Disk {device} is {usage['percent_used']:.1f}% full. Free up space before migration.")
            elif usage['free_gb'] < 10:
                recommendations.append(f"⚠️ Less than 10GB free on {device}. Monitor disk space during migration.")
        
        # CPU recommendations
        if cpu_info['cpu_percent'] > 80:
            recommendations.append("⚠️ High CPU usage. Migration may run slowly.")
        
        if not recommendations:
            recommendations.append("✅ System resources look good for migration operations.")
        
        return recommendations
    
    def analyze_directory_structure(self, path: str, max_depth: int = 3) -> Dict:
        """
        Analyze directory structure for potential migration issues.
        
        Args:
            path: Directory path to analyze
            max_depth: Maximum depth to scan
            
        Returns:
            Analysis results with recommendations
        """
        self.logger.info(f"Analyzing directory structure: {path}")
        
        analysis = {
            'path': path,
            'scan_time': datetime.now().isoformat(),
            'total_files': 0,
            'total_directories': 0,
            'total_size_gb': 0,
            'file_types': {},
            'large_files': [],
            'deep_directories': [],
            'problematic_names': [],
            'recommendations': []
        }
        
        try:
            for root, dirs, files in os.walk(path):
                # Check depth
                depth = root.replace(path, '').count(os.sep)
                if depth > max_depth:
                    dirs.clear()  # Don't go deeper
                    continue
                
                analysis['total_directories'] += len(dirs)
                analysis['total_files'] += len(files)
                
                # Check for deep directory structures
                if depth > 8:
                    analysis['deep_directories'].append({
                        'path': root,
                        'depth': depth
                    })
                
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    try:
                        # File size analysis
                        file_size = os.path.getsize(file_path)
                        analysis['total_size_gb'] += file_size / (1024**3)
                        
                        # Track large files (>1GB)
                        if file_size > 1024**3:
                            analysis['large_files'].append({
                                'path': file_path,
                                'size_gb': file_size / (1024**3)
                            })
                        
                        # File type analysis
                        ext = Path(file).suffix.lower()
                        if ext:
                            analysis['file_types'][ext] = analysis['file_types'].get(ext, 0) + 1
                        else:
                            analysis['file_types']['no_extension'] = analysis['file_types'].get('no_extension', 0) + 1
                        
                        # Check for problematic file names
                        if any(char in file for char in ['<', '>', ':', '"', '|', '?', '*']):
                            analysis['problematic_names'].append(file_path)
                        
                        # Check for very long file names
                        if len(file) > 200:
                            analysis['problematic_names'].append(f"{file_path} (long name)")
                    
                    except (OSError, PermissionError) as e:
                        self.logger.warning(f"Cannot access file {file_path}: {e}")
                        continue
        
        except Exception as e:
            analysis['error'] = str(e)
            self.logger.error(f"Error analyzing directory: {e}")
        
        # Generate recommendations
        analysis['recommendations'] = self._generate_directory_recommendations(analysis)
        
        self.diagnostics_data['directory_analysis'] = analysis
        return analysis
    
    def _generate_directory_recommendations(self, analysis: Dict) -> List[str]:
        """Generate recommendations based on directory analysis."""
        recommendations = []
        
        # File count recommendations
        if analysis['total_files'] > 100000:
            recommendations.append("⚠️ Very large number of files detected. Consider batch processing.")
        elif analysis['total_files'] > 50000:
            recommendations.append("💡 Large file count. Migration may take significant time.")
        
        # Size recommendations
        if analysis['total_size_gb'] > 500:
            recommendations.append("⚠️ Very large data set (>500GB). Ensure sufficient destination space.")
        elif analysis['total_size_gb'] > 100:
            recommendations.append("💡 Large data set (>100GB). Monitor progress during migration.")
        
        # Large files
        if len(analysis['large_files']) > 0:
            recommendations.append(f"📁 {len(analysis['large_files'])} files >1GB detected. These may take longer to process.")
        
        # Deep directories
        if len(analysis['deep_directories']) > 0:
            recommendations.append(f"📂 {len(analysis['deep_directories'])} very deep directory structures found. May cause path length issues.")
        
        # Problematic names
        if len(analysis['problematic_names']) > 0:
            recommendations.append(f"⚠️ {len(analysis['problematic_names'])} files with problematic names. May need renaming.")
        
        # File type diversity
        if len(analysis['file_types']) > 50:
            recommendations.append("📄 High file type diversity. Verify all types are handled correctly.")
        
        return recommendations
    
    def test_drive_performance(self, drive_path: str, test_size_mb: int = 100) -> Dict:
        """
        Test read/write performance of a drive.
        
        Args:
            drive_path: Path to test drive
            test_size_mb: Size of test file in MB
            
        Returns:
            Performance test results
        """
        self.logger.info(f"Testing drive performance: {drive_path}")
        
        test_file = os.path.join(drive_path, f"performance_test_{int(time.time())}.tmp")
        test_data = b'0' * (1024 * 1024)  # 1MB of data
        
        performance = {
            'drive_path': drive_path,
            'test_time': datetime.now().isoformat(),
            'test_size_mb': test_size_mb,
            'write_speed_mbps': 0,
            'read_speed_mbps': 0,
            'error': None
        }
        
        try:
            # Write test
            write_start = time.time()
            with open(test_file, 'wb') as f:
                for _ in range(test_size_mb):
                    f.write(test_data)
            write_time = time.time() - write_start
            performance['write_speed_mbps'] = test_size_mb / write_time
            
            # Read test
            read_start = time.time()
            with open(test_file, 'rb') as f:
                while f.read(1024 * 1024):
                    pass
            read_time = time.time() - read_start
            performance['read_speed_mbps'] = test_size_mb / read_time
            
            # Cleanup
            os.remove(test_file)
            
            # Performance assessment
            if performance['write_speed_mbps'] < 10:
                performance['assessment'] = "Slow drive - migration will take longer"
            elif performance['write_speed_mbps'] < 50:
                performance['assessment'] = "Moderate speed drive"
            else:
                performance['assessment'] = "Fast drive - good for migration"
        
        except Exception as e:
            performance['error'] = str(e)
            self.logger.error(f"Drive performance test failed: {e}")
            
            # Cleanup on error
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
            except:
                pass
        
        return performance
    
    def generate_diagnostic_report(self, output_path: str = None) -> str:
        """
        Generate comprehensive diagnostic report.
        
        Args:
            output_path: Optional path for report file
            
        Returns:
            Path to generated report
        """
        if not output_path:
            output_path = f"diagnostic_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = {
            'report_generated': datetime.now().isoformat(),
            'session_duration_minutes': (time.time() - self.start_time) / 60,
            'diagnostics_data': self.diagnostics_data,
            'system_info': {
                'platform': sys.platform,
                'python_version': sys.version,
                'working_directory': os.getcwd()
            }
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Diagnostic report saved: {output_path}")
        return output_path


class MigrationDebugger:
    """
    Specialized debugging tools for migration operations.
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.debug_session = {
            'start_time': datetime.now().isoformat(),
            'operations': [],
            'errors': [],
            'warnings': []
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Set up debug logging."""
        logger = logging.getLogger('MigrationDebugger')
        logger.setLevel(logging.DEBUG)
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # File handler for debug logs
            file_handler = logging.FileHandler(f'migration_debug_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    def trace_file_operation(self, operation: str, source: str, destination: str = None) -> Dict:
        """
        Trace and debug file operations.
        
        Args:
            operation: Type of operation (copy, move, delete, etc.)
            source: Source file path
            destination: Destination file path (if applicable)
            
        Returns:
            Operation trace results
        """
        trace_id = f"{operation}_{int(time.time())}"
        trace_start = time.time()
        
        trace_data = {
            'trace_id': trace_id,
            'operation': operation,
            'source': source,
            'destination': destination,
            'start_time': datetime.now().isoformat(),
            'source_exists': os.path.exists(source) if source else False,
            'source_size': None,
            'source_permissions': None,
            'destination_exists': os.path.exists(destination) if destination else False,
            'destination_space': None,
            'operation_duration': None,
            'success': False,
            'error': None,
            'warnings': []
        }
        
        try:
            # Source file analysis
            if source and os.path.exists(source):
                stat_info = os.stat(source)
                trace_data['source_size'] = stat_info.st_size
                trace_data['source_permissions'] = oct(stat_info.st_mode)[-3:]
                
                # Check for potential issues
                if stat_info.st_size > 2 * 1024**3:  # >2GB
                    trace_data['warnings'].append("Large file - operation may take time")
                
                if not os.access(source, os.R_OK):
                    trace_data['warnings'].append("Source file may not be readable")
            
            # Destination analysis
            if destination:
                dest_dir = os.path.dirname(destination)
                if os.path.exists(dest_dir):
                    dest_stat = os.statvfs(dest_dir)
                    trace_data['destination_space'] = dest_stat.f_bavail * dest_stat.f_frsize
                    
                    # Check space requirements
                    if trace_data['source_size'] and trace_data['destination_space']:
                        if trace_data['source_size'] > trace_data['destination_space']:
                            trace_data['warnings'].append("Insufficient destination space")
                
                if os.path.exists(destination):
                    trace_data['warnings'].append("Destination file already exists")
            
            # Record operation
            trace_data['operation_duration'] = time.time() - trace_start
            trace_data['success'] = True
            
        except Exception as e:
            trace_data['error'] = str(e)
            trace_data['operation_duration'] = time.time() - trace_start
            self.logger.error(f"File operation trace failed: {e}")
        
        self.debug_session['operations'].append(trace_data)
        
        if trace_data['error']:
            self.debug_session['errors'].append(trace_data)
        
        if trace_data['warnings']:
            self.debug_session['warnings'].extend(trace_data['warnings'])
        
        return trace_data
    
    def analyze_migration_bottlenecks(self, migration_log: List[Dict]) -> Dict:
        """
        Analyze migration operations to identify bottlenecks.
        
        Args:
            migration_log: List of migration operation records
            
        Returns:
            Bottleneck analysis results
        """
        analysis = {
            'total_operations': len(migration_log),
            'total_time': 0,
            'average_time_per_operation': 0,
            'slowest_operations': [],
            'bottleneck_categories': {},
            'recommendations': []
        }
        
        if not migration_log:
            return analysis
        
        # Calculate timing statistics
        operation_times = []
        for op in migration_log:
            if 'duration' in op and op['duration']:
                operation_times.append(op['duration'])
                analysis['total_time'] += op['duration']
        
        if operation_times:
            analysis['average_time_per_operation'] = analysis['total_time'] / len(operation_times)
            
            # Find slowest operations (top 10% or >10 seconds)
            slow_threshold = max(10.0, sorted(operation_times, reverse=True)[len(operation_times)//10])
            
            for op in migration_log:
                if op.get('duration', 0) > slow_threshold:
                    analysis['slowest_operations'].append({
                        'operation': op.get('operation', 'unknown'),
                        'file': op.get('source', 'unknown'),
                        'duration': op['duration'],
                        'size_mb': op.get('size', 0) / (1024**2) if op.get('size') else 0
                    })
        
        # Categorize bottlenecks
        for op in migration_log:
            category = self._categorize_bottleneck(op)
            analysis['bottleneck_categories'][category] = analysis['bottleneck_categories'].get(category, 0) + 1
        
        # Generate recommendations
        analysis['recommendations'] = self._generate_bottleneck_recommendations(analysis)
        
        return analysis
    
    def _categorize_bottleneck(self, operation: Dict) -> str:
        """Categorize the type of bottleneck for an operation."""
        duration = operation.get('duration', 0)
        size = operation.get('size', 0)
        
        if duration > 30:  # >30 seconds
            if size > 100 * 1024**2:  # >100MB
                return "large_file_io"
            else:
                return "slow_disk_io"
        elif duration > 5:  # 5-30 seconds
            return "moderate_delay"
        else:
            return "normal_operation"
    
    def _generate_bottleneck_recommendations(self, analysis: Dict) -> List[str]:
        """Generate recommendations based on bottleneck analysis."""
        recommendations = []
        
        if analysis['bottleneck_categories'].get('large_file_io', 0) > 5:
            recommendations.append("Consider processing large files separately or in smaller batches")
        
        if analysis['bottleneck_categories'].get('slow_disk_io', 0) > 10:
            recommendations.append("Disk I/O appears slow - check drive health and available space")
        
        if analysis['average_time_per_operation'] > 5:
            recommendations.append("Overall operation speed is slow - consider system optimization")
        
        if len(analysis['slowest_operations']) > analysis['total_operations'] * 0.2:
            recommendations.append("High percentage of slow operations - investigate system resources")
        
        return recommendations
    
    def create_debug_summary(self) -> Dict:
        """Create summary of debug session."""
        summary = {
            'session_info': self.debug_session,
            'total_operations': len(self.debug_session['operations']),
            'total_errors': len(self.debug_session['errors']),
            'total_warnings': len(self.debug_session['warnings']),
            'session_duration': datetime.now().isoformat(),
            'most_common_errors': self._get_common_errors(),
            'most_common_warnings': self._get_common_warnings()
        }
        
        return summary
    
    def _get_common_errors(self) -> List[Dict]:
        """Get most common error types."""
        error_counts = {}
        for error in self.debug_session['errors']:
            error_type = error.get('error', 'Unknown error')
            error_counts[error_type] = error_counts.get(error_type, 0) + 1
        
        return [{'error': error, 'count': count} 
                for error, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True)]
    
    def _get_common_warnings(self) -> List[Dict]:
        """Get most common warning types."""
        warning_counts = {}
        for warning in self.debug_session['warnings']:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1
        
        return [{'warning': warning, 'count': count} 
                for warning, count in sorted(warning_counts.items(), key=lambda x: x[1], reverse=True)]


def main():
    """Example usage of debug utilities."""
    print("🔧 Migration Debug Utilities")
    print("=" * 50)
    
    # System diagnostics
    diagnostics = SystemDiagnostics()
    
    print("\n📊 Checking system resources...")
    resources = diagnostics.check_system_resources()
    
    print(f"Memory: {resources['memory']['available_gb']:.1f}GB available")
    print(f"CPU: {resources['cpu']['cpu_percent']:.1f}% usage")
    
    for recommendation in resources['recommendations']:
        print(f"  {recommendation}")
    
    # Directory analysis
    test_path = input("\nEnter directory path to analyze (or press Enter to skip): ").strip()
    if test_path and os.path.exists(test_path):
        print(f"\n📁 Analyzing directory: {test_path}")
        dir_analysis = diagnostics.analyze_directory_structure(test_path)
        
        print(f"Files: {dir_analysis['total_files']:,}")
        print(f"Size: {dir_analysis['total_size_gb']:.2f}GB")
        print(f"File types: {len(dir_analysis['file_types'])}")
        
        for recommendation in dir_analysis['recommendations']:
            print(f"  {recommendation}")
    
    # Drive performance test
    test_drive = input("\nEnter drive path to test performance (or press Enter to skip): ").strip()
    if test_drive and os.path.exists(test_drive):
        print(f"\n⚡ Testing drive performance: {test_drive}")
        performance = diagnostics.test_drive_performance(test_drive, 50)  # 50MB test
        
        if not performance.get('error'):
            print(f"Write speed: {performance['write_speed_mbps']:.1f} MB/s")
            print(f"Read speed: {performance['read_speed_mbps']:.1f} MB/s")
            print(f"Assessment: {performance['assessment']}")
        else:
            print(f"Test failed: {performance['error']}")
    
    # Generate diagnostic report
    print("\n📋 Generating diagnostic report...")
    report_path = diagnostics.generate_diagnostic_report()
    print(f"Report saved: {report_path}")
    
    # Migration debugger example
    print("\n🐛 Migration Debugger Example")
    debugger = MigrationDebugger()
    
    # Example file operation trace
    trace = debugger.trace_file_operation('copy', __file__, '/tmp/test_copy.py')
    print(f"Operation traced: {trace['operation']} - Success: {trace['success']}")
    
    if trace['warnings']:
        for warning in trace['warnings']:
            print(f"  ⚠️ {warning}")
    
    # Debug summary
    debug_summary = debugger.create_debug_summary()
    print(f"\nDebug session: {debug_summary['total_operations']} operations, {debug_summary['total_errors']} errors")


if __name__ == "__main__":
    main()