#!/usr/bin/env python3
"""
Advanced CLI Tool for Digital Migration Backup
Provides file operations, performance analysis, notifications, and backup scheduling.
"""

import argparse
import os
import sys
import json
import time
import hashlib
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import threading
import schedule
import psutil
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging for the CLI tool."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('backup_tool.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class FileMetadata:
    """Metadata for a file."""
    path: str
    size: int
    modified_time: float
    checksum: str
    is_directory: bool
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class BackupStats:
    """Statistics for a backup operation."""
    total_files: int
    total_size: int
    total_time: float
    files_skipped: int
    errors: int
    start_time: str
    end_time: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class PerformanceMetrics:
    """Performance metrics for system analysis."""
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    available_memory: int
    timestamp: str


# ============================================================================
# NOTIFICATION SYSTEM
# ============================================================================

class NotificationHandler(ABC):
    """Abstract base class for notification handlers."""
    
    @abstractmethod
    def send(self, title: str, message: str, level: str = "info") -> bool:
        """Send a notification."""
        pass


class ConsoleNotification(NotificationHandler):
    """Send notifications to console."""
    
    def send(self, title: str, message: str, level: str = "info") -> bool:
        """Send console notification."""
        symbols = {
            "success": "✓",
            "error": "✗",
            "warning": "⚠",
            "info": "ℹ"
        }
        symbol = symbols.get(level, "•")
        print(f"\n[{symbol}] {title}")
        print(f"    {message}\n")
        return True


class FileNotification(NotificationHandler):
    """Save notifications to a file."""
    
    def __init__(self, notification_file: str = "backup_notifications.log"):
        self.notification_file = notification_file
    
    def send(self, title: str, message: str, level: str = "info") -> bool:
        """Send file notification."""
        try:
            with open(self.notification_file, 'a') as f:
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] [{level.upper()}] {title}\n")
                f.write(f"    {message}\n\n")
            return True
        except Exception as e:
            logger.error(f"Failed to write notification: {e}")
            return False


class NotificationManager:
    """Manage multiple notification handlers."""
    
    def __init__(self):
        self.handlers: List[NotificationHandler] = []
    
    def add_handler(self, handler: NotificationHandler) -> None:
        """Add a notification handler."""
        self.handlers.append(handler)
    
    def notify(self, title: str, message: str, level: str = "info") -> None:
        """Send notification through all handlers."""
        for handler in self.handlers:
            try:
                handler.send(title, message, level)
            except Exception as e:
                logger.error(f"Notification handler error: {e}")


# ============================================================================
# FILE OPERATIONS
# ============================================================================

class FileOperations:
    """Handle file operations for backup."""
    
    @staticmethod
    def calculate_checksum(file_path: str, algorithm: str = "md5") -> str:
        """Calculate file checksum."""
        hasher = hashlib.new(algorithm)
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum for {file_path}: {e}")
            return ""
    
    @staticmethod
    def get_file_metadata(file_path: str) -> Optional[FileMetadata]:
        """Get metadata for a file."""
        try:
            path = Path(file_path)
            stat = path.stat()
            checksum = FileOperations.calculate_checksum(file_path) if path.is_file() else ""
            
            return FileMetadata(
                path=str(path),
                size=stat.st_size,
                modified_time=stat.st_mtime,
                checksum=checksum,
                is_directory=path.is_dir()
            )
        except Exception as e:
            logger.error(f"Error getting metadata for {file_path}: {e}")
            return None
    
    @staticmethod
    def copy_file(source: str, destination: str) -> bool:
        """Copy a file from source to destination."""
        try:
            dest_path = Path(destination)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            logger.info(f"Copied: {source} -> {destination}")
            return True
        except Exception as e:
            logger.error(f"Error copying file {source}: {e}")
            return False
    
    @staticmethod
    def get_directory_size(directory: str) -> int:
        """Calculate total size of directory."""
        total_size = 0
        try:
            for entry in Path(directory).rglob('*'):
                if entry.is_file():
                    total_size += entry.stat().st_size
        except Exception as e:
            logger.error(f"Error calculating directory size: {e}")
        return total_size
    
    @staticmethod
    def list_files(directory: str, pattern: str = "*", recursive: bool = True) -> List[str]:
        """List files in directory matching pattern."""
        try:
            path = Path(directory)
            if recursive:
                files = list(path.rglob(pattern))
            else:
                files = list(path.glob(pattern))
            return [str(f) for f in files if f.is_file()]
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []


# ============================================================================
# BACKUP ENGINE
# ============================================================================

class BackupEngine:
    """Core backup functionality."""
    
    def __init__(self, notification_manager: NotificationManager):
        self.notification_manager = notification_manager
        self.stats: Optional[BackupStats] = None
    
    def backup_directory(
        self,
        source: str,
        destination: str,
        incremental: bool = False,
        compression: bool = False,
        exclude_patterns: Optional[List[str]] = None
    ) -> bool:
        """Backup a directory."""
        start_time = datetime.utcnow()
        source_path = Path(source)
        
        if not source_path.exists():
            self.notification_manager.notify(
                "Backup Failed",
                f"Source directory does not exist: {source}",
                "error"
            )
            return False
        
        total_files = 0
        total_size = 0
        files_skipped = 0
        errors = 0
        
        try:
            for file_path in source_path.rglob('*'):
                if file_path.is_file():
                    # Check exclude patterns
                    if exclude_patterns:
                        if any(pattern in str(file_path) for pattern in exclude_patterns):
                            files_skipped += 1
                            continue
                    
                    relative_path = file_path.relative_to(source_path)
                    dest_file = Path(destination) / relative_path
                    
                    if FileOperations.copy_file(str(file_path), str(dest_file)):
                        total_files += 1
                        total_size += file_path.stat().st_size
                    else:
                        errors += 1
            
            end_time = datetime.utcnow()
            elapsed_time = (end_time - start_time).total_seconds()
            
            self.stats = BackupStats(
                total_files=total_files,
                total_size=total_size,
                total_time=elapsed_time,
                files_skipped=files_skipped,
                errors=errors,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat()
            )
            
            self.notification_manager.notify(
                "Backup Completed",
                f"Backed up {total_files} files ({self._format_size(total_size)}) in {elapsed_time:.2f}s",
                "success"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            self.notification_manager.notify(
                "Backup Error",
                f"An error occurred during backup: {str(e)}",
                "error"
            )
            return False
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes to human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"


# ============================================================================
# PERFORMANCE ANALYSIS
# ============================================================================

class PerformanceAnalyzer:
    """Analyze system performance."""
    
    @staticmethod
    def get_system_metrics() -> PerformanceMetrics:
        """Get current system performance metrics."""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return PerformanceMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            disk_usage_percent=disk.percent,
            available_memory=memory.available,
            timestamp=datetime.utcnow().isoformat()
        )
    
    @staticmethod
    def get_process_info() -> Dict:
        """Get current process information."""
        process = psutil.Process()
        return {
            "pid": process.pid,
            "memory_mb": process.memory_info().rss / 1024 / 1024,
            "cpu_percent": process.cpu_percent(interval=1),
            "num_threads": process.num_threads()
        }
    
    @staticmethod
    def monitor_performance(duration: int = 60, interval: int = 5) -> List[PerformanceMetrics]:
        """Monitor performance over time."""
        metrics = []
        end_time = time.time() + duration
        
        while time.time() < end_time:
            metrics.append(PerformanceAnalyzer.get_system_metrics())
            time.sleep(interval)
        
        return metrics
    
    @staticmethod
    def generate_performance_report(metrics: List[PerformanceMetrics]) -> Dict:
        """Generate performance report from metrics."""
        if not metrics:
            return {}
        
        cpu_values = [m.cpu_percent for m in metrics]
        memory_values = [m.memory_percent for m in metrics]
        
        return {
            "cpu": {
                "avg": sum(cpu_values) / len(cpu_values),
                "max": max(cpu_values),
                "min": min(cpu_values)
            },
            "memory": {
                "avg": sum(memory_values) / len(memory_values),
                "max": max(memory_values),
                "min": min(memory_values)
            },
            "samples": len(metrics)
        }


# ============================================================================
# BACKUP SCHEDULER
# ============================================================================

class BackupScheduler:
    """Schedule and manage backup operations."""
    
    def __init__(self, backup_engine: BackupEngine):
        self.backup_engine = backup_engine
        self.scheduled_jobs: Dict[str, schedule.Job] = {}
        self.running = False
    
    def schedule_daily_backup(
        self,
        job_name: str,
        source: str,
        destination: str,
        time_str: str = "02:00"
    ) -> None:
        """Schedule a daily backup at specific time."""
        def job():
            logger.info(f"Running scheduled backup: {job_name}")
            self.backup_engine.backup_directory(source, destination)
        
        self.scheduled_jobs[job_name] = schedule.every().day.at(time_str).do(job)
        logger.info(f"Scheduled daily backup '{job_name}' at {time_str}")
    
    def schedule_weekly_backup(
        self,
        job_name: str,
        source: str,
        destination: str,
        day: str = "monday",
        time_str: str = "02:00"
    ) -> None:
        """Schedule a weekly backup."""
        def job():
            logger.info(f"Running scheduled backup: {job_name}")
            self.backup_engine.backup_directory(source, destination)
        
        day_map = {
            "monday": schedule.every().monday,
            "tuesday": schedule.every().tuesday,
            "wednesday": schedule.every().wednesday,
            "thursday": schedule.every().thursday,
            "friday": schedule.every().friday,
            "saturday": schedule.every().saturday,
            "sunday": schedule.every().sunday
        }
        
        self.scheduled_jobs[job_name] = day_map[day.lower()].at(time_str).do(job)
        logger.info(f"Scheduled weekly backup '{job_name}' on {day} at {time_str}")
    
    def schedule_interval_backup(
        self,
        job_name: str,
        source: str,
        destination: str,
        hours: int = 1
    ) -> None:
        """Schedule a backup at regular intervals."""
        def job():
            logger.info(f"Running scheduled backup: {job_name}")
            self.backup_engine.backup_directory(source, destination)
        
        self.scheduled_jobs[job_name] = schedule.every(hours).hours.do(job)
        logger.info(f"Scheduled backup '{job_name}' every {hours} hour(s)")
    
    def start_scheduler(self) -> None:
        """Start the scheduler in a background thread."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Backup scheduler started")
    
    def _run_scheduler(self) -> None:
        """Run the scheduler loop."""
        while self.running:
            schedule.run_pending()
            time.sleep(60)
    
    def stop_scheduler(self) -> None:
        """Stop the scheduler."""
        self.running = False
        logger.info("Backup scheduler stopped")
    
    def list_jobs(self) -> Dict[str, str]:
        """List all scheduled jobs."""
        return {
            name: str(job)
            for name, job in self.scheduled_jobs.items()
        }


# ============================================================================
# CLI APPLICATION
# ============================================================================

class BackupCLI:
    """Command-line interface for backup operations."""
    
    def __init__(self):
        self.notification_manager = NotificationManager()
        self.notification_manager.add_handler(ConsoleNotification())
        self.notification_manager.add_handler(FileNotification())
        
        self.backup_engine = BackupEngine(self.notification_manager)
        self.scheduler = BackupScheduler(self.backup_engine)
        self.analyzer = PerformanceAnalyzer()
    
    def create_parser(self) -> argparse.ArgumentParser:
        """Create argument parser."""
        parser = argparse.ArgumentParser(
            description="Advanced CLI Tool for Digital Migration Backup",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Backup a directory
  python cli_tool.py backup /source /destination
  
  # Check system performance
  python cli_tool.py performance --monitor 60
  
  # Schedule daily backup
  python cli_tool.py schedule daily /source /destination --time 02:00
  
  # Get file metadata
  python cli_tool.py metadata /path/to/file
            """
        )
        
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Backup command
        backup_parser = subparsers.add_parser('backup', help='Perform a backup')
        backup_parser.add_argument('source', help='Source directory')
        backup_parser.add_argument('destination', help='Destination directory')
        backup_parser.add_argument('--incremental', action='store_true', help='Perform incremental backup')
        backup_parser.add_argument('--compression', action='store_true', help='Enable compression')
        backup_parser.add_argument('--exclude', nargs='*', help='Patterns to exclude')
        
        # Performance command
        perf_parser = subparsers.add_parser('performance', help='Analyze system performance')
        perf_parser.add_argument('--monitor', type=int, help='Monitor for N seconds')
        perf_parser.add_argument('--interval', type=int, default=5, help='Monitoring interval')
        
        # Metadata command
        meta_parser = subparsers.add_parser('metadata', help='Get file metadata')
        meta_parser.add_argument('path', help='File or directory path')
        
        # Schedule command
        schedule_parser = subparsers.add_parser('schedule', help='Schedule backups')
        schedule_subparsers = schedule_parser.add_subparsers(dest='schedule_type', help='Schedule type')
        
        daily_parser = schedule_subparsers.add_parser('daily', help='Daily backup')
        daily_parser.add_argument('source', help='Source directory')
        daily_parser.add_argument('destination', help='Destination directory')
        daily_parser.add_argument('--time', default='02:00', help='Backup time (HH:MM)')
        daily_parser.add_argument('--name', default='daily_backup', help='Job name')
        
        weekly_parser = schedule_subparsers.add_parser('weekly', help='Weekly backup')
        weekly_parser.add_argument('source', help='Source directory')
        weekly_parser.add_argument('destination', help='Destination directory')
        weekly_parser.add_argument('--day', default='monday', help='Day of week')
        weekly_parser.add_argument('--time', default='02:00', help='Backup time (HH:MM)')
        weekly_parser.add_argument('--name', default='weekly_backup', help='Job name')
        
        interval_parser = schedule_subparsers.add_parser('interval', help='Interval-based backup')
        interval_parser.add_argument('source', help='Source directory')
        interval_parser.add_argument('destination', help='Destination directory')
        interval_parser.add_argument('--hours', type=int, default=1, help='Interval in hours')
        interval_parser.add_argument('--name', default='interval_backup', help='Job name')
        
        # List command
        subparsers.add_parser('list-jobs', help='List scheduled jobs')
        
        # Status command
        subparsers.add_parser('status', help='Show system status')
        
        return parser
    
    def handle_backup(self, args) -> None:
        """Handle backup command."""
        logger.info(f"Starting backup: {args.source} -> {args.destination}")
        self.backup_engine.backup_directory(
            args.source,
            args.destination,
            incremental=args.incremental,
            compression=args.compression,
            exclude_patterns=args.exclude
        )
        if self.backup_engine.stats:
            self.print_backup_stats(self.backup_engine.stats)
    
    def handle_performance(self, args) -> None:
        """Handle performance command."""
        if args.monitor:
            print(f"\nMonitoring system performance for {args.monitor} seconds...")
            metrics = self.analyzer.monitor_performance(
                duration=args.monitor,
                interval=args.interval
            )
            report = self.analyzer.generate_performance_report(metrics)
            self.print_performance_report(report)
        else:
            metrics = self.analyzer.get_system_metrics()
            process_info = self.analyzer.get_process_info()
            self.print_system_metrics(metrics, process_info)
    
    def handle_metadata(self, args) -> None:
        """Handle metadata command."""
        path = Path(args.path)
        if path.is_dir():
            size = FileOperations.get_directory_size(args.path)
            print(f"\nDirectory: {args.path}")
            print(f"  Size: {BackupEngine._format_size(size)}")
            files = FileOperations.list_files(args.path)
            print(f"  Files: {len(files)}")
        else:
            metadata = FileOperations.get_file_metadata(args.path)
            if metadata:
                self.print_metadata(metadata)
    
    def handle_schedule(self, args) -> None:
        """Handle schedule command."""
        if args.schedule_type == 'daily':
            self.scheduler.schedule_daily_backup(
                args.name,
                args.source,
                args.destination,
                args.time
            )
            print(f"\n✓ Daily backup scheduled: {args.name}")
        elif args.schedule_type == 'weekly':
            self.scheduler.schedule_weekly_backup(
                args.name,
                args.source,
                args.destination,
                args.day,
                args.time
            )
            print(f"\n✓ Weekly backup scheduled: {args.name}")
        elif args.schedule_type == 'interval':
            self.scheduler.schedule_interval_backup(
                args.name,
                args.source,
                args.destination,
                args.hours
            )
            print(f"\n✓ Interval backup scheduled: {args.name}")
    
    def handle_list_jobs(self) -> None:
        """Handle list-jobs command."""
        jobs = self.scheduler.list_jobs()
        if jobs:
            print("\nScheduled Backup Jobs:")
            for name, job_info in jobs.items():
                print(f"  - {name}: {job_info}")
        else:
            print("\nNo scheduled backup jobs found.")
    
    def handle_status(self) -> None:
        """Handle status command."""
        metrics = self.analyzer.get_system_metrics()
        process_info = self.analyzer.get_process_info()
        jobs = self.scheduler.list_jobs()
        
        print("\n" + "="*60)
        print("BACKUP SYSTEM STATUS")
        print("="*60)
        self.print_system_metrics(metrics, process_info)
        
        if jobs:
            print("\nScheduled Jobs:")
            for name in jobs:
                print(f"  - {name}")
        else:
            print("\nNo scheduled jobs.")
    
    @staticmethod
    def print_metadata(metadata: FileMetadata) -> None:
        """Print file metadata."""
        print(f"\nFile Metadata: {metadata.path}")
        print(f"  Size: {BackupEngine._format_size(metadata.size)}")
        print(f"  Modified: {datetime.fromtimestamp(metadata.modified_time)}")
        print(f"  Checksum: {metadata.checksum}")
        print(f"  Directory: {metadata.is_directory}")
    
    @staticmethod
    def print_backup_stats(stats: BackupStats) -> None:
        """Print backup statistics."""
        print("\n" + "="*60)
        print("BACKUP STATISTICS")
        print("="*60)
        print(f"Total Files: {stats.total_files}")
        print(f"Total Size: {BackupEngine._format_size(stats.total_size)}")
        print(f"Total Time: {stats.total_time:.2f}s")
        print(f"Files Skipped: {stats.files_skipped}")
        print(f"Errors: {stats.errors}")
        print(f"Start Time: {stats.start_time}")
        print(f"End Time: {stats.end_time}")
    
    @staticmethod
    def print_system_metrics(metrics: PerformanceMetrics, process_info: Dict) -> None:
        """Print system metrics."""
        print("\n" + "-"*60)
        print("SYSTEM METRICS")
        print("-"*60)
        print(f"CPU Usage: {metrics.cpu_percent:.1f}%")
        print(f"Memory Usage: {metrics.memory_percent:.1f}%")
        print(f"Available Memory: {BackupEngine._format_size(metrics.available_memory)}")
        print(f"Disk Usage: {metrics.disk_usage_percent:.1f}%")
        print("\nProcess Info:")
        print(f"  PID: {process_info['pid']}")
        print(f"  Memory: {process_info['memory_mb']:.1f} MB")
        print(f"  CPU: {process_info['cpu_percent']:.1f}%")
        print(f"  Threads: {process_info['num_threads']}")
    
    @staticmethod
    def print_performance_report(report: Dict) -> None:
        """Print performance report."""
        print("\n" + "="*60)
        print("PERFORMANCE REPORT")
        print("="*60)
        print(f"Samples: {report.get('samples', 0)}")
        print("\nCPU Usage:")
        cpu = report.get('cpu', {})
        print(f"  Average: {cpu.get('avg', 0):.1f}%")
        print(f"  Maximum: {cpu.get('max', 0):.1f}%")
        print(f"  Minimum: {cpu.get('min', 0):.1f}%")
        print("\nMemory Usage:")
        memory = report.get('memory', {})
        print(f"  Average: {memory.get('avg', 0):.1f}%")
        print(f"  Maximum: {memory.get('max', 0):.1f}%")
        print(f"  Minimum: {memory.get('min', 0):.1f}%")
    
    def run(self, args) -> None:
        """Run the CLI application."""
        try:
            if args.command == 'backup':
                self.handle_backup(args)
            elif args.command == 'performance':
                self.handle_performance(args)
            elif args.command == 'metadata':
                self.handle_metadata(args)
            elif args.command == 'schedule':
                self.handle_schedule(args)
            elif args.command == 'list-jobs':
                self.handle_list_jobs()
            elif args.command == 'status':
                self.handle_status()
            else:
                print("No command specified. Use -h for help.")
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"\n✗ Error: {e}")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main() -> None:
    """Main entry point."""
    cli = BackupCLI()
    parser = cli.create_parser()
    args = parser.parse_args()
    
    if args.command:
        cli.run(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
