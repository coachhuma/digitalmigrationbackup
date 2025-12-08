"""
Performance Analyzer Module

This module provides utilities for calculating and analyzing:
- Drive speeds (read/write performance)
- Transfer times (estimated time for data migration)
- Memory usage tracking and analysis

Author: coachhuma
Date: 2025-12-08
"""

import time
import psutil
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class PerformanceMetrics:
    """Data class to store performance metrics"""
    timestamp: datetime
    drive_speed_mbps: float
    transfer_time_seconds: float
    memory_usage_percent: float
    memory_used_mb: float
    memory_available_mb: float


class DriveSpeedAnalyzer:
    """Analyzer for drive read/write speeds"""
    
    def __init__(self):
        self.measurements: List[float] = []
    
    def measure_write_speed(self, file_path: str, data_size_mb: int = 100) -> float:
        """
        Measure drive write speed by writing test data.
        
        Args:
            file_path: Path where test file will be written
            data_size_mb: Size of test data in MB
            
        Returns:
            Write speed in MB/s
        """
        try:
            test_data = b'0' * (data_size_mb * 1024 * 1024)
            
            start_time = time.time()
            with open(file_path, 'wb') as f:
                f.write(test_data)
            write_time = time.time() - start_time
            
            speed = data_size_mb / write_time if write_time > 0 else 0
            self.measurements.append(speed)
            
            # Cleanup
            if os.path.exists(file_path):
                os.remove(file_path)
            
            return speed
        except Exception as e:
            print(f"Error measuring write speed: {e}")
            return 0.0
    
    def measure_read_speed(self, file_path: str, data_size_mb: int = 100) -> float:
        """
        Measure drive read speed by reading test data.
        
        Args:
            file_path: Path to test file
            data_size_mb: Size of test data in MB
            
        Returns:
            Read speed in MB/s
        """
        try:
            # First write test file
            test_data = b'0' * (data_size_mb * 1024 * 1024)
            with open(file_path, 'wb') as f:
                f.write(test_data)
            
            # Then measure read speed
            start_time = time.time()
            with open(file_path, 'rb') as f:
                while f.read(1024 * 1024):  # Read in 1MB chunks
                    pass
            read_time = time.time() - start_time
            
            speed = data_size_mb / read_time if read_time > 0 else 0
            self.measurements.append(speed)
            
            # Cleanup
            if os.path.exists(file_path):
                os.remove(file_path)
            
            return speed
        except Exception as e:
            print(f"Error measuring read speed: {e}")
            return 0.0
    
    def get_average_speed(self) -> float:
        """Get average drive speed from all measurements"""
        return sum(self.measurements) / len(self.measurements) if self.measurements else 0.0
    
    def get_peak_speed(self) -> float:
        """Get peak drive speed from all measurements"""
        return max(self.measurements) if self.measurements else 0.0


class TransferTimeCalculator:
    """Calculator for estimating transfer times"""
    
    @staticmethod
    def calculate_transfer_time(
        data_size_gb: float,
        speed_mbps: float
    ) -> Dict[str, float]:
        """
        Calculate transfer time given data size and speed.
        
        Args:
            data_size_gb: Size of data to transfer in GB
            speed_mbps: Transfer speed in MB/s
            
        Returns:
            Dictionary with transfer times in different units
        """
        if speed_mbps <= 0:
            return {
                'seconds': float('inf'),
                'minutes': float('inf'),
                'hours': float('inf'),
                'days': float('inf')
            }
        
        data_size_mb = data_size_gb * 1024
        transfer_seconds = data_size_mb / speed_mbps
        
        return {
            'seconds': transfer_seconds,
            'minutes': transfer_seconds / 60,
            'hours': transfer_seconds / 3600,
            'days': transfer_seconds / 86400
        }
    
    @staticmethod
    def estimate_completion_time(
        data_size_gb: float,
        speed_mbps: float,
        start_time: Optional[datetime] = None
    ) -> datetime:
        """
        Estimate completion time for a transfer.
        
        Args:
            data_size_gb: Size of data to transfer in GB
            speed_mbps: Transfer speed in MB/s
            start_time: Start time of transfer (defaults to now)
            
        Returns:
            Estimated completion datetime
        """
        if start_time is None:
            start_time = datetime.now()
        
        times = TransferTimeCalculator.calculate_transfer_time(data_size_gb, speed_mbps)
        duration = timedelta(seconds=times['seconds'])
        
        return start_time + duration


class MemoryUsageAnalyzer:
    """Analyzer for memory usage tracking"""
    
    def __init__(self):
        self.history: List[PerformanceMetrics] = []
    
    def get_current_memory_usage(self) -> Dict[str, float]:
        """
        Get current memory usage statistics.
        
        Returns:
            Dictionary with memory usage information
        """
        try:
            memory_info = psutil.virtual_memory()
            return {
                'total_mb': memory_info.total / (1024 * 1024),
                'used_mb': memory_info.used / (1024 * 1024),
                'available_mb': memory_info.available / (1024 * 1024),
                'percent': memory_info.percent,
                'used_percent': (memory_info.used / memory_info.total) * 100
            }
        except Exception as e:
            print(f"Error getting memory usage: {e}")
            return {}
    
    def get_process_memory_usage(self, pid: Optional[int] = None) -> Dict[str, float]:
        """
        Get memory usage for a specific process.
        
        Args:
            pid: Process ID (defaults to current process)
            
        Returns:
            Dictionary with process memory information
        """
        try:
            if pid is None:
                pid = os.getpid()
            
            process = psutil.Process(pid)
            memory_info = process.memory_info()
            
            return {
                'rss_mb': memory_info.rss / (1024 * 1024),  # Resident set size
                'vms_mb': memory_info.vms / (1024 * 1024),  # Virtual memory size
                'percent': process.memory_percent()
            }
        except Exception as e:
            print(f"Error getting process memory usage: {e}")
            return {}
    
    def record_metrics(
        self,
        drive_speed_mbps: float,
        transfer_time_seconds: float
    ) -> PerformanceMetrics:
        """
        Record performance metrics snapshot.
        
        Args:
            drive_speed_mbps: Current drive speed in MB/s
            transfer_time_seconds: Current transfer time in seconds
            
        Returns:
            PerformanceMetrics object
        """
        memory_usage = self.get_current_memory_usage()
        
        metrics = PerformanceMetrics(
            timestamp=datetime.now(),
            drive_speed_mbps=drive_speed_mbps,
            transfer_time_seconds=transfer_time_seconds,
            memory_usage_percent=memory_usage.get('percent', 0),
            memory_used_mb=memory_usage.get('used_mb', 0),
            memory_available_mb=memory_usage.get('available_mb', 0)
        )
        
        self.history.append(metrics)
        return metrics
    
    def get_memory_stats(self) -> Dict[str, float]:
        """
        Get aggregated memory statistics from recorded history.
        
        Returns:
            Dictionary with memory statistics
        """
        if not self.history:
            return {}
        
        memory_usages = [m.memory_usage_percent for m in self.history]
        
        return {
            'average_percent': sum(memory_usages) / len(memory_usages),
            'peak_percent': max(memory_usages),
            'min_percent': min(memory_usages),
            'latest_percent': self.history[-1].memory_usage_percent
        }


class PerformanceAnalyzer:
    """Main performance analyzer combining all metrics"""
    
    def __init__(self):
        self.drive_analyzer = DriveSpeedAnalyzer()
        self.transfer_calculator = TransferTimeCalculator()
        self.memory_analyzer = MemoryUsageAnalyzer()
    
    def analyze_transfer(
        self,
        data_size_gb: float,
        drive_speed_mbps: Optional[float] = None
    ) -> Dict:
        """
        Perform complete transfer analysis.
        
        Args:
            data_size_gb: Size of data to transfer in GB
            drive_speed_mbps: Drive speed (if None, uses average from measurements)
            
        Returns:
            Dictionary with complete analysis
        """
        if drive_speed_mbps is None:
            drive_speed_mbps = self.drive_analyzer.get_average_speed()
        
        if drive_speed_mbps == 0:
            return {'error': 'No drive speed available. Run drive speed tests first.'}
        
        transfer_times = self.transfer_calculator.calculate_transfer_time(
            data_size_gb,
            drive_speed_mbps
        )
        
        completion_time = self.transfer_calculator.estimate_completion_time(
            data_size_gb,
            drive_speed_mbps
        )
        
        memory_usage = self.memory_analyzer.get_current_memory_usage()
        
        metrics = self.memory_analyzer.record_metrics(
            drive_speed_mbps,
            transfer_times['seconds']
        )
        
        return {
            'data_size_gb': data_size_gb,
            'drive_speed_mbps': drive_speed_mbps,
            'transfer_times': transfer_times,
            'estimated_completion': completion_time.isoformat(),
            'memory_usage': memory_usage,
            'metrics': metrics
        }
    
    def generate_report(self) -> str:
        """
        Generate a text report of all performance metrics.
        
        Returns:
            Formatted performance report
        """
        report = "=" * 60 + "\n"
        report += "PERFORMANCE ANALYSIS REPORT\n"
        report += "=" * 60 + "\n\n"
        
        # Drive Speed Report
        report += "DRIVE SPEED ANALYSIS\n"
        report += "-" * 40 + "\n"
        avg_speed = self.drive_analyzer.get_average_speed()
        peak_speed = self.drive_analyzer.get_peak_speed()
        report += f"Average Speed: {avg_speed:.2f} MB/s\n"
        report += f"Peak Speed: {peak_speed:.2f} MB/s\n"
        report += f"Total Measurements: {len(self.drive_analyzer.measurements)}\n\n"
        
        # Memory Usage Report
        report += "MEMORY USAGE ANALYSIS\n"
        report += "-" * 40 + "\n"
        memory_stats = self.memory_analyzer.get_memory_stats()
        if memory_stats:
            report += f"Average Usage: {memory_stats.get('average_percent', 0):.2f}%\n"
            report += f"Peak Usage: {memory_stats.get('peak_percent', 0):.2f}%\n"
            report += f"Minimum Usage: {memory_stats.get('min_percent', 0):.2f}%\n"
        
        report += "\n" + "=" * 60 + "\n"
        return report


if __name__ == "__main__":
    # Example usage
    analyzer = PerformanceAnalyzer()
    
    print("Performance Analyzer Ready")
    print(f"Current Memory Usage: {analyzer.memory_analyzer.get_current_memory_usage()}")
