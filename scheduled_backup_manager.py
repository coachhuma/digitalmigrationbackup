"""
Scheduled Backup Manager using APScheduler

This module implements automated backup scheduling using APScheduler.
It provides functionality to schedule, manage, and execute backups at specified intervals.
"""

import logging
import os
from datetime import datetime
from typing import Optional, Callable, Dict, Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.base import JobLookupError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScheduledBackupManager:
    """
    Manages scheduled backups using APScheduler.
    
    Provides functionality to:
    - Schedule backups at regular intervals or specific times
    - Start and stop the scheduler
    - Add, update, and remove backup jobs
    - Handle backup execution and logging
    """
    
    def __init__(self, backup_dir: Optional[str] = None):
        """
        Initialize the Scheduled Backup Manager.
        
        Args:
            backup_dir: Directory where backups will be stored.
                       Defaults to './backups' if not specified.
        """
        self.backup_dir = backup_dir or './backups'
        self.scheduler = BackgroundScheduler()
        self._ensure_backup_dir()
        logger.info(f"Initialized ScheduledBackupManager with backup directory: {self.backup_dir}")
    
    def _ensure_backup_dir(self) -> None:
        """Ensure the backup directory exists."""
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
            logger.info(f"Created backup directory: {self.backup_dir}")
    
    def schedule_interval_backup(
        self,
        backup_func: Callable,
        job_id: str,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Schedule a backup job to run at regular intervals.
        
        Args:
            backup_func: The function to call for backup execution.
            job_id: Unique identifier for this backup job.
            hours: Number of hours between backups.
            minutes: Number of minutes between backups.
            seconds: Number of seconds between backups.
            args: Positional arguments to pass to backup_func.
            kwargs: Keyword arguments to pass to backup_func.
            replace_existing: Whether to replace an existing job with the same ID.
        
        Returns:
            The job ID of the scheduled backup.
        
        Raises:
            ValueError: If no interval is specified.
        """
        if hours == 0 and minutes == 0 and seconds == 0:
            raise ValueError("At least one of hours, minutes, or seconds must be specified")
        
        kwargs = kwargs or {}
        
        try:
            self.scheduler.add_job(
                backup_func,
                trigger=IntervalTrigger(hours=hours, minutes=minutes, seconds=seconds),
                id=job_id,
                args=args,
                kwargs=kwargs,
                replace_existing=replace_existing,
                name=f"Interval Backup: {job_id}"
            )
            logger.info(
                f"Scheduled interval backup '{job_id}' to run every "
                f"{hours}h {minutes}m {seconds}s"
            )
            return job_id
        except Exception as e:
            logger.error(f"Failed to schedule interval backup '{job_id}': {str(e)}")
            raise
    
    def schedule_cron_backup(
        self,
        backup_func: Callable,
        job_id: str,
        cron_expression: str,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        replace_existing: bool = True
    ) -> str:
        """
        Schedule a backup job using a cron expression.
        
        Args:
            backup_func: The function to call for backup execution.
            job_id: Unique identifier for this backup job.
            cron_expression: Cron expression for scheduling (e.g., "0 2 * * *" for 2 AM daily).
            args: Positional arguments to pass to backup_func.
            kwargs: Keyword arguments to pass to backup_func.
            replace_existing: Whether to replace an existing job with the same ID.
        
        Returns:
            The job ID of the scheduled backup.
        
        Example:
            "0 2 * * *" - 2 AM every day
            "0 */6 * * *" - Every 6 hours
            "0 0 * * MON" - Midnight every Monday
        """
        kwargs = kwargs or {}
        
        try:
            self.scheduler.add_job(
                backup_func,
                trigger=CronTrigger.from_crontab(cron_expression),
                id=job_id,
                args=args,
                kwargs=kwargs,
                replace_existing=replace_existing,
                name=f"Cron Backup: {job_id}"
            )
            logger.info(
                f"Scheduled cron backup '{job_id}' with expression: {cron_expression}"
            )
            return job_id
        except Exception as e:
            logger.error(f"Failed to schedule cron backup '{job_id}': {str(e)}")
            raise
    
    def start(self) -> None:
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Backup scheduler started")
        else:
            logger.warning("Scheduler is already running")
    
    def stop(self, wait: bool = True) -> None:
        """
        Stop the scheduler.
        
        Args:
            wait: If True, wait for all jobs to complete before stopping.
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("Backup scheduler stopped")
        else:
            logger.warning("Scheduler is not running")
    
    def pause(self) -> None:
        """Pause the scheduler without stopping it."""
        self.scheduler.pause()
        logger.info("Backup scheduler paused")
    
    def resume(self) -> None:
        """Resume the scheduler after pausing."""
        self.scheduler.resume()
        logger.info("Backup scheduler resumed")
    
    def get_job(self, job_id: str):
        """
        Get a specific job by ID.
        
        Args:
            job_id: The ID of the job to retrieve.
        
        Returns:
            The job object or None if not found.
        """
        return self.scheduler.get_job(job_id)
    
    def list_jobs(self) -> list:
        """
        List all scheduled jobs.
        
        Returns:
            A list of all scheduled backup jobs.
        """
        jobs = self.scheduler.get_jobs()
        logger.info(f"Total scheduled jobs: {len(jobs)}")
        return jobs
    
    def remove_job(self, job_id: str) -> bool:
        """
        Remove a scheduled backup job.
        
        Args:
            job_id: The ID of the job to remove.
        
        Returns:
            True if the job was removed, False otherwise.
        """
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed backup job: {job_id}")
            return True
        except JobLookupError:
            logger.warning(f"Job not found: {job_id}")
            return False
    
    def reschedule_job(
        self,
        job_id: str,
        trigger_type: str = "interval",
        **trigger_args
    ) -> bool:
        """
        Reschedule an existing job with new trigger parameters.
        
        Args:
            job_id: The ID of the job to reschedule.
            trigger_type: Type of trigger - "interval" or "cron".
            **trigger_args: Arguments for the trigger.
        
        Returns:
            True if rescheduled successfully, False otherwise.
        """
        try:
            if trigger_type == "interval":
                trigger = IntervalTrigger(**trigger_args)
            elif trigger_type == "cron":
                trigger = CronTrigger.from_crontab(trigger_args.get('cron_expression'))
            else:
                raise ValueError(f"Unknown trigger type: {trigger_type}")
            
            self.scheduler.reschedule_job(job_id, trigger=trigger)
            logger.info(f"Rescheduled job '{job_id}' with {trigger_type} trigger")
            return True
        except JobLookupError:
            logger.warning(f"Job not found: {job_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to reschedule job '{job_id}': {str(e)}")
            return False
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        Get the current status of the scheduler.
        
        Returns:
            A dictionary containing scheduler status information.
        """
        jobs = self.scheduler.get_jobs()
        return {
            'running': self.scheduler.running,
            'paused': self.scheduler.state == 'paused',
            'total_jobs': len(jobs),
            'jobs': [
                {
                    'id': job.id,
                    'name': job.name,
                    'trigger': str(job.trigger),
                    'next_run_time': job.next_run_time
                }
                for job in jobs
            ],
            'backup_directory': self.backup_dir,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


def default_backup_function(backup_dir: str) -> None:
    """
    Default backup function that creates a timestamp-based backup marker.
    
    Args:
        backup_dir: The directory where backup should be stored.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f"backup_{timestamp}.txt")
    
    try:
        with open(backup_file, 'w') as f:
            f.write(f"Backup created at {datetime.utcnow().isoformat()}\n")
        logger.info(f"Backup completed: {backup_file}")
    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")


if __name__ == "__main__":
    # Example usage
    manager = ScheduledBackupManager(backup_dir="./backups")
    
    # Start the scheduler
    manager.start()
    
    # Schedule a backup every 6 hours
    manager.schedule_interval_backup(
        default_backup_function,
        job_id="interval_backup_6h",
        hours=6,
        kwargs={'backup_dir': manager.backup_dir}
    )
    
    # Schedule a backup daily at 2 AM
    manager.schedule_cron_backup(
        default_backup_function,
        job_id="daily_backup_2am",
        cron_expression="0 2 * * *",
        kwargs={'backup_dir': manager.backup_dir}
    )
    
    # Display scheduler status
    import json
    status = manager.get_scheduler_status()
    print("Scheduler Status:")
    print(json.dumps(status, indent=2, default=str))
    
    # Keep the scheduler running
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()
        print("Scheduler stopped")
