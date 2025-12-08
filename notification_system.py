"""
Comprehensive Email Notification and Alerting System for Migration Operations
Includes SMTP support, alert rules, notification templates, SQLite persistence,
background worker threads, and retry logic.

Date: 2025-12-08
"""

import sqlite3
import smtplib
import threading
import queue
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABC, abstractmethod
import hashlib
import random


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class NotificationStatus(Enum):
    """Notification delivery status"""
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    ARCHIVED = "ARCHIVED"


class AlertRuleType(Enum):
    """Types of alert rules"""
    THRESHOLD = "THRESHOLD"
    EVENT = "EVENT"
    TIME_BASED = "TIME_BASED"
    PATTERN = "PATTERN"


@dataclass
class NotificationTemplate:
    """Template for notification messages"""
    name: str
    subject: str
    body: str
    template_type: str = "html"
    variables: List[str] = field(default_factory=list)
    
    def render(self, context: Dict[str, Any]) -> tuple[str, str]:
        """Render template with context variables"""
        subject = self.subject
        body = self.body
        
        for var, value in context.items():
            placeholder = f"{{{{{var}}}}}"
            subject = subject.replace(placeholder, str(value))
            body = body.replace(placeholder, str(value))
        
        return subject, body


@dataclass
class AlertRule:
    """Rule for triggering alerts"""
    name: str
    rule_type: AlertRuleType
    condition: Callable[[Dict[str, Any]], bool]
    notification_level: NotificationLevel
    recipients: List[str]
    enabled: bool = True
    description: str = ""
    
    def matches(self, event_data: Dict[str, Any]) -> bool:
        """Check if event matches rule condition"""
        try:
            return self.condition(event_data)
        except Exception as e:
            logger.error(f"Error evaluating rule '{self.name}': {e}")
            return False


@dataclass
class Notification:
    """Notification object"""
    id: str
    level: NotificationLevel
    subject: str
    body: str
    recipients: List[str]
    status: NotificationStatus = NotificationStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    sent_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    next_retry: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        data['level'] = self.level.value
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        data['sent_at'] = self.sent_at.isoformat() if self.sent_at else None
        data['next_retry'] = self.next_retry.isoformat() if self.next_retry else None
        return data


class NotificationDatabase:
    """SQLite database for notification persistence"""
    
    def __init__(self, db_path: str = "notifications.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Notifications table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id TEXT PRIMARY KEY,
                    level TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    recipients TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    sent_at TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    next_retry TEXT,
                    error_message TEXT,
                    metadata TEXT,
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at),
                    INDEX idx_level (level)
                )
            """)
            
            # Alert rules table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alert_rules (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    rule_type TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    description TEXT,
                    recipients TEXT NOT NULL,
                    notification_level TEXT NOT NULL,
                    condition_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    INDEX idx_enabled (enabled)
                )
            """)
            
            # Notification audit log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_id TEXT NOT NULL,
                    event TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    details TEXT,
                    FOREIGN KEY (notification_id) REFERENCES notifications(id)
                )
            """)
            
            conn.commit()
    
    def save_notification(self, notification: Notification) -> bool:
        """Save notification to database"""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO notifications
                        (id, level, subject, body, recipients, status, created_at, 
                         sent_at, retry_count, max_retries, next_retry, error_message, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        notification.id,
                        notification.level.value,
                        notification.subject,
                        notification.body,
                        json.dumps(notification.recipients),
                        notification.status.value,
                        notification.created_at.isoformat(),
                        notification.sent_at.isoformat() if notification.sent_at else None,
                        notification.retry_count,
                        notification.max_retries,
                        notification.next_retry.isoformat() if notification.next_retry else None,
                        notification.error_message,
                        json.dumps(notification.metadata)
                    ))
                    
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving notification: {e}")
            return False
    
    def get_notification(self, notification_id: str) -> Optional[Notification]:
        """Retrieve notification from database"""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM notifications WHERE id = ?", (notification_id,))
                    row = cursor.fetchone()
                    
                    if row:
                        return self._row_to_notification(row)
        except Exception as e:
            logger.error(f"Error retrieving notification: {e}")
        
        return None
    
    def get_pending_notifications(self) -> List[Notification]:
        """Get pending and retrying notifications"""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT * FROM notifications 
                        WHERE status IN (?, ?) 
                        ORDER BY created_at ASC
                    """, (NotificationStatus.PENDING.value, NotificationStatus.RETRYING.value))
                    
                    rows = cursor.fetchall()
                    return [self._row_to_notification(row) for row in rows]
        except Exception as e:
            logger.error(f"Error retrieving pending notifications: {e}")
        
        return []
    
    def get_notifications_by_status(self, status: NotificationStatus, 
                                    limit: int = 100) -> List[Notification]:
        """Get notifications by status"""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT * FROM notifications 
                        WHERE status = ? 
                        ORDER BY created_at DESC 
                        LIMIT ?
                    """, (status.value, limit))
                    
                    rows = cursor.fetchall()
                    return [self._row_to_notification(row) for row in rows]
        except Exception as e:
            logger.error(f"Error retrieving notifications by status: {e}")
        
        return []
    
    def log_audit_event(self, notification_id: str, event: str, 
                       details: Optional[Dict] = None) -> bool:
        """Log audit event for notification"""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO notification_audit
                        (notification_id, event, timestamp, details)
                        VALUES (?, ?, ?, ?)
                    """, (
                        notification_id,
                        event,
                        datetime.utcnow().isoformat(),
                        json.dumps(details) if details else None
                    ))
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error logging audit event: {e}")
            return False
    
    @staticmethod
    def _row_to_notification(row: tuple) -> Notification:
        """Convert database row to Notification object"""
        return Notification(
            id=row[0],
            level=NotificationLevel(row[1]),
            subject=row[2],
            body=row[3],
            recipients=json.loads(row[4]),
            status=NotificationStatus(row[5]),
            created_at=datetime.fromisoformat(row[6]),
            sent_at=datetime.fromisoformat(row[7]) if row[7] else None,
            retry_count=row[8],
            max_retries=row[9],
            next_retry=datetime.fromisoformat(row[10]) if row[10] else None,
            error_message=row[11],
            metadata=json.loads(row[12]) if row[12] else {}
        )
    
    def cleanup_old_notifications(self, days: int = 30) -> int:
        """Archive and remove old notifications"""
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Archive old notifications
                    cursor.execute("""
                        UPDATE notifications 
                        SET status = ? 
                        WHERE created_at < ? AND status = ?
                    """, (NotificationStatus.ARCHIVED.value, cutoff_date, 
                          NotificationStatus.SENT.value))
                    
                    conn.commit()
                    return cursor.rowcount
        except Exception as e:
            logger.error(f"Error cleaning up old notifications: {e}")
        
        return 0


class SMTPEmailSender:
    """SMTP email sender for notifications"""
    
    def __init__(self, smtp_host: str, smtp_port: int, username: str, password: str,
                 use_tls: bool = True, from_address: str = None):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.from_address = from_address or username
        self.lock = threading.Lock()
    
    def send_email(self, to_addresses: List[str], subject: str, body: str,
                   is_html: bool = True) -> tuple[bool, Optional[str]]:
        """Send email via SMTP"""
        try:
            with self.lock:
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = self.from_address
                msg['To'] = ', '.join(to_addresses)
                
                # Attach body
                mime_type = 'html' if is_html else 'plain'
                msg.attach(MIMEText(body, mime_type))
                
                # Send email
                with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                    if self.use_tls:
                        server.starttls()
                    
                    server.login(self.username, self.password)
                    server.sendmail(self.from_address, to_addresses, msg.as_string())
                
                logger.info(f"Email sent to {', '.join(to_addresses)}")
                return True, None
        
        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Email sending error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg


class NotificationTemplateManager:
    """Manager for notification templates"""
    
    def __init__(self):
        self.templates: Dict[str, NotificationTemplate] = {}
        self._initialize_default_templates()
    
    def _initialize_default_templates(self):
        """Initialize default templates"""
        
        # Migration started template
        self.register_template(NotificationTemplate(
            name="migration_started",
            subject="Migration Operation Started - {{migration_id}}",
            body="""
            <html>
            <body>
                <h2>Migration Operation Started</h2>
                <p><strong>Migration ID:</strong> {{migration_id}}</p>
                <p><strong>Start Time:</strong> {{start_time}}</p>
                <p><strong>Source:</strong> {{source}}</p>
                <p><strong>Destination:</strong> {{destination}}</p>
                <p>The migration operation has been initiated successfully.</p>
            </body>
            </html>
            """,
            variables=["migration_id", "start_time", "source", "destination"]
        ))
        
        # Migration completed template
        self.register_template(NotificationTemplate(
            name="migration_completed",
            subject="Migration Operation Completed - {{migration_id}}",
            body="""
            <html>
            <body>
                <h2>Migration Operation Completed</h2>
                <p><strong>Migration ID:</strong> {{migration_id}}</p>
                <p><strong>Status:</strong> {{status}}</p>
                <p><strong>Duration:</strong> {{duration}}</p>
                <p><strong>Items Migrated:</strong> {{items_count}}</p>
                <p><strong>Completion Time:</strong> {{completion_time}}</p>
                <p>{{additional_info}}</p>
            </body>
            </html>
            """,
            variables=["migration_id", "status", "duration", "items_count", "completion_time", "additional_info"]
        ))
        
        # Migration error template
        self.register_template(NotificationTemplate(
            name="migration_error",
            subject="Migration Operation Failed - {{migration_id}}",
            body="""
            <html>
            <body>
                <h2>Migration Operation Failed</h2>
                <p><strong>Migration ID:</strong> {{migration_id}}</p>
                <p><strong>Error:</strong> {{error_message}}</p>
                <p><strong>Failed Items:</strong> {{failed_count}}</p>
                <p><strong>Failure Time:</strong> {{failure_time}}</p>
                <p><strong>Recommended Action:</strong> {{action}}</p>
                <p>Please review the error details and take appropriate action.</p>
            </body>
            </html>
            """,
            variables=["migration_id", "error_message", "failed_count", "failure_time", "action"]
        ))
        
        # Performance alert template
        self.register_template(NotificationTemplate(
            name="performance_alert",
            subject="Performance Alert - {{alert_type}}",
            body="""
            <html>
            <body>
                <h2>Performance Alert</h2>
                <p><strong>Alert Type:</strong> {{alert_type}}</p>
                <p><strong>Metric:</strong> {{metric}}</p>
                <p><strong>Current Value:</strong> {{current_value}}</p>
                <p><strong>Threshold:</strong> {{threshold}}</p>
                <p><strong>Timestamp:</strong> {{timestamp}}</p>
                <p>Performance monitoring has detected an issue that requires attention.</p>
            </body>
            </html>
            """,
            variables=["alert_type", "metric", "current_value", "threshold", "timestamp"]
        ))
        
        # Resource warning template
        self.register_template(NotificationTemplate(
            name="resource_warning",
            subject="Resource Warning - {{resource_type}}",
            body="""
            <html>
            <body>
                <h2>Resource Warning</h2>
                <p><strong>Resource Type:</strong> {{resource_type}}</p>
                <p><strong>Usage Level:</strong> {{usage_level}}%</p>
                <p><strong>Warning Threshold:</strong> {{threshold}}%</p>
                <p><strong>Timestamp:</strong> {{timestamp}}</p>
                <p>Resource usage has exceeded acceptable limits.</p>
            </body>
            </html>
            """,
            variables=["resource_type", "usage_level", "threshold", "timestamp"]
        ))
    
    def register_template(self, template: NotificationTemplate):
        """Register a notification template"""
        self.templates[template.name] = template
        logger.info(f"Template registered: {template.name}")
    
    def get_template(self, name: str) -> Optional[NotificationTemplate]:
        """Get template by name"""
        return self.templates.get(name)
    
    def list_templates(self) -> List[str]:
        """List all registered templates"""
        return list(self.templates.keys())


class AlertRuleManager:
    """Manager for alert rules"""
    
    def __init__(self, db: NotificationDatabase):
        self.db = db
        self.rules: Dict[str, AlertRule] = {}
    
    def register_rule(self, rule: AlertRule) -> bool:
        """Register an alert rule"""
        try:
            self.rules[rule.name] = rule
            logger.info(f"Alert rule registered: {rule.name}")
            return True
        except Exception as e:
            logger.error(f"Error registering rule: {e}")
            return False
    
    def get_rule(self, name: str) -> Optional[AlertRule]:
        """Get rule by name"""
        return self.rules.get(name)
    
    def list_rules(self) -> List[AlertRule]:
        """List all rules"""
        return list(self.rules.values())
    
    def get_enabled_rules(self) -> List[AlertRule]:
        """Get enabled rules"""
        return [r for r in self.rules.values() if r.enabled]
    
    def disable_rule(self, name: str) -> bool:
        """Disable a rule"""
        rule = self.get_rule(name)
        if rule:
            rule.enabled = False
            return True
        return False
    
    def enable_rule(self, name: str) -> bool:
        """Enable a rule"""
        rule = self.get_rule(name)
        if rule:
            rule.enabled = True
            return True
        return False
    
    def evaluate_event(self, event_data: Dict[str, Any]) -> List[AlertRule]:
        """Evaluate event against all enabled rules"""
        matched_rules = []
        for rule in self.get_enabled_rules():
            if rule.matches(event_data):
                matched_rules.append(rule)
        return matched_rules


class NotificationWorker:
    """Background worker thread for processing notifications"""
    
    def __init__(self, db: NotificationDatabase, email_sender: Optional[SMTPEmailSender] = None,
                 process_interval: int = 5):
        self.db = db
        self.email_sender = email_sender
        self.process_interval = process_interval
        self.queue: queue.Queue = queue.Queue()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.stats = {
            'sent': 0,
            'failed': 0,
            'retried': 0,
            'processed': 0
        }
    
    def start(self):
        """Start worker thread"""
        if self.running:
            logger.warning("Worker is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        logger.info("Notification worker started")
    
    def stop(self):
        """Stop worker thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        logger.info("Notification worker stopped")
    
    def add_notification(self, notification: Notification):
        """Add notification to processing queue"""
        self.queue.put(notification)
    
    def _worker_loop(self):
        """Main worker loop"""
        while self.running:
            try:
                # Process queue items
                try:
                    notification = self.queue.get(timeout=1)
                    self._process_notification(notification)
                except queue.Empty:
                    pass
                
                # Process pending notifications from database
                pending = self.db.get_pending_notifications()
                for notification in pending:
                    # Check if retry time has arrived
                    if notification.next_retry and notification.next_retry > datetime.utcnow():
                        continue
                    
                    self._process_notification(notification)
                
                time.sleep(self.process_interval)
            
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(self.process_interval)
    
    def _process_notification(self, notification: Notification):
        """Process a single notification"""
        try:
            if notification.status == NotificationStatus.SENT:
                return
            
            # Send notification
            success = self._send_notification(notification)
            
            if success:
                notification.status = NotificationStatus.SENT
                notification.sent_at = datetime.utcnow()
                self.stats['sent'] += 1
                self.db.log_audit_event(notification.id, "SENT", {"timestamp": datetime.utcnow().isoformat()})
            else:
                # Handle retry logic
                if notification.retry_count < notification.max_retries:
                    notification.retry_count += 1
                    notification.status = NotificationStatus.RETRYING
                    # Exponential backoff: wait 2^retry_count minutes
                    backoff_minutes = min(2 ** notification.retry_count, 60)
                    notification.next_retry = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                    self.stats['retried'] += 1
                    logger.info(f"Notification {notification.id} scheduled for retry in {backoff_minutes} minutes")
                else:
                    notification.status = NotificationStatus.FAILED
                    self.stats['failed'] += 1
                    logger.error(f"Notification {notification.id} failed after {notification.max_retries} retries")
            
            # Save updated notification
            self.db.save_notification(notification)
            self.stats['processed'] += 1
        
        except Exception as e:
            logger.error(f"Error processing notification: {e}")
    
    def _send_notification(self, notification: Notification) -> bool:
        """Send notification (email)"""
        if not self.email_sender:
            logger.warning("Email sender not configured")
            return False
        
        try:
            success, error = self.email_sender.send_email(
                to_addresses=notification.recipients,
                subject=notification.subject,
                body=notification.body,
                is_html=True
            )
            
            if not success:
                notification.error_message = error
            
            return success
        except Exception as e:
            notification.error_message = str(e)
            logger.error(f"Error sending notification: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get worker statistics"""
        return self.stats.copy()


class NotificationSystem:
    """Main notification system coordinator"""
    
    def __init__(self, db_path: str = "notifications.db", 
                 email_config: Optional[Dict] = None):
        self.db = NotificationDatabase(db_path)
        self.template_manager = NotificationTemplateManager()
        self.rule_manager = AlertRuleManager(self.db)
        
        # Initialize email sender if configured
        self.email_sender: Optional[SMTPEmailSender] = None
        if email_config:
            self.email_sender = SMTPEmailSender(
                smtp_host=email_config['host'],
                smtp_port=email_config['port'],
                username=email_config['username'],
                password=email_config['password'],
                use_tls=email_config.get('use_tls', True),
                from_address=email_config.get('from_address')
            )
        
        # Initialize worker
        self.worker = NotificationWorker(self.db, self.email_sender)
    
    def start(self):
        """Start notification system"""
        self.worker.start()
        logger.info("Notification system started")
    
    def stop(self):
        """Stop notification system"""
        self.worker.stop()
        logger.info("Notification system stopped")
    
    def generate_notification_id(self) -> str:
        """Generate unique notification ID"""
        timestamp = datetime.utcnow().isoformat()
        random_suffix = hashlib.md5(f"{timestamp}{random.random()}".encode()).hexdigest()[:8]
        return f"notif_{int(datetime.utcnow().timestamp())}_{random_suffix}"
    
    def send_notification(self, level: NotificationLevel, recipients: List[str],
                         subject: str, body: str, metadata: Dict = None) -> str:
        """Send a notification"""
        notification = Notification(
            id=self.generate_notification_id(),
            level=level,
            subject=subject,
            body=body,
            recipients=recipients,
            metadata=metadata or {}
        )
        
        self.db.save_notification(notification)
        self.worker.add_notification(notification)
        logger.info(f"Notification queued: {notification.id}")
        
        return notification.id
    
    def send_from_template(self, template_name: str, recipients: List[str],
                          context: Dict[str, Any], level: NotificationLevel = None,
                          metadata: Dict = None) -> Optional[str]:
        """Send notification using template"""
        template = self.template_manager.get_template(template_name)
        if not template:
            logger.error(f"Template not found: {template_name}")
            return None
        
        subject, body = template.render(context)
        return self.send_notification(
            level=level or NotificationLevel.INFO,
            recipients=recipients,
            subject=subject,
            body=body,
            metadata=metadata
        )
    
    def handle_event(self, event_data: Dict[str, Any]) -> List[str]:
        """Handle event and trigger matching alert rules"""
        notification_ids = []
        matched_rules = self.rule_manager.evaluate_event(event_data)
        
        for rule in matched_rules:
            notification_id = self.send_notification(
                level=rule.notification_level,
                recipients=rule.recipients,
                subject=f"Alert: {rule.name}",
                body=f"Alert rule '{rule.name}' triggered:\n\n{json.dumps(event_data, indent=2)}",
                metadata={'rule_name': rule.name, 'rule_type': rule.rule_type.value}
            )
            notification_ids.append(notification_id)
            logger.info(f"Alert triggered by rule '{rule.name}'")
        
        return notification_ids
    
    def get_notification(self, notification_id: str) -> Optional[Dict]:
        """Get notification details"""
        notification = self.db.get_notification(notification_id)
        return notification.to_dict() if notification else None
    
    def get_notifications_by_status(self, status: NotificationStatus, 
                                   limit: int = 100) -> List[Dict]:
        """Get notifications by status"""
        notifications = self.db.get_notifications_by_status(status, limit)
        return [n.to_dict() for n in notifications]
    
    def get_worker_stats(self) -> Dict:
        """Get worker statistics"""
        return self.worker.get_stats()
    
    def cleanup_old_notifications(self, days: int = 30) -> int:
        """Clean up old notifications"""
        return self.db.cleanup_old_notifications(days)
    
    def register_alert_rule(self, rule: AlertRule) -> bool:
        """Register alert rule"""
        return self.rule_manager.register_rule(rule)
    
    def register_template(self, template: NotificationTemplate):
        """Register notification template"""
        self.template_manager.register_template(template)
    
    def list_templates(self) -> List[str]:
        """List available templates"""
        return self.template_manager.list_templates()
    
    def list_alert_rules(self) -> List[Dict]:
        """List alert rules"""
        return [
            {
                'name': r.name,
                'type': r.rule_type.value,
                'enabled': r.enabled,
                'description': r.description,
                'notification_level': r.notification_level.value
            }
            for r in self.rule_manager.list_rules()
        ]


# Example usage and initialization
if __name__ == "__main__":
    # Initialize notification system
    email_config = {
        'host': 'smtp.gmail.com',
        'port': 587,
        'username': 'your-email@gmail.com',
        'password': 'your-password',
        'use_tls': True,
        'from_address': 'migrations@example.com'
    }
    
    system = NotificationSystem(email_config=email_config)
    
    # Register custom alert rules
    
    # Rule 1: Alert when migration fails
    def migration_failure_condition(event: Dict) -> bool:
        return event.get('event_type') == 'migration_failed'
    
    migration_error_rule = AlertRule(
        name="migration_failure",
        rule_type=AlertRuleType.EVENT,
        condition=migration_failure_condition,
        notification_level=NotificationLevel.CRITICAL,
        recipients=['admin@example.com'],
        description="Alert when migration operation fails"
    )
    system.register_alert_rule(migration_error_rule)
    
    # Rule 2: Alert on high memory usage
    def high_memory_condition(event: Dict) -> bool:
        return event.get('metric') == 'memory' and float(event.get('value', 0)) > 85
    
    high_memory_rule = AlertRule(
        name="high_memory_usage",
        rule_type=AlertRuleType.THRESHOLD,
        condition=high_memory_condition,
        notification_level=NotificationLevel.WARNING,
        recipients=['devops@example.com'],
        description="Alert when memory usage exceeds 85%"
    )
    system.register_alert_rule(high_memory_rule)
    
    # Start system
    system.start()
    
    logger.info("Notification system example initialized")
    logger.info(f"Available templates: {system.list_templates()}")
    logger.info(f"Registered rules: {system.list_alert_rules()}")
