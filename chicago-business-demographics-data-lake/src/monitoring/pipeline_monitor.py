"""
Pipeline Monitoring and Alerting System
Monitoring for the Chicago Business Demographics ETL pipeline
"""
import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import requests
from dataclasses import dataclass
from enum import Enum
import psutil
import docker
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import schedule

from config.settings import settings

logger = logging.getLogger(__name__)

class AlertLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class Alert:
    """Alert data structure"""
    alert_id: str
    level: AlertLevel
    message: str
    timestamp: datetime
    component: str
    metric_name: str
    threshold: float
    current_value: float
    resolved: bool = False

class PipelineMonitor:
    """Pipeline monitoring system"""
    
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.docker_client = docker.from_env()
        
        # Prometheus metrics
        self.processing_counter = Counter('pipeline_processing_total', 'Total processing events', ['status'])
        self.processing_duration = Histogram('pipeline_processing_duration_seconds', 'Processing duration')
        self.data_quality_gauge = Gauge('pipeline_data_quality_score', 'Data quality score')
        self.system_metrics = {
            'cpu_usage': Gauge('system_cpu_usage_percent', 'CPU usage percentage'),
            'memory_usage': Gauge('system_memory_usage_percent', 'Memory usage percentage'),
            'disk_usage': Gauge('system_disk_usage_percent', 'Disk usage percentage')
        }
        
        # Alert thresholds
        self.thresholds = {
            'data_quality': 95.0,
            'processing_time': 300.0,  # 5 minutes
            'error_rate': 5.0,  # 5%
            'cpu_usage': 80.0,
            'memory_usage': 85.0,
            'disk_usage': 90.0
        }
        
        # Alert handlers
        self.alert_handlers = {
            AlertLevel.INFO: self._handle_info_alert,
            AlertLevel.WARNING: self._handle_warning_alert,
            AlertLevel.ERROR: self._handle_error_alert,
            AlertLevel.CRITICAL: self._handle_critical_alert
        }
    
    def start_monitoring(self):
        """Start the monitoring system"""
        logger.info("Starting pipeline monitoring system...")
        
        # Start Prometheus metrics server
        start_http_server(8001)
        
        # Schedule monitoring tasks
        schedule.every(30).seconds.do(self._monitor_system_metrics)
        schedule.every(1).minutes.do(self._monitor_pipeline_health)
        schedule.every(5).minutes.do(self._monitor_data_quality)
        schedule.every(10).minutes.do(self._monitor_database_health)
        schedule.every(1).hours.do(self._generate_health_report)
        
        # Start monitoring loop
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def _monitor_system_metrics(self):
        """Monitor system resource usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.system_metrics['cpu_usage'].set(cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.system_metrics['memory_usage'].set(memory.percent)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.system_metrics['disk_usage'].set(disk_percent)
            
            # Check thresholds
            if cpu_percent > self.thresholds['cpu_usage']:
                self._create_alert(
                    AlertLevel.WARNING,
                    f"High CPU usage: {cpu_percent:.1f}%",
                    "system",
                    "cpu_usage",
                    self.thresholds['cpu_usage'],
                    cpu_percent
                )
            
            if memory.percent > self.thresholds['memory_usage']:
                self._create_alert(
                    AlertLevel.WARNING,
                    f"High memory usage: {memory.percent:.1f}%",
                    "system",
                    "memory_usage",
                    self.thresholds['memory_usage'],
                    memory.percent
                )
            
            if disk_percent > self.thresholds['disk_usage']:
                self._create_alert(
                    AlertLevel.CRITICAL,
                    f"High disk usage: {disk_percent:.1f}%",
                    "system",
                    "disk_usage",
                    self.thresholds['disk_usage'],
                    disk_percent
                )
                
        except Exception as e:
            logger.error(f"Error monitoring system metrics: {str(e)}")
    
    def _monitor_pipeline_health(self):
        """Monitor pipeline health and performance"""
        try:
            # Check processing status
            with self.engine.connect() as conn:
                # Get recent processing stats
                stats_query = text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                        AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) as avg_processing_time
                    FROM staging_business_owners 
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """)
                
                result = conn.execute(stats_query).fetchone()
                
                if result:
                    total = result.total_records
                    completed = result.completed
                    failed = result.failed
                    avg_time = result.avg_processing_time or 0
                    
                    # Calculate success rate
                    success_rate = (completed / total * 100) if total > 0 else 0
                    error_rate = (failed / total * 100) if total > 0 else 0
                    
                    # Update metrics
                    self.processing_counter.labels(status='completed').inc(completed)
                    self.processing_counter.labels(status='failed').inc(failed)
                    self.processing_duration.observe(avg_time)
                    
                    # Check thresholds
                    if error_rate > self.thresholds['error_rate']:
                        self._create_alert(
                            AlertLevel.ERROR,
                            f"High error rate: {error_rate:.1f}%",
                            "pipeline",
                            "error_rate",
                            self.thresholds['error_rate'],
                            error_rate
                        )
                    
                    if avg_time > self.thresholds['processing_time']:
                        self._create_alert(
                            AlertLevel.WARNING,
                            f"Slow processing: {avg_time:.1f}s average",
                            "pipeline",
                            "processing_time",
                            self.thresholds['processing_time'],
                            avg_time
                        )
                
        except Exception as e:
            logger.error(f"Error monitoring pipeline health: {str(e)}")
    
    def _monitor_data_quality(self):
        """Monitor data quality metrics"""
        try:
            with self.engine.connect() as conn:
                # Get data quality metrics
                quality_query = text("""
                    SELECT 
                        AVG(completeness_score) as avg_completeness,
                        COUNT(*) as total_quality_checks,
                        COUNT(CASE WHEN completeness_score < 90 THEN 1 END) as low_quality_checks
                    FROM staging_data_quality 
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """)
                
                result = conn.execute(quality_query).fetchone()
                
                if result:
                    avg_completeness = result.avg_completeness or 0
                    low_quality_count = result.low_quality_checks or 0
                    
                    # Update gauge
                    self.data_quality_gauge.set(avg_completeness)
                    
                    # Check quality threshold
                    if avg_completeness < self.thresholds['data_quality']:
                        self._create_alert(
                            AlertLevel.WARNING,
                            f"Low data quality: {avg_completeness:.1f}%",
                            "data_quality",
                            "completeness_score",
                            self.thresholds['data_quality'],
                            avg_completeness
                        )
                    
                    if low_quality_count > 10:
                        self._create_alert(
                            AlertLevel.ERROR,
                            f"Multiple quality issues: {low_quality_count} checks failed",
                            "data_quality",
                            "quality_checks",
                            10,
                            low_quality_count
                        )
                
        except Exception as e:
            logger.error(f"Error monitoring data quality: {str(e)}")
    
    def _monitor_database_health(self):
        """Monitor database health and performance"""
        try:
            with self.engine.connect() as conn:
                # Check database connections
                conn_test = conn.execute(text("SELECT 1")).fetchone()
                
                # Check table sizes
                size_query = text("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    LIMIT 10
                """)
                
                sizes = conn.execute(size_query).fetchall()
                
                # Check for large tables
                for size in sizes:
                    if 'GB' in size.size:
                        self._create_alert(
                            AlertLevel.INFO,
                            f"Large table detected: {size.tablename} ({size.size})",
                            "database",
                            "table_size",
                            0,
                            0
                        )
                
                # Check for long-running queries
                long_queries = conn.execute(text("""
                    SELECT COUNT(*) as long_queries
                    FROM pg_stat_activity 
                    WHERE state = 'active' 
                    AND query_start < NOW() - INTERVAL '5 minutes'
                """)).fetchone()
                
                if long_queries.long_queries > 0:
                    self._create_alert(
                        AlertLevel.WARNING,
                        f"Long-running queries detected: {long_queries.long_queries}",
                        "database",
                        "long_queries",
                        0,
                        long_queries.long_queries
                    )
                
        except Exception as e:
            logger.error(f"Error monitoring database health: {str(e)}")
            self._create_alert(
                AlertLevel.CRITICAL,
                f"Database connection failed: {str(e)}",
                "database",
                "connection",
                0,
                0
            )
    
    def _generate_health_report(self):
        """Generate health report"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'system_metrics': self._get_system_metrics(),
                'pipeline_metrics': self._get_pipeline_metrics(),
                'data_quality_metrics': self._get_data_quality_metrics(),
                'database_metrics': self._get_database_metrics(),
                'alerts': self._get_recent_alerts()
            }
            
            # Save report
            with open(f"health_reports/health_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info("Health report generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating health report: {str(e)}")
    
    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        return {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent,
            'load_average': psutil.getloadavg()
        }
    
    def _get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        COUNT(*) as total_processed,
                        AVG(EXTRACT(EPOCH FROM (processed_at - created_at))) as avg_processing_time,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                    FROM staging_business_owners 
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                """)
                
                result = conn.execute(query).fetchone()
                
                return {
                    'total_processed': result.total_processed,
                    'avg_processing_time': result.avg_processing_time,
                    'success_rate': (result.successful / result.total_processed * 100) if result.total_processed > 0 else 0,
                    'error_rate': (result.failed / result.total_processed * 100) if result.total_processed > 0 else 0
                }
        except Exception as e:
            logger.error(f"Error getting pipeline metrics: {str(e)}")
            return {}
    
    def _get_data_quality_metrics(self) -> Dict[str, Any]:
        """Get data quality metrics"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        AVG(completeness_score) as avg_completeness,
                        MIN(completeness_score) as min_completeness,
                        MAX(completeness_score) as max_completeness,
                        COUNT(*) as total_checks
                    FROM staging_data_quality 
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                """)
                
                result = conn.execute(query).fetchone()
                
                return {
                    'avg_completeness': result.avg_completeness,
                    'min_completeness': result.min_completeness,
                    'max_completeness': result.max_completeness,
                    'total_checks': result.total_checks
                }
        except Exception as e:
            logger.error(f"Error getting data quality metrics: {str(e)}")
            return {}
    
    def _get_database_metrics(self) -> Dict[str, Any]:
        """Get database performance metrics"""
        try:
            with self.engine.connect() as conn:
                # Get table sizes
                size_query = text("""
                    SELECT 
                        COUNT(*) as total_tables,
                        SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total_size_bytes
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                """)
                
                result = conn.execute(size_query).fetchone()
                
                return {
                    'total_tables': result.total_tables,
                    'total_size_mb': result.total_size_bytes / (1024 * 1024) if result.total_size_bytes else 0,
                    'connection_status': 'healthy'
                }
        except Exception as e:
            logger.error(f"Error getting database metrics: {str(e)}")
            return {'connection_status': 'unhealthy', 'error': str(e)}
    
    def _get_recent_alerts(self) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT 
                        alert_id,
                        level,
                        message,
                        timestamp,
                        component,
                        resolved
                    FROM pipeline_alerts 
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    ORDER BY timestamp DESC
                    LIMIT 50
                """)
                
                result = conn.execute(query).fetchall()
                
                return [
                    {
                        'alert_id': row.alert_id,
                        'level': row.level,
                        'message': row.message,
                        'timestamp': row.timestamp.isoformat(),
                        'component': row.component,
                        'resolved': row.resolved
                    }
                    for row in result
                ]
        except Exception as e:
            logger.error(f"Error getting recent alerts: {str(e)}")
            return []
    
    def _create_alert(self, level: AlertLevel, message: str, component: str, 
                     metric_name: str, threshold: float, current_value: float):
        """Create and handle alert"""
        alert = Alert(
            alert_id=f"alert_{int(time.time())}",
            level=level,
            message=message,
            timestamp=datetime.now(),
            component=component,
            metric_name=metric_name,
            threshold=threshold,
            current_value=current_value
        )
        
        # Store alert in database
        self._store_alert(alert)
        
        # Handle alert
        handler = self.alert_handlers.get(level)
        if handler:
            handler(alert)
    
    def _store_alert(self, alert: Alert):
        """Store alert in database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO pipeline_alerts 
                    (alert_id, level, message, timestamp, component, metric_name, threshold, current_value, resolved)
                    VALUES (:alert_id, :level, :message, :timestamp, :component, :metric_name, :threshold, :current_value, :resolved)
                """), {
                    'alert_id': alert.alert_id,
                    'level': alert.level.value,
                    'message': alert.message,
                    'timestamp': alert.timestamp,
                    'component': alert.component,
                    'metric_name': alert.metric_name,
                    'threshold': alert.threshold,
                    'current_value': alert.current_value,
                    'resolved': alert.resolved
                })
                conn.commit()
        except Exception as e:
            logger.error(f"Error storing alert: {str(e)}")
    
    def _handle_info_alert(self, alert: Alert):
        """Handle info level alert"""
        logger.info(f"INFO ALERT: {alert.message}")
    
    def _handle_warning_alert(self, alert: Alert):
        """Handle warning level alert"""
        logger.warning(f"WARNING ALERT: {alert.message}")
        # Send notification to monitoring team
    
    def _handle_error_alert(self, alert: Alert):
        """Handle error level alert"""
        logger.error(f"ERROR ALERT: {alert.message}")
        # Send email notification
        self._send_email_notification(alert)
    
    def _handle_critical_alert(self, alert: Alert):
        """Handle critical level alert"""
        logger.critical(f"CRITICAL ALERT: {alert.message}")
        # Send immediate notification
        self._send_email_notification(alert)
        self._send_slack_notification(alert)
    
    def _send_email_notification(self, alert: Alert):
        """Send email notification for alert"""
        try:
            # Email configuration (would be in settings)
            smtp_server = "smtp.gmail.com"
            smtp_port = 587
            sender_email = "monitoring@company.com"
            receiver_email = "admin@company.com"
            password = "password"
            
            msg = MimeMultipart()
            msg['From'] = sender_email
            msg['To'] = receiver_email
            msg['Subject'] = f"Pipeline Alert: {alert.level.value.upper()}"
            
            body = f"""
            Alert Details:
            - Level: {alert.level.value.upper()}
            - Component: {alert.component}
            - Message: {alert.message}
            - Timestamp: {alert.timestamp}
            - Metric: {alert.metric_name}
            - Current Value: {alert.current_value}
            - Threshold: {alert.threshold}
            """
            
            msg.attach(MimeText(body, 'plain'))
            
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent for alert {alert.alert_id}")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
    
    def _send_slack_notification(self, alert: Alert):
        """Send Slack notification for critical alerts"""
        try:
            webhook_url = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
            
            payload = {
                "text": f"🚨 CRITICAL ALERT: {alert.message}",
                "attachments": [
                    {
                        "color": "danger",
                        "fields": [
                            {"title": "Component", "value": alert.component, "short": True},
                            {"title": "Level", "value": alert.level.value.upper(), "short": True},
                            {"title": "Timestamp", "value": alert.timestamp.isoformat(), "short": True}
                        ]
                    }
                ]
            }
            
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            
            logger.info(f"Slack notification sent for alert {alert.alert_id}")
            
        except Exception as e:
            logger.error(f"Error sending Slack notification: {str(e)}")

if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.start_monitoring()
