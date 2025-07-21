#!/usr/bin/env python3
"""
Advanced Data Quality Monitoring System for ZMS Central Analytics Engineering
=====================================================================

This module provides comprehensive data quality monitoring, alerting, and 
anomaly detection capabilities for the dbt ecommerce analytics platform.

Features:
- Real-time data quality monitoring
- Automated anomaly detection
- Slack/email alerting
- Performance metrics tracking
- Data lineage validation
- Custom quality rules engine

Author: Senior Analytics Engineer
Date: 2024
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import warnings
warnings.filterwarnings('ignore')

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DataQualityResult:
    """Data class to store quality check results"""
    check_name: str
    table_name: str
    column_name: Optional[str]
    check_type: str
    status: str  # 'PASS', 'FAIL', 'WARN'
    value: float
    threshold: float
    message: str
    timestamp: datetime
    severity: str  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data

@dataclass
class AnomalyResult:
    """Data class to store anomaly detection results"""
    table_name: str
    metric_name: str
    current_value: float
    expected_value: float
    deviation: float
    anomaly_score: float
    is_anomaly: bool
    timestamp: datetime
    context: Dict[str, Any]

class DataQualityMonitor:
    """
    Advanced data quality monitoring system with anomaly detection
    and automated alerting capabilities.
    """
    
    def __init__(self, db_path: str, config_path: str = "quality_config.json"):
        self.db_path = Path(db_path)
        self.config_path = Path(config_path)
        self.quality_results: List[DataQualityResult] = []
        self.anomaly_results: List[AnomalyResult] = []
        self.load_configuration()
        self.setup_database()
        
    def load_configuration(self) -> None:
        """Load configuration from JSON file"""
        default_config = {
            "thresholds": {
                "data_completeness": 0.95,
                "data_freshness_hours": 24,
                "revenue_anomaly_threshold": 0.3,
                "order_volume_anomaly_threshold": 0.25,
                "margin_anomaly_threshold": 0.15
            },
            "alerting": {
                "slack_webhook": None,
                "email_config": {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "sender_email": None,
                    "sender_password": None,
                    "recipient_emails": []
                }
            },
            "monitoring_tables": [
                "mart_partner_performance_dashboard",
                "mart_financial_performance_summary",
                "mart_partner_optimization_insights",
                "mart_cohort_analysis",
                "dim_customer_segments",
                "dim_product_intelligence"
            ]
        }
        
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
        else:
            self.config = default_config
            self.save_configuration()
    
    def save_configuration(self) -> None:
        """Save configuration to JSON file"""
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def setup_database(self) -> None:
        """Initialize database tables for monitoring"""
        with sqlite3.connect(self.db_path) as conn:
            # Create a table to store data quality results
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT,
                    check_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    value REAL NOT NULL,
                    threshold REAL NOT NULL,
                    message TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    severity TEXT NOT NULL
                )
            """)
            
            # Create a table to store anomaly detection results
            conn.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    current_value REAL NOT NULL,
                    expected_value REAL NOT NULL,
                    deviation REAL NOT NULL,
                    anomaly_score REAL NOT NULL,
                    is_anomaly BOOLEAN NOT NULL,
                    timestamp TEXT NOT NULL,
                    context TEXT NOT NULL
                )
            """)
            
            # Create a table to store general monitoring metrics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monitoring_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    metadata TEXT
                )
            """)
    
    def run_data_quality_checks(self) -> List[DataQualityResult]:
        """Execute comprehensive data quality checks"""
        logger.info("Starting data quality checks...")
        self.quality_results = []
        
        with sqlite3.connect(self.db_path) as conn:
            for table_name in self.config["monitoring_tables"]:
                try:
                    # Check if a table exists in the database
                    if not self._table_exists(conn, table_name):
                        logger.warning(f"Table {table_name} does not exist, skipping checks")
                        continue
                    
                    # Execute a series of data quality checks
                    self._check_data_completeness(conn, table_name)
                    self._check_data_freshness(conn, table_name)
                    self._check_data_uniqueness(conn, table_name)
                    self._check_data_validity(conn, table_name)
                    self._check_business_rules(conn, table_name)
                    
                except Exception as e:
                    logger.error(f"Error checking table {table_name}: {str(e)}")
                    self._add_quality_result(
                        check_name="table_accessibility",
                        table_name=table_name,
                        check_type="SYSTEM",
                        status="FAIL",
                        value=0,
                        threshold=1,
                        message=f"Table check failed: {str(e)}",
                        severity="HIGH"
                    )
        
        # Save the quality check results to the database
        self._store_quality_results()
        
        logger.info(f"Completed data quality checks. Found {len([r for r in self.quality_results if r.status == 'FAIL'])} failures")
        return self.quality_results
    
    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        """Check if table exists in database"""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None
    
    def _check_data_completeness(self, conn: sqlite3.Connection, table_name: str) -> None:
        """Check data completeness for critical columns"""
        try:
            # Retrieve the schema for the specified table
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Check for null values in each column
            for column in columns:
                if column.endswith('_key') or column in ['revenue_30d', 'overall_health_score', 'order_date']:
                    cursor = conn.execute(f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT({column}) as non_null_rows,
                            CAST(COUNT({column}) AS FLOAT) / COUNT(*) as completeness_rate
                        FROM {table_name}
                    """
                    )
                    
                    result = cursor.fetchone()
                    if result:
                        total_rows, non_null_rows, completeness_rate = result
                        
                        status = "PASS" if completeness_rate >= self.config["thresholds"]["data_completeness"] else "FAIL"
                        severity = "HIGH" if completeness_rate < 0.8 else "MEDIUM" if completeness_rate < 0.95 else "LOW"
                        
                        self._add_quality_result(
                            check_name="data_completeness",
                            table_name=table_name,
                            column_name=column,
                            check_type="COMPLETENESS",
                            status=status,
                            value=completeness_rate,
                            threshold=self.config["thresholds"]["data_completeness"],
                            message=f"Completeness rate: {completeness_rate:.2%} ({non_null_rows}/{total_rows})",
                            severity=severity
                        )
        
        except Exception as e:
            logger.error(f"Error checking completeness for {table_name}: {str(e)}")
    
    def _check_data_freshness(self, conn: sqlite3.Connection, table_name: str) -> None:
        """Check data freshness based on timestamp columns"""
        timestamp_columns = ['dashboard_updated_at', 'summary_created_at', 'insights_generated_at', 
                           'analysis_created_at', 'segment_created_at', 'intelligence_updated_at']
        
        try:
            # Find timestamp columns in the table schema
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            available_columns = [row[1] for row in cursor.fetchall()]
            
            for ts_column in timestamp_columns:
                if ts_column in available_columns:
                    cursor = conn.execute(f"""
                        SELECT 
                            MAX({ts_column}) as latest_timestamp,
                            COUNT(*) as total_rows
                        FROM {table_name}
                    """
                    )
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        latest_timestamp = datetime.fromisoformat(result[0].replace('Z', '+00:00'))
                        hours_since_update = (datetime.now() - latest_timestamp).total_seconds() / 3600
                        
                        status = "PASS" if hours_since_update <= self.config["thresholds"]["data_freshness_hours"] else "FAIL"
                        severity = "CRITICAL" if hours_since_update > 48 else "HIGH" if hours_since_update > 24 else "LOW"
                        
                        self._add_quality_result(
                            check_name="data_freshness",
                            table_name=table_name,
                            column_name=ts_column,
                            check_type="FRESHNESS",
                            status=status,
                            value=hours_since_update,
                            threshold=self.config["thresholds"]["data_freshness_hours"],
                            message=f"Data is {hours_since_update:.1f} hours old",
                            severity=severity
                        )
                    break
        
        except Exception as e:
            logger.error(f"Error checking freshness for {table_name}: {str(e)}")
    
    def _check_data_uniqueness(self, conn: sqlite3.Connection, table_name: str) -> None:
        """Check uniqueness of key columns"""
        try:
            # Find primary key columns in the table schema
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            
            key_columns = [col for col in columns if col.endswith('_key')]
            
            for key_column in key_columns:
                cursor = conn.execute(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT {key_column}) as unique_rows,
                        CAST(COUNT(DISTINCT {key_column}) AS FLOAT) / COUNT(*) as uniqueness_rate
                    FROM {table_name}
                """
                )
                
                result = cursor.fetchone()
                if result:
                    total_rows, unique_rows, uniqueness_rate = result
                    
                    status = "PASS" if uniqueness_rate == 1.0 else "FAIL"
                    severity = "HIGH" if uniqueness_rate < 0.9 else "MEDIUM"
                    
                    self._add_quality_result(
                        check_name="data_uniqueness",
                        table_name=table_name,
                        column_name=key_column,
                        check_type="UNIQUENESS",
                        status=status,
                        value=uniqueness_rate,
                        threshold=1.0,
                        message=f"Uniqueness rate: {uniqueness_rate:.2%} ({unique_rows}/{total_rows})",
                        severity=severity
                    )
        
        except Exception as e:
            logger.error(f"Error checking uniqueness for {table_name}: {str(e)}")
    
    def _check_data_validity(self, conn: sqlite3.Connection, table_name: str) -> None:
        """Check data validity based on business rules"""
        try:
            # Perform validity checks specific to the table
            if table_name == "mart_partner_performance_dashboard":
                # Check for negative revenue values
                cursor = conn.execute(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(CASE WHEN revenue_30d < 0 THEN 1 END) as negative_revenue_rows,
                        COUNT(CASE WHEN overall_health_score < 0 OR overall_health_score > 100 THEN 1 END) as invalid_health_score_rows
                    FROM {table_name}
                """
                )
                
                result = cursor.fetchone()
                if result:
                    total_rows, negative_revenue, invalid_health_score = result
                    
                    # Check negative revenue
                    validity_rate = (total_rows - negative_revenue) / total_rows if total_rows > 0 else 1
                    status = "PASS" if validity_rate == 1.0 else "FAIL"
                    severity = "HIGH" if validity_rate < 0.95 else "MEDIUM"
                    
                    self._add_quality_result(
                        check_name="revenue_validity",
                        table_name=table_name,
                        column_name="revenue_30d",
                        check_type="VALIDITY",
                        status=status,
                        value=validity_rate,
                        threshold=1.0,
                        message=f"Revenue validity: {validity_rate:.2%} ({negative_revenue} negative values)",
                        severity=severity
                    )
                    
                    # Check for invalid health scores
                    validity_rate = (total_rows - invalid_health_score) / total_rows if total_rows > 0 else 1
                    status = "PASS" if validity_rate == 1.0 else "FAIL"
                    severity = "HIGH" if validity_rate < 0.95 else "MEDIUM"
                    
                    self._add_quality_result(
                        check_name="health_score_validity",
                        table_name=table_name,
                        column_name="overall_health_score",
                        check_type="VALIDITY",
                        status=status,
                        value=validity_rate,
                        threshold=1.0,
                        message=f"Health score validity: {validity_rate:.2%} ({invalid_health_score} invalid values)",
                        severity=severity
                    )
        
        except Exception as e:
            logger.error(f"Error checking validity for {table_name}: {str(e)}")
    
    def _check_business_rules(self, conn: sqlite3.Connection, table_name: str) -> None:
        """Check business-specific rules"""
        try:
            if table_name == "mart_partner_performance_dashboard":
                # Business rule: High-revenue partners should have good health scores
                cursor = conn.execute(f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(CASE WHEN revenue_30d > 50000 AND overall_health_score < 30 THEN 1 END) as anomalous_rows
                    FROM {table_name}
                """
                )
                
                result = cursor.fetchone()
                if result:
                    total_rows, anomalous_rows = result
                    
                    if total_rows > 0:
                        rule_compliance = (total_rows - anomalous_rows) / total_rows
                        status = "PASS" if rule_compliance >= 0.95 else "WARN"
                        severity = "MEDIUM" if rule_compliance < 0.9 else "LOW"
                        
                        self._add_quality_result(
                            check_name="revenue_health_consistency",
                            table_name=table_name,
                            check_type="BUSINESS_RULE",
                            status=status,
                            value=rule_compliance,
                            threshold=0.95,
                            message=f"Revenue-health consistency: {rule_compliance:.2%} ({anomalous_rows} anomalous)",
                            severity=severity
                        )
        
        except Exception as e:
            logger.error(f"Error checking business rules for {table_name}: {str(e)}")
    
    def _add_quality_result(self, check_name: str, table_name: str, check_type: str, 
                           status: str, value: float, threshold: float, message: str, 
                           severity: str, column_name: Optional[str] = None) -> None:
        """Add a quality check result"""
        result = DataQualityResult(
            check_name=check_name,
            table_name=table_name,
            column_name=column_name,
            check_type=check_type,
            status=status,
            value=value,
            threshold=threshold,
            message=message,
            timestamp=datetime.now(),
            severity=severity
        )
        self.quality_results.append(result)
    
    def _store_quality_results(self) -> None:
        """Store quality results in database"""
        with sqlite3.connect(self.db_path) as conn:
            for result in self.quality_results:
                conn.execute("""
                    INSERT INTO quality_results 
                    (check_name, table_name, column_name, check_type, status, value, threshold, message, timestamp, severity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result.check_name, result.table_name, result.column_name, result.check_type,
                    result.status, result.value, result.threshold, result.message,
                    result.timestamp.isoformat(), result.severity
                ))
    
    def detect_anomalies(self) -> List[AnomalyResult]:
        """Detect anomalies in key business metrics"""
        logger.info("Starting anomaly detection...")
        self.anomaly_results = []
        
        with sqlite3.connect(self.db_path) as conn:
            # Detect anomalies in revenue data
            self._detect_revenue_anomalies(conn)
            
            # Detect anomalies in order volume
            self._detect_volume_anomalies(conn)
            
            # Detect anomalies in profit margins
            self._detect_margin_anomalies(conn)
        
        logger.info(f"Completed anomaly detection. Found {len([r for r in self.anomaly_results if r.is_anomaly])} anomalies")
        return self.anomaly_results
    
    def _detect_revenue_anomalies(self, conn: sqlite3.Connection) -> None:
        """Detect revenue anomalies using statistical methods"""
        try:
            # Retrieve revenue trends from the financial summary
            cursor = conn.execute("""
                SELECT 
                    order_date,
                    partner_channel,
                    customer_segment,
                    daily_revenue,
                    revenue_growth_30d
                FROM mart_financial_performance_summary
                WHERE order_date >= date('now', '-30 days')
                ORDER BY order_date
            """
            )
            
            data = cursor.fetchall()
            if not data:
                return
            
            df = pd.DataFrame(data, columns=['order_date', 'partner_channel', 'customer_segment', 'daily_revenue', 'revenue_growth_30d'])
            
            # Analyze data by partner channel and customer segment
            for (channel, segment), group in df.groupby(['partner_channel', 'customer_segment']):
                if len(group) < 7:  # A minimum of 7 days of data is required
                    continue
                
                revenue_values = group['daily_revenue'].values
                mean_revenue = np.mean(revenue_values)
                std_revenue = np.std(revenue_values)
                
                if std_revenue > 0:
                    # Calculate z-scores for the last few data points
                    recent_values = revenue_values[-3:]  # Analyze the last 3 days of data
                    z_scores = [(val - mean_revenue) / std_revenue for val in recent_values]
                    
                    for i, z_score in enumerate(z_scores):
                        if abs(z_score) > 2:  # Set the threshold for detecting anomalies
                            anomaly_result = AnomalyResult(
                                table_name="mart_financial_performance_summary",
                                metric_name="daily_revenue",
                                current_value=recent_values[i],
                                expected_value=mean_revenue,
                                deviation=abs(recent_values[i] - mean_revenue),
                                anomaly_score=abs(z_score),
                                is_anomaly=True,
                                timestamp=datetime.now(),
                                context={
                                    'partner_channel': channel,
                                    'customer_segment': segment,
                                    'z_score': z_score,
                                    'threshold': 2.0
                                }
                            )
                            self.anomaly_results.append(anomaly_result)
        
        except Exception as e:
            logger.error(f"Error detecting revenue anomalies: {str(e)}")
    
    def _detect_volume_anomalies(self, conn: sqlite3.Connection) -> None:
        """Detect order volume anomalies"""
        try:
            cursor = conn.execute("""
                SELECT 
                    partner_channel,
                    customer_segment,
                    orders_30d,
                    order_growth_30d
                FROM mart_partner_performance_dashboard
            """
            )
            
            data = cursor.fetchall()
            if not data:
                return
            
            df = pd.DataFrame(data, columns=['partner_channel', 'customer_segment', 'orders_30d', 'order_growth_30d'])
            
            # Identify anomalies based on the growth rate
            for _, row in df.iterrows():
                if row['order_growth_30d'] is not None and abs(row['order_growth_30d']) > self.config["thresholds"]["order_volume_anomaly_threshold"]:
                    anomaly_result = AnomalyResult(
                        table_name="mart_partner_performance_dashboard",
                        metric_name="order_volume_growth",
                        current_value=row['order_growth_30d'],
                        expected_value=0.0,
                        deviation=abs(row['order_growth_30d']),
                        anomaly_score=abs(row['order_growth_30d']) / self.config["thresholds"]["order_volume_anomaly_threshold"],
                        is_anomaly=True,
                        timestamp=datetime.now(),
                        context={
                            'partner_channel': row['partner_channel'],
                            'customer_segment': row['customer_segment'],
                            'orders_30d': row['orders_30d']
                        }
                    )
                    self.anomaly_results.append(anomaly_result)
        
        except Exception as e:
            logger.error(f"Error detecting volume anomalies: {str(e)}")
    
    def _detect_margin_anomalies(self, conn: sqlite3.Connection) -> None:
        """Detect margin anomalies"""
        try:
            cursor = conn.execute("""
                SELECT 
                    partner_channel,
                    customer_segment,
                    avg_gross_margin_30d,
                    avg_net_margin_30d
                FROM mart_partner_performance_dashboard
            """
            )
            
            data = cursor.fetchall()
            if not data:
                return
            
            df = pd.DataFrame(data, columns=['partner_channel', 'customer_segment', 'avg_gross_margin_30d', 'avg_net_margin_30d'])
            
            # Placeholder for historical margin analysis
            # Currently, only checks for negative margins
            for _, row in df.iterrows():
                if row['avg_gross_margin_30d'] is not None and row['avg_gross_margin_30d'] < 0:
                    anomaly_result = AnomalyResult(
                        table_name="mart_partner_performance_dashboard",
                        metric_name="gross_margin",
                        current_value=row['avg_gross_margin_30d'],
                        expected_value=0.2,  # Assume an expected margin of 20%
                        deviation=abs(row['avg_gross_margin_30d'] - 0.2),
                        anomaly_score=abs(row['avg_gross_margin_30d'] - 0.2) / 0.2,
                        is_anomaly=True,
                        timestamp=datetime.now(),
                        context={
                            'partner_channel': row['partner_channel'],
                            'customer_segment': row['customer_segment'],
                            'type': 'negative_margin'
                        }
                    )
                    self.anomaly_results.append(anomaly_result)
        
        except Exception as e:
            logger.error(f"Error detecting margin anomalies: {str(e)}")
    
    def send_alerts(self) -> None:
        """Send alerts based on quality results and anomalies"""
        logger.info("Sending alerts...")
        
        # Create a summary of alerts to be sent
        critical_issues = [r for r in self.quality_results if r.severity == "CRITICAL"]
        high_issues = [r for r in self.quality_results if r.severity == "HIGH"]
        anomalies = [r for r in self.anomaly_results if r.is_anomaly]
        
        if critical_issues or high_issues or anomalies:
            alert_message = self._generate_alert_message(critical_issues, high_issues, anomalies)
            
            # Send an alert to the Slack channel
            if self.config["alerting"]["slack_webhook"]:
                self._send_slack_alert(alert_message)
            
            # Send an alert via email
            if self.config["alerting"]["email_config"]["sender_email"]:
                self._send_email_alert(alert_message)
    
    def _generate_alert_message(self, critical_issues: List[DataQualityResult], 
                               high_issues: List[DataQualityResult], 
                               anomalies: List[AnomalyResult]) -> str:
        """Generate alert message"""
        message = "🚨 **ZMS Analytics Data Quality Alert** 🚨\n\n"
        
        if critical_issues:
            message += f"**CRITICAL ISSUES ({len(critical_issues)}):**\n"
            for issue in critical_issues[:5]:  # Display the first 5 issues
                message += f"• {issue.table_name}: {issue.message}\n"
            message += "\n"
        
        if high_issues:
            message += f"**HIGH PRIORITY ISSUES ({len(high_issues)}):**\n"
            for issue in high_issues[:5]:  # Display the first 5 issues
                message += f"• {issue.table_name}: {issue.message}\n"
            message += "\n"
        
        if anomalies:
            message += f"**ANOMALIES DETECTED ({len(anomalies)}):**\n"
            for anomaly in anomalies[:5]:  # Display the first 5 anomalies
                message += f"• {anomaly.table_name}: {anomaly.metric_name} anomaly (score: {anomaly.anomaly_score:.2f})\n"
            message += "\n"
        
        message += f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += "Please investigate these issues immediately."
        
        return message
    
    def _send_slack_alert(self, message: str) -> None:
        """Send Slack alert"""
        try:
            webhook_url = self.config["alerting"]["slack_webhook"]
            if webhook_url:
                payload = {
                    "text": message,
                    "channel": "#data-quality",
                    "username": "ZMS Data Quality Bot",
                    "icon_emoji": ":warning:"
                }
                
                response = requests.post(webhook_url, json=payload)
                if response.status_code == 200:
                    logger.info("Slack alert sent successfully")
                else:
                    logger.error(f"Failed to send Slack alert: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error sending Slack alert: {str(e)}")
    
    def _send_email_alert(self, message: str) -> None:
        """Send email alert"""
        try:
            email_config = self.config["alerting"]["email_config"]
            if not email_config["sender_email"] or not email_config["recipient_emails"]:
                return
            
            msg = MIMEMultipart()
            msg['From'] = email_config["sender_email"]
            msg['To'] = ", ".join(email_config["recipient_emails"])
            msg['Subject'] = "ZMS Analytics Data Quality Alert"
            
            # Convert markdown to plain text for email compatibility
            plain_message = message.replace("**", "").replace("*", "").replace("\\n", "\n")
            msg.attach(MIMEText(plain_message, 'plain'))
            
            server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
            server.starttls()
            server.login(email_config["sender_email"], email_config["sender_password"])
            
            text = msg.as_string()
            server.sendmail(email_config["sender_email"], email_config["recipient_emails"], text)
            server.quit()
            
            logger.info("Email alert sent successfully")
        
        except Exception as e:
            logger.error(f"Error sending email alert: {str(e)}")
    
    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive quality report"""
        logger.info("Generating quality report...")
        
        # Compute summary statistics for the report
        total_checks = len(self.quality_results)
        passed_checks = len([r for r in self.quality_results if r.status == "PASS"])
        failed_checks = len([r for r in self.quality_results if r.status == "FAIL"])
        warning_checks = len([r for r in self.quality_results if r.status == "WARN"])
        
        # Group the results by severity level
        severity_counts = {}
        for result in self.quality_results:
            severity_counts[result.severity] = severity_counts.get(result.severity, 0) + 1
        
        # Group the results by table name
        table_summary = {}
        for result in self.quality_results:
            if result.table_name not in table_summary:
                table_summary[result.table_name] = {"PASS": 0, "FAIL": 0, "WARN": 0}
            table_summary[result.table_name][result.status] += 1
        
        # Create a summary of the detected anomalies
        anomaly_summary = {
            "total_anomalies": len(self.anomaly_results),
            "high_score_anomalies": len([a for a in self.anomaly_results if a.anomaly_score > 3]),
            "by_metric": {}
        }
        
        for anomaly in self.anomaly_results:
            metric = anomaly.metric_name
            if metric not in anomaly_summary["by_metric"]:
                anomaly_summary["by_metric"][metric] = 0
            anomaly_summary["by_metric"][metric] += 1
        
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_checks": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": failed_checks,
                "warning_checks": warning_checks,
                "success_rate": passed_checks / total_checks if total_checks > 0 else 0
            },
            "severity_breakdown": severity_counts,
            "table_summary": table_summary,
            "anomaly_summary": anomaly_summary,
            "quality_results": [r.to_dict() for r in self.quality_results],
            "anomaly_results": [asdict(a) for a in self.anomaly_results]
        }
        
        return report
    
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """Run complete monitoring cycle"""
        logger.info("Starting monitoring cycle...")
        
        try:
            # Execute the data quality checks
            quality_results = self.run_data_quality_checks()
            
            # Run the anomaly detection process
            anomaly_results = self.detect_anomalies()
            
            # Send alerts if any issues are found
            self.send_alerts()
            
            # Generate the final quality report
            report = self.generate_quality_report()
            
            logger.info("Monitoring cycle completed successfully")
            return report
        
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {str(e)}")
            raise


def main():
    """Main function to run the monitoring system"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ZMS Data Quality Monitoring System")
    parser.add_argument("--db-path", required=True, help="Path to the database file")
    parser.add_argument("--config", default="quality_config.json", help="Path to configuration file")
    parser.add_argument("--output", default="quality_report.json", help="Output file for report")
    
    args = parser.parse_args()
    
    # Initialize the data quality monitor
    monitor = DataQualityMonitor(args.db_path, args.config)
    
    # Execute the complete monitoring cycle
    report = monitor.run_monitoring_cycle()
    
    # Save the generated report to a file
    with open(args.output, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Monitoring completed. Report saved to {args.output}")
    print(f"Quality Score: {report['summary']['success_rate']:.2%}")
    print(f"Failed Checks: {report['summary']['failed_checks']}")
    print(f"Anomalies Found: {report['anomaly_summary']['total_anomalies']}")


if __name__ == "__main__":
    main()