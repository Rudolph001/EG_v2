
import os
import io
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.lib.colors import HexColor
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_agg import FigureCanvasAgg
import seaborn as sns
import openpyxl
from openpyxl.styles import Font, PatternFill, Border, Side
from openpyxl.chart import PieChart, BarChart, LineChart, Reference
from openpyxl.workbook import Workbook
from database import get_db_connection
from ml_models import get_analytics_report, analytics_engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure reports directory exists
os.makedirs('reports', exist_ok=True)

class ReportGenerator:
    """Comprehensive report generator for Email Guardian"""
    
    def __init__(self):
        self.report_styles = getSampleStyleSheet()
        self.colors = {
            'primary': HexColor('#0d6efd'),
            'success': HexColor('#198754'),
            'danger': HexColor('#dc3545'),
            'warning': HexColor('#ffc107'),
            'info': HexColor('#0dcaf0'),
            'secondary': HexColor('#6c757d')
        }
        
        # Configure matplotlib style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def get_report_data(self, date_from: str = None, date_to: str = None) -> Dict[str, Any]:
        """Gather comprehensive data for reporting"""
        try:
            conn = get_db_connection()
            
            # Set default date range if not provided
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            if not date_from:
                date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            data = {
                'date_from': date_from,
                'date_to': date_to,
                'generated_at': datetime.now()
            }
            
            # Summary statistics
            summary_query = f"""
                SELECT 
                    COUNT(*) as total_emails,
                    COUNT(DISTINCT sender) as unique_senders,
                    COUNT(DISTINCT department) as departments,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as escalated_emails,
                    COUNT(CASE WHEN final_outcome IN ('cleared', 'approved', 'resolved') THEN 1 END) as cleared_emails,
                    COUNT(CASE WHEN final_outcome IN ('excluded', 'whitelisted') THEN 1 END) as filtered_emails,
                    COUNT(CASE WHEN attachments IS NOT NULL AND attachments != '-' THEN 1 END) as emails_with_attachments,
                    AVG(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1.0 ELSE 0.0 END) * 100 as escalation_rate
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
            """
            
            summary_result = conn.execute(summary_query).fetchone()
            data['summary'] = {
                'total_emails': summary_result[0] or 0,
                'unique_senders': summary_result[1] or 0,
                'departments': summary_result[2] or 0,
                'escalated_emails': summary_result[3] or 0,
                'cleared_emails': summary_result[4] or 0,
                'filtered_emails': summary_result[5] or 0,
                'emails_with_attachments': summary_result[6] or 0,
                'escalation_rate': round(summary_result[7] or 0, 2)
            }
            
            # Risk category distribution
            risk_query = f"""
                SELECT 
                    CASE 
                        WHEN final_outcome IN ('escalated', 'high_risk', 'critical') THEN 'High Risk'
                        WHEN final_outcome IN ('medium_risk', 'warning', 'pending_review') THEN 'Medium Risk'
                        WHEN final_outcome IN ('cleared', 'approved', 'resolved') THEN 'Low Risk'
                        WHEN final_outcome IN ('excluded', 'whitelisted') THEN 'Filtered'
                        ELSE 'Unknown'
                    END as risk_category,
                    COUNT(*) as count,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY risk_category
                ORDER BY count DESC
            """
            
            risk_data = conn.execute(risk_query).fetchall()
            data['risk_distribution'] = [
                {'category': row[0], 'count': row[1], 'percentage': round(row[2], 1)}
                for row in risk_data
            ]
            
            # Department analysis
            dept_query = f"""
                SELECT 
                    department,
                    COUNT(*) as total_emails,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as high_risk_count,
                    ROUND(COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) * 100.0 / COUNT(*), 2) as risk_percentage
                FROM emails
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                AND department IS NOT NULL
                GROUP BY department
                HAVING COUNT(*) >= 5
                ORDER BY total_emails DESC
                LIMIT 15
            """
            
            dept_data = conn.execute(dept_query).fetchall()
            data['department_analysis'] = [
                {
                    'department': row[0],
                    'total_emails': row[1],
                    'high_risk_count': row[2],
                    'risk_percentage': row[3]
                }
                for row in dept_data
            ]
            
            # Monthly trend analysis
            monthly_query = f"""
                SELECT 
                    strftime('%Y-%m', _time) as month,
                    COUNT(*) as total_emails,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as escalated_emails,
                    COUNT(CASE WHEN final_outcome IN ('cleared', 'approved') THEN 1 END) as cleared_emails
                FROM emails
                WHERE _time >= (DATE '{date_from}' - INTERVAL 12 MONTH)
                GROUP BY strftime('%Y-%m', _time)
                ORDER BY month
            """
            
            monthly_data = conn.execute(monthly_query).fetchall()
            data['monthly_trends'] = [
                {
                    'month': row[0],
                    'total_emails': row[1],
                    'escalated_emails': row[2],
                    'cleared_emails': row[3]
                }
                for row in monthly_data
            ]
            
            # Top senders analysis
            senders_query = f"""
                SELECT 
                    sender,
                    COUNT(*) as email_count,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as high_risk_count,
                    MAX(_time) as last_email_date
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY sender
                ORDER BY email_count DESC
                LIMIT 20
            """
            
            senders_data = conn.execute(senders_query).fetchall()
            data['top_senders'] = [
                {
                    'sender': row[0],
                    'email_count': row[1],
                    'high_risk_count': row[2],
                    'last_email_date': row[3]
                }
                for row in senders_data
            ]
            
            # Cases analysis
            cases_query = f"""
                SELECT 
                    c.status,
                    COUNT(*) as count,
                    AVG(JULIANDAY(COALESCE(c.updated_at, datetime('now'))) - JULIANDAY(c.created_at)) as avg_resolution_days
                FROM cases c
                JOIN emails e ON c.email_id = e.id
                WHERE DATE(c.created_at) BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY c.status
                ORDER BY count DESC
            """
            
            cases_data = conn.execute(cases_query).fetchall()
            data['cases_analysis'] = [
                {
                    'status': row[0],
                    'count': row[1],
                    'avg_resolution_days': round(row[2] or 0, 1)
                }
                for row in cases_data
            ]
            
            # Policy violations
            policy_query = f"""
                SELECT 
                    policy_name,
                    COUNT(*) as violation_count,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as escalated_count
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                AND policy_name IS NOT NULL
                AND policy_name != ''
                GROUP BY policy_name
                ORDER BY violation_count DESC
                LIMIT 10
            """
            
            policy_data = conn.execute(policy_query).fetchall()
            data['policy_violations'] = [
                {
                    'policy_name': row[0],
                    'violation_count': row[1],
                    'escalated_count': row[2]
                }
                for row in policy_data
            ]
            
            # Flagged senders
            flagged_query = """
                SELECT 
                    fs.sender,
                    fs.reason,
                    fs.flagged_at,
                    COUNT(e.id) as email_count_in_period
                FROM flagged_senders fs
                LEFT JOIN emails e ON fs.sender = e.sender 
                    AND DATE(e._time) BETWEEN ? AND ?
                GROUP BY fs.sender, fs.reason, fs.flagged_at
                ORDER BY email_count_in_period DESC, fs.flagged_at DESC
                LIMIT 15
            """
            
            flagged_data = conn.execute(flagged_query, [date_from, date_to]).fetchall()
            data['flagged_senders'] = [
                {
                    'sender': row[0],
                    'reason': row[1],
                    'flagged_at': row[2],
                    'email_count_in_period': row[3]
                }
                for row in flagged_data
            ]
            
            conn.close()
            
            # Get ML analytics if available
            try:
                ml_analytics = get_analytics_report()
                data['ml_insights'] = ml_analytics
            except Exception as e:
                logger.warning(f"Could not load ML insights: {e}")
                data['ml_insights'] = {'error': 'ML insights unavailable'}
            
            return data
            
        except Exception as e:
            logger.error(f"Error gathering report data: {e}")
            raise
    
    def generate_charts(self, data: Dict[str, Any], chart_dir: str = 'reports/charts') -> Dict[str, str]:
        """Generate chart images for reports"""
        os.makedirs(chart_dir, exist_ok=True)
        chart_files = {}
        
        try:
            # Risk Distribution Pie Chart
            if data['risk_distribution']:
                plt.figure(figsize=(8, 6))
                categories = [item['category'] for item in data['risk_distribution']]
                counts = [item['count'] for item in data['risk_distribution']]
                colors = ['#dc3545', '#ffc107', '#198754', '#6c757d', '#0dcaf0'][:len(categories)]
                
                plt.pie(counts, labels=categories, autopct='%1.1f%%', colors=colors, startangle=90)
                plt.title('Risk Category Distribution', fontsize=14, fontweight='bold')
                plt.tight_layout()
                
                risk_chart_path = os.path.join(chart_dir, f'risk_distribution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
                plt.savefig(risk_chart_path, dpi=300, bbox_inches='tight')
                plt.close()
                chart_files['risk_distribution'] = risk_chart_path
            
            # Department Analysis Bar Chart
            if data['department_analysis']:
                plt.figure(figsize=(12, 6))
                departments = [item['department'][:15] for item in data['department_analysis'][:10]]
                total_emails = [item['total_emails'] for item in data['department_analysis'][:10]]
                high_risk_emails = [item['high_risk_count'] for item in data['department_analysis'][:10]]
                
                x = np.arange(len(departments))
                width = 0.35
                
                plt.bar(x - width/2, total_emails, width, label='Total Emails', color='#0d6efd', alpha=0.8)
                plt.bar(x + width/2, high_risk_emails, width, label='High Risk Emails', color='#dc3545', alpha=0.8)
                
                plt.xlabel('Department')
                plt.ylabel('Email Count')
                plt.title('Email Volume and Risk by Department')
                plt.xticks(x, departments, rotation=45, ha='right')
                plt.legend()
                plt.tight_layout()
                
                dept_chart_path = os.path.join(chart_dir, f'department_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
                plt.savefig(dept_chart_path, dpi=300, bbox_inches='tight')
                plt.close()
                chart_files['department_analysis'] = dept_chart_path
            
            # Monthly Trends Line Chart
            if data['monthly_trends']:
                plt.figure(figsize=(12, 6))
                months = [item['month'] for item in data['monthly_trends']]
                total_emails = [item['total_emails'] for item in data['monthly_trends']]
                escalated_emails = [item['escalated_emails'] for item in data['monthly_trends']]
                
                plt.plot(months, total_emails, marker='o', linewidth=2, label='Total Emails', color='#0d6efd')
                plt.plot(months, escalated_emails, marker='s', linewidth=2, label='Escalated Emails', color='#dc3545')
                
                plt.xlabel('Month')
                plt.ylabel('Email Count')
                plt.title('Email Volume Trends Over Time')
                plt.xticks(rotation=45)
                plt.legend()
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                trend_chart_path = os.path.join(chart_dir, f'monthly_trends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
                plt.savefig(trend_chart_path, dpi=300, bbox_inches='tight')
                plt.close()
                chart_files['monthly_trends'] = trend_chart_path
            
            # Policy Violations Chart
            if data['policy_violations']:
                plt.figure(figsize=(10, 6))
                policies = [item['policy_name'][:20] + '...' if len(item['policy_name']) > 20 else item['policy_name'] 
                           for item in data['policy_violations'][:10]]
                violations = [item['violation_count'] for item in data['policy_violations'][:10]]
                
                plt.barh(policies, violations, color='#ffc107', alpha=0.8)
                plt.xlabel('Violation Count')
                plt.title('Top Policy Violations')
                plt.tight_layout()
                
                policy_chart_path = os.path.join(chart_dir, f'policy_violations_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
                plt.savefig(policy_chart_path, dpi=300, bbox_inches='tight')
                plt.close()
                chart_files['policy_violations'] = policy_chart_path
            
            logger.info(f"Generated {len(chart_files)} charts in {chart_dir}")
            return chart_files
            
        except Exception as e:
            logger.error(f"Error generating charts: {e}")
            return {}
    
    def generate_pdf_report(self, date_from: str = None, date_to: str = None) -> str:
        """Generate comprehensive PDF report"""
        try:
            # Get report data
            data = self.get_report_data(date_from, date_to)
            
            # Generate charts
            chart_files = self.generate_charts(data)
            
            # Create PDF filename
            filename = f"reports/email_guardian_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            
            # Create PDF document
            doc = SimpleDocTemplate(filename, pagesize=letter)
            story = []
            
            # Title and header
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=self.report_styles['Heading1'],
                fontSize=24,
                spaceAfter=30,
                alignment=1,
                textColor=self.colors['primary']
            )
            
            story.append(Paragraph("Email Guardian Comprehensive Report", title_style))
            story.append(Paragraph(f"Report Period: {data['date_from']} to {data['date_to']}", self.report_styles['Normal']))
            story.append(Paragraph(f"Generated: {data['generated_at'].strftime('%Y-%m-%d %H:%M:%S')}", self.report_styles['Normal']))
            story.append(Spacer(1, 20))
            
            # Executive Summary
            story.append(Paragraph("Executive Summary", self.report_styles['Heading2']))
            summary_data = [
                ['Metric', 'Value', 'Description'],
                ['Total Emails Processed', f"{data['summary']['total_emails']:,}", 'All emails in the reporting period'],
                ['Unique Senders', f"{data['summary']['unique_senders']:,}", 'Distinct email senders'],
                ['Departments Involved', f"{data['summary']['departments']:,}", 'Number of departments with email activity'],
                ['Escalated Emails', f"{data['summary']['escalated_emails']:,}", 'High-risk emails requiring attention'],
                ['Cleared Emails', f"{data['summary']['cleared_emails']:,}", 'Emails approved as safe'],
                ['Filtered Emails', f"{data['summary']['filtered_emails']:,}", 'Excluded/whitelisted emails'],
                ['Escalation Rate', f"{data['summary']['escalation_rate']}%", 'Percentage of emails escalated'],
            ]
            
            summary_table = Table(summary_data, colWidths=[2.5*inch, 1.5*inch, 3*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
            ]))
            
            story.append(summary_table)
            story.append(Spacer(1, 20))
            
            # Risk Analysis Section
            story.append(Paragraph("Risk Analysis", self.report_styles['Heading2']))
            
            if 'risk_distribution' in chart_files:
                story.append(Paragraph("Risk Category Distribution", self.report_styles['Heading3']))
                img = Image(chart_files['risk_distribution'], width=6*inch, height=4*inch)
                story.append(img)
                story.append(Spacer(1, 10))
            
            risk_table_data = [['Risk Category', 'Count', 'Percentage']]
            for item in data['risk_distribution']:
                risk_table_data.append([item['category'], str(item['count']), f"{item['percentage']}%"])
            
            risk_table = Table(risk_table_data)
            risk_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['secondary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(risk_table)
            story.append(PageBreak())
            
            # Department Analysis
            story.append(Paragraph("Department Analysis", self.report_styles['Heading2']))
            
            if 'department_analysis' in chart_files:
                img = Image(chart_files['department_analysis'], width=7*inch, height=4*inch)
                story.append(img)
                story.append(Spacer(1, 10))
            
            dept_table_data = [['Department', 'Total Emails', 'High Risk', 'Risk %']]
            for item in data['department_analysis'][:10]:
                dept_table_data.append([
                    item['department'][:30],
                    str(item['total_emails']),
                    str(item['high_risk_count']),
                    f"{item['risk_percentage']}%"
                ])
            
            dept_table = Table(dept_table_data, colWidths=[3*inch, 1.2*inch, 1.2*inch, 1*inch])
            dept_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['info']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(dept_table)
            story.append(PageBreak())
            
            # Trend Analysis
            if data['monthly_trends']:
                story.append(Paragraph("Email Volume Trends", self.report_styles['Heading2']))
                
                if 'monthly_trends' in chart_files:
                    img = Image(chart_files['monthly_trends'], width=7*inch, height=4*inch)
                    story.append(img)
                    story.append(Spacer(1, 15))
            
            # Top Senders
            if data['top_senders']:
                story.append(Paragraph("Top Email Senders", self.report_styles['Heading2']))
                
                senders_table_data = [['Sender', 'Email Count', 'High Risk Count', 'Last Email']]
                for item in data['top_senders'][:15]:
                    last_date = item['last_email_date'][:10] if item['last_email_date'] else 'N/A'
                    senders_table_data.append([
                        item['sender'][:40],
                        str(item['email_count']),
                        str(item['high_risk_count']),
                        last_date
                    ])
                
                senders_table = Table(senders_table_data, colWidths=[3*inch, 1*inch, 1.2*inch, 1*inch])
                senders_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), self.colors['success']),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9)
                ]))
                
                story.append(senders_table)
                story.append(PageBreak())
            
            # Policy Violations
            if data['policy_violations']:
                story.append(Paragraph("Policy Violations Analysis", self.report_styles['Heading2']))
                
                if 'policy_violations' in chart_files:
                    img = Image(chart_files['policy_violations'], width=6*inch, height=4*inch)
                    story.append(img)
                    story.append(Spacer(1, 15))
            
            # Cases Analysis
            if data['cases_analysis']:
                story.append(Paragraph("Cases Analysis", self.report_styles['Heading2']))
                
                cases_table_data = [['Status', 'Count', 'Avg Resolution Days']]
                for item in data['cases_analysis']:
                    cases_table_data.append([
                        item['status'].title(),
                        str(item['count']),
                        str(item['avg_resolution_days'])
                    ])
                
                cases_table = Table(cases_table_data)
                cases_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), self.colors['warning']),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(cases_table)
                story.append(Spacer(1, 15))
            
            # ML Insights Section
            if data['ml_insights'] and 'error' not in data['ml_insights']:
                story.append(Paragraph("Machine Learning Insights", self.report_styles['Heading2']))
                
                if 'correlations' in data['ml_insights']:
                    story.append(Paragraph("Key Findings:", self.report_styles['Heading3']))
                    
                    insights_text = []
                    correlations = data['ml_insights']['correlations']
                    
                    if 'high_risk_senders' in correlations and correlations['high_risk_senders']:
                        insights_text.append("• High-risk senders identified with pattern analysis")
                    
                    if 'department_outcome' in correlations:
                        insights_text.append("• Department-specific risk patterns detected")
                    
                    if data['ml_insights'].get('anomalies'):
                        insights_text.append(f"• {len(data['ml_insights']['anomalies'])} anomalies detected in email patterns")
                    
                    for insight in insights_text:
                        story.append(Paragraph(insight, self.report_styles['Normal']))
                        story.append(Spacer(1, 5))
            
            # Flagged Senders
            if data['flagged_senders']:
                story.append(PageBreak())
                story.append(Paragraph("Flagged Senders Report", self.report_styles['Heading2']))
                
                flagged_table_data = [['Sender', 'Reason', 'Flagged Date', 'Recent Activity']]
                for item in data['flagged_senders'][:15]:
                    flagged_date = item['flagged_at'][:10] if item['flagged_at'] else 'N/A'
                    flagged_table_data.append([
                        item['sender'][:35],
                        item['reason'][:30],
                        flagged_date,
                        f"{item['email_count_in_period']} emails"
                    ])
                
                flagged_table = Table(flagged_table_data, colWidths=[2.5*inch, 2*inch, 1.2*inch, 1*inch])
                flagged_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), self.colors['danger']),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 9)
                ]))
                
                story.append(flagged_table)
            
            # Build PDF
            doc.build(story)
            
            logger.info(f"PDF report generated: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {e}")
            raise
    
    def generate_excel_report(self, date_from: str = None, date_to: str = None) -> str:
        """Generate comprehensive Excel report with multiple sheets and charts"""
        try:
            # Get report data
            data = self.get_report_data(date_from, date_to)
            
            # Create Excel filename
            filename = f"reports/email_guardian_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            # Create workbook
            wb = Workbook()
            
            # Remove default sheet
            wb.remove(wb.active)
            
            # Summary Sheet
            ws_summary = wb.create_sheet("Executive Summary")
            ws_summary.append(["Email Guardian Report"])
            ws_summary.append([f"Period: {data['date_from']} to {data['date_to']}"])
            ws_summary.append([f"Generated: {data['generated_at'].strftime('%Y-%m-%d %H:%M:%S')}"])
            ws_summary.append([])
            
            # Format header
            ws_summary['A1'].font = Font(size=18, bold=True, color='0d6efd')
            ws_summary['A2'].font = Font(size=12, italic=True)
            ws_summary['A3'].font = Font(size=10, italic=True)
            
            # Summary metrics
            ws_summary.append(["Key Metrics", "Value", "Description"])
            summary_headers = ws_summary[5]
            for cell in summary_headers:
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="0d6efd", end_color="0d6efd", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True)
            
            metrics = [
                ("Total Emails", data['summary']['total_emails'], "All emails processed"),
                ("Unique Senders", data['summary']['unique_senders'], "Distinct email senders"),
                ("Departments", data['summary']['departments'], "Departments with activity"),
                ("Escalated Emails", data['summary']['escalated_emails'], "High-risk emails"),
                ("Cleared Emails", data['summary']['cleared_emails'], "Approved emails"),
                ("Escalation Rate", f"{data['summary']['escalation_rate']}%", "Percentage escalated")
            ]
            
            for metric, value, description in metrics:
                ws_summary.append([metric, value, description])
            
            # Auto-adjust column widths
            for column in ws_summary.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws_summary.column_dimensions[column_letter].width = adjusted_width
            
            # Risk Distribution Sheet
            if data['risk_distribution']:
                ws_risk = wb.create_sheet("Risk Distribution")
                ws_risk.append(["Risk Category", "Count", "Percentage"])
                
                # Format header
                for cell in ws_risk[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="dc3545", end_color="dc3545", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['risk_distribution']:
                    ws_risk.append([item['category'], item['count'], f"{item['percentage']}%"])
                
                # Create pie chart
                chart = PieChart()
                chart.title = "Risk Category Distribution"
                
                data_range = Reference(ws_risk, min_col=2, min_row=1, max_row=len(data['risk_distribution'])+1)
                labels_range = Reference(ws_risk, min_col=1, min_row=2, max_row=len(data['risk_distribution'])+1)
                chart.add_data(data_range, titles_from_data=True)
                chart.set_categories(labels_range)
                
                ws_risk.add_chart(chart, "E2")
            
            # Department Analysis Sheet
            if data['department_analysis']:
                ws_dept = wb.create_sheet("Department Analysis")
                ws_dept.append(["Department", "Total Emails", "High Risk Count", "Risk Percentage"])
                
                # Format header
                for cell in ws_dept[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="0dcaf0", end_color="0dcaf0", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['department_analysis']:
                    ws_dept.append([
                        item['department'],
                        item['total_emails'],
                        item['high_risk_count'],
                        f"{item['risk_percentage']}%"
                    ])
                
                # Create bar chart
                chart = BarChart()
                chart.title = "Email Volume by Department"
                chart.x_axis.title = "Department"
                chart.y_axis.title = "Email Count"
                
                data_range = Reference(ws_dept, min_col=2, min_row=1, max_col=3, max_row=len(data['department_analysis'])+1)
                categories = Reference(ws_dept, min_col=1, min_row=2, max_row=len(data['department_analysis'])+1)
                
                chart.add_data(data_range, titles_from_data=True)
                chart.set_categories(categories)
                
                ws_dept.add_chart(chart, "F2")
            
            # Monthly Trends Sheet
            if data['monthly_trends']:
                ws_trends = wb.create_sheet("Monthly Trends")
                ws_trends.append(["Month", "Total Emails", "Escalated Emails", "Cleared Emails"])
                
                # Format header
                for cell in ws_trends[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="198754", end_color="198754", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['monthly_trends']:
                    ws_trends.append([
                        item['month'],
                        item['total_emails'],
                        item['escalated_emails'],
                        item['cleared_emails']
                    ])
                
                # Create line chart
                chart = LineChart()
                chart.title = "Email Volume Trends"
                chart.x_axis.title = "Month"
                chart.y_axis.title = "Email Count"
                
                data_range = Reference(ws_trends, min_col=2, min_row=1, max_col=4, max_row=len(data['monthly_trends'])+1)
                categories = Reference(ws_trends, min_col=1, min_row=2, max_row=len(data['monthly_trends'])+1)
                
                chart.add_data(data_range, titles_from_data=True)
                chart.set_categories(categories)
                
                ws_trends.add_chart(chart, "F2")
            
            # Top Senders Sheet
            if data['top_senders']:
                ws_senders = wb.create_sheet("Top Senders")
                ws_senders.append(["Sender", "Email Count", "High Risk Count", "Last Email Date"])
                
                # Format header
                for cell in ws_senders[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="ffc107", end_color="ffc107", fill_type="solid")
                    cell.font = Font(color="000000", bold=True)
                
                for item in data['top_senders']:
                    ws_senders.append([
                        item['sender'],
                        item['email_count'],
                        item['high_risk_count'],
                        item['last_email_date'][:10] if item['last_email_date'] else 'N/A'
                    ])
            
            # Policy Violations Sheet
            if data['policy_violations']:
                ws_policy = wb.create_sheet("Policy Violations")
                ws_policy.append(["Policy Name", "Violation Count", "Escalated Count"])
                
                # Format header
                for cell in ws_policy[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="6f42c1", end_color="6f42c1", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['policy_violations']:
                    ws_policy.append([
                        item['policy_name'],
                        item['violation_count'],
                        item['escalated_count']
                    ])
            
            # Cases Analysis Sheet
            if data['cases_analysis']:
                ws_cases = wb.create_sheet("Cases Analysis")
                ws_cases.append(["Status", "Count", "Avg Resolution Days"])
                
                # Format header
                for cell in ws_cases[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="fd7e14", end_color="fd7e14", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['cases_analysis']:
                    ws_cases.append([
                        item['status'].title(),
                        item['count'],
                        item['avg_resolution_days']
                    ])
            
            # Flagged Senders Sheet
            if data['flagged_senders']:
                ws_flagged = wb.create_sheet("Flagged Senders")
                ws_flagged.append(["Sender", "Reason", "Flagged Date", "Recent Email Count"])
                
                # Format header
                for cell in ws_flagged[1]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="dc3545", end_color="dc3545", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                for item in data['flagged_senders']:
                    ws_flagged.append([
                        item['sender'],
                        item['reason'],
                        item['flagged_at'][:10] if item['flagged_at'] else 'N/A',
                        item['email_count_in_period']
                    ])
            
            # ML Insights Sheet
            if data['ml_insights'] and 'error' not in data['ml_insights']:
                ws_ml = wb.create_sheet("ML Insights")
                ws_ml.append(["Machine Learning Analysis Results"])
                ws_ml['A1'].font = Font(size=14, bold=True)
                
                ws_ml.append([])
                ws_ml.append(["Analysis Type", "Results"])
                
                # Format header
                for cell in ws_ml[3]:
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="20c997", end_color="20c997", fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True)
                
                if 'correlations' in data['ml_insights']:
                    ws_ml.append(["Correlation Analysis", "Completed - patterns identified"])
                
                if 'anomalies' in data['ml_insights']:
                    anomaly_count = len(data['ml_insights']['anomalies'])
                    ws_ml.append(["Anomaly Detection", f"{anomaly_count} anomalies detected"])
                
                ws_ml.append(["Report Generated", data['ml_insights'].get('generated_at', 'N/A')])
            
            # Auto-adjust all column widths
            for ws in wb.worksheets:
                for column in ws.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if cell.value and len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    ws.column_dimensions[column_letter].width = adjusted_width
            
            # Save workbook
            wb.save(filename)
            
            logger.info(f"Excel report generated: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            raise
    
    def generate_summary_report(self, report_type: str = 'escalated', date_from: str = None, date_to: str = None) -> str:
        """Generate focused summary reports for specific categories"""
        try:
            data = self.get_report_data(date_from, date_to)
            
            filename = f"reports/{report_type}_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            doc = SimpleDocTemplate(filename, pagesize=letter)
            story = []
            
            # Title
            title_map = {
                'escalated': 'Escalated Emails Summary',
                'cleared': 'Cleared Emails Summary',
                'flagged': 'Flagged Senders Summary',
                'department': 'Department Risk Summary'
            }
            
            title = title_map.get(report_type, 'Email Summary Report')
            story.append(Paragraph(title, self.report_styles['Title']))
            story.append(Paragraph(f"Period: {data['date_from']} to {data['date_to']}", self.report_styles['Normal']))
            story.append(Spacer(1, 20))
            
            if report_type == 'escalated':
                story.append(Paragraph(f"Total Escalated: {data['summary']['escalated_emails']}", self.report_styles['Heading3']))
                story.append(Paragraph(f"Escalation Rate: {data['summary']['escalation_rate']}%", self.report_styles['Heading3']))
                
                if data['risk_distribution']:
                    high_risk = next((item for item in data['risk_distribution'] if item['category'] == 'High Risk'), None)
                    if high_risk:
                        story.append(Paragraph(f"High Risk Emails: {high_risk['count']} ({high_risk['percentage']}%)", self.report_styles['Normal']))
            
            elif report_type == 'cleared':
                story.append(Paragraph(f"Total Cleared: {data['summary']['cleared_emails']}", self.report_styles['Heading3']))
                clearance_rate = round((data['summary']['cleared_emails'] / max(data['summary']['total_emails'], 1)) * 100, 2)
                story.append(Paragraph(f"Clearance Rate: {clearance_rate}%", self.report_styles['Normal']))
            
            elif report_type == 'flagged':
                story.append(Paragraph(f"Flagged Senders: {len(data['flagged_senders'])}", self.report_styles['Heading3']))
                if data['flagged_senders']:
                    flagged_table_data = [['Sender', 'Reason', 'Recent Activity']]
                    for item in data['flagged_senders'][:10]:
                        flagged_table_data.append([
                            item['sender'][:40],
                            item['reason'][:30],
                            f"{item['email_count_in_period']} emails"
                        ])
                    
                    flagged_table = Table(flagged_table_data)
                    flagged_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), self.colors['danger']),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    story.append(flagged_table)
            
            doc.build(story)
            logger.info(f"Summary report generated: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            raise


# Convenience functions for easy use
def generate_pdf_report(date_from: str = None, date_to: str = None) -> str:
    """Generate PDF report"""
    generator = ReportGenerator()
    return generator.generate_pdf_report(date_from, date_to)

def generate_excel_report(date_from: str = None, date_to: str = None) -> str:
    """Generate Excel report"""
    generator = ReportGenerator()
    return generator.generate_excel_report(date_from, date_to)

def generate_summary_report(report_type: str, date_from: str = None, date_to: str = None) -> str:
    """Generate summary report by type"""
    generator = ReportGenerator()
    return generator.generate_summary_report(report_type, date_from, date_to)

def cleanup_old_reports(days_old: int = 30):
    """Clean up old report files"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        reports_dir = 'reports'
        
        if not os.path.exists(reports_dir):
            return
        
        deleted_count = 0
        for filename in os.listdir(reports_dir):
            filepath = os.path.join(reports_dir, filename)
            
            if os.path.isfile(filepath):
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff_date:
                    os.remove(filepath)
                    deleted_count += 1
        
        # Also clean up chart images
        charts_dir = 'reports/charts'
        if os.path.exists(charts_dir):
            for filename in os.listdir(charts_dir):
                filepath = os.path.join(charts_dir, filename)
                if os.path.isfile(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if file_time < cutoff_date:
                        os.remove(filepath)
                        deleted_count += 1
        
        logger.info(f"Cleaned up {deleted_count} old report files")
        
    except Exception as e:
        logger.error(f"Error cleaning up old reports: {e}")


if __name__ == "__main__":
    # Command line interface for report generation
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Email Guardian reports')
    parser.add_argument('--type', choices=['pdf', 'excel', 'summary'], default='pdf', help='Report type')
    parser.add_argument('--summary-type', choices=['escalated', 'cleared', 'flagged', 'department'], 
                       help='Summary report type (required for summary reports)')
    parser.add_argument('--date-from', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date-to', help='End date (YYYY-MM-DD)')
    parser.add_argument('--cleanup', type=int, help='Clean up reports older than X days')
    
    args = parser.parse_args()
    
    try:
        if args.cleanup:
            cleanup_old_reports(args.cleanup)
        elif args.type == 'pdf':
            filename = generate_pdf_report(args.date_from, args.date_to)
            print(f"PDF report generated: {filename}")
        elif args.type == 'excel':
            filename = generate_excel_report(args.date_from, args.date_to)
            print(f"Excel report generated: {filename}")
        elif args.type == 'summary':
            if not args.summary_type:
                print("Error: --summary-type is required for summary reports")
                exit(1)
            filename = generate_summary_report(args.summary_type, args.date_from, args.date_to)
            print(f"Summary report generated: {filename}")
    except Exception as e:
        print(f"Error generating report: {e}")
        exit(1)
