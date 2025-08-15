import os
from datetime import datetime, timedelta
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from database import get_db_connection
import logging

def generate_pdf_report(date_from=None, date_to=None):
    """Generate PDF report"""
    try:
        # Set default date range if not provided
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        filename = f"reports/email_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        os.makedirs('reports', exist_ok=True)

        # Create PDF document
        doc = SimpleDocTemplate(filename, pagesize=letter)
        story = []
        styles = getSampleStyleSheet()

        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=1  # Center alignment
        )

        story.append(Paragraph("Email Guardian Report", title_style))
        story.append(Paragraph(f"Report Period: {date_from} to {date_to}", styles['Normal']))
        story.append(Spacer(1, 20))

        # Get data from database
        conn = get_db_connection()

        # Summary statistics
        summary_data = conn.execute(f"""
            SELECT 
                COUNT(*) as total_emails,
                COUNT(DISTINCT sender) as unique_senders,
                COUNT(DISTINCT department) as departments,
                SUM(CASE WHEN final_outcome = 'flagged' THEN 1 ELSE 0 END) as flagged_emails
            FROM emails 
            WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
        """).fetchone()

        # Summary table
        summary_table_data = [
            ['Metric', 'Value'],
            ['Total Emails', str(summary_data[0])],
            ['Unique Senders', str(summary_data[1])],
            ['Departments', str(summary_data[2])],
            ['Flagged Emails', str(summary_data[3])]
        ]

        summary_table = Table(summary_table_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))

        story.append(Paragraph("Summary Statistics", styles['Heading2']))
        story.append(summary_table)
        story.append(Spacer(1, 20))

        # Top senders
        top_senders_data = conn.execute(f"""
            SELECT sender, COUNT(*) as email_count
            FROM emails 
            WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY sender
            ORDER BY email_count DESC
            LIMIT 10
        """).fetchall()

        if top_senders_data:
            story.append(Paragraph("Top Email Senders", styles['Heading2']))
            sender_table_data = [['Sender', 'Email Count']]
            sender_table_data.extend([[sender, str(count)] for sender, count in top_senders_data])

            sender_table = Table(sender_table_data)
            sender_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))

            story.append(sender_table)
            story.append(Spacer(1, 20))

        # Department breakdown
        dept_data = conn.execute(f"""
            SELECT department, COUNT(*) as email_count
            FROM emails 
            WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
            AND department IS NOT NULL
            GROUP BY department
            ORDER BY email_count DESC
            LIMIT 10
        """).fetchall()

        if dept_data:
            story.append(Paragraph("Department Breakdown", styles['Heading2']))
            dept_table_data = [['Department', 'Email Count']]
            dept_table_data.extend([[dept, str(count)] for dept, count in dept_data])

            dept_table = Table(dept_table_data)
            dept_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))

            story.append(dept_table)

        conn.close()

        # Build PDF
        doc.build(story)
        logging.info(f"PDF report generated: {filename}")
        return filename

    except Exception as e:
        logging.error(f"PDF report generation error: {e}")
        raise

def generate_excel_report(date_from=None, date_to=None):
    """Generate Excel report"""
    try:
        # Set default date range if not provided
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        filename = f"reports/email_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        os.makedirs('reports', exist_ok=True)

        conn = get_db_connection()

        # Create Excel writer
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:

            # Summary sheet
            summary_data = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_emails,
                    COUNT(DISTINCT sender) as unique_senders,
                    COUNT(DISTINCT department) as departments,
                    SUM(CASE WHEN final_outcome = 'flagged' THEN 1 ELSE 0 END) as flagged_emails,
                    AVG(CASE WHEN final_outcome = 'flagged' THEN 1 ELSE 0 END) * 100 as flag_rate
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
            """).df()

            summary_data.to_excel(writer, sheet_name='Summary', index=False)

            # Detailed emails sheet
            emails_data = conn.execute(f"""
                SELECT * FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                ORDER BY _time DESC
            """).df()

            emails_data.to_excel(writer, sheet_name='Email Details', index=False)

            # Cases sheet
            cases_data = conn.execute(f"""
                SELECT c.*, e.sender, e.subject
                FROM cases c
                JOIN emails e ON c.email_id = e.id
                WHERE DATE(c.created_at) BETWEEN '{date_from}' AND '{date_to}'
                ORDER BY c.created_at DESC
            """).df()

            if not cases_data.empty:
                cases_data.to_excel(writer, sheet_name='Cases', index=False)

            # Top senders sheet
            senders_data = conn.execute(f"""
                SELECT sender, COUNT(*) as email_count,
                       SUM(CASE WHEN final_outcome = 'flagged' THEN 1 ELSE 0 END) as flagged_count
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY sender
                ORDER BY email_count DESC
                LIMIT 50
            """).df()

            senders_data.to_excel(writer, sheet_name='Top Senders', index=False)

            # Department analysis
            dept_data = conn.execute(f"""
                SELECT department, COUNT(*) as email_count,
                       COUNT(DISTINCT sender) as unique_senders,
                       SUM(CASE WHEN final_outcome = 'flagged' THEN 1 ELSE 0 END) as flagged_count
                FROM emails 
                WHERE DATE(_time) BETWEEN '{date_from}' AND '{date_to}'
                AND department IS NOT NULL
                GROUP BY department
                ORDER BY email_count DESC
            """).df()

            if not dept_data.empty:
                dept_data.to_excel(writer, sheet_name='Department Analysis', index=False)

        conn.close()

        logging.info(f"Excel report generated: {filename}")
        return filename

    except Exception as e:
        logging.error(f"Excel report generation error: {e}")
        raise

def generate_dashboard_charts_data():
    """Generate data for dashboard charts"""
    try:
        conn = get_db_connection()

        # Email volume over time (last 30 days)
        volume_data = conn.execute("""
            SELECT DATE(_time) as date, COUNT(*) as count
            FROM emails 
            WHERE _time >= (CURRENT_DATE - INTERVAL 30 DAY)
            GROUP BY DATE(_time)
            ORDER BY date
        """).fetchall()

        # Department distribution
        dept_data = conn.execute("""
            SELECT department, COUNT(*) as count
            FROM emails 
            WHERE department IS NOT NULL
            GROUP BY department
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        # Risk level distribution
        risk_data = conn.execute("""
            SELECT final_outcome, COUNT(*) as count
            FROM emails 
            WHERE final_outcome IS NOT NULL
            GROUP BY final_outcome
        """).fetchall()

        conn.close()

        return {
            'volume': volume_data,
            'departments': dept_data,
            'risk_levels': risk_data
        }

    except Exception as e:
        logging.error(f"Chart data generation error: {e}")
        return {}