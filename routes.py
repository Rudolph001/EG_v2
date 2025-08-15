import os
import csv
import pandas as pd
from flask import render_template, request, jsonify, flash, redirect, url_for, send_file
from werkzeug.utils import secure_filename
from app import app
from database import get_db_connection, get_dashboard_stats, execute_query
from ml_processor import classify_email, train_model
from ml_models import train_advanced_models, predict_email_risk, get_ml_insights, get_analytics_report
from reports import generate_pdf_report, generate_excel_report, generate_summary_report
from csv_ingest import CSVIngestor
from processor import EmailProcessor
from outlook_followup import generate_followup_email, send_followup_email, get_followup_history, bulk_generate_followups
import logging
from datetime import datetime, timedelta
import json

UPLOAD_FOLDER = 'data'
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def dashboard():
    """Main Dashboard: shows all emails not excluded or whitelisted"""
    page = request.args.get('page', 1, type=int)
    per_page = 25
    search = request.args.get('search', '')
    department = request.args.get('department', '')
    risk_level = request.args.get('risk_level', '')

    conn = get_db_connection()

    # Build query for non-excluded/whitelisted/escalated emails
    where_conditions = ["(final_outcome IS NULL OR final_outcome NOT IN ('excluded', 'whitelisted', 'escalated'))"]
    params = []

    if search:
        where_conditions.append("(sender LIKE ? OR subject LIKE ?)")
        params.extend([f'%{search}%', f'%{search}%'])

    if department:
        where_conditions.append("department = ?")
        params.append(department)

    if risk_level:
        where_conditions.append("final_outcome = ?")
        params.append(risk_level)

    where_clause = "WHERE " + " AND ".join(where_conditions)

    # Get total count
    count_query = f"SELECT COUNT(*) FROM emails {where_clause}"
    total = conn.execute(count_query, params).fetchone()[0]

    # Get paginated results with ML insights
    offset = (page - 1) * per_page
    query = f"""
        SELECT e.id, e._time, e.sender, e.subject, e.attachments, e.recipients, 
               e.time_month, e.leaver, e.termination_date, e.bunit, e.department,
               e.user_response, e.final_outcome, e.policy_name, e.justifications,
               e.created_at,
               CASE 
                   WHEN e.final_outcome IN ('high_risk', 'suspicious') THEN 'High Risk'
                   WHEN e.final_outcome IN ('medium_risk', 'warning') THEN 'Medium Risk'
                   ELSE 'Normal'
               END as risk_assessment
        FROM emails e {where_clause}
        ORDER BY e._time DESC
        LIMIT {per_page} OFFSET {offset}
    """

    emails = conn.execute(query, params).fetchall()

    # Get filter options
    departments = conn.execute("SELECT DISTINCT department FROM emails WHERE department IS NOT NULL ORDER BY department").fetchall()
    risk_levels = conn.execute("SELECT DISTINCT final_outcome FROM emails WHERE final_outcome IS NOT NULL ORDER BY final_outcome").fetchall()

    # Get dashboard stats
    stats = get_dashboard_stats()
    
    # Add excluded/whitelisted count
    excluded_whitelisted_count = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE final_outcome IN ('excluded', 'whitelisted')"
    ).fetchone()[0]
    stats['excluded_whitelisted'] = excluded_whitelisted_count
    
    # Add cleared count
    cleared_count = conn.execute(
        "SELECT COUNT(*) FROM emails WHERE final_outcome IN ('cleared', 'approved', 'resolved')"
    ).fetchone()[0]
    stats['cleared'] = cleared_count

    conn.close()

    # Calculate pagination
    has_prev = page > 1
    has_next = offset + per_page < total

    return render_template('main_dashboard.html', 
                         emails=emails, 
                         stats=stats,
                         page=page, 
                         has_prev=has_prev, 
                         has_next=has_next,
                         departments=departments,
                         risk_levels=risk_levels,
                         search=search,
                         department=department,
                         risk_level=risk_level,
                         total=total)

@app.route('/emails')
def emails():
    """Email management page"""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    search = request.args.get('search', '')
    department = request.args.get('department', '')

    conn = get_db_connection()

    # Build query with filters
    where_conditions = []
    params = []

    if search:
        where_conditions.append("(sender LIKE ? OR subject LIKE ?)")
        params.extend([f'%{search}%', f'%{search}%'])

    if department:
        where_conditions.append("department = ?")
        params.append(department)

    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)

    # Get total count
    count_query = f"SELECT COUNT(*) FROM emails {where_clause}"
    total = conn.execute(count_query, params).fetchone()[0]

    # Get paginated results
    offset = (page - 1) * per_page
    query = f"""
        SELECT * FROM emails {where_clause}
        ORDER BY _time DESC
        LIMIT {per_page} OFFSET {offset}
    """

    emails = conn.execute(query, params).fetchall()

    # Get departments for filter
    departments = conn.execute("SELECT DISTINCT department FROM emails WHERE department IS NOT NULL ORDER BY department").fetchall()

    conn.close()

    # Calculate pagination
    has_prev = page > 1
    has_next = offset + per_page < total

    return render_template('emails.html', 
                         emails=emails, 
                         page=page, 
                         has_prev=has_prev, 
                         has_next=has_next,
                         departments=departments,
                         search=search,
                         department=department,
                         total=total)

@app.route('/cases')
def cases():
    """Case management page"""
    status_filter = request.args.get('status', '')

    conn = get_db_connection()

    where_clause = ""
    params = []
    if status_filter:
        where_clause = "WHERE c.status = ?"
        params.append(status_filter)

    query = f"""
        SELECT c.*, e.sender, e.subject, e._time
        FROM cases c
        JOIN emails e ON c.email_id = e.id
        {where_clause}
        ORDER BY c.created_at DESC
    """

    cases = conn.execute(query, params).fetchall()
    conn.close()

    return render_template('cases.html', cases=cases, status_filter=status_filter)

@app.route('/excluded-whitelisted')
def excluded_whitelisted():
    """Excluded/Whitelisted Dashboard: shows filtered emails"""
    page = request.args.get('page', 1, type=int)
    per_page = 25
    filter_type = request.args.get('filter_type', 'excluded')
    search = request.args.get('search', '')

    conn = get_db_connection()

    # Build query for excluded/whitelisted emails
    where_conditions = [f"final_outcome = '{filter_type}'"]
    params = []

    if search:
        where_conditions.append("(sender LIKE ? OR subject LIKE ?)")
        params.extend([f'%{search}%', f'%{search}%'])

    where_clause = "WHERE " + " AND ".join(where_conditions)

    # Get total count
    count_query = f"SELECT COUNT(*) FROM emails {where_clause}"
    total = conn.execute(count_query, params).fetchone()[0]

    # Get paginated results
    offset = (page - 1) * per_page
    query = f"""
        SELECT * FROM emails {where_clause}
        ORDER BY _time DESC
        LIMIT {per_page} OFFSET {offset}
    """

    emails = conn.execute(query, params).fetchall()
    conn.close()

    # Calculate pagination
    has_prev = page > 1
    has_next = offset + per_page < total

    return render_template('excluded_whitelisted.html', 
                         emails=emails,
                         page=page, 
                         has_prev=has_prev, 
                         has_next=has_next,
                         filter_type=filter_type,
                         search=search,
                         total=total)

@app.route('/escalated-emails')
def escalated_emails():
    """Escalated Emails Dashboard: filter by date, sender, department, policy, risk"""
    page = request.args.get('page', 1, type=int)
    per_page = 25
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    sender = request.args.get('sender', '')
    department = request.args.get('department', '')
    policy = request.args.get('policy', '')
    risk = request.args.get('risk', '')

    conn = get_db_connection()

    # Build query for escalated emails
    where_conditions = ["final_outcome IN ('escalated', 'high_risk', 'pending_review')"]
    params = []

    if date_from:
        where_conditions.append("DATE(_time) >= ?")
        params.append(date_from)

    if date_to:
        where_conditions.append("DATE(_time) <= ?")
        params.append(date_to)

    if sender:
        where_conditions.append("sender LIKE ?")
        params.append(f'%{sender}%')

    if department:
        where_conditions.append("department = ?")
        params.append(department)

    if policy:
        where_conditions.append("policy_name = ?")
        params.append(policy)

    if risk:
        where_conditions.append("final_outcome = ?")
        params.append(risk)

    where_clause = "WHERE " + " AND ".join(where_conditions)

    # Get total count
    count_query = f"SELECT COUNT(*) FROM emails {where_clause}"
    total = conn.execute(count_query, params).fetchone()[0]

    # Get paginated results
    offset = (page - 1) * per_page
    query = f"""
        SELECT e.*, c.status as case_status, c.id as case_id
        FROM emails e
        LEFT JOIN cases c ON e.id = c.email_id
        {where_clause}
        ORDER BY e._time DESC
        LIMIT {per_page} OFFSET {offset}
    """

    emails = conn.execute(query, params).fetchall()

    # Get filter options
    departments = conn.execute("SELECT DISTINCT department FROM emails WHERE department IS NOT NULL ORDER BY department").fetchall()
    policies = conn.execute("SELECT DISTINCT policy_name FROM emails WHERE policy_name IS NOT NULL ORDER BY policy_name").fetchall()
    risks = conn.execute("SELECT DISTINCT final_outcome FROM emails WHERE final_outcome IN ('escalated', 'high_risk', 'pending_review') ORDER BY final_outcome").fetchall()

    conn.close()

    # Calculate pagination
    has_prev = page > 1
    has_next = offset + per_page < total

    return render_template('escalated_emails.html', 
                         emails=emails,
                         page=page, 
                         has_prev=has_prev, 
                         has_next=has_next,
                         departments=departments,
                         policies=policies,
                         risks=risks,
                         date_from=date_from,
                         date_to=date_to,
                         sender=sender,
                         department=department,
                         policy=policy,
                         risk=risk,
                         total=total)

@app.route('/cleared-emails')
def cleared_emails():
    """Cleared Emails Dashboard: lists cleared emails"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = 25
        search = request.args.get('search', '')
        department = request.args.get('department', '')

        conn = get_db_connection()

        # Build query for cleared emails
        where_conditions = ["final_outcome IN ('cleared', 'approved', 'resolved')"]
        params = []

        if search:
            where_conditions.append("(sender LIKE ? OR subject LIKE ?)")
            params.extend([f'%{search}%', f'%{search}%'])

        if department:
            where_conditions.append("department = ?")
            params.append(department)

        where_clause = "WHERE " + " AND ".join(where_conditions)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM emails {where_clause}"
        total = conn.execute(count_query, params).fetchone()[0]

        # Get paginated results
        offset = (page - 1) * per_page
        query = f"""
            SELECT * FROM emails {where_clause}
            ORDER BY _time DESC
            LIMIT {per_page} OFFSET {offset}
        """

        emails = conn.execute(query, params).fetchall()

        # Get departments for filter
        departments = conn.execute("SELECT DISTINCT department FROM emails WHERE department IS NOT NULL ORDER BY department").fetchall()

        conn.close()

        # Calculate pagination
        has_prev = page > 1
        has_next = offset + per_page < total

        return render_template('cleared_emails.html', 
                             emails=emails,
                             page=page, 
                             has_prev=has_prev, 
                             has_next=has_next,
                             departments=departments,
                             search=search,
                             department=department,
                             total=total)
    except Exception as e:
        logging.error(f"Cleared emails dashboard error: {e}")
        flash(f'Error loading cleared emails: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

@app.route('/flagged-senders')
def flagged_senders():
    """Flagged Senders Dashboard: monitor flagged senders across imports"""
    try:
        conn = get_db_connection()

        # Get flagged senders with email counts - simplified query
        try:
            senders_raw = conn.execute("""
                SELECT id, sender, reason, flagged_at
                FROM flagged_senders
                ORDER BY flagged_at DESC
            """).fetchall()
            
            # Process each sender to get email counts
            senders = []
            for sender_row in senders_raw:
                try:
                    # Get email count for this sender
                    email_count = conn.execute("""
                        SELECT COUNT(*) FROM emails WHERE sender = ?
                    """, [sender_row[1]]).fetchone()[0]
                    
                    # Get last email date
                    last_email = conn.execute("""
                        SELECT MAX(_time) FROM emails WHERE sender = ?
                    """, [sender_row[1]]).fetchone()[0]
                    
                    # Get high risk count
                    high_risk_count = conn.execute("""
                        SELECT COUNT(*) FROM emails 
                        WHERE sender = ? AND final_outcome IN ('escalated', 'high_risk')
                    """, [sender_row[1]]).fetchone()[0]
                    
                    # Create enhanced sender record
                    sender_dict = {
                        0: sender_row[0],  # id
                        1: sender_row[1],  # sender
                        2: sender_row[2],  # reason
                        3: sender_row[3],  # flagged_at
                        'email_count': email_count,
                        'last_email_date': last_email,
                        'high_risk_count': high_risk_count
                    }
                    senders.append(sender_dict)
                    
                except Exception as e:
                    logging.error(f"Error processing sender {sender_row[1]}: {e}")
                    # Add basic sender info even if email stats fail
                    sender_dict = {
                        0: sender_row[0],
                        1: sender_row[1],
                        2: sender_row[2],
                        3: sender_row[3],
                        'email_count': 0,
                        'last_email_date': None,
                        'high_risk_count': 0
                    }
                    senders.append(sender_dict)
                    
        except Exception as e:
            logging.error(f"Error getting flagged senders: {e}")
            senders = []

        # Get domain statistics - simplified approach
        try:
            # Get all flagged senders first
            flagged_senders_list = conn.execute("SELECT sender FROM flagged_senders").fetchall()
            domain_counts = {}
            
            for sender_row in flagged_senders_list:
                sender = sender_row[0]
                if '@' in sender:
                    domain = sender.split('@')[1].lower()
                    domain_counts[domain] = domain_counts.get(domain, 0) + 1
            
            # Sort by count and limit to top 10
            domain_stats = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
        except Exception as e:
            logging.error(f"Error getting domain stats: {e}")
            domain_stats = []

        conn.close()

        return render_template('flagged_senders.html', 
                             senders=senders,
                             domain_stats=dict(domain_stats))
                             
    except Exception as e:
        logging.error(f"Flagged senders dashboard error: {e}")
        flash(f'Error loading flagged senders: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

@app.route('/analytics')
def analytics():
    """Analytics Dashboard: charts for policy violations, escalations, risk categories"""
    try:
        conn = get_db_connection()

        # Policy violations data
        try:
            policy_violations = conn.execute("""
                SELECT policy_name, COUNT(*) as count
                FROM emails 
                WHERE policy_name IS NOT NULL AND policy_name != ''
                GROUP BY policy_name
                ORDER BY count DESC
                LIMIT 10
            """).fetchall()
        except Exception as e:
            logging.error(f"Policy violations query error: {e}")
            policy_violations = []

        # Escalations over time (last 30 days)
        try:
            escalations_timeline = conn.execute("""
                SELECT CAST(_time AS DATE) as date, COUNT(*) as count
                FROM emails 
                WHERE final_outcome IN ('escalated', 'high_risk', 'pending_review')
                AND _time >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY CAST(_time AS DATE)
                ORDER BY date
            """).fetchall()
        except Exception as e:
            logging.error(f"Escalations timeline query error: {e}")
            escalations_timeline = []

        # Risk categories distribution - simplified
        try:
            risk_categories = conn.execute("""
                SELECT 
                    CASE 
                        WHEN final_outcome IN ('high_risk', 'escalated') THEN 'High Risk'
                        WHEN final_outcome IN ('medium_risk', 'warning') THEN 'Medium Risk'
                        WHEN final_outcome IN ('cleared', 'approved') THEN 'Low Risk'
                        ELSE 'Unknown'
                    END as risk_level,
                    COUNT(*) as count
                FROM emails
                WHERE final_outcome IS NOT NULL
                GROUP BY CASE 
                    WHEN final_outcome IN ('high_risk', 'escalated') THEN 'High Risk'
                    WHEN final_outcome IN ('medium_risk', 'warning') THEN 'Medium Risk'
                    WHEN final_outcome IN ('cleared', 'approved') THEN 'Low Risk'
                    ELSE 'Unknown'
                END
                ORDER BY count DESC
            """).fetchall()
        except Exception as e:
            logging.error(f"Risk categories query error: {e}")
            risk_categories = []

        # Department risk analysis
        try:
            dept_risk = conn.execute("""
                SELECT 
                    department,
                    COUNT(*) as total_emails,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as high_risk_count,
                    ROUND(COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) * 100.0 / COUNT(*), 2) as risk_percentage
                FROM emails
                WHERE department IS NOT NULL AND department != ''
                GROUP BY department
                HAVING COUNT(*) > 5
                ORDER BY risk_percentage DESC
                LIMIT 10
            """).fetchall()
        except Exception as e:
            logging.error(f"Department risk query error: {e}")
            dept_risk = []

        # Monthly trend data - simplified
        try:
            monthly_trends = conn.execute("""
                SELECT 
                    strftime('%Y-%m', _time) as month,
                    COUNT(*) as total_emails,
                    COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as escalated_emails
                FROM emails
                WHERE _time >= CURRENT_DATE - INTERVAL 12 MONTH
                GROUP BY strftime('%Y-%m', _time)
                ORDER BY month
            """).fetchall()
        except Exception as e:
            logging.error(f"Monthly trends query error: {e}")
            monthly_trends = []

        conn.close()

        analytics_data = {
            'policy_violations': policy_violations,
            'escalations_timeline': escalations_timeline,
            'risk_categories': risk_categories,
            'dept_risk': dept_risk,
            'monthly_trends': monthly_trends
        }

        return render_template('analytics.html', analytics=analytics_data)
    
    except Exception as e:
        logging.error(f"Analytics dashboard error: {e}")
        # Return empty analytics data to prevent template errors
        analytics_data = {
            'policy_violations': [],
            'escalations_timeline': [],
            'risk_categories': [],
            'dept_risk': [],
            'monthly_trends': []
        }
        return render_template('analytics.html', analytics=analytics_data)

@app.route('/admin-rules')
def admin_rules():
    """Admin rules configuration"""
    conn = get_db_connection()
    rules_raw = conn.execute("SELECT * FROM admin_rules ORDER BY created_at DESC").fetchall()
    conn.close()

    # Process rules to extract rule names from conditions JSON
    processed_rules = []
    for rule in rules_raw:
        # Handle the rule row structure properly
        rule_dict = {
            'id': rule[0],
            'rule_type': rule[1] if len(rule) > 1 else 'unknown',
            'conditions': rule[2] if len(rule) > 2 else None,
            'action': rule[3] if len(rule) > 3 else 'flag',
            'is_active': bool(rule[4]) if len(rule) > 4 else False,
            'created_at': rule[5].strftime('%Y-%m-%d') if len(rule) > 5 and rule[5] and hasattr(rule[5], 'strftime') else str(rule[5]) if len(rule) > 5 and rule[5] else 'N/A',
        }
        
        # Extract actual rule name from conditions JSON
        try:
            if rule_dict['conditions']:
                import json
                conditions_data = json.loads(rule_dict['conditions'])
                rule_dict['display_name'] = conditions_data.get('rule_name', f"Rule {rule_dict['id']}")
                rule_dict['condition_count'] = len(conditions_data.get('conditions', []))
                rule_dict['display_logic'] = conditions_data.get('logic_type', 'Simple')
            else:
                rule_dict['display_name'] = f"Rule {rule_dict['id']}"
                rule_dict['condition_count'] = 0
                rule_dict['display_logic'] = 'Simple'
        except (json.JSONDecodeError, TypeError):
            rule_dict['display_name'] = f"Rule {rule_dict['id']}"
            rule_dict['condition_count'] = 0 
            rule_dict['display_logic'] = 'Simple'
        
        processed_rules.append(rule_dict)

    return render_template('admin_rules.html', rules=processed_rules)

@app.route('/admin-panel')
def admin_panel():
    """Comprehensive admin panel"""
    conn = get_db_connection()

    # Get stats for dashboard cards
    stats = {
        'exclusion_rules': conn.execute("SELECT COUNT(*) FROM admin_rules WHERE rule_type = 'exclusion'").fetchone()[0],
        'whitelist_rules': conn.execute("SELECT COUNT(*) FROM admin_rules WHERE rule_type = 'whitelist'").fetchone()[0],
        'security_rules': conn.execute("SELECT COUNT(*) FROM admin_rules WHERE rule_type = 'security'").fetchone()[0],
        'risk_keywords': conn.execute("SELECT COUNT(*) FROM admin_rules WHERE rule_type = 'risk_keyword'").fetchone()[0],
        'exclude_keywords': conn.execute("SELECT COUNT(*) FROM admin_rules WHERE rule_type = 'exclude_keyword'").fetchone()[0],
        'ml_models': 1  # Assuming single model for now
    }

    # Get ML status
    ml_status = {
        'last_trained': 'Today',  # Mock data
        'accuracy': 85  # Mock data
    }

    conn.close()

    return render_template('admin_panel.html', stats=stats, ml_status=ml_status)

@app.route('/import-data')
def import_data():
    """Data import page"""
    return render_template('import_data.html')

@app.route('/reports')
def reports():
    """Reports page"""
    return render_template('reports.html')

@app.route('/api/upload-csv', methods=['POST'])
def upload_csv():
    """Handle CSV file upload and bulk import"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file selected'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if file and file.filename and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(filepath)

            # Process CSV with pandas first to validate and transform data
            import pandas as pd
            from datetime import datetime
            df = pd.read_csv(filepath)
            
            # Validate required columns - be flexible with column names
            possible_time_cols = ['_time', 'time', 'timestamp', 'date', 'sent_time']
            possible_sender_cols = ['sender', 'from', 'email_from', 'from_address']
            possible_subject_cols = ['subject', 'title', 'email_subject']
            
            # Find the actual column names
            time_col = None
            sender_col = None
            subject_col = None
            
            for col in df.columns:
                if col.lower() in [c.lower() for c in possible_time_cols]:
                    time_col = col
                if col.lower() in [c.lower() for c in possible_sender_cols]:
                    sender_col = col
                if col.lower() in [c.lower() for c in possible_subject_cols]:
                    subject_col = col
            
            if not time_col or not sender_col or not subject_col:
                os.remove(filepath)
                return jsonify({'error': f'Required columns not found. Please ensure your CSV has time/date, sender, and subject columns.'}), 400
            
            # Rename columns to standard names
            column_mapping = {}
            if time_col != '_time':
                column_mapping[time_col] = '_time'
            if sender_col != 'sender':
                column_mapping[sender_col] = 'sender'
            if subject_col != 'subject':
                column_mapping[subject_col] = 'subject'
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist with defaults
            default_columns = {
                'recipients': '',
                'attachments': '',
                'time_month': '',
                'leaver': '',
                'termination_date': None,
                'bunit': '',
                'department': '',
                'user_response': '',
                'final_outcome': '',
                'policy_name': '',
                'justifications': ''
            }
            
            for col, default_val in default_columns.items():
                if col not in df.columns:
                    df[col] = default_val
            
            # Fix date formats - convert various date formats to YYYY-MM-DD HH:MM:SS
            def fix_date_format(date_str):
                if pd.isna(date_str) or str(date_str).strip() == '-' or str(date_str).strip() == '':
                    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                date_str = str(date_str).strip()
                
                # Try different date formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',  # Already correct
                    '%Y-%m-%d',           # Date only
                    '%m/%d/%Y',           # MM/DD/YYYY
                    '%m/%d/%Y %H:%M:%S',  # MM/DD/YYYY HH:MM:SS
                    '%d/%m/%Y',           # DD/MM/YYYY
                    '%Y/%m/%d',           # YYYY/MM/DD
                ]
                
                for fmt in formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        continue
                
                # If no format matches, use current time
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Apply date fixing to _time column
            df['_time'] = df['_time'].apply(fix_date_format)
            
            # Fix termination_date column if it exists
            if 'termination_date' in df.columns:
                def fix_termination_date(date_str):
                    if pd.isna(date_str) or str(date_str).strip() == '-' or str(date_str).strip() == '':
                        return None
                    
                    date_str = str(date_str).strip()
                    
                    formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
                    for fmt in formats:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            return parsed_date.strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                    return None
                
                df['termination_date'] = df['termination_date'].apply(fix_termination_date)
            
            # Process CSV with DuckDB for high performance
            conn = get_db_connection()
            
            # Insert records one by one to handle any schema mismatches
            rows_inserted = 0
            for idx, row in df.iterrows():
                try:
                    # Debug log first few rows
                    if idx < 3:
                        logging.info(f"Inserting row {idx}: {row['sender']}, {row['subject'][:30]}...")
                    
                    conn.execute("""
                        INSERT INTO emails (
                            _time, sender, subject, attachments, recipients, 
                            time_month, leaver, termination_date, bunit, department,
                            user_response, final_outcome, policy_name, justifications
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        row['_time'], row['sender'], row['subject'], 
                        row['attachments'], row['recipients'], row['time_month'],
                        row['leaver'], row['termination_date'], row['bunit'], 
                        row['department'], row['user_response'], row['final_outcome'],
                        row['policy_name'], row['justifications']
                    ])
                    rows_inserted += 1
                except Exception as e:
                    logging.error(f"Failed to insert row {idx}: {e}")
                    logging.error(f"Row data: sender='{row.get('sender', 'N/A')}', subject='{row.get('subject', 'N/A')}'")
                    continue
            
            # Verify the inserts worked
            verification_count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
            logging.info(f"Database verification: {verification_count} total emails in database after insert")
            
            conn.close()

            # Process newly imported emails automatically
            if rows_inserted > 0:
                try:
                    from processor import EmailProcessor
                    processor = EmailProcessor()
                    
                    # Clear final_outcome for newly imported emails to force reprocessing
                    conn_reprocess = get_db_connection()
                    conn_reprocess.execute("UPDATE emails SET final_outcome = NULL WHERE final_outcome IS NOT NULL")
                    conn_reprocess.close()
                    logging.info(f"Cleared final_outcome for reprocessing of {rows_inserted} emails")
                    
                    # Process the newly imported emails
                    processing_results = processor.process_batch(limit=rows_inserted)
                    processing_message = f" - {processing_results['processed']} emails analyzed, {processing_results['escalated']} escalated to cases"
                except Exception as e:
                    logging.warning(f"Auto-processing failed: {e}")
                    processing_message = " - Note: Email analysis will run later"
            else:
                processing_message = ""

            # Clean up uploaded file
            os.remove(filepath)

            return jsonify({
                'success': True, 
                'message': f'Successfully imported {rows_inserted} out of {len(df)} email records{processing_message}'
            })

        except Exception as e:
            logging.error(f"CSV upload error: {e}")
            if 'filepath' in locals() and os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({'error': f'Import failed: {str(e)}'}), 500

    return jsonify({'error': 'Invalid file type. Please upload a CSV file.'}), 400

@app.route('/api/flag-sender', methods=['POST'])
def flag_sender():
    """Flag a sender"""
    data = request.json or {}
    sender = data.get('sender')
    reason = data.get('reason')

    if not sender or not reason:
        return jsonify({'error': 'Sender and reason are required'}), 400

    try:
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO flagged_senders (sender, reason) 
            VALUES (?, ?)
        """, [sender, reason])
        conn.close()

        return jsonify({'success': True, 'message': 'Sender flagged successfully'})
    except Exception as e:
        logging.error(f"Flag sender error: {e}")
        return jsonify({'error': 'Failed to flag sender'}), 500

@app.route('/api/unflag-sender/<int:flag_id>', methods=['DELETE'])
def unflag_sender(flag_id):
    """Remove flag from a sender"""
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM flagged_senders WHERE id = ?", [flag_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Sender unflagged successfully'})
    except Exception as e:
        logging.error(f"Unflag sender error: {e}")
        return jsonify({'error': 'Failed to unflag sender'}), 500

@app.route('/api/create-case', methods=['POST'])
def create_case():
    """Create a new case"""
    data = request.json or {}
    email_id = data.get('email_id')
    escalation_reason = data.get('escalation_reason')

    if not email_id or not escalation_reason:
        return jsonify({'error': 'Email ID and escalation reason are required'}), 400

    try:
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO cases (email_id, escalation_reason) 
            VALUES (?, ?)
        """, [email_id, escalation_reason])
        conn.close()

        return jsonify({'success': True, 'message': 'Case created successfully'})
    except Exception as e:
        logging.error(f"Create case error: {e}")
        return jsonify({'error': 'Failed to create case'}), 500

@app.route('/api/case-details/<int:case_id>')
def api_case_details(case_id):
    """Get detailed case information"""
    try:
        conn = get_db_connection()
        case_data = conn.execute("""
            SELECT c.id, c.email_id, c.escalation_reason, c.status, c.created_at, c.updated_at,
                   e.sender, e.subject, e._time, e.department, e.justifications, e.final_outcome
            FROM cases c
            JOIN emails e ON c.email_id = e.id
            WHERE c.id = ?
        """, [case_id]).fetchone()
        conn.close()

        if not case_data:
            return jsonify({'error': 'Case not found'}), 404

        # Convert to dict for JSON response
        case_details = {
            'case_id': case_data[0],
            'email_id': case_data[1],
            'escalation_reason': case_data[2],
            'status': case_data[3],
            'created_at': case_data[4].isoformat() if case_data[4] else None,
            'updated_at': case_data[5].isoformat() if case_data[5] else None,
            'email_sender': case_data[6],
            'email_subject': case_data[7],
            'email_time': case_data[8].isoformat() if case_data[8] else None,
            'email_department': case_data[9],
            'email_justifications': case_data[10],
            'email_final_outcome': case_data[11]
        }

        return jsonify(case_details)
    except Exception as e:
        logging.error(f"Case details error: {e}")
        return jsonify({'error': 'Failed to load case details'}), 500

@app.route('/api/update-case-status', methods=['POST'])
def update_case_status():
    """Update case status"""
    data = request.json or {}
    case_id = data.get('case_id')
    status = data.get('status')

    try:
        conn = get_db_connection()
        conn.execute("""
            UPDATE cases 
            SET status = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, [status, case_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Case status updated'})
    except Exception as e:
        logging.error(f"Update case error: {e}")
        return jsonify({'error': 'Failed to update case'}), 500

@app.route('/api/add-admin-rule', methods=['POST'])
def add_admin_rule():
    """Add new admin rule"""
    data = request.json or {}

    try:
        # Log the incoming data for debugging
        logging.info(f"Creating admin rule with data: {data}")
        
        # Extract and validate required fields
        rule_name = data.get('rule_name', 'Unnamed Rule')
        rule_type = data.get('rule_type', 'advanced_rule')
        action = data.get('action', 'flag')
        conditions = data.get('conditions', '{}')
        is_active = bool(data.get('is_active', True))
        
        # Ensure conditions is a string
        if isinstance(conditions, dict):
            conditions = json.dumps(conditions)
        
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active, created_at) 
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [rule_type, conditions, action, is_active])
        
        # Get the ID of the newly created rule
        rule_id = conn.execute("SELECT MAX(id) FROM admin_rules").fetchone()[0]
        conn.close()
        
        logging.info(f"Successfully created rule with ID: {rule_id}")

        return jsonify({
            'success': True, 
            'message': f'Rule "{rule_name}" added successfully',
            'rule_id': rule_id
        })
    except Exception as e:
        logging.error(f"Add rule error: {e}")
        return jsonify({'error': f'Failed to add rule: {str(e)}'}), 500

@app.route('/api/admin/save-rule', methods=['POST'])
def api_admin_save_rule():
    """Save admin rule with advanced logic"""
    data = request.json or {}

    try:
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active) 
            VALUES (?, ?, ?, ?)
        """, [
            data.get('rule_type'),
            data.get('conditions'),
            data.get('logic_type', 'AND'),
            data.get('is_active', True)
        ])
        conn.close()

        return jsonify({'success': True, 'message': 'Rule saved successfully'})
    except Exception as e:
        logging.error(f"Save rule error: {e}")
        return jsonify({'error': 'Failed to save rule'}), 500

@app.route('/api/admin/rules/<rule_type>')
def api_admin_get_rules(rule_type):
    """Get rules by type"""
    try:
        conn = get_db_connection()
        rules = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE rule_type = ?
            ORDER BY created_at DESC
        """, [rule_type]).fetchall()
        conn.close()

        rules_list = []
        for rule in rules:
            rules_list.append({
                'id': rule[0],
                'rule_name': f"Rule {rule[0]}",
                'logic_type': 'AND',  # Default for display
                'conditions_summary': rule[2][:50] + '...' if len(rule[2]) > 50 else rule[2],
                'is_active': rule[4],
                'created_at': rule[5].strftime('%Y-%m-%d') if rule[5] else 'N/A'
            })

        return jsonify(rules_list)
    except Exception as e:
        logging.error(f"Get rules error: {e}")
        return jsonify({'error': 'Failed to load rules'}), 500

@app.route('/api/admin/delete-rule/<int:rule_id>', methods=['DELETE'])
def api_admin_delete_rule(rule_id):
    """Delete admin rule"""
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM admin_rules WHERE id = ?", [rule_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Rule deleted successfully'})
    except Exception as e:
        logging.error(f"Delete rule error: {e}")
        return jsonify({'error': 'Failed to delete rule'}), 500

@app.route('/api/admin/available-fields')
def api_admin_get_available_fields():
    """Get available database fields for rule building"""
    try:
        conn = get_db_connection()
        
        # Get actual column names from emails table
        columns_info = conn.execute("PRAGMA table_info(emails)").fetchall()
        fields = [col[1] for col in columns_info]  # col[1] is the column name
        
        # Get sample data to show field contents
        sample_data = {}
        try:
            sample_email = conn.execute("SELECT * FROM emails LIMIT 1").fetchone()
            if sample_email:
                for i, field in enumerate(fields):
                    sample_value = sample_email[i] if i < len(sample_email) else None
                    if sample_value is not None:
                        sample_data[field] = str(sample_value)[:50]  # First 50 chars
        except Exception as e:
            logging.warning(f"Could not get sample data: {e}")
        
        conn.close()

        return jsonify({
            'success': True,
            'fields': fields,
            'sample_data': sample_data,
            'field_descriptions': {
                'id': 'Email ID (numeric)',
                '_time': 'Email timestamp',
                'sender': 'Email sender address',
                'subject': 'Email subject line',
                'attachments': 'File attachments',
                'recipients': 'Email recipients',
                'time_month': 'Month of email',
                'leaver': 'Is sender a former employee',
                'termination_date': 'Employee termination date',
                'bunit': 'Business unit',
                'department': 'Department',
                'user_response': 'User response to email',
                'final_outcome': 'Processing outcome',
                'policy_name': 'Policy violation name',
                'justifications': 'Email content/justifications',
                'created_at': 'Record creation date'
            }
        })
    except Exception as e:
        logging.error(f"Get available fields error: {e}")
        return jsonify({'error': 'Failed to load available fields'}), 500

@app.route('/api/admin/test-rule', methods=['POST'])
def api_admin_test_rule():
    """Test rule conditions against existing emails"""
    try:
        data = request.json or {}
        conditions = data.get('conditions', [])
        logic_type = data.get('logic_type', 'AND')
        
        if not conditions:
            return jsonify({'error': 'No conditions provided'}), 400

        conn = get_db_connection()
        
        # Build test query
        where_clauses = []
        params = []
        
        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            case_sensitive = condition.get('case_sensitive', False)
            
            if not all([field, operator, value]):
                continue
                
            # Build condition based on operator
            if operator == 'contains':
                if case_sensitive:
                    where_clauses.append(f"{field} LIKE ?")
                    params.append(f'%{value}%')
                else:
                    where_clauses.append(f"LOWER({field}) LIKE LOWER(?)")
                    params.append(f'%{value}%')
            elif operator == 'equals':
                if case_sensitive:
                    where_clauses.append(f"{field} = ?")
                    params.append(value)
                else:
                    where_clauses.append(f"LOWER({field}) = LOWER(?)")
                    params.append(value)
            elif operator == 'not_contains':
                if case_sensitive:
                    where_clauses.append(f"({field} NOT LIKE ? OR {field} IS NULL)")
                    params.append(f'%{value}%')
                else:
                    where_clauses.append(f"(LOWER({field}) NOT LIKE LOWER(?) OR {field} IS NULL)")
                    params.append(f'%{value}%')
            elif operator == 'starts_with':
                if case_sensitive:
                    where_clauses.append(f"{field} LIKE ?")
                    params.append(f'{value}%')
                else:
                    where_clauses.append(f"LOWER({field}) LIKE LOWER(?)")
                    params.append(f'{value}%')
            elif operator == 'ends_with':
                if case_sensitive:
                    where_clauses.append(f"{field} LIKE ?")
                    params.append(f'%{value}')
                else:
                    where_clauses.append(f"LOWER({field}) LIKE LOWER(?)")
                    params.append(f'%{value}')
            elif operator == 'regex':
                # SQLite doesn't have built-in regex, so we'll use LIKE with wildcards
                where_clauses.append(f"{field} LIKE ?")
                params.append(f'%{value}%')
        
        if not where_clauses:
            return jsonify({'error': 'No valid conditions found'}), 400
        
        # Combine conditions with logic type
        logic_operator = ' AND ' if logic_type == 'AND' else ' OR '
        where_clause = f"WHERE ({logic_operator.join(where_clauses)})"
        
        # Get matching emails (limit to 100 for performance)
        query = f"""
            SELECT id, sender, subject, COUNT(*) OVER() as total_count
            FROM emails 
            {where_clause}
            LIMIT 100
        """
        
        results = conn.execute(query, params).fetchall()
        
        # Get total count of all emails for comparison
        total_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        
        conn.close()
        
        matching_count = results[0][3] if results else 0
        sample_matches = [f"ID {r[0]}: {r[1]} - {r[2][:50]}..." for r in results[:5]]
        
        return jsonify({
            'success': True,
            'matching_count': matching_count,
            'total_count': total_emails,
            'sample_matches': sample_matches,
            'logic_type': logic_type,
            'conditions_tested': len(conditions)
        })
        
    except Exception as e:
        logging.error(f"Test rule error: {e}")
        return jsonify({'error': f'Rule test failed: {str(e)}'}), 500

@app.route('/api/admin/rule/<int:rule_id>')
def api_admin_get_rule(rule_id):
    """Get rule details for editing"""
    try:
        conn = get_db_connection()
        rule = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE id = ?
        """, [rule_id]).fetchone()
        conn.close()

        if not rule:
            return jsonify({'error': 'Rule not found'}), 404

        # Parse conditions if it's JSON
        conditions_data = rule[2]
        try:
            parsed_conditions = json.loads(conditions_data) if conditions_data else {}
        except (json.JSONDecodeError, TypeError):
            parsed_conditions = {'conditions': conditions_data}

        rule_data = {
            'id': rule[0],
            'rule_type': rule[1],
            'conditions': rule[2],
            'action': rule[3],
            'is_active': rule[4],
            'created_at': rule[5].strftime('%Y-%m-%d') if rule[5] else 'N/A',
            'rule_name': parsed_conditions.get('rule_name', f'Rule {rule[0]}'),
            'logic_type': parsed_conditions.get('logic_type', 'AND'),
            'parsed_conditions': parsed_conditions.get('conditions', [])
        }

        return jsonify(rule_data)
    except Exception as e:
        logging.error(f"Get rule error: {e}")
        return jsonify({'error': 'Failed to load rule'}), 500

@app.route('/api/admin/toggle-rule', methods=['POST'])
def api_admin_toggle_rule():
    """Toggle rule active status"""
    try:
        data = request.json or {}
        rule_id = data.get('rule_id')
        is_active = data.get('is_active', True)

        if not rule_id:
            return jsonify({'error': 'Rule ID is required'}), 400

        conn = get_db_connection()
        result = conn.execute("""
            UPDATE admin_rules 
            SET is_active = ?
            WHERE id = ?
        """, [is_active, rule_id])
        
        if result.rowcount == 0:
            conn.close()
            return jsonify({'error': 'Rule not found'}), 404
            
        conn.close()

        return jsonify({
            'success': True, 
            'message': f'Rule {"enabled" if is_active else "disabled"} successfully'
        })
    except Exception as e:
        logging.error(f"Toggle rule error: {e}")
        return jsonify({'error': 'Failed to toggle rule'}), 500

@app.route('/api/admin/add-keyword', methods=['POST'])
def api_admin_add_keyword():
    """Add keyword to risk or exclusion list"""
    data = request.json or {}
    keyword_type = data.get('type')  # 'risk' or 'exclude'
    keyword = data.get('keyword')

    try:
        conn = get_db_connection()
        rule_type = f"{keyword_type}_keyword"
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active) 
            VALUES (?, ?, ?, ?)
        """, [rule_type, keyword, 'flag', True])
        conn.close()

        return jsonify({'success': True, 'message': 'Keyword added successfully'})
    except Exception as e:
        logging.error(f"Add keyword error: {e}")
        return jsonify({'error': 'Failed to add keyword'}), 500

@app.route('/api/admin/remove-keyword', methods=['POST'])
def api_admin_remove_keyword():
    """Remove keyword from risk or exclusion list"""
    data = request.json or {}
    keyword_type = data.get('type')
    keyword = data.get('keyword')

    try:
        conn = get_db_connection()
        rule_type = f"{keyword_type}_keyword"
        conn.execute("""
            DELETE FROM admin_rules 
            WHERE rule_type = ? AND conditions = ?
        """, [rule_type, keyword])
        conn.close()

        return jsonify({'success': True, 'message': 'Keyword removed successfully'})
    except Exception as e:
        logging.error(f"Remove keyword error: {e}")
        return jsonify({'error': 'Failed to remove keyword'}), 500

@app.route('/api/admin/keywords/<keyword_type>')
def api_admin_get_keywords(keyword_type):
    """Get keywords by type"""
    try:
        conn = get_db_connection()
        rule_type = f"{keyword_type}_keyword"
        keywords = conn.execute("""
            SELECT conditions FROM admin_rules 
            WHERE rule_type = ? AND is_active = true
        """, [rule_type]).fetchall()
        conn.close()

        return jsonify([k[0] for k in keywords])
    except Exception as e:
        logging.error(f"Get keywords error: {e}")
        return jsonify({'error': 'Failed to load keywords'}), 500

@app.route('/api/admin/add-whitelist', methods=['POST'])
def api_admin_add_whitelist():
    """Add item to whitelist"""
    data = request.json or {}
    whitelist_type = data.get('type')  # 'sender' or 'domain'
    value = data.get('value')

    try:
        conn = get_db_connection()
        rule_type = f"whitelist_{whitelist_type}"
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active) 
            VALUES (?, ?, ?, ?)
        """, [rule_type, value, 'whitelist', True])
        conn.close()

        return jsonify({'success': True, 'message': 'Whitelist item added successfully'})
    except Exception as e:
        logging.error(f"Add whitelist error: {e}")
        return jsonify({'error': 'Failed to add whitelist item'}), 500

@app.route('/api/admin/remove-whitelist', methods=['POST'])
def api_admin_remove_whitelist():
    """Remove item from whitelist"""
    data = request.json or {}
    whitelist_type = data.get('type')
    value = data.get('value')

    try:
        conn = get_db_connection()
        rule_type = f"whitelist_{whitelist_type}"
        conn.execute("""
            DELETE FROM admin_rules 
            WHERE rule_type = ? AND conditions = ?
        """, [rule_type, value])
        conn.close()

        return jsonify({'success': True, 'message': 'Whitelist item removed successfully'})
    except Exception as e:
        logging.error(f"Remove whitelist error: {e}")
        return jsonify({'error': 'Failed to remove whitelist item'}), 500

@app.route('/api/admin/whitelist/<whitelist_type>')
def api_admin_get_whitelist(whitelist_type):
    """Get whitelist items by type"""
    try:
        conn = get_db_connection()
        rule_type = f"whitelist_{whitelist_type}"
        items = conn.execute("""
            SELECT conditions FROM admin_rules 
            WHERE rule_type = ? AND is_active = true
        """, [rule_type]).fetchall()
        conn.close()

        return jsonify([item[0] for item in items])
    except Exception as e:
        logging.error(f"Get whitelist error: {e}")
        return jsonify({'error': 'Failed to load whitelist'}), 500

@app.route('/api/admin/save-ml-settings', methods=['POST'])
def api_admin_save_ml_settings():
    """Save ML configuration settings"""
    data = request.json or {}

    try:
        # In a real implementation, you would save these to a settings table
        # For now, we'll just return success
        logging.info(f"ML settings saved: {data}")

        return jsonify({'success': True, 'message': 'ML settings saved successfully'})
    except Exception as e:
        logging.error(f"Save ML settings error: {e}")
        return jsonify({'error': 'Failed to save ML settings'}), 500

@app.route('/api/admin/ml-settings')
def api_admin_get_ml_settings():
    """Get current ML settings"""
    try:
        # Return default settings for now
        settings = {
            'risk_threshold': 70,
            'confidence_threshold': 85,
            'retrain_frequency': 'weekly',
            'enable_ml_override': True,
            'enable_auto_escalation': True
        }

        return jsonify(settings)
    except Exception as e:
        logging.error(f"Get ML settings error: {e}")
        return jsonify({'error': 'Failed to load ML settings'}), 500

@app.route('/api/admin/reset-model', methods=['POST'])
def api_admin_reset_model():
    """Reset ML model"""
    try:
        # In a real implementation, you would delete model files and training data
        logging.info("ML model reset requested")

        return jsonify({'success': True, 'message': 'Model reset successfully'})
    except Exception as e:
        logging.error(f"Reset model error: {e}")
        return jsonify({'error': 'Failed to reset model'}), 500

@app.route('/api/admin/save-processor-settings', methods=['POST'])
def api_admin_save_processor_settings():
    """Save processor configuration settings"""
    try:
        data = request.json or {}
        
        # Save settings to admin_rules table as configuration
        conn = get_db_connection()
        
        # Remove existing processor settings
        conn.execute("DELETE FROM admin_rules WHERE rule_type = 'processor_config'")
        
        # Save new settings
        settings_json = json.dumps(data)
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active, created_at) 
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, ['processor_config', settings_json, 'configure', True])
        
        conn.close()
        
        logging.info(f"Processor settings saved: {data}")
        return jsonify({'success': True, 'message': 'Processor settings saved successfully'})
    except Exception as e:
        logging.error(f"Save processor settings error: {e}")
        return jsonify({'error': 'Failed to save processor settings'}), 500

@app.route('/api/admin/processor-settings')
def api_admin_get_processor_settings():
    """Get current processor settings"""
    try:
        conn = get_db_connection()
        
        # Get settings from admin_rules table
        settings_row = conn.execute("""
            SELECT conditions FROM admin_rules 
            WHERE rule_type = 'processor_config' AND is_active = true
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()
        
        conn.close()
        
        if settings_row and settings_row[0]:
            try:
                settings = json.loads(settings_row[0])
            except json.JSONDecodeError:
                settings = {}
        else:
            # Return default settings if none saved
            settings = {}
        
        # Merge with defaults
        default_settings = {
            'flagged_sender_score': 40,
            'leaver_score': 35,
            'suspicious_attachment_score': 30,
            'policy_violation_score': 30,
            'personal_domain_score': 15,
            'auto_clear_threshold': 20
        }
        
        # Use saved settings or defaults
        for key, default_value in default_settings.items():
            if key not in settings:
                settings[key] = default_value
        
        return jsonify(settings)
    except Exception as e:
        logging.error(f"Get processor settings error: {e}")
        return jsonify({'error': 'Failed to load processor settings'}), 500

@app.route('/api/dashboard-stats')
def api_dashboard_stats():
    """API endpoint for dashboard statistics"""
    try:
        stats = get_dashboard_stats()
        
        # Ensure all values are serializable and handle None values
        clean_stats = {
            'total_emails': int(stats.get('total_emails', 0)) if stats.get('total_emails') is not None else 0,
            'active_cases': int(stats.get('active_cases', 0)) if stats.get('active_cases') is not None else 0,
            'flagged_senders': int(stats.get('flagged_senders', 0)) if stats.get('flagged_senders') is not None else 0,
            'todays_emails': int(stats.get('todays_emails', 0)) if stats.get('todays_emails') is not None else 0,
            'excluded_whitelisted': int(stats.get('excluded_whitelisted', 0)) if stats.get('excluded_whitelisted') is not None else 0,
            'cleared': int(stats.get('cleared', 0)) if stats.get('cleared') is not None else 0,
            'department_data': list(stats.get('department_data', [])[:10]) if stats.get('department_data') else [],
            'timeline_data': list(stats.get('timeline_data', [])[:30]) if stats.get('timeline_data') else [],
            'success': True
        }
        
        return jsonify(clean_stats)
    except Exception as e:
        logging.error(f"Dashboard stats API error: {e}")
        # Return a valid JSON response even on error
        error_response = {
            'total_emails': 0,
            'active_cases': 0,
            'flagged_senders': 0,
            'todays_emails': 0,
            'excluded_whitelisted': 0,
            'cleared': 0,
            'department_data': [],
            'timeline_data': [],
            'success': False,
            'error': str(e)
        }
        return jsonify(error_response), 200  # Return 200 to prevent JSON parsing errors

@app.route('/api/classify-email', methods=['POST'])
def api_classify_email():
    """Classify email using ML model"""
    data = request.json or {}
    email_text = data.get('text', '')

    try:
        classification = classify_email(email_text)
        return jsonify({'classification': classification})
    except Exception as e:
        logging.error(f"Email classification error: {e}")
        return jsonify({'error': 'Classification failed'}), 500

@app.route('/api/generate-report', methods=['POST'])
def api_generate_report():
    """Generate report in PDF or Excel format"""
    try:
        data = request.json or {}
        report_type = data.get('type', 'pdf')
        date_from = data.get('date_from')
        date_to = data.get('date_to')

        # Validate input parameters
        if not report_type or report_type not in ['pdf', 'excel']:
            return jsonify({'error': 'Invalid or missing report type. Must be "pdf" or "excel"'}), 400

        # Validate date format if provided
        if date_from:
            try:
                datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                return jsonify({'error': 'Invalid date_from format. Use YYYY-MM-DD'}), 400

        if date_to:
            try:
                datetime.strptime(date_to, '%Y-%m-%d')
            except ValueError:
                return jsonify({'error': 'Invalid date_to format. Use YYYY-MM-DD'}), 400

        # Check if date range is reasonable (not more than 1 year)
        if date_from and date_to:
            from_date = datetime.strptime(date_from, '%Y-%m-%d')
            to_date = datetime.strptime(date_to, '%Y-%m-%d')
            if (to_date - from_date).days > 365:
                return jsonify({'error': 'Date range too large. Maximum 365 days allowed'}), 400
            if from_date > to_date:
                return jsonify({'error': 'Start date must be before end date'}), 400

        # Generate report
        if report_type == 'pdf':
            filename = generate_pdf_report(date_from, date_to)
        elif report_type == 'excel':
            filename = generate_excel_report(date_from, date_to)

        # Verify file was created and has content
        if not os.path.exists(filename):
            return jsonify({'error': 'Report file was not created'}), 500

        if os.path.getsize(filename) == 0:
            return jsonify({'error': 'Report file is empty'}), 500

        return send_file(filename, as_attachment=True)

    except FileNotFoundError as e:
        logging.error(f"Report file not found: {e}")
        return jsonify({'error': 'Report file not found after generation'}), 500
    except MemoryError as e:
        logging.error(f"Memory error during report generation: {e}")
        return jsonify({'error': 'Report too large. Try a smaller date range'}), 500
    except PermissionError as e:
        logging.error(f"Permission error during report generation: {e}")
        return jsonify({'error': 'File system permission error'}), 500
    except Exception as e:
        logging.error(f"Report generation error: {e}")
        return jsonify({'error': f'Report generation failed: {str(e)}'}), 500

@app.route('/api/generate-summary-report', methods=['POST'])
def api_generate_summary_report():
    """Generate focused summary reports"""
    data = request.json or {}
    summary_type = data.get('summary_type', 'escalated')
    date_from = data.get('date_from')
    date_to = data.get('date_to')

    try:
        filename = generate_summary_report(summary_type, date_from, date_to)
        return send_file(filename, as_attachment=True)
    except Exception as e:
        logging.error(f"Summary report generation error: {e}")
        return jsonify({'error': 'Summary report generation failed'}), 500

@app.route('/api/train-model', methods=['POST'])
def api_train_model():
    """Train the ML model"""
    try:
        accuracy = train_model()
        return jsonify({
            'success': True,
            'accuracy': accuracy,
            'message': f'Model trained successfully with {accuracy:.2%} accuracy'
        })
    except Exception as e:
        logging.error(f"Model training error: {e}")
        return jsonify({'error': 'Failed to train model'}), 500

@app.route('/api/train-advanced-models', methods=['POST'])
def api_train_advanced_models():
    """Train advanced ML models"""
    try:
        results = train_advanced_models()
        if 'error' in results:
            return jsonify({'error': results['error']}), 400

        return jsonify({
            'success': True,
            'results': results,
            'message': f'Advanced models trained successfully with {results.get("ensemble_accuracy", 0):.2%} accuracy'
        })
    except Exception as e:
        logging.error(f"Advanced model training error: {e}")
        return jsonify({'error': 'Failed to train advanced models'}), 500

@app.route('/api/ingest-csv', methods=['POST'])
def api_ingest_csv():
    """Ingest CSV files from data directory"""
    try:
        ingestor = CSVIngestor(data_dir='data')
        results = ingestor.ingest_csv_files()

        return jsonify({
            'success': True,
            'message': f'Successfully processed {results["files_processed"]} files',
            'files_processed': results['files_processed'],
            'total_records': results['total_records'],
            'successful_inserts': results['successful_inserts'],
            'errors': results['errors']
        })
    except Exception as e:
        logging.error(f"CSV ingestion error: {e}")
        return jsonify({'error': f'CSV ingestion failed: {str(e)}'}), 500

@app.route('/api/process-emails', methods=['POST'])
def api_process_emails():
    """Process emails through analysis pipeline"""
    try:
        data = request.json or {}
        limit = data.get('limit', 100)
        offset = data.get('offset', 0)

        processor = EmailProcessor()
        results = processor.process_batch(limit=limit, offset=offset)

        return jsonify({
            'success': True,
            'message': f'Processed {results["processed"]} emails',
            'results': results
        })
    except Exception as e:
        logging.error(f"Email processing error: {e}")
        return jsonify({'error': f'Email processing failed: {str(e)}'}), 500

@app.route('/api/process-single-email/<int:email_id>', methods=['POST'])
def api_process_single_email(email_id):
    """Process a single email by ID"""
    try:
        conn = get_db_connection()
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        conn.close()

        if not email_data:
            return jsonify({'error': 'Email not found'}), 404

        # Convert to dict
        email_dict = {
            'id': email_data[0], '_time': email_data[1], 'sender': email_data[2],
            'subject': email_data[3], 'attachments': email_data[4], 'recipients': email_data[5],
            'time_month': email_data[6], 'leaver': email_data[7], 'termination_date': email_data[8],
            'bunit': email_data[9], 'department': email_data[10], 'user_response': email_data[11],
            'final_outcome': email_data[12], 'policy_name': email_data[13], 'justifications': email_data[14]
        }

        processor = EmailProcessor()
        result = processor.process_email(email_dict)

        # Update database
        processor.update_email_status(email_id, result)

        # Create case if needed
        case_id = None
        if result.final_status.value in ['escalated', 'pending_review']:
            case_id = processor.create_case_if_needed(email_id, result)

        return jsonify({
            'success': True,
            'email_id': email_id,
            'final_status': result.final_status.value,
            'risk_level': result.risk_level.value,
            'ml_classification': result.ml_classification,
            'actions_count': len(result.actions_taken),
            'case_id': case_id,
            'actions': [
                {
                    'type': action.action_type,
                    'reason': action.reason,
                    'confidence': action.confidence
                } for action in result.actions_taken
            ]
        })
    except Exception as e:
        logging.error(f"Single email processing error: {e}")
        return jsonify({'error': f'Email processing failed: {str(e)}'}), 500

@app.route('/api/move-to-main/<int:email_id>', methods=['POST'])
def api_move_to_main(email_id):
    """Move email from excluded/whitelisted back to main dashboard"""
    try:
        conn = get_db_connection()
        conn.execute("""
            UPDATE emails 
            SET final_outcome = NULL, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, [email_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Email moved to main dashboard'})
    except Exception as e:
        logging.error(f"Move email error: {e}")
        return jsonify({'error': 'Failed to move email'}), 500

@app.route('/api/update-email-status/<int:email_id>', methods=['POST'])
def api_update_email_status(email_id):
    """Update email status"""
    data = request.json or {}
    status = data.get('status')

    try:
        conn = get_db_connection()
        conn.execute("""
            UPDATE emails 
            SET final_outcome = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, [status, email_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Email status updated'})
    except Exception as e:
        logging.error(f"Update email status error: {e}")
        return jsonify({'error': 'Failed to update email status'}), 500

@app.route('/api/move-to-case-management/<int:email_id>', methods=['POST'])
def api_move_to_case_management(email_id):
    """Move email to case management system"""
    try:
        conn = get_db_connection()
        
        # Check if email exists and get details
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        if not email_data:
            conn.close()
            return jsonify({'error': 'Email not found'}), 404
        
        # Check if case already exists for this email
        existing_case = conn.execute("SELECT id FROM cases WHERE email_id = ?", [email_id]).fetchone()
        
        if not existing_case:
            # Create a case for this email
            escalation_reason = f"Moved to case management from main dashboard. Sender: {email_data[2]}, Subject: {email_data[3][:100]}"
            
            conn.execute("""
                INSERT INTO cases (email_id, escalation_reason, status) 
                VALUES (?, ?, ?)
            """, [email_id, escalation_reason, 'open'])
        
        # Update email status to 'escalated' so it doesn't show in main dashboard
        conn.execute("""
            UPDATE emails 
            SET final_outcome = 'escalated' 
            WHERE id = ?
        """, [email_id])
        
        conn.close()

        return jsonify({
            'success': True, 
            'message': 'Email moved to case management successfully'
        })
        
    except Exception as e:
        logging.error(f"Move to case management error: {e}")
        return jsonify({'error': 'Failed to move email to case management'}), 500

@app.route('/api/generate-followup/<int:email_id>', methods=['POST'])
def api_generate_followup(email_id):
    """Generate follow-up email for Outlook"""
    try:
        data = request.json or {}
        followup_type = data.get('type', 'escalation')
        to_addresses = data.get('to_addresses', 'security@company.com')
        cc_addresses = data.get('cc_addresses', '')

        # Generate follow-up using Outlook integration
        result = generate_followup_email(email_id, followup_type, to_addresses)

        if result.get('success'):
            return jsonify({
                'success': True,
                'outlook_link': result['mailto_link'],
                'subject': result['subject'],
                'body': result['body'],
                'followup_type': followup_type,
                'to_addresses': to_addresses,
                'cc_addresses': cc_addresses,
                'email_context': result['email_context']
            })
        else:
            return jsonify({'error': result.get('error', 'Failed to generate follow-up')}), 500

    except Exception as e:
        logging.error(f"Generate follow-up error: {e}")
        return jsonify({'error': 'Failed to generate follow-up email'}), 500

@app.route('/api/email-details/<int:email_id>')
def api_email_details(email_id):
    """Get detailed email information"""
    try:
        conn = get_db_connection()
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        conn.close()

        if not email_data:
            return jsonify({'error': 'Email not found'}), 404

        # Convert to dict for JSON response
        email_dict = {
            'id': email_data[0],
            '_time': email_data[1].isoformat() if email_data[1] else None,
            'sender': email_data[2],
            'subject': email_data[3],
            'attachments': email_data[4],
            'recipients': email_data[5],
            'time_month': email_data[6],
            'leaver': email_data[7],
            'termination_date': email_data[8].isoformat() if email_data[8] else None,
            'bunit': email_data[9],
            'department': email_data[10],
            'user_response': email_data[11],
            'final_outcome': email_data[12],
            'policy_name': email_data[13],
            'justifications': email_data[14]
        }

        return jsonify(email_dict)
    except Exception as e:
        logging.error(f"Email details error: {e}")
        return jsonify({'error': 'Failed to load email details'}), 500

@app.route('/api/ml-insights/<int:email_id>')
def api_ml_insights(email_id):
    """Get ML analysis insights for an email"""
    try:
        conn = get_db_connection()
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        conn.close()

        if not email_data:
            return jsonify({'error': 'Email not found'}), 404

        # Convert to dict for easier access
        email_dict = {
            'id': email_data[0],
            'sender': email_data[2],
            'subject': email_data[3],
            'attachments': email_data[4],
            'recipients': email_data[5],
            'leaver': email_data[7],
            'department': email_data[10],
            'final_outcome': email_data[12],
            'policy_name': email_data[13],
            'justifications': email_data[14]
        }

        # Get ML classification from processor
        from ml_processor import classify_email, get_risk_score
        text = f"{email_dict['subject'] or ''} {email_dict['justifications'] or ''}"
        classification = classify_email(text)
        risk_score = get_risk_score(email_dict)

        # Analyze actual risk factors based on email content
        risk_factors = []
        
        # Check recipient patterns (all emails are external)
        recipients = email_dict.get('recipients', '')
        if recipients and '@' in recipients:
            recipient_count = len(recipients.split(',')) if recipients else 0
            if recipient_count > 10:
                risk_factors.append({'name': 'Mass Distribution (10+ Recipients)', 'severity': 'high'})
            elif recipient_count > 5:
                risk_factors.append({'name': 'Multiple Recipients (5-10)', 'severity': 'medium'})
            
            # Check for personal email domains in recipients
            personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
            if any(domain in recipients.lower() for domain in personal_domains):
                risk_factors.append({'name': 'Personal Email Domain Recipients', 'severity': 'medium'})
        
        # Check for attachments
        attachments = email_dict.get('attachments', '')
        if attachments and attachments != '-' and attachments.strip():
            # Check for risky file types
            risky_extensions = ['.exe', '.zip', '.rar', '.bat', '.scr', '.doc', '.pdf']
            if any(ext in attachments.lower() for ext in risky_extensions):
                risk_factors.append({'name': 'Risky Attachment Type', 'severity': 'high'})
            else:
                risk_factors.append({'name': 'Attachment Present', 'severity': 'medium'})
        
        # Check if sender is a leaver
        if email_dict.get('leaver') == 'Yes' or email_dict.get('leaver') == True:
            risk_factors.append({'name': 'Sender is Former Employee', 'severity': 'high'})
        
        # Check for policy violations
        if email_dict.get('policy_name'):
            risk_factors.append({'name': 'Policy Violation Detected', 'severity': 'high'})
        
        # Check for flagged sender
        conn = get_db_connection()
        flagged_check = conn.execute("SELECT COUNT(*) FROM flagged_senders WHERE sender = ?", [email_dict['sender']]).fetchone()[0]
        conn.close()
        
        if flagged_check > 0:
            risk_factors.append({'name': 'Flagged Sender', 'severity': 'high'})
        
        # Check department risk
        high_risk_depts = ['finance', 'legal', 'hr', 'executive']
        if email_dict.get('department', '').lower() in high_risk_depts:
            risk_factors.append({'name': 'High-Risk Department', 'severity': 'medium'})
        
        # Add default if no specific factors found
        if not risk_factors:
            risk_factors.append({'name': 'Standard Email Pattern', 'severity': 'low'})

        # Calculate confidence based on actual data
        confidence = min(0.95, max(0.3, (risk_score / 100) + 0.3))
        
        # Generate recommendations based on actual analysis
        recommendations = []
        if risk_score >= 70:
            recommendations.append("Immediate escalation recommended due to high risk score")
        elif risk_score >= 40:
            recommendations.append("Enhanced monitoring recommended")
        elif classification in ['high_risk', 'suspicious']:
            recommendations.append("Manual review recommended based on content analysis")
        else:
            recommendations.append("Standard processing - no immediate concerns identified")
        
        if email_dict.get('leaver'):
            recommendations.append("Verify sender access permissions")
        
        insights = {
            'classification': classification,
            'confidence': round(confidence, 2),
            'risk_score': risk_score,
            'explanation': f"Email analyzed with {len(risk_factors)} risk factors identified. Risk score: {risk_score}/100",
            'risk_factors': risk_factors,
            'recommendations': ' • '.join(recommendations)
        }

        return jsonify(insights)
    except Exception as e:
        logging.error(f"ML insights error: {e}")
        return jsonify({'error': 'Failed to load ML insights'}), 500

@app.route('/api/analytics-data')
def api_analytics_data():
    """Get analytics data for charts"""
    try:
        stats = get_dashboard_stats()

        # Prepare data for Chart.js
        analytics_data = {
            'department_chart': {
                'labels': [dept[0] for dept in stats['department_data']],
                'data': [dept[1] for dept in stats['department_data']]
            },
            'timeline_chart': {
                'labels': [str(item[0]) for item in stats['timeline_data']],
                'data': [item[1] for item in stats['timeline_data']]
            },
            'summary_stats': {
                'total_emails': stats['total_emails'],
                'active_cases': stats['active_cases'],
                'flagged_senders': stats['flagged_senders'],
                'todays_emails': stats['todays_emails']
            }
        }

        return jsonify(analytics_data)
    except Exception as e:
        logging.error(f"Analytics data error: {e}")
        return jsonify({'error': 'Failed to load analytics data'}), 500

@app.route('/api/advanced-analytics')
def api_advanced_analytics():
    """Get advanced analytics report"""
    try:
        report = get_analytics_report()
        return jsonify(report)
    except Exception as e:
        logging.error(f"Advanced analytics error: {e}")
        return jsonify({'error': 'Failed to generate analytics report'}), 500

@app.route('/api/send-followup/<int:email_id>', methods=['POST'])
def api_send_followup(email_id):
    """Send follow-up email using Outlook integration"""
    try:
        data = request.json or {}
        method = data.get('method', 'mailto')  # 'mailto' or 'outlook_windows'

        # First generate the follow-up
        followup_result = generate_followup_email(
            email_id, 
            data.get('type', 'escalation'),
            data.get('to_addresses', 'security@company.com')
        )

        if not followup_result.get('success'):
            return jsonify({'error': followup_result.get('error')}), 500

        # Update with any custom content
        if data.get('subject'):
            followup_result['subject'] = data['subject']
        if data.get('body'):
            followup_result['body'] = data['body']
        if data.get('cc_addresses'):
            followup_result['cc_addresses'] = data['cc_addresses']

        # Send the follow-up
        send_result = send_followup_email(email_id, followup_result, method)

        return jsonify(send_result)

    except Exception as e:
        logging.error(f"Send follow-up error: {e}")
        return jsonify({'error': 'Failed to send follow-up email'}), 500

@app.route('/api/bulk-generate-followups', methods=['POST'])
def api_bulk_generate_followups():
    """Generate follow-up emails for multiple cases"""
    try:
        data = request.json or {}
        email_ids = data.get('email_ids', [])
        followup_type = data.get('type', 'escalation')
        to_addresses = data.get('to_addresses', 'security@company.com')

        if not email_ids:
            return jsonify({'error': 'No email IDs provided'}), 400

        result = bulk_generate_followups(email_ids, followup_type, to_addresses)

        return jsonify(result)

    except Exception as e:
        logging.error(f"Bulk generate followups error: {e}")
        return jsonify({'error': 'Failed to generate bulk follow-ups'}), 500

@app.route('/api/followup-history/<int:email_id>')
def api_followup_history(email_id):
    """Get follow-up history for specific email"""
    try:
        history = get_followup_history(email_id)
        return jsonify(history)
    except Exception as e:
        logging.error(f"Followup history error: {e}")
        return jsonify({'error': 'Failed to load follow-up history'}), 500

@app.route('/api/all-followup-history')
def api_all_followup_history():
    """Get all follow-up history"""
    try:
        history = get_followup_history()
        return jsonify(history)
    except Exception as e:
        logging.error(f"All followup history error: {e}")
        return jsonify({'error': 'Failed to load follow-up history'}), 500

@app.route('/api/admin/policies')
def api_admin_get_policies():
    """Get all policies"""
    try:
        conn = get_db_connection()
        policies = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE rule_type = 'policy'
            ORDER BY created_at DESC
        """).fetchall()
        conn.close()

        policies_list = []
        for policy in policies:
            try:
                conditions_data = json.loads(policy[2]) if policy[2] else {}
                policies_list.append({
                    'id': policy[0],
                    'policy_name': conditions_data.get('policy_name', f'Policy {policy[0]}'),
                    'description': conditions_data.get('description', ''),
                    'severity': conditions_data.get('severity', 'medium'),
                    'action': policy[3] or 'flag',
                    'keywords': conditions_data.get('keywords', ''),
                    'rules': conditions_data.get('rules', ''),
                    'is_active': bool(policy[4]),
                    'created_at': policy[5].strftime('%Y-%m-%d') if policy[5] else 'N/A'
                })
            except (json.JSONDecodeError, AttributeError) as e:
                logging.warning(f"Failed to parse policy {policy[0]}: {e}")
                # Add basic policy data even if JSON parsing fails
                policies_list.append({
                    'id': policy[0],
                    'policy_name': f'Policy {policy[0]}',
                    'description': 'Legacy policy - needs update',
                    'severity': 'medium',
                    'action': policy[3] or 'flag',
                    'keywords': '',
                    'rules': policy[2] or '',
                    'is_active': bool(policy[4]),
                    'created_at': policy[5].strftime('%Y-%m-%d') if policy[5] else 'N/A'
                })

        return jsonify(policies_list)
    except Exception as e:
        logging.error(f"Get policies error: {e}")
        return jsonify([])  # Return empty array instead of error to prevent UI issues

@app.route('/api/admin/policy/<int:policy_id>')
def api_admin_get_policy(policy_id):
    """Get single policy by ID"""
    try:
        conn = get_db_connection()
        policy = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE id = ? AND rule_type = 'policy'
        """, [policy_id]).fetchone()
        conn.close()

        if not policy:
            return jsonify({'error': 'Policy not found'}), 404

        conditions_data = json.loads(policy[2]) if policy[2] else {}
        policy_data = {
            'id': policy[0],
            'policy_name': conditions_data.get('policy_name', f'Policy {policy[0]}'),
            'description': conditions_data.get('description', ''),
            'severity': conditions_data.get('severity', 'medium'),
            'action': policy[3],
            'keywords': conditions_data.get('keywords', ''),
            'rules': conditions_data.get('rules', ''),
            'is_active': policy[4],
            'created_at': policy[5].strftime('%Y-%m-%d') if policy[5] else 'N/A'
        }

        return jsonify(policy_data)
    except Exception as e:
        logging.error(f"Get policy error: {e}")
        return jsonify({'error': 'Failed to load policy'}), 500

@app.route('/api/admin/policy-by-name/<policy_name>')
def api_admin_get_policy_by_name(policy_name):
    """Get single policy by name"""
    try:
        conn = get_db_connection()
        
        # Try to find policy in admin_rules first
        policy = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE rule_type IN ('policy', 'policy_control') 
            AND (JSON_EXTRACT(conditions, '$.policy_name') = ? OR conditions = ?)
        """, [policy_name, policy_name]).fetchone()
        
        conn.close()

        if not policy:
            return jsonify({'error': 'Policy not found as admin rule'}), 404

        try:
            conditions_data = json.loads(policy[2]) if policy[2] else {}
        except (json.JSONDecodeError, TypeError):
            conditions_data = {'policy_name': policy_name}

        policy_data = {
            'id': policy[0],
            'rule_type': policy[1],
            'policy_name': conditions_data.get('policy_name', policy_name),
            'description': conditions_data.get('description', f'Policy rule for {policy_name}'),
            'severity': conditions_data.get('severity', 'medium'),
            'action': policy[3],
            'keywords': conditions_data.get('keywords', ''),
            'rules': conditions_data.get('rules', ''),
            'is_active': policy[4],
            'created_at': policy[5].strftime('%Y-%m-%d') if policy[5] else 'N/A'
        }

        return jsonify(policy_data)
    except Exception as e:
        logging.error(f"Get policy by name error: {e}")
        return jsonify({'error': 'Failed to load policy'}), 500

@app.route('/api/admin/save-policy', methods=['POST'])
def api_admin_save_policy():
    """Save or update policy"""
    data = request.json or {}

    try:
        # Validate required fields
        if not data.get('policy_name'):
            return jsonify({'error': 'Policy name is required'}), 400

        if not data.get('description'):
            return jsonify({'error': 'Policy description is required'}), 400

        # Validate JSON if rules provided
        if data.get('rules') and data['rules'].strip():
            try:
                json.loads(data['rules'])
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid JSON format in rules field'}), 400

        # Prepare conditions JSON
        conditions_data = {
            'policy_name': data['policy_name'].strip(),
            'description': data.get('description', '').strip(),
            'severity': data.get('severity', 'medium'),
            'keywords': data.get('keywords', '').strip(),
            'rules': data.get('rules', '').strip()
        }

        conn = get_db_connection()
        
        if data.get('policy_id'):
            # Update existing policy
            result = conn.execute("""
                UPDATE admin_rules 
                SET conditions = ?, action = ?, is_active = ?
                WHERE id = ? AND rule_type = 'policy'
            """, [
                json.dumps(conditions_data),
                data.get('action', 'flag'),
                bool(data.get('is_active', True)),
                int(data['policy_id'])
            ])
            
            if result.rowcount == 0:
                conn.close()
                return jsonify({'error': 'Policy not found'}), 404
        else:
            # Check for duplicate policy names
            existing = conn.execute("""
                SELECT id FROM admin_rules 
                WHERE rule_type = 'policy' AND JSON_EXTRACT(conditions, '$.policy_name') = ?
            """, [conditions_data['policy_name']]).fetchone()
            
            if existing:
                conn.close()
                return jsonify({'error': 'A policy with this name already exists'}), 400
            
            # Create new policy
            conn.execute("""
                INSERT INTO admin_rules (rule_type, conditions, action, is_active, created_at) 
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                'policy',
                json.dumps(conditions_data),
                data.get('action', 'flag'),
                bool(data.get('is_active', True))
            ])
        
        conn.close()

        return jsonify({'success': True, 'message': 'Policy saved successfully'})
    except Exception as e:
        logging.error(f"Save policy error: {e}")
        return jsonify({'error': f'Failed to save policy: {str(e)}'}), 500

@app.route('/api/admin/delete-policy/<int:policy_id>', methods=['DELETE'])
def api_admin_delete_policy(policy_id):
    """Delete policy"""
    try:
        conn = get_db_connection()
        conn.execute("DELETE FROM admin_rules WHERE id = ? AND rule_type = 'policy'", [policy_id])
        conn.close()

        return jsonify({'success': True, 'message': 'Policy deleted successfully'})
    except Exception as e:
        logging.error(f"Delete policy error: {e}")
        return jsonify({'error': 'Failed to delete policy'}), 500

@app.route('/api/admin/policies')
def api_admin_get_all_policies():
    """Get all policies for the View All Policies button"""
    try:
        conn = get_db_connection()
        
        # Get all admin rules that are policy-related
        policies = conn.execute("""
            SELECT id, rule_type, conditions, action, is_active, created_at
            FROM admin_rules 
            WHERE rule_type IN ('policy', 'security_rule', 'content_filter', 'sender_filter', 'attachment_filter')
            ORDER BY created_at DESC
        """).fetchall()
        
        policies_list = []
        for policy in policies:
            try:
                # Try to parse conditions as JSON
                try:
                    conditions_data = json.loads(policy[2]) if policy[2] else {}
                except (json.JSONDecodeError, TypeError):
                    # If not JSON, treat as simple text condition
                    conditions_data = {'description': policy[2] or 'Legacy rule'}
                
                policy_info = {
                    'id': policy[0],
                    'rule_type': policy[1],
                    'policy_name': conditions_data.get('policy_name', conditions_data.get('rule_name', f'Policy {policy[0]}')),
                    'description': conditions_data.get('description', policy[2][:100] + '...' if policy[2] and len(policy[2]) > 100 else policy[2] or 'No description'),
                    'action': policy[3] or 'flag',
                    'is_active': bool(policy[4]),
                    'created_at': policy[5].strftime('%Y-%m-%d %H:%M') if hasattr(policy[5], 'strftime') else str(policy[5]) if policy[5] else 'N/A',
                    'conditions_summary': conditions_data.get('conditions', policy[2])
                }
                policies_list.append(policy_info)
                
            except Exception as e:
                logging.warning(f"Error processing policy {policy[0]}: {e}")
                # Add basic policy info even if processing fails
                policies_list.append({
                    'id': policy[0],
                    'rule_type': policy[1],
                    'policy_name': f'Policy {policy[0]}',
                    'description': 'Error loading policy details',
                    'action': policy[3] or 'flag',
                    'is_active': bool(policy[4]),
                    'created_at': str(policy[5]) if policy[5] else 'N/A',
                    'conditions_summary': 'Error loading conditions'
                })
        
        conn.close()
        return jsonify({
            'success': True,
            'policies': policies_list,
            'total_count': len(policies_list)
        })
        
    except Exception as e:
        logging.error(f"Get all policies error: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to load policies: {str(e)}',
            'policies': []
        }), 500

@app.route('/api/admin/policy-violations-data')
def api_admin_get_policy_violations_data():
    """Get current policy violations data from the database"""
    try:
        conn = get_db_connection()
        
        # Get policy violation counts with error handling
        try:
            violations = conn.execute("""
                SELECT 
                    policy_name, 
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM emails WHERE policy_name IS NOT NULL), 1) as percentage
                FROM emails 
                WHERE policy_name IS NOT NULL AND policy_name != ''
                GROUP BY policy_name
                ORDER BY count DESC
            """).fetchall()
        except Exception as e:
            logging.warning(f"Failed to get policy violations: {e}")
            violations = []
        
        # Get policy active status with safe JSON handling for different rule types
        try:
            policy_status = conn.execute("""
                SELECT 
                    CASE 
                        WHEN rule_type = 'policy' AND conditions LIKE '%"policy_name"%' THEN JSON_EXTRACT(conditions, '$.policy_name')
                        WHEN rule_type = 'policy_control' AND conditions LIKE '%"policy_name"%' THEN JSON_EXTRACT(conditions, '$.policy_name')
                        ELSE conditions
                    END as policy_name, 
                    CASE 
                        WHEN rule_type = 'policy_disable' THEN 0
                        WHEN rule_type = 'policy_enable' THEN 1
                        ELSE is_active
                    END as is_active,
                    rule_type,
                    created_at
                FROM admin_rules 
                WHERE rule_type IN ('policy', 'policy_control', 'policy_disable', 'policy_enable')
                AND (
                    (conditions LIKE '%"policy_name"%') OR 
                    (rule_type IN ('policy_disable', 'policy_enable'))
                )
                ORDER BY created_at DESC
            """).fetchall()
        except Exception as e:
            logging.warning(f"Failed to get policy status: {e}")
            policy_status = []
        
        conn.close()

        # Create status mapping - prioritize disable/enable rules over others
        status_map = {}
        for status in policy_status:
            policy_name = status[0]
            is_active = status[1]
            rule_type = status[2]
            
            if policy_name:
                # Prioritize disable/enable rules (most recent action)
                if rule_type in ['policy_disable', 'policy_enable']:
                    status_map[policy_name] = {
                        'is_active': is_active,
                        'rule_type': rule_type
                    }
                # Only use other rules if no disable/enable rule exists
                elif policy_name not in status_map or status_map[policy_name]['rule_type'] not in ['policy_disable', 'policy_enable']:
                    status_map[policy_name] = {
                        'is_active': is_active,
                        'rule_type': rule_type
                    }

        violations_list = []
        for violation in violations:
            try:
                policy_name = violation[0]
                policy_info = status_map.get(policy_name, {'is_active': True, 'rule_type': 'none'})
                is_active = policy_info['is_active']
                
                violations_list.append({
                    'policy_name': policy_name,
                    'count': violation[1],
                    'percentage': violation[2] if violation[2] is not None else 0,
                    'is_active': is_active,
                    'has_admin_rule': policy_info['rule_type'] != 'none'
                })
            except Exception as e:
                logging.warning(f"Failed to process violation {violation}: {e}")
                continue

        return jsonify(violations_list)
    except Exception as e:
        logging.error(f"Get policy violations data error: {e}")
        return jsonify([])  # Return empty array instead of error to prevent UI issues

@app.route('/api/admin/toggle-policy-violation', methods=['POST'])
def api_admin_toggle_policy_violation():
    """Toggle policy violation detection on/off"""
    data = request.json or {}
    policy_name = data.get('policy_name')
    is_active = data.get('is_active', True)

    try:
        conn = get_db_connection()
        
        # First, try to find any existing rule for this policy
        # Handle both JSON and plain text conditions safely
        existing_rules = conn.execute("""
            SELECT id, rule_type, conditions, is_active FROM admin_rules 
            WHERE (
                (rule_type = 'policy' AND (
                    (conditions LIKE '%"policy_name"%' AND JSON_EXTRACT(conditions, '$.policy_name') = ?) OR
                    conditions = ?
                )) OR
                (rule_type = 'policy_control' AND (
                    (conditions LIKE '%"policy_name"%' AND JSON_EXTRACT(conditions, '$.policy_name') = ?) OR
                    conditions = ?
                )) OR
                (rule_type IN ('policy_disable', 'policy_enable') AND conditions = ?)
            )
        """, [policy_name, policy_name, policy_name, policy_name, policy_name]).fetchall()
        
        updated_existing = False
        
        # Update any existing rules
        for rule in existing_rules:
            conn.execute("""
                UPDATE admin_rules 
                SET is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, [is_active, rule[0]])
            updated_existing = True
            logging.info(f"Updated existing rule {rule[0]} ({rule[1]}) for '{policy_name}' to {'active' if is_active else 'inactive'}")
        
        # If no existing rules found, create a new policy control rule
        if not updated_existing:
            rule_type = 'policy_control'
            conditions_data = {
                'policy_name': policy_name,
                'description': f'Auto-created control for policy: {policy_name}',
                'control_type': 'enable_disable',
                'created_by': 'admin_panel'
            }
            
            conn.execute("""
                INSERT INTO admin_rules (rule_type, conditions, action, is_active, created_at) 
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [rule_type, json.dumps(conditions_data), 'control', is_active])
            logging.info(f"Created new policy control rule for '{policy_name}' with status {'active' if is_active else 'inactive'}")
        
        # Also create/update a specific disable rule for immediate effect
        disable_rule_type = 'policy_disable' if not is_active else 'policy_enable'
        
        # Remove any conflicting disable/enable rules
        conn.execute("""
            DELETE FROM admin_rules 
            WHERE rule_type IN ('policy_disable', 'policy_enable') AND conditions = ?
        """, [policy_name])
        
        # Create new rule for current state
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action, is_active, created_at) 
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [disable_rule_type, policy_name, 'control', True])
        
        conn.close()

        status_text = "enabled" if is_active else "disabled"
        return jsonify({
            'success': True, 
            'message': f'Policy "{policy_name}" {status_text} successfully',
            'policy_name': policy_name,
            'new_status': is_active
        })
    except Exception as e:
        logging.error(f"Toggle policy violation error: {e}")
        return jsonify({'error': f'Failed to toggle policy: {str(e)}'}), 500

@app.route('/api/user-activity')
def api_user_activity():
    """Get user activity log"""
    try:
        from user_actions import action_tracker

        user_id = request.args.get('user_id')
        limit = request.args.get('limit', 100, type=int)

        activity = action_tracker.get_user_activity(user_id, limit)

        # Format activity for JSON response
        formatted_activity = []
        for record in activity:
            formatted_activity.append({
                'id': record[0],
                'user_id': record[1],
                'action_type': record[2],
                'email_id': record[3],
                'case_id': record[4],
                'details': json.loads(record[5]) if record[5] else {},
                'timestamp': record[6].isoformat() if record[6] else None,
                'sender': record[7],
                'subject': record[8],
                'escalation_reason': record[9]
            })

        return jsonify(formatted_activity)
    except Exception as e:
        logging.error(f"User activity error: {e}")
        return jsonify({'error': 'Failed to load user activity'}), 500

@app.route('/api/action-statistics')
def api_action_statistics():
    """Get action statistics"""
    try:
        from user_actions import action_tracker

        days = request.args.get('days', 30, type=int)
        stats = action_tracker.get_action_stats(days)

        # Format stats for JSON response
        formatted_stats = []
        for record in stats:
            formatted_stats.append({
                'action_type': record[0],
                'count': record[1],
                'unique_users': record[2]
            })

        return jsonify({
            'period_days': days,
            'statistics': formatted_stats
        })
    except Exception as e:
        logging.error(f"Action statistics error: {e}")
        return jsonify({'error': 'Failed to load action statistics'}), 500

@app.route('/api/bulk-update-status', methods=['POST'])
def api_bulk_update_status():
    """Bulk update email status"""
    try:
        from user_actions import action_tracker

        data = request.json or {}
        email_ids = data.get('email_ids', [])
        new_status = data.get('status')
        user_id = data.get('user_id', 'anonymous')

        if not email_ids or not new_status:
            return jsonify({'error': 'Email IDs and status are required'}), 400

        conn = get_db_connection()
        updated_count = 0

        for email_id in email_ids:
            try:
                conn.execute("""
                    UPDATE emails 
                    SET final_outcome = ?
                    WHERE id = ?
                """, [new_status, email_id])

                # Track individual action
                action_tracker.track_action('bulk_update_status', email_id=email_id, 
                                          details={'new_status': new_status}, 
                                          user_id=user_id)
                updated_count += 1
            except Exception as e:
                logging.error(f"Failed to update email {email_id}: {e}")

        conn.close()

        return jsonify({
            'success': True,
            'message': f'Updated {updated_count} of {len(email_ids)} emails',
            'updated_count': updated_count
        })
    except Exception as e:
        logging.error(f"Bulk update error: {e}")
        return jsonify({'error': 'Failed to bulk update emails'}), 500


@app.route('/api/admin/clear-emails', methods=['POST'])
def api_admin_clear_emails():
    """Clear all email records from database"""
    try:
        logging.info("Starting email clearing process...")
        conn = get_db_connection()
        
        # Get count before deletion
        try:
            count_result = conn.execute("SELECT COUNT(*) FROM emails").fetchone()
            deleted_count = count_result[0] if count_result else 0
            logging.info(f"Found {deleted_count} emails to delete")
        except Exception as e:
            logging.warning(f"Could not count emails: {e}")
            deleted_count = 0
        
        # Get cases count
        try:
            cases_result = conn.execute("SELECT COUNT(*) FROM cases").fetchone()
            cases_count = cases_result[0] if cases_result else 0
            logging.info(f"Found {cases_count} cases to delete")
        except Exception as e:
            logging.warning(f"Could not count cases: {e}")
            cases_count = 0
        
        # Get flagged senders count
        try:
            flagged_result = conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()
            flagged_count = flagged_result[0] if flagged_result else 0
            logging.info(f"Found {flagged_count} flagged senders to delete")
        except Exception as e:
            logging.warning(f"Could not count flagged senders: {e}")
            flagged_count = 0
        
        # Disable foreign key constraints temporarily for cleanup
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            logging.info("Temporarily disabled foreign key constraints")
        except Exception as e:
            logging.warning(f"Could not disable foreign keys: {e}")
        
        # Clear all tables in order to avoid constraint issues
        try:
            conn.execute("DELETE FROM cases")
            logging.info("Successfully cleared cases table")
        except Exception as e:
            logging.warning(f"Failed to clear cases: {e}")
        
        try:
            conn.execute("DELETE FROM flagged_senders")
            logging.info("Successfully cleared flagged senders table")
        except Exception as e:
            logging.warning(f"Failed to clear flagged senders: {e}")
        
        try:
            conn.execute("DELETE FROM emails")
            logging.info("Successfully cleared emails table")
        except Exception as e:
            logging.error(f"Failed to clear emails: {e}")
            # Try alternative approach - truncate instead of delete
            try:
                conn.execute("TRUNCATE emails")
                logging.info("Successfully truncated emails table")
            except Exception as e2:
                logging.error(f"Failed to truncate emails: {e2}")
                conn.close()
                return jsonify({'error': f'Failed to clear emails: {str(e2)}'}), 500
        
        # Re-enable foreign key constraints
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            logging.info("Re-enabled foreign key constraints")
        except Exception as e:
            logging.warning(f"Could not re-enable foreign keys: {e}")
        
        conn.close()
        logging.info("Email clearing process completed successfully")

        return jsonify({
            'success': True,
            'message': f'Cleared {deleted_count} email records, {cases_count} cases, and {flagged_count} flagged senders',
            'deleted_count': deleted_count,
            'cases_cleared': cases_count,
            'flagged_cleared': flagged_count
        })
    except Exception as e:
        logging.error(f"Clear emails error: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to clear email data: {str(e)}'
        }), 500

@app.route('/api/admin/clear-cases', methods=['POST'])
def api_admin_clear_cases():
    """Clear all case records from database"""
    try:
        conn = get_db_connection()
        
        # Get count before deletion
        count_result = conn.execute("SELECT COUNT(*) FROM cases").fetchone()
        deleted_count = count_result[0] if count_result else 0
        
        # Clear cases table
        conn.execute("DELETE FROM cases")
        
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Cleared {deleted_count} case records',
            'deleted_count': deleted_count
        })
    except Exception as e:
        logging.error(f"Clear cases error: {e}")
        return jsonify({'error': 'Failed to clear case data'}), 500

@app.route('/api/admin/clear-flagged-senders', methods=['POST'])
def api_admin_clear_flagged_senders():
    """Clear all flagged sender records from database"""
    try:
        conn = get_db_connection()
        
        # Get count before deletion
        count_result = conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()
        deleted_count = count_result[0] if count_result else 0
        
        # Clear flagged_senders table
        conn.execute("DELETE FROM flagged_senders")
        
        conn.close()

        return jsonify({
            'success': True,
            'message': f'Cleared {deleted_count} flagged sender records',
            'deleted_count': deleted_count
        })
    except Exception as e:
        logging.error(f"Clear flagged senders error: {e}")
        return jsonify({'error': 'Failed to clear flagged sender data'}), 500

@app.route('/api/admin/clear-all-data', methods=['POST'])
def api_admin_clear_all_data():
    """Clear all imported data from database"""
    try:
        conn = get_db_connection()
        
        # Get counts before deletion (with error handling)
        try:
            email_count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        except:
            email_count = 0
            
        try:
            case_count = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        except:
            case_count = 0
            
        try:
            flagged_count = conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()[0]
        except:
            flagged_count = 0
        
        # Clear all tables with individual error handling
        deleted_emails = 0
        deleted_cases = 0
        deleted_flagged = 0
        
        try:
            # Clear cases first (foreign key dependency)
            conn.execute("DELETE FROM cases")
            deleted_cases = case_count
            logging.info(f"Cleared {case_count} cases")
        except Exception as e:
            logging.warning(f"Failed to clear cases: {e}")
        
        try:
            # Clear emails
            conn.execute("DELETE FROM emails") 
            deleted_emails = email_count
            logging.info(f"Cleared {email_count} emails")
        except Exception as e:
            logging.warning(f"Failed to clear emails: {e}")
        
        try:
            # Clear flagged senders
            conn.execute("DELETE FROM flagged_senders")
            deleted_flagged = flagged_count
            logging.info(f"Cleared {flagged_count} flagged senders")
        except Exception as e:
            logging.warning(f"Failed to clear flagged senders: {e}")
        
        conn.close()

        summary = f"{deleted_emails} emails, {deleted_cases} cases, {deleted_flagged} flagged senders"

        return jsonify({
            'success': True,
            'message': 'Data clearing completed',
            'summary': summary,
            'email_count': deleted_emails,
            'case_count': deleted_cases,
            'flagged_count': deleted_flagged
        })
    except Exception as e:
        logging.error(f"Clear all data error: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to clear all data: {str(e)}'
        }), 500

@app.route('/api/admin/database-stats')
def api_admin_database_stats():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        
        stats = {}
        
        # Get table counts
        stats['emails'] = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        stats['cases'] = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        stats['flagged_senders'] = conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()[0]
        stats['admin_rules'] = conn.execute("SELECT COUNT(*) FROM admin_rules").fetchone()[0]
        
        conn.close()
        
        return jsonify(stats)
    except Exception as e:
        logging.error(f"Database stats error: {e}")
        return jsonify({'error': 'Failed to load database statistics'}), 500

@app.route('/api/admin/export-database')
def api_admin_export_database():
    """Export database to CSV files"""
    try:
        import zipfile
        from io import BytesIO
        import tempfile
        import os
        
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            conn = get_db_connection()
            
            # Export each table to CSV
            tables = ['emails', 'cases', 'flagged_senders', 'admin_rules']
            
            for table in tables:
                try:
                    df = conn.execute(f"SELECT * FROM {table}").df()
                    csv_path = os.path.join(temp_dir, f"{table}.csv")
                    df.to_csv(csv_path, index=False)
                except Exception as e:
                    logging.warning(f"Could not export table {table}: {e}")
            
            conn.close()
            
            # Create ZIP file
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zip_file.write(file_path, file)
            
            zip_buffer.seek(0)
            
            return send_file(
                zip_buffer,
                mimetype='application/zip',
                as_attachment=True,
                download_name=f'email_guardian_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
            )
            
    except Exception as e:
        logging.error(f"Database export error: {e}")
        return jsonify({'error': 'Failed to export database'}), 500

@app.route('/api/admin/optimize-database', methods=['POST'])
def api_admin_optimize_database():
    """Optimize database performance"""
    try:
        conn = get_db_connection()
        
        # Run VACUUM to optimize database
        conn.execute("VACUUM")
        
        # Update statistics
        conn.execute("ANALYZE")
        
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Database optimized successfully'
        })
    except Exception as e:
        logging.error(f"Database optimization error: {e}")
        return jsonify({'error': 'Failed to optimize database'}), 500

@app.route('/api/export-cleared')
def api_export_cleared():
    """Export cleared emails to Excel"""
    try:
        search = request.args.get('search', '')
        department = request.args.get('department', '')

        conn = get_db_connection()

        where_conditions = ["final_outcome IN ('cleared', 'approved', 'resolved')"]
        params = []

        if search:
            where_conditions.append("(sender LIKE ? OR subject LIKE ?)")
            params.extend([f'%{search}%', f'%{search}%'])

        if department:
            where_conditions.append("department = ?")
            params.append(department)

        where_clause = "WHERE " + " AND ".join(where_conditions)

        query = f"""
            SELECT _time, sender, subject, department, final_outcome, justifications
            FROM emails {where_clause}
            ORDER BY _time DESC
        """

        df = conn.execute(query, params).df()
        conn.close()

        # Generate Excel file
        from io import BytesIO
        import pandas as pd

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Cleared Emails', index=False)

        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'cleared_emails_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
    except Exception as e:
        logging.error(f"Export cleared emails error: {e}")
        return jsonify({'error': 'Failed to export data'}), 500

# Add missing route aliases that the template expects
@app.route('/manage-security-rules')
def manage_security_rules():
    """Security rules management page"""
    return redirect(url_for('admin_rules'))

@app.route('/manage-risk-keywords')
def manage_risk_keywords():
    """Risk keywords management page"""
    return redirect(url_for('admin_panel'))

@app.route('/manage-exclusion-keywords')
def manage_exclusion_keywords():
    """Exclusion keywords management page"""
    return redirect(url_for('admin_panel'))

@app.route('/manage-ml-settings')
def manage_ml_settings():
    """ML settings management page"""
    return redirect(url_for('admin_panel'))

# Add index route that the template references
@app.route('/index')
def index():
    """Index page redirects to dashboard"""
    return redirect(url_for('dashboard'))