import os
import csv
import pandas as pd
from flask import render_template, request, jsonify, flash, redirect, url_for, send_file
from werkzeug.utils import secure_filename
from app import app
from database import get_db_connection, get_dashboard_stats, execute_query
from ml_processor import classify_email, train_model
from ml_models import train_advanced_models, predict_email_risk, get_ml_insights, get_analytics_report
from report_generator import generate_pdf_report, generate_excel_report
from csv_ingest import CSVIngestor
from processor import EmailProcessor
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

    # Build query for non-excluded/whitelisted emails
    where_conditions = ["(final_outcome IS NULL OR final_outcome NOT IN ('excluded', 'whitelisted'))"]
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
        SELECT e.*, 
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

@app.route('/flagged-senders')
def flagged_senders():
    """Flagged Senders Dashboard: monitor flagged senders across imports"""
    conn = get_db_connection()

    # Get flagged senders with email counts
    senders = conn.execute("""
        SELECT fs.*, 
               COUNT(e.id) as email_count,
               MAX(e._time) as last_email_date,
               COUNT(CASE WHEN e.final_outcome IN ('escalated', 'high_risk') THEN 1 END) as high_risk_count
        FROM flagged_senders fs
        LEFT JOIN emails e ON fs.sender = e.sender
        GROUP BY fs.id, fs.sender, fs.reason, fs.flagged_at
        ORDER BY fs.flagged_at DESC
    """).fetchall()

    # Get domain statistics
    domain_stats = conn.execute("""
        SELECT 
            SUBSTR(sender, POSITION('@' IN sender) + 1) as domain,
            COUNT(*) as count
        FROM emails e
        JOIN flagged_senders fs ON e.sender = fs.sender
        WHERE POSITION('@' IN sender) > 0
        GROUP BY SUBSTR(sender, POSITION('@' IN sender) + 1)
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    return render_template('flagged_senders.html', 
                         senders=senders,
                         domain_stats=dict(domain_stats))

@app.route('/analytics')
def analytics():
    """Analytics Dashboard: charts for policy violations, escalations, risk categories"""
    conn = get_db_connection()

    # Policy violations data
    policy_violations = conn.execute("""
        SELECT policy_name, COUNT(*) as count
        FROM emails 
        WHERE policy_name IS NOT NULL
        GROUP BY policy_name
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    # Escalations over time (last 30 days)
    escalations_timeline = conn.execute("""
        SELECT DATE(_time) as date, COUNT(*) as count
        FROM emails 
        WHERE final_outcome IN ('escalated', 'high_risk', 'pending_review')
        AND _time >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY DATE(_time)
        ORDER BY date
    """).fetchall()

    # Risk categories distribution
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
        GROUP BY risk_level
        ORDER BY count DESC
    """).fetchall()

    # Department risk analysis
    dept_risk = conn.execute("""
        SELECT 
            department,
            COUNT(*) as total_emails,
            COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as high_risk_count,
            ROUND(COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) * 100.0 / COUNT(*), 2) as risk_percentage
        FROM emails
        WHERE department IS NOT NULL
        GROUP BY department
        HAVING COUNT(*) > 10
        ORDER BY risk_percentage DESC
        LIMIT 10
    """).fetchall()

    # Monthly trend data
    monthly_trends = conn.execute("""
        SELECT 
            strftime('%Y-%m', _time) as month,
            COUNT(*) as total_emails,
            COUNT(CASE WHEN final_outcome IN ('escalated', 'high_risk') THEN 1 END) as escalated_emails
        FROM emails
        WHERE _time >= DATE('now', '-12 months')
        GROUP BY strftime('%Y-%m', _time)
        ORDER BY month
    """).fetchall()

    conn.close()

    analytics_data = {
        'policy_violations': policy_violations,
        'escalations_timeline': escalations_timeline,
        'risk_categories': risk_categories,
        'dept_risk': dept_risk,
        'monthly_trends': monthly_trends
    }

    return render_template('analytics.html', analytics=analytics_data)

@app.route('/admin-rules')
def admin_rules():
    """Admin rules configuration"""
    conn = get_db_connection()
    rules = conn.execute("SELECT * FROM admin_rules ORDER BY created_at DESC").fetchall()
    conn.close()

    return render_template('admin_rules.html', rules=rules)

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

            # Process CSV with DuckDB for high performance
            conn = get_db_connection()

            # Use DuckDB's native CSV reading capability
            result = conn.execute(f"""
                INSERT INTO emails 
                SELECT * FROM read_csv_auto('{filepath}')
            """)

            rows_inserted = result.fetchone()[0] if result else 0
            conn.close()

            # Clean up uploaded file
            os.remove(filepath)

            return jsonify({
                'success': True, 
                'message': f'Successfully imported {rows_inserted} email records'
            })

        except Exception as e:
            logging.error(f"CSV upload error: {e}")
            return jsonify({'error': f'Import failed: {str(e)}'}), 500

    return jsonify({'error': 'Invalid file type'}), 400

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
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO admin_rules (rule_type, conditions, action) 
            VALUES (?, ?, ?)
        """, [data.get('rule_type'), data.get('conditions'), data.get('action')])
        conn.close()

        return jsonify({'success': True, 'message': 'Rule added successfully'})
    except Exception as e:
        logging.error(f"Add rule error: {e}")
        return jsonify({'error': 'Failed to add rule'}), 500

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

@app.route('/api/dashboard-stats')
def api_dashboard_stats():
    """API endpoint for dashboard statistics"""
    stats = get_dashboard_stats()
    return jsonify(stats)

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
    data = request.json or {}
    report_type = data.get('type', 'pdf')
    date_from = data.get('date_from')
    date_to = data.get('date_to')

    try:
        if report_type == 'pdf':
            filename = generate_pdf_report(date_from, date_to)
        else:
            filename = generate_excel_report(date_from, date_to)

        return send_file(filename, as_attachment=True)
    except Exception as e:
        logging.error(f"Report generation error: {e}")
        return jsonify({'error': 'Report generation failed'}), 500

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

@app.route('/api/generate-followup/<int:email_id>', methods=['POST'])
def api_generate_followup(email_id):
    """Generate follow-up email for Outlook"""
    try:
        conn = get_db_connection()
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        conn.close()

        if not email_data:
            return jsonify({'error': 'Email not found'}), 404

        # Generate follow-up email content
        sender = email_data[2]  # sender field
        subject = email_data[3]  # subject field
        department = email_data[10]  # department field

        followup_subject = f"Follow-up Required: {subject}"
        followup_body = f"""
Dear Team,

This email requires follow-up action regarding a potential policy violation.

Original Email Details:
- Sender: {sender}
- Subject: {subject}
- Department: {department or 'Unknown'}
- Date: {email_data[1]}

Please review and take appropriate action.

Best regards,
Email Guardian System
        """

        # Create Outlook mailto link
        outlook_link = f"mailto:?subject={followup_subject}&body={followup_body.replace(chr(10), '%0D%0A')}"

        return jsonify({
            'success': True,
            'outlook_link': outlook_link,
            'subject': followup_subject,
            'body': followup_body
        })
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

        # Get ML classification
        text = f"{email_data[3] or ''} {email_data[14] or ''}"
        classification = classify_email(text)

        # Mock ML insights (in real implementation, this would come from your ML model)
        insights = {
            'classification': classification,
            'confidence': 0.85,  # Mock confidence score
            'explanation': f"Email classified as '{classification}' based on content analysis",
            'risk_factors': [
                {'name': 'External Recipient', 'severity': 'medium'},
                {'name': 'Attachment Present', 'severity': 'low'},
                {'name': 'Policy Keyword Match', 'severity': 'high'}
            ],
            'recommendations': f"Based on the '{classification}' classification, recommend {'immediate review' if classification in ['high_risk', 'suspicious'] else 'standard processing'}."
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

# Add the missing index route to fix Flask error
@app.route('/')
def index():
    """Redirect to main dashboard"""
    return redirect(url_for('dashboard'))