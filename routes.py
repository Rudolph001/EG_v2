import os
import csv
import pandas as pd
from flask import render_template, request, jsonify, flash, redirect, url_for, send_file
from werkzeug.utils import secure_filename
from app import app
from database import get_db_connection, get_dashboard_stats, execute_query
from ml_processor import classify_email, train_model
from report_generator import generate_pdf_report, generate_excel_report
from csv_ingest import CSVIngestor
import logging
from datetime import datetime, timedelta
import json

UPLOAD_FOLDER = 'data'
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def dashboard():
    """Main dashboard with analytics"""
    stats = get_dashboard_stats()
    return render_template('dashboard.html', stats=stats)

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

@app.route('/flagged-senders')
def flagged_senders():
    """Flagged senders management"""
    conn = get_db_connection()
    senders = conn.execute("SELECT * FROM flagged_senders ORDER BY flagged_at DESC").fetchall()
    conn.close()
    
    return render_template('flagged_senders.html', senders=senders)

@app.route('/admin-rules')
def admin_rules():
    """Admin rules configuration"""
    conn = get_db_connection()
    rules = conn.execute("SELECT * FROM admin_rules ORDER BY created_at DESC").fetchall()
    conn.close()
    
    return render_template('admin_rules.html', rules=rules)

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
    """Train ML model with current data"""
    try:
        accuracy = train_model()
        return jsonify({
            'success': True, 
            'message': f'Model trained successfully with {accuracy:.2%} accuracy'
        })
    except Exception as e:
        logging.error(f"Model training error: {e}")
        return jsonify({'error': 'Model training failed'}), 500

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
