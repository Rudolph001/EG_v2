import duckdb
import os
import logging
from datetime import datetime

DATABASE_PATH = "email_guardian.db"

def get_db_connection():
    """Get DuckDB connection"""
    return duckdb.connect(DATABASE_PATH)

def init_database():
    """Initialize the DuckDB database with required tables"""
    try:
        conn = get_db_connection()

        # Create sequences for IDs first
        conn.execute("CREATE SEQUENCE IF NOT EXISTS email_id_seq START 1")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS case_id_seq START 1")

        # Create emails table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY DEFAULT nextval('email_id_seq'),
                _time TIMESTAMP,
                sender VARCHAR,
                subject VARCHAR,
                attachments TEXT,
                recipients TEXT,
                time_month VARCHAR,
                leaver VARCHAR,
                termination_date DATE,
                bunit VARCHAR,
                department VARCHAR,
                user_response VARCHAR,
                final_outcome VARCHAR,
                policy_name VARCHAR,
                justifications TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create cases table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                id INTEGER PRIMARY KEY DEFAULT nextval('case_id_seq'),
                email_id INTEGER,
                escalation_reason TEXT,
                status VARCHAR DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (email_id) REFERENCES emails(id)
            )
        """)

        # Create flagged_senders sequence and table
        conn.execute("CREATE SEQUENCE IF NOT EXISTS flagged_senders_id_seq START 1")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flagged_senders (
                id INTEGER PRIMARY KEY DEFAULT nextval('flagged_senders_id_seq'),
                sender TEXT NOT NULL,
                reason TEXT NOT NULL,
                flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create index for better performance
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_flagged_senders_sender 
            ON flagged_senders(sender)
        """)

        # Create admin_rules sequence and table
        conn.execute("CREATE SEQUENCE IF NOT EXISTS admin_rules_id_seq START 1")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_rules (
                id INTEGER PRIMARY KEY DEFAULT nextval('admin_rules_id_seq'),
                rule_type VARCHAR,
                rule_name VARCHAR,
                logic_type VARCHAR DEFAULT 'AND',
                conditions TEXT,
                action VARCHAR,
                risk_level VARCHAR,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_sender ON emails(sender)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_time ON emails(_time)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_department ON emails(department)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status)")

        conn.close()
        logging.info("Database initialized successfully")

    except Exception as e:
        logging.error(f"Database initialization error: {e}")

def execute_query(query, params=None, fetch=False):
    """Execute a query with error handling"""
    try:
        conn = get_db_connection()
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)

        if fetch:
            data = result.fetchall()
            conn.close()
            return data
        else:
            conn.close()
            return result
    except Exception as e:
        logging.error(f"Database query error: {e}")
        return None

def get_dashboard_stats():
    """Get dashboard statistics with limits to prevent endless growth"""
    conn = get_db_connection()

    try:
        # Basic counts with safety limits
        total_emails = min(conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0], 999999)
        active_cases = min(conn.execute("SELECT COUNT(*) FROM cases WHERE status != 'closed'").fetchone()[0], 9999)
        flagged_senders = min(conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()[0], 9999)

        # Today's emails with limit
        todays_emails = min(conn.execute("""
            SELECT COUNT(*) FROM emails 
            WHERE DATE(_time) = CURRENT_DATE
        """).fetchone()[0], 9999)

        # Get excluded/whitelisted count
        excluded_whitelisted = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE final_outcome IN ('excluded', 'whitelisted')"
        ).fetchone()[0]

        # Department breakdown (limited to top 10)
        department_data = conn.execute("""
            SELECT department, COUNT(*) as count
            FROM emails 
            WHERE department IS NOT NULL AND department != ''
            GROUP BY department
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        # Timeline data (limited to last 30 days max)
        timeline_data = conn.execute("""
            SELECT DATE(_time) as date, COUNT(*) as count
            FROM emails 
            WHERE _time >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY DATE(_time)
            ORDER BY date DESC
            LIMIT 30
        """).fetchall()

        # Convert to safe lists with size limits
        safe_department_data = []
        for i, (dept, count) in enumerate(department_data):
            if i >= 10:  # Hard limit
                break
            safe_department_data.append([str(dept)[:50], min(int(count), 99999)])

        safe_timeline_data = []
        for i, (date, count) in enumerate(timeline_data):
            if i >= 30:  # Hard limit
                break
            safe_timeline_data.append([str(date), min(int(count), 9999)])

        return {
            'total_emails': total_emails,
            'active_cases': active_cases,
            'flagged_senders': flagged_senders,
            'todays_emails': todays_emails,
            'excluded_whitelisted': excluded_whitelisted,
            'department_data': safe_department_data,
            'timeline_data': safe_timeline_data
        }
    except Exception as e:
        logging.error(f"Error getting dashboard stats: {e}")
        return {
            'total_emails': 0,
            'active_cases': 0,
            'flagged_senders': 0,
            'todays_emails': 0,
            'excluded_whitelisted': 0,
            'department_data': [],
            'timeline_data': []
        }
    finally:
        conn.close()