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
        
        # Create sequence for email IDs first
        conn.execute("CREATE SEQUENCE IF NOT EXISTS email_id_seq START 1")
        
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
                id INTEGER PRIMARY KEY,
                email_id INTEGER,
                escalation_reason TEXT,
                status VARCHAR DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (email_id) REFERENCES emails(id)
            )
        """)
        
        # Create flagged_senders table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flagged_senders (
                id INTEGER PRIMARY KEY,
                sender VARCHAR UNIQUE,
                reason TEXT,
                flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create admin_rules table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_rules (
                id INTEGER PRIMARY KEY,
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
    """Get statistics for dashboard"""
    conn = get_db_connection()
    
    stats = {}
    
    # Total emails
    result = conn.execute("SELECT COUNT(*) FROM emails").fetchone()
    stats['total_emails'] = result[0] if result else 0
    
    # Active cases
    result = conn.execute("SELECT COUNT(*) FROM cases WHERE status = 'open'").fetchone()
    stats['active_cases'] = result[0] if result else 0
    
    # Flagged senders
    result = conn.execute("SELECT COUNT(*) FROM flagged_senders").fetchone()
    stats['flagged_senders'] = result[0] if result else 0
    
    # Today's emails
    result = conn.execute("SELECT COUNT(*) FROM emails WHERE DATE(_time) = CURRENT_DATE").fetchone()
    stats['todays_emails'] = result[0] if result else 0
    
    # Department breakdown
    dept_data = conn.execute("""
        SELECT department, COUNT(*) as count 
        FROM emails 
        WHERE department IS NOT NULL 
        GROUP BY department 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()
    stats['department_data'] = dept_data
    
    # Timeline data (last 30 days)
    timeline_data = conn.execute("""
        SELECT DATE(_time) as date, COUNT(*) as count
        FROM emails 
        WHERE _time >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY DATE(_time)
        ORDER BY date
    """).fetchall()
    stats['timeline_data'] = timeline_data
    
    conn.close()
    return stats
