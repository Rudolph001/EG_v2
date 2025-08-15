
import duckdb
from database import DATABASE_PATH

def fix_cases_table():
    """Fix the cases table to have proper auto-incrementing ID"""
    conn = duckdb.connect(DATABASE_PATH)
    
    try:
        # Create the sequence if it doesn't exist
        conn.execute("CREATE SEQUENCE IF NOT EXISTS case_id_seq START 1")
        
        # Drop and recreate the cases table with proper ID handling
        conn.execute("DROP TABLE IF EXISTS cases")
        
        conn.execute("""
            CREATE TABLE cases (
                id INTEGER PRIMARY KEY DEFAULT nextval('case_id_seq'),
                email_id INTEGER,
                escalation_reason TEXT,
                status VARCHAR DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (email_id) REFERENCES emails(id)
            )
        """)
        
        print("Cases table fixed successfully")
        
    except Exception as e:
        print(f"Error fixing cases table: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    fix_cases_table()
