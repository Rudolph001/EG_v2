
import logging
import json
from datetime import datetime
from database import get_db_connection
from typing import Optional, Dict, Any

# Configure action logger
action_logger = logging.getLogger('user_actions')
action_handler = logging.FileHandler('logs/user_actions.log')
action_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
action_logger.addHandler(action_handler)
action_logger.setLevel(logging.INFO)

class ActionTracker:
    """Track and log user actions across the application"""
    
    @staticmethod
    def track_action(action_type: str, email_id: Optional[int] = None, 
                    case_id: Optional[int] = None, details: Optional[Dict[Any, Any]] = None,
                    user_id: str = 'system'):
        """
        Track a user action and log it to both database and file
        
        Args:
            action_type: Type of action (clear, escalate, flag, search, filter, etc.)
            email_id: Related email ID if applicable
            case_id: Related case ID if applicable
            details: Additional details about the action
            user_id: User performing the action (default: system)
        """
        try:
            # Log to database
            conn = get_db_connection()
            
            # Create user_actions table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_actions (
                    id INTEGER PRIMARY KEY,
                    user_id VARCHAR NOT NULL,
                    action_type VARCHAR NOT NULL,
                    email_id INTEGER,
                    case_id INTEGER,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (email_id) REFERENCES emails(id),
                    FOREIGN KEY (case_id) REFERENCES cases(id)
                )
            """)
            
            # Insert the action
            conn.execute("""
                INSERT INTO user_actions (user_id, action_type, email_id, case_id, details)
                VALUES (?, ?, ?, ?, ?)
            """, [user_id, action_type, email_id, case_id, json.dumps(details) if details else None])
            
            conn.close()
            
            # Log to file
            log_message = f"User: {user_id} | Action: {action_type}"
            if email_id:
                log_message += f" | Email ID: {email_id}"
            if case_id:
                log_message += f" | Case ID: {case_id}"
            if details:
                log_message += f" | Details: {json.dumps(details)}"
            
            action_logger.info(log_message)
            
        except Exception as e:
            logging.error(f"Failed to track action: {e}")
    
    @staticmethod
    def get_user_activity(user_id: Optional[str] = None, limit: int = 100):
        """Get recent user activity"""
        try:
            conn = get_db_connection()
            
            if user_id:
                query = """
                    SELECT ua.*, e.sender, e.subject, c.escalation_reason
                    FROM user_actions ua
                    LEFT JOIN emails e ON ua.email_id = e.id
                    LEFT JOIN cases c ON ua.case_id = c.id
                    WHERE ua.user_id = ?
                    ORDER BY ua.timestamp DESC
                    LIMIT ?
                """
                result = conn.execute(query, [user_id, limit]).fetchall()
            else:
                query = """
                    SELECT ua.*, e.sender, e.subject, c.escalation_reason
                    FROM user_actions ua
                    LEFT JOIN emails e ON ua.email_id = e.id
                    LEFT JOIN cases c ON ua.case_id = c.id
                    ORDER BY ua.timestamp DESC
                    LIMIT ?
                """
                result = conn.execute(query, [limit]).fetchall()
            
            conn.close()
            return result
            
        except Exception as e:
            logging.error(f"Failed to get user activity: {e}")
            return []
    
    @staticmethod
    def get_action_stats(days: int = 30):
        """Get action statistics for the last N days"""
        try:
            conn = get_db_connection()
            
            stats = conn.execute("""
                SELECT 
                    action_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM user_actions
                WHERE timestamp >= DATE('now', '-{} days')
                GROUP BY action_type
                ORDER BY count DESC
            """.format(days)).fetchall()
            
            conn.close()
            return stats
            
        except Exception as e:
            logging.error(f"Failed to get action stats: {e}")
            return []

# Global instance
action_tracker = ActionTracker()
