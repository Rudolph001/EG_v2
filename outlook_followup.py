
import os
import json
import logging
import subprocess
import platform
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import tempfile
import webbrowser
from urllib.parse import quote

from database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OutlookFollowupGenerator:
    """Generate and send follow-up emails via Outlook integration"""
    
    def __init__(self, logs_dir='logs'):
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(exist_ok=True)
        self.followup_log_file = self.logs_dir / 'followup_emails.log'
        
        # Setup followup logging
        self.followup_logger = logging.getLogger('followup')
        if not self.followup_logger.handlers:
            handler = logging.FileHandler(self.followup_log_file)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            ))
            self.followup_logger.addHandler(handler)
            self.followup_logger.setLevel(logging.INFO)
    
    def get_email_context(self, email_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve detailed email context for follow-up"""
        try:
            conn = get_db_connection()
            
            # Get email details with case information
            query = """
                SELECT e.*, c.status as case_status, c.escalation_reason, c.created_at as case_created
                FROM emails e
                LEFT JOIN cases c ON e.id = c.email_id
                WHERE e.id = ?
            """
            
            email_data = conn.execute(query, [email_id]).fetchone()
            conn.close()
            
            if not email_data:
                return None
            
            # Convert to dictionary with meaningful field names
            email_context = {
                'id': email_data[0],
                'timestamp': email_data[1],
                'sender': email_data[2],
                'subject': email_data[3],
                'attachments': email_data[4] or 'None',
                'recipients': email_data[5] or 'Not specified',
                'time_month': email_data[6],
                'leaver': email_data[7],
                'termination_date': email_data[8],
                'bunit': email_data[9],
                'department': email_data[10] or 'Unknown',
                'user_response': email_data[11],
                'final_outcome': email_data[12],
                'policy_name': email_data[13] or 'General Policy',
                'justifications': email_data[14],
                'case_status': email_data[15] if len(email_data) > 15 else 'N/A',
                'escalation_reason': email_data[16] if len(email_data) > 16 else 'Email flagged for review',
                'case_created': email_data[17] if len(email_data) > 17 else None
            }
            
            return email_context
            
        except Exception as e:
            logger.error(f"Error getting email context: {e}")
            return None
    
    def generate_followup_content(self, email_context: Dict[str, Any], 
                                 followup_type: str = 'escalation') -> Dict[str, str]:
        """Generate follow-up email content based on context"""
        
        sender = email_context.get('sender', 'Unknown')
        subject = email_context.get('subject', 'No Subject')
        department = email_context.get('department', 'Unknown')
        policy = email_context.get('policy_name', 'General Policy')
        timestamp = email_context.get('timestamp', 'Unknown')
        attachments = email_context.get('attachments', 'None')
        recipients = email_context.get('recipients', 'Not specified')
        escalation_reason = email_context.get('escalation_reason', 'Email flagged for review')
        
        # Format timestamp
        try:
            if timestamp and timestamp != 'Unknown':
                if isinstance(timestamp, str):
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                formatted_date = timestamp.strftime('%B %d, %Y at %I:%M %p')
            else:
                formatted_date = 'Unknown date'
        except:
            formatted_date = str(timestamp)
        
        if followup_type == 'escalation':
            email_subject = f"URGENT: Email Policy Violation Requires Review - Case #{email_context.get('id')}"
            
            email_body = f"""Dear Security Team,

An email has been escalated for immediate review due to potential policy violations.

ESCALATION DETAILS:
• Case ID: #{email_context.get('id')}
• Escalation Reason: {escalation_reason}
• Priority: High
• Status: Requires Immediate Attention

ORIGINAL EMAIL DETAILS:
• From: {sender}
• Subject: {subject}
• Date: {formatted_date}
• Department: {department}
• Policy Triggered: {policy}
• Recipients: {recipients}
• Attachments: {attachments}

RECOMMENDED ACTIONS:
1. Review the original email content in the Email Guardian system
2. Investigate potential policy violations
3. Contact the sender if clarification is needed
4. Document findings and resolution
5. Update case status in the system

Please log into the Email Guardian system to review the complete email content and take appropriate action.

System Link: [Email Guardian Dashboard]

This is an automated notification from Email Guardian.
For questions, contact the IT Security team.

Best regards,
Email Guardian System
"""
        
        elif followup_type == 'investigation':
            email_subject = f"Investigation Required: Email Case #{email_context.get('id')} - {subject}"
            
            email_body = f"""Dear Team,

An email requires further investigation as part of our security monitoring process.

INVESTIGATION DETAILS:
• Case ID: #{email_context.get('id')}
• Original Email Date: {formatted_date}
• Sender: {sender}
• Department: {department}
• Subject: {subject}

CONTEXT:
• Policy Involved: {policy}
• Recipients: {recipients}
• Attachments: {attachments}
• Current Status: Under Investigation

NEXT STEPS:
1. Review email content for compliance violations
2. Check sender's recent email activity
3. Verify recipient list for data exposure risks
4. Document investigation findings
5. Recommend appropriate actions

Please complete your investigation within 24 hours and update the case status.

Access the Email Guardian system for full details.

Regards,
Email Guardian System
"""
        
        elif followup_type == 'notification':
            email_subject = f"Email Monitoring Alert - Case #{email_context.get('id')}"
            
            email_body = f"""Team,

This is a notification regarding an email that has been flagged by our monitoring system.

NOTIFICATION DETAILS:
• Email Date: {formatted_date}
• From: {sender}
• To: {recipients}
• Subject: {subject}
• Department: {department}
• Attachments: {attachments}

The email has been processed according to our policies. No immediate action is required unless specified.

For reference, this email is catalogued as Case #{email_context.get('id')} in our system.

Email Guardian Monitoring System
"""
        
        else:  # Default generic follow-up
            email_subject = f"Email Follow-up Required - Case #{email_context.get('id')}"
            email_body = f"""Dear Team,

Please review the following email case that requires follow-up action.

Case ID: #{email_context.get('id')}
Original Email: {subject}
From: {sender}
Date: {formatted_date}
Department: {department}

Please take appropriate action and update the case status.

Email Guardian System
"""
        
        return {
            'subject': email_subject,
            'body': email_body
        }
    
    def create_outlook_mailto_link(self, to_addresses: str, subject: str, body: str) -> str:
        """Create a mailto link that opens in Outlook"""
        
        # URL encode the parameters
        encoded_subject = quote(subject)
        encoded_body = quote(body.replace('\n', '\r\n'))
        encoded_to = quote(to_addresses)
        
        mailto_link = f"mailto:{encoded_to}?subject={encoded_subject}&body={encoded_body}"
        
        return mailto_link
    
    def generate_outlook_vbs_script(self, to_addresses: str, subject: str, body: str, 
                                  cc_addresses: str = "", bcc_addresses: str = "") -> str:
        """Generate VBS script to create Outlook email on Windows"""
        
        vbs_script = f'''
Set objOutlook = CreateObject("Outlook.Application")
Set objMail = objOutlook.CreateItem(0)

objMail.To = "{to_addresses}"
objMail.CC = "{cc_addresses}"
objMail.BCC = "{bcc_addresses}"
objMail.Subject = "{subject.replace('"', '""')}"
objMail.Body = "{body.replace('"', '""').replace(chr(10), chr(13) + chr(10))}"

objMail.Display
'''
        
        return vbs_script
    
    def send_via_outlook_windows(self, to_addresses: str, subject: str, body: str,
                               cc_addresses: str = "", auto_send: bool = False) -> bool:
        """Send email via Outlook on Windows using VBS script"""
        
        if platform.system() != "Windows":
            logger.warning("Outlook VBS integration only works on Windows")
            return False
        
        try:
            # Create VBS script
            vbs_content = self.generate_outlook_vbs_script(
                to_addresses, subject, body, cc_addresses
            )
            
            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.vbs', delete=False) as f:
                f.write(vbs_content)
                vbs_file = f.name
            
            # Execute VBS script
            result = subprocess.run(['cscript', '//nologo', vbs_file], 
                                  capture_output=True, text=True)
            
            # Clean up
            os.unlink(vbs_file)
            
            if result.returncode == 0:
                logger.info("Outlook email created successfully")
                return True
            else:
                logger.error(f"VBS script execution failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending via Outlook Windows: {e}")
            return False
    
    def open_mailto_link(self, mailto_link: str) -> bool:
        """Open mailto link in default email client"""
        
        try:
            webbrowser.open(mailto_link)
            return True
        except Exception as e:
            logger.error(f"Error opening mailto link: {e}")
            return False
    
    def generate_followup_email(self, email_id: int, followup_type: str = 'escalation',
                              to_addresses: str = "security@company.com",
                              cc_addresses: str = "") -> Dict[str, Any]:
        """Generate a follow-up email for an escalated case"""
        
        try:
            # Get email context
            email_context = self.get_email_context(email_id)
            if not email_context:
                return {'error': 'Email not found or could not retrieve context'}
            
            # Generate content
            content = self.generate_followup_content(email_context, followup_type)
            
            # Create mailto link (works cross-platform)
            mailto_link = self.create_outlook_mailto_link(
                to_addresses, content['subject'], content['body']
            )
            
            # Log the generation
            self.followup_logger.info(
                f"Generated follow-up email for Case #{email_id} - Type: {followup_type} - To: {to_addresses}"
            )
            
            return {
                'success': True,
                'email_id': email_id,
                'followup_type': followup_type,
                'subject': content['subject'],
                'body': content['body'],
                'to_addresses': to_addresses,
                'cc_addresses': cc_addresses,
                'mailto_link': mailto_link,
                'email_context': email_context
            }
            
        except Exception as e:
            logger.error(f"Error generating follow-up email: {e}")
            return {'error': str(e)}
    
    def send_followup_email(self, email_id: int, followup_data: Dict[str, Any],
                          method: str = 'mailto') -> Dict[str, Any]:
        """Send the follow-up email using specified method"""
        
        try:
            to_addresses = followup_data.get('to_addresses', 'security@company.com')
            cc_addresses = followup_data.get('cc_addresses', '')
            subject = followup_data['subject']
            body = followup_data['body']
            
            success = False
            
            if method == 'outlook_windows' and platform.system() == "Windows":
                success = self.send_via_outlook_windows(to_addresses, subject, body, cc_addresses)
            elif method == 'mailto':
                mailto_link = followup_data.get('mailto_link')
                if mailto_link:
                    success = self.open_mailto_link(mailto_link)
            else:
                return {'error': f'Unsupported send method: {method}'}
            
            if success:
                # Log the successful send
                self.log_sent_followup(email_id, followup_data, method)
                
                return {
                    'success': True,
                    'message': 'Follow-up email sent successfully',
                    'method': method,
                    'email_id': email_id
                }
            else:
                return {'error': 'Failed to send follow-up email'}
                
        except Exception as e:
            logger.error(f"Error sending follow-up email: {e}")
            return {'error': str(e)}
    
    def log_sent_followup(self, email_id: int, followup_data: Dict[str, Any], method: str):
        """Log sent follow-up email details"""
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'email_id': email_id,
            'followup_type': followup_data.get('followup_type', 'unknown'),
            'to_addresses': followup_data.get('to_addresses', ''),
            'cc_addresses': followup_data.get('cc_addresses', ''),
            'subject': followup_data.get('subject', ''),
            'method': method,
            'status': 'sent'
        }
        
        # Log to both regular log and structured JSON log
        self.followup_logger.info(f"SENT - Case #{email_id} - {method} - To: {followup_data.get('to_addresses', '')}")
        
        # Also save to JSON log for structured data
        json_log_file = self.logs_dir / 'followup_emails.json'
        
        try:
            # Load existing logs
            if json_log_file.exists():
                with open(json_log_file, 'r') as f:
                    logs = json.load(f)
            else:
                logs = []
            
            # Add new entry
            logs.append(log_entry)
            
            # Save updated logs
            with open(json_log_file, 'w') as f:
                json.dump(logs, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving JSON log: {e}")
    
    def get_followup_history(self, email_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get history of sent follow-up emails"""
        
        json_log_file = self.logs_dir / 'followup_emails.json'
        
        try:
            if not json_log_file.exists():
                return []
            
            with open(json_log_file, 'r') as f:
                logs = json.load(f)
            
            if email_id:
                # Filter by specific email ID
                logs = [log for log in logs if log.get('email_id') == email_id]
            
            # Sort by timestamp (newest first)
            logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            return logs
            
        except Exception as e:
            logger.error(f"Error reading followup history: {e}")
            return []
    
    def bulk_generate_followups(self, email_ids: List[int], followup_type: str = 'escalation',
                              to_addresses: str = "security@company.com") -> Dict[str, Any]:
        """Generate follow-up emails for multiple cases"""
        
        results = {
            'success_count': 0,
            'error_count': 0,
            'results': [],
            'errors': []
        }
        
        for email_id in email_ids:
            try:
                result = self.generate_followup_email(email_id, followup_type, to_addresses)
                
                if result.get('success'):
                    results['success_count'] += 1
                    results['results'].append(result)
                else:
                    results['error_count'] += 1
                    results['errors'].append({
                        'email_id': email_id,
                        'error': result.get('error', 'Unknown error')
                    })
                    
            except Exception as e:
                results['error_count'] += 1
                results['errors'].append({
                    'email_id': email_id,
                    'error': str(e)
                })
        
        # Create combined email content for bulk send
        if results['results']:
            combined_subject = f"Bulk Follow-up: {len(results['results'])} Cases Require Attention"
            
            combined_body = f"""Dear Security Team,

Multiple cases require your attention. Please review the following escalated emails:

BULK FOLLOW-UP SUMMARY:
• Total Cases: {len(results['results'])}
• Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
• Priority: High

CASE DETAILS:
"""
            
            for i, result in enumerate(results['results'], 1):
                email_context = result['email_context']
                combined_body += f"""
{i}. Case #{result['email_id']}
   • From: {email_context.get('sender', 'Unknown')}
   • Subject: {email_context.get('subject', 'No Subject')}
   • Department: {email_context.get('department', 'Unknown')}
   • Date: {email_context.get('timestamp', 'Unknown')}
"""
            
            combined_body += f"""
Please log into the Email Guardian system to review each case individually.

This is an automated bulk notification from Email Guardian.

Best regards,
Email Guardian System
"""
            
            # Create bulk mailto link
            bulk_mailto = self.create_outlook_mailto_link(to_addresses, combined_subject, combined_body)
            results['bulk_mailto'] = bulk_mailto
            results['bulk_subject'] = combined_subject
            results['bulk_body'] = combined_body
        
        return results


# Global instance for easy import
outlook_generator = OutlookFollowupGenerator()

# Convenience functions
def generate_followup_email(email_id: int, followup_type: str = 'escalation', 
                          to_addresses: str = "security@company.com") -> Dict[str, Any]:
    """Generate a follow-up email for an escalated case"""
    return outlook_generator.generate_followup_email(email_id, followup_type, to_addresses)

def send_followup_email(email_id: int, followup_data: Dict[str, Any], 
                       method: str = 'mailto') -> Dict[str, Any]:
    """Send a follow-up email"""
    return outlook_generator.send_followup_email(email_id, followup_data, method)

def get_followup_history(email_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get history of sent follow-up emails"""
    return outlook_generator.get_followup_history(email_id)

def bulk_generate_followups(email_ids: List[int], followup_type: str = 'escalation',
                          to_addresses: str = "security@company.com") -> Dict[str, Any]:
    """Generate follow-up emails for multiple cases"""
    return outlook_generator.bulk_generate_followups(email_ids, followup_type, to_addresses)


if __name__ == "__main__":
    # Command line interface for testing
    import argparse
    
    parser = argparse.ArgumentParser(description='Outlook Follow-up Email Generator')
    parser.add_argument('--generate', type=int, help='Generate follow-up for email ID')
    parser.add_argument('--type', default='escalation', help='Follow-up type (escalation, investigation, notification)')
    parser.add_argument('--to', default='security@company.com', help='Recipient email addresses')
    parser.add_argument('--send', action='store_true', help='Send the generated email')
    parser.add_argument('--history', type=int, nargs='?', const=-1, help='Show follow-up history (optional email ID)')
    parser.add_argument('--bulk', nargs='+', type=int, help='Generate bulk follow-ups for multiple email IDs')
    
    args = parser.parse_args()
    
    if args.history is not None:
        email_id = args.history if args.history > 0 else None
        history = get_followup_history(email_id)
        print(f"Follow-up History ({len(history)} entries):")
        for entry in history:
            print(f"- {entry['timestamp']}: Case #{entry['email_id']} -> {entry['to_addresses']} ({entry['method']})")
    
    elif args.bulk:
        print(f"Generating bulk follow-ups for {len(args.bulk)} cases...")
        result = bulk_generate_followups(args.bulk, args.type, args.to)
        print(f"Success: {result['success_count']}, Errors: {result['error_count']}")
        if result.get('bulk_mailto'):
            print(f"Bulk mailto link: {result['bulk_mailto'][:100]}...")
    
    elif args.generate:
        print(f"Generating follow-up for email {args.generate}...")
        result = generate_followup_email(args.generate, args.type, args.to)
        
        if result.get('success'):
            print(f"Subject: {result['subject']}")
            print(f"Mailto link: {result['mailto_link'][:100]}...")
            
            if args.send:
                send_result = send_followup_email(args.generate, result)
                if send_result.get('success'):
                    print("Email sent successfully!")
                else:
                    print(f"Send failed: {send_result.get('error')}")
        else:
            print(f"Generation failed: {result.get('error')}")
    
    else:
        print("Use --generate <email_id>, --bulk <id1> <id2> ..., or --history [email_id]")
