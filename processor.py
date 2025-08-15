import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json

from database import get_db_connection, execute_query
from ml_processor import classify_email

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RiskLevel(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"

class ProcessingResult(Enum):
    ESCALATED = "escalated"
    CLEARED = "cleared"
    EXCLUDED = "excluded"
    WHITELISTED = "whitelisted"
    PENDING_REVIEW = "pending_review"

@dataclass
class ProcessingAction:
    """Represents an action taken during email processing"""
    action_type: str
    rule_applied: Optional[str] = None
    reason: Optional[str] = None
    confidence: Optional[float] = None
    details: Optional[Dict[str, Any]] = None

@dataclass
class EmailProcessingResult:
    """Result of processing a single email"""
    email_id: int
    final_status: ProcessingResult
    risk_level: RiskLevel
    actions_taken: List[ProcessingAction]
    ml_classification: Optional[str] = None
    processing_notes: Optional[str] = None

class EmailProcessor:
    """
    Comprehensive email processor for Email Guardian system
    Handles admin rules, security analysis, ML classification, and routing
    """
    
    def __init__(self):
        self.risk_keywords = [
            # Financial/Legal risk keywords
            'confidential', 'classified', 'restricted', 'sensitive', 'proprietary',
            'trade secret', 'merger', 'acquisition', 'insider', 'lawsuit', 'legal action',
            'subpoena', 'investigation', 'fraud', 'breach', 'violation', 'compliance',
            
            # Security risk keywords
            'password', 'credential', 'login', 'access key', 'api key', 'token',
            'vulnerability', 'exploit', 'malware', 'phishing', 'ransomware',
            
            # HR/Personnel risk keywords
            'termination', 'resignation', 'dismissal', 'harassment', 'discrimination',
            'grievance', 'complaint', 'misconduct', 'policy violation',
            
            # Data risk keywords
            'personal data', 'pii', 'gdpr', 'hipaa', 'sox', 'customer data',
            'financial records', 'bank account', 'social security', 'credit card'
            
            # Add your custom risk keywords here
            # 'custom_keyword1', 'custom_keyword2'
        ]
        
        self.exclusion_keywords = [
            # Administrative/System emails to exclude
            'automated', 'no-reply', 'noreply', 'do not reply', 'system notification',
            'newsletter', 'marketing', 'promotional', 'advertisement', 'unsubscribe',
            'out of office', 'auto-reply', 'vacation', 'away message',
            
            # Common safe communications
            'meeting invite', 'calendar', 'reminder', 'thank you', 'congratulations',
            'welcome', 'birthday', 'holiday', 'lunch', 'coffee', 'social event'
            
            # Add your custom safe keywords here that should result in clearing
            # 'custom_safe_keyword1', 'custom_safe_keyword2'
        ]
        
        # Compile regex patterns for better performance
        self.risk_pattern = re.compile(
            r'\b(?:' + '|'.join(re.escape(kw) for kw in self.risk_keywords) + r')\b',
            re.IGNORECASE
        )
        
        self.exclusion_pattern = re.compile(
            r'\b(?:' + '|'.join(re.escape(kw) for kw in self.exclusion_keywords) + r')\b',
            re.IGNORECASE
        )
    
    def load_admin_rules(self) -> List[Dict[str, Any]]:
        """Load active admin rules from database"""
        try:
            conn = get_db_connection()
            rules = conn.execute("""
                SELECT id, rule_type, conditions, action 
                FROM admin_rules 
                WHERE is_active = true 
                ORDER BY created_at ASC
            """).fetchall()
            conn.close()
            
            parsed_rules = []
            for rule in rules:
                try:
                    conditions = json.loads(rule[2]) if rule[2] else {}
                    parsed_rules.append({
                        'id': rule[0],
                        'rule_type': rule[1],
                        'conditions': conditions,
                        'action': rule[3]
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in rule {rule[0]}: {rule[2]}")
                    # Treat as simple text condition
                    parsed_rules.append({
                        'id': rule[0],
                        'rule_type': rule[1],
                        'conditions': {'text': rule[2]},
                        'action': rule[3]
                    })
            
            logger.info(f"Loaded {len(parsed_rules)} active admin rules")
            return parsed_rules
            
        except Exception as e:
            logger.error(f"Error loading admin rules: {e}")
            return []
    
    def load_flagged_senders(self) -> List[str]:
        """Load list of flagged senders"""
        try:
            conn = get_db_connection()
            senders = conn.execute("SELECT sender FROM flagged_senders").fetchall()
            conn.close()
            
            flagged_list = [sender[0].lower() for sender in senders]
            logger.info(f"Loaded {len(flagged_list)} flagged senders")
            return flagged_list
            
        except Exception as e:
            logger.error(f"Error loading flagged senders: {e}")
            return []
    
    def check_admin_rules(self, email: Dict[str, Any], rules: List[Dict[str, Any]]) -> List[ProcessingAction]:
        """Apply admin-defined rules to email"""
        actions = []
        
        for rule in rules:
            try:
                if self._rule_matches(email, rule):
                    action = ProcessingAction(
                        action_type=rule['action'],
                        rule_applied=f"Admin Rule {rule['id']}",
                        reason=f"Rule type: {rule['rule_type']}",
                        details=rule['conditions']
                    )
                    actions.append(action)
                    logger.debug(f"Email {email.get('id')} matched rule {rule['id']}")
                    
            except Exception as e:
                logger.warning(f"Error applying rule {rule['id']}: {e}")
        
        return actions
    
    def _rule_matches(self, email: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Check if email matches admin rule conditions"""
        conditions = rule['conditions']
        rule_type = rule['rule_type']
        
        # Handle different rule types
        if rule_type == 'sender_domain':
            sender = email.get('sender', '').lower()
            domains = conditions.get('domains', [])
            return any(domain.lower() in sender for domain in domains)
        
        elif rule_type == 'sender_exact':
            sender = email.get('sender', '').lower()
            senders = conditions.get('senders', [])
            return sender in [s.lower() for s in senders]
        
        elif rule_type == 'subject_contains':
            subject = email.get('subject', '').lower()
            keywords = conditions.get('keywords', [])
            return any(keyword.lower() in subject for keyword in keywords)
        
        elif rule_type == 'department':
            department = email.get('department', '').lower()
            departments = conditions.get('departments', [])
            return department in [d.lower() for d in departments]
        
        elif rule_type == 'attachment_type':
            attachments = email.get('attachments', '')
            if not attachments:
                return False
            extensions = conditions.get('extensions', [])
            return any(ext.lower() in attachments.lower() for ext in extensions)
        
        elif rule_type == 'attachment_whitelist':
            # Check if ALL attachments are whitelisted
            attachments = email.get('attachments', '')
            if not attachments or attachments.strip() == '-':
                return True  # No attachments = safe
            
            whitelisted_extensions = conditions.get('extensions', [])
            attachment_list = [att.strip() for att in attachments.split(';') if att.strip()]
            
            # Check if all attachments have whitelisted extensions
            for attachment in attachment_list:
                if not any(ext.lower() in attachment.lower() for ext in whitelisted_extensions):
                    return False  # Found non-whitelisted attachment
            return True  # All attachments are whitelisted
        
        elif rule_type == 'recipient_domain':
            recipients = email.get('recipients', '')
            if not recipients:
                return False
            domains = conditions.get('domains', [])
            # Check if ANY recipient matches the domain
            return any(domain.lower() in recipients.lower() for domain in domains)
        
        elif rule_type == 'keyword_match':
            # Search across multiple fields
            search_text = ' '.join([
                email.get('subject', ''),
                email.get('sender', ''),
                email.get('justifications', ''),
                email.get('attachments', '')
            ]).lower()
            
            keywords = conditions.get('keywords', [])
            return any(keyword.lower() in search_text for keyword in keywords)
        
        elif rule_type == 'text':
            # Simple text matching for legacy rules
            text = conditions.get('text', '')
            search_text = ' '.join([
                email.get('subject', ''),
                email.get('sender', ''),
                email.get('justifications', '')
            ]).lower()
            return text.lower() in search_text
        
        return False
    
    def check_whitelist(self, email: Dict[str, Any]) -> Optional[ProcessingAction]:
        """Check if email should be whitelisted (excluded from monitoring)"""
        sender = (email.get('sender') or '').lower()
        
        # Common whitelisted domains
        whitelist_domains = [
            'company.com',  # Internal company domain
            'noreply.com', 'no-reply.com', 'donotreply.com',
            'notification.com', 'alerts.com', 'system.com'
        ]
        
        for domain in whitelist_domains:
            if domain in sender:
                return ProcessingAction(
                    action_type='whitelist',
                    reason=f'Sender from whitelisted domain: {domain}',
                    confidence=0.9
                )
        
        # Check for system/automated emails
        subject = (email.get('subject') or '').lower()
        automated_indicators = [
            'automated', 'no-reply', 'system notification',
            'out of office', 'auto-reply', 'delivery status'
        ]
        
        for indicator in automated_indicators:
            if indicator in subject or indicator in sender:
                return ProcessingAction(
                    action_type='whitelist',
                    reason=f'Automated email detected: {indicator}',
                    confidence=0.8
                )
        
        return None
    
    def analyze_security_risk(self, email: Dict[str, Any]) -> Tuple[RiskLevel, List[ProcessingAction]]:
        """Analyze email for security risks using keyword matching"""
        actions = []
        risk_score = 0
        risk_factors = []
        
        # Combine relevant text fields for analysis
        text_to_analyze = ' '.join([
            email.get('subject') or '',
            email.get('justifications') or '',
            email.get('attachments') or '',
            email.get('policy_name') or ''
        ])
        
        # Check for exclusion keywords (reduces risk)
        exclusion_matches = self.exclusion_pattern.findall(text_to_analyze)
        if exclusion_matches:
            actions.append(ProcessingAction(
                action_type='exclude_keywords_found',
                reason=f'Found exclusion keywords: {", ".join(set(exclusion_matches))}',
                confidence=0.7
            ))
            risk_score -= len(exclusion_matches) * 10
        
        # Check for risk keywords (increases risk)
        risk_matches = self.risk_pattern.findall(text_to_analyze)
        if risk_matches:
            unique_risks = list(set([match.lower() for match in risk_matches]))
            risk_factors.extend(unique_risks)
            risk_score += len(unique_risks) * 20
            
            actions.append(ProcessingAction(
                action_type='risk_keywords_found',
                reason=f'Found risk keywords: {", ".join(unique_risks)}',
                confidence=0.8,
                details={'keywords': unique_risks}
            ))
        
        # Additional risk factors
        
        # Sender domain analysis (focus on suspicious patterns since all are external)
        sender = email.get('sender') or ''
        if sender:
            # Check for personal email domains from company users
            personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
            if any(domain in sender.lower() for domain in personal_domains):
                risk_score += 15
                risk_factors.append('personal_email_domain')
            
            # Check for government/education domains (typically lower risk)
            if sender.endswith('.gov') or sender.endswith('.edu'):
                risk_score -= 5  # Reduce risk for trusted domains
        
        # Attachment risk - Modify this list to change what's considered risky
        attachments = email.get('attachments') or ''
        if attachments:
            risky_extensions = ['.exe', '.zip', '.rar', '.bat', '.scr', '.com']
            # Add or remove extensions as needed:
            # risky_extensions = ['.exe', '.bat', '.scr', '.com']  # More permissive (allows .zip, .rar)
            # risky_extensions.extend(['.doc', '.docx', '.pdf'])  # More strict (includes documents)
            
            if any(ext in attachments.lower() for ext in risky_extensions):
                risk_score += 30  # Lower this number to be less strict about risky attachments
                risk_factors.append('risky_attachments')
            elif attachments.strip() != '-':  # Has attachments but not risky
                risk_score += 5  # Lower this to be less strict about any attachments
                risk_factors.append('has_attachments')
        
        # Policy violation indicators
        if email.get('final_outcome') == 'Escalated':
            risk_score += 25
            risk_factors.append('previously_escalated')
        
        # User response indicates concern
        user_response = (email.get('user_response') or '').lower()
        if any(word in user_response for word in ['flagged', 'concern', 'suspicious', 'violation']):
            risk_score += 15
            risk_factors.append('user_concern')
        
        # Determine risk level based on score
        # You can adjust these thresholds to make clearing more or less strict
        if risk_score >= 60:  # Increase this to make CRITICAL harder to reach
            risk_level = RiskLevel.CRITICAL
        elif risk_score >= 40:  # Increase this to make HIGH harder to reach
            risk_level = RiskLevel.HIGH
        elif risk_score >= 20:  # Increase this to make MEDIUM harder to reach
            risk_level = RiskLevel.MEDIUM
        elif risk_score >= 0:
            risk_level = RiskLevel.LOW
        else:
            risk_level = RiskLevel.LOW  # Exclusion keywords can make score negative
        
        # Add risk assessment action
        actions.append(ProcessingAction(
            action_type='risk_analysis',
            reason=f'Risk score: {risk_score}, Factors: {", ".join(risk_factors)}',
            confidence=0.85,
            details={
                'score': risk_score,
                'factors': risk_factors,
                'level': risk_level.value
            }
        ))
        
        return risk_level, actions
    
    def run_ml_classification(self, email: Dict[str, Any]) -> Tuple[Optional[str], List[ProcessingAction]]:
        """Run ML classification on email"""
        actions = []
        
        try:
            # Prepare text for ML analysis
            email_text = f"{email.get('subject') or ''} {email.get('justifications') or ''}"
            
            if not email_text.strip():
                return None, actions
            
            # Run classification
            classification = classify_email(email_text)
            
            actions.append(ProcessingAction(
                action_type='ml_classification',
                reason=f'ML classified as: {classification}',
                confidence=0.75,
                details={'classification': classification, 'text_length': len(email_text)}
            ))
            
            logger.debug(f"Email {email.get('id')} ML classification: {classification}")
            return classification, actions
            
        except Exception as e:
            logger.warning(f"ML classification failed for email {email.get('id')}: {e}")
            actions.append(ProcessingAction(
                action_type='ml_classification_failed',
                reason=f'ML classification error: {str(e)}',
                confidence=0.0
            ))
            return None, actions
    
    def determine_final_status(self, email: Dict[str, Any], admin_actions: List[ProcessingAction], 
                              risk_level: RiskLevel, ml_classification: Optional[str]) -> ProcessingResult:
        """Determine final processing status based on all analysis results"""
        
        # Check for explicit admin rule actions first
        for action in admin_actions:
            if action.action_type == 'exclude':
                return ProcessingResult.EXCLUDED
            elif action.action_type == 'whitelist':
                return ProcessingResult.WHITELISTED
            elif action.action_type == 'escalate':
                return ProcessingResult.ESCALATED
        
        # Check whitelist actions
        if any(action.action_type == 'whitelist' for action in admin_actions):
            return ProcessingResult.WHITELISTED
        
        # Risk-based routing - Modify these rules to change when emails are cleared
        if risk_level == RiskLevel.CRITICAL:
            return ProcessingResult.ESCALATED
        elif risk_level == RiskLevel.HIGH:
            # High risk emails need review unless ML says they're safe
            if ml_classification and ml_classification.lower() in ['low_risk', 'safe', 'clear', 'cleared', 'approved']:
                return ProcessingResult.CLEARED  # Change to CLEARED if you want more clearing
            else:
                return ProcessingResult.ESCALATED
        elif risk_level == RiskLevel.MEDIUM:
            # Medium risk - depends on ML classification
            if ml_classification and ml_classification.lower() in ['high_risk', 'critical']:
                return ProcessingResult.ESCALATED
            else:
                return ProcessingResult.CLEARED  # Change to CLEARED for more automatic clearing
        else:
            # Low risk emails are generally cleared
            if ml_classification and ml_classification.lower() in ['high_risk', 'critical']:
                return ProcessingResult.PENDING_REVIEW
            else:
                return ProcessingResult.CLEARED
    
    def create_case_if_needed(self, email_id: int, result: EmailProcessingResult) -> Optional[int]:
        """Create a case if email needs escalation"""
        if result.final_status not in [ProcessingResult.ESCALATED, ProcessingResult.PENDING_REVIEW]:
            return None
        
        try:
            conn = get_db_connection()
            
            # Check if case already exists for this email
            existing = conn.execute(
                "SELECT id FROM cases WHERE email_id = ?", [email_id]
            ).fetchone()
            
            if existing:
                logger.debug(f"Case already exists for email {email_id}")
                return existing[0]
            
            # Create escalation reason from processing actions
            reasons = []
            for action in result.actions_taken:
                if action.reason:
                    reasons.append(f"{action.action_type}: {action.reason}")
            
            escalation_reason = f"Risk Level: {result.risk_level.value.title()}. " + \
                               ". ".join(reasons[:3])  # Limit to first 3 reasons
            
            # Insert new case
            conn.execute("""
                INSERT INTO cases (email_id, escalation_reason, status) 
                VALUES (?, ?, ?)
            """, [email_id, escalation_reason, 'open'])
            
            # Get the case ID that was just inserted
            case_id = conn.execute("SELECT currval('case_id_seq')").fetchone()[0]
            
            conn.close()
            
            logger.info(f"Created case {case_id} for email {email_id}")
            return case_id
            
        except Exception as e:
            logger.error(f"Error creating case for email {email_id}: {e}")
            return None
    
    def update_email_status(self, email_id: int, result: EmailProcessingResult) -> bool:
        """Update email record with processing results"""
        try:
            conn = get_db_connection()
            
            # Prepare processing notes
            notes = f"Status: {result.final_status.value}, Risk: {result.risk_level.value}"
            if result.ml_classification:
                notes += f", ML: {result.ml_classification}"
            
            # Update email record
            conn.execute("""
                UPDATE emails 
                SET final_outcome = ?, user_response = ? 
                WHERE id = ?
            """, [result.final_status.value, notes, email_id])
            
            conn.close()
            
            logger.debug(f"Updated email {email_id} status to {result.final_status.value}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating email {email_id}: {e}")
            return False
    
    def process_email(self, email: Dict[str, Any]) -> EmailProcessingResult:
        """Process a single email through all analysis stages"""
        email_id = email['id']
        all_actions = []
        
        logger.info(f"Processing email {email_id} from {email.get('sender', 'unknown')}")
        
        # 1. Load admin rules and check them
        admin_rules = self.load_admin_rules()
        admin_actions = self.check_admin_rules(email, admin_rules)
        all_actions.extend(admin_actions)
        
        # 2. Check whitelist
        whitelist_action = self.check_whitelist(email)
        if whitelist_action:
            all_actions.append(whitelist_action)
        
        # 3. Security risk analysis
        risk_level, security_actions = self.analyze_security_risk(email)
        all_actions.extend(security_actions)
        
        # 4. ML classification
        ml_classification, ml_actions = self.run_ml_classification(email)
        all_actions.extend(ml_actions)
        
        # 5. Determine final status
        final_status = self.determine_final_status(email, all_actions, risk_level, ml_classification)
        
        # Create result
        result = EmailProcessingResult(
            email_id=email_id,
            final_status=final_status,
            risk_level=risk_level,
            actions_taken=all_actions,
            ml_classification=ml_classification,
            processing_notes=f"Processed {len(all_actions)} actions"
        )
        
        # Log processing summary
        logger.info(f"Email {email_id} processed: Status={final_status.value}, Risk={risk_level.value}, Actions={len(all_actions)}")
        
        return result
    
    def process_batch(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Process a batch of emails"""
        logger.info(f"Starting batch processing: limit={limit}, offset={offset}")
        
        try:
            conn = get_db_connection()
            
            # Get emails to process (those not already processed or needing reprocessing)
            emails = conn.execute("""
                SELECT * FROM emails 
                WHERE final_outcome IS NULL OR final_outcome = 'Pending' OR final_outcome = '' OR final_outcome = '-'
                ORDER BY _time DESC 
                LIMIT ? OFFSET ?
            """, [limit, offset]).fetchall()
            
            conn.close()
            
            if not emails:
                logger.info("No emails found to process")
                return {
                    'processed': 0,
                    'escalated': 0,
                    'cleared': 0,
                    'excluded': 0,
                    'whitelisted': 0,
                    'pending_review': 0,
                    'errors': []
                }
            
            # Convert to dictionaries
            email_dicts = []
            for email in emails:
                email_dict = {
                    'id': email[0], '_time': email[1], 'sender': email[2],
                    'subject': email[3], 'attachments': email[4], 'recipients': email[5],
                    'time_month': email[6], 'leaver': email[7], 'termination_date': email[8],
                    'bunit': email[9], 'department': email[10], 'user_response': email[11],
                    'final_outcome': email[12], 'policy_name': email[13], 'justifications': email[14]
                }
                email_dicts.append(email_dict)
            
            # Process each email
            results = {
                'processed': 0,
                'escalated': 0,
                'cleared': 0,
                'excluded': 0,
                'whitelisted': 0,
                'pending_review': 0,
                'errors': []
            }
            
            for email_dict in email_dicts:
                try:
                    # Process the email
                    result = self.process_email(email_dict)
                    
                    # Update database
                    self.update_email_status(email_dict['id'], result)
                    
                    # Create case if needed
                    if result.final_status in [ProcessingResult.ESCALATED, ProcessingResult.PENDING_REVIEW]:
                        self.create_case_if_needed(email_dict['id'], result)
                    
                    # Update counters
                    results['processed'] += 1
                    if result.final_status == ProcessingResult.ESCALATED:
                        results['escalated'] += 1
                    elif result.final_status == ProcessingResult.CLEARED:
                        results['cleared'] += 1
                    elif result.final_status == ProcessingResult.EXCLUDED:
                        results['excluded'] += 1
                    elif result.final_status == ProcessingResult.WHITELISTED:
                        results['whitelisted'] += 1
                    elif result.final_status == ProcessingResult.PENDING_REVIEW:
                        results['pending_review'] += 1
                    
                except Exception as e:
                    error_msg = f"Error processing email {email_dict['id']}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            logger.info(f"Batch processing complete: {results}")
            return results
            
        except Exception as e:
            error_msg = f"Batch processing failed: {e}"
            logger.error(error_msg)
            return {
                'processed': 0,
                'escalated': 0,
                'cleared': 0,
                'excluded': 0,
                'whitelisted': 0,
                'pending_review': 0,
                'errors': [error_msg]
            }


def main():
    """Command-line interface for email processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process emails through Email Guardian analysis pipeline')
    parser.add_argument('--limit', type=int, default=100, help='Number of emails to process')
    parser.add_argument('--offset', type=int, default=0, help='Offset for batch processing')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize processor
    processor = EmailProcessor()
    
    # Process batch
    results = processor.process_batch(limit=args.limit, offset=args.offset)
    
    # Print summary
    print(f"\n📧 Email Processing Complete!")
    print(f"Processed: {results['processed']}")
    print(f"Escalated: {results['escalated']}")
    print(f"Cleared: {results['cleared']}")
    print(f"Excluded: {results['excluded']}")
    print(f"Whitelisted: {results['whitelisted']}")
    print(f"Pending Review: {results['pending_review']}")
    
    if results['errors']:
        print(f"\n⚠️  Errors:")
        for error in results['errors']:
            print(f"  - {error}")


if __name__ == "__main__":
    main()