from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

@dataclass
class Email:
    """Email record model"""
    id: Optional[int] = None
    _time: Optional[datetime] = None
    sender: Optional[str] = None
    subject: Optional[str] = None
    attachments: Optional[str] = None
    recipients: Optional[str] = None
    time_month: Optional[str] = None
    leaver: Optional[str] = None
    termination_date: Optional[str] = None
    bunit: Optional[str] = None
    department: Optional[str] = None
    user_response: Optional[str] = None
    final_outcome: Optional[str] = None
    policy_name: Optional[str] = None
    justifications: Optional[str] = None
    created_at: Optional[datetime] = None

@dataclass
class Case:
    """Case record model"""
    id: Optional[int] = None
    email_id: Optional[int] = None
    escalation_reason: Optional[str] = None
    status: str = 'open'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class FlaggedSender:
    """Flagged sender model"""
    id: Optional[int] = None
    sender: Optional[str] = None
    reason: Optional[str] = None
    flagged_at: Optional[datetime] = None

@dataclass
class AdminRule:
    """Admin rule model"""
    id: Optional[int] = None
    rule_type: Optional[str] = None
    conditions: Optional[str] = None
    action: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
