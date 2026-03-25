"""
Arkitekt OpenCRM - Email Sender Module

This module orchestrates sending email notifications for leads that have been
ingested into the database. It queries for leads with status='intake', generates
personalized emails, sends them via Microsoft Graph API, and updates lead status.

Features:
- Query leads with status='intake' and valid email addresses
- Generate personalized emails using templates
- Send emails via Microsoft Graph API
- Update lead status to 'emailed' with timestamp
- Retry logic for failed sends
- Duplicate email prevention
- Email sending statistics and reporting
- Graceful error handling

Workflow:
    1. Query database for leads with status='intake'
    2. Filter leads with valid email addresses
    3. For each lead:
       - Generate personalized email using templates
       - Send email via email client
       - Update lead status to 'emailed' on success
       - Update status to 'failed' on error
       - Log results
    4. Return sending statistics

Usage:
    from email_sender import EmailSender
    
    # Initialize sender
    sender = EmailSender()
    
    # Send emails to intake leads
    stats = sender.send_pending_emails()
    
    # Get statistics
    print(f"Sent: {stats['sent_count']}, Failed: {stats['failed_count']}")

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

from email_client import EmailClient, EmailError
from database import Database, DatabaseError, get_database
from templates import generate_lead_email, EMAIL_SUBJECT, get_email_subject
from config import config


# Configure module logger
logger = logging.getLogger(__name__)


class EmailSenderError(Exception):
    """Custom exception for email sender errors."""
    pass


class SendingStats:
    """
    Container for email sending statistics.
    
    Attributes:
        total_candidates: Total leads eligible for email
        sent_count: Number of emails successfully sent
        failed_count: Number of failed email sends
        skipped_count: Number of leads skipped (invalid email, etc.)
        sent_lead_ids: List of lead IDs successfully emailed
        failed_lead_ids: List of lead IDs that failed
        error_details: List of error details for failures
    """
    
    def __init__(self):
        """Initialize sending statistics."""
        self.total_candidates: int = 0
        self.sent_count: int = 0
        self.failed_count: int = 0
        self.skipped_count: int = 0
        self.sent_lead_ids: List[str] = []
        self.failed_lead_ids: List[str] = []
        self.error_details: List[Dict[str, Any]] = []
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert statistics to dictionary.
        
        Returns:
            Dictionary representation of statistics
        """
        return {
            'total_candidates': self.total_candidates,
            'sent_count': self.sent_count,
            'failed_count': self.failed_count,
            'skipped_count': self.skipped_count,
            'success_rate': f"{(self.sent_count / max(self.total_candidates, 1)) * 100:.1f}%",
            'sent_lead_ids': self.sent_lead_ids,
            'failed_lead_ids': self.failed_lead_ids,
            'error_count': len(self.error_details)
        }
    
    def __str__(self) -> str:
        """String representation of statistics."""
        return (
            f"EmailSendingStats(candidates={self.total_candidates}, "
            f"sent={self.sent_count}, failed={self.failed_count}, "
            f"skipped={self.skipped_count})"
        )


class EmailSender:
    """
    Email sender that orchestrates sending notifications to leads.
    
    This class coordinates between the database, email client, and
    template generator to send personalized lead notification emails.
    
    Attributes:
        email_client: Email client instance
        database: Database instance
    """
    
    def __init__(
        self,
        email_client: Optional[EmailClient] = None,
        database: Optional[Database] = None
    ):
        """
        Initialize email sender with clients.
        
        Args:
            email_client: Optional email client (creates new if None)
            database: Optional database instance (uses singleton if None)
            
        Raises:
            EmailSenderError: If initialization fails
        """
        try:
            self.email_client = email_client or EmailClient()
            self.database = database or get_database()
            
            logger.info("Email sender initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize email sender: {e}")
            raise EmailSenderError(f"Initialization failed: {e}") from e
    
    def send_pending_emails(
        self,
        limit: Optional[int] = None,
        retry_failed: bool = False
    ) -> Dict[str, Any]:
        """
        Send emails to leads with status='intake'.
        
        This is the main entry point for email sending. It queries the database
        for leads ready to be emailed, generates personalized content, and
        sends emails.
        
        Args:
            limit: Optional limit on number of emails to send
            retry_failed: Whether to also retry previously failed sends
            
        Returns:
            Dictionary with sending statistics
            
        Raises:
            EmailSenderError: If sending fails critically
        """
        stats = SendingStats()
        
        try:
            logger.info("="*80)
            logger.info("Starting email sending workflow")
            logger.info(f"Limit: {limit or 'No limit'}")
            logger.info(f"Retry failed: {retry_failed}")
            logger.info("="*80)
            
            # Get leads ready to be emailed
            leads = self._get_leads_to_email(limit, retry_failed)
            stats.total_candidates = len(leads)
            
            logger.info(f"Found {stats.total_candidates} leads to email")
            
            if not leads:
                logger.info("No leads found to email")
                return stats.to_dict()
            
            # Send email to each lead
            for lead in leads:
                try:
                    # Send email for this lead
                    result = self._send_lead_email(lead)
                    
                    if result == 'sent':
                        stats.sent_count += 1
                        stats.sent_lead_ids.append(lead.get('lead_id', 'unknown'))
                    elif result == 'skipped':
                        stats.skipped_count += 1
                    else:  # 'failed'
                        stats.failed_count += 1
                        stats.failed_lead_ids.append(lead.get('lead_id', 'unknown'))
                        
                except Exception as e:
                    stats.failed_count += 1
                    lead_id = lead.get('lead_id', 'unknown')
                    error_detail = {
                        'lead_id': lead_id,
                        'error': str(e),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    stats.error_details.append(error_detail)
                    logger.error(f"Failed to send email for lead {lead_id}: {e}", exc_info=True)
                    
                    # Update lead status to failed
                    try:
                        self.database.update_lead_status(lead_id, 'failed')
                    except:
                        pass
                    
                    # Continue with other leads
                    continue
            
            # Log summary
            logger.info("="*80)
            logger.info("Email sending completed")
            logger.info(f"Total candidates: {stats.total_candidates}")
            logger.info(f"Sent: {stats.sent_count}")
            logger.info(f"Failed: {stats.failed_count}")
            logger.info(f"Skipped: {stats.skipped_count}")
            logger.info("="*80)
            
            return stats.to_dict()
            
        except DatabaseError as e:
            logger.error(f"Database error during email sending: {e}")
            raise EmailSenderError(f"Database operation failed: {e}") from e
            
        except EmailError as e:
            logger.error(f"Email client error during sending: {e}")
            raise EmailSenderError(f"Email operation failed: {e}") from e
            
        except Exception as e:
            logger.error(f"Unexpected error during email sending: {e}", exc_info=True)
            raise EmailSenderError(f"Email sending failed: {e}") from e
    
    def _get_leads_to_email(
        self,
        limit: Optional[int] = None,
        retry_failed: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get leads that need emails sent.
        
        Args:
            limit: Optional limit on number of leads
            retry_failed: Whether to include previously failed leads
            
        Returns:
            List of lead dictionaries
        """
        try:
            # Get leads with 'intake' status
            leads = self.database.get_leads_by_status('intake', limit=limit)

            # Optionally add failed leads for retry
            if retry_failed:
                failed_leads = self.database.get_leads_by_status('failed', limit=limit)
                leads.extend(failed_leads)
                logger.info(f"Including {len(failed_leads)} failed leads for retry")

            # Exclude manual leads — they use AI-generated emails from the dashboard
            leads = [l for l in leads if l.get('lead_source', 'meta') != 'manual']

            logger.debug(f"Retrieved {len(leads)} leads to email")
            return leads
            
        except Exception as e:
            logger.error(f"Failed to retrieve leads: {e}")
            raise
    
    def _send_lead_email(self, lead_data: Dict[str, Any]) -> str:
        """
        Send email for a single lead.
        
        Args:
            lead_data: Lead data from database
            
        Returns:
            str: 'sent' if successful, 'skipped' if skipped, 'failed' if error
        """
        lead_id = lead_data.get('lead_id')
        
        if not lead_id:
            logger.error("Lead has no ID, skipping")
            return 'skipped'
        
        # Validate email address
        email = lead_data.get('email', '').strip()
        if not email or not self._is_valid_email(email):
            logger.warning(f"Lead {lead_id} has invalid email '{email}', skipping")
            self.database.update_lead_status(lead_id, 'skipped_invalid_email')
            return 'skipped'
        
        # Check if email was already sent (prevent duplicates)
        if lead_data.get('email_sent_at'):
            logger.debug(f"Email already sent for lead {lead_id}, skipping")
            return 'skipped'
        
        try:
            # Generate personalized email content
            html_body = self._generate_email_content(lead_data)
            
            # Generate subject line
            first_name = lead_data.get('first_name', '')
            subject = get_email_subject('with_name' if first_name else 'default', first_name)
            
            # Send directly to the lead
            recipients = [email]

            # Send email
            logger.info(f"Sending email for lead {lead_id} to {', '.join(recipients)}")
            
            success = self.email_client.send_email(
                recipients=recipients,
                subject=subject,
                body=html_body,
                body_type='html',
                importance=config.email_priority,
                save_to_sent_items=True
            )
            
            if success:
                # Update lead status to 'emailed' with timestamp
                timestamp = datetime.utcnow().isoformat()
                self.database.update_lead_status(lead_id, 'emailed', email_sent_at=timestamp)
                
                logger.info(
                    f"Email sent successfully for lead {lead_id} - "
                    f"{email} ({lead_data.get('full_name', 'Unknown')})"
                )
                return 'sent'
            else:
                logger.error(f"Email send returned False for lead {lead_id}")
                self.database.update_lead_status(lead_id, 'failed')
                return 'failed'
                
        except EmailError as e:
            logger.error(f"Email error for lead {lead_id}: {e}")
            self.database.update_lead_status(lead_id, 'failed')
            return 'failed'
            
        except Exception as e:
            logger.error(f"Unexpected error sending email for lead {lead_id}: {e}")
            self.database.update_lead_status(lead_id, 'failed')
            return 'failed'
    
    def _generate_email_content(self, lead_data: Dict[str, Any]) -> str:
        """
        Generate personalized email HTML content.
        
        Args:
            lead_data: Lead data from database
            
        Returns:
            HTML email content
        """
        try:
            # Extract lead fields
            html_body = generate_lead_email(
                first_name=lead_data.get('first_name'),
                full_name=lead_data.get('full_name'),
                email=lead_data.get('email'),
                phone=lead_data.get('phone_number'),
                company=lead_data.get('company_name'),
                job_title=lead_data.get('job_title'),
                created_time=lead_data.get('created_time'),
                lead_id=lead_data.get('lead_id')
            )
            
            logger.debug(f"Generated email content for lead {lead_data.get('lead_id')}")
            return html_body
            
        except Exception as e:
            logger.error(f"Failed to generate email content: {e}")
            raise
    
    def _is_valid_email(self, email: str) -> bool:
        """
        Validate email address format.
        
        Args:
            email: Email address to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not email:
            return False
        
        # Basic email regex pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        return bool(re.match(pattern, email))
    
    def process_intake_leads(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Process and send emails to leads with status='intake'.

        This is a convenience method that wraps send_pending_emails and returns
        results with keys expected by the main pipeline.

        Args:
            limit: Optional limit on number of emails to send

        Returns:
            Dictionary with 'emails_sent' and 'failures' keys
        """
        stats = self.send_pending_emails(limit=limit)
        return {
            'emails_sent': stats.get('sent_count', 0),
            'failures': stats.get('failed_count', 0),
            'skipped': stats.get('skipped_count', 0),
            'total_candidates': stats.get('total_candidates', 0)
        }

    def retry_failed_emails(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Retry sending emails to leads with 'failed' status.

        Args:
            limit: Optional limit on number of retries

        Returns:
            Dictionary with sending statistics
        """
        logger.info("Starting retry of failed email sends")
        return self.send_pending_emails(limit=limit, retry_failed=True)
    
    def get_sending_summary(self) -> Dict[str, Any]:
        """
        Get summary of email sending status from database.
        
        Returns:
            Dictionary with status breakdown
        """
        try:
            stats = self.database.get_stats()
            
            # Calculate email-specific stats
            status_breakdown = stats.get('status_breakdown', {})
            
            summary = {
                'total_leads': stats.get('total_leads', 0),
                'pending_email': status_breakdown.get('intake', 0),
                'emailed': status_breakdown.get('emailed', 0),
                'failed': status_breakdown.get('failed', 0),
                'skipped': status_breakdown.get('skipped_invalid_email', 0),
                'status_breakdown': status_breakdown
            }
            
            logger.debug(f"Email sending summary: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get sending summary: {e}")
            return {'error': str(e)}


if __name__ == '__main__':
    """
    Test email sender when run as script.
    
    Usage:
        python email_sender.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        print("\n" + "="*80)
        print("Email Sender - Test")
        print("="*80 + "\n")
        
        # Initialize sender
        print("[TEST] Initializing email sender...")
        sender = EmailSender()
        print("[SUCCESS] Email sender initialized\n")
        
        # Get current summary
        print("[TEST] Getting email sending summary...")
        summary = sender.get_sending_summary()
        print("[SUMMARY] Current status:")
        for key, value in summary.items():
            if key != 'status_breakdown':
                print(f"  {key}: {value}")
        print()
        
        # Send pending emails (limit to 3 for testing)
        print("[TEST] Sending pending emails (limit: 3)...")
        stats = sender.send_pending_emails(limit=3)
        print("\n[RESULTS] Sending statistics:")
        for key, value in stats.items():
            if key not in ['sent_lead_ids', 'failed_lead_ids']:
                print(f"  {key}: {value}")
        print()
        
        print("="*80)
        print("[SUCCESS] All tests completed!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
