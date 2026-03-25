#!/usr/bin/env python3
"""
Arkitekt OpenCRM - Manual Email Send Utility

This utility script allows manual sending of emails to leads for testing purposes.
It's useful for validating email templates, testing API connectivity, and
sending emails to specific leads without running the full pipeline.

Features:
- Fetch lead from database by lead_id
- Display lead information for review
- Generate and preview email content
- Prompt for confirmation before sending
- Send email via Microsoft Graph API
- Update database status after sending
- Dry-run mode for testing without sending

Usage:
    # Send email to a specific lead
    python scripts/manual_send.py --lead-id LEAD_ID_123
    
    # Preview email without sending
    python scripts/manual_send.py --lead-id LEAD_ID_123 --dry-run
    
    # Send to all 'intake' status leads
    python scripts/manual_send.py --status intake
    
    # Show lead information only
    python scripts/manual_send.py --lead-id LEAD_ID_123 --show-only

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config
from database import Database, get_database
from email_client import EmailClient, EmailError
from templates import generate_lead_email, EMAIL_SUBJECT


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_header(text: str):
    """Print formatted section header."""
    print("\n" + "=" * 80)
    print(text.center(80))
    print("=" * 80 + "\n")


def print_lead_info(lead: Dict[str, Any]):
    """
    Print formatted lead information.
    
    Args:
        lead: Lead dictionary from database
    """
    print("\nLead Information:")
    print("-" * 60)
    print(f"Lead ID:        {lead.get('lead_id', 'Unknown')}")
    print(f"Form ID:        {lead.get('form_id', 'N/A')}")
    print(f"Created:        {lead.get('created_time', 'Unknown')}")
    print(f"Status:         {lead.get('status', 'Unknown')}")
    print(f"")
    print(f"Name:           {lead.get('full_name', 'N/A')}")
    print(f"Email:          {lead.get('email', 'N/A')}")
    print(f"Phone:          {lead.get('phone_number', 'N/A')}")
    print(f"Company:        {lead.get('company_name', 'N/A')}")
    print(f"Job Title:      {lead.get('job_title', 'N/A')}")
    print(f"")
    print(f"Email Sent:     {lead.get('email_sent_at', 'Never')}")
    print(f"Inserted:       {lead.get('inserted_at', 'Unknown')}")
    print(f"Updated:        {lead.get('updated_at', 'Unknown')}")
    print("-" * 60)


def preview_email(lead: Dict[str, Any]) -> str:
    """
    Generate and display email preview.
    
    Args:
        lead: Lead dictionary from database
        
    Returns:
        Generated HTML email content
    """
    print("\nGenerating Email Preview...")
    print("-" * 60)
    
    # Generate HTML email
    html_content = generate_lead_email(
        first_name=lead.get('first_name'),
        full_name=lead.get('full_name'),
        email=lead.get('email'),
        phone=lead.get('phone_number'),
        company=lead.get('company_name'),
        job_title=lead.get('job_title'),
        created_time=lead.get('created_time'),
        lead_id=lead.get('lead_id')
    )
    
    # Generate subject
    subject = config.email_subject_template.format(
        name=lead.get('full_name', 'Lead'),
        email=lead.get('email', ''),
        phone=lead.get('phone_number', ''),
        date=lead.get('created_time', '')
    )
    
    print(f"Subject:        {subject}")
    print(f"From:           {config.ms_sender_email}")
    print(f"To:             {', '.join(config.ms_recipient_emails)}")
    print(f"Content Type:   HTML")
    print(f"Content Length: {len(html_content)} characters")
    print("-" * 60)
    
    # Offer to save preview
    save_preview = input("\nSave email preview to file? (y/n): ").strip().lower()
    if save_preview == 'y':
        preview_file = f"email_preview_{lead.get('lead_id', 'unknown')}.html"
        try:
            with open(preview_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"✓ Preview saved to: {preview_file}")
        except Exception as e:
            print(f"✗ Failed to save preview: {e}")
    
    return html_content


def send_email_to_lead(
    lead: Dict[str, Any],
    email_client: EmailClient,
    database: Database,
    dry_run: bool = False
) -> bool:
    """
    Send email to a lead and update database.
    
    Args:
        lead: Lead dictionary from database
        email_client: Configured email client
        database: Database instance
        dry_run: If True, don't actually send email
        
    Returns:
        bool: True if successful
    """
    lead_id = lead.get('lead_id')
    
    try:
        # Generate email content
        html_content = generate_lead_email(
            first_name=lead.get('first_name'),
            full_name=lead.get('full_name'),
            email=lead.get('email'),
            phone=lead.get('phone_number'),
            company=lead.get('company_name'),
            job_title=lead.get('job_title'),
            created_time=lead.get('created_time'),
            lead_id=lead_id
        )
        
        # Generate subject
        subject = config.email_subject_template.format(
            name=lead.get('full_name', 'Lead'),
            email=lead.get('email', ''),
            phone=lead.get('phone_number', ''),
            date=lead.get('created_time', '')
        )
        
        if dry_run:
            print("\n[DRY RUN] Email would be sent with:")
            print(f"  Subject: {subject}")
            print(f"  From: {config.ms_sender_email}")
            print(f"  To: {', '.join(config.ms_recipient_emails)}")
            print(f"  Content: {len(html_content)} characters")
            print("\n✓ Dry run completed (no email sent)")
            return True
        
        # Send email
        print("\nSending email...")
        success = email_client.send_email(
            recipients=config.ms_recipient_emails,
            subject=subject,
            body=html_content,
            body_type='html',
            importance=config.email_priority,
            save_to_sent_items=True
        )
        
        if success:
            # Update database status
            timestamp = datetime.utcnow().isoformat()
            database.update_lead_status(lead_id, 'emailed', timestamp)
            
            print(f"✓ Email sent successfully!")
            print(f"✓ Database updated (status: 'emailed', timestamp: {timestamp})")
            return True
        else:
            print(f"✗ Email sending failed")
            return False
            
    except Exception as e:
        logger.error(f"Failed to send email to lead {lead_id}: {e}")
        print(f"\n✗ Error: {e}")
        return False


def confirm_action(message: str) -> bool:
    """
    Prompt user for confirmation.
    
    Args:
        message: Confirmation message
        
    Returns:
        bool: True if user confirms
    """
    response = input(f"\n{message} (y/n): ").strip().lower()
    return response == 'y'


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Manual Email Send Utility for Meta Leads Automation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send email to specific lead
  python scripts/manual_send.py --lead-id LEAD_123
  
  # Preview without sending
  python scripts/manual_send.py --lead-id LEAD_123 --dry-run
  
  # Send to all intake leads
  python scripts/manual_send.py --status intake
  
  # Show lead info only
  python scripts/manual_send.py --lead-id LEAD_123 --show-only
        """
    )
    
    parser.add_argument(
        '--lead-id',
        type=str,
        help='Lead ID to send email to'
    )
    
    parser.add_argument(
        '--status',
        type=str,
        choices=['intake', 'new', 'emailed', 'failed'],
        help='Send to all leads with this status'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview email without actually sending'
    )
    
    parser.add_argument(
        '--show-only',
        action='store_true',
        help='Show lead information only (no email)'
    )
    
    parser.add_argument(
        '--skip-confirmation',
        action='store_true',
        help='Skip confirmation prompts (use with caution)'
    )
    
    parser.add_argument(
        '--list-leads',
        action='store_true',
        help='List all leads in database'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print_header("META LEADS OUTLOOK AUTOMATION - MANUAL EMAIL SENDER")
    print(f"Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Sender: {config.ms_sender_email}")
    print(f"Recipients: {', '.join(config.ms_recipient_emails)}")
    
    if args.dry_run:
        print("\n⚠  DRY RUN MODE - No emails will be sent")
    
    try:
        # Initialize database
        print("\nInitializing database...")
        database = get_database()
        print(f"✓ Database connected: {config.db_path}")
        
        # List leads if requested
        if args.list_leads:
            print_header("ALL LEADS IN DATABASE")
            
            stats = database.get_stats()
            print(f"Total Leads: {stats.get('total_leads', 0)}")
            print(f"\nStatus Breakdown:")
            for status, count in stats.get('status_breakdown', {}).items():
                print(f"  {status}: {count}")
            
            # Show recent leads
            print(f"\nRecent Leads (last 10):")
            print("-" * 80)
            leads = database.get_all_leads(limit=10)
            
            if leads:
                for lead in leads:
                    print(f"ID: {lead['lead_id'][:20]:20}  "
                          f"Name: {lead.get('full_name', 'N/A')[:25]:25}  "
                          f"Email: {lead.get('email', 'N/A')[:30]:30}  "
                          f"Status: {lead.get('status', 'N/A')}")
            else:
                print("No leads found in database")
            
            print("-" * 80)
            return
        
        # Validate arguments
        if not args.lead_id and not args.status:
            print("\n✗ Error: Must specify either --lead-id or --status")
            print("   Use --help for usage information")
            sys.exit(1)
        
        # Fetch leads
        leads = []
        
        if args.lead_id:
            # Fetch specific lead
            print(f"\nFetching lead: {args.lead_id}")
            lead = database.get_lead_by_id(args.lead_id)
            
            if not lead:
                print(f"✗ Error: Lead not found with ID: {args.lead_id}")
                sys.exit(1)
            
            leads = [lead]
            print(f"✓ Lead found")
            
        elif args.status:
            # Fetch leads by status
            print(f"\nFetching leads with status: {args.status}")
            leads = database.get_leads_by_status(args.status)
            
            if not leads:
                print(f"✗ No leads found with status: {args.status}")
                sys.exit(1)
            
            print(f"✓ Found {len(leads)} lead(s)")
        
        # Initialize email client (unless show-only)
        email_client = None
        if not args.show_only:
            print("\nInitializing email client...")
            email_client = EmailClient()
            print(f"✓ Email client ready")
        
        # Process each lead
        sent_count = 0
        failed_count = 0
        
        for i, lead in enumerate(leads, 1):
            print(f"\n{'=' * 80}")
            print(f"Processing Lead {i} of {len(leads)}")
            print(f"{'=' * 80}")
            
            # Display lead information
            print_lead_info(lead)
            
            # Show-only mode
            if args.show_only:
                continue
            
            # Check if email already sent
            if lead.get('status') == 'emailed' and lead.get('email_sent_at'):
                print(f"\n⚠  Warning: Email already sent to this lead at {lead['email_sent_at']}")
                
                if not args.skip_confirmation:
                    if not confirm_action("Send email again?"):
                        print("Skipped")
                        continue
            
            # Preview email
            preview_email(lead)
            
            # Confirmation
            if not args.skip_confirmation and not args.dry_run:
                if not confirm_action("Send this email?"):
                    print("Skipped")
                    continue
            
            # Send email
            success = send_email_to_lead(lead, email_client, database, args.dry_run)
            
            if success:
                sent_count += 1
            else:
                failed_count += 1
            
            # Pause between multiple sends
            if len(leads) > 1 and i < len(leads):
                if not args.skip_confirmation:
                    input("\nPress Enter to continue to next lead...")
        
        # Summary
        print_header("SUMMARY")
        print(f"Total Leads Processed: {len(leads)}")
        print(f"✓ Sent Successfully:   {sent_count}")
        print(f"✗ Failed:              {failed_count}")
        
        if args.dry_run:
            print(f"\n⚠  DRY RUN - No emails were actually sent")
        
        print("\n" + "=" * 80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
