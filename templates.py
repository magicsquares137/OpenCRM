"""
Arkitekt OpenCRM - Email Templates Module

This module provides HTML email templates for lead notifications.
It includes template functions with variable substitution and formatting
to create professional, branded email content.

Features:
- Professional HTML email templates
- Variable substitution with fallbacks
- Responsive design for mobile and desktop
- Configurable branding via SENDER_NAME, COMPANY_NAME, BOOKING_URL
- Call-to-action buttons
- Personalized greetings
- Helper functions for template processing

Usage:
    from templates import generate_lead_email, EMAIL_SUBJECT
    
    # Generate email body
    html_body = generate_lead_email(
        first_name='John',
        full_name='John Doe',
        email='john@example.com',
        phone='+1234567890',
        company='Acme Corp',
        job_title='CEO'
    )
    
    # Use email subject
    subject = EMAIL_SUBJECT

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

from typing import Dict, Any, Optional
import logging
from datetime import datetime

from config import config


# Configure module logger
logger = logging.getLogger(__name__)


# Email subject constant
EMAIL_SUBJECT = "Thanks for reaching out"

# Alternative subject templates for different scenarios
SUBJECT_TEMPLATES = {
    'default': "Thanks for reaching out",
    'with_name': "Thanks for reaching out, {first_name}",
    'follow_up': "Following up on your inquiry",
}

def generate_lead_email(
    first_name: Optional[str] = None,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    company: Optional[str] = None,
    job_title: Optional[str] = None,
    created_time: Optional[str] = None,
    lead_id: Optional[str] = None,
    additional_fields: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a simple, human-sounding HTML email sent to the lead.

    Kept intentionally minimal so it reads like a real person wrote it
    rather than an automated marketing blast.

    Uses SENDER_NAME, COMPANY_NAME, and BOOKING_URL from config.
    """
    greeting_name = first_name if first_name else 'there'
    sender_name = config.sender_name or 'The Team'
    company_name = config.company_name
    booking_url = config.booking_url

    # Build signature
    signature = sender_name
    if company_name:
        signature += f'<br>\n{company_name}'

    # Build booking link block
    booking_block = ''
    if booking_url:
        booking_block = f"""
<p>Once I have a better idea of your needs we can set up a quick scoping call to talk through it:</p>

<p><a href="{booking_url}" style="color:#1a73e8;">{booking_url}</a></p>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; font-family: Arial, Helvetica, sans-serif; font-size:15px; line-height:1.6; color:#222;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fff;">
<tr><td style="padding:30px 20px; max-width:560px;">

<p>Hi {greeting_name},</p>

<p>Thanks for reaching out through our ad -- I appreciate you taking the time.</p>

<p>I'd love to learn a bit more about what you're looking for and how we can help.</p>
{booking_block}
<p>Looking forward to hearing from you.</p>

<p>
{signature}
</p>

</td></tr>
</table>
</body>
</html>"""

    logger.debug(f"Generated email template for lead: {greeting_name}")
    return html


def generate_simple_text_email(
    first_name: Optional[str] = None,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    company: Optional[str] = None,
    job_title: Optional[str] = None
) -> str:
    """Generate plain text fallback email."""
    greeting_name = first_name if first_name else 'there'
    sender_name = config.sender_name or 'The Team'
    company_name = config.company_name
    booking_url = config.booking_url

    # Build signature
    signature = sender_name
    if company_name:
        signature += f'\n{company_name}'

    # Build booking block
    booking_block = ''
    if booking_url:
        booking_block = f"""
Once I have a better idea of your needs we can set up a quick scoping call to talk through it:
{booking_url}
"""

    text = f"""Hi {greeting_name},

Thanks for reaching out through our ad -- I appreciate you taking the time.

I'd love to learn a bit more about what you're looking for and how we can help.
{booking_block}
Looking forward to hearing from you.

{signature}"""

    return text.strip()


def substitute_variables(template: str, variables: Dict[str, str]) -> str:
    """
    Substitute variables in template string.
    
    Args:
        template: Template string with {variable} placeholders
        variables: Dictionary of variable names and values
        
    Returns:
        Template with variables substituted
    """
    result = template
    for key, value in variables.items():
        placeholder = f"{{{key}}}"
        result = result.replace(placeholder, str(value))
    return result


def get_email_subject(
    subject_type: str = 'default',
    first_name: Optional[str] = None
) -> str:
    """
    Get email subject based on type and personalization.
    
    Args:
        subject_type: Type of subject template to use
        first_name: Lead's first name for personalization
        
    Returns:
        Email subject line
    """
    template = SUBJECT_TEMPLATES.get(subject_type, SUBJECT_TEMPLATES['default'])
    
    if first_name and '{first_name}' in template:
        return template.format(first_name=first_name)
    
    return template


if __name__ == '__main__':
    """
    Test template generation when run as script.
    
    Usage:
        python templates.py
    """
    print("\n" + "=" * 80)
    print("Email Template Generator - Test")
    print("=" * 80 + "\n")
    
    # Sample lead data
    sample_lead = {
        'first_name': 'John',
        'full_name': 'John Doe',
        'email': 'john.doe@example.com',
        'phone': '+1 (555) 123-4567',
        'company': 'Acme Corporation',
        'job_title': 'Chief Technology Officer',
        'created_time': datetime.utcnow().isoformat(),
        'lead_id': 'test_lead_12345'
    }
    
    print("[TEST] Generating HTML email template...")
    html_email = generate_lead_email(**sample_lead)
    
    # Save to file for preview
    output_file = 'test_email_template.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_email)
    
    print(f"[SUCCESS] HTML email generated ({len(html_email)} characters)")
    print(f"[INFO] Saved to: {output_file}")
    print(f"[INFO] Open this file in a browser to preview the email\n")
    
    print("[TEST] Generating plain text email...")
    text_email = generate_simple_text_email(**sample_lead)
    print(f"[SUCCESS] Plain text email generated ({len(text_email)} characters)\n")
    
    print("[TEST] Email subjects:")
    for subject_type in SUBJECT_TEMPLATES.keys():
        subject = get_email_subject(subject_type, sample_lead.get('first_name'))
        print(f"  {subject_type:12} - {subject}")
    
    print("\n" + "=" * 80)
    print("[SUCCESS] All template tests completed!")
    print("=" * 80 + "\n")
