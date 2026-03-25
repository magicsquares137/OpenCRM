"""
Arkitekt OpenCRM - Email Client Module

This module handles sending emails via Microsoft Graph API using OAuth2
client credentials flow. It manages access token acquisition, caching,
and automatic refresh.

Features:
- OAuth2 client credentials flow for authentication
- Access token caching with automatic expiration tracking
- Token refresh before expiration (60 minutes buffer)
- HTML email composition and sending
- Multiple recipient support
- Retry logic with token refresh on 401 errors
- Comprehensive error handling and logging
- Save to Sent Items configuration

API Documentation:
- Microsoft Graph: https://docs.microsoft.com/en-us/graph/api/user-sendmail
- OAuth2: https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow

Usage:
    from email_client import EmailClient
    
    # Initialize client
    client = EmailClient()
    
    # Send email
    success = client.send_email(
        recipients=['recipient@example.com'],
        subject='Test Email',
        body='<h1>Hello World</h1>',
        body_type='html'
    )

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import email.mime.multipart
import email.mime.text
import requests
import logging
import smtplib
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import msal

from config import config


# Configure module logger
logger = logging.getLogger(__name__)


class EmailError(Exception):
    """Custom exception for email-related errors."""
    pass


class EmailClient:
    """
    Client for sending emails via Microsoft Graph API.
    
    This class handles OAuth2 authentication using client credentials flow
    and provides methods for sending emails through Outlook/Exchange Online.
    
    Attributes:
        tenant_id: Azure AD tenant ID
        client_id: Application client ID
        client_secret: Application client secret
        sender_email: Email address to send from
        graph_endpoint: Microsoft Graph API endpoint
        access_token: Cached access token
        token_expires_at: Token expiration timestamp
    """
    
    # Token refresh buffer (refresh if token expires in less than this time)
    TOKEN_REFRESH_BUFFER_SECONDS = 3600  # 60 minutes
    
    def __init__(self):
        """
        Initialize email client with configuration.
        
        Raises:
            EmailError: If configuration is invalid
        """
        self.tenant_id = config.ms_tenant_id
        self.client_id = config.ms_client_id
        self.client_secret = config.ms_client_secret
        self.sender_email = config.ms_sender_email
        self.authority = config.ms_authority
        self.graph_endpoint = config.ms_graph_endpoint
        self.scope = config.ms_scope
        
        # Token caching
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        # Configuration
        self.timeout = config.request_timeout
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        self.ssl_verify = config.ssl_verify
        self.dry_run = config.dry_run
        self.mock_api_enabled = config.mock_api_enabled

        # SMTP configuration (preferred over Graph API when available)
        import os
        self.smtp_password = os.getenv('SMTP_PASSWORD', '').strip()
        self.use_smtp = os.getenv('USE_SMTP', '').strip().lower() in ('true', '1', 'yes')
        if self.use_smtp:
            mode = 'password' if self.smtp_password else 'OAuth2 XOAUTH2'
            logger.info(f"SMTP mode enabled ({mode}) — sending via smtp.office365.com as {self.sender_email}")

        # Validate configuration
        if not all([self.tenant_id, self.client_id, self.client_secret, self.sender_email]):
            raise EmailError("Microsoft Graph API credentials not configured properly")
        
        # Initialize MSAL confidential client application
        if self.dry_run or self.mock_api_enabled:
            self.app = None
            logger.info(f"Email client initialized in {'DRY RUN' if self.dry_run else 'MOCK'} mode (Sender: {self.sender_email})")
        else:
            try:
                self.app = msal.ConfidentialClientApplication(
                    self.client_id,
                    authority=self.authority,
                    client_credential=self.client_secret
                )
                logger.info(f"Email client initialized (Sender: {self.sender_email})")
            except Exception as e:
                raise EmailError(f"Failed to initialize MSAL client: {e}") from e
    
    def _get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get access token with automatic caching and refresh.
        
        Args:
            force_refresh: Force token refresh even if cached token is valid
            
        Returns:
            Valid access token
            
        Raises:
            EmailError: If token acquisition fails
        """
        # Check if we have a valid cached token
        if not force_refresh and self.access_token and self.token_expires_at:
            time_until_expiry = (self.token_expires_at - datetime.utcnow()).total_seconds()
            
            # If token is still valid with buffer, use it
            if time_until_expiry > self.TOKEN_REFRESH_BUFFER_SECONDS:
                logger.debug(f"Using cached token (expires in {time_until_expiry/60:.1f} minutes)")
                return self.access_token
            else:
                logger.info(f"Token expires soon ({time_until_expiry/60:.1f} minutes), refreshing...")
        
        # Acquire new token
        try:
            logger.info("Acquiring new access token...")
            
            result = self.app.acquire_token_for_client(scopes=self.scope)
            
            if "access_token" in result:
                self.access_token = result["access_token"]
                
                # Calculate expiration time (default to 3600 seconds if not provided)
                expires_in = result.get("expires_in", 3600)
                self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                
                logger.info(f"Access token acquired successfully (expires in {expires_in/60:.1f} minutes)")
                return self.access_token
            else:
                error = result.get("error", "Unknown error")
                error_description = result.get("error_description", "No description")
                error_msg = f"Failed to acquire token: {error} - {error_description}"
                logger.error(error_msg)
                raise EmailError(error_msg)
                
        except Exception as e:
            logger.error(f"Token acquisition failed: {e}")
            raise EmailError(f"Failed to acquire access token: {e}") from e
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Make HTTP request to Microsoft Graph API with retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to graph_endpoint)
            data: Request body data
            retry_count: Current retry attempt number
            
        Returns:
            JSON response as dictionary
            
        Raises:
            EmailError: If request fails after all retries
        """
        url = f"{self.graph_endpoint}/{endpoint}"
        
        # Get access token (may be cached)
        try:
            token = self._get_access_token()
        except EmailError as e:
            raise EmailError(f"Authentication failed: {e}") from e
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            logger.debug(f"Making {method} request to: {endpoint}")
            
            if method == 'GET':
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    verify=self.ssl_verify
                )
            elif method == 'POST':
                response = requests.post(
                    url,
                    headers=headers,
                    json=data,
                    timeout=self.timeout,
                    verify=self.ssl_verify
                )
            else:
                raise EmailError(f"Unsupported HTTP method: {method}")
            
            # Handle 401 Unauthorized - token may have expired
            if response.status_code == 401:
                logger.warning("Received 401 Unauthorized, refreshing token...")
                
                if retry_count < self.max_retries:
                    # Force token refresh and retry
                    self._get_access_token(force_refresh=True)
                    return self._make_request(method, endpoint, data, retry_count + 1)
                else:
                    raise EmailError("Authentication failed after token refresh")
            
            # Check for other HTTP errors
            response.raise_for_status()
            
            # Return empty dict for 202 Accepted (no content)
            if response.status_code == 202:
                logger.debug("Request accepted (202)")
                return {}
            
            # Parse JSON response if present
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout: {e}")
            
            if retry_count < self.max_retries:
                wait_time = self.retry_delay * (2 ** retry_count)
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise EmailError(f"Request timed out after {self.max_retries} retries") from e
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            
            if retry_count < self.max_retries:
                wait_time = self.retry_delay * (2 ** retry_count)
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(method, endpoint, data, retry_count + 1)
            else:
                raise EmailError(f"Request failed after {self.max_retries} retries: {e}") from e
    
    def send_email(
        self,
        recipients: List[str],
        subject: str,
        body: str,
        body_type: str = 'html',
        cc_recipients: Optional[List[str]] = None,
        bcc_recipients: Optional[List[str]] = None,
        importance: str = 'normal',
        save_to_sent_items: bool = True
    ) -> bool:
        """
        Send email via Microsoft Graph API.
        
        Args:
            recipients: List of recipient email addresses
            subject: Email subject line
            body: Email body content
            body_type: Body content type ('html' or 'text')
            cc_recipients: Optional CC recipients
            bcc_recipients: Optional BCC recipients
            importance: Email importance ('low', 'normal', 'high')
            save_to_sent_items: Whether to save email to Sent Items folder
            
        Returns:
            bool: True if email sent successfully, False otherwise
            
        Raises:
            EmailError: If email sending fails
        """
        if not recipients:
            raise EmailError("No recipients specified")

        # In dry_run or mock mode, log instead of sending
        if self.dry_run or self.mock_api_enabled:
            logger.info(f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Would send email to {', '.join(recipients)}: '{subject}'")
            return True

        # Validate body type
        if body_type not in ['html', 'text']:
            body_type = 'html'

        # Validate importance
        if importance not in ['low', 'normal', 'high']:
            importance = 'normal'

        # Use SMTP if configured (bypasses Graph API IP blocklist issues)
        if self.use_smtp:
            return self._send_via_smtp(recipients, subject, body, body_type,
                                       cc_recipients, bcc_recipients)

        # Build recipient list
        to_recipients = [{'emailAddress': {'address': email}} for email in recipients]
        
        # Build CC list if provided
        cc_list = []
        if cc_recipients:
            cc_list = [{'emailAddress': {'address': email}} for email in cc_recipients]
        
        # Build BCC list if provided
        bcc_list = []
        if bcc_recipients:
            bcc_list = [{'emailAddress': {'address': email}} for email in bcc_recipients]
        
        # Construct email message
        message = {
            'message': {
                'subject': subject,
                'body': {
                    'contentType': body_type.capitalize(),  # 'Html' or 'Text'
                    'content': body
                },
                'from': {
                    'emailAddress': {
                        'address': self.sender_email
                    }
                },
                'toRecipients': to_recipients,
                'importance': importance
            },
            'saveToSentItems': save_to_sent_items
        }
        
        # Add CC if provided
        if cc_list:
            message['message']['ccRecipients'] = cc_list
        
        # Add BCC if provided
        if bcc_list:
            message['message']['bccRecipients'] = bcc_list
        
        try:
            logger.info(f"Sending email to {len(recipients)} recipient(s): {', '.join(recipients[:3])}...")

            # Two-step: create draft then send — routes through mailbox send
            # infrastructure rather than the /sendMail direct endpoint
            draft_payload = message['message']
            draft_payload['isDraft'] = True

            create_endpoint = f"users/{self.sender_email}/messages"
            draft = self._make_request('POST', create_endpoint, data=draft_payload)

            if draft and draft.get('id'):
                send_endpoint = f"users/{self.sender_email}/messages/{draft['id']}/send"
                self._make_request('POST', send_endpoint)
                logger.info(f"Email sent successfully (via draft): '{subject}' to {', '.join(recipients)}")
            else:
                # Fallback to direct sendMail
                endpoint = f"users/{self.sender_email}/sendMail"
                self._make_request('POST', endpoint, data=message)
                logger.info(f"Email sent successfully (direct): '{subject}' to {', '.join(recipients)}")

            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            raise EmailError(f"Failed to send email: {e}") from e
    
    def _send_via_smtp(
        self,
        recipients: List[str],
        subject: str,
        body: str,
        body_type: str = 'html',
        cc_recipients: Optional[List[str]] = None,
        bcc_recipients: Optional[List[str]] = None,
    ) -> bool:
        """Send email via SMTP through smtp.office365.com using OAuth2 or password."""
        import base64

        msg = email.mime.multipart.MIMEMultipart('alternative')
        msg['From'] = self.sender_email
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        if cc_recipients:
            msg['Cc'] = ', '.join(cc_recipients)

        content_type = 'html' if body_type == 'html' else 'plain'
        msg.attach(email.mime.text.MIMEText(body, content_type, 'utf-8'))

        all_recipients = list(recipients)
        if cc_recipients:
            all_recipients.extend(cc_recipients)
        if bcc_recipients:
            all_recipients.extend(bcc_recipients)

        try:
            logger.info(f"[SMTP] Sending email to {', '.join(recipients[:3])}...")
            with smtplib.SMTP('smtp.office365.com', 587, timeout=self.timeout) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()

                if self.smtp_password:
                    # Plain password auth
                    server.login(self.sender_email, self.smtp_password)
                else:
                    # OAuth2 XOAUTH2 auth — acquire token with SMTP.Send scope
                    smtp_scope = ["https://outlook.office365.com/.default"]
                    result = self.app.acquire_token_for_client(scopes=smtp_scope)
                    if "access_token" not in result:
                        err = result.get("error_description", result.get("error", "Unknown"))
                        raise EmailError(f"SMTP OAuth2 token failed: {err}")
                    token = result["access_token"]
                    auth_string = f"user={self.sender_email}\x01auth=Bearer {token}\x01\x01"
                    auth_b64 = base64.b64encode(auth_string.encode()).decode()
                    code, resp = server.docmd('AUTH', f'XOAUTH2 {auth_b64}')
                    if code != 235:
                        raise EmailError(f"XOAUTH2 auth failed: {code} {resp}")

                server.sendmail(self.sender_email, all_recipients, msg.as_string())
            logger.info(f"[SMTP] Email sent successfully: '{subject}' to {', '.join(recipients)}")
            return True
        except EmailError:
            raise
        except Exception as e:
            logger.error(f"[SMTP] Failed to send email: {e}")
            raise EmailError(f"SMTP send failed: {e}") from e

    def send_lead_notification(self, lead_data: Dict[str, Any]) -> bool:
        """
        Send lead notification email with formatted content.
        
        Args:
            lead_data: Lead information dictionary
            
        Returns:
            bool: True if email sent successfully
        """
        # Extract lead information
        lead_id = lead_data.get('lead_id', 'Unknown')
        email = lead_data.get('email', 'Not provided')
        full_name = lead_data.get('full_name', 'Unknown')
        first_name = lead_data.get('first_name', '')
        last_name = lead_data.get('last_name', '')
        phone = lead_data.get('phone_number', 'Not provided')
        company = lead_data.get('company_name', 'Not provided')
        job_title = lead_data.get('job_title', 'Not provided')
        created_time = lead_data.get('created_time', 'Unknown')
        
        # Format subject using template
        subject = config.email_subject_template.format(
            name=full_name,
            email=email,
            phone=phone,
            date=created_time
        )
        
        # Build HTML email body
        html_body = self._build_lead_email_html(lead_data)
        
        # Get recipients from config
        recipients = config.ms_recipient_emails
        
        # Determine importance based on config
        importance = config.email_priority
        
        try:
            # Send email
            return self.send_email(
                recipients=recipients,
                subject=subject,
                body=html_body,
                body_type='html',
                importance=importance,
                save_to_sent_items=True
            )
        except Exception as e:
            logger.error(f"Failed to send lead notification for {lead_id}: {e}")
            return False
    
    def _build_lead_email_html(self, lead_data: Dict[str, Any]) -> str:
        """
        Build HTML email body for lead notification.
        
        Args:
            lead_data: Lead information dictionary
            
        Returns:
            HTML email content
        """
        # Extract data with fallbacks
        full_name = lead_data.get('full_name', 'Not provided')
        email = lead_data.get('email', 'Not provided')
        phone = lead_data.get('phone_number', 'Not provided')
        company = lead_data.get('company_name', 'Not provided')
        job_title = lead_data.get('job_title', 'Not provided')
        created_time = lead_data.get('created_time', 'Unknown')
        lead_id = lead_data.get('lead_id', 'Unknown')
        
        # Build HTML
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>New Lead Notification</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .email-container {{
            background-color: #ffffff;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            border-bottom: 3px solid #0078d4;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        h1 {{
            color: #0078d4;
            font-size: 24px;
            margin: 0 0 10px 0;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
        }}
        .lead-info {{
            margin: 20px 0;
        }}
        .info-row {{
            display: flex;
            padding: 12px 0;
            border-bottom: 1px solid #eee;
        }}
        .info-row:last-child {{
            border-bottom: none;
        }}
        .info-label {{
            font-weight: 600;
            color: #555;
            min-width: 140px;
        }}
        .info-value {{
            color: #333;
            flex: 1;
        }}
        .cta-button {{
            display: inline-block;
            background-color: #0078d4;
            color: #ffffff;
            text-decoration: none;
            padding: 12px 30px;
            border-radius: 4px;
            font-weight: 600;
            margin: 20px 0;
        }}
        .cta-button:hover {{
            background-color: #005a9e;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            font-size: 12px;
            color: #666;
            text-align: center;
        }}
        .highlight {{
            background-color: #fff4e5;
            padding: 15px;
            border-left: 4px solid #ff8c00;
            margin: 20px 0;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <div class="header">
            <h1>🎯 New Lead Received</h1>
            <div class="timestamp">Received: {created_time}</div>
        </div>
        
        <div class="highlight">
            <strong>Action Required:</strong> A new lead has been submitted through your Meta Lead Ads form. 
            Please review the details below and follow up promptly.
        </div>
        
        <div class="lead-info">
            <div class="info-row">
                <div class="info-label">👤 Name:</div>
                <div class="info-value"><strong>{full_name}</strong></div>
            </div>
            <div class="info-row">
                <div class="info-label">📧 Email:</div>
                <div class="info-value"><a href="mailto:{email}">{email}</a></div>
            </div>
            <div class="info-row">
                <div class="info-label">📱 Phone:</div>
                <div class="info-value"><a href="tel:{phone}">{phone}</a></div>
            </div>
            <div class="info-row">
                <div class="info-label">🏢 Company:</div>
                <div class="info-value">{company}</div>
            </div>
            <div class="info-row">
                <div class="info-label">💼 Job Title:</div>
                <div class="info-value">{job_title}</div>
            </div>
            <div class="info-row">
                <div class="info-label">🆔 Lead ID:</div>
                <div class="info-value">{lead_id}</div>
            </div>
        </div>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{config.booking_url}" class="cta-button">📅 Schedule a Call</a>
        </div>
        
        <div class="footer">
            <p>This is an automated notification from Arkitekt OpenCRM.</p>
            <p>Lead ID: {lead_id}</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test connection to Microsoft Graph API.

        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.dry_run or self.mock_api_enabled:
            message = f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Microsoft Graph API connection test skipped"
            logger.info(message)
            return True, message

        try:
            # Try to get access token
            token = self._get_access_token()
            
            # Try to get sender's profile
            endpoint = f"users/{self.sender_email}"
            data = self._make_request('GET', endpoint)
            
            user_name = data.get('displayName', 'Unknown')
            message = f"Successfully connected to Microsoft Graph API. User: {user_name} ({self.sender_email})"
            logger.info(message)
            return True, message
            
        except Exception as e:
            message = f"Failed to connect to Microsoft Graph API: {e}"
            logger.error(message)
            return False, message


if __name__ == '__main__':
    """
    Test email client when run as script.
    
    Usage:
        python email_client.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        print("\n" + "=" * 80)
        print("Email Client - Connection Test")
        print("=" * 80 + "\n")
        
        # Initialize client
        client = EmailClient()
        
        # Test connection
        print("[TEST] Testing Microsoft Graph API connection...")
        success, message = client.test_connection()
        print(f"[{'SUCCESS' if success else 'FAILED'}] {message}\n")
        
        if success:
            # Test sending a sample email (only if explicitly confirmed)
            print("[INFO] To test sending an email, uncomment the code below and run again.")
            print("[INFO] Make sure the recipient email is correct.\n")
            
            # Uncomment below to test email sending
            # print("[TEST] Sending test email...")
            # test_lead = {
            #     'lead_id': 'test_123',
            #     'full_name': 'Test User',
            #     'email': 'test@example.com',
            #     'phone_number': '+1234567890',
            #     'company_name': 'Test Company',
            #     'job_title': 'Test Manager',
            #     'created_time': datetime.utcnow().isoformat()
            # }
            # success = client.send_lead_notification(test_lead)
            # print(f"[{'SUCCESS' if success else 'FAILED'}] Email {'sent' if success else 'failed'}\n")
        
        print("=" * 80)
        print("[SUCCESS] All tests completed!")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
