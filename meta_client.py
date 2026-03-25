"""
Arkitekt OpenCRM - Meta Graph API Client Module

This module handles all interactions with the Meta (Facebook) Graph API for
fetching lead ads data. It implements authentication, pagination, field parsing,
retry logic, and comprehensive error handling.

Features:
- Authentication using long-lived page access token
- Fetch all leadgen forms for a Facebook page
- Retrieve leads with pagination support
- Parse lead field data with intelligent field mapping
- Retry logic with exponential backoff
- Rate limiting compliance
- Comprehensive error handling and logging
- Raw JSON storage for all field data

API Documentation:
- Lead Ads: https://developers.facebook.com/docs/marketing-api/guides/lead-ads
- Graph API: https://developers.facebook.com/docs/graph-api

Usage:
    from meta_client import MetaClient
    
    # Initialize client
    client = MetaClient()
    
    # Get all forms
    forms = client.get_leadgen_forms()
    
    # Get leads for a specific form
    leads = client.get_leads_for_form(form_id)
    
    # Get all leads from all forms
    all_leads = client.get_all_leads()

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import requests
import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from config import config


# Configure module logger
logger = logging.getLogger(__name__)


class MetaAPIError(Exception):
    """Custom exception for Meta API errors."""
    pass


class MetaClient:
    """
    Client for interacting with Meta (Facebook) Graph API.
    
    This class provides methods for fetching leadgen forms and leads from
    Facebook Lead Ads through the Graph API.
    
    Attributes:
        base_url: Base URL for Meta Graph API
        page_id: Facebook Page ID
        access_token: Page access token with leads_retrieval permission
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (seconds)
    """
    
    # Common field name mappings for lead form fields
    FIELD_MAPPINGS = {
        'email': ['email', 'e-mail', 'work_email', 'email_address', 'your_email'],
        'full_name': ['full_name', 'name', 'fullname', 'your_name'],
        'first_name': ['first_name', 'firstname', 'given_name'],
        'last_name': ['last_name', 'lastname', 'surname', 'family_name'],
        'phone_number': ['phone_number', 'phone', 'mobile', 'cell_phone', 'telephone', 'contact_number'],
        'company_name': ['company_name', 'company', 'business_name', 'organization'],
        'job_title': ['job_title', 'title', 'position', 'role', 'occupation']
    }
    
    def __init__(self):
        """
        Initialize Meta API client with configuration.

        Raises:
            MetaAPIError: If configuration is invalid
        """
        self.base_url = config.meta_api_base_url
        self.page_id = config.meta_page_id
        self.access_token = config.meta_page_access_token
        self.timeout = config.request_timeout
        self.max_retries = config.max_retries
        self.retry_delay = config.retry_delay
        self.ssl_verify = config.ssl_verify
        self.dry_run = config.dry_run
        self.mock_api_enabled = config.mock_api_enabled

        # Validate configuration
        if not self.page_id or not self.access_token:
            raise MetaAPIError("Meta API credentials not configured properly")

        if self.dry_run or self.mock_api_enabled:
            logger.info(f"Meta API client initialized in {'DRY RUN' if self.dry_run else 'MOCK'} mode (Page ID: {self.page_id})")
        else:
            # Exchange for a Page Access Token if the configured token is a
            # System User or User token.  The Graph API returns a page-scoped
            # token when you request the ``access_token`` field on the page node.
            self.access_token = self._resolve_page_token(self.access_token)
            logger.info(f"Meta API client initialized (Page ID: {self.page_id}, API Version: {config.meta_api_version})")
    
    def _resolve_page_token(self, token: str) -> str:
        """
        Exchange a System User / User token for a Page Access Token.

        If the token is already a page token the API simply returns it back,
        so this is safe to call unconditionally.

        Args:
            token: The configured access token (may be user, system-user, or page).

        Returns:
            A Page Access Token suitable for leadgen_forms / leads endpoints.
        """
        try:
            url = f"{self.base_url}/{self.page_id}"
            resp = requests.get(
                url,
                params={'access_token': token, 'fields': 'access_token'},
                timeout=self.timeout,
                verify=self.ssl_verify,
            )
            resp.raise_for_status()
            page_token = resp.json().get('access_token')
            if page_token:
                logger.info("Resolved Page Access Token from configured token")
                return page_token
        except Exception as e:
            logger.warning(f"Could not resolve page token, using configured token: {e}")
        return token

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = 'GET'
    ) -> Dict[str, Any]:
        """
        Make HTTP request to Meta API with retry logic.
        
        Args:
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            method: HTTP method (GET, POST, etc.)
            
        Returns:
            JSON response as dictionary
            
        Raises:
            MetaAPIError: If request fails after all retries
        """
        url = f"{self.base_url}/{endpoint}"
        
        # Add access token to parameters
        if params is None:
            params = {}
        params['access_token'] = self.access_token
        
        # Retry loop with exponential backoff
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Making {method} request to: {endpoint} (attempt {attempt + 1}/{self.max_retries + 1})")
                
                if method == 'GET':
                    response = requests.get(
                        url,
                        params=params,
                        timeout=self.timeout,
                        verify=self.ssl_verify
                    )
                elif method == 'POST':
                    response = requests.post(
                        url,
                        data=params,
                        timeout=self.timeout,
                        verify=self.ssl_verify
                    )
                else:
                    raise MetaAPIError(f"Unsupported HTTP method: {method}")
                
                # Check for HTTP errors
                response.raise_for_status()
                
                # Parse JSON response
                data = response.json()
                
                # Check for API-level errors
                if 'error' in data:
                    error = data['error']
                    error_msg = f"Meta API Error: {error.get('message', 'Unknown error')} (Code: {error.get('code', 'N/A')})"
                    logger.error(error_msg)
                    
                    # Check if error is retryable
                    error_code = error.get('code')
                    error_subcode = error.get('error_subcode')
                    
                    # Rate limiting or temporary errors - retry
                    if error_code in [4, 17, 32, 613] or error_subcode in [99, 2108006]:
                        if attempt < self.max_retries:
                            wait_time = self.retry_delay * (2 ** attempt)
                            logger.warning(f"Temporary error, retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            continue
                    
                    # Permanent error - don't retry
                    raise MetaAPIError(error_msg)
                
                # Successful request
                logger.debug(f"Request successful: {endpoint}")
                return data
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"Request timeout (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise MetaAPIError(f"Request timed out after {self.max_retries} retries") from e
                    
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                # Only retry on server errors (5xx) or rate limiting (429)
                if status_code and (status_code >= 500 or status_code == 429):
                    logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * (2 ** attempt)
                        logger.info(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        raise MetaAPIError(f"Request failed after {self.max_retries} retries: {e}") from e
                else:
                    # Client errors (4xx) are permanent - do not retry
                    logger.error(f"Request failed with client error {status_code}: {e}")
                    raise MetaAPIError(f"API request failed ({status_code}): {e}") from e

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise MetaAPIError(f"Request failed after {self.max_retries} retries: {e}") from e
            
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                raise MetaAPIError("Invalid JSON response from API") from e
        
        # Should never reach here, but just in case
        raise MetaAPIError("Request failed after all retry attempts")
    
    def get_leadgen_forms(self) -> List[Dict[str, Any]]:
        """
        Fetch all leadgen forms for the configured Facebook page.

        Returns:
            List of form dictionaries containing form details

        Raises:
            MetaAPIError: If API request fails
        """
        if self.dry_run or self.mock_api_enabled:
            logger.info(f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Skipping Meta API call to fetch leadgen forms")
            return []

        endpoint = f"{self.page_id}/leadgen_forms"
        params = {
            'fields': 'id,name,status,leads_count,created_time,locale,follow_up_action_url',
            'limit': config.batch_size
        }

        all_forms = []

        try:
            logger.info(f"Fetching leadgen forms for page: {self.page_id}")
            
            # Handle pagination
            while True:
                data = self._make_request(endpoint, params)
                
                # Extract forms from response
                forms = data.get('data', [])
                all_forms.extend(forms)
                
                logger.debug(f"Retrieved {len(forms)} forms (total: {len(all_forms)})")
                
                # Check for next page
                paging = data.get('paging', {})
                next_url = paging.get('next')
                
                if not next_url:
                    break
                
                # Extract cursor for next page
                cursors = paging.get('cursors', {})
                after_cursor = cursors.get('after')
                
                if not after_cursor:
                    break
                
                # Update params for next page
                params['after'] = after_cursor
            
            logger.info(f"Retrieved {len(all_forms)} total forms")
            return all_forms
            
        except Exception as e:
            logger.error(f"Failed to fetch leadgen forms: {e}")
            raise MetaAPIError(f"Failed to fetch leadgen forms: {e}") from e
    
    def get_leads_for_form(
        self,
        form_id: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch leads for a specific leadgen form with pagination.

        Args:
            form_id: Lead form ID
            limit: Optional maximum number of leads to fetch

        Returns:
            List of parsed lead dictionaries

        Raises:
            MetaAPIError: If API request fails
        """
        if self.dry_run or self.mock_api_enabled:
            logger.info(f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Skipping Meta API call to fetch leads for form {form_id}")
            return []

        endpoint = f"{form_id}/leads"
        params = {
            'fields': 'id,created_time,field_data',
            'limit': limit or config.batch_size
        }
        
        all_leads = []
        
        try:
            logger.info(f"Fetching leads for form: {form_id}")
            
            # Handle pagination
            while True:
                data = self._make_request(endpoint, params)
                
                # Extract leads from response
                raw_leads = data.get('data', [])
                
                # Parse each lead
                for raw_lead in raw_leads:
                    try:
                        parsed_lead = self._parse_lead(raw_lead, form_id)
                        all_leads.append(parsed_lead)
                    except Exception as e:
                        logger.error(f"Failed to parse lead {raw_lead.get('id')}: {e}")
                        # Continue with other leads
                        continue
                
                logger.debug(f"Retrieved {len(raw_leads)} leads (total: {len(all_leads)})")
                
                # Check if we've reached the limit
                if limit and len(all_leads) >= limit:
                    logger.info(f"Reached limit of {limit} leads")
                    break
                
                # Check for next page
                paging = data.get('paging', {})
                next_url = paging.get('next')
                
                if not next_url:
                    break
                
                # Extract cursor for next page
                cursors = paging.get('cursors', {})
                after_cursor = cursors.get('after')
                
                if not after_cursor:
                    break
                
                # Update params for next page
                params['after'] = after_cursor
            
            logger.info(f"Retrieved {len(all_leads)} total leads for form {form_id}")
            return all_leads
            
        except Exception as e:
            logger.error(f"Failed to fetch leads for form {form_id}: {e}")
            raise MetaAPIError(f"Failed to fetch leads: {e}") from e
    
    def _parse_lead(self, raw_lead: Dict[str, Any], form_id: str) -> Dict[str, Any]:
        """
        Parse raw lead data from Meta API into structured format.
        
        Extracts common fields like email, name, phone with intelligent
        field matching and fallback handling.
        
        Args:
            raw_lead: Raw lead data from Meta API
            form_id: Form ID this lead belongs to
            
        Returns:
            Parsed lead dictionary with standardized fields
        """
        lead_id = raw_lead.get('id', '')
        created_time = raw_lead.get('created_time', '')
        field_data = raw_lead.get('field_data', [])
        
        # Initialize parsed lead structure
        parsed_lead = {
            'lead_id': lead_id,
            'form_id': form_id,
            'created_time': created_time,
            'email': '',
            'full_name': '',
            'first_name': '',
            'last_name': '',
            'phone_number': '',
            'company_name': '',
            'job_title': '',
            'raw_field_data': field_data  # Store complete raw data
        }
        
        # Create a map of field names to values for easier lookup
        field_map = {}
        for field in field_data:
            field_name = field.get('name', '').lower().strip()
            field_values = field.get('values', [])
            
            # Get the first value if available
            if field_values and len(field_values) > 0:
                field_map[field_name] = field_values[0].strip()
        
        # Extract fields using mapping
        parsed_lead['email'] = self._extract_field(field_map, 'email')
        parsed_lead['full_name'] = self._extract_field(field_map, 'full_name')
        parsed_lead['first_name'] = self._extract_field(field_map, 'first_name')
        parsed_lead['last_name'] = self._extract_field(field_map, 'last_name')
        parsed_lead['phone_number'] = self._extract_field(field_map, 'phone_number')
        parsed_lead['company_name'] = self._extract_field(field_map, 'company_name')
        parsed_lead['job_title'] = self._extract_field(field_map, 'job_title')
        
        # Fallback: If full_name is empty but we have first/last name
        if not parsed_lead['full_name'] and (parsed_lead['first_name'] or parsed_lead['last_name']):
            parsed_lead['full_name'] = f"{parsed_lead['first_name']} {parsed_lead['last_name']}".strip()
        
        # Fallback: If we have full_name but no first/last, try to split
        if parsed_lead['full_name'] and not (parsed_lead['first_name'] and parsed_lead['last_name']):
            name_parts = parsed_lead['full_name'].split(None, 1)  # Split on first space
            if len(name_parts) >= 1 and not parsed_lead['first_name']:
                parsed_lead['first_name'] = name_parts[0]
            if len(name_parts) >= 2 and not parsed_lead['last_name']:
                parsed_lead['last_name'] = name_parts[1]
        
        logger.debug(f"Parsed lead {lead_id}: {parsed_lead.get('email', 'no email')} - {parsed_lead.get('full_name', 'no name')}")
        
        return parsed_lead
    
    def _extract_field(self, field_map: Dict[str, str], field_type: str) -> str:
        """
        Extract a field value using intelligent field name matching.
        
        Tries multiple possible field name variations to find the value.
        
        Args:
            field_map: Dictionary of field names to values
            field_type: Type of field to extract (e.g., 'email', 'phone_number')
            
        Returns:
            Field value or empty string if not found
        """
        # Get possible field names for this field type
        possible_names = self.FIELD_MAPPINGS.get(field_type, [])
        
        # Try each possible name
        for name in possible_names:
            if name in field_map:
                value = field_map[name]
                if value:  # Return first non-empty value found
                    return value
        
        # Also try exact match with field_type itself
        if field_type in field_map:
            return field_map[field_type]
        
        # Not found
        return ''
    
    def get_all_leads(self, limit_per_form: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch all leads from all forms on the page.

        Args:
            limit_per_form: Optional limit on leads per form

        Returns:
            List of all leads from all forms

        Raises:
            MetaAPIError: If API request fails
        """
        all_leads = []

        try:
            # Get all forms (returns empty list in dry_run/mock mode)
            forms = self.get_leadgen_forms()
            
            if not forms:
                logger.warning("No leadgen forms found for this page")
                return all_leads
            
            logger.info(f"Found {len(forms)} forms, fetching leads...")
            
            # Fetch leads from each form
            for form in forms:
                form_id = form.get('id')
                form_name = form.get('name', 'Unknown')
                
                if not form_id:
                    logger.warning(f"Form has no ID, skipping: {form_name}")
                    continue
                
                logger.info(f"Fetching leads from form: {form_name} (ID: {form_id})")
                
                try:
                    leads = self.get_leads_for_form(form_id, limit=limit_per_form)
                    all_leads.extend(leads)
                    logger.info(f"Retrieved {len(leads)} leads from form {form_name}")
                except Exception as e:
                    logger.error(f"Failed to fetch leads from form {form_id}: {e}")
                    # Continue with other forms
                    continue
            
            logger.info(f"Retrieved {len(all_leads)} total leads from all forms")
            return all_leads
            
        except Exception as e:
            logger.error(f"Failed to fetch all leads: {e}")
            raise MetaAPIError(f"Failed to fetch all leads: {e}") from e
    
    def get_lead_by_id(self, lead_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a specific lead by ID.

        Args:
            lead_id: Lead ID

        Returns:
            Parsed lead dictionary or None if not found

        Raises:
            MetaAPIError: If API request fails
        """
        if self.dry_run or self.mock_api_enabled:
            logger.info(f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Skipping Meta API call to fetch lead {lead_id}")
            return None

        endpoint = lead_id
        params = {
            'fields': 'id,created_time,field_data'
        }
        
        try:
            logger.info(f"Fetching lead: {lead_id}")
            data = self._make_request(endpoint, params)
            
            # Parse the lead
            parsed_lead = self._parse_lead(data, '')  # Form ID unknown
            
            logger.info(f"Retrieved lead: {lead_id}")
            return parsed_lead
            
        except MetaAPIError as e:
            logger.error(f"Failed to fetch lead {lead_id}: {e}")
            return None
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the connection to Meta API.

        Returns:
            Tuple of (success: bool, message: str)
        """
        if self.dry_run or self.mock_api_enabled:
            message = f"[{'DRY RUN' if self.dry_run else 'MOCK'}] Meta API connection test skipped"
            logger.info(message)
            return True, message

        try:
            # Test by fetching page info
            endpoint = self.page_id
            params = {
                'fields': 'id,name'
            }
            
            data = self._make_request(endpoint, params)
            page_name = data.get('name', 'Unknown')
            
            message = f"Successfully connected to Meta API. Page: {page_name}"
            logger.info(message)
            return True, message
            
        except Exception as e:
            message = f"Failed to connect to Meta API: {e}"
            logger.error(message)
            return False, message


if __name__ == '__main__':
    """
    Test Meta API client when run as script.
    
    Usage:
        python meta_client.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        print("\n" + "=" * 80)
        print("Meta API Client - Connection Test")
        print("=" * 80 + "\n")
        
        # Initialize client
        client = MetaClient()
        
        # Test connection
        print("[TEST] Testing API connection...")
        success, message = client.test_connection()
        print(f"[{'SUCCESS' if success else 'FAILED'}] {message}\n")
        
        if success:
            # Fetch forms
            print("[TEST] Fetching leadgen forms...")
            forms = client.get_leadgen_forms()
            print(f"[SUCCESS] Found {len(forms)} forms\n")
            
            if forms:
                for i, form in enumerate(forms, 1):
                    print(f"Form {i}:")
                    print(f"  ID: {form.get('id')}")
                    print(f"  Name: {form.get('name')}")
                    print(f"  Status: {form.get('status')}")
                    print(f"  Leads Count: {form.get('leads_count', 0)}")
                    print()
                
                # Fetch leads from first form
                first_form = forms[0]
                form_id = first_form.get('id')
                
                print(f"[TEST] Fetching leads from form: {first_form.get('name')}...")
                leads = client.get_leads_for_form(form_id, limit=5)
                print(f"[SUCCESS] Found {len(leads)} leads (limited to 5 for testing)\n")
                
                if leads:
                    for i, lead in enumerate(leads, 1):
                        print(f"Lead {i}:")
                        print(f"  ID: {lead.get('lead_id')}")
                        print(f"  Email: {lead.get('email', 'N/A')}")
                        print(f"  Name: {lead.get('full_name', 'N/A')}")
                        print(f"  Phone: {lead.get('phone_number', 'N/A')}")
                        print(f"  Created: {lead.get('created_time')}")
                        print()
        
        print("=" * 80)
        print("[SUCCESS] All tests completed!")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
