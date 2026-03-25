"""
Arkitekt OpenCRM - Lead Processor Module

This module orchestrates the lead processing workflow, coordinating between
Meta API client for fetching leads, database for storage, and email client
for sending notifications.

Features:
- Fetch leads from Meta Lead Ads API
- Duplicate detection using lead_id
- Database insertion with status='intake'
- Field extraction and normalization
- Raw field data storage as JSON
- Batch processing with error isolation
- Processing statistics and reporting
- Comprehensive error handling

Workflow:
    1. Fetch leads from Meta API (all forms or specific form)
    2. For each lead:
       - Check if lead already exists (duplicate detection)
       - Extract and normalize lead fields
       - Store raw_field_data as JSON
       - Insert into database with status='intake'
       - Log success/failure
    3. Return processing statistics

Usage:
    from lead_processor import LeadProcessor
    
    # Initialize processor
    processor = LeadProcessor()
    
    # Process new leads
    stats = processor.process_new_leads()
    
    # Get processing summary
    print(f"Processed: {stats['new_leads']}, Skipped: {stats['duplicates']}")

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from meta_client import MetaClient, MetaAPIError
from database import Database, DatabaseError, get_database
from config import config


# Configure module logger
logger = logging.getLogger(__name__)


class LeadProcessorError(Exception):
    """Custom exception for lead processor errors."""
    pass


class ProcessingStats:
    """
    Container for lead processing statistics.
    
    Attributes:
        total_fetched: Total leads fetched from API
        new_leads: Number of new leads inserted
        duplicates: Number of duplicate leads skipped
        errors: Number of leads that failed to process
        processed_lead_ids: List of successfully processed lead IDs
        error_details: List of error details for failed leads
    """
    
    def __init__(self):
        """Initialize processing statistics."""
        self.total_fetched: int = 0
        self.new_leads: int = 0
        self.duplicates: int = 0
        self.errors: int = 0
        self.processed_lead_ids: List[str] = []
        self.error_details: List[Dict[str, Any]] = []
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert statistics to dictionary.
        
        Returns:
            Dictionary representation of statistics
        """
        return {
            'total_fetched': self.total_fetched,
            'new_leads': self.new_leads,
            'duplicates': self.duplicates,
            'errors': self.errors,
            'success_rate': f"{(self.new_leads / max(self.total_fetched, 1)) * 100:.1f}%",
            'processed_lead_ids': self.processed_lead_ids,
            'error_count': len(self.error_details)
        }
    
    def __str__(self) -> str:
        """String representation of statistics."""
        return (
            f"LeadProcessingStats(fetched={self.total_fetched}, "
            f"new={self.new_leads}, duplicates={self.duplicates}, "
            f"errors={self.errors})"
        )


class LeadProcessor:
    """
    Lead processor that coordinates fetching, storing, and processing leads.
    
    This class orchestrates the entire lead processing workflow from
    fetching leads via Meta API to storing them in the database.
    
    Attributes:
        meta_client: Meta API client instance
        database: Database instance
    """
    
    def __init__(
        self,
        meta_client: Optional[MetaClient] = None,
        database: Optional[Database] = None
    ):
        """
        Initialize lead processor with clients.
        
        Args:
            meta_client: Optional Meta API client (creates new if None)
            database: Optional database instance (uses singleton if None)
            
        Raises:
            LeadProcessorError: If initialization fails
        """
        try:
            self.meta_client = meta_client or MetaClient()
            self.database = database or get_database()
            
            logger.info("Lead processor initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize lead processor: {e}")
            raise LeadProcessorError(f"Initialization failed: {e}") from e
    
    def process_new_leads(
        self,
        form_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Fetch and process new leads from Meta API.
        
        This is the main entry point for lead processing. It fetches leads
        from Meta API, checks for duplicates, and inserts new leads into
        the database.
        
        Args:
            form_id: Optional specific form ID to process. If None, processes all forms
            limit: Optional limit on number of leads to fetch per form
            
        Returns:
            Dictionary with processing statistics
            
        Raises:
            LeadProcessorError: If processing fails critically
        """
        stats = ProcessingStats()
        
        try:
            logger.info("="*80)
            logger.info("Starting lead processing workflow")
            logger.info(f"Form ID: {form_id or 'All forms'}")
            logger.info(f"Limit: {limit or 'No limit'}")
            logger.info("="*80)
            
            # Fetch leads from Meta API
            leads = self._fetch_leads(form_id, limit)
            stats.total_fetched = len(leads)
            
            logger.info(f"Fetched {stats.total_fetched} leads from Meta API")
            
            if not leads:
                logger.warning("No leads found to process")
                return stats.to_dict()
            
            # Process each lead
            for lead in leads:
                try:
                    # Process individual lead
                    result = self._process_lead(lead)
                    
                    if result == 'new':
                        stats.new_leads += 1
                        stats.processed_lead_ids.append(lead.get('lead_id', 'unknown'))
                    elif result == 'duplicate':
                        stats.duplicates += 1
                    else:
                        stats.errors += 1
                        
                except Exception as e:
                    stats.errors += 1
                    lead_id = lead.get('lead_id', 'unknown')
                    error_detail = {
                        'lead_id': lead_id,
                        'error': str(e),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    stats.error_details.append(error_detail)
                    logger.error(f"Failed to process lead {lead_id}: {e}", exc_info=True)
                    # Continue processing other leads
                    continue
            
            # Log summary
            logger.info("="*80)
            logger.info("Lead processing completed")
            logger.info(f"Total fetched: {stats.total_fetched}")
            logger.info(f"New leads: {stats.new_leads}")
            logger.info(f"Duplicates skipped: {stats.duplicates}")
            logger.info(f"Errors: {stats.errors}")
            logger.info("="*80)
            
            return stats.to_dict()
            
        except MetaAPIError as e:
            logger.error(f"Meta API error during lead processing: {e}")
            raise LeadProcessorError(f"Failed to fetch leads: {e}") from e
            
        except DatabaseError as e:
            logger.error(f"Database error during lead processing: {e}")
            raise LeadProcessorError(f"Database operation failed: {e}") from e
            
        except Exception as e:
            logger.error(f"Unexpected error during lead processing: {e}", exc_info=True)
            raise LeadProcessorError(f"Lead processing failed: {e}") from e
    
    def _fetch_leads(
        self,
        form_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch leads from Meta API.
        
        Args:
            form_id: Optional specific form ID
            limit: Optional limit on number of leads
            
        Returns:
            List of lead dictionaries
            
        Raises:
            MetaAPIError: If API request fails
        """
        try:
            if form_id:
                # Fetch from specific form
                logger.info(f"Fetching leads from form: {form_id}")
                leads = self.meta_client.get_leads_for_form(form_id, limit=limit)
            else:
                # Fetch from all forms
                logger.info("Fetching leads from all forms")
                leads = self.meta_client.get_all_leads(limit_per_form=limit)
            
            logger.debug(f"Fetched {len(leads)} leads from Meta API")
            return leads
            
        except Exception as e:
            logger.error(f"Failed to fetch leads: {e}")
            raise
    
    def _process_lead(self, lead_data: Dict[str, Any]) -> str:
        """
        Process a single lead: check for duplicates and insert into database.
        
        Args:
            lead_data: Lead data from Meta API
            
        Returns:
            str: 'new' if inserted, 'duplicate' if already exists, 'error' if failed
            
        Raises:
            Exception: If processing fails
        """
        lead_id = lead_data.get('lead_id')
        
        if not lead_id:
            logger.error("Lead has no ID, skipping")
            return 'error'
        
        # Check for duplicate
        if self.database.lead_exists(lead_id):
            logger.debug(f"Lead {lead_id} already exists, skipping (duplicate)")
            return 'duplicate'
        
        # Normalize and prepare lead data
        normalized_lead = self._normalize_lead_data(lead_data)
        
        # Set initial status to 'intake'
        normalized_lead['status'] = 'intake'
        
        # Insert into database
        db_id = self.database.insert_lead(normalized_lead)
        
        if db_id:
            logger.info(
                f"Inserted new lead: {lead_id} (DB ID: {db_id}) - "
                f"{normalized_lead.get('email', 'no email')} - "
                f"{normalized_lead.get('full_name', 'no name')}"
            )
            return 'new'
        else:
            logger.warning(f"Failed to insert lead {lead_id} (possibly duplicate)")
            return 'duplicate'
    
    def _normalize_lead_data(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize and validate lead data before database insertion.
        
        Extracts and normalizes fields, ensures raw_field_data is JSON serializable.
        
        Args:
            lead_data: Raw lead data from Meta API
            
        Returns:
            Normalized lead data dictionary
        """
        # Extract core fields
        normalized = {
            'lead_id': lead_data.get('lead_id', ''),
            'form_id': lead_data.get('form_id', ''),
            'created_time': lead_data.get('created_time', ''),
            'email': self._normalize_email(lead_data.get('email', '')),
            'full_name': self._normalize_name(lead_data.get('full_name', '')),
            'first_name': self._normalize_name(lead_data.get('first_name', '')),
            'last_name': self._normalize_name(lead_data.get('last_name', '')),
            'phone_number': self._normalize_phone(lead_data.get('phone_number', '')),
            'company_name': lead_data.get('company_name', '').strip(),
            'job_title': lead_data.get('job_title', '').strip(),
        }
        
        # Handle raw_field_data - ensure it's JSON serializable
        raw_field_data = lead_data.get('raw_field_data', [])
        
        if isinstance(raw_field_data, list):
            # Already in correct format from Meta API
            normalized['raw_field_data'] = raw_field_data
        elif isinstance(raw_field_data, dict):
            # Convert dict to list format
            normalized['raw_field_data'] = [raw_field_data]
        elif isinstance(raw_field_data, str):
            # Try to parse JSON string
            try:
                parsed = json.loads(raw_field_data)
                normalized['raw_field_data'] = parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse raw_field_data as JSON: {raw_field_data[:100]}")
                normalized['raw_field_data'] = []
        else:
            normalized['raw_field_data'] = []
        
        logger.debug(f"Normalized lead data for {normalized['lead_id']}")
        return normalized
    
    def _normalize_email(self, email: str) -> str:
        """
        Normalize email address.
        
        Args:
            email: Raw email address
            
        Returns:
            Normalized email address (lowercase, trimmed)
        """
        if not email:
            return ''
        
        # Convert to lowercase and strip whitespace
        normalized = email.strip().lower()
        
        # Basic validation (optional)
        if '@' not in normalized:
            logger.warning(f"Invalid email format: {email}")
        
        return normalized
    
    def _normalize_name(self, name: str) -> str:
        """
        Normalize person name.
        
        Args:
            name: Raw name
            
        Returns:
            Normalized name (title case, trimmed)
        """
        if not name:
            return ''
        
        # Strip whitespace and convert to title case
        normalized = name.strip().title()
        
        return normalized
    
    def _normalize_phone(self, phone: str) -> str:
        """
        Normalize phone number.
        
        Args:
            phone: Raw phone number
            
        Returns:
            Normalized phone number (digits and + only)
        """
        if not phone:
            return ''
        
        # Remove common formatting characters but keep + for international
        normalized = phone.strip()
        
        # Remove parentheses, dashes, spaces, dots
        chars_to_remove = ['(', ')', '-', ' ', '.']
        for char in chars_to_remove:
            normalized = normalized.replace(char, '')
        
        return normalized
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """
        Get summary of all leads in the database.
        
        Returns:
            Dictionary with database statistics
        """
        try:
            stats = self.database.get_stats()
            logger.debug(f"Retrieved processing summary: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Failed to get processing summary: {e}")
            return {'error': str(e)}
    
    def reprocess_failed_leads(self) -> Dict[str, Any]:
        """
        Reprocess leads that previously failed.
        
        Returns:
            Dictionary with reprocessing statistics
        """
        stats = ProcessingStats()
        
        try:
            logger.info("Starting reprocessing of failed leads")
            
            # Get leads with 'error' or 'failed' status
            failed_leads = self.database.get_leads_by_status('failed')
            failed_leads.extend(self.database.get_leads_by_status('error'))
            
            stats.total_fetched = len(failed_leads)
            
            logger.info(f"Found {stats.total_fetched} failed leads to reprocess")
            
            for lead in failed_leads:
                try:
                    lead_id = lead.get('lead_id')
                    
                    # Reset status to 'intake' for reprocessing
                    success = self.database.update_lead_status(lead_id, 'intake')
                    
                    if success:
                        stats.new_leads += 1
                        stats.processed_lead_ids.append(lead_id)
                        logger.info(f"Reset lead {lead_id} status to 'intake'")
                    else:
                        stats.errors += 1
                        
                except Exception as e:
                    stats.errors += 1
                    logger.error(f"Failed to reprocess lead {lead.get('lead_id')}: {e}")
                    continue
            
            logger.info(f"Reprocessing completed: {stats.new_leads} leads reset")
            return stats.to_dict()
            
        except Exception as e:
            logger.error(f"Failed to reprocess leads: {e}")
            raise LeadProcessorError(f"Reprocessing failed: {e}") from e


if __name__ == '__main__':
    """
    Test lead processor when run as script.
    
    Usage:
        python lead_processor.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        print("\n" + "="*80)
        print("Lead Processor - Test")
        print("="*80 + "\n")
        
        # Initialize processor
        print("[TEST] Initializing lead processor...")
        processor = LeadProcessor()
        print("[SUCCESS] Lead processor initialized\n")
        
        # Get current database stats
        print("[TEST] Getting database statistics...")
        summary = processor.get_processing_summary()
        print("[STATS] Current database status:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
        print()
        
        # Process new leads (limit to 5 for testing)
        print("[TEST] Processing new leads (limit: 5)...")
        stats = processor.process_new_leads(limit=5)
        print("\n[RESULTS] Processing statistics:")
        for key, value in stats.items():
            if key != 'processed_lead_ids':  # Don't print long lists
                print(f"  {key}: {value}")
        print()
        
        print("="*80)
        print("[SUCCESS] All tests completed!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
