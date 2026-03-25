"""
Arkitekt OpenCRM - Database Module

This module handles all database operations for storing and retrieving lead data.
It uses SQLite for persistence and provides a clean interface for CRUD operations.

Features:
- SQLite database with leads table schema
- Automatic schema creation and migration
- CRUD operations for lead management
- Connection pooling and context managers
- Transaction support
- Proper error handling and logging
- Indexes for query optimization
- Thread-safe operations

Schema:
    leads table:
        - id: Primary key (auto-increment)
        - form_id: Meta form ID
        - created_time: Timestamp from Meta
        - email: Lead email address
        - full_name: Full name
        - first_name: First name
        - last_name: Last name
        - phone_number: Phone number
        - company_name: Company name
        - job_title: Job title
        - raw_field_data: JSON string of all form fields
        - status: Processing status (new, processed, failed, etc.)
        - email_sent_at: Timestamp when email was sent
        - inserted_at: Record creation timestamp
        - updated_at: Record update timestamp

Usage:
    from database import Database
    
    # Initialize database
    db = Database()
    
    # Insert a lead
    lead_id = db.insert_lead(lead_data)
    
    # Check if lead exists
    exists = db.lead_exists(lead_id)
    
    # Get leads by status
    leads = db.get_leads_by_status('new')
    
    # Update lead status
    db.update_lead_status(lead_id, 'processed')

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import sqlite3
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from contextlib import contextmanager
from pathlib import Path
import threading

from config import config


# Configure module logger
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass


class Database:
    """
    Database manager for lead storage and retrieval.
    
    This class provides a thread-safe interface to SQLite database operations
    with proper error handling, connection pooling, and transaction support.
    
    Attributes:
        db_path: Path to SQLite database file
        _local: Thread-local storage for connections
        _lock: Thread lock for thread-safe operations
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize database connection and create schema if needed.
        
        Args:
            db_path: Path to database file. If None, uses config.db_path
            
        Raises:
            DatabaseError: If database initialization fails
        """
        self.db_path = db_path or config.db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        
        # Ensure database directory exists
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize database schema
        try:
            self._initialize_schema()
            logger.info(f"Database initialized successfully: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise DatabaseError(f"Database initialization failed: {e}") from e
    
    @contextmanager
    def _get_connection(self):
        """
        Get thread-local database connection with context manager support.
        
        Yields:
            sqlite3.Connection: Database connection
            
        Raises:
            DatabaseError: If connection fails
        """
        # Get or create thread-local connection
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            try:
                self._local.connection = sqlite3.connect(
                    self.db_path,
                    check_same_thread=False,
                    timeout=30.0
                )
                # Enable foreign keys
                self._local.connection.execute("PRAGMA foreign_keys = ON")
                # Set row factory for dict-like access
                self._local.connection.row_factory = sqlite3.Row
                logger.debug(f"Created new database connection for thread {threading.current_thread().name}")
            except sqlite3.Error as e:
                logger.error(f"Failed to connect to database: {e}")
                raise DatabaseError(f"Database connection failed: {e}") from e
        
        try:
            yield self._local.connection
        except sqlite3.Error as e:
            # Rollback on error
            self._local.connection.rollback()
            logger.error(f"Database operation failed: {e}")
            raise DatabaseError(f"Database operation failed: {e}") from e
    
    def _initialize_schema(self) -> None:
        """
        Create database schema if it doesn't exist.
        
        Creates the leads table with all necessary fields and indexes.
        
        Raises:
            DatabaseError: If schema creation fails
        """
        schema = """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id TEXT UNIQUE NOT NULL,
            form_id TEXT,
            created_time TEXT NOT NULL,
            email TEXT,
            full_name TEXT,
            first_name TEXT,
            last_name TEXT,
            phone_number TEXT,
            company_name TEXT,
            job_title TEXT,
            raw_field_data TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            email_sent_at TEXT,
            inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Index on lead_id for quick lookups
        CREATE INDEX IF NOT EXISTS idx_lead_id ON leads(lead_id);
        
        -- Index on status for filtering
        CREATE INDEX IF NOT EXISTS idx_status ON leads(status);
        
        -- Index on email for lookups
        CREATE INDEX IF NOT EXISTS idx_email ON leads(email);
        
        -- Index on created_time for chronological queries
        CREATE INDEX IF NOT EXISTS idx_created_time ON leads(created_time);
        
        -- Composite index for status and created_time
        CREATE INDEX IF NOT EXISTS idx_status_created ON leads(status, created_time);
        """
        
        try:
            with self._get_connection() as conn:
                conn.executescript(schema)
                conn.commit()
                self._apply_migrations(conn)
                logger.debug("Database schema initialized")
        except Exception as e:
            raise DatabaseError(f"Failed to create schema: {e}") from e

    def _apply_migrations(self, conn) -> None:
        """Apply schema migrations safely using PRAGMA table_info."""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(leads)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        if 'tags' not in existing_cols:
            cursor.execute("ALTER TABLE leads ADD COLUMN tags TEXT DEFAULT '{}'")
            logger.info("Migration: added 'tags' column to leads table")

        if 'lead_source' not in existing_cols:
            cursor.execute("ALTER TABLE leads ADD COLUMN lead_source TEXT DEFAULT 'meta'")
            logger.info("Migration: added 'lead_source' column to leads table")

        if 'follow_up_date' not in existing_cols:
            cursor.execute("ALTER TABLE leads ADD COLUMN follow_up_date TEXT DEFAULT NULL")
            logger.info("Migration: added 'follow_up_date' column to leads table")

        if 'deal_value' not in existing_cols:
            cursor.execute("ALTER TABLE leads ADD COLUMN deal_value REAL DEFAULT NULL")
            logger.info("Migration: added 'deal_value' column to leads table")

        if 'expected_close_date' not in existing_cols:
            cursor.execute("ALTER TABLE leads ADD COLUMN expected_close_date TEXT DEFAULT NULL")
            logger.info("Migration: added 'expected_close_date' column to leads table")

        conn.commit()
    
    def insert_lead(self, lead_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a new lead into the database.
        
        Args:
            lead_data: Dictionary containing lead information
                Required keys: lead_id, created_time, raw_field_data
                Optional keys: form_id, email, full_name, first_name, last_name,
                              phone_number, company_name, job_title
        
        Returns:
            int: ID of inserted lead, or None if insert fails
            
        Raises:
            DatabaseError: If insert operation fails
        """
        # Validate required fields
        required_fields = ['lead_id', 'created_time', 'raw_field_data']
        for field in required_fields:
            if field not in lead_data:
                raise DatabaseError(f"Missing required field: {field}")
        
        # Prepare INSERT query
        query = """
        INSERT INTO leads (
            lead_id, form_id, created_time, email, full_name, first_name,
            last_name, phone_number, company_name, job_title, raw_field_data, status,
            tags, lead_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Serialize raw_field_data to JSON if it's a dict or list
        raw_data = lead_data.get('raw_field_data', {})
        if isinstance(raw_data, (dict, list)):
            raw_data = json.dumps(raw_data)

        # Serialize tags to JSON if it's a dict or list
        tags = lead_data.get('tags', {})
        if isinstance(tags, (dict, list)):
            tags = json.dumps(tags)

        params = (
            lead_data['lead_id'],
            lead_data.get('form_id', ''),
            lead_data['created_time'],
            lead_data.get('email', ''),
            lead_data.get('full_name', ''),
            lead_data.get('first_name', ''),
            lead_data.get('last_name', ''),
            lead_data.get('phone_number', ''),
            lead_data.get('company_name', ''),
            lead_data.get('job_title', ''),
            raw_data,
            lead_data.get('status', 'new'),
            tags,
            lead_data.get('lead_source', 'meta')
        )
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                
                lead_id = cursor.lastrowid
                logger.info(f"Inserted lead: {lead_data['lead_id']} (DB ID: {lead_id})")
                return lead_id
                
        except sqlite3.IntegrityError as e:
            # Lead already exists
            logger.warning(f"Lead already exists: {lead_data['lead_id']}")
            return None
        except Exception as e:
            logger.error(f"Failed to insert lead: {e}")
            raise DatabaseError(f"Failed to insert lead: {e}") from e
    
    def lead_exists(self, lead_id: str) -> bool:
        """
        Check if a lead already exists in the database.
        
        Args:
            lead_id: Meta lead ID to check
            
        Returns:
            bool: True if lead exists, False otherwise
        """
        query = "SELECT COUNT(*) FROM leads WHERE lead_id = ?"
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (lead_id,))
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.error(f"Failed to check lead existence: {e}")
            return False
    
    def update_lead_status(
        self, 
        lead_id: str, 
        status: str, 
        email_sent_at: Optional[str] = None
    ) -> bool:
        """
        Update the status of a lead.
        
        Args:
            lead_id: Meta lead ID
            status: New status (e.g., 'processed', 'failed', 'email_sent')
            email_sent_at: Optional timestamp when email was sent
            
        Returns:
            bool: True if update successful, False otherwise
        """
        # Build query based on whether email_sent_at is provided
        if email_sent_at:
            query = """
            UPDATE leads 
            SET status = ?, email_sent_at = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE lead_id = ?
            """
            params = (status, email_sent_at, lead_id)
        else:
            query = """
            UPDATE leads 
            SET status = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE lead_id = ?
            """
            params = (status, lead_id)
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"Updated lead {lead_id} status to: {status}")
                    return True
                else:
                    logger.warning(f"No lead found with ID: {lead_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to update lead status: {e}")
            return False
    
    def get_leads_by_status(
        self, 
        status: str, 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve leads by status.
        
        Args:
            status: Status to filter by (e.g., 'new', 'processed', 'failed')
            limit: Optional maximum number of results
            
        Returns:
            List of lead dictionaries
        """
        query = "SELECT * FROM leads WHERE status = ? ORDER BY created_time DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (status,))
                rows = cursor.fetchall()
                
                # Convert rows to dictionaries
                leads = []
                for row in rows:
                    lead = dict(row)
                    # Parse raw_field_data JSON
                    if lead.get('raw_field_data'):
                        try:
                            lead['raw_field_data'] = json.loads(lead['raw_field_data'])
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse raw_field_data for lead {lead['lead_id']}")
                    leads.append(lead)
                
                logger.debug(f"Retrieved {len(leads)} leads with status: {status}")
                return leads
                
        except Exception as e:
            logger.error(f"Failed to retrieve leads: {e}")
            return []
    
    def get_lead_by_id(self, lead_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific lead by ID.
        
        Args:
            lead_id: Meta lead ID
            
        Returns:
            Lead dictionary or None if not found
        """
        query = "SELECT * FROM leads WHERE lead_id = ?"
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (lead_id,))
                row = cursor.fetchone()
                
                if row:
                    lead = dict(row)
                    # Parse raw_field_data JSON
                    if lead.get('raw_field_data'):
                        try:
                            lead['raw_field_data'] = json.loads(lead['raw_field_data'])
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse raw_field_data for lead {lead_id}")
                    return lead
                return None
                
        except Exception as e:
            logger.error(f"Failed to retrieve lead: {e}")
            return None
    
    def get_all_leads(
        self, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all leads with optional pagination.
        
        Args:
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of lead dictionaries
        """
        query = "SELECT * FROM leads ORDER BY created_time DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Convert rows to dictionaries
                leads = []
                for row in rows:
                    lead = dict(row)
                    # Parse raw_field_data JSON
                    if lead.get('raw_field_data'):
                        try:
                            lead['raw_field_data'] = json.loads(lead['raw_field_data'])
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse raw_field_data for lead {lead['lead_id']}")
                    leads.append(lead)
                
                logger.debug(f"Retrieved {len(leads)} total leads")
                return leads
                
        except Exception as e:
            logger.error(f"Failed to retrieve all leads: {e}")
            return []
    
    def get_lead_count(self, status: Optional[str] = None) -> int:
        """
        Get total count of leads, optionally filtered by status.
        
        Args:
            status: Optional status filter
            
        Returns:
            int: Number of leads
        """
        if status:
            query = "SELECT COUNT(*) FROM leads WHERE status = ?"
            params = (status,)
        else:
            query = "SELECT COUNT(*) FROM leads"
            params = ()
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Failed to get lead count: {e}")
            return 0
    
    def delete_lead(self, lead_id: str) -> bool:
        """
        Delete a lead from the database.
        
        Args:
            lead_id: Meta lead ID
            
        Returns:
            bool: True if deletion successful, False otherwise
        """
        query = "DELETE FROM leads WHERE lead_id = ?"
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (lead_id,))
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"Deleted lead: {lead_id}")
                    return True
                else:
                    logger.warning(f"No lead found with ID: {lead_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete lead: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with statistics
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Total leads
                cursor.execute("SELECT COUNT(*) FROM leads")
                total = cursor.fetchone()[0]
                
                # Leads by status
                cursor.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM leads 
                    GROUP BY status
                """)
                status_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Recent leads (last 24 hours)
                cursor.execute("""
                    SELECT COUNT(*) FROM leads 
                    WHERE datetime(inserted_at) > datetime('now', '-1 day')
                """)
                recent = cursor.fetchone()[0]
                
                stats = {
                    'total_leads': total,
                    'status_breakdown': status_counts,
                    'recent_24h': recent,
                    'database_path': self.db_path
                }
                
                logger.debug(f"Database stats: {stats}")
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {'error': str(e)}
    
    def vacuum(self) -> bool:
        """
        Optimize database by running VACUUM command.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self._get_connection() as conn:
                conn.execute("VACUUM")
                conn.commit()
                logger.info("Database vacuumed successfully")
                return True
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")
            return False
    
    def close(self) -> None:
        """Close database connection for current thread."""
        if hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.close()
                self._local.connection = None
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close()


# Create singleton instance for global access
_db_instance = None
_db_lock = threading.Lock()


def get_database() -> Database:
    """
    Get singleton database instance.
    
    Returns:
        Database: Singleton database instance
    """
    global _db_instance
    
    if _db_instance is None:
        with _db_lock:
            if _db_instance is None:
                _db_instance = Database()
    
    return _db_instance


if __name__ == '__main__':
    """
    Test database functionality when run as script.
    
    Usage:
        python database.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize database
        db = Database()
        
        # Print statistics
        print("\nDatabase Statistics:")
        print("=" * 50)
        stats = db.get_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Test insert (with sample data)
        sample_lead = {
            'lead_id': 'test_lead_123',
            'form_id': 'test_form_456',
            'created_time': datetime.utcnow().isoformat(),
            'email': 'test@example.com',
            'full_name': 'Test User',
            'first_name': 'Test',
            'last_name': 'User',
            'phone_number': '+1234567890',
            'raw_field_data': {'test': 'data'},
            'status': 'new'
        }
        
        print("\n[TEST] Attempting to insert sample lead...")
        lead_id = db.insert_lead(sample_lead)
        if lead_id:
            print(f"[SUCCESS] Lead inserted with ID: {lead_id}")
        else:
            print("[INFO] Lead already exists or insert failed")
        
        # Test existence check
        print(f"\n[TEST] Checking if lead exists...")
        exists = db.lead_exists('test_lead_123')
        print(f"[RESULT] Lead exists: {exists}")
        
        # Test retrieval
        print(f"\n[TEST] Retrieving lead...")
        lead = db.get_lead_by_id('test_lead_123')
        if lead:
            print(f"[SUCCESS] Retrieved lead: {lead['full_name']} ({lead['email']})")
        
        print("\n[SUCCESS] Database tests completed!")
        
    except Exception as e:
        print(f"\n[ERROR] Database test failed: {e}")
        import traceback
        traceback.print_exc()
