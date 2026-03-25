#!/usr/bin/env python3
"""
Arkitekt OpenCRM - Database Initialization Script

This script initializes the SQLite database with the proper schema,
creates necessary indexes, and optionally seeds test data.

Features:
- Creates leads table with full schema
- Creates all necessary indexes
- Validates database structure
- Optional test data seeding
- Verbose output for troubleshooting
- Can be run standalone or imported

Usage:
    # Standalone execution
    python scripts/init_db.py
    
    # With test data
    python scripts/init_db.py --seed-test-data
    
    # Verbose mode
    python scripts/init_db.py --verbose
    
    # Import in code
    from scripts.init_db import initialize_database
    initialize_database()

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Database, DatabaseError, get_database
from config import config, ConfigurationError


# Configure module logger
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """
    Setup logging configuration for the script.
    
    Args:
        verbose: Enable verbose (DEBUG) logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def validate_database_schema(db: Database) -> bool:
    """
    Validate that the database has the correct schema.
    
    Args:
        db: Database instance
        
    Returns:
        bool: True if schema is valid, False otherwise
    """
    print("\n[VALIDATION] Checking database schema...")
    
    try:
        with db._get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if leads table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='leads'
            """)
            
            if not cursor.fetchone():
                print("[ERROR] Leads table does not exist")
                return False
            
            print("[✓] Leads table exists")
            
            # Check table schema
            cursor.execute("PRAGMA table_info(leads)")
            columns = cursor.fetchall()
            
            expected_columns = [
                'id', 'lead_id', 'form_id', 'created_time', 'email',
                'full_name', 'first_name', 'last_name', 'phone_number',
                'company_name', 'job_title', 'raw_field_data', 'status',
                'email_sent_at', 'inserted_at', 'updated_at'
            ]
            
            actual_columns = [col[1] for col in columns]
            
            for expected_col in expected_columns:
                if expected_col in actual_columns:
                    print(f"[✓] Column '{expected_col}' exists")
                else:
                    print(f"[ERROR] Column '{expected_col}' is missing")
                    return False
            
            # Check indexes
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='index' AND tbl_name='leads'
            """)
            
            indexes = [row[0] for row in cursor.fetchall()]
            
            expected_indexes = [
                'idx_lead_id', 'idx_status', 'idx_email',
                'idx_created_time', 'idx_status_created'
            ]
            
            print("\n[VALIDATION] Checking indexes...")
            for expected_idx in expected_indexes:
                if expected_idx in indexes:
                    print(f"[✓] Index '{expected_idx}' exists")
                else:
                    print(f"[WARNING] Index '{expected_idx}' is missing")
            
            print("\n[SUCCESS] Database schema validation complete!")
            return True
            
    except Exception as e:
        print(f"[ERROR] Schema validation failed: {e}")
        logger.error(f"Schema validation error: {e}")
        return False


def seed_test_data(db: Database, count: int = 5) -> bool:
    """
    Seed the database with test data.
    
    Args:
        db: Database instance
        count: Number of test records to create
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n[SEED] Inserting {count} test leads...")
    
    test_leads = [
        {
            'lead_id': f'test_lead_{i}',
            'form_id': 'test_form_001',
            'created_time': datetime.utcnow().isoformat(),
            'email': f'test{i}@example.com',
            'full_name': f'Test User {i}',
            'first_name': 'Test',
            'last_name': f'User{i}',
            'phone_number': f'+1555000{i:04d}',
            'company_name': f'Test Company {i}',
            'job_title': 'Test Manager',
            'raw_field_data': {
                'email': f'test{i}@example.com',
                'full_name': f'Test User {i}',
                'phone': f'+1555000{i:04d}'
            },
            'status': 'intake' if i % 2 == 0 else 'emailed'
        }
        for i in range(1, count + 1)
    ]
    
    inserted_count = 0
    failed_count = 0
    
    for lead in test_leads:
        try:
            lead_id = db.insert_lead(lead)
            if lead_id:
                inserted_count += 1
                print(f"[✓] Inserted test lead: {lead['email']}")
            else:
                failed_count += 1
                print(f"[SKIP] Lead already exists: {lead['email']}")
        except Exception as e:
            failed_count += 1
            print(f"[ERROR] Failed to insert lead {lead['email']}: {e}")
    
    print(f"\n[SEED] Results: {inserted_count} inserted, {failed_count} failed/skipped")
    return inserted_count > 0


def print_database_stats(db: Database):
    """
    Print database statistics.
    
    Args:
        db: Database instance
    """
    print("\n" + "=" * 80)
    print("DATABASE STATISTICS")
    print("=" * 80)
    
    try:
        stats = db.get_stats()
        
        print(f"\nDatabase Path: {stats.get('database_path', 'N/A')}")
        print(f"Total Leads:   {stats.get('total_leads', 0)}")
        print(f"Recent (24h):  {stats.get('recent_24h', 0)}")
        
        status_breakdown = stats.get('status_breakdown', {})
        if status_breakdown:
            print("\nStatus Breakdown:")
            for status, count in status_breakdown.items():
                print(f"  {status:15} {count:>5}")
        else:
            print("\nNo leads in database yet.")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Failed to get database stats: {e}")
        logger.error(f"Stats error: {e}")


def initialize_database(
    seed_data: bool = False,
    test_data_count: int = 5,
    verbose: bool = False
) -> bool:
    """
    Initialize the database with proper schema and optionally seed test data.
    
    Args:
        seed_data: Whether to seed test data
        test_data_count: Number of test records to create
        verbose: Enable verbose output
        
    Returns:
        bool: True if successful, False otherwise
    """
    print("\n" + "=" * 80)
    print("META LEADS OUTLOOK AUTOMATION - DATABASE INITIALIZATION")
    print("=" * 80)
    
    try:
        # Initialize database (schema creation happens automatically)
        print("\n[INIT] Initializing database...")
        db = Database()
        print(f"[SUCCESS] Database initialized at: {db.db_path}")
        
        # Validate schema
        if not validate_database_schema(db):
            print("\n[ERROR] Database schema validation failed!")
            return False
        
        # Seed test data if requested
        if seed_data:
            if not seed_test_data(db, count=test_data_count):
                print("\n[WARNING] No test data was inserted")
        
        # Print statistics
        print_database_stats(db)
        
        print("\n[SUCCESS] Database initialization complete!")
        print("=" * 80 + "\n")
        
        return True
        
    except ConfigurationError as e:
        print(f"\n[ERROR] Configuration error: {e}")
        print("\nPlease ensure your .env file is properly configured.")
        print("See .env.example for required configuration values.\n")
        return False
        
    except DatabaseError as e:
        print(f"\n[ERROR] Database error: {e}")
        logger.error(f"Database initialization failed: {e}")
        return False
        
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        logger.error(f"Initialization error: {e}")
        import traceback
        if verbose:
            traceback.print_exc()
        return False


def drop_and_recreate(db: Database) -> bool:
    """
    Drop and recreate the database (use with caution!).
    
    Args:
        db: Database instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    print("\n[WARNING] This will DELETE all existing data!")
    response = input("Are you sure you want to continue? (yes/no): ")
    
    if response.lower() != 'yes':
        print("[CANCELLED] Operation cancelled by user")
        return False
    
    try:
        with db._get_connection() as conn:
            cursor = conn.cursor()
            
            # Drop existing tables and indexes
            print("\n[DROP] Dropping existing tables and indexes...")
            cursor.execute("DROP TABLE IF EXISTS leads")
            
            # Drop indexes (if they exist)
            indexes = ['idx_lead_id', 'idx_status', 'idx_email', 
                      'idx_created_time', 'idx_status_created']
            for idx in indexes:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {idx}")
                except:
                    pass
            
            conn.commit()
            print("[SUCCESS] Existing tables and indexes dropped")
        
        # Recreate schema
        print("\n[CREATE] Recreating database schema...")
        db._initialize_schema()
        print("[SUCCESS] Database schema recreated")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to drop and recreate database: {e}")
        logger.error(f"Drop/recreate error: {e}")
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Initialize Arkitekt OpenCRM database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Basic initialization
  %(prog)s --seed-test-data         # Initialize with test data
  %(prog)s --verbose                # Verbose output
  %(prog)s --recreate               # Drop and recreate (CAUTION!)
        """
    )
    
    parser.add_argument(
        '--seed-test-data',
        action='store_true',
        help='Seed database with test data'
    )
    
    parser.add_argument(
        '--test-data-count',
        type=int,
        default=5,
        help='Number of test records to create (default: 5)'
    )
    
    parser.add_argument(
        '--recreate',
        action='store_true',
        help='Drop and recreate database (WARNING: deletes all data!)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Only print database statistics (no initialization)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    try:
        # Stats only mode
        if args.stats_only:
            db = get_database()
            print_database_stats(db)
            sys.exit(0)
        
        # Recreate mode
        if args.recreate:
            db = Database()
            if drop_and_recreate(db):
                print("\n[INFO] Proceeding with initialization...")
            else:
                print("\n[ERROR] Failed to recreate database")
                sys.exit(1)
        
        # Initialize database
        success = initialize_database(
            seed_data=args.seed_test_data,
            test_data_count=args.test_data_count,
            verbose=args.verbose
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n[CANCELLED] Operation cancelled by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        logger.error(f"Main error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
