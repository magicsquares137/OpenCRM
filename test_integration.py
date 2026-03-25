#!/usr/bin/env python3
"""
Arkitekt OpenCRM - Integration Test Suite

This script provides comprehensive integration testing for all components
of the Arkitekt OpenCRM system.

Features:
- Environment configuration validation
- Database connectivity and CRUD operations testing
- Meta API connection and lead fetching tests
- Microsoft Graph API authentication tests
- Email template generation validation
- Health check endpoint testing
- Test database setup and teardown
- Detailed pass/fail reporting

Usage:
    # Run locally
    python test_integration.py
    
    # Run in Docker
    docker-compose run lead-pipeline python test_integration.py
    
    # Run with verbose output
    python test_integration.py --verbose

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import sys
import os
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Import application modules
from config import config, ConfigurationError
from database import Database, DatabaseError
from meta_client import MetaClient, MetaAPIError
from email_client import EmailClient, EmailError
from templates import generate_lead_email, EMAIL_SUBJECT
from health_check import start_health_check_server, stop_health_check_server


# Test results tracking
test_results = {
    'passed': [],
    'failed': [],
    'warnings': []
}


def print_header(text: str):
    """Print formatted section header."""
    print("\n" + "=" * 80)
    print(text.center(80))
    print("=" * 80 + "\n")


def print_test_result(test_name: str, passed: bool, message: str = "", warning: bool = False):
    """
    Print test result and track in results dict.
    
    Args:
        test_name: Name of the test
        passed: Whether test passed
        message: Additional message
        warning: Whether this is a warning (not failure)
    """
    if warning:
        status = "⚠ WARNING"
        color = "\033[93m"  # Yellow
        test_results['warnings'].append(test_name)
    elif passed:
        status = "✓ PASS"
        color = "\033[92m"  # Green
        test_results['passed'].append(test_name)
    else:
        status = "✗ FAIL"
        color = "\033[91m"  # Red
        test_results['failed'].append(test_name)
    
    reset = "\033[0m"
    
    print(f"{color}[{status}]{reset} {test_name}")
    if message:
        print(f"        {message}")


def test_configuration() -> bool:
    """
    Test configuration loading and validation.
    
    Returns:
        bool: True if all configuration tests pass
    """
    print_header("CONFIGURATION TESTS")
    
    all_passed = True
    
    # Test 1: Configuration loaded
    try:
        if config._initialized:
            print_test_result("Configuration Loading", True, "Configuration loaded successfully")
        else:
            print_test_result("Configuration Loading", False, "Configuration not initialized")
            all_passed = False
    except Exception as e:
        print_test_result("Configuration Loading", False, f"Error: {e}")
        all_passed = False
    
    # Test 2: Required variables present
    try:
        required_vars = {
            'META_PAGE_ID': config.meta_page_id,
            'META_PAGE_ACCESS_TOKEN': config.meta_page_access_token,
            'MS_TENANT_ID': config.ms_tenant_id,
            'MS_CLIENT_ID': config.ms_client_id,
            'MS_CLIENT_SECRET': config.ms_client_secret,
            'MS_SENDER_EMAIL': config.ms_sender_email,
            'BOOKING_URL': config.booking_url
        }
        
        missing_vars = []
        for var_name, var_value in required_vars.items():
            if not var_value or var_value.startswith('YOUR_') or var_value == '<REQUIRED>':
                missing_vars.append(var_name)
        
        if missing_vars:
            print_test_result("Required Variables", False, 
                            f"Missing or invalid: {', '.join(missing_vars)}")
            all_passed = False
        else:
            print_test_result("Required Variables", True, 
                            "All required variables present")
    except Exception as e:
        print_test_result("Required Variables", False, f"Error: {e}")
        all_passed = False
    
    # Test 3: Configuration values valid
    try:
        issues = []
        
        if config.poll_interval_seconds < 60:
            issues.append("Poll interval too short (< 60s)")
        
        if config.batch_size > 500:
            issues.append("Batch size exceeds Meta API limit (> 500)")
        
        if config.log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            issues.append(f"Invalid log level: {config.log_level}")
        
        if issues:
            print_test_result("Configuration Values", False, 
                            "; ".join(issues))
            all_passed = False
        else:
            print_test_result("Configuration Values", True, 
                            "All configuration values valid")
    except Exception as e:
        print_test_result("Configuration Values", False, f"Error: {e}")
        all_passed = False
    
    return all_passed


def test_database() -> bool:
    """
    Test database connectivity and operations.
    
    Returns:
        bool: True if all database tests pass
    """
    print_header("DATABASE TESTS")
    
    all_passed = True
    test_db_path = "data/test_leads.db"
    test_lead_id = f"test_integration_{int(time.time())}"
    
    # Cleanup old test database if exists
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    db = None
    
    try:
        # Test 1: Database initialization
        try:
            db = Database(db_path=test_db_path)
            print_test_result("Database Initialization", True, 
                            f"Database created at {test_db_path}")
        except Exception as e:
            print_test_result("Database Initialization", False, f"Error: {e}")
            all_passed = False
            return all_passed
        
        # Test 2: Insert lead
        try:
            test_lead = {
                'lead_id': test_lead_id,
                'form_id': 'test_form',
                'created_time': datetime.utcnow().isoformat(),
                'email': 'test@integration.com',
                'full_name': 'Integration Test User',
                'first_name': 'Integration',
                'last_name': 'Test',
                'phone_number': '+15555551234',
                'company_name': 'Test Company',
                'job_title': 'Test Engineer',
                'raw_field_data': {'test': 'data'},
                'status': 'intake'
            }
            
            lead_id = db.insert_lead(test_lead)
            if lead_id:
                print_test_result("Lead Insertion", True, 
                                f"Lead inserted with DB ID: {lead_id}")
            else:
                print_test_result("Lead Insertion", False, "Insert returned None")
                all_passed = False
        except Exception as e:
            print_test_result("Lead Insertion", False, f"Error: {e}")
            all_passed = False
        
        # Test 3: Lead exists check
        try:
            exists = db.lead_exists(test_lead_id)
            if exists:
                print_test_result("Lead Exists Check", True, 
                                "Lead found in database")
            else:
                print_test_result("Lead Exists Check", False, 
                                "Lead not found after insertion")
                all_passed = False
        except Exception as e:
            print_test_result("Lead Exists Check", False, f"Error: {e}")
            all_passed = False
        
        # Test 4: Retrieve lead
        try:
            lead = db.get_lead_by_id(test_lead_id)
            if lead and lead['email'] == 'test@integration.com':
                print_test_result("Lead Retrieval", True, 
                                f"Lead retrieved: {lead['full_name']}")
            else:
                print_test_result("Lead Retrieval", False, 
                                "Lead not retrieved or data mismatch")
                all_passed = False
        except Exception as e:
            print_test_result("Lead Retrieval", False, f"Error: {e}")
            all_passed = False
        
        # Test 5: Update lead status
        try:
            success = db.update_lead_status(test_lead_id, 'emailed', 
                                           datetime.utcnow().isoformat())
            if success:
                updated_lead = db.get_lead_by_id(test_lead_id)
                if updated_lead and updated_lead['status'] == 'emailed':
                    print_test_result("Lead Status Update", True, 
                                    "Status updated to 'emailed'")
                else:
                    print_test_result("Lead Status Update", False, 
                                    "Status not updated correctly")
                    all_passed = False
            else:
                print_test_result("Lead Status Update", False, "Update failed")
                all_passed = False
        except Exception as e:
            print_test_result("Lead Status Update", False, f"Error: {e}")
            all_passed = False
        
        # Test 6: Get database statistics
        try:
            stats = db.get_stats()
            if 'total_leads' in stats and stats['total_leads'] >= 1:
                print_test_result("Database Statistics", True, 
                                f"Total leads: {stats['total_leads']}")
            else:
                print_test_result("Database Statistics", False, 
                                "Invalid stats returned")
                all_passed = False
        except Exception as e:
            print_test_result("Database Statistics", False, f"Error: {e}")
            all_passed = False
        
    finally:
        # Cleanup: Close database and remove test file
        if db:
            db.close()
        
        if os.path.exists(test_db_path):
            try:
                os.remove(test_db_path)
                print("\nTest database cleaned up")
            except:
                print("\nWarning: Could not clean up test database")
    
    return all_passed


def test_meta_api() -> bool:
    """
    Test Meta API connectivity and operations.
    
    Returns:
        bool: True if Meta API tests pass
    """
    print_header("META API TESTS")
    
    all_passed = True
    
    try:
        # Test 1: Client initialization
        try:
            client = MetaClient()
            print_test_result("Meta Client Initialization", True, 
                            f"Client initialized for page: {config.meta_page_id[:8]}...")
        except Exception as e:
            print_test_result("Meta Client Initialization", False, f"Error: {e}")
            return False
        
        # Test 2: API connection test
        try:
            success, message = client.test_connection()
            if success:
                print_test_result("Meta API Connection", True, message)
            else:
                print_test_result("Meta API Connection", False, message)
                all_passed = False
        except Exception as e:
            print_test_result("Meta API Connection", False, f"Error: {e}")
            all_passed = False
        
        # Test 3: Fetch forms
        try:
            forms = client.get_leadgen_forms()
            if forms:
                print_test_result("Fetch Leadgen Forms", True, 
                                f"Found {len(forms)} form(s)")
                
                # Test 4: Fetch leads (if forms exist)
                try:
                    first_form = forms[0]
                    form_id = first_form.get('id')
                    leads = client.get_leads_for_form(form_id, limit=5)
                    
                    if leads:
                        print_test_result("Fetch Leads", True, 
                                        f"Retrieved {len(leads)} lead(s) from form")
                    else:
                        print_test_result("Fetch Leads", True, 
                                        "No leads found (form may be empty)", 
                                        warning=True)
                except Exception as e:
                    print_test_result("Fetch Leads", False, f"Error: {e}")
                    all_passed = False
            else:
                print_test_result("Fetch Leadgen Forms", True, 
                                "No forms found (page may have no lead ads)", 
                                warning=True)
        except Exception as e:
            print_test_result("Fetch Leadgen Forms", False, f"Error: {e}")
            all_passed = False
    
    except Exception as e:
        print_test_result("Meta API Tests", False, f"Unexpected error: {e}")
        all_passed = False
    
    return all_passed


def test_microsoft_graph_api() -> bool:
    """
    Test Microsoft Graph API authentication.
    
    Returns:
        bool: True if Graph API tests pass
    """
    print_header("MICROSOFT GRAPH API TESTS")
    
    all_passed = True
    
    try:
        # Test 1: Client initialization
        try:
            client = EmailClient()
            print_test_result("Email Client Initialization", True, 
                            f"Client initialized for {config.ms_sender_email}")
        except Exception as e:
            print_test_result("Email Client Initialization", False, f"Error: {e}")
            return False
        
        # Test 2: Token acquisition
        try:
            token = client._get_access_token()
            if token:
                print_test_result("Access Token Acquisition", True, 
                                f"Token acquired (length: {len(token)})")
            else:
                print_test_result("Access Token Acquisition", False, 
                                "Token is empty")
                all_passed = False
        except Exception as e:
            print_test_result("Access Token Acquisition", False, f"Error: {e}")
            all_passed = False
        
        # Test 3: API connection test
        try:
            success, message = client.test_connection()
            if success:
                print_test_result("Graph API Connection", True, message)
            else:
                print_test_result("Graph API Connection", False, message)
                all_passed = False
        except Exception as e:
            print_test_result("Graph API Connection", False, f"Error: {e}")
            all_passed = False
    
    except Exception as e:
        print_test_result("Graph API Tests", False, f"Unexpected error: {e}")
        all_passed = False
    
    return all_passed


def test_email_templates() -> bool:
    """
    Test email template generation.
    
    Returns:
        bool: True if template tests pass
    """
    print_header("EMAIL TEMPLATE TESTS")
    
    all_passed = True
    
    # Test 1: Generate email with full data
    try:
        test_lead = {
            'first_name': 'John',
            'full_name': 'John Doe',
            'email': 'john.doe@example.com',
            'phone': '+15555551234',
            'company': 'Test Company',
            'job_title': 'Test Manager',
            'created_time': datetime.utcnow().isoformat(),
            'lead_id': 'test_123'
        }
        
        html = generate_lead_email(**test_lead)
        
        if html and len(html) > 100:
            # Check for required elements
            checks = [
                ('Greeting' in html or 'Hi' in html or 'Hello' in html),
                ('John' in html),
            ]
            
            if all(checks):
                print_test_result("Email Template Generation", True, 
                                f"Template generated ({len(html)} chars)")
            else:
                print_test_result("Email Template Generation", False, 
                                "Template missing required elements")
                all_passed = False
        else:
            print_test_result("Email Template Generation", False, 
                            "Template too short or empty")
            all_passed = False
    except Exception as e:
        print_test_result("Email Template Generation", False, f"Error: {e}")
        all_passed = False
    
    # Test 2: Generate email with minimal data (fallbacks)
    try:
        minimal_lead = {
            'first_name': None,
            'full_name': 'Anonymous User'
        }
        
        html = generate_lead_email(**minimal_lead)
        
        if html and len(html) > 100:
            print_test_result("Email Template Fallbacks", True, 
                            "Template handles missing data correctly")
        else:
            print_test_result("Email Template Fallbacks", False, 
                            "Template failed with minimal data")
            all_passed = False
    except Exception as e:
        print_test_result("Email Template Fallbacks", False, f"Error: {e}")
        all_passed = False
    
    # Test 3: Email subject
    try:
        if EMAIL_SUBJECT and isinstance(EMAIL_SUBJECT, str):
            print_test_result("Email Subject Constant", True, 
                            f"Subject: '{EMAIL_SUBJECT}'")
        else:
            print_test_result("Email Subject Constant", False, 
                            "EMAIL_SUBJECT not defined or invalid")
            all_passed = False
    except Exception as e:
        print_test_result("Email Subject Constant", False, f"Error: {e}")
        all_passed = False
    
    return all_passed


def test_health_check() -> bool:
    """
    Test health check endpoint.
    
    Returns:
        bool: True if health check tests pass
    """
    print_header("HEALTH CHECK TESTS")
    
    all_passed = True
    health_check_thread = None
    
    try:
        # Test 1: Start health check server
        try:
            health_check_thread = start_health_check_server()
            if health_check_thread:
                print_test_result("Health Check Server Start", True, 
                                f"Server started on port {config.health_check_port}")
                time.sleep(2)  # Wait for server to be ready
            else:
                print_test_result("Health Check Server Start", True, 
                                "Server not started (may be disabled)", 
                                warning=True)
                return all_passed
        except Exception as e:
            print_test_result("Health Check Server Start", False, f"Error: {e}")
            return False
        
        # Test 2: Test health endpoint
        try:
            import requests
            url = f"http://localhost:{config.health_check_port}/health"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                healthy = data.get('healthy', False)
                
                if healthy:
                    print_test_result("Health Endpoint Response", True, 
                                    "System reported as healthy")
                else:
                    print_test_result("Health Endpoint Response", False, 
                                    f"System reported as unhealthy: {data}")
                    all_passed = False
            else:
                print_test_result("Health Endpoint Response", False, 
                                f"HTTP {response.status_code}")
                all_passed = False
        except Exception as e:
            print_test_result("Health Endpoint Response", False, f"Error: {e}")
            all_passed = False
        
        # Test 3: Test root endpoint
        try:
            url = f"http://localhost:{config.health_check_port}/"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if 'service' in data and 'version' in data:
                    print_test_result("Root Endpoint Response", True, 
                                    f"Service: {data['service']}")
                else:
                    print_test_result("Root Endpoint Response", False, 
                                    "Missing expected fields in response")
                    all_passed = False
            else:
                print_test_result("Root Endpoint Response", False, 
                                f"HTTP {response.status_code}")
                all_passed = False
        except Exception as e:
            print_test_result("Root Endpoint Response", False, f"Error: {e}")
            all_passed = False
    
    finally:
        # Stop health check server
        if health_check_thread:
            try:
                stop_health_check_server()
                print("\nHealth check server stopped")
            except:
                print("\nWarning: Could not stop health check server")
    
    return all_passed


def print_summary():
    """Print test execution summary."""
    print_header("TEST SUMMARY")
    
    total_tests = len(test_results['passed']) + len(test_results['failed']) + len(test_results['warnings'])
    passed = len(test_results['passed'])
    failed = len(test_results['failed'])
    warnings = len(test_results['warnings'])
    
    success_rate = (passed / total_tests * 100) if total_tests > 0 else 0
    
    print(f"Total Tests:    {total_tests}")
    print(f"✓ Passed:      {passed}")
    print(f"✗ Failed:      {failed}")
    print(f"⚠ Warnings:    {warnings}")
    print(f"Success Rate:  {success_rate:.1f}%")
    
    if failed > 0:
        print(f"\n\033[91mFailed Tests:\033[0m")
        for test in test_results['failed']:
            print(f"  - {test}")
    
    if warnings > 0:
        print(f"\n\033[93mWarnings:\033[0m")
        for test in test_results['warnings']:
            print(f"  - {test}")
    
    print("\n" + "=" * 80)
    
    if failed == 0:
        print("\033[92m✓ ALL TESTS PASSED!\033[0m".center(80))
    else:
        print("\033[91m✗ SOME TESTS FAILED\033[0m".center(80))
    
    print("=" * 80 + "\n")


def main():
    """Main test execution."""
    parser = argparse.ArgumentParser(
        description='Arkitekt OpenCRM - Integration Tests'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--skip-api',
        action='store_true',
        help='Skip API tests (useful for offline testing)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Print banner
    print("\n" + "=" * 80)
    print("META LEADS OUTLOOK AUTOMATION - INTEGRATION TEST SUITE".center(80))
    print("=" * 80)
    print(f"\nStarted: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Environment: {config.environment}")
    
    all_passed = True
    
    # Run test suites
    try:
        # Configuration tests
        if not test_configuration():
            all_passed = False
        
        # Database tests
        if not test_database():
            all_passed = False
        
        # API tests (optional)
        if not args.skip_api:
            if not test_meta_api():
                all_passed = False
            
            if not test_microsoft_graph_api():
                all_passed = False
        else:
            print_header("API TESTS SKIPPED")
            print("API tests skipped (--skip-api flag set)\n")
        
        # Template tests
        if not test_email_templates():
            all_passed = False
        
        # Health check tests
        if not test_health_check():
            all_passed = False
    
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n\nUnexpected error during testing: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    # Print summary
    print_summary()
    
    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
