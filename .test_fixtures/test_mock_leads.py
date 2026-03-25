#!/usr/bin/env python3
"""
Test script to verify lead processing workflow with mock data
"""
import sys
import os
sys.path.insert(0, '/app')

from database import get_database
from datetime import datetime
import json

def test_database_operations():
    """Test database operations with mock data"""
    print("\n=== Testing Database Operations ===")
    
    # Get database instance
    db = get_database()
    
    # Test 1: Insert mock lead
    mock_lead = {
        'lead_id': 'mock_lead_001',
        'form_id': 'mock_form_123',
        'created_time': datetime.utcnow().isoformat(),
        'email': 'test@example.com',
        'full_name': 'John Doe',
        'first_name': 'John',
        'last_name': 'Doe',
        'phone_number': '+1-555-0123',
        'company_name': 'Test Corp',
        'job_title': 'Manager',
        'raw_field_data': {
            'email': 'test@example.com',
            'full_name': 'John Doe',
            'phone': '+1-555-0123'
        },
        'status': 'intake'
    }
    
    try:
        print(f"Inserting mock lead: {mock_lead['email']}")
        result = db.insert_lead(mock_lead)
        print(f"Insert result: {result}")
        
        # Test 2: Check if lead exists
        print(f"Checking if lead exists...")
        exists = db.lead_exists(mock_lead['lead_id'])
        print(f"Lead exists: {exists}")
        
        # Test 3: Get lead by ID
        print(f"Retrieving lead by ID...")
        retrieved_lead = db.get_lead_by_id(mock_lead['lead_id'])
        if retrieved_lead:
            print(f"Retrieved: {retrieved_lead['full_name']} ({retrieved_lead['status']})")
        else:
            print("Lead not found!")
            
        # Test 4: Get leads by status
        print(f"Getting leads with status 'intake'...")
        intake_leads = db.get_leads_by_status('intake')
        print(f"Found {len(intake_leads)} intake leads")
        
        # Test 5: Update lead status
        print(f"Updating lead status to 'emailed'...")
        success = db.update_lead_status(mock_lead['lead_id'], 'emailed', email_sent_at=datetime.utcnow().isoformat())
        print(f"Status update success: {success}")
        
        # Test 6: Verify update
        updated_lead = db.get_lead_by_id(mock_lead['lead_id'])
        if updated_lead:
            print(f"Updated status: {updated_lead['status']}")
            print(f"Email sent at: {updated_lead['email_sent_at']}")
        
        # Test 7: Database stats
        print("\nDatabase Statistics:")
        stats = db.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
            
        return True
        
    except Exception as e:
        print(f"Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_database_operations()
    print(f"\n=== Test Result: {'PASSED' if success else 'FAILED'} ===")
    sys.exit(0 if success else 1)