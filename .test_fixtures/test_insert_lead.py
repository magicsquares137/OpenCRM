#!/usr/bin/env python3
"""
Test script to insert mock lead data into the database.
"""
import sys
import os
sys.path.insert(0, '/app')

from database import get_database
from datetime import datetime
import json

# Create test lead data
test_lead = {
    'lead_id': 'test_lead_qa_verification',
    'form_id': 'test_form_123',
    'created_time': datetime.utcnow().isoformat(),
    'email': 'testuser@example.com',
    'full_name': 'John Test User',
    'first_name': 'John',
    'last_name': 'User',
    'phone_number': '+1234567890',
    'company_name': 'Test Company Inc',
    'job_title': 'Test Manager',
    'raw_field_data': json.dumps([
        {'name': 'email', 'values': ['testuser@example.com']},
        {'name': 'full_name', 'values': ['John Test User']},
        {'name': 'phone_number', 'values': ['+1234567890']},
        {'name': 'company_name', 'values': ['Test Company Inc']}
    ]),
    'status': 'intake'  # This status should trigger email sending
}

print("Inserting test lead into database...")
try:
    db = get_database()
    lead_id = db.insert_lead(test_lead)
    if lead_id:
        print(f"✅ Test lead inserted successfully with ID: {lead_id}")
        
        # Verify it exists
        retrieved_lead = db.get_lead_by_id('test_lead_qa_verification')
        if retrieved_lead:
            print(f"✅ Lead verified in database: {retrieved_lead['full_name']} ({retrieved_lead['email']})")
            print(f"   Status: {retrieved_lead['status']}")
            print(f"   Lead ID: {retrieved_lead['lead_id']}")
        else:
            print("❌ Failed to retrieve inserted lead")
    else:
        print("❌ Failed to insert lead")
        
    # Show database stats
    stats = db.get_stats()
    print(f"\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("Test completed.")