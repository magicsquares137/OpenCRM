"""
Credential & Connection Test Script

Tests connectivity to all external services used by the Meta Leads pipeline:
1. Meta Graph API - fetches page info
2. Microsoft Graph API - acquires OAuth2 token and fetches sender profile
3. SQLite Database - verifies schema and reports lead count

Usage:
    python scripts/test_credentials.py
"""

import sys
import os

# Ensure project root is on the path so imports work from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _pad(label: str, width: int = 30) -> str:
    """Right-pad a label with dots for aligned output."""
    dots = '.' * (width - len(label))
    return f"{label} {dots}"


def test_meta_api() -> bool:
    """Test Meta Graph API connectivity."""
    try:
        from meta_client import MetaClient
        client = MetaClient()
        success, message = client.test_connection()
        if success:
            print(f"  {message}")
        else:
            print(f"  FAIL: {message}")
        return success
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def test_microsoft_graph() -> bool:
    """Test Microsoft Graph API connectivity."""
    try:
        from email_client import EmailClient
        client = EmailClient()
        success, message = client.test_connection()
        if success:
            print(f"  {message}")
        else:
            print(f"  FAIL: {message}")
        return success
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def test_database() -> bool:
    """Test database connectivity and report stats."""
    try:
        from database import Database
        db = Database()
        stats = db.get_stats()
        if 'error' in stats:
            print(f"  FAIL: {stats['error']}")
            return False
        total = stats.get('total_leads', 0)
        recent = stats.get('recent_24h', 0)
        breakdown = stats.get('status_breakdown', {})
        parts = [f"{total} leads total", f"{recent} in last 24h"]
        if breakdown:
            parts.append("statuses: " + ", ".join(f"{k}={v}" for k, v in breakdown.items()))
        print(f"  {'; '.join(parts)}")
        db.close()
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    print()
    print("=" * 48)
    print("  CREDENTIAL & CONNECTION TEST")
    print("=" * 48)

    tests = [
        ("Meta Graph API", test_meta_api),
        ("Microsoft Graph API", test_microsoft_graph),
        ("Database", test_database),
    ]

    results = []
    for i, (name, func) in enumerate(tests, 1):
        label = _pad(f"[{i}/{len(tests)}] {name}")
        # Run the test
        success = func()
        tag = "PASS" if success else "FAIL"
        print(f"{label} {tag}")
        print()
        results.append(success)

    print("=" * 48)

    passed = sum(results)
    total = len(results)
    if passed == total:
        print(f"  All {total} checks passed.")
    else:
        print(f"  {passed}/{total} checks passed.")

    print("=" * 48)
    print()

    sys.exit(0 if all(results) else 1)


if __name__ == '__main__':
    main()
