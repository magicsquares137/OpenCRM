"""
Arkitekt OpenCRM

Web dashboard for viewing and managing leads collected by the Arkitekt OpenCRM pipeline.
Includes admin authentication, table view, and kanban board.

Usage:
    python dashboard.py

Environment variables:
    DB_PATH              - Path to SQLite database (default: data/leads.db)
    DASHBOARD_PORT       - Port to run dashboard on (default: 5050)
    DASHBOARD_USERNAME   - Login username (default: admin)
    DASHBOARD_PASSWORD   - Login password (required for production)
    DASHBOARD_SECRET_KEY - Flask session secret (generated if not set)
"""

import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, jsonify, request, Response, session, redirect, url_for


def _hash_password(password: str) -> str:
    """Hash a password with a random salt using SHA-256."""
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}${h}"


def _check_password(password: str, stored: str) -> bool:
    """Verify a password against a stored salt$hash."""
    if '$' not in stored:
        return password == stored  # legacy plain-text fallback
    salt, h = stored.split('$', 1)
    return hashlib.sha256((salt + password).encode()).hexdigest() == h

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = os.getenv('DB_PATH', os.path.join(os.path.dirname(__file__), 'data', 'leads.db'))
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '5050'))

DASHBOARD_USERNAME = os.getenv('DASHBOARD_USERNAME', 'admin')
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD', 'admin')
WEBHOOK_API_KEY = os.getenv('WEBHOOK_API_KEY', '').strip()

# Mailgun config (optional — for campaign sends)
MAILGUN_API_KEY = os.getenv('MAILGUN_API_KEY', '').strip()
MAILGUN_DOMAIN = os.getenv('MAILGUN_DOMAIN', '').strip()
MAILGUN_SENDER_EMAIL = os.getenv('MAILGUN_SENDER_EMAIL', '').strip()
MAILGUN_REGION = os.getenv('MAILGUN_REGION', 'us').strip().lower()
app.secret_key = os.getenv('DASHBOARD_SECRET_KEY', secrets.token_hex(32))

# Whitelist of columns that may be used for sorting (prevents SQL injection)
ALLOWED_SORT_COLUMNS = {
    'id', 'lead_id', 'email', 'full_name', 'phone_number',
    'company_name', 'status', 'created_time', 'inserted_at', 'assigned_to',
    'follow_up_date', 'deal_value', 'expected_close_date',
}

# Whitelist of statuses a lead can be moved to
ALLOWED_STATUSES = {
    'intake', 'emailed', 'contacted', 'qualified', 'closed',
    'failed', 'skipped_invalid_email',
}

# Kanban column order (subset shown as board columns)
KANBAN_COLUMNS = ['intake', 'emailed', 'contacted', 'qualified', 'closed']

# LLM config (optional — for AI email generation)
# Supports OpenAI-compatible APIs (OpenAI, Ollama, Together, Groq) and Anthropic
LLM_API_KEY = (os.getenv('LLM_API_KEY', '') or os.getenv('CLAUDE_API_KEY', '')).strip()
LLM_MODEL = (os.getenv('LLM_MODEL', '') or os.getenv('CLAUDE_MODEL', '')).strip() or 'gpt-4o'
LLM_BASE_URL = os.getenv('LLM_BASE_URL', '').strip()
LLM_PROVIDER = os.getenv('LLM_PROVIDER', '').strip().lower()
if not LLM_PROVIDER:
    LLM_PROVIDER = 'anthropic' if LLM_API_KEY.startswith('sk-ant-') else 'openai'
SENDER_NAME = os.getenv('SENDER_NAME', '')

# Predefined tag options for dropdowns
TAG_OPTIONS = {
    'industries': [
        'Technology', 'Healthcare', 'Finance', 'Real Estate', 'Retail',
        'Manufacturing', 'Education', 'Legal', 'Construction', 'Hospitality',
        'Marketing', 'Consulting', 'Nonprofit', 'Other',
    ],
    'lead_sources': [
        'Conference', 'Referral', 'Website', 'LinkedIn', 'Cold Outreach',
        'Networking Event', 'Trade Show', 'Partner', 'Other',
    ],
}

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

_migrated = False

def _run_migrations(conn: sqlite3.Connection) -> None:
    """Run schema migrations."""
    global _migrated
    if _migrated:
        return
    existing = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
    if 'tags' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN tags TEXT DEFAULT '{}'")
    if 'lead_source' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN lead_source TEXT DEFAULT 'meta'")
    if 'assigned_to' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN assigned_to TEXT DEFAULT ''")
    if 'follow_up_date' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN follow_up_date TEXT DEFAULT NULL")
    if 'deal_value' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN deal_value REAL DEFAULT NULL")
    if 'expected_close_date' not in existing:
        conn.execute("ALTER TABLE leads ADD COLUMN expected_close_date TEXT DEFAULT NULL")

    # Users table
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        display_name TEXT NOT NULL DEFAULT '',
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Lead notes table
    conn.execute("""CREATE TABLE IF NOT EXISTS lead_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id TEXT NOT NULL,
        author TEXT NOT NULL DEFAULT '',
        content TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_notes_lead_id ON lead_notes(lead_id)")

    # Email sequences
    conn.execute("""CREATE TABLE IF NOT EXISTS sequences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        steps TEXT NOT NULL DEFAULT '[]',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    conn.execute("""CREATE TABLE IF NOT EXISTS sequence_enrollments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id TEXT NOT NULL,
        sequence_id INTEGER NOT NULL,
        current_step INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',
        next_send_at TEXT,
        enrolled_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE,
        FOREIGN KEY (sequence_id) REFERENCES sequences(id) ON DELETE CASCADE
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_lead ON sequence_enrollments(lead_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_next_send ON sequence_enrollments(status, next_send_at)")

    # Campaigns
    conn.execute("""CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        campaign_type TEXT NOT NULL DEFAULT 'single',
        email_provider TEXT NOT NULL DEFAULT 'outlook',
        subject_template TEXT NOT NULL DEFAULT '',
        body_template TEXT NOT NULL DEFAULT '',
        lead_filters TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'draft',
        total_recipients INTEGER NOT NULL DEFAULT 0,
        sent_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        sent_at TEXT
    )""")

    conn.execute("""CREATE TABLE IF NOT EXISTS campaign_sends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL,
        lead_id TEXT NOT NULL,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        sent_at TEXT,
        error TEXT,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_sends_campaign ON campaign_sends(campaign_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_sends_status ON campaign_sends(campaign_id, status)")

    # Bootstrap admin from env if users table is empty
    cur = conn.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        conn.execute(
            "INSERT INTO users (username, password, display_name, role) VALUES (?, ?, ?, ?)",
            (DASHBOARD_USERNAME, _hash_password(DASHBOARD_PASSWORD), 'Admin', 'admin'),
        )

    conn.commit()
    _migrated = True

def get_db() -> sqlite3.Connection:
    """Open a read-write connection to the leads database."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    _run_migrations(conn)
    return conn


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Unauthorized'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Unauthorized'}), 401
            return redirect(url_for('login'))
        if session.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = ''
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        finally:
            conn.close()
        if row and _check_password(password, row['password']):
            session['authenticated'] = True
            session['username'] = row['username']
            session['display_name'] = row['display_name'] or row['username']
            session['role'] = row['role']
            return redirect(url_for('index'))
        error = 'Invalid username or password'
    return Response(LOGIN_HTML.replace('{{ERROR}}', error), mimetype='text/html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/api/me')
@login_required
def api_me():
    """Return current user info."""
    return jsonify({
        'username': session.get('username'),
        'display_name': session.get('display_name'),
        'role': session.get('role'),
    })


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/stats')
@login_required
def api_stats():
    """Return aggregate statistics about leads."""
    conn = get_db()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]

        cur.execute("SELECT status, COUNT(*) AS cnt FROM leads GROUP BY status")
        breakdown = {row['status']: row['cnt'] for row in cur.fetchall()}

        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE datetime(inserted_at) > datetime('now', '-1 day')"
        )
        recent = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE follow_up_date IS NOT NULL AND follow_up_date <= date('now') AND status NOT IN ('closed', 'failed')"
        )
        reminders_due = cur.fetchone()[0]

        cur.execute(
            "SELECT COALESCE(SUM(deal_value), 0) FROM leads WHERE status NOT IN ('closed', 'failed', 'skipped_invalid_email')"
        )
        pipeline_value = cur.fetchone()[0]

        return jsonify({
            'total': total,
            'breakdown': breakdown,
            'recent_24h': recent,
            'reminders_due': reminders_due,
            'pipeline_value': pipeline_value,
        })
    finally:
        conn.close()


@app.route('/api/leads')
@login_required
def api_leads():
    """Return a paginated, filterable, sortable list of leads."""
    status = request.args.get('status', '').strip()
    search = request.args.get('search', '').strip()
    assignee = request.args.get('assigned_to', '').strip()
    page = max(int(request.args.get('page', '1')), 1)
    per_page = min(max(int(request.args.get('per_page', '25')), 1), 100)
    sort_by = request.args.get('sort_by', 'created_time').strip()
    sort_dir = request.args.get('sort_dir', 'desc').strip().upper()

    if sort_by not in ALLOWED_SORT_COLUMNS:
        sort_by = 'created_time'
    if sort_dir not in ('ASC', 'DESC'):
        sort_dir = 'DESC'

    conditions = []
    params: list = []

    if status:
        conditions.append("status = ?")
        params.append(status)

    if assignee == '__unassigned__':
        conditions.append("(assigned_to IS NULL OR assigned_to = '')")
    elif assignee:
        conditions.append("assigned_to = ?")
        params.append(assignee)

    if search:
        conditions.append(
            "(full_name LIKE ? OR email LIKE ? OR phone_number LIKE ? OR company_name LIKE ?)"
        )
        like = f"%{search}%"
        params.extend([like, like, like, like])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    conn = get_db()
    try:
        cur = conn.cursor()

        # Total matching count
        cur.execute(f"SELECT COUNT(*) FROM leads {where}", params)
        total = cur.fetchone()[0]

        # Paginated results
        offset = (page - 1) * per_page
        cur.execute(
            f"""SELECT id, lead_id, form_id, created_time, email, full_name,
                       first_name, last_name, phone_number, company_name,
                       job_title, status, email_sent_at, inserted_at, updated_at,
                       tags, lead_source, assigned_to,
                       follow_up_date, deal_value, expected_close_date
                FROM leads {where}
                ORDER BY {sort_by} {sort_dir}
                LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        )
        rows = [dict(row) for row in cur.fetchall()]
        leads = []
        for lead in rows:
            if lead.get('tags'):
                try:
                    lead['tags'] = json.loads(lead['tags'])
                except (json.JSONDecodeError, TypeError):
                    pass
            leads.append(lead)

        return jsonify({
            'leads': leads,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': max(1, -(-total // per_page)),
        })
    finally:
        conn.close()


@app.route('/api/leads/export/csv')
@login_required
def api_leads_export_csv():
    """Export all leads (with metadata) as a CSV file download."""
    import csv
    import io

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, lead_id, form_id, created_time, email, full_name,
                      first_name, last_name, phone_number, company_name,
                      job_title, status, email_sent_at, inserted_at, updated_at,
                      tags, lead_source, assigned_to,
                      follow_up_date, deal_value, expected_close_date,
                      raw_field_data
               FROM leads ORDER BY created_time DESC"""
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[col] for col in columns])

        buf.seek(0)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f'opencrm_leads_export_{timestamp}.csv'

        return Response(
            buf.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>')
@login_required
def api_lead_detail(lead_id: str):
    """Return full detail for a single lead (including raw field data)."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM leads WHERE lead_id = ?", (lead_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Lead not found'}), 404
        lead = dict(row)
        if lead.get('raw_field_data'):
            try:
                lead['raw_field_data'] = json.loads(lead['raw_field_data'])
            except (json.JSONDecodeError, TypeError):
                pass
        if lead.get('tags'):
            try:
                lead['tags'] = json.loads(lead['tags'])
            except (json.JSONDecodeError, TypeError):
                pass
        return jsonify(lead)
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/status', methods=['PATCH'])
@login_required
def api_update_status(lead_id: str):
    """Update a lead's status."""
    data = request.get_json(silent=True) or {}
    new_status = (data.get('status') or '').strip()

    if not new_status or new_status not in ALLOWED_STATUSES:
        return jsonify({
            'error': f'Invalid status. Allowed: {", ".join(sorted(ALLOWED_STATUSES))}'
        }), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE leads SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (new_status, lead_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404

        cur.execute("SELECT * FROM leads WHERE lead_id = ?", (lead_id,))
        row = cur.fetchone()
        lead = dict(row) if row else {}
        return jsonify(lead)
    finally:
        conn.close()


@app.route('/api/kanban')
@login_required
def api_kanban():
    """Return all leads grouped by kanban column status."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, lead_id, email, full_name, first_name, phone_number,
                      company_name, job_title, status, created_time, tags, lead_source, assigned_to,
                      follow_up_date, deal_value, expected_close_date
               FROM leads ORDER BY created_time DESC"""
        )
        raw_rows = [dict(r) for r in cur.fetchall()]
        rows = []
        for row in raw_rows:
            if row.get('tags'):
                try:
                    row['tags'] = json.loads(row['tags'])
                except (json.JSONDecodeError, TypeError):
                    pass
            rows.append(row)

        columns = {col: [] for col in KANBAN_COLUMNS}
        columns['other'] = []
        for row in rows:
            s = row.get('status', '')
            if s in columns:
                columns[s].append(row)
            else:
                columns['other'].append(row)

        return jsonify({'columns': columns, 'column_order': KANBAN_COLUMNS + ['other']})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Manual Lead, Tags & AI Email Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/lead', methods=['POST'])
@login_required
def api_create_lead():
    """Create a manual lead."""
    data = request.get_json(silent=True) or {}

    first_name = (data.get('first_name') or '').strip()
    last_name = (data.get('last_name') or '').strip()
    email = (data.get('email') or '').strip()

    if not email:
        return jsonify({'error': 'Email is required'}), 400

    full_name = f"{first_name} {last_name}".strip()
    lead_id = f"manual-{uuid.uuid4()}"
    now = datetime.utcnow().isoformat()

    tags = {
        'industry': (data.get('industry') or '').strip(),
        'lead_source': (data.get('lead_source_tag') or '').strip(),
        'custom': [t.strip() for t in (data.get('custom_tags') or '').split(',') if t.strip()],
    }

    deal_value = data.get('deal_value')
    if deal_value is not None and deal_value != '':
        try:
            deal_value = float(deal_value)
        except (ValueError, TypeError):
            deal_value = None
    else:
        deal_value = None
    expected_close_date = (data.get('expected_close_date') or '').strip() or None

    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO leads
               (lead_id, form_id, created_time, email, full_name, first_name,
                last_name, phone_number, company_name, job_title,
                raw_field_data, status, tags, lead_source,
                deal_value, expected_close_date)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                lead_id, '', now, email, full_name, first_name, last_name,
                (data.get('phone_number') or '').strip(),
                (data.get('company_name') or '').strip(),
                (data.get('job_title') or '').strip(),
                json.dumps(data),
                'intake',
                json.dumps(tags),
                'manual',
                deal_value,
                expected_close_date,
            ),
        )
        conn.commit()
        return jsonify({'lead_id': lead_id, 'status': 'intake'}), 201
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/tags', methods=['PATCH'])
@login_required
def api_update_tags(lead_id: str):
    """Update tags on a lead."""
    data = request.get_json(silent=True) or {}
    tags = {
        'industry': (data.get('industry') or '').strip(),
        'lead_source': (data.get('lead_source') or '').strip(),
        'custom': data.get('custom', []),
    }
    if isinstance(tags['custom'], str):
        tags['custom'] = [t.strip() for t in tags['custom'].split(',') if t.strip()]

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE leads SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (json.dumps(tags), lead_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404
        return jsonify({'tags': tags})
    finally:
        conn.close()


@app.route('/api/tags/options')
@login_required
def api_tag_options():
    """Return predefined dropdown options for tags."""
    return jsonify(TAG_OPTIONS)


def _llm_complete(prompt: str) -> str:
    """Call the configured LLM and return the response text.

    Supports OpenAI-compatible APIs (default) and Anthropic.
    Configure via LLM_API_KEY, LLM_MODEL, LLM_BASE_URL, LLM_PROVIDER.
    """
    if not LLM_API_KEY:
        raise ValueError(
            'AI is not configured. Set LLM_API_KEY (and optionally LLM_MODEL, '
            'LLM_BASE_URL, LLM_PROVIDER) in your .env file.'
        )

    if LLM_PROVIDER == 'anthropic':
        import anthropic
        client = anthropic.Anthropic(api_key=LLM_API_KEY)
        message = client.messages.create(
            model=LLM_MODEL,
            max_tokens=1024,
            messages=[{'role': 'user', 'content': prompt}],
        )
        return message.content[0].text.strip()
    else:
        # OpenAI-compatible API (works with OpenAI, Ollama, Together, Groq, etc.)
        from openai import OpenAI
        kwargs = {'api_key': LLM_API_KEY}
        if LLM_BASE_URL:
            kwargs['base_url'] = LLM_BASE_URL
        client = OpenAI(**kwargs)
        response = client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=1024,
            messages=[{'role': 'user', 'content': prompt}],
        )
        return response.choices[0].message.content.strip()


def generate_ai_email(lead_data: dict, tags: dict) -> dict:
    """Use AI to generate a personalized outreach email."""
    if not LLM_API_KEY:
        raise ValueError(
            'AI is not configured. Set LLM_API_KEY in your .env file.'
        )

    name = lead_data.get('full_name') or lead_data.get('first_name') or 'there'
    company = lead_data.get('company_name') or ''
    title = lead_data.get('job_title') or ''
    industry = tags.get('industry') or ''
    source = tags.get('lead_source') or ''
    custom = ', '.join(tags.get('custom', []))

    company_name = os.getenv('COMPANY_NAME', '')
    company_desc = os.getenv('COMPANY_DESCRIPTION', '')
    sender = SENDER_NAME or (f'The {company_name} Team' if company_name else 'The Team')

    # Build the "About" section dynamically from config
    about_section = ''
    if company_name or company_desc:
        about_section = f"\nAbout {company_name or 'us'}:\n"
        if company_desc:
            about_section += f"- {company_desc}\n"
        else:
            about_section += f"- {company_name} provides products and services to help businesses grow\n"

    at_company = f' at {company_name}' if company_name else ''
    signoff_company = f' and "{company_name}"' if company_name else ''

    prompt = f"""Write a personalized business outreach email from {sender}{at_company} to a prospective lead.

Lead info:
- Name: {name}
- Company: {company}
- Job Title: {title}
- Industry: {industry}
- How we met / Source: {source}
- Additional context: {custom}
{about_section}
Requirements:
- Sound like a real person, not a sales pitch — casual, warm, and brief
- Do NOT list out specific pain points or guess what their problems are
- Do NOT use phrases like "streamline workflows", "tailored solutions", "leverage AI", or consulting-speak
- Express genuine curiosity about their business
- Keep it short — 2-3 short paragraphs max
- Include a simple call-to-action (suggest a quick chat)
- Sign off with "{sender}"{signoff_company} — do NOT use placeholders like [Your Name]
- Return ONLY a JSON object with "subject" and "body" keys
- The body should be simple HTML (use <p> tags, <br> for line breaks)
- Do NOT wrap the JSON in markdown code fences"""

    raw = _llm_complete(prompt)

    # Strip markdown code fences if present
    cleaned = re.sub(r'^```(?:json)?\s*', '', raw)
    cleaned = re.sub(r'\s*```$', '', cleaned)

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError:
        result = {'subject': f'Connecting with {name}', 'body': f'<p>{raw}</p>'}

    return result


@app.route('/api/lead/<lead_id>/generate-email', methods=['POST'])
@login_required
def api_generate_email(lead_id: str):
    """Generate an AI email for a lead."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM leads WHERE lead_id = ?", (lead_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Lead not found'}), 404
        lead = dict(row)
    finally:
        conn.close()

    tags = {}
    if lead.get('tags'):
        try:
            tags = json.loads(lead['tags']) if isinstance(lead['tags'], str) else lead['tags']
        except (json.JSONDecodeError, TypeError):
            pass

    try:
        email = generate_ai_email(lead, tags)
        return jsonify(email)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"AI email generation failed: {e}")
        return jsonify({'error': f'AI generation failed: {str(e)}'}), 500


@app.route('/api/lead/<lead_id>/send-email', methods=['POST'])
@login_required
def api_send_email(lead_id: str):
    """Send an email to a lead and update status to emailed."""
    data = request.get_json(silent=True) or {}
    subject = (data.get('subject') or '').strip()
    body = (data.get('body') or '').strip()

    if not subject or not body:
        return jsonify({'error': 'Subject and body are required'}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT email, lead_id FROM leads WHERE lead_id = ?", (lead_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'Lead not found'}), 404
        lead = dict(row)
    finally:
        conn.close()

    recipient = lead.get('email', '').strip()
    if not recipient:
        return jsonify({'error': 'Lead has no email address'}), 400

    # Import and use EmailClient
    try:
        from email_client import EmailClient
        ec = EmailClient()
        success = ec.send_email(
            recipients=[recipient],
            subject=subject,
            body=body,
            body_type='html',
        )
    except Exception as e:
        logger.error(f"Failed to send email to {recipient}: {e}")
        return jsonify({'error': f'Email send failed: {str(e)}'}), 500

    if not success:
        return jsonify({'error': 'Email send returned failure'}), 500

    # Update lead status to emailed
    conn = get_db()
    try:
        conn.execute(
            "UPDATE leads SET status = 'emailed', email_sent_at = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (datetime.utcnow().isoformat(), lead_id),
        )
        conn.commit()
    finally:
        conn.close()

    return jsonify({'status': 'emailed', 'message': f'Email sent to {recipient}'})


@app.route('/api/lead/<lead_id>', methods=['DELETE'])
@login_required
def api_delete_lead(lead_id: str):
    """Delete a lead."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM leads WHERE lead_id = ?", (lead_id,))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404
        return jsonify({'message': 'Lead deleted'}), 200
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/assign', methods=['PATCH'])
@login_required
def api_assign_lead(lead_id: str):
    """Assign a lead to a user."""
    data = request.get_json(silent=True) or {}
    assigned_to = (data.get('assigned_to') or '').strip()

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE leads SET assigned_to = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (assigned_to, lead_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404
        return jsonify({'assigned_to': assigned_to})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Notes, Follow-up & Deal Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/lead/<lead_id>/notes')
@login_required
def api_get_notes(lead_id: str):
    """Return all notes for a lead."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, lead_id, author, content, created_at FROM lead_notes WHERE lead_id = ? ORDER BY created_at DESC",
            (lead_id,),
        ).fetchall()
        return jsonify({'notes': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/notes', methods=['POST'])
@login_required
def api_create_note(lead_id: str):
    """Add a note to a lead."""
    data = request.get_json(silent=True) or {}
    content = (data.get('content') or '').strip()
    if not content:
        return jsonify({'error': 'Content is required'}), 400

    author = session.get('display_name') or session.get('username') or ''
    now = datetime.utcnow().isoformat()

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO lead_notes (lead_id, author, content, created_at) VALUES (?, ?, ?, ?)",
            (lead_id, author, content, now),
        )
        conn.commit()
        return jsonify({'lead_id': lead_id, 'author': author, 'content': content, 'created_at': now}), 201
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/notes/<int:note_id>', methods=['DELETE'])
@login_required
def api_delete_note(lead_id: str, note_id: int):
    """Delete a note."""
    conn = get_db()
    try:
        cur = conn.execute(
            "DELETE FROM lead_notes WHERE id = ? AND lead_id = ?", (note_id, lead_id)
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Note not found'}), 404
        return jsonify({'message': 'Note deleted'})
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/follow-up', methods=['PATCH'])
@login_required
def api_set_follow_up(lead_id: str):
    """Set or clear follow-up date on a lead."""
    data = request.get_json(silent=True) or {}
    follow_up_date = (data.get('follow_up_date') or '').strip() or None

    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE leads SET follow_up_date = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (follow_up_date, lead_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404
        return jsonify({'follow_up_date': follow_up_date})
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/deal', methods=['PATCH'])
@login_required
def api_update_deal(lead_id: str):
    """Update deal value and/or expected close date."""
    data = request.get_json(silent=True) or {}

    deal_value = data.get('deal_value')
    if deal_value is not None and deal_value != '':
        try:
            deal_value = float(deal_value)
        except (ValueError, TypeError):
            return jsonify({'error': 'deal_value must be a number'}), 400
    else:
        deal_value = None

    expected_close_date = (data.get('expected_close_date') or '').strip() or None

    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE leads SET deal_value = ?, expected_close_date = ?, updated_at = CURRENT_TIMESTAMP WHERE lead_id = ?",
            (deal_value, expected_close_date, lead_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Lead not found'}), 404
        return jsonify({'deal_value': deal_value, 'expected_close_date': expected_close_date})
    finally:
        conn.close()


@app.route('/api/reminders')
@login_required
def api_reminders():
    """Return leads with follow-up dates, split into overdue and upcoming."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT lead_id, full_name, email, company_name, status, follow_up_date
               FROM leads
               WHERE follow_up_date IS NOT NULL AND status NOT IN ('closed', 'failed')
               ORDER BY follow_up_date ASC"""
        )
        rows = [dict(r) for r in cur.fetchall()]
        today = datetime.utcnow().strftime('%Y-%m-%d')
        overdue = [r for r in rows if r['follow_up_date'] <= today]
        upcoming = [r for r in rows if r['follow_up_date'] > today]
        return jsonify({'overdue': overdue, 'upcoming': upcoming})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Reporting Endpoints
# ---------------------------------------------------------------------------

@app.route('/api/reports/funnel')
@login_required
def api_report_funnel():
    """Conversion funnel: count of leads at each status."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM leads GROUP BY status ORDER BY count DESC"
        ).fetchall()
        # Order by pipeline stage
        stage_order = ['intake', 'emailed', 'contacted', 'qualified', 'closed', 'failed', 'skipped_invalid_email']
        result = []
        counts = {r['status']: r['count'] for r in rows}
        for stage in stage_order:
            if stage in counts:
                result.append({'status': stage, 'count': counts[stage]})
        # Add any stages not in the predefined order
        for status, count in counts.items():
            if status not in stage_order:
                result.append({'status': status, 'count': count})
        return jsonify({'funnel': result})
    finally:
        conn.close()


@app.route('/api/reports/by-source')
@login_required
def api_report_by_source():
    """Leads grouped by source with counts and pipeline value."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT lead_source, COUNT(*) as count,
                      COALESCE(SUM(deal_value), 0) as total_value,
                      SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as closed_count
               FROM leads GROUP BY lead_source ORDER BY count DESC"""
        ).fetchall()
        return jsonify({'by_source': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/api/reports/over-time')
@login_required
def api_report_over_time():
    """Leads created per day for the last 30 days."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT date(created_time) as day, COUNT(*) as count
               FROM leads
               WHERE created_time >= date('now', '-30 days')
               GROUP BY day ORDER BY day ASC"""
        ).fetchall()
        return jsonify({'over_time': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/api/reports/pipeline-value')
@login_required
def api_report_pipeline_value():
    """Deal value broken down by status."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT status, COUNT(*) as count,
                      COALESCE(SUM(deal_value), 0) as total_value
               FROM leads
               WHERE deal_value IS NOT NULL AND deal_value > 0
               GROUP BY status ORDER BY total_value DESC"""
        ).fetchall()
        return jsonify({'pipeline_value': [dict(r) for r in rows]})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Email Sequences
# ---------------------------------------------------------------------------

@app.route('/api/sequences')
@login_required
def api_sequences():
    """List all sequences."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, name, steps, created_at, updated_at FROM sequences ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            seq = dict(r)
            try:
                seq['steps'] = json.loads(seq['steps'])
            except (json.JSONDecodeError, TypeError):
                seq['steps'] = []
            # Count active enrollments
            cnt = conn.execute(
                "SELECT COUNT(*) FROM sequence_enrollments WHERE sequence_id = ? AND status = 'active'",
                (seq['id'],),
            ).fetchone()[0]
            seq['active_enrollments'] = cnt
            result.append(seq)
        return jsonify({'sequences': result})
    finally:
        conn.close()


@app.route('/api/sequences', methods=['POST'])
@login_required
def api_create_sequence():
    """Create a new email sequence.

    Body: { name: str, steps: [{ delay_days: int, subject: str, body: str }] }
    """
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    steps = data.get('steps', [])

    if not name:
        return jsonify({'error': 'Name is required'}), 400
    if not steps or not isinstance(steps, list):
        return jsonify({'error': 'At least one step is required'}), 400

    # Validate steps
    for i, step in enumerate(steps):
        if not step.get('subject') or not step.get('body'):
            return jsonify({'error': f'Step {i+1} needs subject and body'}), 400
        step.setdefault('delay_days', 0)

    now = datetime.utcnow().isoformat()
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO sequences (name, steps, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (name, json.dumps(steps), now, now),
        )
        conn.commit()
        return jsonify({'id': cur.lastrowid, 'name': name, 'steps': steps}), 201
    finally:
        conn.close()


@app.route('/api/sequences/<int:seq_id>', methods=['PUT'])
@login_required
def api_update_sequence(seq_id: int):
    """Update a sequence."""
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    steps = data.get('steps', [])

    if not name:
        return jsonify({'error': 'Name is required'}), 400

    now = datetime.utcnow().isoformat()
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE sequences SET name = ?, steps = ?, updated_at = ? WHERE id = ?",
            (name, json.dumps(steps), now, seq_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Sequence not found'}), 404
        return jsonify({'id': seq_id, 'name': name, 'steps': steps})
    finally:
        conn.close()


@app.route('/api/sequences/<int:seq_id>', methods=['DELETE'])
@login_required
def api_delete_sequence(seq_id: int):
    """Delete a sequence and all its enrollments."""
    conn = get_db()
    try:
        cur = conn.execute("DELETE FROM sequences WHERE id = ?", (seq_id,))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Sequence not found'}), 404
        return jsonify({'message': 'Sequence deleted'})
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/enroll', methods=['POST'])
@login_required
def api_enroll_lead(lead_id: str):
    """Enroll a lead in a sequence."""
    data = request.get_json(silent=True) or {}
    seq_id = data.get('sequence_id')

    if not seq_id:
        return jsonify({'error': 'sequence_id is required'}), 400

    conn = get_db()
    try:
        # Check sequence exists and get first step delay
        seq = conn.execute("SELECT steps FROM sequences WHERE id = ?", (seq_id,)).fetchone()
        if not seq:
            return jsonify({'error': 'Sequence not found'}), 404

        # Check not already enrolled in this sequence
        existing = conn.execute(
            "SELECT id FROM sequence_enrollments WHERE lead_id = ? AND sequence_id = ? AND status = 'active'",
            (lead_id, seq_id),
        ).fetchone()
        if existing:
            return jsonify({'error': 'Lead is already enrolled in this sequence'}), 409

        steps = json.loads(seq['steps']) if seq['steps'] else []
        delay_days = steps[0].get('delay_days', 0) if steps else 0

        now = datetime.utcnow()
        next_send = (now + timedelta(days=delay_days)).isoformat()

        conn.execute(
            """INSERT INTO sequence_enrollments
               (lead_id, sequence_id, current_step, status, next_send_at, enrolled_at, updated_at)
               VALUES (?, ?, 0, 'active', ?, ?, ?)""",
            (lead_id, seq_id, next_send, now.isoformat(), now.isoformat()),
        )
        conn.commit()
        return jsonify({'lead_id': lead_id, 'sequence_id': seq_id, 'status': 'active', 'next_send_at': next_send}), 201
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/unenroll', methods=['POST'])
@login_required
def api_unenroll_lead(lead_id: str):
    """Unenroll a lead from a sequence."""
    data = request.get_json(silent=True) or {}
    seq_id = data.get('sequence_id')

    conn = get_db()
    try:
        if seq_id:
            conn.execute(
                "UPDATE sequence_enrollments SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE lead_id = ? AND sequence_id = ? AND status = 'active'",
                (lead_id, seq_id),
            )
        else:
            conn.execute(
                "UPDATE sequence_enrollments SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE lead_id = ? AND status = 'active'",
                (lead_id,),
            )
        conn.commit()
        return jsonify({'message': 'Unenrolled'})
    finally:
        conn.close()


@app.route('/api/lead/<lead_id>/enrollments')
@login_required
def api_lead_enrollments(lead_id: str):
    """Get all sequence enrollments for a lead."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT se.id, se.sequence_id, s.name as sequence_name, se.current_step,
                      se.status, se.next_send_at, se.enrolled_at, s.steps
               FROM sequence_enrollments se
               JOIN sequences s ON s.id = se.sequence_id
               WHERE se.lead_id = ? ORDER BY se.enrolled_at DESC""",
            (lead_id,),
        ).fetchall()
        result = []
        for r in rows:
            e = dict(r)
            try:
                e['steps'] = json.loads(e['steps'])
            except (json.JSONDecodeError, TypeError):
                e['steps'] = []
            e['total_steps'] = len(e['steps'])
            result.append(e)
        return jsonify({'enrollments': result})
    finally:
        conn.close()


@app.route('/api/sequences/process', methods=['POST'])
@login_required
def api_process_sequences():
    """Process due sequence steps: send emails and advance enrollments.

    Call this endpoint periodically (via cron, the main pipeline loop, or manually)
    to send due sequence emails and advance leads to the next step.
    """
    now = datetime.utcnow().isoformat()
    conn = get_db()
    sent = 0
    completed = 0
    errors = []

    try:
        # Find due enrollments
        rows = conn.execute(
            """SELECT se.id, se.lead_id, se.sequence_id, se.current_step,
                      s.steps, l.email, l.first_name, l.full_name
               FROM sequence_enrollments se
               JOIN sequences s ON s.id = se.sequence_id
               JOIN leads l ON l.lead_id = se.lead_id
               WHERE se.status = 'active' AND se.next_send_at <= ?""",
            (now,),
        ).fetchall()

        for row in rows:
            enrollment_id = row['id']
            lead_email = row['email']
            step_idx = row['current_step']

            try:
                steps = json.loads(row['steps']) if row['steps'] else []
            except (json.JSONDecodeError, TypeError):
                steps = []

            if step_idx >= len(steps) or not lead_email:
                conn.execute(
                    "UPDATE sequence_enrollments SET status = 'completed', updated_at = ? WHERE id = ?",
                    (now, enrollment_id),
                )
                completed += 1
                continue

            step = steps[step_idx]
            subject = step.get('subject', '')
            body = step.get('body', '')

            # Simple variable substitution
            name = row['first_name'] or row['full_name'] or 'there'
            subject = subject.replace('{name}', name).replace('{first_name}', name)
            body = body.replace('{name}', name).replace('{first_name}', name)

            # Send email
            try:
                from email_client import EmailClient
                ec = EmailClient()
                success = ec.send_email(
                    recipients=[lead_email],
                    subject=subject,
                    body=body,
                    body_type='html',
                )
            except Exception as e:
                errors.append({'enrollment_id': enrollment_id, 'error': str(e)})
                continue

            if success:
                sent += 1
                next_step = step_idx + 1
                if next_step >= len(steps):
                    conn.execute(
                        "UPDATE sequence_enrollments SET current_step = ?, status = 'completed', updated_at = ? WHERE id = ?",
                        (next_step, now, enrollment_id),
                    )
                    completed += 1
                else:
                    next_delay = steps[next_step].get('delay_days', 1)
                    next_send = (datetime.utcnow() + timedelta(days=next_delay)).isoformat()
                    conn.execute(
                        "UPDATE sequence_enrollments SET current_step = ?, next_send_at = ?, updated_at = ? WHERE id = ?",
                        (next_step, next_send, now, enrollment_id),
                    )
            else:
                errors.append({'enrollment_id': enrollment_id, 'error': 'Send returned false'})

        conn.commit()
    finally:
        conn.close()

    return jsonify({'sent': sent, 'completed': completed, 'errors': errors})


# ---------------------------------------------------------------------------
# Webhook Ingestion
# ---------------------------------------------------------------------------

@app.route('/api/webhook/leads', methods=['POST'])
def api_webhook_leads():
    """Ingest leads from external sources via webhook.

    Authenticated via ``Authorization: Bearer <WEBHOOK_API_KEY>`` header
    or ``?api_key=<KEY>`` query param.  If WEBHOOK_API_KEY is not configured
    the endpoint is disabled.

    Accepts a single lead object or a list of lead objects.  Minimum required
    field: ``email``.  Optional: first_name, last_name, full_name,
    phone_number, company_name, job_title, lead_source, tags (object),
    deal_value, expected_close_date.
    """
    if not WEBHOOK_API_KEY:
        return jsonify({'error': 'Webhook endpoint is not configured. Set WEBHOOK_API_KEY.'}), 403

    # Auth check
    provided_key = ''
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        provided_key = auth_header[7:].strip()
    if not provided_key:
        provided_key = request.args.get('api_key', '').strip()
    if provided_key != WEBHOOK_API_KEY:
        return jsonify({'error': 'Invalid API key'}), 401

    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({'error': 'JSON body required'}), 400

    # Normalise to list
    items = payload if isinstance(payload, list) else [payload]
    created = []
    errors = []

    conn = get_db()
    try:
        for item in items:
            email = (item.get('email') or '').strip()
            if not email:
                errors.append({'error': 'email is required', 'input': item})
                continue

            first_name = (item.get('first_name') or '').strip()
            last_name = (item.get('last_name') or '').strip()
            full_name = (item.get('full_name') or f"{first_name} {last_name}").strip()
            lead_id = f"webhook-{uuid.uuid4()}"
            now = datetime.utcnow().isoformat()
            source = (item.get('lead_source') or 'webhook').strip()

            tags = item.get('tags') or {}
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except json.JSONDecodeError:
                    tags = {}

            deal_value = item.get('deal_value')
            if deal_value is not None and deal_value != '':
                try:
                    deal_value = float(deal_value)
                except (ValueError, TypeError):
                    deal_value = None
            else:
                deal_value = None

            expected_close = (item.get('expected_close_date') or '').strip() or None

            conn.execute(
                """INSERT INTO leads
                   (lead_id, form_id, created_time, email, full_name, first_name,
                    last_name, phone_number, company_name, job_title,
                    raw_field_data, status, tags, lead_source,
                    deal_value, expected_close_date)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    lead_id, '', now, email, full_name, first_name, last_name,
                    (item.get('phone_number') or '').strip(),
                    (item.get('company_name') or '').strip(),
                    (item.get('job_title') or '').strip(),
                    json.dumps(item),
                    'intake',
                    json.dumps(tags),
                    source,
                    deal_value,
                    expected_close,
                ),
            )
            created.append({'lead_id': lead_id, 'email': email})

        conn.commit()
    finally:
        conn.close()

    return jsonify({
        'created': created,
        'errors': errors,
        'total_created': len(created),
        'total_errors': len(errors),
    }), 201 if created else 400


# ---------------------------------------------------------------------------
# Campaigns
# ---------------------------------------------------------------------------

def _send_via_mailgun(to_email: str, subject: str, html_body: str) -> bool:
    """Send a single email via Mailgun API."""
    import requests as _req
    if not MAILGUN_API_KEY or not MAILGUN_DOMAIN:
        raise ValueError('Mailgun is not configured (MAILGUN_API_KEY / MAILGUN_DOMAIN)')
    sender = MAILGUN_SENDER_EMAIL or f'campaigns@{MAILGUN_DOMAIN}'
    base = 'https://api.eu.mailgun.net' if MAILGUN_REGION == 'eu' else 'https://api.mailgun.net'
    resp = _req.post(
        f'{base}/v3/{MAILGUN_DOMAIN}/messages',
        auth=('api', MAILGUN_API_KEY),
        data={'from': sender, 'to': [to_email], 'subject': subject, 'html': html_body},
        timeout=30,
    )
    resp.raise_for_status()
    return True


def _campaign_filter_query(filters: dict):
    """Build WHERE clause and params from campaign lead filters."""
    conditions = []
    params = []

    if filters.get('status'):
        conditions.append("status = ?")
        params.append(filters['status'])
    if filters.get('lead_source'):
        conditions.append("lead_source = ?")
        params.append(filters['lead_source'])
    if filters.get('search'):
        like = f"%{filters['search']}%"
        conditions.append("(full_name LIKE ? OR email LIKE ? OR company_name LIKE ?)")
        params.extend([like, like, like])
    if filters.get('assigned_to'):
        conditions.append("assigned_to = ?")
        params.append(filters['assigned_to'])
    if filters.get('has_email', True):
        conditions.append("email IS NOT NULL AND email != ''")
    if filters.get('tag_industry'):
        conditions.append("json_extract(tags, '$.industry') = ?")
        params.append(filters['tag_industry'])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


@app.route('/api/campaigns')
@login_required
def api_campaigns():
    """List all campaigns."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM campaigns ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            c = dict(r)
            try:
                c['lead_filters'] = json.loads(c['lead_filters'])
            except (json.JSONDecodeError, TypeError):
                c['lead_filters'] = {}
            result.append(c)
        return jsonify({'campaigns': result})
    finally:
        conn.close()


@app.route('/api/campaigns', methods=['POST'])
@login_required
def api_create_campaign():
    """Create a new campaign (draft).

    Body: { name, campaign_type, email_provider, subject_template, body_template, lead_filters }
    """
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Name is required'}), 400

    campaign_type = data.get('campaign_type', 'single')
    email_provider = data.get('email_provider', 'outlook')
    subject = (data.get('subject_template') or '').strip()
    body = (data.get('body_template') or '').strip()
    filters = data.get('lead_filters', {})

    now = datetime.utcnow().isoformat()
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO campaigns
               (name, campaign_type, email_provider, subject_template, body_template,
                lead_filters, status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (name, campaign_type, email_provider, subject, body, json.dumps(filters), 'draft', now, now),
        )
        conn.commit()
        return jsonify({'id': cur.lastrowid, 'name': name, 'status': 'draft'}), 201
    finally:
        conn.close()


@app.route('/api/campaigns/<int:cid>', methods=['PUT'])
@login_required
def api_update_campaign(cid: int):
    """Update a draft campaign."""
    data = request.get_json(silent=True) or {}
    conn = get_db()
    try:
        existing = conn.execute("SELECT status FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not existing:
            return jsonify({'error': 'Campaign not found'}), 404
        if existing['status'] not in ('draft', 'paused'):
            return jsonify({'error': 'Can only edit draft or paused campaigns'}), 400

        name = (data.get('name') or '').strip()
        conn.execute(
            """UPDATE campaigns SET name=?, campaign_type=?, email_provider=?,
               subject_template=?, body_template=?, lead_filters=?, updated_at=?
               WHERE id=?""",
            (
                name,
                data.get('campaign_type', 'single'),
                data.get('email_provider', 'outlook'),
                (data.get('subject_template') or '').strip(),
                (data.get('body_template') or '').strip(),
                json.dumps(data.get('lead_filters', {})),
                datetime.utcnow().isoformat(),
                cid,
            ),
        )
        conn.commit()
        return jsonify({'id': cid, 'status': 'updated'})
    finally:
        conn.close()


@app.route('/api/campaigns/<int:cid>', methods=['DELETE'])
@login_required
def api_delete_campaign(cid: int):
    """Delete a campaign."""
    conn = get_db()
    try:
        cur = conn.execute("DELETE FROM campaigns WHERE id = ?", (cid,))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'Campaign not found'}), 404
        return jsonify({'message': 'Campaign deleted'})
    finally:
        conn.close()


@app.route('/api/campaigns/<int:cid>/preview')
@login_required
def api_campaign_preview(cid: int):
    """Preview matching leads for a campaign's filters."""
    conn = get_db()
    try:
        row = conn.execute("SELECT lead_filters FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not row:
            return jsonify({'error': 'Campaign not found'}), 404
        try:
            filters = json.loads(row['lead_filters'])
        except (json.JSONDecodeError, TypeError):
            filters = {}

        where, params = _campaign_filter_query(filters)
        count = conn.execute(f"SELECT COUNT(*) FROM leads {where}", params).fetchone()[0]
        leads = conn.execute(
            f"SELECT lead_id, email, full_name, company_name, status FROM leads {where} LIMIT 20",
            params,
        ).fetchall()
        return jsonify({'count': count, 'sample': [dict(r) for r in leads]})
    finally:
        conn.close()


@app.route('/api/campaigns/filter-preview', methods=['POST'])
@login_required
def api_campaign_filter_preview():
    """Preview lead count for arbitrary filters (used in campaign builder)."""
    filters = request.get_json(silent=True) or {}
    conn = get_db()
    try:
        where, params = _campaign_filter_query(filters)
        count = conn.execute(f"SELECT COUNT(*) FROM leads {where}", params).fetchone()[0]
        leads = conn.execute(
            f"SELECT lead_id, email, full_name, company_name, status FROM leads {where} LIMIT 10",
            params,
        ).fetchall()
        return jsonify({'count': count, 'sample': [dict(r) for r in leads]})
    finally:
        conn.close()


@app.route('/api/campaigns/<int:cid>/generate-copy', methods=['POST'])
@login_required
def api_campaign_generate_copy(cid: int):
    """Use AI to generate campaign email copy."""
    if not LLM_API_KEY:
        return jsonify({'error': 'AI is not configured. Set LLM_API_KEY in your .env file.'}), 400

    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not row:
            return jsonify({'error': 'Campaign not found'}), 404
    finally:
        conn.close()

    data = request.get_json(silent=True) or {}
    context = (data.get('context') or '').strip()

    company_name = os.getenv('COMPANY_NAME', '')
    company_desc = os.getenv('COMPANY_DESCRIPTION', '')
    sender = SENDER_NAME or (f'The {company_name} Team' if company_name else 'The Team')

    about = ''
    if company_name or company_desc:
        about = f"\nAbout the sender's company ({company_name}): {company_desc}\n"

    prompt = f"""Write a marketing email for a campaign called "{row['name']}".
{about}
Additional context from the user: {context or 'None provided'}

Requirements:
- Sound like a real person, not a marketing blast
- Keep it concise — 2-3 short paragraphs max
- Use {{name}} as a placeholder for the recipient's first name
- Include a clear call-to-action
- Sign off with "{sender}"{' and "' + company_name + '"' if company_name else ''}
- Return ONLY a JSON object with "subject" and "body" keys
- The body should be simple HTML (<p> tags, <br> for line breaks)
- Do NOT wrap in markdown code fences"""

    raw = _llm_complete(prompt)
    cleaned = re.sub(r'^```(?:json)?\s*', '', raw)
    cleaned = re.sub(r'\s*```$', '', cleaned)

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError:
        result = {'subject': row['name'], 'body': f'<p>{raw}</p>'}

    return jsonify(result)


@app.route('/api/campaigns/<int:cid>/send', methods=['POST'])
@login_required
def api_campaign_send(cid: int):
    """Execute a campaign: resolve leads, queue sends, and send emails."""
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not row:
            return jsonify({'error': 'Campaign not found'}), 404
        campaign = dict(row)

        if campaign['status'] not in ('draft', 'paused'):
            return jsonify({'error': f"Cannot send campaign with status '{campaign['status']}'"}), 400

        if not campaign['subject_template'] or not campaign['body_template']:
            return jsonify({'error': 'Subject and body are required'}), 400

        try:
            filters = json.loads(campaign['lead_filters'])
        except (json.JSONDecodeError, TypeError):
            filters = {}

        where, params = _campaign_filter_query(filters)
        leads = conn.execute(
            f"SELECT lead_id, email, first_name, full_name FROM leads {where}", params
        ).fetchall()

        if not leads:
            return jsonify({'error': 'No leads match the filters'}), 400

        # Update campaign status
        now = datetime.utcnow().isoformat()
        conn.execute(
            "UPDATE campaigns SET status='sending', total_recipients=?, sent_at=?, updated_at=? WHERE id=?",
            (len(leads), now, now, cid),
        )

        # Clear any previous sends for this campaign (in case of re-send)
        conn.execute("DELETE FROM campaign_sends WHERE campaign_id = ?", (cid,))

        # Insert pending sends
        for lead in leads:
            conn.execute(
                "INSERT INTO campaign_sends (campaign_id, lead_id, email, status) VALUES (?,?,?,?)",
                (cid, lead['lead_id'], lead['email'], 'pending'),
            )
        conn.commit()
    finally:
        conn.close()

    # Now send the emails
    provider = campaign['email_provider']
    sent = 0
    failed = 0

    conn = get_db()
    try:
        pending = conn.execute(
            "SELECT cs.id, cs.lead_id, cs.email, l.first_name, l.full_name FROM campaign_sends cs JOIN leads l ON l.lead_id = cs.lead_id WHERE cs.campaign_id = ? AND cs.status = 'pending'",
            (cid,),
        ).fetchall()

        for send_row in pending:
            name = send_row['first_name'] or send_row['full_name'] or 'there'
            subject = campaign['subject_template'].replace('{name}', name).replace('{first_name}', name)
            body = campaign['body_template'].replace('{name}', name).replace('{first_name}', name)

            try:
                if provider == 'mailgun':
                    _send_via_mailgun(send_row['email'], subject, body)
                else:
                    from email_client import EmailClient
                    ec = EmailClient()
                    ec.send_email(recipients=[send_row['email']], subject=subject, body=body, body_type='html')

                conn.execute(
                    "UPDATE campaign_sends SET status='sent', sent_at=? WHERE id=?",
                    (datetime.utcnow().isoformat(), send_row['id']),
                )
                sent += 1
            except Exception as e:
                conn.execute(
                    "UPDATE campaign_sends SET status='failed', error=? WHERE id=?",
                    (str(e)[:500], send_row['id']),
                )
                failed += 1

        # Update campaign totals
        final_status = 'completed' if failed == 0 else ('completed' if sent > 0 else 'failed')
        conn.execute(
            "UPDATE campaigns SET status=?, sent_count=?, failed_count=?, updated_at=? WHERE id=?",
            (final_status, sent, failed, datetime.utcnow().isoformat(), cid),
        )
        conn.commit()
    finally:
        conn.close()

    return jsonify({'sent': sent, 'failed': failed, 'total': sent + failed, 'status': final_status})


@app.route('/api/campaigns/<int:cid>/stats')
@login_required
def api_campaign_stats(cid: int):
    """Get send stats for a campaign."""
    conn = get_db()
    try:
        campaign = conn.execute("SELECT * FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not campaign:
            return jsonify({'error': 'Campaign not found'}), 404

        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM campaign_sends WHERE campaign_id = ? GROUP BY status",
            (cid,),
        ).fetchall()
        breakdown = {r['status']: r['count'] for r in rows}

        return jsonify({
            'campaign': dict(campaign),
            'breakdown': breakdown,
        })
    finally:
        conn.close()


@app.route('/api/email-providers')
@login_required
def api_email_providers():
    """Return available email providers."""
    providers = [{'id': 'outlook', 'name': 'Outlook (Microsoft Graph)', 'configured': True}]
    if MAILGUN_API_KEY and MAILGUN_DOMAIN:
        providers.append({'id': 'mailgun', 'name': f'Mailgun ({MAILGUN_DOMAIN})', 'configured': True})
    else:
        providers.append({'id': 'mailgun', 'name': 'Mailgun (not configured)', 'configured': False})
    return jsonify({'providers': providers})


# ---------------------------------------------------------------------------
# User Management (admin only)
# ---------------------------------------------------------------------------

@app.route('/api/users')
@admin_required
def api_users():
    """List all users."""
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, username, display_name, role, created_at FROM users ORDER BY id").fetchall()
        return jsonify({'users': [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route('/api/users/list')
@login_required
def api_users_list():
    """Return a simple list of usernames for assignment dropdowns."""
    conn = get_db()
    try:
        rows = conn.execute("SELECT username, display_name FROM users ORDER BY display_name").fetchall()
        return jsonify({'users': [{'username': r['username'], 'display_name': r['display_name'] or r['username']} for r in rows]})
    finally:
        conn.close()


@app.route('/api/users', methods=['POST'])
@admin_required
def api_create_user():
    """Create a new user."""
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip().lower()
    password = (data.get('password') or '').strip()
    display_name = (data.get('display_name') or '').strip()
    role = (data.get('role') or 'user').strip()

    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400
    if len(password) < 4:
        return jsonify({'error': 'Password must be at least 4 characters'}), 400
    if role not in ('admin', 'user'):
        return jsonify({'error': 'Role must be admin or user'}), 400

    conn = get_db()
    try:
        existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if existing:
            return jsonify({'error': 'Username already exists'}), 409
        conn.execute(
            "INSERT INTO users (username, password, display_name, role) VALUES (?, ?, ?, ?)",
            (username, _hash_password(password), display_name or username, role),
        )
        conn.commit()
        return jsonify({'username': username, 'display_name': display_name or username, 'role': role}), 201
    finally:
        conn.close()


@app.route('/api/users/<username>', methods=['DELETE'])
@admin_required
def api_delete_user(username: str):
    """Delete a user."""
    if username == session.get('username'):
        return jsonify({'error': 'Cannot delete yourself'}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'User not found'}), 404
        # Unassign leads from deleted user
        conn.execute("UPDATE leads SET assigned_to = '' WHERE assigned_to = ?", (username,))
        conn.commit()
        return jsonify({'message': 'User deleted'}), 200
    finally:
        conn.close()


@app.route('/api/users/<username>/password', methods=['PATCH'])
@admin_required
def api_reset_password(username: str):
    """Reset a user's password."""
    data = request.get_json(silent=True) or {}
    password = (data.get('password') or '').strip()
    if not password or len(password) < 4:
        return jsonify({'error': 'Password must be at least 4 characters'}), 400

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET password = ? WHERE username = ?", (_hash_password(password), username))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'error': 'User not found'}), 404
        return jsonify({'message': 'Password updated'})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Login HTML
# ---------------------------------------------------------------------------

LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Login - Arkitekt OpenCRM</title>
<script src="https://cdn.tailwindcss.com"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>body{font-family:'Inter',system-ui,sans-serif}</style>
</head>
<body class="bg-[#0a0a0f] min-h-screen flex items-center justify-center px-4">
<div class="relative w-full max-w-sm">
  <div class="absolute -inset-1 bg-gradient-to-r from-blue-600/20 via-purple-600/20 to-blue-600/20 rounded-2xl blur-xl"></div>
  <div class="relative bg-[#111118] border border-white/[0.06] rounded-2xl shadow-2xl p-8">
    <div class="text-center mb-8">
      <h1 class="text-2xl font-bold bg-gradient-to-r from-white to-gray-400 bg-clip-text text-transparent">Arkitekt OpenCRM</h1>
      <p class="text-sm text-gray-500 mt-1">Sign in to continue</p>
    </div>
    <form method="POST" class="space-y-4">
      <div>
        <label class="block text-xs font-medium text-gray-400 mb-1.5 uppercase tracking-wider">Username</label>
        <input type="text" name="username" required autofocus
               class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3.5 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50 placeholder-gray-600 transition">
      </div>
      <div>
        <label class="block text-xs font-medium text-gray-400 mb-1.5 uppercase tracking-wider">Password</label>
        <input type="password" name="password" required
               class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3.5 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50 placeholder-gray-600 transition">
      </div>
      <button type="submit"
              class="w-full bg-gradient-to-r from-blue-600 to-blue-500 text-white py-2.5 rounded-lg text-sm font-semibold hover:from-blue-500 hover:to-blue-400 transition-all shadow-lg shadow-blue-600/20 active:scale-[0.98]">
        Sign In
      </button>
    </form>
    <p class="text-red-400 text-sm text-center mt-3">{{ERROR}}</p>
  </div>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# SPA HTML (served at /)
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Arkitekt OpenCRM</title>
<script src="https://cdn.tailwindcss.com"></script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
body{font-family:'Inter',system-ui,sans-serif}
[x-cloak]{display:none !important}
/* Subtle card glass effect */
.card{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.06);backdrop-filter:blur(12px)}
.card:hover{border-color:rgba(255,255,255,0.1)}
/* Smooth transitions everywhere */
*{transition-property:color,background-color,border-color,box-shadow,opacity;transition-duration:150ms}
/* Better scrollbar */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.1);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:rgba(255,255,255,0.2)}
/* Input focus glow */
input:focus,select:focus,textarea:focus{box-shadow:0 0 0 2px rgba(59,130,246,0.3)}
/* Mobile responsive */
@media (max-width: 767px) {
  header .max-w-7xl { flex-direction: column; align-items: stretch !important; gap: 0.5rem; }
  header .flex.items-center.gap-3 { flex-wrap: wrap; gap: 0.4rem; justify-content: space-between; }
  header h1 { font-size: 1.1rem; }
  .filter-bar { flex-direction: column !important; }
  .filter-bar .flex.gap-3 { flex-direction: column; }
  .filter-bar select, .filter-bar button { width: 100%; }
  .leads-table thead { display: none; }
  .leads-table tbody tr {
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 2px 12px; padding: 12px 16px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
  }
  .leads-table tbody td { padding: 2px 0; font-size: 0.8rem; }
  .leads-table tbody td:first-child { font-weight: 600; grid-column: 1 / -1; font-size: 0.9rem; }
  .leads-table tbody td:last-child { font-size: 0.7rem; color: rgb(107 114 128); grid-column: 1 / -1; }
  .grid.grid-cols-2 { gap: 0.5rem; }
  .grid.grid-cols-2 .p-4 { padding: 0.75rem; }
  .grid.grid-cols-2 .text-2xl { font-size: 1.25rem; }
  .kanban-cols { flex-direction: column !important; }
  .modal-content { width: 95vw !important; max-width: 95vw !important; max-height: 90vh; margin: 1rem auto; }
}
</style>
</head>
<body class="bg-[#0a0a0f] min-h-screen text-gray-100" x-data="dashboard()" x-init="init()">

<!-- Header -->
<header class="bg-[#111118]/80 backdrop-blur-xl border-b border-white/[0.06] sticky top-0 z-40">
  <div class="max-w-7xl mx-auto px-4 py-3 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
    <h1 class="text-xl font-bold bg-gradient-to-r from-white to-gray-400 bg-clip-text text-transparent">Arkitekt OpenCRM</h1>
    <div class="flex items-center gap-3 flex-wrap">
      <!-- View toggle -->
      <div class="flex bg-white/[0.04] rounded-lg p-0.5 border border-white/[0.06]">
        <button
          class="px-3 py-1.5 text-xs rounded-md font-medium tracking-wide"
          :class="view === 'table' ? 'bg-white/10 shadow-sm text-white' : 'text-gray-500 hover:text-gray-300'"
          @click="view='table'"
        >Table</button>
        <button
          class="px-3 py-1.5 text-xs rounded-md font-medium tracking-wide"
          :class="view === 'kanban' ? 'bg-white/10 shadow-sm text-white' : 'text-gray-500 hover:text-gray-300'"
          @click="view='kanban'; fetchKanban()"
        >Kanban</button>
        <button
          class="px-3 py-1.5 text-xs rounded-md font-medium tracking-wide"
          :class="view === 'reports' ? 'bg-white/10 shadow-sm text-white' : 'text-gray-500 hover:text-gray-300'"
          @click="view='reports'; fetchReports()"
        >Reports</button>
        <button
          class="px-3 py-1.5 text-xs rounded-md font-medium tracking-wide"
          :class="view === 'campaigns' ? 'bg-white/10 shadow-sm text-white' : 'text-gray-500 hover:text-gray-300'"
          @click="view='campaigns'; fetchCampaigns()"
        >Campaigns</button>
      </div>
      <span class="text-xs text-gray-500" x-text="lastRefresh ? 'Updated ' + lastRefresh : ''"></span>
      <span class="text-sm text-gray-400" x-text="currentUser.display_name || ''"></span>
      <template x-if="currentUser.role === 'admin'">
        <button class="text-sm text-blue-400 hover:text-blue-300 transition" @click="usersModalOpen=true; fetchUsers()">Manage Users</button>
      </template>
      <a href="/logout" class="text-sm text-gray-400 hover:text-red-400 transition">Logout</a>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-4 py-6 space-y-6">

  <!-- Stats Cards -->
  <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3">
    <div class="card rounded-xl p-4 group">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Total Leads</div>
      <div class="text-2xl font-bold text-white mt-1" x-text="stats.total ?? '-'"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Emailed</div>
      <div class="text-2xl font-bold text-green-400 mt-1" x-text="stats.breakdown?.emailed ?? 0"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Contacted</div>
      <div class="text-2xl font-bold text-blue-400 mt-1" x-text="stats.breakdown?.contacted ?? 0"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Qualified</div>
      <div class="text-2xl font-bold text-purple-400 mt-1" x-text="stats.breakdown?.qualified ?? 0"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Pending</div>
      <div class="text-2xl font-bold text-amber-400 mt-1" x-text="(stats.breakdown?.new ?? 0) + (stats.breakdown?.intake ?? 0)"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Pipeline Value</div>
      <div class="text-2xl font-bold text-emerald-400 mt-1" x-text="fmtCurrency(stats.pipeline_value)"></div>
    </div>
    <div class="card rounded-xl p-4">
      <div class="text-[11px] font-medium text-gray-500 uppercase tracking-wider">Reminders Due</div>
      <div class="text-2xl font-bold mt-1" :class="(stats.reminders_due ?? 0) > 0 ? 'text-red-400' : 'text-gray-600'" x-text="stats.reminders_due ?? 0"></div>
    </div>
  </div>

  <!-- ============================================================= -->
  <!-- TABLE VIEW -->
  <!-- ============================================================= -->
  <template x-if="view === 'table'">
    <div class="space-y-6">

      <!-- Filters -->
      <div class="filter-bar card rounded-xl p-4 flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
        <input
          type="text"
          placeholder="Search name, email, phone, company..."
          class="flex-1 min-w-0 bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-blue-500/50 placeholder-gray-600"
          x-model.debounce.300ms="search"
          @input="page=1; fetchLeads()"
        >
        <div class="flex gap-3">
          <select
            class="flex-1 sm:flex-none bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            x-model="statusFilter"
            @change="page=1; fetchLeads()"
          >
            <option value="">All Statuses</option>
            <template x-for="s in allStatuses" :key="s">
              <option :value="s" x-text="s"></option>
            </template>
          </select>
          <select
            class="flex-1 sm:flex-none bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            x-model="assigneeFilter"
            @change="page=1; fetchLeads()"
          >
            <option value="">All Assignees</option>
            <option value="__mine__">My Leads</option>
            <option value="__unassigned__">Unassigned</option>
            <template x-for="u in usersList" :key="u.username">
              <option :value="u.username" x-text="u.display_name"></option>
            </template>
          </select>
          <button
            class="bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 px-4 py-2 rounded-md text-sm hover:bg-green-500 transition font-medium"
            @click="addLeadOpen=true; fetchTagOptions()"
          >+ Add Lead</button>
          <button
            class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-4 py-2 rounded-md text-sm hover:bg-blue-500 transition"
            @click="refresh()"
          >Refresh</button>
          <a
            href="/api/leads/export/csv"
            class="bg-gradient-to-r from-purple-600 to-violet-500 text-white shadow-lg shadow-purple-600/10 px-4 py-2 rounded-md text-sm hover:bg-purple-500 transition font-medium inline-flex items-center gap-1"
            download
          >&#11123; Export CSV</a>
        </div>
      </div>

      <!-- Leads Table -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg shadow overflow-x-auto">
        <table class="leads-table min-w-full divide-y divide-gray-800 text-sm">
          <thead class="bg-gray-800/50">
            <tr>
              <template x-for="col in columns" :key="col.key">
                <th
                  class="px-4 py-3 text-left font-medium text-gray-400 uppercase tracking-wider cursor-pointer select-none hover:text-gray-200"
                  @click="toggleSort(col.key)"
                >
                  <span x-text="col.label"></span>
                  <span x-show="sortBy===col.key" x-text="sortDir==='asc' ? ' ▲' : ' ▼'" class="text-blue-400"></span>
                </th>
              </template>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-800/50">
            <template x-if="loading">
              <tr><td :colspan="columns.length" class="px-4 py-8 text-center text-gray-500">Loading...</td></tr>
            </template>
            <template x-if="!loading && leads.length === 0">
              <tr><td :colspan="columns.length" class="px-4 py-8 text-center text-gray-500">No leads found.</td></tr>
            </template>
            <template x-for="lead in leads" :key="lead.lead_id">
              <tr class="hover:bg-gray-800/50 cursor-pointer transition" @click="openDetail(lead.lead_id)">
                <td class="px-4 py-3 text-gray-200" x-text="lead.full_name || '-'"></td>
                <td class="px-4 py-3 text-gray-300" x-text="lead.email || '-'"></td>
                <td class="px-4 py-3 text-gray-300" x-text="lead.phone_number || '-'"></td>
                <td class="px-4 py-3 text-gray-300" x-text="lead.company_name || '-'"></td>
                <td class="px-4 py-3">
                  <span
                    class="inline-block px-2 py-0.5 rounded-full text-xs font-medium"
                    :class="badgeClass(lead.status)"
                    x-text="lead.status"
                  ></span>
                </td>
                <td class="px-4 py-3">
                  <div class="flex flex-wrap gap-1">
                    <template x-if="lead.tags && lead.tags.industry">
                      <span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-900/50 text-blue-300" x-text="lead.tags.industry"></span>
                    </template>
                    <template x-if="lead.tags && lead.tags.lead_source">
                      <span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-green-900/50 text-green-300" x-text="lead.tags.lead_source"></span>
                    </template>
                    <template x-if="lead.lead_source === 'manual'">
                      <span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-900/50 text-amber-300">manual</span>
                    </template>
                  </div>
                </td>
                <td class="px-4 py-3 text-gray-400 text-xs" x-text="lead.assigned_to || '-'"></td>
                <td class="px-4 py-3 whitespace-nowrap text-gray-400" x-text="fmtDate(lead.created_time)"></td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      <div class="flex flex-col sm:flex-row items-center justify-between text-sm text-gray-400 gap-2" x-show="totalPages > 1">
        <span>Page <span x-text="page"></span> of <span x-text="totalPages"></span> (<span x-text="totalLeads"></span> leads)</span>
        <div class="space-x-2">
          <button class="px-3 py-1 border border-gray-700 rounded hover:bg-gray-800 disabled:opacity-40 text-gray-300" :disabled="page<=1" @click="page--; fetchLeads()">Prev</button>
          <button class="px-3 py-1 border border-gray-700 rounded hover:bg-gray-800 disabled:opacity-40 text-gray-300" :disabled="page>=totalPages" @click="page++; fetchLeads()">Next</button>
        </div>
      </div>

    </div>
  </template>

  <!-- ============================================================= -->
  <!-- KANBAN VIEW -->
  <!-- ============================================================= -->
  <template x-if="view === 'kanban'">
    <div>
      <div class="flex items-center gap-3 mb-4">
        <button
          class="bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 px-4 py-2 rounded-md text-sm hover:bg-green-500 transition font-medium"
          @click="addLeadOpen=true; fetchTagOptions()"
        >+ Add Lead</button>
        <button
          class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-4 py-2 rounded-md text-sm hover:bg-blue-500 transition"
          @click="fetchKanban(); fetchStats(); lastRefresh = new Date().toLocaleTimeString()"
        >Refresh</button>
        <a
          href="/api/leads/export/csv"
          class="bg-gradient-to-r from-purple-600 to-violet-500 text-white shadow-lg shadow-purple-600/10 px-4 py-2 rounded-md text-sm hover:bg-purple-500 transition font-medium inline-flex items-center gap-1"
          download
        >&#11123; Export CSV</a>
      </div>

      <div class="kanban-cols flex gap-4 overflow-x-auto pb-4 snap-x snap-mandatory sm:snap-none" style="min-height:60vh">
        <template x-for="col in kanbanOrder" :key="col">
          <div class="flex-shrink-0 w-[85vw] sm:w-72 card rounded-xl p-3 flex flex-col snap-center">
            <!-- Column header -->
            <div class="flex items-center justify-between mb-3">
              <h3 class="font-semibold text-sm uppercase tracking-wide"
                  :class="kanbanHeaderClass(col)" x-text="col"></h3>
              <span class="text-xs text-gray-500 bg-gray-800 rounded-full px-2 py-0.5"
                    x-text="(kanbanData[col] || []).length"></span>
            </div>

            <!-- Cards -->
            <div class="flex-1 space-y-2 overflow-y-auto" style="max-height:calc(100vh - 300px)">
              <template x-for="lead in (kanbanData[col] || [])" :key="lead.lead_id">
                <div class="bg-white/[0.03] rounded-xl border border-white/[0.06] p-3 hover:border-white/[0.12] hover:bg-white/[0.05] transition cursor-pointer"
                     @click="openDetail(lead.lead_id)">
                  <div class="font-medium text-gray-200 text-sm" x-text="lead.full_name || lead.email || 'Unknown'"></div>
                  <div class="text-xs text-gray-400 mt-1" x-text="lead.email || ''"></div>
                  <div class="text-xs text-gray-500 mt-0.5" x-text="lead.company_name || ''"></div>
                  <div class="text-xs text-gray-500 mt-0.5" x-text="fmtDate(lead.created_time)"></div>
                  <template x-if="lead.assigned_to">
                    <div class="text-[10px] text-cyan-400 mt-0.5" x-text="'@ ' + lead.assigned_to"></div>
                  </template>
                  <div class="flex items-center gap-2 mt-0.5">
                    <template x-if="lead.deal_value">
                      <span class="text-[10px] font-medium text-emerald-400" x-text="fmtCurrency(lead.deal_value)"></span>
                    </template>
                    <template x-if="lead.follow_up_date">
                      <span class="text-[10px] font-medium"
                        :class="lead.follow_up_date <= new Date().toISOString().slice(0,10) ? 'text-red-400' : 'text-amber-400'"
                        x-text="'Follow-up: ' + lead.follow_up_date"></span>
                    </template>
                  </div>

                  <!-- Move menu -->
                  <div class="mt-2 flex flex-wrap gap-1" @click.stop>
                    <template x-for="target in kanbanMoveTargets(col)" :key="target">
                      <button
                        class="text-[10px] px-1.5 py-0.5 rounded border hover:bg-gray-700 transition"
                        :class="moveBtnClass(target)"
                        x-text="target"
                        @click="moveLeadStatus(lead.lead_id, target)"
                      ></button>
                    </template>
                  </div>
                </div>
              </template>

              <template x-if="(kanbanData[col] || []).length === 0">
                <div class="text-xs text-gray-600 text-center py-6">No leads</div>
              </template>
            </div>
          </div>
        </template>
      </div>
    </div>
  </template>

  <!-- ============================================================= -->
  <!-- REPORTS VIEW -->
  <!-- ============================================================= -->
  <template x-if="view === 'reports'">
    <div class="space-y-6">

      <!-- Conversion Funnel -->
      <div class="card rounded-xl p-5">
        <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wide mb-4">Conversion Funnel</h3>
        <div class="space-y-2">
          <template x-for="stage in reportFunnel" :key="stage.status">
            <div class="flex items-center gap-3">
              <span class="w-32 text-sm text-gray-400 text-right" x-text="stage.status"></span>
              <div class="flex-1 bg-gray-800 rounded-full h-6 overflow-hidden">
                <div class="h-full rounded-full transition-all duration-500"
                  :class="badgeClass(stage.status)"
                  :style="'width:' + (reportFunnel.length ? Math.max(4, (stage.count / Math.max(...reportFunnel.map(s=>s.count))) * 100) : 0) + '%'">
                </div>
              </div>
              <span class="w-12 text-sm font-medium text-gray-200 text-right" x-text="stage.count"></span>
            </div>
          </template>
          <template x-if="reportFunnel.length === 0">
            <p class="text-sm text-gray-600">No data</p>
          </template>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

        <!-- Leads by Source -->
        <div class="card rounded-xl p-5">
          <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wide mb-4">Leads by Source</h3>
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead>
                <tr class="border-b border-white/[0.06]">
                  <th class="text-left py-2 text-gray-500 font-medium">Source</th>
                  <th class="text-right py-2 text-gray-500 font-medium">Leads</th>
                  <th class="text-right py-2 text-gray-500 font-medium">Closed</th>
                  <th class="text-right py-2 text-gray-500 font-medium">Value</th>
                </tr>
              </thead>
              <tbody>
                <template x-for="src in reportBySource" :key="src.lead_source">
                  <tr class="border-b border-white/[0.06]/50">
                    <td class="py-2 text-gray-300" x-text="src.lead_source || 'unknown'"></td>
                    <td class="py-2 text-gray-200 text-right" x-text="src.count"></td>
                    <td class="py-2 text-green-400 text-right" x-text="src.closed_count"></td>
                    <td class="py-2 text-emerald-400 text-right" x-text="fmtCurrency(src.total_value)"></td>
                  </tr>
                </template>
              </tbody>
            </table>
            <template x-if="reportBySource.length === 0">
              <p class="text-sm text-gray-600 py-4">No data</p>
            </template>
          </div>
        </div>

        <!-- Pipeline Value by Status -->
        <div class="card rounded-xl p-5">
          <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wide mb-4">Pipeline Value by Status</h3>
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead>
                <tr class="border-b border-white/[0.06]">
                  <th class="text-left py-2 text-gray-500 font-medium">Status</th>
                  <th class="text-right py-2 text-gray-500 font-medium">Deals</th>
                  <th class="text-right py-2 text-gray-500 font-medium">Total Value</th>
                </tr>
              </thead>
              <tbody>
                <template x-for="pv in reportPipelineValue" :key="pv.status">
                  <tr class="border-b border-white/[0.06]/50">
                    <td class="py-2"><span class="px-2 py-0.5 rounded text-xs font-medium" :class="badgeClass(pv.status)" x-text="pv.status"></span></td>
                    <td class="py-2 text-gray-200 text-right" x-text="pv.count"></td>
                    <td class="py-2 text-emerald-400 text-right font-medium" x-text="fmtCurrency(pv.total_value)"></td>
                  </tr>
                </template>
              </tbody>
            </table>
            <template x-if="reportPipelineValue.length === 0">
              <p class="text-sm text-gray-600 py-4">No deals with values yet</p>
            </template>
          </div>
        </div>
      </div>

      <!-- Leads Over Time (last 30 days) -->
      <div class="card rounded-xl p-5">
        <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wide mb-4">Leads Over Time (30 days)</h3>
        <div class="flex items-end gap-[2px] h-32" x-show="reportOverTime.length > 0">
          <template x-for="day in reportOverTime" :key="day.day">
            <div class="flex-1 flex flex-col items-center justify-end h-full group relative">
              <div class="w-full bg-blue-500/70 rounded-t transition-all hover:bg-blue-400/80"
                :style="'height:' + (reportOverTime.length ? Math.max(4, (day.count / Math.max(...reportOverTime.map(d=>d.count))) * 100) : 0) + '%'"
                :title="day.day + ': ' + day.count + ' leads'">
              </div>
            </div>
          </template>
        </div>
        <div class="flex justify-between text-[10px] text-gray-600 mt-1" x-show="reportOverTime.length > 0">
          <span x-text="reportOverTime.length ? reportOverTime[0].day : ''"></span>
          <span x-text="reportOverTime.length ? reportOverTime[reportOverTime.length-1].day : ''"></span>
        </div>
        <template x-if="reportOverTime.length === 0">
          <p class="text-sm text-gray-600">No data in the last 30 days</p>
        </template>
      </div>

      <!-- Email Sequences Manager -->
      <div class="card rounded-xl p-5">
        <div class="flex items-center justify-between mb-4">
          <h3 class="text-sm font-semibold text-gray-300 uppercase tracking-wide">Email Sequences</h3>
          <button class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-3 py-1.5 rounded text-xs hover:bg-blue-500" @click="seqEditing={name:'',steps:[{delay_days:0,subject:'',body:''}]}; seqModalOpen=true">New Sequence</button>
        </div>
        <div class="space-y-2">
          <template x-for="seq in sequencesList" :key="seq.id">
            <div class="bg-white/[0.03] rounded-xl p-3 flex items-center justify-between border border-white/[0.04]">
              <div>
                <div class="text-sm font-medium text-gray-200" x-text="seq.name"></div>
                <div class="text-xs text-gray-500">
                  <span x-text="(seq.steps?.length || 0) + ' steps'"></span> &middot;
                  <span x-text="(seq.active_enrollments || 0) + ' active enrollments'"></span>
                </div>
              </div>
              <div class="flex gap-2">
                <button class="text-xs px-2 py-1 rounded border border-gray-600 text-gray-400 hover:bg-gray-700"
                  @click="seqEditing=JSON.parse(JSON.stringify(seq)); seqModalOpen=true">Edit</button>
                <button class="text-xs px-2 py-1 rounded border border-red-700 text-red-400 hover:bg-red-900/30"
                  @click="deleteSequence(seq.id)">Delete</button>
              </div>
            </div>
          </template>
          <template x-if="sequencesList.length === 0">
            <p class="text-sm text-gray-600">No sequences yet. Create one to start drip campaigns.</p>
          </template>
        </div>
      </div>

    </div>
  </template>

  <!-- ============================================================= -->
  <!-- CAMPAIGNS VIEW -->
  <!-- ============================================================= -->
  <template x-if="view === 'campaigns'">
    <div class="space-y-6">

      <div class="flex items-center justify-between">
        <h2 class="text-lg font-semibold text-gray-200">Campaigns</h2>
        <button class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-4 py-2 rounded-md text-sm hover:bg-blue-500 transition font-medium"
          @click="openCampaignBuilder()">New Campaign</button>
      </div>

      <!-- Campaign list -->
      <div class="space-y-3">
        <template x-for="c in campaignsList" :key="c.id">
          <div class="card rounded-xl p-4">
            <div class="flex items-center justify-between">
              <div>
                <div class="font-medium text-gray-200" x-text="c.name"></div>
                <div class="text-xs text-gray-500 mt-1">
                  <span x-text="c.email_provider"></span> &middot;
                  <span x-text="c.campaign_type"></span> &middot;
                  <span x-text="c.total_recipients + ' recipients'"></span> &middot;
                  <span x-text="c.sent_count + ' sent'"></span>
                  <template x-if="c.failed_count > 0">
                    <span class="text-red-400" x-text="', ' + c.failed_count + ' failed'"></span>
                  </template>
                </div>
              </div>
              <div class="flex items-center gap-3">
                <span class="text-xs px-2 py-0.5 rounded font-medium"
                  :class="{
                    'bg-gray-700 text-gray-400': c.status === 'draft',
                    'bg-amber-900/50 text-amber-300': c.status === 'sending',
                    'bg-green-900/50 text-green-300': c.status === 'completed',
                    'bg-red-900/50 text-red-300': c.status === 'failed',
                    'bg-blue-900/50 text-blue-300': c.status === 'paused',
                  }"
                  x-text="c.status"></span>
                <template x-if="c.status === 'draft' || c.status === 'paused'">
                  <button class="text-xs px-2 py-1 rounded border border-gray-600 text-gray-400 hover:bg-gray-700"
                    @click="openCampaignBuilder(c)">Edit</button>
                </template>
                <button class="text-xs px-2 py-1 rounded border border-red-700 text-red-400 hover:bg-red-900/30"
                  @click="deleteCampaign(c.id)">Delete</button>
              </div>
            </div>
          </div>
        </template>
        <template x-if="campaignsList.length === 0">
          <div class="card rounded-xl p-8 text-center">
            <p class="text-gray-500">No campaigns yet. Create one to start sending.</p>
          </div>
        </template>
      </div>

    </div>
  </template>

</main>

<!-- Campaign Builder Modal -->
<div
  x-show="campaignBuilderOpen"
  x-cloak
  class="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60"
  @click.self="campaignBuilderOpen=false"
  @keydown.escape.window="campaignBuilderOpen ? campaignBuilderOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-3xl w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100" x-text="campaignDraft.id ? 'Edit Campaign' : 'New Campaign'"></h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="campaignBuilderOpen=false">&times;</button>
    </div>
    <div class="px-6 py-4 space-y-4">

      <!-- Name & Provider -->
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <div class="sm:col-span-1">
          <label class="block text-xs text-gray-400 mb-1">Campaign Name</label>
          <input type="text" x-model="campaignDraft.name" placeholder="e.g. Q1 Outreach"
            class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        </div>
        <div>
          <label class="block text-xs text-gray-400 mb-1">Email Provider</label>
          <select x-model="campaignDraft.email_provider"
            class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
            <template x-for="p in emailProviders" :key="p.id">
              <option :value="p.id" x-text="p.name" :disabled="!p.configured"></option>
            </template>
          </select>
        </div>
        <div>
          <label class="block text-xs text-gray-400 mb-1">Type</label>
          <select x-model="campaignDraft.campaign_type"
            class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
            <option value="single">Single Send</option>
          </select>
        </div>
      </div>

      <!-- Lead Filters -->
      <div class="border border-gray-800 rounded-lg p-4">
        <div class="flex items-center justify-between mb-3">
          <label class="text-xs font-medium text-gray-400 uppercase tracking-wide">Target Audience</label>
          <span class="text-xs text-gray-500" x-text="campaignFilterCount + ' leads match'"></span>
        </div>
        <div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
          <select x-model="campaignDraft.lead_filters.status" @change="previewCampaignFilter()"
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
            <option value="">Any Status</option>
            <template x-for="s in allKanbanStatuses" :key="s">
              <option :value="s" x-text="s"></option>
            </template>
          </select>
          <select x-model="campaignDraft.lead_filters.lead_source" @change="previewCampaignFilter()"
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
            <option value="">Any Source</option>
            <option value="meta">meta</option>
            <option value="manual">manual</option>
            <option value="webhook">webhook</option>
          </select>
          <input type="text" x-model="campaignDraft.lead_filters.search" @input.debounce.500ms="previewCampaignFilter()"
            placeholder="Search name/email/company..."
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm placeholder-gray-500">
        </div>
        <!-- Sample preview -->
        <template x-if="campaignFilterSample.length > 0">
          <div class="mt-3 text-xs text-gray-500">
            <span class="font-medium">Sample:</span>
            <template x-for="l in campaignFilterSample.slice(0,5)" :key="l.lead_id">
              <span class="ml-2" x-text="(l.full_name || l.email)"></span>
            </template>
            <template x-if="campaignFilterCount > 5">
              <span class="ml-1" x-text="'... and ' + (campaignFilterCount - 5) + ' more'"></span>
            </template>
          </div>
        </template>
      </div>

      <!-- Email Content -->
      <div class="space-y-3">
        <div class="flex items-center justify-between">
          <label class="text-xs font-medium text-gray-400 uppercase tracking-wide">Email Content</label>
          <button class="text-xs text-purple-400 hover:text-purple-300 font-medium"
            @click="generateCampaignCopy()" :disabled="campaignGenerating">
            <span x-show="!campaignGenerating">Generate with AI</span>
            <span x-show="campaignGenerating">Generating...</span>
          </button>
        </div>
        <div>
          <label class="block text-xs text-gray-500 mb-1">Subject (use {name} for recipient name)</label>
          <input type="text" x-model="campaignDraft.subject_template"
            class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600"
            placeholder="Hey {name}, quick question">
        </div>
        <div>
          <label class="block text-xs text-gray-500 mb-1">Body HTML (use {name} for recipient name)</label>
          <textarea x-model="campaignDraft.body_template" rows="6"
            class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm font-mono placeholder-gray-500"
            placeholder="<p>Hi {name},</p><p>...</p>"></textarea>
        </div>
        <!-- AI context input (shown when generating) -->
        <div>
          <label class="block text-xs text-gray-500 mb-1">AI Context (optional — describe the campaign goal)</label>
          <input type="text" x-model="campaignAiContext"
            class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600"
            placeholder="e.g. Follow up on Q1 demo requests, offer free trial">
        </div>
        <!-- Preview -->
        <template x-if="campaignDraft.body_template">
          <div>
            <label class="block text-xs text-gray-500 mb-1">Preview</label>
            <div class="bg-white rounded-md p-4 text-gray-900 text-sm" x-html="campaignDraft.body_template.replace(/\{name\}/g, 'John')"></div>
          </div>
        </template>
      </div>

      <!-- Actions -->
      <div class="flex gap-3 pt-2 border-t border-white/[0.06]">
        <button
          class="flex-1 bg-gray-700 text-gray-300 py-2 rounded-md text-sm hover:bg-gray-600 transition"
          @click="saveCampaignDraft()"
          :disabled="!campaignDraft.name"
        >Save Draft</button>
        <button
          class="flex-1 bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 py-2 rounded-md text-sm font-medium hover:bg-green-500 transition"
          @click="sendCampaign()"
          :disabled="!campaignDraft.name || !campaignDraft.subject_template || !campaignDraft.body_template || campaignFilterCount === 0 || campaignSending"
        >
          <span x-show="!campaignSending" x-text="'Send to ' + campaignFilterCount + ' leads'"></span>
          <span x-show="campaignSending">Sending...</span>
        </button>
        <button
          class="bg-gray-800 text-gray-400 px-4 py-2 rounded-md text-sm hover:bg-gray-700 transition"
          @click="campaignBuilderOpen=false"
        >Cancel</button>
      </div>
      <p class="text-red-400 text-xs" x-show="campaignError" x-text="campaignError"></p>
      <p class="text-green-400 text-xs" x-show="campaignSuccess" x-text="campaignSuccess"></p>
    </div>
  </div>
</div>

<!-- Sequence Editor Modal -->
<div
  x-show="seqModalOpen"
  x-cloak
  class="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60"
  @click.self="seqModalOpen=false"
  @keydown.escape.window="seqModalOpen ? seqModalOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-2xl w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100" x-text="seqEditing?.id ? 'Edit Sequence' : 'New Sequence'"></h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="seqModalOpen=false">&times;</button>
    </div>
    <div class="px-6 py-4 space-y-4">
      <div>
        <label class="block text-xs text-gray-400 mb-1">Sequence Name</label>
        <input type="text" x-model="seqEditing.name" placeholder="e.g. Welcome Drip"
          class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
      </div>
      <div>
        <div class="flex items-center justify-between mb-2">
          <label class="text-xs text-gray-400">Steps</label>
          <button class="text-xs text-blue-400 hover:text-blue-300" @click="seqEditing.steps.push({delay_days:3,subject:'',body:''})">+ Add Step</button>
        </div>
        <div class="space-y-3">
          <template x-for="(step, idx) in seqEditing.steps" :key="idx">
            <div class="bg-gray-800 rounded-lg p-3 border border-gray-700">
              <div class="flex items-center justify-between mb-2">
                <span class="text-xs font-medium text-gray-400" x-text="'Step ' + (idx+1)"></span>
                <div class="flex items-center gap-3">
                  <label class="text-xs text-gray-500">Delay (days):</label>
                  <input type="number" min="0" x-model.number="step.delay_days"
                    class="w-16 bg-gray-700 border border-gray-600 text-gray-100 rounded px-2 py-1 text-xs">
                  <button class="text-xs text-red-400 hover:text-red-300" @click="seqEditing.steps.splice(idx,1)" x-show="seqEditing.steps.length > 1">&times;</button>
                </div>
              </div>
              <input type="text" x-model="step.subject" placeholder="Subject (use {name} for lead name)"
                class="w-full bg-gray-700 border border-gray-600 text-gray-100 rounded px-2 py-1 text-sm placeholder-gray-500 mb-2">
              <textarea x-model="step.body" rows="3" placeholder="Body HTML (use {name} for lead name)"
                class="w-full bg-gray-700 border border-gray-600 text-gray-100 rounded px-2 py-1 text-sm font-mono placeholder-gray-500"></textarea>
            </div>
          </template>
        </div>
      </div>
      <div class="flex gap-3 pt-2">
        <button
          class="flex-1 bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 py-2 rounded-md text-sm font-medium hover:bg-green-500 transition"
          @click="saveSequence()"
          :disabled="!seqEditing.name || !seqEditing.steps.length"
        >Save Sequence</button>
        <button
          class="flex-1 bg-gray-700 text-gray-300 py-2 rounded-md text-sm hover:bg-gray-600 transition"
          @click="seqModalOpen=false"
        >Cancel</button>
      </div>
      <p class="text-red-400 text-xs" x-show="seqError" x-text="seqError"></p>
    </div>
  </div>
</div>

<!-- Detail Modal -->
<div
  x-show="modalOpen"
  x-cloak
  class="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60"
  @click.self="modalOpen=false"
  @keydown.escape.window="modalOpen && !emailModalOpen && !addLeadOpen ? modalOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-lg w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100">Lead Detail</h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="modalOpen=false">&times;</button>
    </div>
    <template x-if="modalLoading">
      <div class="px-6 py-8 text-center text-gray-500">Loading...</div>
    </template>
    <template x-if="!modalLoading && detail">
      <div class="px-6 py-4 space-y-3 text-sm">
        <template x-for="[k,v] in detailPairs()" :key="k">
          <div class="flex flex-col sm:flex-row">
            <span class="sm:w-40 font-medium text-gray-500" x-text="k"></span>
            <span class="flex-1 text-gray-200 break-all" x-text="typeof v === 'object' ? JSON.stringify(v) : v"></span>
          </div>
        </template>

        <!-- Tags display & edit -->
        <div class="pt-2 border-t border-white/[0.06]">
          <div class="font-medium text-gray-500 mb-2">Tags</div>
          <div class="flex flex-wrap gap-1 mb-2">
            <template x-if="detail.tags && detail.tags.industry">
              <span class="inline-block px-2 py-0.5 rounded text-xs font-medium bg-blue-900/50 text-blue-300" x-text="detail.tags.industry"></span>
            </template>
            <template x-if="detail.tags && detail.tags.lead_source">
              <span class="inline-block px-2 py-0.5 rounded text-xs font-medium bg-green-900/50 text-green-300" x-text="detail.tags.lead_source"></span>
            </template>
            <template x-if="detail.tags && detail.tags.custom">
              <template x-for="ct in detail.tags.custom" :key="ct">
                <span class="inline-block px-2 py-0.5 rounded text-xs font-medium bg-gray-700 text-gray-300" x-text="ct"></span>
              </template>
            </template>
          </div>
          <button
            class="text-xs text-blue-400 hover:text-blue-300"
            @click="editingTags = !editingTags; if(editingTags) { tagEditIndustry = (detail.tags||{}).industry||''; tagEditSource = (detail.tags||{}).lead_source||''; tagEditCustom = ((detail.tags||{}).custom||[]).join(', '); fetchTagOptions(); }"
            x-text="editingTags ? 'Cancel' : 'Edit Tags'"
          ></button>
          <template x-if="editingTags">
            <div class="mt-2 space-y-2">
              <select x-model="tagEditIndustry" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
                <option value="">Select Industry</option>
                <template x-for="ind in tagOptions.industries" :key="ind">
                  <option :value="ind" x-text="ind"></option>
                </template>
              </select>
              <select x-model="tagEditSource" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
                <option value="">Select Lead Source</option>
                <template x-for="src in tagOptions.lead_sources" :key="src">
                  <option :value="src" x-text="src"></option>
                </template>
              </select>
              <input type="text" x-model="tagEditCustom" placeholder="Custom tags (comma-separated)"
                class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm placeholder-gray-600">
              <button
                class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-3 py-1 rounded text-xs hover:bg-blue-500"
                @click="saveTags()"
              >Save Tags</button>
            </div>
          </template>
        </div>

        <!-- Status change in modal -->
        <div class="flex flex-col sm:flex-row items-start sm:items-center gap-2 pt-2 border-t border-white/[0.06]">
          <span class="sm:w-40 font-medium text-gray-500">Change status</span>
          <select
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm"
            :value="detail.status"
            @change="moveLeadStatus(detail.lead_id, $event.target.value); detail.status = $event.target.value"
          >
            <template x-for="s in allKanbanStatuses" :key="s">
              <option :value="s" x-text="s" :selected="s === detail.status"></option>
            </template>
          </select>
        </div>

        <!-- Deal Info -->
        <div class="flex flex-col gap-2 pt-2 border-t border-white/[0.06]">
          <span class="font-medium text-gray-500">Deal Info</span>
          <div class="grid grid-cols-2 gap-3">
            <div>
              <label class="block text-xs text-gray-500 mb-1">Deal Value ($)</label>
              <input type="number" step="0.01" min="0"
                :value="detail.deal_value ?? ''"
                @change="updateDeal('deal_value', $event.target.value)"
                class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm placeholder-gray-600"
                placeholder="0.00">
            </div>
            <div>
              <label class="block text-xs text-gray-500 mb-1">Expected Close</label>
              <input type="date"
                :value="detail.expected_close_date ?? ''"
                @change="updateDeal('expected_close_date', $event.target.value)"
                class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
            </div>
          </div>
        </div>

        <!-- Follow-up Reminder -->
        <div class="flex flex-col sm:flex-row items-start sm:items-center gap-2 pt-2 border-t border-white/[0.06]">
          <span class="sm:w-40 font-medium text-gray-500">Follow-up</span>
          <div class="flex items-center gap-2 flex-1">
            <input type="date"
              :value="detail.follow_up_date ?? ''"
              @change="setFollowUp($event.target.value)"
              class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
            <template x-if="detail.follow_up_date">
              <button class="text-xs text-red-400 hover:text-red-300" @click="setFollowUp('')">Clear</button>
            </template>
            <template x-if="detail.follow_up_date && detail.follow_up_date <= new Date().toISOString().slice(0,10)">
              <span class="text-xs font-medium text-red-400">Overdue</span>
            </template>
          </div>
        </div>

        <!-- Notes -->
        <div class="pt-2 border-t border-white/[0.06]">
          <div class="flex items-center justify-between mb-2">
            <span class="font-medium text-gray-500">Notes</span>
            <span class="text-xs text-gray-600" x-text="(detailNotes.length || 0) + ' note(s)'"></span>
          </div>
          <div class="flex gap-2 mb-2">
            <input type="text" x-model="newNoteContent" placeholder="Add a note..."
              class="flex-1 bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm placeholder-gray-600"
              @keydown.enter="addNote()">
            <button class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-3 py-1 rounded text-xs hover:bg-blue-500" @click="addNote()" :disabled="!newNoteContent.trim()">Add</button>
          </div>
          <div class="space-y-2 max-h-48 overflow-y-auto">
            <template x-for="note in detailNotes" :key="note.id">
              <div class="bg-white/[0.03] rounded-lg p-2.5 text-sm border border-white/[0.04]">
                <div class="flex justify-between items-start">
                  <span class="text-gray-300" x-text="note.content"></span>
                  <button class="text-gray-600 hover:text-red-400 text-xs ml-2 shrink-0" @click="deleteNote(note.id)">&times;</button>
                </div>
                <div class="text-xs text-gray-600 mt-1">
                  <span x-text="note.author"></span> &middot; <span x-text="fmtDate(note.created_at)"></span>
                </div>
              </div>
            </template>
            <template x-if="detailNotes.length === 0">
              <p class="text-xs text-gray-600">No notes yet</p>
            </template>
          </div>
        </div>

        <!-- Sequences -->
        <div class="pt-2 border-t border-white/[0.06]">
          <div class="flex items-center justify-between mb-2">
            <span class="font-medium text-gray-500">Sequences</span>
          </div>
          <!-- Active enrollments -->
          <template x-for="enr in detailEnrollments" :key="enr.id">
            <div class="bg-white/[0.03] rounded-lg p-2.5 text-sm border border-white/[0.04] mb-2">
              <div class="flex justify-between items-center">
                <span class="text-gray-300" x-text="enr.sequence_name"></span>
                <div class="flex items-center gap-2">
                  <span class="text-xs px-1.5 py-0.5 rounded" :class="enr.status === 'active' ? 'bg-green-900/50 text-green-300' : enr.status === 'completed' ? 'bg-gray-700 text-gray-400' : 'bg-red-900/50 text-red-300'" x-text="enr.status"></span>
                  <template x-if="enr.status === 'active'">
                    <button class="text-xs text-red-400 hover:text-red-300" @click="unenrollLead(enr.sequence_id)">Cancel</button>
                  </template>
                </div>
              </div>
              <div class="text-xs text-gray-600 mt-1">
                Step <span x-text="enr.current_step + 1"></span> of <span x-text="enr.total_steps"></span>
                <template x-if="enr.status === 'active' && enr.next_send_at">
                  <span> &middot; Next: <span x-text="fmtDate(enr.next_send_at)"></span></span>
                </template>
              </div>
            </div>
          </template>
          <!-- Enroll -->
          <div class="flex gap-2">
            <select x-model="enrollSequenceId" class="flex-1 bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm">
              <option value="">Enroll in sequence...</option>
              <template x-for="seq in sequencesList" :key="seq.id">
                <option :value="seq.id" x-text="seq.name + ' (' + (seq.steps?.length || 0) + ' steps)'"></option>
              </template>
            </select>
            <button class="bg-gradient-to-r from-blue-600 to-blue-500 text-white shadow-lg shadow-blue-600/10 px-3 py-1 rounded text-xs hover:bg-blue-500" @click="enrollLead()" :disabled="!enrollSequenceId">Enroll</button>
          </div>
          <template x-if="sequencesList.length === 0">
            <p class="text-xs text-gray-600 mt-1">No sequences created yet. Go to Reports > Sequences to create one.</p>
          </template>
        </div>

        <!-- AI Email button (manual leads only) -->
        <template x-if="detail.lead_source === 'manual' || (detail.lead_id && detail.lead_id.startsWith('manual-'))">
          <div class="pt-2 border-t border-white/[0.06]">
            <button
              class="bg-gradient-to-r from-purple-600 to-violet-500 text-white shadow-lg shadow-purple-600/10 px-4 py-2 rounded-md text-sm hover:bg-purple-500 transition font-medium w-full"
              @click="generateEmail()"
              :disabled="emailGenerating"
            >
              <span x-show="!emailGenerating">Generate AI Email</span>
              <span x-show="emailGenerating">Generating...</span>
            </button>
            <p class="text-xs text-gray-500 mt-1">Uses AI to create a personalized outreach email for this lead</p>
            <p class="text-red-400 text-xs mt-1" x-show="emailError" x-text="emailError"></p>
          </div>
        </template>
        <!-- Meta lead info -->
        <template x-if="detail.lead_source !== 'manual' && !(detail.lead_id && detail.lead_id.startsWith('manual-'))">
          <div class="pt-2 border-t border-white/[0.06]">
            <p class="text-xs text-gray-500">This lead was auto-imported from Meta and emailed via the automated pipeline.</p>
          </div>
        </template>

        <!-- Assign lead -->
        <div class="flex flex-col sm:flex-row items-start sm:items-center gap-2 pt-2 border-t border-white/[0.06]">
          <span class="sm:w-40 font-medium text-gray-500">Assign to</span>
          <select
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-2 py-1.5 text-sm"
            :value="detail.assigned_to || ''"
            @change="assignLead(detail.lead_id, $event.target.value); detail.assigned_to = $event.target.value"
          >
            <option value="">Unassigned</option>
            <template x-for="u in usersList" :key="u.username">
              <option :value="u.username" x-text="u.display_name" :selected="u.username === detail.assigned_to"></option>
            </template>
          </select>
        </div>

        <!-- Delete lead -->
        <div class="pt-2 border-t border-white/[0.06]">
          <button
            class="bg-red-600 text-white px-4 py-2 rounded-md text-sm hover:bg-red-500 transition font-medium w-full"
            @click="deleteLead()"
          >Delete Lead</button>
        </div>

        <!-- Raw field data -->
        <template x-if="detail.raw_field_data">
          <div>
            <div class="font-medium text-gray-500 mb-1">Raw Field Data</div>
            <pre class="bg-gray-800 border border-gray-700 rounded p-3 text-xs text-gray-300 overflow-x-auto" x-text="JSON.stringify(detail.raw_field_data, null, 2)"></pre>
          </div>
        </template>
      </div>
    </template>
  </div>
</div>

<!-- Add Lead Modal -->
<div
  x-show="addLeadOpen"
  x-cloak
  class="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60"
  @click.self="addLeadOpen=false"
  @keydown.escape.window="addLeadOpen ? addLeadOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-lg w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100">Add Lead</h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="addLeadOpen=false">&times;</button>
    </div>
    <div class="px-6 py-4 space-y-3">
      <div class="grid grid-cols-2 gap-3">
        <div>
          <label class="block text-xs text-gray-400 mb-1">First Name</label>
          <input type="text" x-model="newLead.first_name" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        </div>
        <div>
          <label class="block text-xs text-gray-400 mb-1">Last Name</label>
          <input type="text" x-model="newLead.last_name" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        </div>
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Email *</label>
        <input type="email" x-model="newLead.email" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600" required>
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Phone</label>
        <input type="text" x-model="newLead.phone_number" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Company</label>
        <input type="text" x-model="newLead.company_name" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Job Title</label>
        <input type="text" x-model="newLead.job_title" class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Industry</label>
        <select x-model="newLead.industry" class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
          <option value="">Select Industry</option>
          <template x-for="ind in tagOptions.industries" :key="ind">
            <option :value="ind" x-text="ind"></option>
          </template>
        </select>
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Lead Source</label>
        <select x-model="newLead.lead_source_tag" class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
          <option value="">Select Source</option>
          <template x-for="src in tagOptions.lead_sources" :key="src">
            <option :value="src" x-text="src"></option>
          </template>
        </select>
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Custom Tags (comma-separated)</label>
        <input type="text" x-model="newLead.custom_tags" placeholder="e.g. VIP, follow-up, demo"
          class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        <div class="flex flex-wrap gap-1 mt-1">
          <template x-for="tag in (newLead.custom_tags||'').split(',').filter(t=>t.trim())" :key="tag">
            <span class="inline-block px-2 py-0.5 rounded text-[10px] font-medium bg-gray-700 text-gray-300" x-text="tag.trim()"></span>
          </template>
        </div>
      </div>
      <div class="grid grid-cols-2 gap-3">
        <div>
          <label class="block text-xs text-gray-400 mb-1">Deal Value ($)</label>
          <input type="number" step="0.01" min="0" x-model="newLead.deal_value" placeholder="0.00"
            class="w-full bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        </div>
        <div>
          <label class="block text-xs text-gray-400 mb-1">Expected Close</label>
          <input type="date" x-model="newLead.expected_close_date"
            class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
        </div>
      </div>
      <div class="flex gap-3 pt-2">
        <button
          class="flex-1 bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 py-2 rounded-md text-sm font-medium hover:bg-green-500 transition"
          @click="submitNewLead()"
          :disabled="!newLead.email"
        >Save Lead</button>
        <button
          class="flex-1 bg-gray-700 text-gray-300 py-2 rounded-md text-sm hover:bg-gray-600 transition"
          @click="addLeadOpen=false"
        >Cancel</button>
      </div>
      <p class="text-red-400 text-xs" x-show="addLeadError" x-text="addLeadError"></p>
    </div>
  </div>
</div>

<!-- AI Email Preview Modal -->
<div
  x-show="emailModalOpen"
  x-cloak
  class="fixed inset-0 z-[60] flex items-end sm:items-center justify-center bg-black/60"
  @click.self="emailModalOpen=false"
  @keydown.escape.window="emailModalOpen ? emailModalOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-2xl w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100">AI Email Preview</h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="emailModalOpen=false">&times;</button>
    </div>
    <div class="px-6 py-4 space-y-3">
      <div>
        <label class="block text-xs text-gray-400 mb-1">Subject</label>
        <input type="text" x-model="emailDraft.subject"
          class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Body (HTML)</label>
        <textarea x-model="emailDraft.body" rows="8"
          class="w-full bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm font-mono"></textarea>
      </div>
      <div>
        <label class="block text-xs text-gray-400 mb-1">Preview</label>
        <div class="bg-white rounded-md p-4 text-gray-900 text-sm" x-html="emailDraft.body"></div>
      </div>
      <div class="flex gap-3 pt-2">
        <button
          class="bg-gradient-to-r from-purple-600 to-violet-500 text-white shadow-lg shadow-purple-600/10 px-4 py-2 rounded-md text-sm hover:bg-purple-500 transition"
          @click="generateEmail()"
          :disabled="emailGenerating"
        >Regenerate</button>
        <button
          class="flex-1 bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 py-2 rounded-md text-sm font-medium hover:bg-green-500 transition"
          @click="sendEmail()"
          :disabled="emailSending"
        >
          <span x-show="!emailSending">Send Email</span>
          <span x-show="emailSending">Sending...</span>
        </button>
        <button
          class="bg-gray-700 text-gray-300 px-4 py-2 rounded-md text-sm hover:bg-gray-600 transition"
          @click="emailModalOpen=false"
        >Cancel</button>
      </div>
      <p class="text-red-400 text-xs" x-show="emailError" x-text="emailError"></p>
      <p class="text-green-400 text-xs" x-show="emailSuccess" x-text="emailSuccess"></p>
    </div>
  </div>
</div>

<!-- Users Management Modal (admin only) -->
<div
  x-show="usersModalOpen"
  x-cloak
  class="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60"
  @click.self="usersModalOpen=false"
  @keydown.escape.window="usersModalOpen ? usersModalOpen=false : null"
>
  <div class="modal-content bg-[#111118] border border-white/[0.06] rounded-t-2xl sm:rounded-2xl shadow-2xl max-w-lg w-full sm:mx-4 max-h-[90vh] sm:max-h-[85vh] overflow-y-auto">
    <div class="flex items-center justify-between px-6 py-4 border-b border-white/[0.06]">
      <h2 class="text-lg font-bold text-gray-100">Manage Users</h2>
      <button class="text-gray-500 hover:text-gray-300 text-xl" @click="usersModalOpen=false">&times;</button>
    </div>
    <div class="px-6 py-4 space-y-4">
      <!-- Existing users -->
      <template x-for="u in managedUsers" :key="u.username">
        <div class="flex items-center justify-between bg-gray-800 rounded-lg p-3">
          <div>
            <div class="text-sm font-medium text-gray-200" x-text="u.display_name || u.username"></div>
            <div class="text-xs text-gray-500" x-text="u.username + ' (' + u.role + ')'"></div>
          </div>
          <div class="flex gap-2">
            <button
              class="text-xs px-2 py-1 rounded border border-gray-600 text-gray-400 hover:bg-gray-700"
              @click="resetUserPassword(u.username)"
            >Reset PW</button>
            <template x-if="u.username !== currentUser.username">
              <button
                class="text-xs px-2 py-1 rounded border border-red-700 text-red-400 hover:bg-red-900/30"
                @click="deleteUser(u.username)"
              >Delete</button>
            </template>
          </div>
        </div>
      </template>

      <!-- Add new user form -->
      <div class="border-t border-white/[0.06] pt-4">
        <h3 class="text-sm font-medium text-gray-400 mb-2">Add New User</h3>
        <div class="grid grid-cols-2 gap-2">
          <input type="text" x-model="newUser.username" placeholder="Username"
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
          <input type="text" x-model="newUser.display_name" placeholder="Display Name"
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
        </div>
        <div class="grid grid-cols-2 gap-2 mt-2">
          <input type="password" x-model="newUser.password" placeholder="Password"
            class="bg-white/[0.04] border border-white/[0.08] text-gray-100 rounded-lg px-3 py-2 text-sm placeholder-gray-600">
          <select x-model="newUser.role"
            class="bg-gray-800 border border-gray-700 text-gray-100 rounded-md px-3 py-2 text-sm">
            <option value="user">User</option>
            <option value="admin">Admin</option>
          </select>
        </div>
        <button
          class="mt-2 bg-gradient-to-r from-green-600 to-emerald-500 text-white shadow-lg shadow-green-600/10 px-4 py-2 rounded-md text-sm hover:bg-green-500 transition font-medium w-full"
          @click="createUser()"
        >Add User</button>
        <p class="text-red-400 text-xs mt-1" x-show="userError" x-text="userError"></p>
      </div>
    </div>
  </div>
</div>

<script>
function dashboard() {
  return {
    // state
    view: 'table',
    stats: {},
    leads: [],
    loading: false,
    search: '',
    statusFilter: '',
    sortBy: 'created_time',
    sortDir: 'desc',
    page: 1,
    perPage: 25,
    totalPages: 1,
    totalLeads: 0,
    lastRefresh: '',
    allStatuses: [],

    // kanban
    kanbanData: {},
    kanbanOrder: [],
    allKanbanStatuses: ['intake','emailed','contacted','qualified','closed','failed','skipped_invalid_email'],

    // modal
    modalOpen: false,
    modalLoading: false,
    detail: null,

    // add lead
    addLeadOpen: false,
    addLeadError: '',
    newLead: { first_name:'', last_name:'', email:'', phone_number:'', company_name:'', job_title:'', industry:'', lead_source_tag:'', custom_tags:'', deal_value:'', expected_close_date:'' },

    // notes
    detailNotes: [],
    newNoteContent: '',

    // reports
    reportFunnel: [],
    reportBySource: [],
    reportOverTime: [],
    reportPipelineValue: [],

    // campaigns
    campaignsList: [],
    campaignBuilderOpen: false,
    campaignDraft: { name: '', campaign_type: 'single', email_provider: 'outlook', subject_template: '', body_template: '', lead_filters: {} },
    campaignFilterCount: 0,
    campaignFilterSample: [],
    campaignError: '',
    campaignSuccess: '',
    campaignSending: false,
    campaignGenerating: false,
    campaignAiContext: '',
    emailProviders: [],

    // sequences
    sequencesList: [],
    seqModalOpen: false,
    seqEditing: { name: '', steps: [{ delay_days: 0, subject: '', body: '' }] },
    seqError: '',
    detailEnrollments: [],
    enrollSequenceId: '',

    // tags
    tagOptions: { industries: [], lead_sources: [] },
    editingTags: false,
    tagEditIndustry: '',
    tagEditSource: '',
    tagEditCustom: '',

    // AI email
    emailModalOpen: false,
    emailGenerating: false,
    emailSending: false,
    emailDraft: { subject: '', body: '' },
    emailError: '',
    emailSuccess: '',

    // users & assignment
    currentUser: {},
    usersList: [],
    assigneeFilter: '',
    usersModalOpen: false,
    managedUsers: [],
    newUser: { username: '', password: '', display_name: '', role: 'user' },
    userError: '',

    columns: [
      { key: 'full_name', label: 'Name' },
      { key: 'email', label: 'Email' },
      { key: 'phone_number', label: 'Phone' },
      { key: 'company_name', label: 'Company' },
      { key: 'status', label: 'Status' },
      { key: 'tags', label: 'Tags' },
      { key: 'assigned_to', label: 'Assigned' },
      { key: 'created_time', label: 'Date' },
    ],

    async init() {
      await this.fetchCurrentUser();
      await this.fetchUsersList();
      await this.fetchSequences();
      await this.fetchEmailProviders();
      await this.refresh();
    },

    async refresh() {
      await Promise.all([this.fetchStats(), this.fetchLeads()]);
      this.lastRefresh = new Date().toLocaleTimeString();
    },

    async fetchStats() {
      try {
        const r = await fetch('/api/stats');
        if (r.status === 401) { window.location = '/login'; return; }
        this.stats = await r.json();
        if (this.stats.breakdown) {
          this.allStatuses = Object.keys(this.stats.breakdown).sort();
        }
      } catch (e) { console.error('stats', e); }
    },

    async fetchLeads() {
      this.loading = true;
      try {
        const params = new URLSearchParams({
          page: this.page,
          per_page: this.perPage,
          sort_by: this.sortBy,
          sort_dir: this.sortDir,
        });
        if (this.statusFilter) params.set('status', this.statusFilter);
        if (this.search) params.set('search', this.search);
        if (this.assigneeFilter === '__mine__') params.set('assigned_to', this.currentUser.username);
        else if (this.assigneeFilter) params.set('assigned_to', this.assigneeFilter);

        const r = await fetch('/api/leads?' + params);
        if (r.status === 401) { window.location = '/login'; return; }
        const data = await r.json();
        this.leads = data.leads;
        this.totalLeads = data.total;
        this.totalPages = data.total_pages;
      } catch (e) { console.error('leads', e); }
      this.loading = false;
    },

    async fetchKanban() {
      try {
        const r = await fetch('/api/kanban');
        if (r.status === 401) { window.location = '/login'; return; }
        const data = await r.json();
        this.kanbanData = data.columns;
        this.kanbanOrder = data.column_order;
      } catch (e) { console.error('kanban', e); }
    },

    async fetchReports() {
      try {
        const [funnelR, sourceR, timeR, valueR] = await Promise.all([
          fetch('/api/reports/funnel'),
          fetch('/api/reports/by-source'),
          fetch('/api/reports/over-time'),
          fetch('/api/reports/pipeline-value'),
        ]);
        await this.fetchSequences();
        if (funnelR.ok) { const d = await funnelR.json(); this.reportFunnel = d.funnel || []; }
        if (sourceR.ok) { const d = await sourceR.json(); this.reportBySource = d.by_source || []; }
        if (timeR.ok) { const d = await timeR.json(); this.reportOverTime = d.over_time || []; }
        if (valueR.ok) { const d = await valueR.json(); this.reportPipelineValue = d.pipeline_value || []; }
      } catch(e) { console.error('reports', e); }
    },

    async moveLeadStatus(leadId, newStatus) {
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(leadId) + '/status', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ status: newStatus }),
        });
        if (r.status === 401) { window.location = '/login'; return; }
        if (r.ok) {
          // Refresh the current view
          if (this.view === 'kanban') await this.fetchKanban();
          else await this.fetchLeads();
          await this.fetchStats();
        } else {
          const err = await r.json();
          alert(err.error || 'Failed to update status');
        }
      } catch (e) { console.error('moveStatus', e); }
    },

    toggleSort(col) {
      if (this.sortBy === col) {
        this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        this.sortBy = col;
        this.sortDir = 'asc';
      }
      this.page = 1;
      this.fetchLeads();
    },

    async openDetail(leadId) {
      this.modalOpen = true;
      this.modalLoading = true;
      this.detail = null;
      this.detailNotes = [];
      this.newNoteContent = '';
      this.detailEnrollments = [];
      this.enrollSequenceId = '';
      try {
        const [detailR, notesR, enrollR] = await Promise.all([
          fetch('/api/lead/' + encodeURIComponent(leadId)),
          fetch('/api/lead/' + encodeURIComponent(leadId) + '/notes'),
          fetch('/api/lead/' + encodeURIComponent(leadId) + '/enrollments'),
        ]);
        if (detailR.status === 401) { window.location = '/login'; return; }
        this.detail = await detailR.json();
        if (notesR.ok) { const nd = await notesR.json(); this.detailNotes = nd.notes || []; }
        if (enrollR.ok) { const ed = await enrollR.json(); this.detailEnrollments = ed.enrollments || []; }
      } catch (e) { console.error('detail', e); }
      this.modalLoading = false;
    },

    detailPairs() {
      if (!this.detail) return [];
      const skip = new Set(['raw_field_data', 'tags']);
      return Object.entries(this.detail).filter(([k]) => !skip.has(k));
    },

    async fetchTagOptions() {
      try {
        const r = await fetch('/api/tags/options');
        if (r.ok) this.tagOptions = await r.json();
      } catch(e) { console.error('tagOptions', e); }
    },

    async submitNewLead() {
      this.addLeadError = '';
      try {
        const r = await fetch('/api/lead', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.newLead),
        });
        if (r.status === 401) { window.location = '/login'; return; }
        const data = await r.json();
        if (!r.ok) { this.addLeadError = data.error || 'Failed to create lead'; return; }
        this.addLeadOpen = false;
        this.newLead = { first_name:'', last_name:'', email:'', phone_number:'', company_name:'', job_title:'', industry:'', lead_source_tag:'', custom_tags:'', deal_value:'', expected_close_date:'' };
        await this.refresh();
        if (this.view === 'kanban') await this.fetchKanban();
      } catch(e) { this.addLeadError = 'Network error'; console.error('addLead', e); }
    },

    async saveTags() {
      if (!this.detail) return;
      const customArr = this.tagEditCustom.split(',').map(t => t.trim()).filter(Boolean);
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/tags', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ industry: this.tagEditIndustry, lead_source: this.tagEditSource, custom: customArr }),
        });
        if (r.ok) {
          const data = await r.json();
          this.detail.tags = data.tags;
          this.editingTags = false;
        }
      } catch(e) { console.error('saveTags', e); }
    },

    async generateEmail() {
      if (!this.detail) return;
      this.emailGenerating = true;
      this.emailError = '';
      this.emailSuccess = '';
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/generate-email', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        });
        const data = await r.json();
        if (!r.ok) { this.emailError = data.error || 'Generation failed'; this.emailGenerating = false; return; }
        this.emailDraft = { subject: data.subject || '', body: data.body || '' };
        this.emailModalOpen = true;
      } catch(e) { this.emailError = 'Network error'; console.error('generateEmail', e); }
      this.emailGenerating = false;
    },

    async sendEmail() {
      if (!this.detail) return;
      this.emailSending = true;
      this.emailError = '';
      this.emailSuccess = '';
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/send-email', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ subject: this.emailDraft.subject, body: this.emailDraft.body }),
        });
        const data = await r.json();
        if (!r.ok) { this.emailError = data.error || 'Send failed'; this.emailSending = false; return; }
        this.emailSuccess = data.message || 'Email sent!';
        this.detail.status = 'emailed';
        await this.refresh();
        if (this.view === 'kanban') await this.fetchKanban();
        setTimeout(() => { this.emailModalOpen = false; this.emailSuccess = ''; }, 2000);
      } catch(e) { this.emailError = 'Network error'; console.error('sendEmail', e); }
      this.emailSending = false;
    },

    async fetchCurrentUser() {
      try {
        const r = await fetch('/api/me');
        if (r.ok) this.currentUser = await r.json();
      } catch(e) { console.error('me', e); }
    },

    async fetchUsersList() {
      try {
        const r = await fetch('/api/users/list');
        if (r.ok) { const data = await r.json(); this.usersList = data.users; }
      } catch(e) { console.error('usersList', e); }
    },

    async fetchUsers() {
      try {
        const r = await fetch('/api/users');
        if (r.ok) { const data = await r.json(); this.managedUsers = data.users; }
      } catch(e) { console.error('users', e); }
    },

    async createUser() {
      this.userError = '';
      if (!this.newUser.username || !this.newUser.password) { this.userError = 'Username and password required'; return; }
      try {
        const r = await fetch('/api/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.newUser),
        });
        const data = await r.json();
        if (!r.ok) { this.userError = data.error || 'Failed'; return; }
        this.newUser = { username: '', password: '', display_name: '', role: 'user' };
        await this.fetchUsers();
        await this.fetchUsersList();
      } catch(e) { this.userError = 'Network error'; }
    },

    async deleteUser(username) {
      if (!confirm('Delete user ' + username + '?')) return;
      try {
        const r = await fetch('/api/users/' + encodeURIComponent(username), { method: 'DELETE' });
        if (r.ok) { await this.fetchUsers(); await this.fetchUsersList(); }
        else { const d = await r.json(); alert(d.error || 'Failed'); }
      } catch(e) { console.error('deleteUser', e); }
    },

    async resetUserPassword(username) {
      const pw = prompt('New password for ' + username + ':');
      if (!pw) return;
      try {
        const r = await fetch('/api/users/' + encodeURIComponent(username) + '/password', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ password: pw }),
        });
        const data = await r.json();
        if (r.ok) alert('Password updated');
        else alert(data.error || 'Failed');
      } catch(e) { console.error('resetPw', e); }
    },

    async assignLead(leadId, username) {
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(leadId) + '/assign', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ assigned_to: username }),
        });
        if (r.status === 401) { window.location = '/login'; return; }
        if (r.ok) {
          await this.refresh();
          if (this.view === 'kanban') await this.fetchKanban();
        }
      } catch(e) { console.error('assign', e); }
    },

    async addNote() {
      if (!this.detail || !this.newNoteContent.trim()) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/notes', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ content: this.newNoteContent.trim() }),
        });
        if (r.ok) {
          const note = await r.json();
          this.detailNotes.unshift(note);
          this.newNoteContent = '';
        }
      } catch(e) { console.error('addNote', e); }
    },

    async deleteNote(noteId) {
      if (!this.detail) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/notes/' + noteId, { method: 'DELETE' });
        if (r.ok) { this.detailNotes = this.detailNotes.filter(n => n.id !== noteId); }
      } catch(e) { console.error('deleteNote', e); }
    },

    async setFollowUp(date) {
      if (!this.detail) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/follow-up', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ follow_up_date: date || null }),
        });
        if (r.ok) {
          this.detail.follow_up_date = date || null;
          await this.fetchStats();
        }
      } catch(e) { console.error('setFollowUp', e); }
    },

    async updateDeal(field, value) {
      if (!this.detail) return;
      const payload = {
        deal_value: this.detail.deal_value,
        expected_close_date: this.detail.expected_close_date,
      };
      payload[field] = value || null;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/deal', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (r.ok) {
          const data = await r.json();
          this.detail.deal_value = data.deal_value;
          this.detail.expected_close_date = data.expected_close_date;
          await this.fetchStats();
        }
      } catch(e) { console.error('updateDeal', e); }
    },

    async fetchSequences() {
      try {
        const r = await fetch('/api/sequences');
        if (r.ok) { const d = await r.json(); this.sequencesList = d.sequences || []; }
      } catch(e) { console.error('sequences', e); }
    },

    async fetchEmailProviders() {
      try {
        const r = await fetch('/api/email-providers');
        if (r.ok) { const d = await r.json(); this.emailProviders = d.providers || []; }
      } catch(e) { console.error('providers', e); }
    },

    async fetchCampaigns() {
      try {
        const r = await fetch('/api/campaigns');
        if (r.ok) { const d = await r.json(); this.campaignsList = d.campaigns || []; }
      } catch(e) { console.error('campaigns', e); }
    },

    openCampaignBuilder(existing) {
      if (existing) {
        this.campaignDraft = JSON.parse(JSON.stringify(existing));
        if (typeof this.campaignDraft.lead_filters === 'string') {
          try { this.campaignDraft.lead_filters = JSON.parse(this.campaignDraft.lead_filters); } catch { this.campaignDraft.lead_filters = {}; }
        }
      } else {
        this.campaignDraft = { name: '', campaign_type: 'single', email_provider: 'outlook', subject_template: '', body_template: '', lead_filters: {} };
      }
      this.campaignError = '';
      this.campaignSuccess = '';
      this.campaignAiContext = '';
      this.campaignFilterCount = 0;
      this.campaignFilterSample = [];
      this.campaignBuilderOpen = true;
      this.previewCampaignFilter();
    },

    async previewCampaignFilter() {
      try {
        const r = await fetch('/api/campaigns/filter-preview', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.campaignDraft.lead_filters),
        });
        if (r.ok) {
          const d = await r.json();
          this.campaignFilterCount = d.count;
          this.campaignFilterSample = d.sample || [];
        }
      } catch(e) { console.error('filterPreview', e); }
    },

    async saveCampaignDraft() {
      this.campaignError = '';
      try {
        const url = this.campaignDraft.id ? '/api/campaigns/' + this.campaignDraft.id : '/api/campaigns';
        const method = this.campaignDraft.id ? 'PUT' : 'POST';
        const r = await fetch(url, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.campaignDraft),
        });
        const d = await r.json();
        if (!r.ok) { this.campaignError = d.error || 'Failed'; return; }
        if (!this.campaignDraft.id && d.id) this.campaignDraft.id = d.id;
        this.campaignSuccess = 'Draft saved';
        await this.fetchCampaigns();
        setTimeout(() => this.campaignSuccess = '', 2000);
      } catch(e) { this.campaignError = 'Network error'; }
    },

    async generateCampaignCopy() {
      this.campaignGenerating = true;
      this.campaignError = '';
      try {
        // Save draft first if needed
        if (!this.campaignDraft.id) {
          if (!this.campaignDraft.name) { this.campaignError = 'Name required first'; this.campaignGenerating = false; return; }
          await this.saveCampaignDraft();
        }
        const r = await fetch('/api/campaigns/' + this.campaignDraft.id + '/generate-copy', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ context: this.campaignAiContext }),
        });
        const d = await r.json();
        if (!r.ok) { this.campaignError = d.error || 'Generation failed'; this.campaignGenerating = false; return; }
        this.campaignDraft.subject_template = d.subject || this.campaignDraft.subject_template;
        this.campaignDraft.body_template = d.body || this.campaignDraft.body_template;
      } catch(e) { this.campaignError = 'Network error'; }
      this.campaignGenerating = false;
    },

    async sendCampaign() {
      if (!confirm('Send this campaign to ' + this.campaignFilterCount + ' leads? This cannot be undone.')) return;
      this.campaignSending = true;
      this.campaignError = '';
      this.campaignSuccess = '';
      try {
        // Save first
        await this.saveCampaignDraft();
        if (!this.campaignDraft.id) { this.campaignError = 'Save failed'; this.campaignSending = false; return; }

        const r = await fetch('/api/campaigns/' + this.campaignDraft.id + '/send', { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { this.campaignError = d.error || 'Send failed'; this.campaignSending = false; return; }
        this.campaignSuccess = `Sent ${d.sent} emails` + (d.failed > 0 ? `, ${d.failed} failed` : '');
        await this.fetchCampaigns();
        setTimeout(() => { this.campaignBuilderOpen = false; this.campaignSuccess = ''; }, 3000);
      } catch(e) { this.campaignError = 'Network error'; }
      this.campaignSending = false;
    },

    async deleteCampaign(cid) {
      if (!confirm('Delete this campaign?')) return;
      try {
        const r = await fetch('/api/campaigns/' + cid, { method: 'DELETE' });
        if (r.ok) await this.fetchCampaigns();
      } catch(e) { console.error('deleteCampaign', e); }
    },

    async saveSequence() {
      this.seqError = '';
      const editing = this.seqEditing;
      if (!editing.name || !editing.steps.length) { this.seqError = 'Name and at least one step required'; return; }
      try {
        const url = editing.id ? '/api/sequences/' + editing.id : '/api/sequences';
        const method = editing.id ? 'PUT' : 'POST';
        const r = await fetch(url, {
          method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: editing.name, steps: editing.steps }),
        });
        const data = await r.json();
        if (!r.ok) { this.seqError = data.error || 'Failed'; return; }
        this.seqModalOpen = false;
        await this.fetchSequences();
      } catch(e) { this.seqError = 'Network error'; }
    },

    async deleteSequence(seqId) {
      if (!confirm('Delete this sequence? All enrollments will be removed.')) return;
      try {
        const r = await fetch('/api/sequences/' + seqId, { method: 'DELETE' });
        if (r.ok) await this.fetchSequences();
      } catch(e) { console.error('deleteSeq', e); }
    },

    async enrollLead() {
      if (!this.detail || !this.enrollSequenceId) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/enroll', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sequence_id: parseInt(this.enrollSequenceId) }),
        });
        if (r.ok) {
          this.enrollSequenceId = '';
          const er = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/enrollments');
          if (er.ok) { const ed = await er.json(); this.detailEnrollments = ed.enrollments || []; }
        } else {
          const d = await r.json();
          alert(d.error || 'Enrollment failed');
        }
      } catch(e) { console.error('enroll', e); }
    },

    async unenrollLead(seqId) {
      if (!this.detail) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/unenroll', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sequence_id: seqId }),
        });
        if (r.ok) {
          const er = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id) + '/enrollments');
          if (er.ok) { const ed = await er.json(); this.detailEnrollments = ed.enrollments || []; }
        }
      } catch(e) { console.error('unenroll', e); }
    },

    async deleteLead() {
      if (!this.detail) return;
      if (!confirm('Are you sure you want to delete this lead?')) return;
      try {
        const r = await fetch('/api/lead/' + encodeURIComponent(this.detail.lead_id), { method: 'DELETE' });
        if (r.status === 401) { window.location = '/login'; return; }
        if (r.ok) {
          this.modalOpen = false;
          await this.refresh();
          if (this.view === 'kanban') await this.fetchKanban();
        } else {
          const err = await r.json();
          alert(err.error || 'Failed to delete lead');
        }
      } catch (e) { console.error('deleteLead', e); }
    },

    kanbanMoveTargets(currentCol) {
      return this.allKanbanStatuses.filter(s => s !== currentCol);
    },

    badgeClass(status) {
      const map = {
        'new':        'bg-blue-900/50 text-blue-300',
        'intake':     'bg-amber-900/50 text-amber-300',
        'emailed':    'bg-green-900/50 text-green-300',
        'contacted':  'bg-blue-900/50 text-blue-300',
        'qualified':  'bg-purple-900/50 text-purple-300',
        'closed':     'bg-gray-700 text-gray-200',
        'processed':  'bg-green-900/50 text-green-300',
        'email_sent': 'bg-green-900/50 text-green-300',
        'failed':     'bg-red-900/50 text-red-300',
        'error':      'bg-red-900/50 text-red-300',
        'skipped_invalid_email': 'bg-gray-800 text-gray-400',
      };
      return map[status] || 'bg-gray-800 text-gray-400';
    },

    kanbanHeaderClass(col) {
      const map = {
        'intake':    'text-amber-400',
        'emailed':   'text-green-400',
        'contacted': 'text-blue-400',
        'qualified': 'text-purple-400',
        'closed':    'text-gray-400',
        'other':     'text-gray-500',
      };
      return map[col] || 'text-gray-500';
    },

    moveBtnClass(target) {
      const map = {
        'intake':    'border-amber-700 text-amber-400',
        'emailed':   'border-green-700 text-green-400',
        'contacted': 'border-blue-700 text-blue-400',
        'qualified': 'border-purple-700 text-purple-400',
        'closed':    'border-gray-600 text-gray-400',
        'failed':    'border-red-700 text-red-400',
        'skipped_invalid_email': 'border-gray-700 text-gray-500',
      };
      return map[target] || 'border-gray-700 text-gray-500';
    },

    fmtDate(d) {
      if (!d) return '-';
      try {
        return new Date(d).toLocaleDateString('en-US', {
          month: 'short', day: 'numeric', year: 'numeric',
          hour: '2-digit', minute: '2-digit'
        });
      } catch { return d; }
    },

    fmtCurrency(val) {
      if (val == null || val === '' || val === 0) return '$0';
      return '$' + Number(val).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    },
  };
}
</script>
</body>
</html>"""


@app.route('/')
@login_required
def index():
    return Response(DASHBOARD_HTML, mimetype='text/html')


if __name__ == '__main__':
    print(f"Starting Arkitekt OpenCRM Dashboard on http://localhost:{DASHBOARD_PORT}")
    print(f"Database: {DB_PATH}")
    app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=True)
