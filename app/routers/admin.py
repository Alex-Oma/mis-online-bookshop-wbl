"""
Admin UI router — serves Jinja2 HTML pages for the browser-based
admin interface. All routes require a valid JWT cookie.
"""
from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.dependencies import get_current_user, require_role
from app.database import get_pool
from app.reports.queries import (
    KPI_SUMMARY,
    REVENUE_BY_CHANNEL,
    TOP_BOOKS,
    SALES_BY_CATEGORY,
)

# Here we use a separate router for the admin UI pages, which are server-rendered with Jinja2 templates.
# This keeps the UI routes separate from the JSON API routes.
router = APIRouter()
# The Jinja2 templates are stored in the "app/templates" directory. Each route handler will render a specific template and pass data to it.
templates = Jinja2Templates(directory="app/templates")


def _sanitise(row: dict) -> dict:
    """Convert asyncpg row values to JSON-serialisable Python types."""
    out = {}
    # asyncpg returns Decimal for numeric types, and datetime/date for date types, which are not directly JSON-serialisable.
    for k, v in row.items():
        # Convert Decimal to float, and datetime/date to ISO format strings. Other types are left as-is.
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    # return the sanitised dictionary, which can be safely passed to Jinja2 templates or JSON responses.
    return out


# ── Login / logout UI pages ───────────────────────────────────────────────────

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    '''Render the login page.'''
    return templates.TemplateResponse("login.html", {"request": request})


@router.get("/logout")
async def logout_ui():
    '''Logout the user by clearing the JWT cookie and redirecting to the login page.'''
    response = RedirectResponse(url="/admin/login")
    response.delete_cookie("access_token")
    return response


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    current_user=Depends(get_current_user),
):
    '''Render the dashboard page with KPIs and charts.'''
    # Get a connection from the database pool and execute the necessary queries to fetch data for the dashboard.
    pool = await get_pool()
    # The dashboard shows a KPI summary, revenue by channel, top 10 books, and sales by category.
    async with pool.acquire() as conn:
        # Fetch the KPI summary, which is a single row with aggregated metrics.
        kpi = await conn.fetchrow(KPI_SUMMARY)

        # date objects required — asyncpg cannot bind bare strings to date columns
        date_from = date(2015, 1, 1)   # covers full history since business started
        date_to   = date(2099, 12, 31) # open-ended upper bound

        # Revenue by channel — all history
        rev_rows = await conn.fetch(
            REVENUE_BY_CHANNEL.replace(":from_date", "$1").replace(":to_date", "$2"),
            date_from, date_to,
        )

        # Top 10 books — current year
        year_from = date(date.today().year, 1, 1)
        top_rows = await conn.fetch(
            TOP_BOOKS
            .replace(":from_date",  "$1")
            .replace(":to_date",    "$2")
            .replace(":channel_id", "$3")
            .replace(":limit",      "$4"),
            year_from, date_to, None, 10,
        )

        # Sales by category — current year
        cat_rows = await conn.fetch(
            SALES_BY_CATEGORY
            .replace(":from_date",  "$1")
            .replace(":to_date",    "$2")
            .replace(":channel_id", "$3"),
            year_from, date_to, None,
        )

    # Render the "dashboard.html" template, passing the current user and the fetched data.
    # The _sanitise function is used to convert asyncpg row objects into regular dictionaries with JSON-serialisable values.
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": current_user,
            "kpi": _sanitise(dict(kpi)) if kpi else {},
            "revenue_by_channel": [_sanitise(dict(r)) for r in rev_rows],
            "top_books": [_sanitise(dict(r)) for r in top_rows],
            "sales_by_category": [_sanitise(dict(r)) for r in cat_rows],
        },
    )


# ── Reports page ──────────────────────────────────────────────────────────────

@router.get(
    "/reports",
    response_class=HTMLResponse,
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def reports_page(request: Request, current_user=Depends(get_current_user)):
    '''Render the reports page, which allows users to generate and view scheduled reports.'''
    # Get a connection from the database pool and fetch the recent scheduled reports and available sales channels to populate the page.
    pool = await get_pool()
    # The reports page shows a list of recently generated reports and a form to create new reports.
    # It also needs the list of sales channels for the report generation form.
    async with pool.acquire() as conn:
        # Fetch the 20 most recent scheduled reports, ordered by generation time. This will be displayed in a table on the page.
        recent_reports = await conn.fetch(
            """
            SELECT report_id, report_type, format, generated_at
            FROM mis.scheduled_report
            ORDER BY generated_at DESC LIMIT 20
            """
        )
        # Fetch the list of sales channels from the dimension table, ordered by channel_id.
        # This will be used to populate a dropdown in the report generation form.
        channels = await conn.fetch(
            "SELECT channel_id, channel_name FROM core.dim_sales_channel ORDER BY channel_id"
        )
    # Render the "reports.html" template, passing the current user, recent reports, and channels.
    # The recent reports and channels are converted to lists of dictionaries for easier use in the template.
    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "user": current_user,
            "recent_reports": [dict(r) for r in recent_reports],
            "channels": [dict(c) for c in channels],
        },
    )


# ── Alerts page ───────────────────────────────────────────────────────────────

@router.get(
    "/alerts",
    response_class=HTMLResponse,
    dependencies=[Depends(require_role("admin"))],
)
async def alerts_page(request: Request, current_user=Depends(get_current_user)):
    '''Render the alerts page, which allows users to manage alert rules and view recent alert events.'''
    # Get a connection from the database pool and fetch the existing alert rules and recent alert events to populate the page.
    pool = await get_pool()
    # The alerts page shows a list of existing alert rules and a form to create new rules. It also shows a table of recent alert events.
    async with pool.acquire() as conn:
        # Fetch all alert rules, ordered by creation time. This will be displayed in a table on the page.
        rules = await conn.fetch(
            "SELECT * FROM mis.alert_rule ORDER BY created_at DESC"
        )
        # Fetch the 50 most recent alert events, joined with their corresponding rules and product titles. This will be displayed in a table on the page.
        events = await conn.fetch(
            """
            SELECT e.event_id, r.rule_name, p.title AS product_title,
                   e.triggered_at, e.email_sent
            FROM mis.alert_event e
            JOIN mis.alert_rule r ON r.rule_id = e.rule_id
            LEFT JOIN core.dim_product p ON p.product_id = e.product_id
            ORDER BY e.triggered_at DESC LIMIT 50
            """
        )
    # Render the "alerts.html" template, passing the current user, alert rules, and alert events.
    return templates.TemplateResponse(
        "alerts.html",
        {
            "request": request,
            "user": current_user,
            "rules": [dict(r) for r in rules],
            "events": [dict(e) for e in events],
        },
    )


# ── Audit log page ────────────────────────────────────────────────────────────

@router.get(
    "/audit",
    response_class=HTMLResponse,
    dependencies=[Depends(require_role("admin"))],
)
async def audit_log_page(request: Request, current_user=Depends(get_current_user)):
    '''Render the audit log page, which shows a list of recent actions performed by users in the system.'''
    # Get a connection from the database pool and fetch the recent audit log entries, joined with user information to display usernames instead of user IDs.
    pool = await get_pool()
    # The audit log page shows a table of recent actions performed by users, including the action type, resource affected, IP address, and timestamp.
    async with pool.acquire() as conn:
        logs = await conn.fetch(
            """
            SELECT l.log_id, u.username, l.action, l.resource,
                   l.ip_address, l.created_at
            FROM mis.audit_log l
            LEFT JOIN mis.mis_user u ON u.user_id = l.user_id
            ORDER BY l.created_at DESC LIMIT 200
            """
        )
    # Render the "audit.html" template, passing the current user and the fetched audit log entries.
    # The log entries are converted to a list of dictionaries for easier use in the template.
    return templates.TemplateResponse(
        "audit.html",
        {
            "request": request,
            "user": current_user,
            "logs": [dict(l) for l in logs],
        },
    )

