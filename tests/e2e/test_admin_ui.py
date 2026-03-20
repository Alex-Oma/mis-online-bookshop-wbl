"""
E2E tests using Playwright — browser-based smoke tests of the admin UI.
Run: pytest tests/e2e/ --headed  (or headless by default)

Requires the MIS backend to be running at BASE_URL.
"""
import os
import pytest
from playwright.sync_api import Page, expect
from app.config import get_settings


_settings = get_settings()

BASE_URL = _settings.mis_base_url
ADMIN_USER = _settings.mis_admin_user
ADMIN_PASS = _settings.mis_admin_password


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {**browser_context_args, "ignore_https_errors": True}


# ── Helper ────────────────────────────────────────────────────────────────────

def login(page: Page) -> None:
    """Navigate to login page and authenticate."""
    page.goto(f"{BASE_URL}/admin/login")
    page.fill("#username", ADMIN_USER)
    page.fill("#password", ADMIN_PASS)
    page.click("button[type='submit']")
    page.wait_for_url(f"{BASE_URL}/admin/dashboard", timeout=10_000)


# ── Login flow ────────────────────────────────────────────────────────────────

def test_login_page_loads(page: Page):
    '''Verify that the login page loads and displays the expected elements.'''
    page.goto(f"{BASE_URL}/admin/login")
    expect(page).to_have_title("Sign In — MyEnglishBooks MIS")
    expect(page.locator("#username")).to_be_visible()
    expect(page.locator("#password")).to_be_visible()


def test_login_invalid_credentials_shows_error(page: Page):
    '''Verify that invalid login credentials show an error message.'''
    page.goto(f"{BASE_URL}/admin/login")
    page.fill("#username", "baduser")
    page.fill("#password", "badpass")
    page.click("button[type='submit']")
    error_div = page.locator("#error-msg")
    expect(error_div).to_be_visible(timeout=5_000)
    expect(error_div).not_to_be_empty()


def test_login_success_redirects_to_dashboard(page: Page):
    '''Verify that valid login credentials redirect to the dashboard.'''
    login(page)
    expect(page).to_have_url(f"{BASE_URL}/admin/dashboard")


# ── Dashboard ─────────────────────────────────────────────────────────────────

def test_dashboard_kpi_cards_visible(page: Page):
    '''Verify that the KPI cards are visible on the dashboard.'''
    login(page)
    # Four KPI cards should be present
    cards = page.locator(".bg-white.rounded-xl")
    expect(cards.first).to_be_visible()


def test_dashboard_charts_rendered(page: Page):
    '''Verify that the charts on the dashboard are rendered (Chart.js uses <canvas> elements).'''
    login(page)
    # Chart.js renders into <canvas> elements
    canvases = page.locator("canvas")
    assert canvases.count() >= 3, "Expected at least 3 Chart.js canvases on dashboard"


def test_dashboard_sidebar_navigation_visible(page: Page):
    '''Verify that the sidebar navigation links are visible on the dashboard.'''
    login(page)
    expect(page.locator("text=📊 Dashboard")).to_be_visible()
    expect(page.locator("text=📄 Reports")).to_be_visible()
    expect(page.locator("text=🔔 Alerts")).to_be_visible()
    expect(page.locator("text=🔍 Audit Log")).to_be_visible()


# ── Reports page ──────────────────────────────────────────────────────────────

def test_reports_page_loads(page: Page):
    '''Verify that the reports page loads and displays the expected elements.'''
    login(page)
    page.click("text=Reports")
    expect(page).to_have_url(f"{BASE_URL}/admin/reports")
    expect(page.locator("text=Generate New Report")).to_be_visible()
    expect(page.locator("text=Recent Reports")).to_be_visible()


def test_reports_page_has_format_selector(page: Page):
    '''Verify that the report format selector is present on the reports page.'''
    login(page)
    page.goto(f"{BASE_URL}/admin/reports")
    expect(page.locator("select[name='format']")).to_be_visible()


# ── Alerts page ───────────────────────────────────────────────────────────────

def test_alerts_page_loads(page: Page):
    '''Verify that the alerts page loads and displays the expected elements.'''
    login(page)
    page.click("text=Alerts")
    expect(page).to_have_url(f"{BASE_URL}/admin/alerts")
    expect(page.locator("h3", has_text="Create Alert Rule")).to_be_visible()
    expect(page.locator("h3", has_text="Active Rules")).to_be_visible()
    expect(page.locator("h2", has_text="🔔 Alert Rules")).to_be_visible()


def test_create_low_stock_alert_rule(page: Page):
    '''Verify that a new low stock alert rule can be created successfully.'''
    login(page)
    page.goto(f"{BASE_URL}/admin/alerts")
    page.select_option("select[name='rule_type']", "low_stock")
    page.fill("input[name='rule_name']", "E2E Test Low Stock Alert")
    page.fill("input[name='threshold']", "3")
    page.click("button[type='submit']")
    # Success status message should appear
    status_div = page.locator("#alert-status")
    expect(status_div).to_contain_text("created", timeout=5_000)


# ── Audit log page ────────────────────────────────────────────────────────────

def test_audit_log_page_loads(page: Page):
    '''Verify that the audit log page loads and displays the expected table structure.'''
    login(page)
    page.click("text=Audit Log")
    expect(page).to_have_url(f"{BASE_URL}/admin/audit")
    # Target the heading specifically to avoid ambiguity with the sidebar link
    expect(page.locator("h2", has_text="🔍 Audit Log")).to_be_visible()
    # Table header should be visible
    expect(page.locator("th", has_text="Time")).to_be_visible()
    expect(page.locator("th", has_text="User")).to_be_visible()
    expect(page.locator("th", has_text="Action")).to_be_visible()
    expect(page.locator("th", has_text="Resource")).to_be_visible()
    expect(page.locator("th", has_text="IP Address")).to_be_visible()


# ── Logout ────────────────────────────────────────────────────────────────────

def test_logout_redirects_to_login(page: Page):
    '''Verify that logging out redirects back to the login page.'''
    login(page)
    page.goto(f"{BASE_URL}/admin/logout")
    expect(page).to_have_url(f"{BASE_URL}/admin/login")

