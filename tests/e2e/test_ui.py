"""
E2E smoke tests using Playwright.

Requires the app to be running locally (or in CI via docker-compose).
Set BASE_URL env var to override the default http://localhost:8000.

Run: playwright install  (once)
     pytest tests/e2e/ --headed  (to watch in browser)
"""
import os
import pytest
from playwright.sync_api import Page, expect
from app.config import get_settings

_settings = get_settings()

BASE_URL = _settings.mis_base_url
TEST_USERNAME = _settings.mis_admin_user
TEST_PASSWORD = _settings.mis_admin_password


# ── Login flow ────────────────────────────────────────────────────────────────

def test_login_page_loads(page: Page):
    page.goto(f"{BASE_URL}/admin/login")
    expect(page).to_have_title("Sign In — MyEnglishBooks MIS")
    expect(page.locator("input#username")).to_be_visible()
    expect(page.locator("input#password")).to_be_visible()


def test_login_with_valid_credentials(page: Page):
    page.goto(f"{BASE_URL}/admin/login")
    page.fill("#username", TEST_USERNAME)
    page.fill("#password", TEST_PASSWORD)
    page.click("button[type=submit]")
    page.wait_for_url(f"{BASE_URL}/admin/dashboard", timeout=5000)
    expect(page).to_have_url(f"{BASE_URL}/admin/dashboard")


def test_login_with_invalid_credentials_shows_error(page: Page):
    page.goto(f"{BASE_URL}/admin/login")
    page.fill("#username", "nobody")
    page.fill("#password", "wrongpassword")
    page.click("button[type=submit]")
    # Error message div should become visible
    error_div = page.locator("#error-msg")
    expect(error_div).to_be_visible(timeout=3000)
    expect(error_div).not_to_be_empty()


# ── Dashboard ─────────────────────────────────────────────────────────────────

@pytest.fixture
def authenticated_page(page: Page):
    """Return a page already logged in as admin."""
    page.goto(f"{BASE_URL}/admin/login")
    page.fill("#username", TEST_USERNAME)
    page.fill("#password", TEST_PASSWORD)
    page.click("button[type=submit]")
    page.wait_for_url(f"{BASE_URL}/admin/dashboard", timeout=5000)
    return page


def test_dashboard_kpi_cards_visible(authenticated_page: Page):
    page = authenticated_page
    # Four KPI cards should be present
    expect(page.locator(".rounded-xl").first).to_be_visible()
    # Chart canvases rendered
    expect(page.locator("canvas#revenueChart")).to_be_visible()
    expect(page.locator("canvas#topBooksChart")).to_be_visible()
    expect(page.locator("canvas#categoryChart")).to_be_visible()


def test_navigation_links_present(authenticated_page: Page):
    page = authenticated_page
    # Use specific class to differentiate from dashboard card links which might share the same href
    expect(page.locator("a.sidebar-link[href='/admin/reports']")).to_be_visible()
    expect(page.locator("a.sidebar-link[href='/admin/alerts']")).to_be_visible()
    expect(page.locator("a.sidebar-link[href='/admin/audit']")).to_be_visible()


# ── Reports page ──────────────────────────────────────────────────────────────

def test_reports_page_loads(authenticated_page: Page):
    page = authenticated_page
    page.goto(f"{BASE_URL}/admin/reports")
    expect(page.locator("select#report_type")).to_be_visible()
    expect(page.locator("input#from_date")).to_be_visible()
    expect(page.locator("input#to_date")).to_be_visible()


# ── Alerts page ───────────────────────────────────────────────────────────────

def test_alerts_page_loads(authenticated_page: Page):
    page = authenticated_page
    page.goto(f"{BASE_URL}/admin/alerts")
    expect(page.locator("select#rule_type")).to_be_visible()


# ── Logout ────────────────────────────────────────────────────────────────────

def test_logout_redirects_to_login(authenticated_page: Page):
    page = authenticated_page
    page.goto(f"{BASE_URL}/admin/logout")
    page.wait_for_url(f"{BASE_URL}/admin/login", timeout=5000)
    expect(page).to_have_url(f"{BASE_URL}/admin/login")

