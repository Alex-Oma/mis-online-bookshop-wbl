"""
Pydantic models for MIS application entities:
users, roles, reports, alerts, API request/response schemas.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    '''Request body for /auth/login endpoint.'''
    username: str
    password: str


class TokenResponse(BaseModel):
    '''Response body for /auth/login endpoint on successful authentication.'''
    access_token: str
    token_type: str = "bearer"
    role: str


# ── Users ─────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    '''Request body for creating a new user.'''
    username: str = Field(min_length=3, max_length=64)
    email: str
    password: str = Field(min_length=8)
    full_name: Optional[str] = None
    role: str = "viewer"


class UserRead(BaseModel):
    '''Response model for user information.'''
    user_id: int
    username: str
    email: str
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]


# ── Ingestion ─────────────────────────────────────────────────────────────────

class IngestionRequest(BaseModel):
    '''Request body for /ingest endpoint to trigger data ingestion.'''
    from_date: Optional[str] = None    # YYYY-MM-DD; None = use last sync timestamp
    to_date: Optional[str] = None      # YYYY-MM-DD; None = now
    channels: list[str] = ["website", "rozetka"]


class IngestionStatus(BaseModel):
    '''Response model for /ingest/status endpoint to report ingestion job status.'''
    job_id: str
    status: str                        # running | success | failed
    channel: Optional[str] = None
    records_ingested: int = 0
    records_failed: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_detail: Optional[str] = None


# ── Reports ───────────────────────────────────────────────────────────────────

class ReportRequest(BaseModel):
    '''Request body for /reports endpoint to generate a new report.'''
    report_type: str                   # weekly_sales | monthly_revenue | top_books | inventory
    from_date: str                     # YYYY-MM-DD
    to_date: str                       # YYYY-MM-DD
    format: str = "pdf"                # pdf | xlsx
    channel_id: Optional[int] = None  # None = all channels
    category_id: Optional[int] = None # None = all categories


class ReportResponse(BaseModel):
    '''Response model for /reports endpoint after report generation.'''
    report_id: int
    download_url: str
    generated_at: datetime
    report_type: str
    format: str


# ── Alerts ────────────────────────────────────────────────────────────────────

class AlertRuleCreate(BaseModel):
    '''Request body for creating a new alert rule.'''
    rule_type: str                     # low_stock | revenue_spike | no_orders
    rule_name: str
    threshold: Decimal
    channel_id: Optional[int] = None
    category_id: Optional[int] = None
    cooldown_hours: int = 24
    notify_email: Optional[str] = None


class AlertRuleRead(BaseModel):
    '''Response model for alert rule information.'''
    rule_id: int
    rule_type: str
    rule_name: str
    threshold: Decimal
    channel_id: Optional[int]
    category_id: Optional[int]
    is_active: bool
    cooldown_hours: int
    notify_email: Optional[str]
    created_at: datetime


class AlertEventRead(BaseModel):
    '''Response model for alert event information.'''
    event_id: int
    rule_id: int
    rule_name: str
    product_id: Optional[int]
    product_title: Optional[str]
    triggered_at: datetime
    email_sent: bool


# ── Dashboard KPIs ────────────────────────────────────────────────────────────

class KpiSummary(BaseModel):
    '''Summary of key performance indicators for the dashboard.'''
    total_revenue_mtd: Decimal         # Month-to-date revenue in UAH
    orders_mtd: int                    # Order count month-to-date
    top_book_title: Optional[str]      # Best-selling book this month
    top_book_units: Optional[int]
    low_stock_count: int               # Products below alert threshold
    last_sync_website: Optional[datetime]
    last_sync_rozetka: Optional[datetime]


class RevenueByChannelRow(BaseModel):
    '''Row model for revenue by channel report.'''
    year: int
    month: int
    month_name: str
    channel_name: str
    total_revenue_uah: Decimal
    order_count: int


class TopBookRow(BaseModel):
    '''Row model for top-selling books report.'''
    product_id: int
    title: str
    author: Optional[str]
    units_sold: int
    revenue_uah: Decimal


class SalesByAgeGroupRow(BaseModel):
    '''Row model for sales by customer age group report.'''
    category_name: str
    parent_name: Optional[str]
    units_sold: int
    revenue_uah: Decimal
    pct_of_total: float

