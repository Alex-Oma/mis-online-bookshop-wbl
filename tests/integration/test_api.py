"""
Integration tests — FastAPI endpoints using the synchronous TestClient.
External services (DB, Rozetka API) are mocked so these run without
any real database or network connection.
"""
import os
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from app.config import get_settings

_settings = get_settings()

from app.main import create_app  # noqa: E402
from app.auth.jwt import create_access_token  # noqa: E402


@pytest.fixture(scope="module")
def client():
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def _cookie(role: str = "admin") -> dict:
    token = create_access_token(user_id=1, username="testuser", role=role)
    return {"access_token": token}


# ── Health ────────────────────────────────────────────────────────────────────

def test_health_check(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── Auth ──────────────────────────────────────────────────────────────────────

def test_login_invalid_credentials_returns_401(client):
    with patch("app.routers.auth.get_db_connection") as mock_dep:
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        mock_dep.return_value = mock_conn
        resp = client.post("/auth/login", json={"username": "x", "password": "y"})
    assert resp.status_code == 401


def test_logout_succeeds(client):
    resp = client.post("/auth/logout")
    assert resp.status_code == 200


# ── RBAC ──────────────────────────────────────────────────────────────────────

def test_viewer_cannot_trigger_ingest(client):
    resp = client.post(
        "/ingest/run",
        json={"channels": ["website"]},
        cookies=_cookie("viewer"),
    )
    assert resp.status_code == 403


def test_admin_can_trigger_ingest(client):
    with patch("app.routers.ingest._run_ingestion", new_callable=AsyncMock):
        resp = client.post(
            "/ingest/run",
            json={"channels": ["website"]},
            cookies=_cookie("admin"),
        )
    assert resp.status_code == 202
    assert "job_id" in resp.json()


def test_viewer_cannot_create_alert_rule(client):
    resp = client.post(
        "/alerts/rules",
        json={"rule_type": "low_stock", "rule_name": "Test", "threshold": 5, "cooldown_hours": 24},
        cookies=_cookie("viewer"),
    )
    assert resp.status_code == 403


# ── Reports ───────────────────────────────────────────────────────────────────

def test_unauthenticated_report_request_returns_401(client):
    resp = client.post("/reports/generate", json={
        "report_type": "top_books",
        "from_date": "2026-01-01",
        "to_date": "2026-03-08",
        "format": "pdf",
    })
    assert resp.status_code == 401


def test_unknown_report_type_returns_400(client):
    with patch(
        "app.reports.generator.ReportGenerator.generate",
        new_callable=AsyncMock,
        side_effect=ValueError("Unknown report type: garbage"),
    ):
        resp = client.post(
            "/reports/generate",
            json={
                "report_type": "garbage",
                "from_date": "2026-01-01",
                "to_date": "2026-03-08",
                "format": "pdf",
            },
            cookies=_cookie("admin"),
        )
    assert resp.status_code == 400


def test_report_download_not_found(client):
    with patch("app.database.get_pool") as mock_pool:
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        mock_pool.return_value.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.return_value.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        resp = client.get("/reports/download/99999999", cookies=_cookie("admin"))
    assert resp.status_code == 404

