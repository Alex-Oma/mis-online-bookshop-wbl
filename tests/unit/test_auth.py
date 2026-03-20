"""
Unit tests for app/auth/jwt.py and app/auth/password.py
"""
from datetime import timedelta

import pytest
from freezegun import freeze_time
from jose import JWTError

from app.auth.jwt import create_access_token, decode_token, extract_role
from app.auth.password import hash_password, verify_password


# ── Password tests ────────────────────────────────────────────────────────────

class TestPassword:
    def test_hash_is_not_plain(self):
        hashed = hash_password("secret123")
        assert hashed != "secret123"

    def test_verify_correct_password(self):
        hashed = hash_password("mypassword")
        assert verify_password("mypassword", hashed) is True

    def test_verify_wrong_password(self):
        hashed = hash_password("mypassword")
        assert verify_password("wrongpassword", hashed) is False

    def test_two_hashes_differ(self):
        # bcrypt salts are random — same password → different hashes
        assert hash_password("abc") != hash_password("abc")


# ── JWT tests ─────────────────────────────────────────────────────────────────

class TestJWT:
    def test_create_and_decode_token(self):
        token = create_access_token(user_id=1, username="admin", role="admin")
        payload = decode_token(token)
        assert payload["sub"] == "1"
        assert payload["username"] == "admin"
        assert payload["role"] == "admin"

    def test_extract_role(self):
        token = create_access_token(user_id=2, username="mgr", role="manager")
        assert extract_role(token) == "manager"

    def test_tampered_token_raises(self):
        token = create_access_token(user_id=1, username="admin", role="admin")
        tampered = token[:-4] + "xxxx"
        with pytest.raises(JWTError):
            decode_token(tampered)

    def test_expired_token_raises(self):
        with freeze_time("2026-01-01"):
            token = create_access_token(
                user_id=1, username="admin", role="admin",
                expires_delta=timedelta(seconds=1)
            )
        # Token created at 2026-01-01, now we are in the future
        with freeze_time("2026-01-02"):
            with pytest.raises(JWTError):
                decode_token(token)

    def test_extract_role_invalid_token(self):
        assert extract_role("not.a.valid.token") is None

