"""
Unit tests — app/auth/password.py
"""
from app.auth.password import hash_password, verify_password


def test_hash_is_not_plain_text():
    hashed = hash_password("secret123")
    assert hashed != "secret123"
    assert hashed.startswith("$2b$")


def test_correct_password_verifies():
    hashed = hash_password("mypassword")
    assert verify_password("mypassword", hashed) is True


def test_wrong_password_fails():
    hashed = hash_password("correct")
    assert verify_password("wrong", hashed) is False


def test_different_plaintexts_produce_different_hashes():
    h1 = hash_password("abc")
    h2 = hash_password("abc")
    # bcrypt generates a random salt each time
    assert h1 != h2
