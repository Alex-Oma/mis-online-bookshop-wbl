from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt

from app.config import get_settings


def create_access_token(
    user_id: int,
    username: str,
    role: str,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """Create a signed JWT containing user_id, username, and role."""
    # Use settings for secret key, algorithm, and default expiration time.
    settings = get_settings()
    # Calculate expiration time as now + expires_delta (or default if not provided).
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(hours=settings.jwt_expire_hours)
    )
    # Create JWT payload with standard 'sub' claim for user_id, plus username, role, and exp.
    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "exp": expire,
    }
    # Encode and sign the JWT using the secret key and algorithm from settings.
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    """Decode and validate a JWT. Raises JWTError on failure."""
    # Use settings for secret key and algorithm to decode the token.
    settings = get_settings()
    # Decode the token and return the payload. This will raise JWTError if the token is invalid or expired.
    return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])


def extract_role(token: str) -> Optional[str]:
    """Return the role claim from a token, or None if invalid."""
    try:
        # Decode the token to get the payload, then return the 'role' claim. If decoding fails, return None.
        payload = decode_token(token)
        return payload.get("role")
    except JWTError:
        return None

