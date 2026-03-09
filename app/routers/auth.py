"""
Authentication endpoints for the MIS admin interface. This includes login and logout.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.auth.jwt import create_access_token
from app.auth.password import verify_password
from app.database import get_db_connection
from app.models.mis import LoginRequest, TokenResponse

if TYPE_CHECKING:
    import asyncpg

# Create a router for auth-related endpoints
router = APIRouter()


@router.post("/login", response_model=TokenResponse)
async def login(
    request: LoginRequest,
    response: Response,
    conn=Depends(get_db_connection),
):
    """
    Authenticate a MIS user. Returns a JWT stored as an httpOnly cookie
    and also in the response body for API clients.
    """
    # Fetch user with role information in a single query
    user = await conn.fetchrow(
        """
        SELECT u.user_id, u.username, u.password_hash, u.is_active,
               r.role_name
        FROM mis.mis_user u
        JOIN mis.mis_user_role ur ON ur.user_id = u.user_id
        JOIN mis.mis_role r       ON r.role_id  = ur.role_id
        WHERE u.username = $1
        LIMIT 1
        """,
        request.username,
    )

    if not user or not user["is_active"]:
        # Return exception for both non-existent users and inactive accounts to avoid user enumeration
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    if not verify_password(request.password, user["password_hash"]):
        # Password mismatch - same error message to prevent user enumeration
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    # Create JWT token with user_id, username, and role
    token = create_access_token(
        user_id=user["user_id"],
        username=user["username"],
        role=user["role_name"],
    )

    # Update last_login
    await conn.execute(
        "UPDATE mis.mis_user SET last_login = NOW() WHERE user_id = $1",
        user["user_id"],
    )

    # Audit log
    await conn.execute(
        """
        INSERT INTO mis.audit_log (user_id, action, resource)
        VALUES ($1, 'login', 'auth')
        """,
        user["user_id"],
    )

    # Set httpOnly cookie (for browser-based admin UI)
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=8 * 3600,
    )

    # Also return token and role in response body for API clients that cannot use cookies
    return TokenResponse(access_token=token, role=user["role_name"])


@router.post("/logout")
async def logout(response: Response):
    """Clear the auth cookie."""
    response.delete_cookie("access_token")
    return {"message": "Logged out successfully"}

