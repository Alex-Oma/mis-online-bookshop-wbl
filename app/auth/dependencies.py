from typing import Annotated

from fastapi import Cookie, Depends, HTTPException, status
from jose import JWTError

from app.auth.jwt import decode_token
from app.database import get_db_connection


class CurrentUser:
    '''Represents the currently authenticated user, extracted from the JWT.'''
    def __init__(self, user_id: int, username: str, role: str):
        '''Initialize CurrentUser with user_id, username, and role.'''
        self.user_id = user_id
        self.username = username
        self.role = role


async def get_current_user(
    access_token: Annotated[str | None, Cookie()] = None,
) -> CurrentUser:
    """
    FastAPI dependency — extracts and validates the JWT from the
    httpOnly cookie. Raises 401 if missing or invalid.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )
    # If the cookie is missing, we can immediately raise the exception without trying to decode the token.
    if not access_token:
        raise credentials_exception
    try:
        # Decode the token and extract user information.
        # If decoding fails (e.g., invalid signature, expired), a JWTError will be raised.
        payload = decode_token(access_token)
        user_id: str = payload.get("sub")
        username: str = payload.get("username")
        role: str = payload.get("role")
        if not user_id or not role:
            raise credentials_exception
    except JWTError:
        # If any error occurs during decoding (invalid token, expired, etc.),
        # we catch the JWTError and raise the credentials_exception to return a 401 response.
        raise credentials_exception

    # Return a CurrentUser instance with the extracted information. The user_id is converted to an integer.
    return CurrentUser(user_id=int(user_id), username=username, role=role)


def require_role(*allowed_roles: str):
    """
    FastAPI dependency factory — enforces that the current user
    has one of the specified roles.

    Usage:
        @router.get("/revenue", dependencies=[Depends(require_role("admin"))])
    """
    async def _check(current_user: CurrentUser = Depends(get_current_user)):
        '''Inner dependency that checks the user's role against allowed_roles.'''
        if current_user.role not in allowed_roles:
            # If the user's role is not in the list of allowed roles,
            # we raise a 403 Forbidden error with a message indicating the required roles.
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required role(s): {', '.join(allowed_roles)}",
            )
        return current_user

    # Return the inner dependency function that will be used in FastAPI routes.
    # This allows us to create reusable role-based access control dependencies.
    return _check

