import asyncpg
from asyncpg import Pool
from typing import Optional
import logging

from app.config import get_settings

# Set up a logger for this module
logger = logging.getLogger(__name__)

# Global variable to hold the connection pool instance
_pool: Optional[Pool] = None


async def get_pool() -> Pool:
    """Return the shared asyncpg connection pool, creating it on first call."""
    global _pool
    # Only create the pool once and reuse it across the app.
    # This is more efficient than creating a new pool for each request.
    if _pool is None:
        # Read the database URL from settings and create the connection pool
        settings = get_settings()

        # The database URL (DSN) is expected to be in the format:
        # postgresql://user:password@host:port/database
        dsn = settings.database_url

        # Determine SSL requirement based on environment
        # Production/Supabase requires SSL. Development env doesn't require SSL.
        ssl_mode = "require" if settings.environment == "production" else None

        # Create the asyncpg connection pool with appropriate settings
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=10,
            command_timeout=60,
            ssl=ssl_mode,
        )
        # Log the successful creation of the pool
        logger.info("Database connection pool created")
    # return the existing pool instance
    return _pool


async def close_pool() -> None:
    """Close the connection pool gracefully on app shutdown."""
    global _pool
    # Only close the pool if it exists.
    # This is important to avoid errors during shutdown if the pool was never created.
    if _pool is not None:
        # close the pool and set the global variable to None to clean up resources
        await _pool.close()
        _pool = None
        # Log the successful closure of the pool
        logger.info("Database connection pool closed")


async def get_db_connection():
    """FastAPI dependency — yields a single asyncpg connection from the pool."""
    pool = await get_pool()
    # Use the pool's acquire() method to get a connection, and ensure it is released back to the pool after use.
    async with pool.acquire() as conn:
        yield conn
